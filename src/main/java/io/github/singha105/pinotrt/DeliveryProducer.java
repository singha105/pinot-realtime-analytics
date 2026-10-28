package io.github.singha105.pinotrt;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Streams synthetic webhook-delivery events to a partitioned Kafka topic at a fixed rate. Records are
 * keyed by tenant, so all of a tenant's deliveries land in one partition.
 */
final class DeliveryProducer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DeliveryProducer() {}

    static int run(String[] argv) throws Exception {
        Args args = Args.parse(argv, Args.keys(
                "bootstrap", "topic", "partitions", "rate", "count", "seed", "summary"));
        String bootstrap = args.str("bootstrap", "localhost:9094");
        String topic = args.str("topic", "webhook-deliveries");
        int partitions = args.integer("partitions", 4);
        double rate = args.dbl("rate", 20_000);
        long count = args.lng("count", 1_000_000);
        long seed = args.lng("seed", 42);
        String summaryPath = args.str("summary", "");

        ensureTopic(bootstrap, topic, partitions);

        EventGenerator generator = new EventGenerator(seed, Long.toString(System.currentTimeMillis(), 36));
        AtomicLong acked = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicReference<Exception> firstError = new AtomicReference<>();

        System.out.printf("producing %,d events to %s (%d partitions) at %,.0f events/s%n",
                count, topic, partitions, rate);
        long start = System.nanoTime();
        try (Producer<String, byte[]> producer = new KafkaProducer<>(producerProps(bootstrap))) {
            RatePacer pacer = new RatePacer(rate, start);
            long nextReport = start + 5_000_000_000L;
            for (long i = 0; i < count; i++) {
                pacer.awaitTurn(i);
                DeliveryEvent event = generator.next(System.currentTimeMillis());
                producer.send(new ProducerRecord<>(topic, event.tenantId(), MAPPER.writeValueAsBytes(event)),
                        (meta, err) -> {
                            if (err == null) {
                                acked.incrementAndGet();
                            } else {
                                failed.incrementAndGet();
                                firstError.compareAndSet(null, err);
                            }
                        });
                long now = System.nanoTime();
                if (now >= nextReport) {
                    System.out.printf("  sent %,d  acked %,d  failed %,d  (%.0f events/s)%n",
                            i + 1, acked.get(), failed.get(), (i + 1) / ((now - start) / 1e9));
                    nextReport += 5_000_000_000L;
                }
            }
            producer.flush();
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        double achieved = acked.get() / seconds;
        System.out.printf("done: acked %,d  failed %,d  in %.1f s  (%,.0f events/s)%n",
                acked.get(), failed.get(), seconds, achieved);
        if (firstError.get() != null) {
            System.err.println("first send error: " + firstError.get());
        }
        if (!summaryPath.isEmpty()) {
            writeSummary(Path.of(summaryPath), Map.of(
                    "topic", topic,
                    "partitions", partitions,
                    "target_rate", rate,
                    "requested", count,
                    "acked", acked.get(),
                    "failed", failed.get(),
                    "seconds", Math.round(seconds * 10) / 10.0,
                    "achieved_rate", Math.round(achieved)));
        }
        return failed.get() == 0 && acked.get() == count ? 0 : 1;
    }

    static int createTopic(String[] argv) throws Exception {
        Args args = Args.parse(argv, Args.keys("bootstrap", "topic", "partitions"));
        ensureTopic(args.str("bootstrap", "localhost:9094"), args.str("topic", "webhook-deliveries"),
                args.integer("partitions", 4));
        return 0;
    }

    static Properties producerProps(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(256 * 1024));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }

    /** Creates the topic with the requested partition count, or checks an existing one matches. */
    static void ensureTopic(String bootstrap, String topic, int partitions)
            throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(props)) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
                System.out.printf("created topic %s with %d partitions%n", topic, partitions);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
                TopicDescription desc = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
                if (desc.partitions().size() != partitions) {
                    throw new IllegalStateException("topic " + topic + " exists with "
                            + desc.partitions().size() + " partitions, expected " + partitions);
                }
                System.out.printf("topic %s exists with %d partitions%n", topic, partitions);
            }
        }
    }

    private static void writeSummary(Path path, Map<String, Object> summary) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), new TreeMap<>(summary));
    }
}
