package io.github.singha105.pinotrt;

import java.util.Arrays;

/** Entry point: {@code java -jar target/pinot-rt.jar <command> [--option value ...]}. */
public final class Main {

    private static final String USAGE = """
            usage: java -jar pinot-rt.jar <command> [--option value ...]

              topic      create the partitioned Kafka topic (Pinot checks it exists when a table is added)
              produce    stream synthetic webhook-delivery events to Kafka
              await      wait until each table has the expected row count
              commit     force-commit consuming segments so star-tree indexes get built
              loadgen    drive N QPS at the broker and record p50/p95/p99 latency
              freshness  measure produce-to-queryable time
            """;

    private Main() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.print(USAGE);
            System.exit(2);
        }
        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        int code = switch (args[0]) {
            case "topic" -> DeliveryProducer.createTopic(rest);
            case "produce" -> DeliveryProducer.run(rest);
            case "await" -> TableOps.awaitCount(rest);
            case "commit" -> TableOps.forceCommit(rest);
            case "loadgen" -> LoadGenerator.run(rest);
            case "freshness" -> FreshnessProbe.run(rest);
            default -> {
                System.err.print(USAGE);
                yield 2;
            }
        };
        System.exit(code);
    }
}
