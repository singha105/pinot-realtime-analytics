package io.github.singha105.pinotrt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/** Admin steps the benchmark needs: wait until every row is ingested, then seal consuming segments. */
final class TableOps {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TableOps() {}

    /** Polls {@code COUNT(*)} on each table until it reaches the expected row count. */
    static int awaitCount(String[] argv) throws Exception {
        Args args = Args.parse(argv, Args.keys("broker", "tables", "expected", "timeout-sec", "summary"));
        PinotClient client = new PinotClient(args.str("broker", "http://localhost:18000"), Duration.ofSeconds(10));
        List<String> tables = args.list("tables", "deliveries_noidx,deliveries_inverted,deliveries_startree");
        long expected = args.lng("expected", -1);
        long start = System.nanoTime();
        long deadline = start + Duration.ofSeconds(args.integer("timeout-sec", 600)).toNanos();
        if (expected < 0) {
            throw new IllegalArgumentException("--expected is required");
        }
        for (String table : tables) {
            long count = -1;
            long lastPrint = 0;
            while (System.nanoTime() < deadline) {
                PinotClient.QueryResult r = client.query("SELECT COUNT(*) FROM " + table);
                count = r.ok() ? r.firstLong(MAPPER) : -1;
                if (count >= expected) {
                    break;
                }
                if (System.nanoTime() - lastPrint > 5_000_000_000L) {
                    System.out.printf("  %s: %,d / %,d rows%n", table, count, expected);
                    lastPrint = System.nanoTime();
                }
                Thread.sleep(500);
            }
            if (count != expected) {
                System.err.printf("%s has %,d rows, expected %,d%n", table, count, expected);
                return 1;
            }
            System.out.printf("%s: %,d rows ingested%n", table, count);
        }
        double seconds = Math.round((System.nanoTime() - start) / 1e8) / 10.0;
        System.out.printf("all tables caught up %.1f s after the wait started%n", seconds);
        String summary = args.str("summary", "");
        if (!summary.isEmpty()) {
            LoadGenerator.createParent(Path.of(summary));
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(Path.of(summary).toFile(),
                    Map.of("expected_rows", expected, "seconds", seconds));
        }
        return 0;
    }

    /**
     * Force-commits consuming segments so every row sits in an immutable segment. Star-tree indexes are
     * only built when a segment is sealed, so without this step the star-tree table would still scan.
     */
    static int forceCommit(String[] argv) throws Exception {
        Args args = Args.parse(argv, Args.keys("controller", "tables", "timeout-sec"));
        String controller = args.str("controller", "http://localhost:19000");
        List<String> tables = args.list("tables", "deliveries_noidx,deliveries_inverted,deliveries_startree");
        long timeoutNanos = Duration.ofSeconds(args.integer("timeout-sec", 300)).toNanos();
        PinotClient client = new PinotClient("http://unused", Duration.ofSeconds(30));

        for (String table : tables) {
            JsonNode submitted = client.controllerPost(controller, "/tables/" + table + "/forceCommit");
            String jobId = submitted.path("forceCommitJobId").asText();
            if (jobId.isEmpty()) {
                throw new IllegalStateException("no forceCommitJobId in response: " + submitted);
            }
            long start = System.nanoTime();
            int remaining = -1;
            while (System.nanoTime() - start < timeoutNanos) {
                JsonNode status = client.controllerGet(controller, "/tables/forceCommitStatus/" + jobId);
                remaining = status.path("numberOfSegmentsYetToBeCommitted").asInt(-1);
                if (remaining == 0) {
                    break;
                }
                Thread.sleep(1000);
            }
            if (remaining != 0) {
                System.err.printf("%s: %d segment(s) still committing after timeout%n", table, remaining);
                return 1;
            }
            System.out.printf("%s: consuming segments committed (job %s)%n", table, jobId);
        }
        return 0;
    }
}
