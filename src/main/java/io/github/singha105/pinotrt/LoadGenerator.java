package io.github.singha105.pinotrt;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

/**
 * Open-loop query load against the Pinot broker. Request {@code i} is scheduled at {@code start + i/qps}
 * and its latency is measured from that scheduled time, not from when it was actually sent, so a stalled
 * broker shows up in the tail instead of quietly lowering the offered load (coordinated omission).
 */
final class LoadGenerator {

    record Query(String id, String description, String sqlTemplate) {
        String sqlFor(String table) {
            return sqlTemplate.replace("{table}", table);
        }
    }

    record Run(
            String queryId,
            String table,
            double qps,
            int requests,
            int errors,
            int dropped,
            LatencyStats.Summary client,
            long brokerTimeP50Ms,
            long docsScannedMedian,
            long totalDocs,
            long entriesInFilterMedian,
            long entriesPostFilterMedian,
            String firstError) {}

    private LoadGenerator() {}

    static int run(String[] argv) throws Exception {
        Args args = Args.parse(argv, Args.keys(
                "broker", "tables", "queries", "only", "qps", "duration-sec", "warmup-sec", "max-in-flight",
                "timeout-ms", "csv", "markdown", "fail-on-errors"));
        String broker = args.str("broker", "http://localhost:18000");
        List<String> tables = args.list("tables", "deliveries_noidx,deliveries_inverted,deliveries_startree");
        Path queryDir = Path.of(args.str("queries", "queries"));
        List<Double> qpsLevels = args.list("qps", "20").stream().map(Double::parseDouble).toList();
        List<String> only = args.list("only", "");
        int durationSec = args.integer("duration-sec", 30);
        int warmupSec = args.integer("warmup-sec", 5);
        int maxInFlight = args.integer("max-in-flight", 64);
        Duration timeout = Duration.ofMillis(args.integer("timeout-ms", 10_000));

        List<Query> queries = loadQueries(queryDir).stream()
                .filter(q -> only.isEmpty() || only.contains(q.id()))
                .toList();
        if (queries.isEmpty()) {
            throw new IllegalArgumentException("no queries match --only " + only);
        }
        PinotClient client = new PinotClient(broker, timeout);

        Map<String, Boolean> sameAnswer = new LinkedHashMap<>();
        for (Query q : queries) {
            sameAnswer.put(q.id(), checkSameAnswer(client, q, tables));
        }

        List<Run> runs = new ArrayList<>();
        for (Query q : queries) {
            for (double qps : qpsLevels) {
                for (String table : tables) {
                    String sql = q.sqlFor(table);
                    System.out.printf("%s on %s: warmup %ds, then %ds at %.0f QPS%n",
                            q.id(), table, warmupSec, durationSec, qps);
                    drive(client, sql, qps, warmupSec, maxInFlight);
                    Run run = toRun(q.id(), table, qps, drive(client, sql, qps, durationSec, maxInFlight));
                    System.out.printf("  p50 %.2f ms  p95 %.2f ms  p99 %.2f ms  errors %d  dropped %d%n",
                            run.client().p50Ms(), run.client().p95Ms(), run.client().p99Ms(),
                            run.errors(), run.dropped());
                    runs.add(run);
                }
            }
        }

        if (args.str("csv", null) != null) {
            writeCsv(Path.of(args.str("csv", null)), runs, durationSec);
        }
        if (args.str("markdown", null) != null) {
            writeMarkdown(Path.of(args.str("markdown", null)), queries, runs, sameAnswer, qpsLevels, durationSec);
        }
        boolean allMatch = !sameAnswer.containsValue(false);
        boolean clean = runs.stream().allMatch(r -> r.errors() == 0 && r.dropped() == 0);
        // Overload sweeps expect drops and timeouts; they still fail if an index changes an answer.
        boolean failOnErrors = Boolean.parseBoolean(args.str("fail-on-errors", "true"));
        return allMatch && (clean || !failOnErrors) ? 0 : 1;
    }

    /** Reads {@code *.sql} files in name order; leading {@code --} lines become the description. */
    static List<Query> loadQueries(Path dir) throws IOException {
        List<Path> files;
        try (Stream<Path> s = Files.list(dir)) {
            files = s.filter(p -> p.toString().endsWith(".sql")).sorted().toList();
        }
        if (files.isEmpty()) {
            throw new IOException("no .sql files in " + dir);
        }
        List<Query> queries = new ArrayList<>();
        for (Path f : files) {
            queries.add(parseQuery(f.getFileName().toString().replace(".sql", ""), Files.readString(f)));
        }
        return queries;
    }

    static Query parseQuery(String id, String text) {
        StringBuilder description = new StringBuilder();
        StringBuilder sql = new StringBuilder();
        for (String line : text.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.startsWith("--")) {
                description.append(description.length() > 0 ? " " : "").append(trimmed.substring(2).trim());
            } else if (!trimmed.isEmpty()) {
                sql.append(sql.length() > 0 ? " " : "").append(trimmed);
            }
        }
        String statement = sql.toString().replaceAll(";\\s*$", "");
        if (!statement.contains("{table}")) {
            throw new IllegalArgumentException(id + ": query must use the {table} placeholder");
        }
        return new Query(id, description.toString(), statement);
    }

    /** Every table holds the same rows, so an index must never change the answer. */
    private static boolean checkSameAnswer(PinotClient client, Query q, List<String> tables) throws Exception {
        String reference = null;
        for (String table : tables) {
            PinotClient.QueryResult r = client.query(q.sqlFor(table));
            if (!r.ok()) {
                System.err.printf("%s on %s failed: %s%n", q.id(), table, r.exceptions());
                return false;
            }
            if (reference == null) {
                reference = r.rowsJson();
            } else if (!reference.equals(r.rowsJson())) {
                System.err.printf("%s: %s returned a different answer%n  expected %s%n  got      %s%n",
                        q.id(), table, reference, r.rowsJson());
                return false;
            }
        }
        System.out.printf("%s: same answer on all %d tables%n", q.id(), tables.size());
        return true;
    }

    record Samples(long[] latencyNanos, PinotClient.QueryResult[] results, int dropped) {}

    static Samples drive(PinotClient client, String sql, double qps, int seconds, int maxInFlight) {
        int total = (int) Math.round(qps * seconds);
        long[] latency = new long[total];
        PinotClient.QueryResult[] results = new PinotClient.QueryResult[total];
        Semaphore inFlight = new Semaphore(maxInFlight);
        List<CompletableFuture<Void>> pending = new ArrayList<>(total);
        int dropped = 0;

        RatePacer pacer = new RatePacer(qps, System.nanoTime(), true);
        for (int i = 0; i < total; i++) {
            pacer.awaitTurn(i);
            long due = pacer.dueNanos(i);
            if (!inFlight.tryAcquire()) {
                dropped++;
                continue;
            }
            int idx = i;
            pending.add(client.sendAsync(sql).handle((resp, err) -> {
                latency[idx] = System.nanoTime() - due;
                inFlight.release();
                results[idx] = err != null
                        ? new PinotClient.QueryResult(-1, "[]", List.of(err.toString()), -1, -1, -1, -1, -1, -1, -1)
                        : PinotClient.parse(resp.statusCode(), resp.body());
                return null;
            }));
        }
        CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
        return new Samples(latency, results, dropped);
    }

    static Run toRun(String queryId, String table, double qps, Samples s) {
        List<Long> okLatency = new ArrayList<>();
        List<Long> brokerMs = new ArrayList<>();
        List<Long> docs = new ArrayList<>();
        List<Long> inFilter = new ArrayList<>();
        List<Long> postFilter = new ArrayList<>();
        long totalDocs = -1;
        int errors = 0;
        String firstError = "";
        for (int i = 0; i < s.results().length; i++) {
            PinotClient.QueryResult r = s.results()[i];
            if (r == null) {
                continue; // dropped before sending
            }
            if (!r.ok()) {
                errors++;
                if (firstError.isEmpty()) {
                    firstError = String.join("; ", r.exceptions());
                }
                continue;
            }
            okLatency.add(s.latencyNanos()[i]);
            brokerMs.add(r.timeUsedMs());
            docs.add(r.numDocsScanned());
            inFilter.add(r.numEntriesScannedInFilter());
            postFilter.add(r.numEntriesScannedPostFilter());
            totalDocs = r.totalDocs();
        }
        if (okLatency.isEmpty()) {
            throw new IllegalStateException(queryId + " on " + table + ": every request failed: " + firstError);
        }
        return new Run(queryId, table, qps, s.results().length, errors, s.dropped(),
                LatencyStats.summarize(toArray(okLatency)),
                LatencyStats.median(toArray(brokerMs)),
                LatencyStats.median(toArray(docs)),
                totalDocs,
                LatencyStats.median(toArray(inFilter)),
                LatencyStats.median(toArray(postFilter)),
                firstError);
    }

    private static long[] toArray(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).toArray();
    }

    static void writeCsv(Path path, List<Run> runs, int durationSec) throws IOException {
        createParent(path);
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(path))) {
            out.println("query,table,qps,duration_sec,requests,errors,dropped,p50_ms,p95_ms,p99_ms,max_ms,mean_ms,"
                    + "broker_time_p50_ms,docs_scanned_median,total_docs,entries_in_filter_median,entries_post_filter_median");
            for (Run r : runs) {
                LatencyStats.Summary c = r.client();
                out.println(String.format(Locale.ROOT, "%s,%s,%.0f,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d",
                        r.queryId(), r.table(), r.qps(), durationSec, r.requests(), r.errors(), r.dropped(),
                        c.p50Ms(), c.p95Ms(), c.p99Ms(), c.maxMs(), c.meanMs(),
                        r.brokerTimeP50Ms(), r.docsScannedMedian(), r.totalDocs(),
                        r.entriesInFilterMedian(), r.entriesPostFilterMedian()));
            }
        }
    }

    static void writeMarkdown(Path path, List<Query> queries, List<Run> runs, Map<String, Boolean> sameAnswer,
            List<Double> qpsLevels, int durationSec) throws IOException {
        createParent(path);
        boolean sweep = qpsLevels.size() > 1;
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(path))) {
            String rates = qpsLevels.stream().map(q -> String.format(Locale.ROOT, "%.0f", q))
                    .reduce((a, b) -> a + ", " + b).orElse("");
            out.printf(Locale.ROOT, "Each row: %s QPS for %d s after a warm-up, open loop, latency measured client-side"
                    + " from the scheduled send time. Errors include requests dropped at the in-flight cap.%n%n",
                    rates, durationSec);
            for (Query q : queries) {
                out.printf("**%s**: %s Same answer on every table: %s.%n%n", q.id(), q.description(),
                        sameAnswer.get(q.id()) ? "yes" : "NO");
                out.println("```sql");
                out.println(q.sqlFor("<table>"));
                out.println("```");
                out.println();
                out.println((sweep ? "| QPS " : "")
                        + "| Table | p50 ms | p95 ms | p99 ms | Broker time p50 ms | Docs scanned | Filter entries scanned | Errors |");
                out.println((sweep ? "|---:" : "") + "|---|---:|---:|---:|---:|---:|---:|---:|");
                for (Run r : runs) {
                    if (!r.queryId().equals(q.id())) {
                        continue;
                    }
                    LatencyStats.Summary c = r.client();
                    out.println(String.format(Locale.ROOT, "%s| %s | %.2f | %.2f | %.2f | %d | %,d | %,d | %d |",
                            sweep ? String.format(Locale.ROOT, "| %.0f ", r.qps()) : "",
                            r.table(), c.p50Ms(), c.p95Ms(), c.p99Ms(), r.brokerTimeP50Ms(),
                            r.docsScannedMedian(), r.entriesInFilterMedian(), r.errors() + r.dropped()));
                }
                out.println();
            }
        }
    }

    static void createParent(Path path) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
    }
}
