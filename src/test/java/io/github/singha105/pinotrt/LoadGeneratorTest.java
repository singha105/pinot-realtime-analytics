package io.github.singha105.pinotrt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LoadGeneratorTest {

    @Test
    void parsesDescriptionAndStatement() {
        LoadGenerator.Query q = LoadGenerator.parseQuery("q1", """
                -- Rollup per region.
                -- Second line.
                SELECT region, COUNT(*)
                FROM {table}
                GROUP BY region;
                """);
        assertEquals("q1", q.id());
        assertEquals("Rollup per region. Second line.", q.description());
        assertEquals("SELECT region, COUNT(*) FROM {table} GROUP BY region", q.sqlTemplate());
        assertEquals("SELECT region, COUNT(*) FROM t1 GROUP BY region", q.sqlFor("t1"));
    }

    @Test
    void requiresTablePlaceholder() {
        assertThrows(IllegalArgumentException.class, () -> LoadGenerator.parseQuery("bad", "SELECT 1 FROM t"));
    }

    @Test
    void loadsQueriesInNameOrder(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("q2_b.sql"), "SELECT 2 FROM {table}");
        Files.writeString(dir.resolve("q1_a.sql"), "SELECT 1 FROM {table}");
        Files.writeString(dir.resolve("notes.txt"), "ignored");
        List<LoadGenerator.Query> queries = LoadGenerator.loadQueries(dir);
        assertEquals(List.of("q1_a", "q2_b"), queries.stream().map(LoadGenerator.Query::id).toList());
    }

    @Test
    void shippedQueriesParse() throws Exception {
        List<LoadGenerator.Query> queries = LoadGenerator.loadQueries(Path.of("queries"));
        assertEquals(3, queries.size());
    }

    @Test
    void runSummaryCountsErrorsAndDropsSeparately() {
        PinotClient.QueryResult ok = PinotClient.parse(200, """
                {"resultTable":{"rows":[[1]]},"exceptions":[],"timeUsedMs":2,"numDocsScanned":10,"totalDocs":100,
                 "numEntriesScannedInFilter":5,"numEntriesScannedPostFilter":10}""");
        PinotClient.QueryResult failed = PinotClient.parse(500, "{}");
        LoadGenerator.Samples samples = new LoadGenerator.Samples(
                new long[] {2_000_000, 4_000_000, 9_000_000, 0},
                new PinotClient.QueryResult[] {ok, ok, failed, null},
                1);
        LoadGenerator.Run run = LoadGenerator.toRun("q", "t", 20, samples);
        assertEquals(4, run.requests());
        assertEquals(1, run.errors());
        assertEquals(1, run.dropped());
        assertEquals(2, run.client().count());
        assertEquals(4.0, run.client().p99Ms());
        assertEquals(2, run.brokerTimeP50Ms());
        assertEquals(10, run.docsScannedMedian());
        assertEquals(100, run.totalDocs());
    }
}
