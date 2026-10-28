package io.github.singha105.pinotrt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class LatencyStatsTest {

    @Test
    void nearestRankOnOneToHundred() {
        long[] sorted = LongStream.rangeClosed(1, 100).toArray();
        assertEquals(50, LatencyStats.percentile(sorted, 50));
        assertEquals(95, LatencyStats.percentile(sorted, 95));
        assertEquals(99, LatencyStats.percentile(sorted, 99));
        assertEquals(100, LatencyStats.percentile(sorted, 100));
        assertEquals(1, LatencyStats.percentile(sorted, 0.5));
    }

    @Test
    void smallSamplesRoundUpToTheNextRank() {
        long[] sorted = {10, 20, 30};
        assertEquals(20, LatencyStats.percentile(sorted, 50));
        assertEquals(30, LatencyStats.percentile(sorted, 95));
    }

    @Test
    void summarizeSortsAndConvertsToMillis() {
        long[] nanos = new long[100];
        for (int i = 0; i < 100; i++) {
            nanos[i] = (100 - i) * 1_000_000L; // 100 ms down to 1 ms, unsorted on purpose
        }
        LatencyStats.Summary s = LatencyStats.summarize(nanos);
        assertEquals(100, s.count());
        assertEquals(50.0, s.p50Ms());
        assertEquals(95.0, s.p95Ms());
        assertEquals(99.0, s.p99Ms());
        assertEquals(100.0, s.maxMs());
        assertEquals(50.5, s.meanMs(), 1e-9);
        assertEquals(100 * 1_000_000L, nanos[0], "input array must not be reordered");
    }

    @Test
    void rejectsBadInput() {
        assertThrows(IllegalArgumentException.class, () -> LatencyStats.percentile(new long[0], 50));
        assertThrows(IllegalArgumentException.class, () -> LatencyStats.percentile(new long[] {1}, 0));
        assertThrows(IllegalArgumentException.class, () -> LatencyStats.percentile(new long[] {1}, 101));
    }
}
