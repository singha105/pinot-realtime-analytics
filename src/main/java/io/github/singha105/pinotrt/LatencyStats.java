package io.github.singha105.pinotrt;

import java.util.Arrays;

/** Nearest-rank percentiles over raw samples; no histogram bucketing, so nothing is rounded away. */
final class LatencyStats {

    record Summary(int count, double p50Ms, double p95Ms, double p99Ms, double maxMs, double meanMs) {}

    private LatencyStats() {}

    /** Nearest-rank percentile of an ascending array: the smallest value with at least p% of samples at or below it. */
    static long percentile(long[] sorted, double p) {
        if (sorted.length == 0) {
            throw new IllegalArgumentException("no samples");
        }
        if (p <= 0 || p > 100) {
            throw new IllegalArgumentException("percentile must be in (0, 100]");
        }
        int rank = (int) Math.ceil(p / 100.0 * sorted.length);
        return sorted[Math.max(rank, 1) - 1];
    }

    static Summary summarize(long[] nanos) {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        double sum = 0;
        for (long v : sorted) {
            sum += v;
        }
        return new Summary(
                sorted.length,
                toMs(percentile(sorted, 50)),
                toMs(percentile(sorted, 95)),
                toMs(percentile(sorted, 99)),
                toMs(sorted[sorted.length - 1]),
                sum / sorted.length / 1_000_000.0);
    }

    static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return percentile(sorted, 50);
    }

    private static double toMs(long nanos) {
        return nanos / 1_000_000.0;
    }
}
