package io.github.singha105.pinotrt;

import java.util.Arrays;

/**
 * Samples ranks 0..n-1 with probability proportional to 1/(rank+1)^s, so a few tenants
 * produce most of the traffic, like a real multi-tenant webhook service.
 */
final class ZipfSampler {

    private final double[] cdf;

    ZipfSampler(int n, double exponent) {
        if (n <= 0) {
            throw new IllegalArgumentException("n must be positive");
        }
        cdf = new double[n];
        double total = 0;
        for (int rank = 0; rank < n; rank++) {
            total += 1.0 / Math.pow(rank + 1, exponent);
            cdf[rank] = total;
        }
        for (int rank = 0; rank < n; rank++) {
            cdf[rank] /= total;
        }
        cdf[n - 1] = 1.0;
    }

    /** @param u a uniform draw in [0, 1) */
    int sample(double u) {
        int idx = Arrays.binarySearch(cdf, u);
        // binarySearch returns (-(insertion point) - 1) on a miss; the insertion point is the rank whose
        // cumulative probability first exceeds u.
        int rank = idx >= 0 ? idx + 1 : -idx - 1;
        return Math.min(rank, cdf.length - 1);
    }

    int size() {
        return cdf.length;
    }
}
