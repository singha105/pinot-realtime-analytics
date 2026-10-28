package io.github.singha105.pinotrt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class ZipfSamplerTest {

    @Test
    void boundaryDrawsMapToFirstAndLastRank() {
        ZipfSampler zipf = new ZipfSampler(10, 1.0);
        assertEquals(0, zipf.sample(0.0));
        assertEquals(9, zipf.sample(Math.nextDown(1.0)));
    }

    @Test
    void lowerRanksAreDrawnMoreOften() {
        ZipfSampler zipf = new ZipfSampler(50, 1.1);
        SplittableRandom rng = new SplittableRandom(1);
        int[] counts = new int[zipf.size()];
        for (int i = 0; i < 200_000; i++) {
            int rank = zipf.sample(rng.nextDouble());
            assertTrue(rank >= 0 && rank < zipf.size());
            counts[rank]++;
        }
        assertTrue(counts[0] > counts[1] && counts[1] > counts[5] && counts[5] > counts[49]);
        // With s = 1.1, rank 0 should get about 2^1.1 = 2.14 times the draws of rank 1.
        double ratio = (double) counts[0] / counts[1];
        assertTrue(ratio > 1.9 && ratio < 2.4, "ratio was " + ratio);
    }

    @Test
    void rejectsEmptyDomain() {
        assertThrows(IllegalArgumentException.class, () -> new ZipfSampler(0, 1.0));
    }
}
