package io.github.singha105.pinotrt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RatePacerTest {

    @Test
    void schedulesEventsEvenly() {
        RatePacer pacer = new RatePacer(1_000, 5_000);
        assertEquals(5_000, pacer.dueNanos(0));
        assertEquals(5_000 + 1_000_000, pacer.dueNanos(1));
        assertEquals(5_000 + 250_000_000L, pacer.dueNanos(250));
    }

    @Test
    void holdsTheRequestedRate() {
        long start = System.nanoTime();
        RatePacer pacer = new RatePacer(200, start);
        for (int i = 0; i <= 20; i++) {
            pacer.awaitTurn(i);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMs >= 100, "finished early: " + elapsedMs + " ms");
    }

    @Test
    void preciseModeWakesCloseToTheDueTime() {
        long start = System.nanoTime();
        RatePacer pacer = new RatePacer(20, start, true);
        long worstLateNanos = 0;
        for (int i = 1; i <= 10; i++) {
            pacer.awaitTurn(i);
            worstLateNanos = Math.max(worstLateNanos, System.nanoTime() - pacer.dueNanos(i));
        }
        // A plain 50 ms park oversleeps by several ms on macOS; precise mode should stay well under 2 ms.
        assertTrue(worstLateNanos < 2_000_000L, "woke " + worstLateNanos / 1e6 + " ms late");
    }

    @Test
    void doesNotWaitWhenBehindSchedule() {
        RatePacer pacer = new RatePacer(10, System.nanoTime() - 10_000_000_000L);
        long before = System.nanoTime();
        pacer.awaitTurn(5);
        assertTrue(System.nanoTime() - before < 50_000_000L);
    }

    @Test
    void rejectsNonPositiveRate() {
        assertThrows(IllegalArgumentException.class, () -> new RatePacer(0, 0));
    }
}
