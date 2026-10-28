package io.github.singha105.pinotrt;

import java.util.concurrent.locks.LockSupport;

/**
 * Fixed-rate schedule: event {@code i} is due at {@code start + i / rate}. Callers that fall behind
 * are not slept, so the long-run rate holds even when a single send stalls.
 */
final class RatePacer {

    private static final long SPIN_NANOS = 1_000_000;

    private final long startNanos;
    private final double nanosPerEvent;
    private final boolean precise;

    RatePacer(double eventsPerSecond, long startNanos) {
        this(eventsPerSecond, startNanos, false);
    }

    /**
     * @param precise wake within ~0.1 ms of the due time at the cost of spinning for the last millisecond.
     *     The load generator needs this because it measures latency from the due time, so any oversleep
     *     would be reported as query latency.
     */
    RatePacer(double eventsPerSecond, long startNanos, boolean precise) {
        if (eventsPerSecond <= 0) {
            throw new IllegalArgumentException("rate must be positive");
        }
        this.startNanos = startNanos;
        this.nanosPerEvent = 1_000_000_000.0 / eventsPerSecond;
        this.precise = precise;
    }

    long dueNanos(long index) {
        return startNanos + (long) (index * nanosPerEvent);
    }

    /** Parks until event {@code index} is due; returns immediately when already late. */
    void awaitTurn(long index) {
        long due = dueNanos(index);
        long wait;
        if (!precise) {
            while ((wait = due - System.nanoTime()) > 0) {
                LockSupport.parkNanos(wait);
            }
            return;
        }
        // macOS coalesces timers, so a single 50 ms park can oversleep by ~6 ms. Parking for half the
        // remaining wait keeps each oversleep inside the slack that is left; the last millisecond spins.
        while ((wait = due - System.nanoTime()) > SPIN_NANOS) {
            LockSupport.parkNanos(wait / 2);
        }
        while (System.nanoTime() < due) {
            Thread.onSpinWait();
        }
    }
}
