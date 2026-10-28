package io.github.singha105.pinotrt;

import java.util.List;
import java.util.SplittableRandom;

/**
 * Deterministic synthetic webhook-delivery events. The same seed always yields the same sequence of
 * dimensions and metrics; only {@code event_time} comes from the caller.
 */
public final class EventGenerator {

    static final int TENANTS = 200;
    static final int ENDPOINTS_PER_TENANT = 25;
    /** Every 50th endpoint is a flaky receiver that fails far more often than the rest. */
    static final int FLAKY_ENDPOINT_STRIDE = 50;
    static final int TIMEOUT_MS = 10_000;

    static final List<String> REGIONS =
            List.of("us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-south-1", "ap-southeast-2");
    private static final int[] REGION_MEDIAN_LATENCY_MS = {80, 95, 110, 120, 170, 210};

    static final List<String> EVENT_TYPES = List.of(
            "invoice.paid", "charge.succeeded", "customer.updated", "order.shipped",
            "invoice.failed", "subscription.created", "customer.created", "order.delivered",
            "charge.refunded", "subscription.canceled", "user.signup", "payout.sent");

    private static final int[] SUCCESS_CODES = {200, 200, 200, 200, 200, 200, 200, 202, 202, 204};
    private static final int[] CLIENT_ERROR_CODES = {400, 401, 404, 410, 429, 429};
    private static final int[] SERVER_ERROR_CODES = {500, 502, 503, 503, 504};

    private final SplittableRandom rng;
    private final ZipfSampler tenants = new ZipfSampler(TENANTS, 1.1);
    private final ZipfSampler eventTypes = new ZipfSampler(EVENT_TYPES.size(), 0.8);
    private final String runTag;
    private long sequence;

    public EventGenerator(long seed, String runTag) {
        this.rng = new SplittableRandom(seed);
        this.runTag = runTag;
    }

    public DeliveryEvent next(long eventTimeMillis) {
        int tenant = tenants.sample(rng.nextDouble());
        int endpoint = tenant * ENDPOINTS_PER_TENANT + rng.nextInt(ENDPOINTS_PER_TENANT);
        // Each tenant has a home region; 20% of its deliveries leave from another region.
        int region = rng.nextDouble() < 0.8 ? tenant % REGIONS.size() : rng.nextInt(REGIONS.size());
        String eventType = EVENT_TYPES.get(eventTypes.sample(rng.nextDouble()));

        int httpStatus = pickStatus(endpoint % FLAKY_ENDPOINT_STRIDE == 7);
        int latencyMs = httpStatus == 0
                ? TIMEOUT_MS
                : (int) Math.min(TIMEOUT_MS - 1,
                        Math.max(1, REGION_MEDIAN_LATENCY_MS[region] * Math.exp(0.5 * rng.nextGaussian())));
        long payloadBytes = (long) Math.min(262_144, Math.max(128, 2_048 * Math.exp(0.8 * rng.nextGaussian())));

        return new DeliveryEvent(
                "d-" + runTag + "-" + sequence++,
                String.format("tenant-%03d", tenant),
                String.format("ep-%05d", endpoint),
                eventType,
                REGIONS.get(region),
                httpStatus,
                DeliveryEvent.statusClass(httpStatus),
                pickAttempt(),
                latencyMs,
                payloadBytes,
                eventTimeMillis);
    }

    private int pickStatus(boolean flaky) {
        double u = rng.nextDouble();
        double serverError = flaky ? 0.30 : 0.045;
        double clientError = 0.04;
        double timeout = flaky ? 0.05 : 0.015;
        if (u < serverError) {
            return SERVER_ERROR_CODES[rng.nextInt(SERVER_ERROR_CODES.length)];
        }
        if (u < serverError + clientError) {
            return CLIENT_ERROR_CODES[rng.nextInt(CLIENT_ERROR_CODES.length)];
        }
        if (u < serverError + clientError + timeout) {
            return 0;
        }
        return SUCCESS_CODES[rng.nextInt(SUCCESS_CODES.length)];
    }

    private int pickAttempt() {
        double u = rng.nextDouble();
        if (u < 0.85) {
            return 1;
        }
        if (u < 0.95) {
            return 2;
        }
        if (u < 0.98) {
            return 3;
        }
        return u < 0.995 ? 4 : 5;
    }
}
