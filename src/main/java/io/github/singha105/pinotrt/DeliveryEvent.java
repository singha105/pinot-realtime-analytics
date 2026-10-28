package io.github.singha105.pinotrt;

import com.fasterxml.jackson.annotation.JsonProperty;

/** One webhook delivery attempt, serialized as the JSON row the Pinot table ingests. */
public record DeliveryEvent(
        @JsonProperty("delivery_id") String deliveryId,
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("endpoint_id") String endpointId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("region") String region,
        @JsonProperty("http_status") int httpStatus,
        @JsonProperty("status_class") String statusClass,
        @JsonProperty("attempt") int attempt,
        @JsonProperty("latency_ms") int latencyMs,
        @JsonProperty("payload_bytes") long payloadBytes,
        @JsonProperty("event_time") long eventTime) {

    /** Status 0 means the receiver never answered before the delivery timeout. */
    static String statusClass(int httpStatus) {
        if (httpStatus == 0) {
            return "timeout";
        }
        if (httpStatus >= 200 && httpStatus < 300) {
            return "2xx";
        }
        if (httpStatus >= 400 && httpStatus < 500) {
            return "4xx";
        }
        if (httpStatus >= 500 && httpStatus < 600) {
            return "5xx";
        }
        return "other";
    }
}
