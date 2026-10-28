package io.github.singha105.pinotrt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class EventGeneratorTest {

    @Test
    void sameSeedGivesSameEvents() {
        EventGenerator a = new EventGenerator(42, "run");
        EventGenerator b = new EventGenerator(42, "run");
        for (int i = 0; i < 1_000; i++) {
            assertEquals(a.next(1_000L + i), b.next(1_000L + i));
        }
    }

    @Test
    void differentSeedsDiverge() {
        assertNotEquals(new EventGenerator(1, "run").next(0), new EventGenerator(2, "run").next(0));
    }

    @Test
    void fieldsStayWithinTheModel() {
        EventGenerator gen = new EventGenerator(7, "run");
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 50_000; i++) {
            DeliveryEvent e = gen.next(123L);
            assertTrue(ids.add(e.deliveryId()), "duplicate id " + e.deliveryId());
            int tenant = Integer.parseInt(e.tenantId().substring("tenant-".length()));
            int endpoint = Integer.parseInt(e.endpointId().substring("ep-".length()));
            assertTrue(tenant >= 0 && tenant < EventGenerator.TENANTS);
            assertEquals(tenant, endpoint / EventGenerator.ENDPOINTS_PER_TENANT, "endpoint belongs to its tenant");
            assertTrue(EventGenerator.REGIONS.contains(e.region()));
            assertTrue(EventGenerator.EVENT_TYPES.contains(e.eventType()));
            assertEquals(DeliveryEvent.statusClass(e.httpStatus()), e.statusClass());
            assertTrue(e.attempt() >= 1 && e.attempt() <= 5);
            assertTrue(e.payloadBytes() >= 128 && e.payloadBytes() <= 262_144);
            if (e.httpStatus() == 0) {
                assertEquals(EventGenerator.TIMEOUT_MS, e.latencyMs());
            } else {
                assertTrue(e.latencyMs() >= 1 && e.latencyMs() < EventGenerator.TIMEOUT_MS);
            }
            assertEquals(123L, e.eventTime());
        }
    }

    @Test
    void flakyEndpointsFailMoreOften() {
        EventGenerator gen = new EventGenerator(11, "run");
        long flaky = 0;
        long flakyFailures = 0;
        long healthy = 0;
        long healthyFailures = 0;
        for (int i = 0; i < 400_000; i++) {
            DeliveryEvent e = gen.next(0);
            boolean failed = e.statusClass().equals("5xx");
            if (Integer.parseInt(e.endpointId().substring(3)) % EventGenerator.FLAKY_ENDPOINT_STRIDE == 7) {
                flaky++;
                flakyFailures += failed ? 1 : 0;
            } else {
                healthy++;
                healthyFailures += failed ? 1 : 0;
            }
        }
        assertTrue((double) flakyFailures / flaky > 4 * ((double) healthyFailures / healthy));
    }

    @Test
    void serializesWithPinotColumnNames() throws Exception {
        JsonNode json = new ObjectMapper().valueToTree(new EventGenerator(3, "run").next(99L));
        for (String column : new String[] {"delivery_id", "tenant_id", "endpoint_id", "event_type", "region",
                "http_status", "status_class", "attempt", "latency_ms", "payload_bytes", "event_time"}) {
            assertTrue(json.has(column), "missing " + column);
        }
        assertEquals(11, json.size());
        assertEquals(99L, json.get("event_time").asLong());
    }
}
