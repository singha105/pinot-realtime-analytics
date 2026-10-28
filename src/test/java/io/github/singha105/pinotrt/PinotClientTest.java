package io.github.singha105.pinotrt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PinotClientTest {

    private static final String OK_RESPONSE = """
            {"resultTable":{"dataSchema":{"columnNames":["region","deliveries"],
              "columnDataTypes":["STRING","LONG"]},"rows":[["us-east-1",42],["eu-west-1",7]]},
             "exceptions":[],"numServersQueried":1,"numDocsScanned":49,"totalDocs":1000,
             "numEntriesScannedInFilter":0,"numEntriesScannedPostFilter":98,"numSegmentsQueried":4,
             "numConsumingSegmentsQueried":1,"timeUsedMs":3}
            """;

    @Test
    void parsesStatsAndRows() throws Exception {
        PinotClient.QueryResult r = PinotClient.parse(200, OK_RESPONSE);
        assertTrue(r.ok());
        assertEquals("[[\"us-east-1\",42],[\"eu-west-1\",7]]", r.rowsJson());
        assertEquals(3, r.timeUsedMs());
        assertEquals(49, r.numDocsScanned());
        assertEquals(1000, r.totalDocs());
        assertEquals(0, r.numEntriesScannedInFilter());
        assertEquals(98, r.numEntriesScannedPostFilter());
        assertEquals(4, r.numSegmentsQueried());
        assertEquals(1, r.numConsumingSegmentsQueried());
    }

    @Test
    void firstLongReadsSingleValueQueries() throws Exception {
        PinotClient.QueryResult r = PinotClient.parse(200, """
                {"resultTable":{"rows":[[2000000]]},"exceptions":[],"timeUsedMs":1}""");
        assertEquals(2_000_000L, r.firstLong(new ObjectMapper()));
    }

    @Test
    void brokerExceptionsMarkTheResultFailed() {
        PinotClient.QueryResult r = PinotClient.parse(200, """
                {"exceptions":[{"errorCode":190,"message":"TableDoesNotExistError"}],"timeUsedMs":0}""");
        assertFalse(r.ok());
        assertEquals("190: TableDoesNotExistError", r.exceptions().get(0));
    }

    @Test
    void nonJsonBodyIsAnError() {
        PinotClient.QueryResult r = PinotClient.parse(502, "<html>Bad Gateway</html>");
        assertFalse(r.ok());
        assertEquals(502, r.httpStatus());
    }

    @Test
    void httpErrorWithoutExceptionsIsStillAnError() {
        assertFalse(PinotClient.parse(500, "{}").ok());
    }
}
