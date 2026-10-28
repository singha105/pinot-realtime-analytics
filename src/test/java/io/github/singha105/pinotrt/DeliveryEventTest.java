package io.github.singha105.pinotrt;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DeliveryEventTest {

    @ParameterizedTest
    @CsvSource({"0,timeout", "200,2xx", "204,2xx", "301,other", "404,4xx", "429,4xx", "500,5xx", "504,5xx"})
    void mapsStatusCodeToClass(int status, String expected) {
        assertEquals(expected, DeliveryEvent.statusClass(status));
    }
}
