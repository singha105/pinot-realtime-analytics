package io.github.singha105.pinotrt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ArgsTest {

    @Test
    void parsesTypedValuesAndDefaults() {
        Args args = Args.parse(new String[] {"--rate", "2500", "--tables", "a, b,,c", "--verbose"},
                Args.keys("rate", "tables", "verbose", "count"));
        assertEquals(2500.0, args.dbl("rate", 1));
        assertEquals(List.of("a", "b", "c"), args.list("tables", ""));
        assertTrue(args.flag("verbose"));
        assertEquals(7L, args.lng("count", 7));
        assertFalse(Args.parse(new String[0], Args.keys("verbose")).flag("verbose"));
    }

    @Test
    void rejectsUnknownOptionsAndBareWords() {
        assertThrows(IllegalArgumentException.class,
                () -> Args.parse(new String[] {"--qsp", "10"}, Args.keys("qps")));
        assertThrows(IllegalArgumentException.class,
                () -> Args.parse(new String[] {"qps"}, Args.keys("qps")));
    }
}
