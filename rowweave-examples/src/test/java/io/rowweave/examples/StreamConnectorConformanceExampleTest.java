package io.rowweave.examples;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Published connector SDK conformance example contract. */
class StreamConnectorConformanceExampleTest {

    @Test
    void publicFixturesVerifySourceAndBothSinkGuarantees() throws Exception {
        assertEquals(Arrays.asList(
                        "source=5", "eager-sink=3",
                        "transactional-sink=5"),
                StreamConnectorConformanceExample.run());
    }
}
