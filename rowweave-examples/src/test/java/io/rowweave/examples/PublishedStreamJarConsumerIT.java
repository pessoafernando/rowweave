package io.rowweave.examples;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Black-box gate: examples compile and run from packaged artifacts only. */
class PublishedStreamJarConsumerIT {

    @Test
    void packagedCoreCompilesAndRunsFivePublicExamples() throws Exception {
        assertEquals(Arrays.asList(
                        "append=[B-20, C-30]",
                        "atomic=[calc, flat-map, union, keyed-process, tagged-output, async-wait]",
                        "elastic=[dynamic-split, unaligned-checkpoint, live-rescale, retract]",
                        "event-time=[category-a:0:1000:3.0000000001]",
                        "conformance=[source=5, eager-sink=3, transactional-sink=5]",
                        "cluster=[SNAPSHOT, A=12.50, SAVEPOINT, LOG, A=15.00]"),
                PublishedStreamJarConsumerVerifier.verify(
                        Paths.get(required("rowweave.published.core.jar")),
                        Paths.get(required("rowweave.stream.example.sources"))));
    }

    private static String required(String property) {
        String value = System.getProperty(property);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException(property + " is required");
        }
        return value;
    }
}
