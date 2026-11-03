package io.rowweave.examples;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Public consumer gate for Stream elastic primitives. */
class StreamElasticExampleTest {
    @Test
    void publicExampleRunsDynamicUnalignedRescaleAndCompilesRetract(
            @TempDir Path root) throws Exception {
        assertEquals(Arrays.asList("dynamic-split", "unaligned-checkpoint",
                        "live-rescale", "retract"),
                StreamElasticExample.run(root));
    }
}
