package io.rowweave.examples;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Published-JAR consumer test for the Local APPEND Stream example. */
class StreamLocalAppendExampleTest {
    @Test
    void publicExampleRunsWithoutInternalStreamImports(@TempDir Path root)
            throws Exception {
        assertEquals(Arrays.asList("B-20", "C-30"),
                StreamLocalAppendExample.run(root));
    }
}
