package io.rowweave.examples;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Public-JAR Local event-time checkpoint/recovery example contract. */
class StreamLocalEventTimeRecoveryExampleTest {

    @Test
    void pendingWindowRestoresBeforeItsFinalWatermark(@TempDir Path root)
            throws Exception {
        assertEquals(Collections.singletonList(
                        "category-a:0:1000:3.0000000001"),
                StreamLocalEventTimeRecoveryExample.run(root));
    }
}
