package io.rowweave.soak;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamSoakTerminalDrainCadenceTest {
    @Test void slowOracleWaitContinuesPublicSampling(@TempDir Path root)
            throws Exception {
        Path expected = root.resolve("expected.log");
        Files.write(expected, Collections.singletonList("A|1|10.00"),
                StandardCharsets.UTF_8);
        AtomicInteger samples = new AtomicInteger();

        assertThrows(IllegalStateException.class,
                () -> StreamSoakMain.awaitOracle(expected,
                        root.resolve("not-yet-visible"), 650L, 100L,
                        samples::incrementAndGet));
        assertTrue(samples.get() >= 3,
                "the terminal oracle wait must not freeze signed sampling");
    }
}
