package io.rowweave.internal;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** blocking provider code permanently poisons one JVM-wide lane set. */
class ProviderActionSupervisorTest {

    @ParameterizedTest
    @ValueSource(strings = {"open", "write", "prepare", "control"})
    void blockingActionPoisonsTheProcessWideFixedSupervisor(String action)
            throws Exception {
        Path java = Paths.get(System.getProperty("java.home"), "bin", "java");
        Process process = new ProcessBuilder(
                java.toString(), "-cp", System.getProperty("java.class.path"),
                ProviderActionSupervisorProbe.class.getName(), action)
                .redirectErrorStream(true)
                .start();
        assertTrue(process.waitFor(15, TimeUnit.SECONDS),
                "forked supervisor probe did not terminate");
        String output = new String(process.getInputStream().readAllBytes(),
                StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), output);
        assertTrue(output.contains("POISONED capacity=2 threads="), output);
    }
}
