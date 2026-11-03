package io.rowweave.cluster;

import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JvmCapacityEnvelopeForkTest {

    @Test
    void capturesTheOptionsSeenByAnActualChildJvm() throws Exception {
        Process process = new ProcessBuilder(
                ForkedJvm.command(JvmCapacityProbe.class.getName()))
                .redirectErrorStream(true)
                .start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append('\n');
            }
        }
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), "capacity probe timed out");
        assertEquals(0, process.exitValue(), output.toString());
        assertTrue(output.toString().contains("CAPACITY_OK"), output.toString());
        assertTrue(output.toString().contains("codeCache=134217728"), output.toString());
    }
}
