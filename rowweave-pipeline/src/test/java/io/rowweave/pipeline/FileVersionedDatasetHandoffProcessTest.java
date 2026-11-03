package io.rowweave.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileVersionedDatasetHandoffProcessTest {
    @TempDir
    Path temporary;

    @Test
    void producerAndConsumerRecoverAcrossIndependentJvms() throws Exception {
        String producer = run("producer");
        assertTrue(producer.contains("WINNER="), producer);
        String consumer = run("consumer");
        assertTrue(consumer.contains("BINDING="), consumer);
        String recovered = run("consumer");
        assertEquals(bindingLine(consumer), bindingLine(recovered));
    }

    private String run(String mode) throws Exception {
        Path java = Paths.get(System.getProperty("java.home"), "bin", "java");
        Process process = new ProcessBuilder(java.toString(), "-Xms16m", "-Xmx64m",
                "-cp", System.getProperty("java.class.path"),
                VersionedDatasetHandoffProcessFixture.class.getName(),
                temporary.toString(), mode).redirectErrorStream(true).start();
        assertTrue(process.waitFor(20, TimeUnit.SECONDS), "child JVM timed out");
        String output = readAll(process.getInputStream());
        assertEquals(0, process.exitValue(), output);
        return output;
    }

    private static String bindingLine(String output) {
        for (String line : output.split("\\R")) {
            if (line.startsWith("BINDING=")) return line;
        }
        throw new AssertionError(output);
    }

    private static String readAll(InputStream input) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = input.read(buffer)) >= 0) bytes.write(buffer, 0, read);
        return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    }
}
