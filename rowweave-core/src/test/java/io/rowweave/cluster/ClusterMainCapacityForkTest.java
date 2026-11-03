package io.rowweave.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** aggregate rejection precedes TLS material and all role resources. */
class ClusterMainCapacityForkTest {

    @Test
    @Timeout(20)
    void aggregateHeapFailureHappensBeforeMissingMtlsMaterialIsRead() throws Exception {
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/java").getAbsolutePath());
        command.add("-Xms512m");
        command.add("-Xmx4g");
        command.add("-XX:MaxDirectMemorySize=768m");
        command.add("-XX:MaxMetaspaceSize=384m");
        command.add("-XX:ReservedCodeCacheSize=128m");
        command.add("-Xss1m");
        command.add("-XX:ActiveProcessorCount=4");
        command.add("-Drowweave.process.memoryLimitBytes=8589934592");
        command.add("-Drowweave.master.enable=true");
        command.add("-Drowweave.worker.enable=true");
        command.add("-Drowweave.worker.memory=2187748967");
        command.add("-Drowweave.control.securityProfile=MTLS");
        command.add("-Drowweave.control.host=127.0.0.1");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ClusterMain.class.getName());

        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "capacity child timed out");
        String output;
        try (InputStream input = process.getInputStream()) {
            output = new String(readAll(input), "UTF-8");
        }
        assertTrue(process.exitValue() != 0, output);
        assertTrue(output.contains("ROWWEAVE-CAP-003"), output);
        assertTrue(!output.contains("missing required MTLS property"), output);
        assertTrue(!output.contains("failed to load MTLS material"), output);
        assertTrue(!output.contains("MASTER_PORT="), output);
        assertTrue(!output.contains("WORKER_READY="), output);
    }

    private static byte[] readAll(InputStream input) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        for (int read; (read = input.read(buffer)) >= 0; ) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }
}
