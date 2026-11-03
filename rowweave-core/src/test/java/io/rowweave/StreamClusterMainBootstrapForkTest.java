package io.rowweave;

import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Stream process admission shares Batch's earliest proof. */
class StreamClusterMainBootstrapForkTest {

    @Test
    @Timeout(20)
    void finiteWorkerResourcesMustFitCertifiedProcessRss(@TempDir Path root)
            throws Exception {
        List<String> command = new ArrayList<>(Arrays.asList(
                new File(System.getProperty("java.home"), "bin/java")
                        .getAbsolutePath(),
                "-Xms64m", "-Xmx64m",
                "-XX:MaxDirectMemorySize=64m",
                "-XX:MaxMetaspaceSize=64m",
                "-XX:ReservedCodeCacheSize=32m",
                "-Xss512k", "-XX:ActiveProcessorCount=1",
                "-Drowweave.process.memoryLimitBytes=134217728",
                "-Drowweave.stream.worker.enable=true",
                "-Drowweave.stream.worker.id=capacity-worker",
                "-Drowweave.stream.state.dir=" + root,
                "-cp", System.getProperty("java.class.path"),
                StreamClusterMain.class.getName()));
        Path outputFile = root.resolve("stream-capacity.log");
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).redirectOutput(outputFile.toFile())
                .start();
        boolean exited = process.waitFor(10, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
        }
        String text = new String(Files.readAllBytes(outputFile), "UTF-8");
        assertTrue(exited, "Stream process escaped its RSS gate: " + text);
        assertTrue(process.exitValue() != 0, text);
        assertTrue(text.contains(
                "declared process memory limit is smaller than required RSS"),
                text);
        assertTrue(!text.contains("STREAM_WORKER_READY="), text);
    }

    @Test
    @Timeout(20)
    void capacityProofRunsBeforeStreamRoleConfiguration() throws Exception {
        Process process = new ProcessBuilder(Arrays.asList(
                new File(System.getProperty("java.home"), "bin/java")
                        .getAbsolutePath(),
                "-cp", System.getProperty("java.class.path"),
                StreamClusterMain.class.getName()))
                .redirectErrorStream(true)
                .start();
        boolean exited = process.waitFor(10, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
        }
        assertTrue(exited, "StreamClusterMain did not fail closed");
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (InputStream input = process.getInputStream()) {
            byte[] buffer = new byte[1_024];
            for (int read; (read = input.read(buffer)) >= 0; ) {
                output.write(buffer, 0, read);
            }
        }
        String text = output.toString("UTF-8");
        assertTrue(process.exitValue() != 0, text);
        assertTrue(text.contains("-Xms must be explicit"), text);
        assertTrue(!text.contains(
                "at least one Stream cluster role must be enabled"), text);
    }

    @Test
    @Timeout(20)
    void certifiedExternalRegistryProcessRejectsPlaintextBeforeRegistryAccess(
            @TempDir Path root) throws Exception {
        List<String> command = ForkedJvm.baseCommand();
        command.add("-Drowweave.stream.master.enable=true");
        command.add("-Drowweave.stream.state.dir=" + root.resolve("state"));
        command.add("-Drowweave.stream.master.artifact.dir="
                + root.resolve("artifacts"));
        command.add("-Drowweave.registry=zk");
        command.add("-Drowweave.zk.connectString=127.0.0.1:1");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(StreamClusterMain.class.getName());
        Path outputFile = root.resolve("stream-plaintext.log");
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).redirectOutput(outputFile.toFile())
                .start();
        boolean exited = process.waitFor(10, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
        }
        String text = new String(Files.readAllBytes(outputFile), "UTF-8");
        assertTrue(exited, "plaintext external Stream process kept running: "
                + text);
        assertTrue(process.exitValue() != 0, text);
        assertTrue(text.contains(
                "external Stream process roles require MTLS"), text);
        assertTrue(!text.contains("ServiceRegistry"),
                "registry access happened before the security gate: " + text);
    }
}
