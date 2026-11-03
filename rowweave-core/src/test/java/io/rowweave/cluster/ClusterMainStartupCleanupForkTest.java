package io.rowweave.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterMainStartupCleanupForkTest {

    @Test
    @Timeout(10)
    void directMtlsWorkerRejectsBeforeReadingTlsMaterial() throws Exception {
        Process process = new ProcessBuilder(
                new File(System.getProperty("java.home"), "bin/java").getAbsolutePath(),
                "-Xms64m",
                "-Xmx256m",
                "-XX:MaxDirectMemorySize=64m",
                "-XX:MaxMetaspaceSize=128m",
                "-XX:ReservedCodeCacheSize=64m",
                "-Xss1m",
                "-XX:ActiveProcessorCount=2",
                "-Drowweave.process.memoryLimitBytes=2147483648",
                "-Drowweave.control.securityProfile=MTLS",
                "-Drowweave.control.keyStore=/definitely/missing/direct-worker.p12",
                "-cp", System.getProperty("java.class.path"),
                DirectWorkerProbe.class.getName())
                .redirectErrorStream(true)
                .start();
        boolean exited = process.waitFor(5, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
        }
        String output;
        try (InputStream input = process.getInputStream()) {
            output = new String(readAll(input), StandardCharsets.UTF_8);
        }
        assertTrue(exited, output);
        assertTrue(process.exitValue() == 0, output);
        assertTrue(output.contains("DIRECT_MTLS_WORKER_REJECTED_BEFORE_MATERIAL"), output);
        assertTrue(!output.contains("failed to load MTLS material"), output);
        assertTrue(!output.contains("missing required MTLS property"), output);
    }

    @Test
    @Timeout(30)
    void workerConstructionFailureClosesAlreadyStartedMasterAndProcessExits(
            @TempDir Path temporary) throws Exception {
        Path release = temporary.resolve("release-child");
        Process process = new ProcessBuilder(
                new File(System.getProperty("java.home"), "bin/java").getAbsolutePath(),
                "-Xms128m",
                "-Xmx512m",
                "-XX:MaxDirectMemorySize=256m",
                "-XX:MaxMetaspaceSize=192m",
                "-XX:ReservedCodeCacheSize=64m",
                "-Xss1m",
                "-XX:ActiveProcessorCount=4",
                "-Drowweave.process.memoryLimitBytes=4294967296",
                "-Drowweave.control.residentBytes=234881024",
                "-Drowweave.deployment.memoryBytes=8388608",
                "-Drowweave.master.enable=true",
                "-Drowweave.worker.enable=true",
                "-Drowweave.worker.memory=16777216",
                "-Drowweave.shuffle.mux=false",
                "-Drowweave.worker.slots=1",
                "-Drowweave.worker.closeDrainMs=-1",
                "-cp", System.getProperty("java.class.path"),
                StartupFailureProbe.class.getName(), release.toString())
                .redirectErrorStream(true)
                .start();
        ExecutorService readerPool = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rowweave-startup-cleanup-reader");
            thread.setDaemon(true);
            return thread;
        });
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                process.getInputStream(), StandardCharsets.UTF_8))) {
            Future<String> heldOutput = readerPool.submit(() -> readUntilHeld(reader));
            String output = heldOutput.get(10, TimeUnit.SECONDS);
            assertTrue(process.isAlive(),
                    "fixture must hold the JVM before OS process cleanup:\n" + output);
            assertTrue(output.contains("rowweave.worker.closeDrainMs must be >= 0"), output);

            Matcher port = Pattern.compile("MASTER_PORT=(\\d+)").matcher(output);
            assertTrue(port.find(),
                    "fixture did not start Master before Worker failed:\n" + output);
            int closedPort = Integer.parseInt(port.group(1));
            assertThrows(java.io.IOException.class, () -> {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress("127.0.0.1", closedPort), 250);
                }
            }, "Master port must be closed by StartupCleanup while child JVM is alive");

            Files.write(release, new byte[] {1});
            boolean exited = process.waitFor(5, TimeUnit.SECONDS);
            assertTrue(exited, "held startup-failure child did not exit:\n" + output);
            assertTrue(process.exitValue() != 0, output);
        } finally {
            try {
                Files.write(release, new byte[] {1});
            } catch (Exception ignored) {
                // best-effort release before forced termination
            }
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
            readerPool.shutdownNow();
            readerPool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static String readUntilHeld(BufferedReader reader) throws Exception {
        StringBuilder output = new StringBuilder();
        for (String line; (line = reader.readLine()) != null; ) {
            output.append(line).append('\n');
            if (line.startsWith("STARTUP_FAILURE_HELD=")) return output.toString();
        }
        throw new AssertionError("startup-failure child exited before hold marker:\n" + output);
    }

    private static byte[] readAll(InputStream input) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        for (int read; (read = input.read(buffer)) >= 0; ) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    public static final class DirectWorkerProbe {
        private DirectWorkerProbe() {
        }

        public static void main(String[] args) {
            try {
                new WorkerServer("direct-mtls", 1,
                        new FixedMasterLocator(new Endpoint("127.0.0.1", 1)),
                        new File(System.getProperty("java.io.tmpdir"),
                                "rowweave-direct-mtls-worker"));
                throw new AssertionError("direct MTLS Worker unexpectedly started");
            } catch (IllegalStateException expected) {
                if (expected.getMessage() != null
                        && expected.getMessage().contains(
                        "direct MTLS Worker construction is unsupported")) {
                    System.out.println("DIRECT_MTLS_WORKER_REJECTED_BEFORE_MATERIAL");
                    return;
                }
                expected.printStackTrace(System.out);
                System.exit(7);
            } catch (Throwable failure) {
                failure.printStackTrace(System.out);
                System.exit(8);
            }
        }
    }

    public static final class StartupFailureProbe {
        private StartupFailureProbe() {
        }

        public static void main(String[] args) throws Exception {
            Path release = java.nio.file.Paths.get(args[0]);
            Thread.setDefaultUncaughtExceptionHandler((thread, failure) -> {
                System.out.println("STARTUP_FAILURE_HELD="
                        + failure.getClass().getSimpleName() + ':' + failure.getMessage());
                System.out.flush();
                try {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (!Files.exists(release) && System.nanoTime() < deadline) {
                        Thread.sleep(5L);
                    }
                    System.exit(Files.exists(release) ? 17 : 18);
                } catch (Throwable holdFailure) {
                    holdFailure.printStackTrace(System.out);
                    System.exit(19);
                }
            });
            ClusterMain.main(new String[0]);
            throw new AssertionError("ClusterMain unexpectedly returned");
        }
    }
}

