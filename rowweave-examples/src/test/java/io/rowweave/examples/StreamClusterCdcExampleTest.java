package io.rowweave.examples;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Public-JAR M+2W resident CDC example contract. */
class StreamClusterCdcExampleTest {

    @Test
    void independentWorkersSavepointAndRestoreExactLogPosition(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source-worker", exchange);
        Process sink = launch("sink-worker", exchange);
        try {
            awaitFile(exchange.resolve("source-worker.port"), 5_000L);
            awaitFile(exchange.resolve("sink-worker.port"), 5_000L);
            assertEquals(Arrays.asList(
                    "SNAPSHOT", "A=12.50", "SAVEPOINT",
                    "LOG", "A=15.00"),
                    StreamClusterCdcExample.runMaster(exchange));
            assertEquals("SNAPSHOT:10\nLOG:11\n", text(
                    exchange.resolve("source-open-phases")));
        } finally {
            Files.write(exchange.resolve("shutdown"), new byte[] {1});
            boolean sourceExited = source.waitFor(5L, TimeUnit.SECONDS);
            boolean sinkExited = sink.waitFor(5L, TimeUnit.SECONDS);
            source.destroyForcibly();
            sink.destroyForcibly();
            assertTrue(sourceExited, () -> log(exchange, "source-worker"));
            assertTrue(sinkExited, () -> log(exchange, "sink-worker"));
        }
        assertEquals(0, source.exitValue(),
                () -> log(exchange, "source-worker"));
        assertEquals(0, sink.exitValue(),
                () -> log(exchange, "sink-worker"));
    }

    private static Process launch(String role, Path exchange)
            throws Exception {
        String java = Paths.get(System.getProperty("java.home"), "bin", "java")
                .toString();
        String classpath = System.getProperty("surefire.test.class.path",
                System.getProperty("java.class.path"));
        return new ProcessBuilder(java, "-Xms32m", "-Xmx128m",
                "-XX:MaxDirectMemorySize=32m",
                "-XX:MaxMetaspaceSize=128m",
                "-XX:ReservedCodeCacheSize=32m", "-Xss512k",
                "-XX:ActiveProcessorCount=2", "-cp", classpath,
                StreamClusterCdcExample.class.getName(), role,
                exchange.toString())
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(role + ".log").toFile())
                .start();
    }

    private static void awaitFile(Path file, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while ((!Files.exists(file) || Files.size(file) == 0L)
                && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(Files.exists(file) && Files.size(file) > 0L,
                "process did not publish " + file);
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static String log(Path exchange, String role) {
        try {
            return text(exchange.resolve(role + ".log"));
        } catch (Exception ignored) {
            return "missing process log";
        }
    }
}
