package io.rowweave.stream.cluster;

import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** independent Master plus two Worker JVMs use the Stream data wire. */
@Tag("multiprocess")
class StreamRealProcessClusterTest {

    @Test
    void independentSourceAndSinkWorkerJvmsExchangeOnlyOverTcp(
            @TempDir Path exchange) throws Exception {
        Process sink = launch("sink", exchange);
        Process source = null;
        try {
            awaitFile(exchange.resolve("port"), 5_000L);
            source = launch("source", exchange);
            awaitFile(exchange.resolve("sink.identity"), 5_000L);
            awaitFile(exchange.resolve("source.identity"), 5_000L);
            String sinkIdentity = text(exchange.resolve("sink.identity"));
            String sourceIdentity = text(exchange.resolve("source.identity"));
            assertNotEquals(ManagementFactory.getRuntimeMXBean().getName(),
                    sinkIdentity);
            assertNotEquals(ManagementFactory.getRuntimeMXBean().getName(),
                    sourceIdentity);
            assertNotEquals(sinkIdentity, sourceIdentity);

            awaitFile(exchange.resolve("result"), 10_000L);
            assertEquals(expectedResult(), new String(
                    Files.readAllBytes(exchange.resolve("result")),
                    StandardCharsets.US_ASCII));
            Files.write(exchange.resolve("cancel"), new byte[] {1});
            assertTrue(source.waitFor(5L, TimeUnit.SECONDS));
            assertTrue(sink.waitFor(5L, TimeUnit.SECONDS));
            assertEquals(0, source.exitValue());
            assertEquals(0, sink.exitValue());
        } finally {
            if (source != null) source.destroyForcibly();
            sink.destroyForcibly();
        }
    }

    private static Process launch(String role, Path exchange) throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(role + ".log").toFile())
                .start();
    }

    private static void awaitFile(Path file, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!Files.exists(file) && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(Files.exists(file), "worker did not publish " + file);
    }

    private static String expectedResult() throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (int index = 0; index < 1_000; index++) {
            digest.update((byte) index);
        }
        return "1000:" + hex(digest.digest());
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int current = value[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }
}
