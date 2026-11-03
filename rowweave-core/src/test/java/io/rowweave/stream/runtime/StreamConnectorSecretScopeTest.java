package io.rowweave.stream.runtime;

import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.stream.api.StreamConnectorConfiguration;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamConnectorSecretScopeTest {
    @Test
    void oneOpenScopeExposesAliasOnlyUntilCloseAndZerosTransferredBytes()
            throws Exception {
        byte[] issued = "temporary".getBytes(StandardCharsets.UTF_8);
        StreamConnectorSecretScope scope = StreamConnectorSecretScope.resolve(
                configuration(Collections.singletonMap(
                        "password", "vault://db/password")),
                reference -> {
                    assertEquals("vault://db/password", reference);
                    return issued;
                }, ProviderActionSupervisor.processWide(2), deadline());
        assertEquals("temporary", scope.withSecret("password",
                bytes -> new String(bytes, StandardCharsets.UTF_8)));
        assertThrows(IllegalStateException.class,
                () -> scope.withSecret("missing", bytes -> null));
        scope.close();
        assertArrayEquals(new byte[issued.length], issued);
        assertThrows(IllegalStateException.class,
                () -> scope.withSecret("password", bytes -> null));
    }

    @Test
    void resolutionFailureAndOversizeZeroAllAcquiredValues() {
        Map<String, String> references = new LinkedHashMap<>();
        references.put("a", "vault://a");
        references.put("b", "vault://b");
        byte[] first = new byte[] {9, 8, 7};
        assertEquals("STREAM_CONNECTOR_SECRET_RESOLUTION_FAILED",
                assertThrows(IllegalStateException.class,
                        () -> StreamConnectorSecretScope.resolve(
                                configuration(references),
                                reference -> {
                                    if (reference.endsWith("a")) return first;
                                    throw new IllegalStateException(
                                            "sensitive provider detail");
                                }, ProviderActionSupervisor.processWide(2),
                                deadline())).getMessage());
        assertArrayEquals(new byte[first.length], first);

        byte[] oversized = new byte[
                StreamConnectorSecretScope.MAX_SECRET_BYTES + 1];
        Arrays.fill(oversized, (byte) 7);
        assertEquals("STREAM_CONNECTOR_SECRET_INVALID",
                assertThrows(IllegalStateException.class,
                        () -> StreamConnectorSecretScope.resolve(
                                configuration(Collections.singletonMap(
                                        "password", "vault://password")),
                                reference -> oversized,
                                ProviderActionSupervisor.processWide(2),
                                deadline())).getMessage());
        assertArrayEquals(new byte[oversized.length], oversized);
    }

    @Test
    void closeDoesNotWaitForUncooperativeProviderCallback()
            throws Exception {
        byte[] issued = new byte[] {1, 2, 3};
        StreamConnectorSecretScope scope = StreamConnectorSecretScope.resolve(
                configuration(Collections.singletonMap(
                        "password", "vault://password")),
                reference -> issued,
                ProviderActionSupervisor.processWide(2), deadline());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            Future<?> callback = threads.submit(() -> {
                try {
                    scope.withSecret("password", secret -> {
                        entered.countDown();
                        release.await();
                        return null;
                    });
                } catch (Exception failure) {
                    throw new IllegalStateException(failure);
                }
            });
            if (!entered.await(2L, TimeUnit.SECONDS)) {
                throw new AssertionError("secret callback did not start");
            }
            Future<?> close = threads.submit(scope::close);
            close.get(500L, TimeUnit.MILLISECONDS);
            assertArrayEquals(new byte[issued.length], issued);
            release.countDown();
            callback.get(2L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            threads.shutdownNow();
        }
    }

    private static byte[] configuration(Map<String, String> references) {
        return StreamConnectorConfiguration.of(
                Collections.emptyMap(), references).encode();
    }

    private static long deadline() {
        return System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
    }
}
