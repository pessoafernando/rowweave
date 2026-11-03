package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcConnectionRegistryTest {

    @Test
    void closeBeforeLateRegisterRejectsAndClosesSocket() {
        RpcConnectionRegistry registry = new RpcConnectionRegistry();
        registry.close();
        TrackingSocket late = new TrackingSocket("late", false);

        assertFalse(registry.register(late));
        assertTrue(late.closeAttempted.get());
        assertEquals(0, registry.size());
        assertTrue(registry.isClosed());
    }

    @Test
    void registerBeforeCloseAttemptsEverySocketAndPreservesFailures() {
        RpcConnectionRegistry registry = new RpcConnectionRegistry();
        TrackingSocket first = new TrackingSocket("first", true);
        TrackingSocket second = new TrackingSocket("second", true);
        TrackingSocket healthy = new TrackingSocket("healthy", false);
        assertTrue(registry.register(first));
        assertTrue(registry.register(second));
        assertTrue(registry.register(healthy));

        UncheckedIOException failure = assertThrows(UncheckedIOException.class, registry::close);

        assertEquals("first", failure.getCause().getMessage());
        assertEquals(1, failure.getCause().getSuppressed().length);
        assertEquals("second", failure.getCause().getSuppressed()[0].getMessage());
        assertTrue(first.closeAttempted.get());
        assertTrue(second.closeAttempted.get());
        assertTrue(healthy.closeAttempted.get(), "one close failure must not short-circuit the cleanup of the remaining connections");
        assertEquals(0, registry.size());
        assertTrue(registry.isClosed());
    }

    @Test
    void concurrentRegisterAndCloseNeverLeavesAcceptedSocketOpen() throws Exception {
        for (int round = 0; round < 200; round++) {
            RpcConnectionRegistry registry = new RpcConnectionRegistry();
            TrackingSocket socket = new TrackingSocket("race-" + round, false);
            CountDownLatch start = new CountDownLatch(1);
            Thread register = new Thread(() -> {
                await(start);
                registry.register(socket);
            });
            Thread close = new Thread(() -> {
                await(start);
                registry.close();
            });
            register.start();
            close.start();
            start.countDown();
            register.join();
            close.join();

            assertTrue(registry.isClosed());
            assertEquals(0, registry.size());
            assertTrue(socket.closeAttempted.get(), "round=" + round);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interrupted);
        }
    }

    private static final class TrackingSocket extends Socket {
        private final String failureMessage;
        private final boolean failClose;
        private final AtomicBoolean closeAttempted = new AtomicBoolean();

        private TrackingSocket(String failureMessage, boolean failClose) {
            this.failureMessage = failureMessage;
            this.failClose = failClose;
        }

        @Override
        public synchronized void close() throws IOException {
            closeAttempted.set(true);
            if (failClose) {
                throw new IOException(failureMessage);
            }
            super.close();
        }
    }
}
