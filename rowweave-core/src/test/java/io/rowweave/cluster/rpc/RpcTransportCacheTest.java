package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RpcTransportCacheTest {

    static final class FakeTransport implements RpcTransport {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger sends = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final int throwFirstNCalls;
        FakeTransport(int throwFirstNCalls) { this.throwFirstNCalls = throwFirstNCalls; }
        FakeTransport() { this(0); }
        @Override public RpcMessage call(RpcMessage request) {
            if (calls.incrementAndGet() <= throwFirstNCalls) {
                throw new RuntimeException("boom");
            }
            return RpcMessage.ack();
        }
        @Override public void send(RpcMessage request) {
            if (sends.incrementAndGet() <= throwFirstNCalls) {
                throw new RuntimeException("boom");
            }
        }
        @Override public void close() { closes.incrementAndGet(); }
    }

    static final class RecordingConnector implements RpcTransportCache.Connector {
        final AtomicInteger connects = new AtomicInteger();
        final List<FakeTransport> issued = new CopyOnWriteArrayList<>();
        private final java.util.function.Supplier<FakeTransport> factory;
        RecordingConnector(java.util.function.Supplier<FakeTransport> factory) { this.factory = factory; }
        RecordingConnector() { this(FakeTransport::new); }
        @Override public RpcTransport connect(String host, int port) {
            connects.incrementAndGet();
            FakeTransport t = factory.get();
            issued.add(t);
            return t;
        }
    }

    @Test
    void sameEndpointReusesOneTransport() {
        RecordingConnector conn = new RecordingConnector();
        try (RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> true)) {
            cache.call("h", 1, RpcMessage.ack());
            cache.call("h", 1, RpcMessage.ack());
            cache.call("h", 1, RpcMessage.ack());
        }
        assertEquals(1, conn.connects.get(), "one endpoint is connected only once");
        assertEquals(3, conn.issued.get(0).calls.get(), "three calls share one transport");
    }

    @Test
    void differentEndpointsSeparateTransports() {
        RecordingConnector conn = new RecordingConnector();
        try (RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> true)) {
            cache.call("h", 1, RpcMessage.ack());
            cache.call("h", 2, RpcMessage.ack());
            cache.call("g", 1, RpcMessage.ack());
        }
        assertEquals(3, conn.connects.get(), "each distinct host:port gets its own connection");
    }

    @Test
    void concurrentGetBuildsConnectionOnce() throws Exception {
        RecordingConnector conn = new RecordingConnector();
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try (RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> true)) {
            for (int i = 0; i < threads; i++) {
                Thread t = new Thread(() -> {
                    try { start.await(); cache.call("h", 1, RpcMessage.ack()); }
                    catch (InterruptedException ignored) { }
                    finally { done.countDown(); }
                });
                t.setDaemon(true);
                t.start();
            }
            start.countDown();
            done.await();
        }
        assertEquals(1, conn.connects.get(), "concurrent racing builds of one endpoint connect only once (an atomic putIfAbsent)");
    }

    @Test
    void callExceptionEvictsAndReconnects() {
        AtomicInteger seq = new AtomicInteger();
        RecordingConnector conn = new RecordingConnector(
                () -> new FakeTransport(seq.getAndIncrement() == 0 ? 1 : 0));
        try (RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> true)) {
            RpcMessage resp = cache.call("h", 1, RpcMessage.ack());
            assertEquals(RpcMessage.Type.ACK, resp.type(), "a retry after reconnecting returns ACK");
        }
        assertEquals(2, conn.connects.get(), "after a broken connection is evicted a new one is built (2 connects)");
        assertEquals(1, conn.issued.get(0).closes.get(), "the broken connection was closed");
    }

    @Test
    void guardedCallDoesNotReconnectAfterEndpointGenerationIsWithdrawn() {
        RecordingConnector conn = new RecordingConnector(() -> new FakeTransport(1));
        try (RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> true)) {
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> cache.call("h", 1, RpcMessage.ack(), () -> false));
            assertEquals("boom", failure.getMessage());
        }
        assertEquals(1, conn.connects.get(),
                "once a Worker departure revokes the endpoint generation, the old address must not be reconnected at the cost of another read timeout");
        assertEquals(1, conn.issued.get(0).closes.get());
    }

    @Test
    void callExceptionTwiceGivesUp() {
        RecordingConnector conn = new RecordingConnector(() -> new FakeTransport(1));
        try (RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> true)) {
            assertThrows(RuntimeException.class, () -> cache.call("h", 1, RpcMessage.ack()));
        }
        assertEquals(2, conn.connects.get(), "one retry: give up after 2 connects");
    }

    @Test
    void sendExceptionEvictsAndReconnects() {
        AtomicInteger seq = new AtomicInteger();
        RecordingConnector conn = new RecordingConnector(
                () -> new FakeTransport(seq.getAndIncrement() == 0 ? 1 : 0));
        try (RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> true)) {
            cache.send("h", 1, RpcMessage.ack());
        }
        assertEquals(2, conn.connects.get(), "a send on a broken connection also evicts and reconnects");
        assertEquals(1, conn.issued.get(0).closes.get());
    }

    @Test
    void inProcessBypassNotCached() {
        RecordingConnector conn = new RecordingConnector();
        try (RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> false)) {
            cache.call("h", 1, RpcMessage.ack());
            cache.call("h", 1, RpcMessage.ack());
        }
        assertEquals(2, conn.connects.get(), "bypass: every call builds a new connection and reuses nothing");
        assertEquals(1, conn.issued.get(0).closes.get(), "a bypass transport is closed right after use");
        assertEquals(1, conn.issued.get(1).closes.get());
    }

    @Test
    void invalidateClosesAndRemoves() {
        RecordingConnector conn = new RecordingConnector();
        try (RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> true)) {
            cache.call("h", 1, RpcMessage.ack());
            cache.invalidate("h", 1);
            assertEquals(1, conn.issued.get(0).closes.get(), "invalidate closes the connection");
            cache.call("h", 1, RpcMessage.ack());
            assertEquals(2, conn.connects.get(), "a connection is rebuilt after invalidate");
        }
    }

    @Test
    void closeClosesAllCachedTransportsButAllowsReconnect() {
        RecordingConnector conn = new RecordingConnector();
        RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> true);
        cache.call("h", 1, RpcMessage.ack());
        cache.call("h", 2, RpcMessage.ack());
        cache.close();
        assertEquals(1, conn.issued.get(0).closes.get());
        assertEquals(1, conn.issued.get(1).closes.get());

        cache.call("h", 1, RpcMessage.ack());
        assertEquals(3, conn.connects.get(), "close only rotates the connection, so later calls must be able to reconnect");
        cache.close();
    }

    @Test
    void shutdownClosesAllCachedTransportsAndPermanentlyRejectsCalls() {
        RecordingConnector conn = new RecordingConnector();
        RpcTransportCache cache = new RpcTransportCache(conn, (h, p) -> true);
        cache.call("h", 1, RpcMessage.ack());
        cache.call("h", 2, RpcMessage.ack());

        cache.shutdown();

        assertEquals(1, conn.issued.get(0).closes.get());
        assertEquals(1, conn.issued.get(1).closes.get());
        assertThrows(IllegalStateException.class,
                () -> cache.call("h", 1, RpcMessage.ack()));
        assertThrows(IllegalStateException.class,
                () -> cache.send("h", 1, RpcMessage.ack()));
        assertEquals(2, conn.connects.get(), "no new connection may be established after shutdown");
    }

    @Test
    void shutdownLinearizesAfterInflightCallWithoutLeavingLateConnection() throws Exception {
        CountDownLatch callEntered = new CountDownLatch(1);
        CountDownLatch releaseCall = new CountDownLatch(1);
        AtomicInteger connects = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        AtomicReference<Throwable> callFailure = new AtomicReference<>();
        RpcTransportCache cache = new RpcTransportCache((host, port) -> {
            connects.incrementAndGet();
            return new RpcTransport() {
                @Override public RpcMessage call(RpcMessage request) {
                    callEntered.countDown();
                    try {
                        releaseCall.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    return RpcMessage.ack();
                }
                @Override public void send(RpcMessage request) { }
                @Override public void close() { closes.incrementAndGet(); }
            };
        }, (host, port) -> true);
        Thread caller = new Thread(() -> {
            try {
                cache.call("h", 1, RpcMessage.ack());
            } catch (Throwable failure) {
                callFailure.set(failure);
            }
        }, "rpc-cache-inflight-call");
        Thread shutdown = new Thread(cache::shutdown, "rpc-cache-shutdown");
        try {
            caller.start();
            assertTrue(callEntered.await(5, TimeUnit.SECONDS));

            shutdown.start();
            while (closes.get() == 0 && shutdown.isAlive()) {
                Thread.yield();
            }
            assertTrue(shutdown.isAlive(), "shutdown must wait for an in-flight call holding the read lock to exit");

            releaseCall.countDown();
            caller.join(5_000L);
            shutdown.join(5_000L);
            assertFalse(caller.isAlive());
            assertFalse(shutdown.isAlive());
            assertNull(callFailure.get());
            assertEquals(1, connects.get());
            assertEquals(1, closes.get());
            assertThrows(IllegalStateException.class,
                    () -> cache.call("h", 1, RpcMessage.ack()));
            assertEquals(1, connects.get(), "once the in-flight call exits, a late connection must neither remain nor be rebuilt");
        } finally {
            releaseCall.countDown();
            caller.join(5_000L);
            shutdown.join(5_000L);
            cache.shutdown();
        }
    }
}

