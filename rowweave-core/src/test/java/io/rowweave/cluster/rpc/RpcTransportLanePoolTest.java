package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RpcTransportLanePoolTest {

    @Test
    void registeredInProcessEndpointNeverFallsBackToSocket() {
        String host = "in-process-only";
        int port = 49201;
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(
                host, port, request -> RpcMessage.ack());
        try (RpcTransportLanePool lanes = new RpcTransportLanePool(4)) {
            assertEquals(RpcMessage.Type.ACK,
                    lanes.call(host, port, RpcMessage.cancelStage("w", 1L)).type());
        } finally {
            endpointRegistration.close();
        }
    }

    @Test
    void sameEndpointUsesFourConcurrentReusableLanes() throws Exception {
        AtomicInteger connections = new AtomicInteger();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);
        RpcTransportCache.Connector connector = (host, port) -> {
            connections.incrementAndGet();
            return new RpcTransport() {
                @Override public RpcMessage call(RpcMessage request) {
                    int now = active.incrementAndGet();
                    peak.accumulateAndGet(now, Math::max);
                    entered.countDown();
                    try {
                        assertTrue(release.await(2, TimeUnit.SECONDS));
                        return RpcMessage.ack();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    } finally {
                        active.decrementAndGet();
                    }
                }
                @Override public void send(RpcMessage request) { call(request); }
                @Override public void close() { }
            };
        };

        try (RpcTransportLanePool lanes = new RpcTransportLanePool(4, connector, (h, p) -> true)) {
            ExecutorService callers = Executors.newFixedThreadPool(4);
            try {
                List<Future<RpcMessage>> futures = new ArrayList<>();
                for (int i = 0; i < 4; i++) {
                    long stageId = i;
                    futures.add(callers.submit(() -> lanes.call("worker", 9000,
                            RpcMessage.cancelStage("w", stageId))));
                }
                assertTrue(entered.await(1, TimeUnit.SECONDS), "4 lanes should enter a call on the same endpoint at the same time");
                assertEquals(4, peak.get(), "a single synchronized RpcClient must not re-serialize the lane pool");
                release.countDown();
                for (Future<RpcMessage> future : futures) {
                    assertEquals(RpcMessage.Type.ACK, future.get(1, TimeUnit.SECONDS).type());
                }
                assertEquals(4, connections.get(), "each lane holds at most one long connection to that endpoint");
            } finally {
                callers.shutdownNow();
            }
        }
    }

    @Test
    void closePreventsConnectionRecreation() {
        AtomicInteger connections = new AtomicInteger();
        RpcTransportLanePool lanes = new RpcTransportLanePool(2,
                (host, port) -> {
                    connections.incrementAndGet();
                    throw new AssertionError("closed lane must not connect");
                }, (h, p) -> true);
        lanes.close();
        assertThrows(IllegalStateException.class,
                () -> lanes.call("worker", 9000, RpcMessage.ack()));
        assertEquals(0, connections.get());
    }
}

