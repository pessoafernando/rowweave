package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcServerAdmissionTest {

    @Test
    void connectionCapClosesExcessPeerWithoutStartingHandlerThread() throws Exception {
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(1, 1, 5_000L, 128, 1024, 1200);
        try (RpcServer server = server(request -> RpcMessage.ack(), admission);
             Socket first = new Socket("127.0.0.1", server.port())) {
            await(() -> server.liveConnectionCount() == 1
                    && server.connectionThreadStartCount() == 1);
            int starts = server.connectionThreadStartCount();
            try (Socket excess = new Socket("127.0.0.1", server.port())) {
                excess.setSoTimeout(2_000);
                assertEquals(-1, excess.getInputStream().read());
            }
            assertEquals(1, server.liveConnectionCount());
            assertEquals(starts, server.connectionThreadStartCount());
        }
    }

    @Test
    void responseOnlyHeaderIsRejectedBeforeBodyOrHandler() throws Exception {
        AtomicInteger handled = new AtomicInteger();
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(4, 4, 5_000L, 128, 1024, 1200);
        try (RpcServer server = server(request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, admission);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(1000);
            out.writeInt(RpcMessage.Type.ACK.ordinal());
            out.flush();

            assertPeerClosed(socket);
            assertEquals(0, handled.get());
            assertEquals(0L, server.inFlightFrameBytes());
        }
    }

    @Test
    void perTypeLimitAndConcurrentFrameBudgetRejectBeforeBodyRead() throws Exception {
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(4, 4, 5_000L, 128, 1024, 1200);
        try (RpcServer server = server(request -> RpcMessage.ack(), admission)) {
            try (Socket normal = new Socket("127.0.0.1", server.port())) {
                normal.setSoTimeout(2_000);
                writeHeader(normal, 1000, RpcMessage.Type.HEARTBEAT);
                assertPeerClosed(normal);
            }

            try (Socket first = new Socket("127.0.0.1", server.port())) {
                first.setSoTimeout(2_000);
                writeHeader(first, 1000, RpcMessage.Type.SUBMIT_JOB);
                await(() -> server.inFlightFrameBytes() == 1000L);
                try (Socket second = new Socket("127.0.0.1", server.port())) {
                    second.setSoTimeout(2_000);
                    writeHeader(second, 1000, RpcMessage.Type.SUBMIT_JOB);
                    assertPeerClosed(second);
                }
                assertEquals(1000L, server.inFlightFrameBytes());
            }
            await(() -> server.inFlightFrameBytes() == 0L);
        }
    }

    @Test
    void dripFedHeaderCannotRefreshAbsoluteFrameDeadline() throws Exception {
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(2, 2, 1_000L, 128, 1024, 1200);
        long started = System.nanoTime();
        try (RpcServer server = server(request -> RpcMessage.ack(), admission);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3_000);
            byte[] header = {0, 0, 0, 4};
            for (byte value : header) {
                try {
                    socket.getOutputStream().write(value);
                    socket.getOutputStream().flush();
                } catch (java.io.IOException closed) {
                    break;
                }
                Thread.sleep(350L);
            }
            assertPeerClosed(socket);
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertTrue(elapsedMillis < 2_500L, "absolute deadline was refreshed by drip feed: " + elapsedMillis);
    }

    @Test
    void frameReservationLivesThroughHandlerAndReleasesAfterResponse() throws Exception {
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(2, 2, 5_000L, 128, 1024, 1200);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (RpcServer server = server(request -> {
            entered.countDown();
            try {
                if (!release.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("test release timeout");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
            return RpcMessage.ack();
        }, admission);
             RpcClient client = new RpcClient("127.0.0.1", server.port())) {
            Future<RpcMessage> response = caller.submit(
                    () -> client.call(RpcMessage.submitJob(new byte[] {1})));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertTrue(server.inFlightFrameBytes() > 0L, "handler must retain the wire reservation");
            release.countDown();
            assertEquals(RpcMessage.Type.ACK, response.get(2, TimeUnit.SECONDS).type());
            await(() -> server.inFlightFrameBytes() == 0L);
        } finally {
            release.countDown();
            caller.shutdownNow();
        }
    }

    private static RpcServer server(java.util.function.Function<RpcMessage, RpcMessage> handler,
                                    RpcAdmissionConfig admission) {
        return new RpcServer(handler, 0, ControlSecurityConfig.fromSystemProperties(),
                admission, RpcRequestPolicy.GENERAL);
    }

    private static void writeHeader(Socket socket, int length, RpcMessage.Type type) throws Exception {
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeInt(length);
        out.writeInt(type.ordinal());
        out.flush();
    }

    private static void assertPeerClosed(Socket socket) throws Exception {
        try {
            assertEquals(-1, socket.getInputStream().read());
        } catch (java.net.SocketException reset) {
            // macOS/Linux may surface server fail-fast close as RST instead of EOF.
        }
    }

    private static void await(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertTrue(condition.getAsBoolean(), "condition did not converge");
    }
}

