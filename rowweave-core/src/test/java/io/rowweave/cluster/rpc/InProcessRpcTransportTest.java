package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class InProcessRpcTransportTest {

    @Test
    void customFactoryTimeoutBoundsInProcessCallWithoutCancellingHandler() throws Exception {
        String host = "inproc-timeout-" + java.util.UUID.randomUUID();
        int port = 1;
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        AtomicBoolean handlerInterrupted = new AtomicBoolean();
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
            handlerEntered.countDown();
            try {
                releaseHandler.await();
            } catch (InterruptedException interrupted) {
                handlerInterrupted.set(true);
                Thread.currentThread().interrupt();
            }
            return RpcMessage.ack();
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (RpcTransport transport = RpcTransports.connect(host, port, 100, 50)) {
            Future<Throwable> outcome = caller.submit(() -> {
                try {
                    transport.call(RpcMessage.heartbeat("worker"));
                    return null;
                } catch (Throwable failure) {
                    return failure;
                }
            });
            assertTrue(handlerEntered.await(1, TimeUnit.SECONDS));

            Throwable failure = null;
            boolean returnedWithinBound = true;
            try {
                failure = outcome.get(500, TimeUnit.MILLISECONDS);
            } catch (TimeoutException oldImplementationHung) {
                returnedWithinBound = false;
            } finally {
                releaseHandler.countDown();
            }
            outcome.get(1, TimeUnit.SECONDS);

            assertTrue(returnedWithinBound,
                    "an in-process call honours the read timeout from the factory instead of waiting on Future.get forever");
            assertNotNull(failure);
            assertTrue(failure.getMessage().contains("timed out after 50ms"));
            assertFalse(handlerInterrupted.get(),
                    "a waiter timeout does not implicitly cancel the remote handler just because the local thread was interrupted");
        } finally {
            releaseHandler.countDown();
            caller.shutdownNow();
            endpointRegistration.close();
        }
    }

    @Test
    void zeroTimeoutKeepsExplicitInfiniteWaitConvention() throws Exception {
        String host = "inproc-no-timeout-" + java.util.UUID.randomUUID();
        int port = 1;
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
            handlerEntered.countDown();
            try {
                releaseHandler.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return RpcMessage.ack();
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (RpcTransport transport = RpcTransports.connect(host, port, 100, 0)) {
            Future<RpcMessage> outcome = caller.submit(() ->
                    transport.call(RpcMessage.heartbeat("worker")));
            assertTrue(handlerEntered.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> outcome.get(100, TimeUnit.MILLISECONDS),
                    "readTimeoutMs=0 keeps the explicit wait-forever convention");
            releaseHandler.countDown();
            assertEquals(RpcMessage.Type.ACK, outcome.get(1, TimeUnit.SECONDS).type());
        } finally {
            releaseHandler.countDown();
            caller.shutdownNow();
            endpointRegistration.close();
        }
    }

    @Test
    void negativeTimeoutFailsBeforeDispatchAndHandlerFailureKeepsIdentity() {
        String host = "inproc-validation-" + java.util.UUID.randomUUID();
        int port = 1;
        AtomicInteger calls = new AtomicInteger();
        RuntimeException root = new IllegalStateException("handler-root");
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
            calls.incrementAndGet();
            throw root;
        });
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> RpcTransports.connect(host, port, 100, -1),
                    "a negative read timeout must fail before dispatch");
            assertEquals(0, calls.get());
            try (RpcTransport transport = RpcTransports.connect(host, port, 100, 100)) {
                assertSame(root, assertThrows(IllegalStateException.class,
                        () -> transport.call(RpcMessage.heartbeat("worker"))),
                        "the identity of a handler RuntimeException must survive the timeout wrapper");
            }
            assertEquals(1, calls.get());
        } finally {
            endpointRegistration.close();
        }
    }

    @Test
    void callDispatchesToHandlerAndReturns() {
        Function<RpcMessage, RpcMessage> handler = req -> RpcMessage.ack();
        try (InProcessRpcTransport t = new InProcessRpcTransport(handler)) {
            RpcMessage resp = t.call(RpcMessage.heartbeat("w0"));
            assertEquals(RpcMessage.Type.ACK, resp.type(), "call returns the handler response");
        }
    }

    @Test
    void blockingCallDoesNotBlockConcurrentCall() throws Exception {
        CountDownLatch blockerEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        AtomicInteger fastHandled = new AtomicInteger();
        Function<RpcMessage, RpcMessage> handler = req -> {
            if (req.type() == RpcMessage.Type.SUBMIT_JOB) {
                blockerEntered.countDown();
                try { releaseBlocker.await(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
                return RpcMessage.ack();
            }
            fastHandled.incrementAndGet();
            return RpcMessage.ack();
        };
        try (InProcessRpcTransport t = new InProcessRpcTransport(handler)) {
            Thread a = new Thread(() -> t.call(RpcMessage.submitJob(new byte[]{1})));
            a.start();
            assertTrue(blockerEntered.await(2, TimeUnit.SECONDS), "a blocking call should enter the handler");
            RpcMessage resp = t.call(RpcMessage.heartbeat("w0"));
            assertEquals(RpcMessage.Type.ACK, resp.type());
            assertEquals(1, fastHandled.get(), "a concurrent fast call is served while the long blocking call is still running (threads are decoupled)");
            releaseBlocker.countDown();
            a.join(3000);
        }
    }

    @Test
    void sendIsFireAndForget() throws Exception {
        CountDownLatch handled = new CountDownLatch(1);
        Function<RpcMessage, RpcMessage> handler = req -> { handled.countDown(); return RpcMessage.ack(); };
        try (InProcessRpcTransport t = new InProcessRpcTransport(handler)) {
            t.send(RpcMessage.heartbeat("w0"));
            assertTrue(handled.await(2, TimeUnit.SECONDS), "a message sent one-way is eventually handled");
        }
    }
}

