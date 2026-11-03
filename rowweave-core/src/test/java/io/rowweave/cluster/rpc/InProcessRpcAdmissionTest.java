package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InProcessRpcAdmissionTest {

    @Test
    @Timeout(10)
    void saturatedRegistryRejectsCallAndSendWithoutInvokingHandlerThenRecovers() throws Exception {
        String host = "inproc-admission-" + java.util.UUID.randomUUID();
        int port = 1;
        int maxHandlers = 2;
        CountDownLatch entered = new CountDownLatch(maxHandlers);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        InProcessRpcRegistry.Registration registration = InProcessRpcRegistry.registerForTesting(
                host, port, request -> {
                    invocations.incrementAndGet();
                    entered.countDown();
                    awaitUninterruptibly(release);
                    return RpcMessage.ack();
                }, maxHandlers);
        RpcTransport transport = InProcessRpcRegistry.lookup(host, port);
        ExecutorService callers = Executors.newFixedThreadPool(maxHandlers);
        try {
            Future<RpcMessage> first = callers.submit(() ->
                    transport.call(RpcMessage.heartbeat("first")));
            Future<RpcMessage> second = callers.submit(() ->
                    transport.call(RpcMessage.heartbeat("second")));
            assertTrue(entered.await(2, TimeUnit.SECONDS), "both bounded handler slots are occupied");

            IllegalStateException callRejected = assertThrows(IllegalStateException.class,
                    () -> transport.call(RpcMessage.heartbeat("rejected-call")));
            assertEquals("in-process RPC admission exhausted: maxHandlers=2",
                    callRejected.getMessage());
            IllegalStateException sendRejected = assertThrows(IllegalStateException.class,
                    () -> transport.send(RpcMessage.heartbeat("rejected-send")));
            assertEquals("in-process RPC admission exhausted: maxHandlers=2",
                    sendRejected.getMessage());
            assertEquals(2, invocations.get(), "a saturated refusal neither queues, runs on the caller thread, nor invokes the handler");

            release.countDown();
            assertEquals(RpcMessage.Type.ACK, first.get(2, TimeUnit.SECONDS).type());
            assertEquals(RpcMessage.Type.ACK, second.get(2, TimeUnit.SECONDS).type());
            assertEquals(RpcMessage.Type.ACK,
                    callWhenWorkerIsReady(transport, "after-release").type(),
                    "the endpoint resumes admission once a slot is released");
            assertEquals(3, invocations.get());
        } finally {
            release.countDown();
            callers.shutdownNow();
            registration.close();
        }
    }

    @Test
    @Timeout(10)
    void timeoutKeepsBoundedSlotUntilHandlerReallyReturns() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        try (InProcessRpcTransport transport = InProcessRpcTransport.forTesting(request -> {
            invocations.incrementAndGet();
            entered.countDown();
            awaitUninterruptibly(release);
            exited.countDown();
            return RpcMessage.ack();
        }, 1, 25L)) {
            RuntimeException timedOut = assertThrows(RuntimeException.class,
                    () -> transport.call(RpcMessage.heartbeat("timeout")));
            assertTrue(timedOut.getMessage().contains("timed out after 25ms"));
            assertTrue(entered.await(1, TimeUnit.SECONDS));

            IllegalStateException rejected = assertThrows(IllegalStateException.class,
                    () -> transport.call(RpcMessage.heartbeat("while-handler-running")));
            assertEquals("in-process RPC admission exhausted: maxHandlers=1", rejected.getMessage());
            assertEquals(1, invocations.get(), "a caller timeout does not free a server handler slot that is still running");

            release.countDown();
            assertTrue(exited.await(2, TimeUnit.SECONDS));
            assertEquals(RpcMessage.Type.ACK,
                    callWhenWorkerIsReady(transport, "after-real-exit").type());
            assertEquals(2, invocations.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    @Timeout(10)
    void ownerCloseInterruptsHandlerAndStaleWrapperRejectsWithClosedContract() throws Exception {
        String host = "inproc-close-" + java.util.UUID.randomUUID();
        int port = 1;
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicBoolean handlerSawInterrupt = new AtomicBoolean();
        InProcessRpcRegistry.Registration registration = InProcessRpcRegistry.registerForTesting(
                host, port, request -> {
                    entered.countDown();
                    try {
                        new CountDownLatch(1).await();
                    } catch (InterruptedException expected) {
                        handlerSawInterrupt.set(true);
                        interrupted.countDown();
                        Thread.currentThread().interrupt();
                    }
                    return RpcMessage.ack();
                }, 1);
        RpcTransport stale = InProcessRpcRegistry.lookup(host, port);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> running = caller.submit(() ->
                    stale.call(RpcMessage.heartbeat("running")));
            assertTrue(entered.await(2, TimeUnit.SECONDS));

            registration.close();
            assertTrue(interrupted.await(2, TimeUnit.SECONDS), "owner close interrupts the handlers it knows about");
            running.get(2, TimeUnit.SECONDS);
            assertTrue(handlerSawInterrupt.get());
            assertFalse(InProcessRpcRegistry.isRegistered(host, port));

            IllegalStateException closed = assertThrows(IllegalStateException.class,
                    () -> stale.call(RpcMessage.heartbeat("stale")));
            assertEquals("in-process RPC endpoint is closed", closed.getMessage());
            registration.close();
        } finally {
            registration.close();
            caller.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    void twoSlotsLetFastReportPassWhileLongSubmitIsBlocked() throws Exception {
        CountDownLatch submitEntered = new CountDownLatch(1);
        CountDownLatch releaseSubmit = new CountDownLatch(1);
        AtomicInteger reports = new AtomicInteger();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (InProcessRpcTransport transport = InProcessRpcTransport.forTesting(request -> {
            if (request.type() == RpcMessage.Type.SUBMIT_JOB) {
                submitEntered.countDown();
                awaitUninterruptibly(releaseSubmit);
            } else {
                reports.incrementAndGet();
            }
            return RpcMessage.ack();
        }, 2, 1_000L)) {
            Future<RpcMessage> submit = caller.submit(() ->
                    transport.call(RpcMessage.submitJob(new byte[]{1})));
            assertTrue(submitEntered.await(2, TimeUnit.SECONDS));

            assertEquals(RpcMessage.Type.ACK,
                    transport.call(RpcMessage.heartbeat("stage-report")).type(),
                    "while a long submit holds one handler the second slot still serves report");
            assertEquals(1, reports.get());
            releaseSubmit.countDown();
            assertEquals(RpcMessage.Type.ACK, submit.get(2, TimeUnit.SECONDS).type());
        } finally {
            releaseSubmit.countDown();
            caller.shutdownNow();
        }
    }

    @Test
    void productionInProcessPathsCannotUseCachedThreadPools() throws Exception {
        String transport = source("InProcessRpcTransport.java");
        String registry = source("InProcessRpcRegistry.java");
        assertFalse(transport.contains("newCachedThreadPool"),
                "a self-owned transport keeps no unbounded cached handler pool");
        assertFalse(registry.contains("newCachedThreadPool"),
                "a registry endpoint keeps no unbounded cached handler pool");
        assertTrue(transport.contains("BoundedInProcessExecutor"));
        assertTrue(registry.contains("BoundedInProcessExecutor"));
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static RpcMessage callWhenWorkerIsReady(RpcTransport transport, String worker)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (true) {
            try {
                return transport.call(RpcMessage.heartbeat(worker));
            } catch (IllegalStateException admission) {
                if (!admission.getMessage().startsWith("in-process RPC admission exhausted:")
                        || System.nanoTime() >= deadline) {
                    throw admission;
                }
                Thread.sleep(5L);
            }
        }
    }

    private static String source(String fileName) throws Exception {
        Path cwd = Paths.get(System.getProperty("user.dir"));
        Path path = cwd.resolve("src/main/java/io/rowweave/cluster/rpc").resolve(fileName);
        if (!Files.exists(path)) {
            path = cwd.resolve("rowweave-core/src/main/java/io/rowweave/cluster/rpc")
                    .resolve(fileName);
        }
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}

