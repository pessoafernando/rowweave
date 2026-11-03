package io.rowweave.sched.net;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A bidirectional connect must not deadlock by exhausting the shared permits in one direction. */
class DataPlaneHandshakeAdmissionTest {

    @Test
    void certifiedResourceEnvelopeRejectsMissingColdReserveAndUnmeasuredConcurrency() {
        assertEquals(832L * 1024L * 1024L,
                DataPlaneHandshakeAdmission.requiredResidentBytes(4));
        assertThrows(IllegalArgumentException.class, () ->
                DataPlaneHandshakeAdmission.forTesting(
                        4, residentBytes(4) - 1L));
        assertThrows(IllegalArgumentException.class, () ->
                DataPlaneHandshakeAdmission.forTesting(
                        5, DataPlaneHandshakeAdmission.requiredResidentBytes(5)));
    }

    @Test
    @Timeout(5)
    void simultaneousWorkersKeepInboundProgressAfterExhaustingOutboundPartitions()
            throws Exception {
        DataPlaneHandshakeAdmission worker0 =
                DataPlaneHandshakeAdmission.forTesting(4, residentBytes(4));
        DataPlaneHandshakeAdmission worker1 =
                DataPlaneHandshakeAdmission.forTesting(4, residentBytes(4));
        CountDownLatch outboundSaturated = new CountDownLatch(2);
        CountDownLatch connectTogether = new CountDownLatch(1);
        ExecutorService connectors = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> w0ToW1 = connectors.submit(() -> connectAfterSaturatingOutbound(
                    worker0, worker1, outboundSaturated, connectTogether));
            Future<Boolean> w1ToW0 = connectors.submit(() -> connectAfterSaturatingOutbound(
                    worker1, worker0, outboundSaturated, connectTogether));

            assertTrue(outboundSaturated.await(1, TimeUnit.SECONDS),
                    "both Workers must first fill their outbound partitions at the same time");
            assertEquals(2, worker0.snapshot().activeOutbound());
            assertEquals(2, worker1.snapshot().activeOutbound());
            assertEquals(384L * 1024L * 1024L,
                    worker0.snapshot().residentBytes());
            assertEquals(DataPlaneHandshakeAdmission.PROCESS_COLD_RESERVE_BYTES,
                    worker0.snapshot().processColdReservedBytes());
            assertEquals(448L * 1024L * 1024L,
                    worker0.snapshot().totalReservedBytes());
            assertEquals(832L * 1024L * 1024L,
                    worker0.snapshot().configuredResidentBytes());
            connectTogether.countDown();

            assertTrue(w0ToW1.get(1, TimeUnit.SECONDS));
            assertTrue(w1ToW0.get(1, TimeUnit.SECONDS));
        } finally {
            connectTogether.countDown();
            connectors.shutdownNow();
            assertTrue(connectors.awaitTermination(2, TimeUnit.SECONDS));
        }

        assertEquals(0, worker0.snapshot().activeTotal());
        assertEquals(0, worker1.snapshot().activeTotal());
        assertEquals(0L, worker0.snapshot().residentBytes());
        assertEquals(0L, worker1.snapshot().residentBytes());
        assertEquals(DataPlaneHandshakeAdmission.PROCESS_COLD_RESERVE_BYTES,
                worker0.snapshot().totalReservedBytes());
    }

    @Test
    @Timeout(5)
    void delegatedTasksAreDirectionallyBoundedWithoutAnUnboundedQueue() throws Exception {
        DataPlaneHandshakeAdmission admission =
                DataPlaneHandshakeAdmission.forTesting(4, residentBytes(4));
        CountDownLatch running = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        List<DataPlaneHandshakeAdmission.Lease> leases = new ArrayList<>();
        List<DataPlaneHandshakeAdmission.HandlerLease> handlerLeases = new ArrayList<>();
        try {
            for (int i = 0; i < admission.inboundCapacity(); i++) {
                DataPlaneHandshakeAdmission.Lease lease = admission.acquireInbound(
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
                leases.add(lease);
                DataPlaneHandshakeAdmission.HandlerLease handlerLease =
                        admission.claimForHandler(lease,
                                DataPlaneHandshakeAdmission.Direction.INBOUND);
                handlerLeases.add(handlerLease);
                handlerLease.delegatedTaskExecutor().execute(() -> {
                    running.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            assertTrue(running.await(1, TimeUnit.SECONDS));
            CountDownLatch successors = new CountDownLatch(admission.inboundCapacity());
            // Netty posts its resume callback before SslTasksRunner.run() returns.
            // The event loop can submit each successor while every worker is still
            // returning from the preceding task; this is not a new handshake.
            for (DataPlaneHandshakeAdmission.HandlerLease handlerLease : handlerLeases) {
                handlerLease.delegatedTaskExecutor().execute(successors::countDown);
            }
            ThreadPoolExecutor executor = (ThreadPoolExecutor)
                    handlerLeases.get(0).delegatedTaskExecutor();
            assertEquals(admission.inboundCapacity(), executor.getQueue().size());
            assertEquals(admission.inboundCapacity(), successors.getCount());
            assertThrows(RejectedExecutionException.class, () ->
                    handlerLeases.get(0).delegatedTaskExecutor().execute(() -> { }));
            assertThrows(RejectedExecutionException.class, () ->
                    admission.claimForHandler(leases.get(0),
                            DataPlaneHandshakeAdmission.Direction.INBOUND));
            try (DataPlaneHandshakeAdmission.Lease outbound = admission.acquireOutbound(
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(1))) {
                try (DataPlaneHandshakeAdmission.HandlerLease handlerLease =
                             admission.claimForHandler(outbound,
                                     DataPlaneHandshakeAdmission.Direction.OUTBOUND)) {
                    handlerLease.delegatedTaskExecutor().execute(() -> { });
                }
            }
            release.countDown();
            assertTrue(successors.await(1, TimeUnit.SECONDS));
            assertEquals(admission.inboundCapacity(), executor.getLargestPoolSize());
        } finally {
            release.countDown();
            for (DataPlaneHandshakeAdmission.HandlerLease handlerLease : handlerLeases) {
                handlerLease.close();
            }
            for (DataPlaneHandshakeAdmission.Lease lease : leases) lease.close();
            admission.close();
        }
        assertThrows(RejectedExecutionException.class, () ->
                admission.acquireInbound(System.nanoTime() + TimeUnit.SECONDS.toNanos(1)));
        assertThrows(RejectedExecutionException.class, () ->
                handlerLeases.get(0).delegatedTaskExecutor().execute(() -> { }));
    }

    @Test
    @Timeout(10)
    void admittedSingleSlotHandshakeCanHandOffBeforeDelegatedWorkerReturns()
            throws Exception {
        for (DataPlaneHandshakeAdmission.Direction direction
                : DataPlaneHandshakeAdmission.Direction.values()) {
            DataPlaneHandshakeAdmission admission =
                    DataPlaneHandshakeAdmission.forTesting(2, residentBytes(2));
            CountDownLatch callbackPosted = new CountDownLatch(1);
            CountDownLatch returnFromPredecessor = new CountDownLatch(1);
            CountDownLatch successorRan = new CountDownLatch(1);
            DataPlaneHandshakeAdmission.Lease caller = direction
                    == DataPlaneHandshakeAdmission.Direction.INBOUND
                    ? admission.acquireInbound(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))
                    : admission.acquireOutbound(System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
            DataPlaneHandshakeAdmission.HandlerLease handler =
                    admission.claimForHandler(caller, direction);
            try {
                handler.delegatedTaskExecutor().execute(() -> {
                    callbackPosted.countDown();
                    try {
                        returnFromPredecessor.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                });
                assertTrue(callbackPosted.await(1, TimeUnit.SECONDS));
                handler.delegatedTaskExecutor().execute(successorRan::countDown);
                assertEquals(1L, successorRan.getCount(),
                        "handoff must queue, never run TLS work on the submitting event loop");
                assertThrows(RejectedExecutionException.class, () ->
                        handler.delegatedTaskExecutor().execute(() -> { }));
                assertEquals(1, admission.snapshot().activeTotal());
                assertEquals(DataPlaneHandshakeAdmission.HANDSHAKE_CHARGE_BYTES,
                        admission.snapshot().residentBytes());
                returnFromPredecessor.countDown();
                assertTrue(successorRan.await(1, TimeUnit.SECONDS));
            } finally {
                returnFromPredecessor.countDown();
                handler.close();
                caller.close();
                admission.close();
            }
        }
    }

    @Test
    @Timeout(5)
    void closeWakesBlockedAcquireAndNoLeaseCanLinearizeAfterClose() throws Exception {
        DataPlaneHandshakeAdmission admission =
                DataPlaneHandshakeAdmission.forTesting(4, residentBytes(4));
        List<DataPlaneHandshakeAdmission.Lease> active = new ArrayList<>();
        for (int i = 0; i < admission.inboundCapacity(); i++) {
            active.add(admission.acquireInbound(
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(1)));
        }
        ExecutorService waiter = Executors.newSingleThreadExecutor();
        try {
            Future<?> waiting = waiter.submit(() -> admission.acquireInbound(
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(30)));
            long observedBy = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (admission.inboundWaitersForTesting() == 0
                    && System.nanoTime() < observedBy) {
                Thread.yield();
            }
            assertEquals(1, admission.inboundWaitersForTesting());
            IllegalStateException activeFailure = assertThrows(
                    IllegalStateException.class, admission::close);
            assertTrue(activeFailure.getMessage().contains("active leases"),
                    activeFailure.toString());
            ExecutionException rejected = assertThrows(ExecutionException.class,
                    () -> waiting.get(1, TimeUnit.SECONDS));
            assertTrue(rejected.getCause() instanceof RejectedExecutionException,
                    rejected.toString());
        } finally {
            for (DataPlaneHandshakeAdmission.Lease lease : active) lease.close();
            admission.close();
            waiter.shutdownNow();
            assertTrue(waiter.awaitTermination(2, TimeUnit.SECONDS));
        }
        assertEquals(0, admission.snapshot().activeTotal());
    }

    @Test
    void claimedHandlerLeaseCannotBeReleasedByOriginalCallerLease() throws Exception {
        DataPlaneHandshakeAdmission admission =
                DataPlaneHandshakeAdmission.forTesting(4, residentBytes(4));
        DataPlaneHandshakeAdmission.Lease callerLease = admission.acquireInbound(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
        DataPlaneHandshakeAdmission.HandlerLease handlerLease = admission.claimForHandler(
                callerLease, DataPlaneHandshakeAdmission.Direction.INBOUND);
        callerLease.close();
        assertEquals(1, admission.snapshot().activeInbound());
        handlerLease.close();
        assertEquals(0, admission.snapshot().activeInbound());
        admission.close();
    }

    private static boolean connectAfterSaturatingOutbound(
            DataPlaneHandshakeAdmission local,
            DataPlaneHandshakeAdmission peer,
            CountDownLatch outboundSaturated,
            CountDownLatch connectTogether) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        List<DataPlaneHandshakeAdmission.Lease> outbound = new ArrayList<>();
        try {
            for (int i = 0; i < local.outboundCapacity(); i++) {
                outbound.add(local.acquireOutbound(deadline));
            }
            outboundSaturated.countDown();
            if (!connectTogether.await(1, TimeUnit.SECONDS)) {
                return false;
            }
            try (DataPlaneHandshakeAdmission.Lease inbound =
                         peer.acquireInbound(deadline)) {
                return inbound.direction()
                        == DataPlaneHandshakeAdmission.Direction.INBOUND;
            }
        } finally {
            for (DataPlaneHandshakeAdmission.Lease lease : outbound) {
                lease.close();
            }
        }
    }

    private static long residentBytes(int handshakes) {
        return DataPlaneHandshakeAdmission.requiredResidentBytes(handshakes);
    }
}
