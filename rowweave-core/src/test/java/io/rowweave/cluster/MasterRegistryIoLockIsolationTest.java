package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MasterRegistryIoLockIsolationTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    @Test
    void slowAssignmentLookupDoesNotBlockReplacementAndStaleSnapshotCannotClaim() throws Exception {
        String host = "lookup-lock-worker";
        int port = 61_041;
        BlockingLookupRegistry registry = new BlockingLookupRegistry();
        AtomicInteger assignments = new AtomicInteger();
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                assignments.incrementAndGet();
            }
            return RpcMessage.ack();
        });
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            RpcMessage oldRegistration = master.handler().apply(
                    RpcMessage.registerWorker("w", host, port, 4, 62_041, "inc-old"));
            assertEquals(RpcMessage.Type.ACK, oldRegistration.type());

            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, java.util.Collections.emptyList()),
                    PlanNode.sink(META, 0, new DiscardSink(META)).build()));
            registry.blockPointLookupAfter(0);
            Future<RpcMessage> submitted = callers.submit(
                    () -> master.handler().apply(RpcMessage.submitJob(graph.toBytes())));
            assertTrue(registry.awaitBlockedLookup(2, TimeUnit.SECONDS),
                    "the test must block on the registry point lookup of the assignment claim");

            Future<RpcMessage> replacement = callers.submit(() -> master.handler().apply(
                    RpcMessage.registerWorker("w", host, port, 4, 62_041, "inc-new")));
            try {
                RpcMessage response = replacement.get(1, TimeUnit.SECONDS);
                assertEquals(RpcMessage.Type.ACK, response.type(),
                        "a slow registry read must not hold the Worker lifecycle lock and freeze a replacement REGISTER");
                assertEquals("inc-new", master.worker("w").incarnationId());
            } finally {
                registry.releaseBlockedLookup();
            }

            RpcMessage result = submitted.get(2, TimeUnit.SECONDS);
            assertFalse(result.success(), "an assignment claim capturing an old endpoint must fail after re-validation inside the lock");
            assertEquals(0, assignments.get(), "an old snapshot must not dispatch a stage to the replacement");
            assertEquals("inc-new", master.worker("w").incarnationId());
            assertFalse(master.workerQuarantined("w"), "a failed old claim must not quarantine the replacement");
        } finally {
            registry.releaseBlockedLookup();
            callers.shutdownNow();
            endpointRegistration.close();
        }
    }

    @Test
    void slowDepartureLookupDoesNotBlockReplacementAndOldDepartureIsNoop() throws Exception {
        BlockingLookupRegistry registry = new BlockingLookupRegistry();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            master.handler().apply(RpcMessage.registerWorker(
                    "w", "departure-host", 61_042, 4, 62_042, "inc-old"));
            registry.blockPointLookupAfter(0);
            Future<RpcMessage> departure = callers.submit(() -> master.handler().apply(
                    RpcMessage.deregisterWorker(
                            "w", "departure-host", 61_042, 4, 62_042, "inc-old")));
            assertTrue(registry.awaitBlockedLookup(2, TimeUnit.SECONDS));

            Future<RpcMessage> replacement = callers.submit(() -> master.handler().apply(
                    RpcMessage.registerWorker(
                            "w", "departure-host", 61_042, 4, 62_042, "inc-new")));
            try {
                assertEquals(RpcMessage.Type.ACK,
                        replacement.get(1, TimeUnit.SECONDS).type());
            } finally {
                registry.releaseBlockedLookup();
            }

            assertNull(departure.get(2, TimeUnit.SECONDS));
            assertEquals("inc-new", master.worker("w").incarnationId());
            assertFalse(master.workerQuarantined("w"), "a failed old departure must not quarantine the replacement");
        } finally {
            registry.releaseBlockedLookup();
            callers.shutdownNow();
        }
    }

    @Test
    void slowQuarantineRecheckDoesNotBlockReplacementOrQuarantineNewGeneration() throws Exception {
        BlockingLookupRegistry registry = new BlockingLookupRegistry();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            master.handler().apply(RpcMessage.registerWorker(
                    "w", "quarantine-host", 61_043, 4, 62_043, "inc-old"));
            registry.blockPointLookupAfter(1);
            Future<?> quarantine = callers.submit(() -> master.quarantineWorker("w"));
            assertTrue(registry.awaitBlockedLookup(2, TimeUnit.SECONDS));

            Future<RpcMessage> replacement = callers.submit(() -> master.handler().apply(
                    RpcMessage.registerWorker(
                            "w", "quarantine-host", 61_043, 4, 62_043, "inc-new")));
            try {
                assertEquals(RpcMessage.Type.ACK,
                        replacement.get(1, TimeUnit.SECONDS).type());
            } finally {
                registry.releaseBlockedLookup();
            }

            quarantine.get(2, TimeUnit.SECONDS);
            assertEquals("inc-new", master.worker("w").incarnationId());
            assertFalse(master.workerQuarantined("w"), "a failed old cancel must not quarantine the replacement");
        } finally {
            registry.releaseBlockedLookup();
            callers.shutdownNow();
        }
    }

    private static final class BlockingLookupRegistry implements ServiceRegistry {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final AtomicInteger lookupsBeforeBlock = new AtomicInteger(-1);
        private volatile CountDownLatch lookupBlocked = new CountDownLatch(1);
        private volatile CountDownLatch releaseLookup = new CountDownLatch(1);

        void blockPointLookupAfter(int passThroughLookups) {
            lookupBlocked = new CountDownLatch(1);
            releaseLookup = new CountDownLatch(1);
            lookupsBeforeBlock.set(passThroughLookups);
        }

        boolean awaitBlockedLookup(long timeout, TimeUnit unit) throws InterruptedException {
            return lookupBlocked.await(timeout, unit);
        }

        void releaseBlockedLookup() {
            releaseLookup.countDown();
        }

        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() { return delegate.liveWorkers(); }
        @Override public RegistryWorker worker(String workerId) {
            RegistryWorker snapshot = delegate.worker(workerId);
            int remaining = lookupsBeforeBlock.getAndUpdate(value -> value >= 0 ? value - 1 : value);
            if (remaining == 0) {
                lookupBlocked.countDown();
                try {
                    releaseLookup.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while blocking registry lookup", interrupted);
                }
            }
            return snapshot;
        }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }
}

