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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MasterRegistryLookupAmplificationTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    @Test
    void oneWorkerIsLookedUpOncePerJobLivenessPollDespiteMultipleStages() throws Exception {
        String host = "lookup-count-worker";
        int port = 61_040;
        CountingRegistry registry = new CountingRegistry();
        CopyOnWriteArrayList<Long> stageIds = new CopyOnWriteArrayList<>();
        java.util.concurrent.ConcurrentHashMap<Long, io.rowweave.run.RunAttemptFence> stageFences =
                new java.util.concurrent.ConcurrentHashMap<>();
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                stageIds.add(request.stageId());
                stageFences.put(request.stageId(), request.runAttemptFence());
            }
            return RpcMessage.ack();
        });
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            master.handler().apply(RpcMessage.registerWorker("w", host, port, 8, 62_040));
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Collections.emptyList()),
                    PlanNode.unary("distinct", META, 0)
                            .listParam("keys", Collections.singletonList("v")).build(),
                    PlanNode.sink(META, 1, new DiscardSink(META)).build()));
            Future<RpcMessage> result = submitter.submit(
                    () -> master.handler().apply(RpcMessage.submitJob(graph.toBytes())));

            awaitAtLeast(stageIds, 2, 2_000L);
            Thread.sleep(100L);
            int beforePoll = registry.pointLookups.get();
            awaitGreaterThan(registry.pointLookups, beforePoll, 2_000L);
            Thread.sleep(50L);
            assertEquals(1, registry.pointLookups.get() - beforePoll,
                    "a real Worker may do at most one point lookup per round and must not re-read once per stage owner object");

            master.handler().apply(RpcMessage.stageReport(
                    "w", stageIds.get(0), false, 0L, 0L, "end counting test", null,
                    stageFences.get(stageIds.get(0))));
            assertFalse(result.get(2, TimeUnit.SECONDS).success());
        } finally {
            submitter.shutdownNow();
            endpointRegistration.close();
        }
    }

    private static void awaitAtLeast(List<Long> values, int expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (values.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(values.size() >= expected, "the test must produce several stage assignments first, actual=" + values.size());
    }

    private static void awaitGreaterThan(AtomicInteger value, int floor, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (value.get() <= floor && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(value.get() > floor, "timed out waiting for the Job liveness registry point lookup");
    }

    private static final class CountingRegistry implements ServiceRegistry {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final AtomicInteger pointLookups = new AtomicInteger();

        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() { return delegate.liveWorkers(); }
        @Override public RegistryWorker worker(String workerId) {
            pointLookups.incrementAndGet();
            return delegate.worker(workerId);
        }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }
}

