package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class BoundedStageCancellationTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));
    private static volatile CountDownLatch bodyStarted;
    private static volatile CountDownLatch releaseBody;

    private static final class UncooperativeMap implements MapFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public DataRow map(DataRow input) {
            bodyStarted.countDown();
            while (releaseBody.getCount() > 0) {
                try {
                    Thread.sleep(20L);
                } catch (InterruptedException ignored) {
                }
            }
            return input;
        }
    }

    @Test
    void uncooperativeRunningStageReturnsBoundedCancellationFailure(@TempDir Path temp) throws Exception {
        String key = "rowweave.worker.cancelDrainMs";
        String previous = System.setProperty(key, "0");
        String masterHost = "bounded-cancel-master";
        int masterPort = 45_143;
        bodyStarted = new CountDownLatch(1);
        releaseBody = new CountDownLatch(1);
        InProcessRpcRegistry.Registration masterRegistration = InProcessRpcRegistry.register(masterHost, masterPort, message ->
                message.type() == RpcMessage.Type.HEARTBEAT
                        || message.type() == RpcMessage.Type.PROGRESS_REPORT ? null : RpcMessage.ack());
        WorkerServer worker = null;
        Thread cancelThread = null;
        AtomicReference<RpcMessage> response = new AtomicReference<>();
        CountDownLatch cancelReturned = new CountDownLatch(1);
        try {
            worker = WorkerServer.newUnguarded(
                    "bounded-cancel-worker", 1, masterHost, masterPort, temp.toFile());
            long stageId = 14_301L;
            assertEquals(RpcMessage.Type.ACK, worker.handler().apply(RunAttemptFenceTestSupport.inProcess(
                    "bounded-cancel-worker", stageId, StageCommand.wholeGraph(uncooperativeGraph()),
                    worker.incarnationId())).type());
            assertTrue(bodyStarted.await(5, TimeUnit.SECONDS), "uncooperative body did not start");

            WorkerServer target = worker;
            cancelThread = new Thread(() -> {
                response.set(target.handler().apply(RpcMessage.cancelStage(
                        "bounded-cancel-worker", stageId, target.incarnationId(),
                        RunAttemptFenceTestSupport.FENCE)));
                cancelReturned.countDown();
            }, "bounded-cancel-caller");
            cancelThread.start();

            assertTrue(cancelReturned.await(200, TimeUnit.MILLISECONDS),
                    "a CANCEL handler must not block forever on a user function that swallows interrupts");
            assertNotNull(response.get());
            assertEquals(RpcMessage.Type.JOB_RESULT, response.get().type());
            assertFalse(response.get().success());
            assertTrue(response.get().errorMessage().contains("14301"), response.get().errorMessage());
            assertEquals(1, worker.runningStageCount(), "a timeout must not report a false terminal state or release the slot early");
        } finally {
            releaseBody.countDown();
            if (cancelThread != null) {
                cancelThread.join(5_000L);
            }
            if (worker != null) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (worker.runningStageCount() != 0 && System.nanoTime() < deadline) {
                    Thread.sleep(10L);
                }
                worker.close();
            }
            masterRegistration.close();
            restoreProperty(key, previous);
        }
    }

    @Test
    void rejectsNegativeCancelDrainBeforeOpeningWorker(@TempDir Path temp) {
        String key = "rowweave.worker.cancelDrainMs";
        String previous = System.setProperty(key, "-1");
        try {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> WorkerServer.newUnguarded(
                            "invalid-cancel-worker", 1, "unused-master", 45_144, temp.toFile()));
            assertTrue(failure.getMessage().contains(key), failure.getMessage());
        } finally {
            restoreProperty(key, previous);
        }
    }

    @Test
    void masterQuarantinesCurrentGenerationWhenWorkerRejectsCancellation() {
        String host = "reject-cancel-worker";
        int port = 61_047;
        String incarnation = "inc-reject-cancel";
        AtomicInteger assignments = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        AtomicInteger staleControlFences = new AtomicInteger();
        java.util.concurrent.ConcurrentHashMap<Long, io.rowweave.run.RunAttemptFence> fences =
                new java.util.concurrent.ConcurrentHashMap<>();
        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    fences.put(request.stageId(), request.runAttemptFence());
                    if (assignments.getAndIncrement() == 0) {
                        master.handler().apply(RpcMessage.stageReport(
                                request.workerId(), request.stageId(), false, 0, 0,
                                "trigger cancellation", incarnation, request.runAttemptFence()));
                    }
                    return RpcMessage.ack();
                }
                if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                    cancellations.incrementAndGet();
                    if (!java.util.Objects.equals(
                            fences.get(request.stageId()), request.runAttemptFence())) {
                        staleControlFences.incrementAndGet();
                    }
                    return RpcMessage.jobResult(false, 0, "stage cancellation timed out");
                }
                return RpcMessage.ack();
            });
            try {
                master.handler().apply(RpcMessage.registerWorker("w", host, port, 8, -1, incarnation));
                RpcMessage result = master.handler().apply(RpcMessage.submitJob(distinctGraph().toBytes()));

                assertFalse(result.success());
                assertTrue(cancellations.get() > 0, "a failed stage must cancel the remaining stages in a cascade");
                assertEquals(0, staleControlFences.get(),
                        "CANCEL_STAGE must preserve the assignment attempt fence");
                assertTrue(master.workerQuarantined("w"),
                        "a non-ACK cancellation response must isolate the current endpoint plus incarnation");
                assertEquals(0, master.activeStageTrackerCount());
                assertEquals(0, master.activeStageAssignmentCount());
            } finally {
                endpointRegistration.close();
            }
        }
    }

    @Test
    void leaderChangeDoesNotRegisterAfterOldTermCancellationTimedOut(@TempDir Path temp) throws Exception {
        String cancelKey = "rowweave.worker.cancelDrainMs";
        String heartbeatKey = "rowweave.worker.heartbeatMs";
        String previousCancel = System.setProperty(cancelKey, "0");
        String previousHeartbeat = System.setProperty(heartbeatKey, "60000");
        String oldHost = "old-term-master";
        String newHost = "new-term-master";
        int oldPort = 45_145;
        int newPort = 45_146;
        long oldTerm = 143_001L;
        long newTerm = 143_002L;
        AtomicInteger newRegistrations = new AtomicInteger();
        InProcessRegistry registry = new InProcessRegistry();
        RegistryWorker oldLeader = leaderRole(oldHost, oldPort, oldTerm);
        registry.register(oldLeader);
        InProcessRpcRegistry.Registration oldRegistration = InProcessRpcRegistry.register(
                oldHost, oldPort, request -> RpcMessage.ack());
        InProcessRpcRegistry.Registration newRegistration = InProcessRpcRegistry.register(newHost, newPort, request -> {
            if (request.type() == RpcMessage.Type.REGISTER_WORKER) {
                newRegistrations.incrementAndGet();
            }
            return RpcMessage.ack();
        });
        bodyStarted = new CountDownLatch(1);
        releaseBody = new CountDownLatch(1);
        WorkerServer worker = null;
        try {
            worker = WorkerServer.newUnguarded(
                    "term-timeout-worker", 1, new RegistryMasterLocator(registry), temp.toFile());
            RpcMessage accepted = worker.handler().apply(RunAttemptFenceTestSupport.inProcess(
                    "term-timeout-worker", 14_302L, StageCommand.wholeGraph(uncooperativeGraph()),
                    new Endpoint(oldHost, oldPort), oldTerm, worker.incarnationId()));
            assertEquals(RpcMessage.Type.ACK, accepted.type());
            assertTrue(bodyStarted.await(5, TimeUnit.SECONDS));

            registry.deregister(MasterServer.MASTER_LEADER_ROLE_ID);
            registry.register(leaderRole(newHost, newPort, newTerm));

            Thread.sleep(200L);
            releaseBody.countDown();
            long terminalDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (worker.runningStageCount() != 0 && System.nanoTime() < terminalDeadline) {
                Thread.sleep(10L);
            }
            Thread.sleep(100L);

            assertEquals(0, worker.runningStageCount());
            assertEquals(0, newRegistrations.get(),
                    "after a cancellation confirmation times out on an old term, a late exit of the user function must not re-register across terms");
        } finally {
            releaseBody.countDown();
            if (worker != null) {
                worker.close();
            }
            oldRegistration.close();
            newRegistration.close();
            restoreProperty(cancelKey, previousCancel);
            restoreProperty(heartbeatKey, previousHeartbeat);
        }
    }

    private static RegistryWorker leaderRole(String host, int port, long term) {
        return new RegistryWorker(MasterServer.MASTER_LEADER_ROLE_ID, host, port,
                MasterElection.high(term), MasterElection.low(term));
    }

    private static PlanGraph uncooperativeGraph() {
        DataRow row = new DataRow(1);
        row.set(0, 1);
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row)),
                PlanNode.unaryFn("map", META, 0, new UncooperativeMap()),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()));
    }

    private static PlanGraph distinctGraph() {
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.unary("distinct", META, 0)
                        .listParam("keys", Collections.singletonList("v")).build(),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()));
    }

    private static void restoreProperty(String key, String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }
}

