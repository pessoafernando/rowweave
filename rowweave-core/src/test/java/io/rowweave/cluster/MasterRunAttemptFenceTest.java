package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.monitor.InMemoryJobMonitor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterRunAttemptFenceTest {

    @Test
    void wholeGraphAssignmentCarriesFenceAndWrongFenceCannotCompleteTracker() throws Exception {
        String host = "attempt-fence-worker";
        int port = 61_066;
        String incarnation = "inc-attempt-fence";
        AtomicReference<RpcMessage> assignment = new AtomicReference<>();
        CountDownLatch assigned = new CountDownLatch(1);
        ExecutorService submitter = Executors.newSingleThreadExecutor();

        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(host, port, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    assignment.set(request);
                    assigned.countDown();
                }
                return RpcMessage.ack();
            });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", host, port, 2, -1, incarnation));
                Future<RpcMessage> submitted = submitter.submit(
                        () -> master.handler().apply(RpcMessage.submitJob(emptyWholeGraph().toBytes())));
                assertTrue(assigned.await(5, TimeUnit.SECONDS));

                RpcMessage assign = assignment.get();
                RunAttemptFence current = assign.runAttemptFence();
                assertNotNull(current, "production assignment must never use legacy-null fence");
                assertEquals(current, deserialize(assign.stageCommandBlob()).runAttemptFence());
                RunAttemptFence[] staleFences = {
                        new RunAttemptFence("other-run", current.attemptId(), current.fenceToken()),
                        new RunAttemptFence(current.runId(), "other-attempt", current.fenceToken()),
                        new RunAttemptFence(current.runId(), current.attemptId(), current.fenceToken() + 1L)
                };
                for (RunAttemptFence stale : staleFences) {
                    assertEquals(RpcMessage.Type.ACK, master.handler().apply(RpcMessage.stageReport(
                            "w", assign.stageId(), true, 0L, 0L, null, incarnation, stale)).type());
                    Thread.sleep(50L);
                    assertFalse(submitted.isDone(),
                            "wrong run/attempt/token terminal report must not release the job tracker");
                }

                assertEquals(RpcMessage.Type.ACK, master.handler().apply(RpcMessage.stageReport(
                        "w", assign.stageId(), true, 0L, 0L, null, incarnation, current)).type());
                RpcMessage result = submitted.get(5, TimeUnit.SECONDS);
                assertTrue(result.success(), result.errorMessage());
            } finally {
                endpoint.close();
            }
        } finally {
            submitter.shutdownNow();
        }
    }

    @Test
    void wrongFencePortReportCannotReleaseListeningBarrier() throws Exception {
        String host = "attempt-port-worker";
        int port = 61_068;
        String incarnation = "inc-attempt-port";
        AtomicReference<RpcMessage> firstAssignment = new AtomicReference<>();
        CountDownLatch firstAssigned = new CountDownLatch(1);
        AtomicInteger assignments = new AtomicInteger();
        ExecutorService submitter = Executors.newSingleThreadExecutor();

        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())
                .setObjectReuse(false)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(host, port, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    int index = assignments.incrementAndGet();
                    if (index == 1) {
                        firstAssignment.set(request);
                        firstAssigned.countDown();
                    } else {
                        master.handler().apply(RpcMessage.portReport(
                                request.workerId(), request.stageId(), 20_000 + index, 0,
                                incarnation, request.runAttemptFence()));
                        master.handler().apply(RpcMessage.stageReport(
                                request.workerId(), request.stageId(), true, 0L, 0L, null,
                                incarnation, request.runAttemptFence()));
                    }
                }
                return RpcMessage.ack();
            });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", host, port, 8, -1, incarnation));
                Future<RpcMessage> submitted = submitter.submit(
                        () -> master.handler().apply(RpcMessage.submitJob(genericDag().toBytes())));
                assertTrue(firstAssigned.await(5, TimeUnit.SECONDS));
                RpcMessage assign = firstAssignment.get();
                RunAttemptFence current = assign.runAttemptFence();
                RunAttemptFence stale = new RunAttemptFence(
                        current.runId(), current.attemptId(), current.fenceToken() + 1L);

                assertEquals(RpcMessage.Type.ACK, master.handler().apply(RpcMessage.portReport(
                        "w", assign.stageId(), 19_999, 0, incarnation, stale)).type());
                Thread.sleep(100L);
                assertEquals(1, assignments.get(),
                        "wrong fence port must not release the setup barrier or schedule upstream");

                assertEquals(RpcMessage.Type.ACK, master.handler().apply(RpcMessage.portReport(
                        "w", assign.stageId(), 19_999, 0, incarnation, current)).type());
                master.handler().apply(RpcMessage.stageReport(
                        "w", assign.stageId(), true, 0L, 0L, null, incarnation, current));
                RpcMessage result = submitted.get(5, TimeUnit.SECONDS);
                assertTrue(result.success(), result.errorMessage());
                assertTrue(assignments.get() >= 2);
            } finally {
                endpoint.close();
            }
        } finally {
            submitter.shutdownNow();
        }
    }

    @Test
    void wrongFenceProgressCannotMutateMonitor() throws Exception {
        String host = "attempt-progress-worker";
        int port = 61_069;
        String incarnation = "inc-attempt-progress";
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(10);
        java.util.concurrent.CopyOnWriteArrayList<RpcMessage> assignments =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        ExecutorService submitter = Executors.newSingleThreadExecutor();

        try (MasterServer master = new MasterServer(
                60_000L, 0, new InProcessRegistry(), monitor)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(host, port, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) assignments.add(request);
                return RpcMessage.ack();
            });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", host, port, 8, 62_069, incarnation));
                Future<RpcMessage> submitted = submitter.submit(
                        () -> master.handler().apply(RpcMessage.submitJob(genericDag().toBytes())));
                awaitAssignments(assignments, 2);
                Thread.sleep(50L); // allow stageRefs to be installed after the final assignment wave

                for (RpcMessage assign : assignments) {
                    RunAttemptFence current = assign.runAttemptFence();
                    RunAttemptFence stale = new RunAttemptFence(
                            current.runId(), current.attemptId(), current.fenceToken() + 1L);
                    master.handler().apply(RpcMessage.progressReport(
                            "w", assign.stageId(), 99L, 77L, true, incarnation, stale));
                }
                Thread.sleep(50L);
                assertFalse(monitorHasOutputRows(monitor, 77L),
                        "wrong fence progress must not mutate stage or instance monitor state");

                for (RpcMessage assign : assignments) {
                    master.handler().apply(RpcMessage.progressReport(
                            "w", assign.stageId(), 99L, 77L, true, incarnation,
                            assign.runAttemptFence()));
                }
                long deadline = System.currentTimeMillis() + 2_000L;
                while (!monitorHasOutputRows(monitor, 77L) && System.currentTimeMillis() < deadline) {
                    Thread.sleep(10L);
                }
                assertTrue(monitorHasOutputRows(monitor, 77L),
                        "current fence progress must retain existing monitor behavior");

                for (RpcMessage assign : assignments) {
                    master.handler().apply(RpcMessage.stageReport(
                            "w", assign.stageId(), true, 99L, 77L, null, incarnation,
                            assign.runAttemptFence()));
                }
                assertTrue(submitted.get(5, TimeUnit.SECONDS).success());
            } finally {
                endpoint.close();
            }
        } finally {
            submitter.shutdownNow();
        }
    }

    private static void awaitAssignments(
            java.util.List<RpcMessage> assignments, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (assignments.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(assignments.size() >= expected, "assignments=" + assignments.size());
    }

    private static boolean monitorHasOutputRows(InMemoryJobMonitor monitor, long rows) {
        return monitor.jobs().stream().flatMap(job -> job.stages().stream())
                .anyMatch(stage -> stage.outputRows() == rows
                        || stage.instances().stream().anyMatch(instance -> instance.outputRows == rows));
    }

    private static StageCommand deserialize(byte[] blob) throws Exception {
        return StageCommandCodecV2.preflight(blob, ignored -> { }).materialize(
                ObjectTrustContext.localTrusted(
                        MasterRunAttemptFenceTest.class.getClassLoader()), ignored -> { });
    }

    private static PlanGraph emptyWholeGraph() {
        DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
        return new PlanGraph(Arrays.asList(
                PlanNode.source(meta, Collections.emptyList()),
                PlanNode.sink(meta, 0, new DiscardSink(meta)).build()));
    }

    private static PlanGraph genericDag() {
        DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
        return new PlanGraph(Arrays.asList(
                PlanNode.source(meta, Collections.emptyList()),
                PlanNode.unary("distinct", meta, 0)
                        .listParam("keys", Collections.singletonList("v")).build(),
                PlanNode.sink(meta, 1, new DiscardSink(meta)).build()));
    }
}
