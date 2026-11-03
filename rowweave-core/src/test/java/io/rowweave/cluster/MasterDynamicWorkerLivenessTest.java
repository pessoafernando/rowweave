package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MasterDynamicWorkerLivenessTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    @Test
    void sameEndpointReplacementIncarnationCannotReceiveAlreadyPlannedDag() throws Exception {
        String property = "rowweave.cluster.assign.parallelism";
        String previous = System.getProperty(property);
        System.setProperty(property, "1");
        String host = "same-endpoint-worker";
        int port = 61_030;
        int dataPort = 62_030;
        CountDownLatch firstAssignmentEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstAssignment = new CountDownLatch(1);
        AtomicInteger assignments = new AtomicInteger();
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                if (assignments.incrementAndGet() == 1) {
                    firstAssignmentEntered.countDown();
                    try {
                        releaseFirstAssignment.await(2, TimeUnit.SECONDS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            return RpcMessage.ack();
        });
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (MasterServer master = new MasterServer(60_000L)) {
            master.handler().apply(RpcMessage.registerWorker(
                    "w", host, port, 8, dataPort, "incarnation-old"));
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Collections.emptyList()),
                    PlanNode.unary("distinct", META, 0)
                            .listParam("keys", Collections.singletonList("v")).build(),
                    PlanNode.sink(META, 1, new DiscardSink(META)).build()));
            Future<RpcMessage> result = submitter.submit(
                    () -> master.handler().apply(RpcMessage.submitJob(graph.toBytes())));
            assertTrue(firstAssignmentEntered.await(2, TimeUnit.SECONDS));

            master.handler().apply(RpcMessage.registerWorker(
                    "w", host, port, 8, dataPort, "incarnation-new"));
            releaseFirstAssignment.countDown();

            assertFalse(result.get(2, TimeUnit.SECONDS).success());
            assertEquals(1, assignments.get(),
                    "when the endpoint triple is reused with the same values, a new incarnation must still not receive later assignments of the old plan");
        } finally {
            releaseFirstAssignment.countDown();
            submitter.shutdownNow();
            endpointRegistration.close();
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    @Test
    void staleSameEndpointMessagesCannotMutateReplacementOrCompleteTracker() throws Exception {
        String host = "same-report-worker";
        int port = 61_031;
        int dataPort = 62_031;
        CountDownLatch assigned = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicLong stageId = new java.util.concurrent.atomic.AtomicLong();
        AtomicReference<io.rowweave.run.RunAttemptFence> stageFence = new AtomicReference<>();
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                stageId.set(request.stageId());
                stageFence.set(request.runAttemptFence());
                assigned.countDown();
            }
            return RpcMessage.ack();
        });
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (MasterServer master = new MasterServer(60_000L)) {
            master.handler().apply(RpcMessage.registerWorker(
                    "w", host, port, 4, dataPort, "incarnation-current"));
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Collections.emptyList()),
                    PlanNode.sink(META, 0, new DiscardSink(META)).build()));
            Future<RpcMessage> result = submitter.submit(
                    () -> master.handler().apply(RpcMessage.submitJob(graph.toBytes())));
            assertTrue(assigned.await(2, TimeUnit.SECONDS));

            master.handler().apply(RpcMessage.stageReport(
                    "w", stageId.get(), true, 0L, 0L, null, "incarnation-old", stageFence.get()));
            Thread.sleep(100L);
            assertFalse(result.isDone(), "a report from an old incarnation must not complete the current stage tracker");

            master.handler().apply(RpcMessage.stageReport(
                    "w", stageId.get(), true, 0L, 0L, null, "incarnation-current", stageFence.get()));
            assertTrue(result.get(2, TimeUnit.SECONDS).success());

            master.quarantineWorker("w");
            master.handler().apply(RpcMessage.heartbeat("w", 4L, "incarnation-old"));
            assertTrue(master.workerQuarantined("w"), "an old heartbeat must not restore capacity to the replacement");
            master.handler().apply(RpcMessage.deregisterWorker(
                    "w", host, port, 4, dataPort, "incarnation-old"));
            assertNotNull(master.worker("w"), "an old departure must not remove the replacement on the same endpoint");
        } finally {
            submitter.shutdownNow();
            endpointRegistration.close();
        }
    }

    @Test
    void staleDepartureCannotQuarantineReplacementEndpointGeneration() {
        try (MasterServer master = new MasterServer(60_000L)) {
            master.handler().apply(RpcMessage.registerWorker("w", "127.0.0.1", 61_010, 2, 62_010));
            master.handler().apply(RpcMessage.registerWorker("w", "127.0.0.1", 61_010, 2, 62_011));

            RpcMessage response = master.handler().apply(
                    RpcMessage.deregisterWorker("w", "127.0.0.1", 61_010, 2, 62_010));

            assertNull(response, "DEREGISTER is one-way and must not leave an orphan ACK that poisons a reused connection");
            assertNotNull(master.worker("w"));
            assertEquals(62_011, master.worker("w").dataPort());
            assertFalse(master.workerQuarantined("w"),
                    "a late departure from an old generation must ACK idempotently and must not kill the replacement");
        }
    }

    @Test
    void lateSameGenerationHeartbeatCannotResurrectDepartedEndpoint() {
        try (MasterServer master = new MasterServer(60_000L)) {
            master.handler().apply(RpcMessage.registerWorker("w", "127.0.0.1", 61_013, 2, 62_013));
            master.handler().apply(
                    RpcMessage.deregisterWorker("w", "127.0.0.1", 61_013, 2, 62_013));

            master.handler().apply(RpcMessage.registerWorker("w", "127.0.0.1", 61_013, 2, 62_013));
            master.handler().apply(RpcMessage.heartbeat("w", 2));

            assertTrue(master.workerQuarantined("w"),
                    "a cooperative departure is terminal for that endpoint generation, so a late heartbeat must not revive it through ordinary quarantine handling");
        }
    }

    @Test
    void matchingDepartureImmediatelyFailsInFlightStageBarrier() throws Exception {
        String host = "departing-fake-worker";
        int port = 61_012;
        CountDownLatch assigned = new CountDownLatch(1);
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                assigned.countDown();
            }
            return RpcMessage.ack();
        });
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (MasterServer master = new MasterServer(60_000L)) {
            master.handler().apply(RpcMessage.registerWorker("w", host, port, 2, 62_012));
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Collections.emptyList()),
                    PlanNode.sink(META, 0, new DiscardSink(META)).build()));
            Future<RpcMessage> result = submitter.submit(
                    () -> master.handler().apply(RpcMessage.submitJob(graph.toBytes())));
            assertTrue(assigned.await(2, TimeUnit.SECONDS));

            master.handler().apply(RpcMessage.deregisterWorker("w", host, port, 2, 62_012));

            RpcMessage completed = result.get(2, TimeUnit.SECONDS);
            assertFalse(completed.success());
            assertTrue(completed.errorMessage().contains("depart"), completed.errorMessage());
        } finally {
            submitter.shutdownNow();
            endpointRegistration.close();
        }
    }

    @Test
    void replacementGenerationCannotReceiveAssignmentsFromAlreadyPlannedDag() throws Exception {
        String property = "rowweave.cluster.assign.parallelism";
        String previous = System.getProperty(property);
        System.setProperty(property, "1");
        String oldHost = "planned-old-worker";
        String newHost = "replacement-worker";
        int oldPort = 61_020;
        int newPort = 61_021;
        CountDownLatch oldAssignmentEntered = new CountDownLatch(1);
        CountDownLatch releaseOldAssignment = new CountDownLatch(1);
        AtomicInteger replacementAssignments = new AtomicInteger();
        InProcessRpcRegistry.Registration oldRegistration = InProcessRpcRegistry.register(oldHost, oldPort, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                oldAssignmentEntered.countDown();
                try {
                    releaseOldAssignment.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return RpcMessage.ack();
        });
        InProcessRpcRegistry.Registration newRegistration = InProcessRpcRegistry.register(newHost, newPort, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                replacementAssignments.incrementAndGet();
            }
            return RpcMessage.ack();
        });
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        MasterServer master = null;
        try {
            master = new MasterServer(60_000L);
            master.handler().apply(RpcMessage.registerWorker("w", oldHost, oldPort, 8, 62_020));
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Collections.emptyList()),
                    PlanNode.unary("distinct", META, 0)
                            .listParam("keys", Collections.singletonList("v")).build(),
                    PlanNode.sink(META, 1, new DiscardSink(META)).build()));
            MasterServer submittedMaster = master;
            Future<RpcMessage> result = submitter.submit(
                    () -> submittedMaster.handler().apply(RpcMessage.submitJob(graph.toBytes())));
            assertTrue(oldAssignmentEntered.await(2, TimeUnit.SECONDS), "the old generation should receive the first assignment of this Job");

            master.handler().apply(RpcMessage.registerWorker("w", newHost, newPort, 8, 62_021));
            releaseOldAssignment.countDown();
            long deadline = System.currentTimeMillis() + 1_000L;
            while (replacementAssignments.get() == 0 && !result.isDone()
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(10L);
            }

            assertEquals(0, replacementAssignments.get(),
                    "a replacement registered after planning completed must not take later assignments of the old DAG");
        } finally {
            releaseOldAssignment.countDown();
            if (master != null) {
                master.close();
            }
            submitter.shutdownNow();
            oldRegistration.close();
            newRegistration.close();
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    @Test
    void oldGenerationFailureCannotCancelStagesOnReplacementEndpoint() throws Exception {
        String oldHost = "cancel-old-worker";
        String newHost = "cancel-replacement-worker";
        int oldPort = 61_022;
        int newPort = 61_023;
        CopyOnWriteArrayList<Long> oldStageIds = new CopyOnWriteArrayList<>();
        java.util.concurrent.ConcurrentHashMap<Long, io.rowweave.run.RunAttemptFence> oldStageFences =
                new java.util.concurrent.ConcurrentHashMap<>();
        AtomicInteger replacementCancels = new AtomicInteger();
        InProcessRpcRegistry.Registration oldRegistration = InProcessRpcRegistry.register(oldHost, oldPort, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                oldStageIds.add(request.stageId());
                oldStageFences.put(request.stageId(), request.runAttemptFence());
            }
            return RpcMessage.ack();
        });
        InProcessRpcRegistry.Registration newRegistration = InProcessRpcRegistry.register(newHost, newPort, request -> {
            if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                replacementCancels.incrementAndGet();
            }
            return RpcMessage.ack();
        });
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (MasterServer master = new MasterServer(60_000L)) {
            master.handler().apply(RpcMessage.registerWorker("w", oldHost, oldPort, 8, 62_022));
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Collections.emptyList()),
                    PlanNode.unary("distinct", META, 0)
                            .listParam("keys", Collections.singletonList("v")).build(),
                    PlanNode.sink(META, 1, new DiscardSink(META)).build()));
            Future<RpcMessage> result = submitter.submit(
                    () -> master.handler().apply(RpcMessage.submitJob(graph.toBytes())));
            long deadline = System.currentTimeMillis() + 2_000L;
            while (oldStageIds.size() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10L);
            }
            assertTrue(oldStageIds.size() >= 2, "the old generation must hold at least two stages for the cascading cancellation ownership check to be meaningful");

            master.handler().apply(RpcMessage.registerWorker("w", newHost, newPort, 8, 62_023));
            master.handler().apply(RpcMessage.stageReport(
                    "w", oldStageIds.get(0), false, 0L, 0L, "old generation failed", null,
                    oldStageFences.get(oldStageIds.get(0))));

            RpcMessage completed = result.get(2, TimeUnit.SECONDS);
            assertFalse(completed.success());
            assertEquals(0, replacementCancels.get(),
                    "once the stage owner of the old Job is invalid, CANCEL_STAGE must not be sent to a replacement with the same id");
        } finally {
            submitter.shutdownNow();
            oldRegistration.close();
            newRegistration.close();
        }
    }

    @Test
    void replacementGenerationCannotMaskLostOwnerDuringJobWait() throws Exception {
        String oldHost = "await-old-worker";
        String newHost = "await-replacement-worker";
        int oldPort = 61_024;
        int newPort = 61_025;
        AtomicInteger oldAssignments = new AtomicInteger();
        InProcessRpcRegistry.Registration oldRegistration = InProcessRpcRegistry.register(oldHost, oldPort, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                oldAssignments.incrementAndGet();
            }
            return RpcMessage.ack();
        });
        InProcessRpcRegistry.Registration newRegistration = InProcessRpcRegistry.register(
                newHost, newPort, request -> RpcMessage.ack());
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (MasterServer master = new MasterServer(60_000L)) {
            master.handler().apply(RpcMessage.registerWorker("w", oldHost, oldPort, 8, 62_024));
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Collections.emptyList()),
                    PlanNode.unary("distinct", META, 0)
                            .listParam("keys", Collections.singletonList("v")).build(),
                    PlanNode.sink(META, 1, new DiscardSink(META)).build()));
            Future<RpcMessage> result = submitter.submit(
                    () -> master.handler().apply(RpcMessage.submitJob(graph.toBytes())));
            long deadline = System.currentTimeMillis() + 2_000L;
            while (oldAssignments.get() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10L);
            }
            assertTrue(oldAssignments.get() >= 2);

            master.handler().apply(RpcMessage.registerWorker("w", newHost, newPort, 8, 62_025));

            RpcMessage completed = result.get(2, TimeUnit.SECONDS);
            assertFalse(completed.success(), "the replacement has no state for the old stage, so the old Job must fail fast");
            assertTrue(completed.errorMessage().contains("worker lost"), completed.errorMessage());
        } finally {
            submitter.shutdownNow();
            oldRegistration.close();
            newRegistration.close();
        }
    }

    @Test
    void lateOldCancelFailureCannotQuarantineReplacementGeneration() throws Exception {
        String oldHost = "quarantine-old-worker";
        String newHost = "quarantine-replacement-worker";
        int oldPort = 61_026;
        int newPort = 61_027;
        CopyOnWriteArrayList<Long> oldStageIds = new CopyOnWriteArrayList<>();
        java.util.concurrent.ConcurrentHashMap<Long, io.rowweave.run.RunAttemptFence> oldStageFences =
                new java.util.concurrent.ConcurrentHashMap<>();
        CountDownLatch cancelEntered = new CountDownLatch(1);
        CountDownLatch releaseCancel = new CountDownLatch(1);
        InProcessRpcRegistry.Registration oldRegistration = InProcessRpcRegistry.register(oldHost, oldPort, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                oldStageIds.add(request.stageId());
                oldStageFences.put(request.stageId(), request.runAttemptFence());
                return RpcMessage.ack();
            }
            if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                cancelEntered.countDown();
                try {
                    releaseCancel.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                throw new RuntimeException("old endpoint cancel failed");
            }
            return RpcMessage.ack();
        });
        InProcessRpcRegistry.Registration newRegistration = InProcessRpcRegistry.register(
                newHost, newPort, request -> RpcMessage.ack());
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (MasterServer master = new MasterServer(60_000L)) {
            master.handler().apply(RpcMessage.registerWorker("w", oldHost, oldPort, 8, 62_026));
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Collections.emptyList()),
                    PlanNode.unary("distinct", META, 0)
                            .listParam("keys", Collections.singletonList("v")).build(),
                    PlanNode.sink(META, 1, new DiscardSink(META)).build()));
            Future<RpcMessage> result = submitter.submit(
                    () -> master.handler().apply(RpcMessage.submitJob(graph.toBytes())));
            long deadline = System.currentTimeMillis() + 2_000L;
            while (oldStageIds.size() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10L);
            }
            assertTrue(oldStageIds.size() >= 2);

            master.handler().apply(RpcMessage.stageReport(
                    "w", oldStageIds.get(0), false, 0L, 0L, "trigger cascade cancel", null,
                    oldStageFences.get(oldStageIds.get(0))));
            assertTrue(cancelEntered.await(2, TimeUnit.SECONDS));
            master.handler().apply(RpcMessage.registerWorker("w", newHost, newPort, 8, 62_027));
            releaseCancel.countDown();

            RpcMessage completed = result.get(2, TimeUnit.SECONDS);
            assertFalse(completed.success());
            assertFalse(master.workerQuarantined("w"),
                    "a late cancellation failure from the old endpoint must not quarantine a replacement with the same id");
            assertNotNull(master.worker("w"));
            assertEquals(newPort, master.worker("w").port());
            assertEquals(62_027, master.worker("w").dataPort());
        } finally {
            releaseCancel.countDown();
            submitter.shutdownNow();
            oldRegistration.close();
            newRegistration.close();
        }
    }

    @Test
    void currentGenerationCancelFailureStillQuarantinesWorker() throws Exception {
        String host = "quarantine-current-worker";
        int port = 61_028;
        CopyOnWriteArrayList<Long> stageIds = new CopyOnWriteArrayList<>();
        java.util.concurrent.ConcurrentHashMap<Long, io.rowweave.run.RunAttemptFence> stageFences =
                new java.util.concurrent.ConcurrentHashMap<>();
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
            if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                stageIds.add(request.stageId());
                stageFences.put(request.stageId(), request.runAttemptFence());
                return RpcMessage.ack();
            }
            if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                throw new RuntimeException("current endpoint cancel failed");
            }
            return RpcMessage.ack();
        });
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (MasterServer master = new MasterServer(60_000L)) {
            master.handler().apply(RpcMessage.registerWorker("w", host, port, 8, 62_028));
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Collections.emptyList()),
                    PlanNode.unary("distinct", META, 0)
                            .listParam("keys", Collections.singletonList("v")).build(),
                    PlanNode.sink(META, 1, new DiscardSink(META)).build()));
            Future<RpcMessage> result = submitter.submit(
                    () -> master.handler().apply(RpcMessage.submitJob(graph.toBytes())));
            long deadline = System.currentTimeMillis() + 2_000L;
            while (stageIds.size() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10L);
            }
            assertTrue(stageIds.size() >= 2);

            master.handler().apply(RpcMessage.stageReport(
                    "w", stageIds.get(0), false, 0L, 0L, "trigger current cancel failure", null,
                    stageFences.get(stageIds.get(0))));

            assertFalse(result.get(2, TimeUnit.SECONDS).success());
            assertTrue(master.workerQuarantined("w"),
                    "a cancellation confirmation failure for the current generation must stay fail-safe and quarantine");
        } finally {
            submitter.shutdownNow();
            endpointRegistration.close();
        }
    }

    @Test
    void periodicRegisterReplacementIsStillDetectedAsLost() throws Exception {
        try (MasterServer master = new MasterServer(80L)) {
            master.handler().apply(RpcMessage.registerWorker("w", "127.0.0.1", 61_001, 2));
            master.startHeartbeatMonitor(10L);

            master.handler().apply(RpcMessage.registerWorker("w", "127.0.0.1", 61_001, 2));

            awaitQuarantined(master, "w", 2_000L);
            assertNull(master.worker("w"), "a lost generation must be removed from the local active table by identity");
        }
    }

    @Test
    void automaticSweepExcludesLostWorkerFromNewSubmit() throws Exception {
        try (MasterServer master = new MasterServer(60L)) {
            master.handler().apply(RpcMessage.registerWorker("dead", "127.0.0.1", 61_002, 2));

            awaitQuarantined(master, "dead", 2_000L);

            PlanNode source = PlanNode.source(META, Collections.emptyList());
            PlanNode sink = PlanNode.sink(META, 0, new DiscardSink(META)).build();
            PlanGraph graph = new PlanGraph(Arrays.asList(source, sink));
            RpcMessage result = master.handler().apply(RpcMessage.submitJob(graph.toBytes()));

            assertFalse(result.success());
            assertEquals("no registered worker", result.errorMessage(),
                    "a lost endpoint must be excluded from the scheduling candidates before connect or assign");
        }
    }

    @Test
    void closeStopsAutomaticSweepWithoutLateCallback() throws Exception {
        CopyOnWriteArrayList<String> lost = new CopyOnWriteArrayList<>();
        MasterServer master = new MasterServer(60L);
        master.setOnWorkerLost(lost::add);
        master.handler().apply(RpcMessage.registerWorker("w", "127.0.0.1", 61_003, 2));

        master.close();
        Thread.sleep(250L);

        assertTrue(lost.isEmpty(), "no automatic liveness callback may fire after close returns");
    }

    private static void awaitQuarantined(MasterServer master, String workerId, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!master.workerQuarantined(workerId) && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(master.workerQuarantined(workerId), "a stale Worker should reach quarantine within a bounded time");
    }
}

