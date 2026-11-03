package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.EphemeralSubmitTimeoutPolicy;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** the request execution budget starts before admission and assignment. */
class MasterEphemeralPreExecutionDeadlineTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    @Timeout(value = 8, unit = TimeUnit.SECONDS)
    void admissionWaitCannotOutliveTheRequestExecutionDeadline() throws Exception {
        String admitKey = "rowweave.cluster.admitTimeoutMs";
        String downgradeKey = "rowweave.cluster.downgrade";
        String previousAdmit = System.setProperty(admitKey, "3000");
        String previousDowngrade = System.setProperty(downgradeKey, "false");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<RpcMessage> firstAssignment = new AtomicReference<>();
        CountDownLatch firstAssigned = new CountDownLatch(1);
        String worker = "admission-deadline-worker";
        String host = "admission-deadline-host";
        int port = 62_042;
        String incarnation = "inc-admission-deadline";
        try (MasterServer master = new MasterServer(
                60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration endpoint =
                    InProcessRpcRegistry.register(host, port, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            firstAssignment.compareAndSet(null, request);
                            firstAssigned.countDown();
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        worker, host, port, 1, -1, incarnation));
                Future<RpcMessage> first = executor.submit(() ->
                        master.handler().apply(RpcMessage.submitJob(
                                graph().toBytes(), "slot-owner",
                                EphemeralSubmitTimeoutPolicy.Budget.of(
                                        5_000L, 1_000L))));
                assertTrue(firstAssigned.await(2, TimeUnit.SECONDS));

                RpcMessage timedOut;
                long elapsedMs;
                try {
                    long started = System.nanoTime();
                    timedOut = master.handler().apply(RpcMessage.submitJob(
                            graph().toBytes(), "admission-timeout",
                            EphemeralSubmitTimeoutPolicy.Budget.of(
                                    1_000L, 1_000L)));
                    elapsedMs = TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - started);
                } finally {
                    RpcMessage assignment = firstAssignment.get();
                    if (assignment != null) {
                        master.handler().apply(RpcMessage.stageReport(
                                assignment.workerId(), assignment.stageId(), true,
                                1L, 1L, null, incarnation,
                                assignment.runAttemptFence()));
                    }
                }
                assertTrue(first.get(2, TimeUnit.SECONDS).success());

                assertFalse(timedOut.success());
                assertTrue(timedOut.errorMessage().startsWith(
                        "CLUSTER_JOB_EXECUTION_TIMEOUT"), timedOut.errorMessage());
                assertTrue(elapsedMs >= 800L && elapsedMs < 1_800L,
                        "admission must use the request deadline: " + elapsedMs + "ms");

                assertEquals(0, master.activeStageTrackerCount());
                assertEquals(0, master.activeStageAssignmentCount());
            } finally {
                endpoint.close();
            }
        } finally {
            executor.shutdownNow();
            restore(admitKey, previousAdmit);
            restore(downgradeKey, previousDowngrade);
        }
    }

    @Test
    @Timeout(value = 8, unit = TimeUnit.SECONDS)
    void assignmentRpcCannotOutliveTheRequestExecutionDeadline() {
        AtomicInteger cancellations = new AtomicInteger();
        String worker = "assignment-deadline-worker";
        String host = "assignment-deadline-host";
        int port = 62_043;
        String incarnation = "inc-assignment-deadline";
        try (MasterServer master = new MasterServer(
                60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration endpoint =
                    InProcessRpcRegistry.register(host, port, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            try {
                                Thread.sleep(2_500L);
                            } catch (InterruptedException ignored) {
                                // A remote handler is not canceled by the caller socket timeout.
                            }
                        } else if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                            cancellations.incrementAndGet();
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        worker, host, port, 1, -1, incarnation));
                long started = System.nanoTime();

                RpcMessage timedOut = master.handler().apply(RpcMessage.submitJob(
                        graph().toBytes(), "assignment-timeout",
                        EphemeralSubmitTimeoutPolicy.Budget.of(
                                1_000L, 1_000L)));
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - started);

                assertFalse(timedOut.success());
                assertTrue(timedOut.errorMessage().startsWith(
                        "CLUSTER_JOB_EXECUTION_TIMEOUT"), timedOut.errorMessage());
                assertTrue(elapsedMs >= 800L && elapsedMs < 1_800L,
                        "ASSIGN RPC must use the request deadline: " + elapsedMs + "ms");
                assertEquals(1, cancellations.get());
                assertEquals(0, master.activeStageTrackerCount());
                assertEquals(0, master.activeStageAssignmentCount());
            } finally {
                endpoint.close();
            }
        }
    }

    @Test
    @Timeout(value = 8, unit = TimeUnit.SECONDS)
    void portSetupCannotOutliveTheRequestExecutionDeadline() {
        AtomicInteger assignments = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        String worker = "port-deadline-worker";
        String host = "port-deadline-host";
        int port = 62_044;
        String incarnation = "inc-port-deadline";
        try (MasterServer master = new MasterServer(
                60_000L, 0, new InProcessRegistry())
                .setObjectReuse(false)) {
            InProcessRpcRegistry.Registration endpoint =
                    InProcessRpcRegistry.register(host, port, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            assignments.incrementAndGet();
                            // Deliberately omit PORT_REPORT: the setup barrier must use
                            // the request budget instead of the independent 60s default.
                        } else if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                            cancellations.incrementAndGet();
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        worker, host, port, 4, -1, incarnation));
                long started = System.nanoTime();

                RpcMessage timedOut = master.handler().apply(RpcMessage.submitJob(
                        genericDag().toBytes(), "port-timeout",
                        EphemeralSubmitTimeoutPolicy.Budget.of(
                                1_000L, 1_000L)));
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - started);

                assertFalse(timedOut.success());
                assertTrue(timedOut.errorMessage().startsWith(
                        "CLUSTER_JOB_EXECUTION_TIMEOUT"), timedOut.errorMessage());
                assertTrue(elapsedMs >= 800L && elapsedMs < 1_800L,
                        "PORT_REPORT setup must use the request deadline: "
                                + elapsedMs + "ms");
                assertEquals(1, assignments.get());
                assertEquals(1, cancellations.get());
                assertEquals(0, master.activeStageTrackerCount());
                assertEquals(0, master.activeStageAssignmentCount());
            } finally {
                endpoint.close();
            }
        }
    }

    private static PlanGraph graph() {
        DataRow row = new DataRow(1);
        row.set(0, 1);
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row)),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()));
    }

    private static PlanGraph genericDag() {
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.unary("distinct", META, 0)
                        .listParam("keys", Collections.singletonList("id"))
                        .build(),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()));
    }

    private static void restore(String key, String previous) {
        if (previous == null) System.clearProperty(key);
        else System.setProperty(key, previous);
    }
}
