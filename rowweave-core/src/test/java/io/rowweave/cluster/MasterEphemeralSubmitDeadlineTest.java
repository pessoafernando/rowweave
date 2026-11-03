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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** a live Worker cannot keep an ephemeral request alive beyond its budget. */
class MasterEphemeralSubmitDeadlineTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));
    private static final String WORKER_ID = "live-nonterminal-worker";
    private static final String WORKER_HOST = "live-nonterminal-host";
    private static final int WORKER_PORT = 62_041;
    private static final String INCARNATION = "inc-live-nonterminal";

    @Test
    @Timeout(value = 6, unit = TimeUnit.SECONDS)
    void explicitRequestDeadlineCancelsLiveNonterminalStageAndReleasesNextAdmission()
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            long started = System.nanoTime();
            RpcMessage timeout = fixture.master.handler().apply(RpcMessage.submitJob(
                    graph().toBytes(), "timeout-attempt",
                    EphemeralSubmitTimeoutPolicy.Budget.of(1_000L, 1_000L)));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

            assertFalse(timeout.success());
            assertTrue(timeout.errorMessage().startsWith("CLUSTER_JOB_EXECUTION_TIMEOUT"),
                    timeout.errorMessage());
            assertTrue(elapsedMs >= 800L && elapsedMs < 3_500L,
                    "execution deadline and bounded cleanup must converge before client read: "
                            + elapsedMs + "ms");
            assertEquals(1, fixture.cancellations.get());
            assertEquals(0, fixture.master.activeStageTrackerCount());
            assertEquals(0, fixture.master.activeStageAssignmentCount());

            fixture.completeAssignments.set(true);
            RpcMessage next = fixture.master.handler().apply(RpcMessage.submitJob(
                    graph().toBytes(), "next-attempt",
                    EphemeralSubmitTimeoutPolicy.Budget.of(1_000L, 1_000L)));
            assertTrue(next.success(), next.errorMessage());
            assertEquals(1L, next.rowCount());
        }
    }

    @Test
    @Timeout(value = 6, unit = TimeUnit.SECONDS)
    void legacyRequestUsesFiniteServerDefaultInsteadOfInfiniteLivenessWait() throws Exception {
        String executionKey = "rowweave.cluster.ephemeralJobTimeoutMs";
        String cleanupKey = "rowweave.cluster.ephemeralCleanupTimeoutMs";
        String previousExecution = System.setProperty(executionKey, "1000");
        String previousCleanup = System.setProperty(cleanupKey, "1000");
        try (Fixture fixture = new Fixture()) {
            RpcMessage timeout = fixture.master.handler().apply(
                    RpcMessage.submitJob(graph().toBytes(), "legacy-attempt"));

            assertFalse(timeout.success());
            assertTrue(timeout.errorMessage().startsWith("CLUSTER_JOB_EXECUTION_TIMEOUT"),
                    timeout.errorMessage());
            assertEquals(1, fixture.cancellations.get());
            assertEquals(0, fixture.master.activeStageTrackerCount());
            assertEquals(0, fixture.master.activeStageAssignmentCount());
        } finally {
            restore(executionKey, previousExecution);
            restore(cleanupKey, previousCleanup);
        }
    }

    @Test
    @Timeout(value = 6, unit = TimeUnit.SECONDS)
    void rejectedCancellationReportsCleanupIncompleteAndQuarantinesExactWorker() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.rejectCancellations.set(true);

            RpcMessage timeout = fixture.master.handler().apply(RpcMessage.submitJob(
                    graph().toBytes(), "rejected-cancel-attempt",
                    EphemeralSubmitTimeoutPolicy.Budget.of(1_000L, 1_000L)));

            assertFalse(timeout.success());
            assertTrue(timeout.errorMessage().startsWith(
                            "CLUSTER_JOB_TIMEOUT_CLEANUP_INCOMPLETE"),
                    timeout.errorMessage());
            assertEquals(1, fixture.cancellations.get());
            assertTrue(fixture.master.workerQuarantined(WORKER_ID));
            assertEquals(0, fixture.master.activeStageTrackerCount());
            assertEquals(0, fixture.master.activeStageAssignmentCount());
        }
    }

    @Test
    @Timeout(value = 6, unit = TimeUnit.SECONDS)
    void blockedCancellationCannotExtendTheTerminalDeadline() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.blockCancellations.set(true);
            long started = System.nanoTime();

            RpcMessage timeout = fixture.master.handler().apply(RpcMessage.submitJob(
                    graph().toBytes(), "blocked-cancel-attempt",
                    EphemeralSubmitTimeoutPolicy.Budget.of(1_000L, 1_000L)));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

            assertFalse(timeout.success());
            assertTrue(timeout.errorMessage().startsWith(
                            "CLUSTER_JOB_TIMEOUT_CLEANUP_INCOMPLETE"),
                    timeout.errorMessage());
            assertTrue(elapsedMs >= 1_800L && elapsedMs < 3_500L,
                    "one terminal deadline must cap the blocked control call: "
                            + elapsedMs + "ms");
            assertTrue(fixture.master.workerQuarantined(WORKER_ID));
            assertEquals(0, fixture.master.activeStageTrackerCount());
            assertEquals(0, fixture.master.activeStageAssignmentCount());
        }
    }

    private static PlanGraph graph() {
        DataRow row = new DataRow(1);
        row.set(0, 7L);
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row)),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()));
    }

    private static void restore(String key, String previous) {
        if (previous == null) System.clearProperty(key);
        else System.setProperty(key, previous);
    }

    private static final class Fixture implements AutoCloseable {
        private final AtomicBoolean completeAssignments = new AtomicBoolean();
        private final AtomicBoolean rejectCancellations = new AtomicBoolean();
        private final AtomicBoolean blockCancellations = new AtomicBoolean();
        private final AtomicInteger cancellations = new AtomicInteger();
        private final java.util.concurrent.CountDownLatch releaseCancellation =
                new java.util.concurrent.CountDownLatch(1);
        private final MasterServer master =
                new MasterServer(60_000L, 0, new InProcessRegistry());
        private final InProcessRpcRegistry.Registration workerEndpoint;
        private final ScheduledExecutorService heartbeats =
                Executors.newSingleThreadScheduledExecutor();

        private Fixture() {
            workerEndpoint = InProcessRpcRegistry.register(
                    WORKER_HOST, WORKER_PORT, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE
                                && completeAssignments.get()) {
                            master.handler().apply(RpcMessage.stageReport(
                                    request.workerId(), request.stageId(), true,
                                    1L, 1L, null, INCARNATION,
                                    request.runAttemptFence()));
                        } else if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                            cancellations.incrementAndGet();
                            while (blockCancellations.get()
                                    && releaseCancellation.getCount() > 0L) {
                                try {
                                    releaseCancellation.await(20L, TimeUnit.MILLISECONDS);
                                } catch (InterruptedException ignored) {
                                    // Simulate a control handler that ignores caller interruption.
                                }
                            }
                            if (rejectCancellations.get()) {
                                return RpcMessage.jobResult(
                                        false, 0, "stage cancellation timed out");
                            }
                        }
                        return RpcMessage.ack();
                    });
            master.handler().apply(RpcMessage.registerWorker(
                    WORKER_ID, WORKER_HOST, WORKER_PORT, 1, -1, INCARNATION));
            heartbeats.scheduleAtFixedRate(() -> {
                try {
                    master.handler().apply(RpcMessage.heartbeat(
                            WORKER_ID, completeAssignments.get() ? 1L : 0L, INCARNATION));
                } catch (RuntimeException ignoredDuringClose) {
                    // Fixture close races are irrelevant; liveness during submit is the tested property.
                }
            }, 0L, 100L, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            releaseCancellation.countDown();
            heartbeats.shutdownNow();
            workerEndpoint.close();
            master.close();
        }
    }
}
