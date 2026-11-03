package io.rowweave.cluster;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.ClockedRunServiceRegistry;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunDeadlineAuthority;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterActiveRunDeadlineTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));
    private static volatile CountDownLatch inputOpened;
    private static volatile CountDownLatch releaseInput;

    private static final class BlockingInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final BatchSourceSplitDescriptor descriptor =
                new BatchSourceSplitDescriptor(
                        1, "test-blocking", 1, "active-deadline-source",
                        SchemaFingerprint.of(META), "active-deadline-snapshot",
                        BatchSourceSplitDescriptor.ReplayGuarantee
                                .EXACT_ORDERED_FROM_START);

        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() {
            return descriptor;
        }

        @Override
        public Iterator<DataRow> iterator() {
            inputOpened.countDown();
            try {
                releaseInput.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "blocking input interrupted", interrupted);
            }
            return Collections.emptyIterator();
        }
    }

    @Test
    void alreadyDueRunFailsFromReadyWithoutCreatingAttempt() {
        AtomicLong clock = new AtomicLong(100L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            RpcMessage result = master.handler().apply(RpcMessage.submitRun(
                    envelope("ready-timeout", "attempt", "publication",
                            100L, emptyDiscardGraph())));

            assertFalse(result.success());
            RunSnapshot terminal = registry.runStateStore().get("ready-timeout");
            assertEquals(RunStatus.FAILED, terminal.status());
            assertEquals(0, terminal.attemptCount());
            assertNull(terminal.currentAttempt());
            assertEquals(1L, registry.runStateStore().events(
                    "ready-timeout", 0L, 10).stream()
                    .filter(event -> event.type()
                            == io.rowweave.run.ExecutionEvent.Type.TIMEOUT_REQUESTED)
                    .count());
        }
    }

    @Test
    void runningDiscardDeadlineCancelsDrainsAndEndsTimedOut(
            @TempDir Path temp) throws Exception {
        resetBlockingInput();
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        File spill = temp.resolve("deadline-worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "deadline-worker", 8, "127.0.0.1", master.port(), spill)) {
            Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                    RpcMessage.submitRun(envelope(
                            "running-timeout", "attempt", "publication",
                            100L, blockingGraph(new DiscardSink(META))))));
            assertTrue(inputOpened.await(5, TimeUnit.SECONDS));
            clock.set(100L);

            RpcMessage result = submitted.get(5, TimeUnit.SECONDS);

            assertFalse(result.success());
            RunSnapshot terminal = registry.runStateStore().get("running-timeout");
            assertEquals(RunStatus.FAILED, terminal.status());
            assertEquals(AttemptStatus.TIMED_OUT,
                    terminal.currentAttempt().status());
            assertNull(registry.durableStageAssignmentAuthority().assignmentSnapshot(
                    new RunAttemptFence("running-timeout", "attempt", 1L)));
            assertEquals(0, worker.runningStageCount());
            assertEquals(0, master.activeStageAssignmentCount());
        } finally {
            releaseInput.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void deadlineDuringDispatchFencesEveryLaterAssignment(@TempDir Path temp)
            throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger assignments = new AtomicInteger();
        CountDownLatch firstAssigned = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        Map<Long, RpcMessage> active = new ConcurrentHashMap<>();
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    "127.0.0.1", 61_194, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            int ordinal = assignments.incrementAndGet();
                            active.put(request.stageId(), request);
                            accept(master, request, "inc-deadline-dispatch");
                            if (ordinal == 1) {
                                firstAssigned.countDown();
                                try {
                                    releaseFirst.await(5, TimeUnit.SECONDS);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                }
                            } else {
                                reportCanceled(master, request);
                            }
                            return RpcMessage.ack();
                        }
                        if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                            reportCanceled(master, active.get(request.stageId()));
                            return RpcMessage.ack();
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", "127.0.0.1", 61_194, 8, -1,
                        "inc-deadline-dispatch"));
                Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                        RpcMessage.submitRun(envelope(
                                "dispatch-timeout", "attempt", "publication",
                                100L, multiStageGraph(new ManifestFileSink(
                                        META, temp.resolve("dispatch-domain").toString(),
                                        "ledger"))))));
                assertTrue(firstAssigned.await(5, TimeUnit.SECONDS));
                clock.set(100L);
                releaseFirst.countDown();

                assertFalse(submitted.get(10, TimeUnit.SECONDS).success());
                assertEquals(1, assignments.get(),
                        "no physical assignment may appear after the timeout CAS");
                RunSnapshot terminal = registry.runStateStore().get(
                        "dispatch-timeout");
                assertEquals(RunStatus.FAILED, terminal.status());
                assertEquals(AttemptStatus.TIMED_OUT,
                        terminal.currentAttempt().status());
                assertEquals("RUN_DEADLINE",
                        registry.durableStageAssignmentAuthority().batchSinkAbort(
                                new RunAttemptFence(
                                        "dispatch-timeout", "attempt", 1L))
                                .reasonCode());
            } finally {
                releaseFirst.countDown();
                endpoint.close();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void manifestDeadlineRecordsAbortAndNeverPublishes(@TempDir Path temp)
            throws Exception {
        resetBlockingInput();
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Path domain = temp.resolve("deadline-manifest-domain");
        File spill = temp.resolve("deadline-manifest-worker").toFile();
        assertTrue(spill.mkdirs());
        RunAttemptFence fence = new RunAttemptFence(
                "manifest-timeout", "attempt", 1L);
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "deadline-manifest-worker", 8, "127.0.0.1",
                     master.port(), spill)) {
            Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                    RpcMessage.submitRun(envelope(
                            fence.runId(), fence.attemptId(), "publication",
                            100L, blockingGraph(new ManifestFileSink(
                                    META, domain.toString(), "ledger"))))));
            assertTrue(inputOpened.await(5, TimeUnit.SECONDS));
            clock.set(100L);

            assertFalse(submitted.get(5, TimeUnit.SECONDS).success());

            BatchSinkAbortRecord abort = registry.durableStageAssignmentAuthority()
                    .batchSinkAbort(fence);
            assertEquals(BatchSinkAbortRecord.State.ABORTED, abort.state());
            assertEquals("RUN_DEADLINE", abort.reasonCode());
            assertNull(new ManifestFileStore(domain).latest());
            RunSnapshot terminal = registry.runStateStore().get(fence.runId());
            assertEquals(RunStatus.FAILED, terminal.status());
            assertEquals(AttemptStatus.TIMED_OUT,
                    terminal.currentAttempt().status());
            assertEquals(0, worker.preparedOutputActiveCount());
        } finally {
            releaseInput.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void missingTerminalEvidenceKeepsDeadlineCancelingAndQuarantinesWorker()
            throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger assignments = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    "127.0.0.1", 61_195, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            assignments.incrementAndGet();
                            accept(master, request, "inc-unresponsive-deadline");
                            return RpcMessage.ack();
                        }
                        if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                            cancellations.incrementAndGet();
                            return RpcMessage.jobResult(
                                    false, 0, "stage cancellation timed out");
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", "127.0.0.1", 61_195, 8, 62_195,
                        "inc-unresponsive-deadline"));
                Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                        RpcMessage.submitRun(envelope(
                                "unresponsive-timeout", "attempt", "publication",
                                100L, emptyDiscardGraph()))));
                long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (assignments.get() == 0 && System.nanoTime() < waitUntil) {
                    Thread.sleep(10L);
                }
                assertTrue(assignments.get() > 0);
                clock.set(100L);

                RpcMessage result = submitted.get(5, TimeUnit.SECONDS);

                assertFalse(result.success());
                assertEquals(io.rowweave.run.DurableRunDisposition.ACCEPTED_NON_TERMINAL,
                        result.durableRunDisposition());
                RunSnapshot pending = registry.runStateStore().get(
                        "unresponsive-timeout");
                assertEquals(RunStatus.CANCELING, pending.status());
                assertEquals(AttemptStatus.CANCELING,
                        pending.currentAttempt().status());
                assertEquals(CancellationCause.TIMEOUT,
                        pending.currentAttempt().cancellationCause());
                assertTrue(cancellations.get() > 0);
                assertTrue(master.workerQuarantined("w"));
            } finally {
                endpoint.close();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void distantDeadlineDoesNotPollAuthorityEveryAwaitTick(@TempDir Path temp)
            throws Exception {
        resetBlockingInput();
        AtomicLong clock = new AtomicLong(99L);
        AtomicInteger probes = new AtomicInteger();
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(() -> {
            probes.incrementAndGet();
            return clock.get();
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        File spill = temp.resolve("distant-worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "distant-worker", 8, "127.0.0.1", master.port(), spill)) {
            Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                    RpcMessage.submitRun(envelope(
                            "distant-deadline", "attempt", "publication",
                            100_000L, blockingGraph(new DiscardSink(META))))));
            assertTrue(inputOpened.await(5, TimeUnit.SECONDS));
            int afterDispatch = probes.get();
            Thread.sleep(1_200L);
            assertEquals(afterDispatch, probes.get(),
                    "an await tick must not keep reading the deadline authority");
            releaseInput.countDown();
            assertTrue(submitted.get(5, TimeUnit.SECONDS).success());
            assertEquals(RunStatus.SUCCEEDED,
                    registry.runStateStore().get("distant-deadline").status());
        } finally {
            releaseInput.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void committingWinsDeadlineRaceAndPublicationRemainsMonotonic(
            @TempDir Path temp) throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch beforePublish = new CountDownLatch(1);
        CountDownLatch releasePublish = new CountDownLatch(1);
        Path domain = temp.resolve("deadline-committing-domain");
        File spill = temp.resolve("deadline-committing-worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "deadline-committing-worker", 8, "127.0.0.1",
                     master.port(), spill)) {
            master.setManifestPublishHooks(() -> {
                beforePublish.countDown();
                try {
                    assertTrue(releasePublish.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }, null);
            Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                    RpcMessage.submitRun(envelope(
                            "deadline-commit-wins", "attempt", "publication",
                            100L, emptyManifestGraph(domain)))));
            assertTrue(beforePublish.await(5, TimeUnit.SECONDS));
            clock.set(100L);

            RunDeadlineResult deadline = ((RunDeadlineAuthority)
                    registry.runStateStore()).evaluateDeadline("deadline-commit-wins");
            assertEquals(RunDeadlineResult.Disposition.TOO_LATE,
                    deadline.disposition());
            releasePublish.countDown();

            assertTrue(submitted.get(10, TimeUnit.SECONDS).success());
            assertEquals(RunStatus.SUCCEEDED,
                    registry.runStateStore().get("deadline-commit-wins").status());
            assertEquals("publication",
                    new ManifestFileStore(domain).latest().attemptId());
            assertNull(registry.durableStageAssignmentAuthority().batchSinkAbort(
                    new RunAttemptFence("deadline-commit-wins", "attempt", 1L)));
        } finally {
            releasePublish.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void userCancellationWinsBeforeDeadlineAndIsNotOverwritten(
            @TempDir Path temp) throws Exception {
        resetBlockingInput();
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        File spill = temp.resolve("user-before-deadline-worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "user-before-deadline-worker", 8, "127.0.0.1",
                     master.port(), spill)) {
            Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                    RpcMessage.submitRun(envelope(
                            "user-before-deadline", "attempt", "publication",
                            100L, blockingGraph(new DiscardSink(META))))));
            assertTrue(inputOpened.await(5, TimeUnit.SECONDS));

            RpcMessage cancellation = master.handler().apply(RpcMessage.cancelRun(
                    "user-before-deadline", "request-user-first"));
            assertTrue(cancellation.success(), cancellation.errorMessage());
            assertEquals("ACCEPTED", cancellation.runCancellationDisposition());
            clock.set(100L);

            assertFalse(submitted.get(5, TimeUnit.SECONDS).success());
            RunSnapshot terminal = registry.runStateStore().get(
                    "user-before-deadline");
            assertEquals(RunStatus.CANCELED, terminal.status());
            assertEquals(AttemptStatus.CANCELED,
                    terminal.currentAttempt().status());
            assertEquals(CancellationCause.USER,
                    terminal.currentAttempt().cancellationCause());
        } finally {
            releaseInput.countDown();
            executor.shutdownNow();
        }
    }

    private static void resetBlockingInput() {
        inputOpened = new CountDownLatch(1);
        releaseInput = new CountDownLatch(1);
    }

    private static byte[] envelope(
            String runId, String attemptId, String publicationId,
            long deadline, PlanGraph graph) {
        RunSubmission submission = new RunSubmission(
                runId, attemptId, publicationId, 1, deadline, 1L);
        return RunSubmissionCodec.encode(submission, PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap("active-run-deadline", graph)));
    }

    private static PlanGraph emptyDiscardGraph() {
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(
                                META, Collections.emptyList(), "ready-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
    }

    private static PlanGraph blockingGraph(Sink output) {
        return new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META,
                                Collections.singletonList(new BlockingInput()))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, output).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
    }

    private static PlanGraph emptyManifestGraph(Path domain) {
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(
                                META, Collections.emptyList(), "commit-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new ManifestFileSink(
                                META, domain.toString(), "ledger")).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
    }

    private static PlanGraph multiStageGraph(Sink output) {
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(
                                META, Collections.emptyList(), "dispatch-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.unary("distinct", META, 0)
                        .listParam("keys", Collections.singletonList("id")).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "distinct"),
                PlanNode.sink(META, 1, output).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), 1);
    }

    private static void accept(
            MasterServer master, RpcMessage assignment, String incarnation) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(assignment.stageCommandBlob());
            RpcMessage accepted = master.handler().apply(RpcMessage.assignmentAccepted(
                    assignment.workerId(), assignment.stageId(),
                    assignment.stageAssignmentTokenBytes(), digest,
                    incarnation, assignment.runAttemptFence()));
            assertEquals(RpcMessage.Type.ACK, accepted.type(), accepted.errorMessage());
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void reportCanceled(
            MasterServer master, RpcMessage assignment) {
        try {
            StageAssignmentToken token = StageAssignmentManifestCodec.decodeToken(
                    assignment.stageAssignmentTokenBytes());
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(assignment.stageCommandBlob());
            RpcMessage terminal = master.handler().apply(RpcMessage.durableStageReport(
                    assignment.workerId(), StageTerminalReceipt.canceled(
                            token, digest, 0L, 0L, "CANCELED",
                            MessageDigest.getInstance("SHA-256").digest(
                                    "cancel".getBytes(
                                            java.nio.charset.StandardCharsets.UTF_8)),
                            System.currentTimeMillis())));
            assertEquals(RpcMessage.Type.ACK, terminal.type(), terminal.errorMessage());
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

}

