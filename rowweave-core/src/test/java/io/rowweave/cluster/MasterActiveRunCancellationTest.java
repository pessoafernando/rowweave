package io.rowweave.cluster;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunCommand;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterActiveRunCancellationTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static volatile CountDownLatch inputOpened;
    private static volatile CountDownLatch releaseInput;

    private static final class BlockingInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final BatchSourceSplitDescriptor descriptor = new BatchSourceSplitDescriptor(
                1, "test-blocking", 1, "active-cancel-source",
                SchemaFingerprint.of(META), "active-cancel-snapshot",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);

        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }

        @Override
        public Iterator<DataRow> iterator() {
            inputOpened.countDown();
            try {
                releaseInput.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("blocking input interrupted", interrupted);
            }
            return Collections.emptyIterator();
        }
    }

    @Test
    void runningDiscardRunCancelsAndDrainsFromDurableTerminalEvidence(
            @TempDir Path temp) throws Exception {
        resetBlockingInput();
        InProcessRegistry registry = new InProcessRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        File spill = temp.resolve("discard-worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "discard-worker", 8, "127.0.0.1", master.port(), spill)) {
            Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                    RpcMessage.submitRun(envelope(
                            "run-cancel-discard", "attempt-discard", "publication-discard",
                            blockingGraph(new DiscardSink(META))))));
            assertTrue(inputOpened.await(5, TimeUnit.SECONDS));

            RpcMessage cancellation = master.handler().apply(
                    RpcMessage.cancelRun("run-cancel-discard", "request-discard"));
            assertCancellation(cancellation, RunCancellationResult.Disposition.ACCEPTED);

            RpcMessage result = submitted.get(10, TimeUnit.SECONDS);
            assertFalse(result.success());
            RunSnapshot terminal = registry.runStateStore().get("run-cancel-discard");
            assertEquals(RunStatus.CANCELED, terminal.status());
            assertEquals(AttemptStatus.CANCELED, terminal.currentAttempt().status());
            assertNull(registry.durableStageAssignmentAuthority().assignmentSnapshot(
                    new RunAttemptFence("run-cancel-discard", "attempt-discard", 1L)));
            assertEquals(0, worker.runningStageCount());
            assertEquals(0, master.activeStageAssignmentCount());
        } finally {
            releaseInput.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void cancellationDuringDispatchFencesEveryLaterAssignment() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger assignments = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        CountDownLatch firstAssigned = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        Map<Long, RpcMessage> active = new ConcurrentHashMap<>();
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    "127.0.0.1", 61_192, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            assignments.incrementAndGet();
                            active.put(request.stageId(), request);
                            accept(master, request);
                            firstAssigned.countDown();
                            try {
                                releaseFirst.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                            }
                            return RpcMessage.ack();
                        }
                        if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                            cancellations.incrementAndGet();
                            RpcMessage assignment = active.get(request.stageId());
                            assertEquals(assignment.runAttemptFence(), request.runAttemptFence());
                            reportCanceled(master, assignment);
                            return RpcMessage.ack();
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", "127.0.0.1", 61_192, 8, -1,
                        "inc-dispatch-cancel"));
                Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                        RpcMessage.submitRun(envelope(
                                "run-cancel-dispatch", "attempt-dispatch",
                                "publication-dispatch", genericDiscardGraph()))));
                assertTrue(firstAssigned.await(5, TimeUnit.SECONDS));

                RpcMessage cancellation = master.handler().apply(
                        RpcMessage.cancelRun("run-cancel-dispatch", "request-dispatch"));
                assertCancellation(cancellation, RunCancellationResult.Disposition.ACCEPTED);
                releaseFirst.countDown();

                assertFalse(submitted.get(10, TimeUnit.SECONDS).success());
                assertEquals(1, assignments.get(),
                        "no physical assignment may appear after the cancel CAS");
                assertEquals(1, cancellations.get());
                assertEquals(RunStatus.CANCELED,
                        registry.runStateStore().get("run-cancel-dispatch").status());
            } finally {
                releaseFirst.countDown();
                endpoint.close();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void cancellationOfDispatchClaimWithoutAssignmentsConvergesToCanceled() {
        InProcessRegistry registry = new InProcessRegistry();
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "unassigned-dispatch", genericDiscardGraph());
        byte[] artifactBytes = PlanArtifactCodec.encode(artifact);
        RunSubmission submission = new RunSubmission(
                "run-unassigned-dispatch", "attempt-unassigned",
                "publication-unassigned", 1, 0L, 10L);
        byte[] encodedSubmission = RunSubmissionCodec.encode(submission, artifactBytes);
        String seed = sha256Hex(encodedSubmission);
        registry.artifactRepository().put(artifactBytes);
        registry.runStateStore().apply(RunCommand.createRun(
                "run-submit-v1-create-run-" + seed,
                "run-unassigned-dispatch", artifact.definitionId(),
                artifact.artifactId(), 1, 0L, 10L));
        registry.runStateStore().apply(RunCommand.createAttempt(
                "run-submit-v1-create-attempt-" + seed,
                "run-unassigned-dispatch", 1L,
                "attempt-unassigned", "publication-unassigned", 10L));
        registry.runStateStore().apply(RunCommand.advanceAttempt(
                "run-submit-v1-dispatch-" + seed,
                "run-unassigned-dispatch", 2L,
                "attempt-unassigned", 1L, AttemptStatus.DISPATCHING,
                false, 10L, null, null));

        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            assertCancellation(master.handler().apply(RpcMessage.cancelRun(
                    "run-unassigned-dispatch", "request-unassigned")),
                    RunCancellationResult.Disposition.ACCEPTED);
            RpcMessage result = master.handler().apply(RpcMessage.submitRun(
                    encodedSubmission));

            assertFalse(result.success());
            RunSnapshot terminal = registry.runStateStore().get(
                    "run-unassigned-dispatch");
            assertEquals(RunStatus.CANCELED, terminal.status());
            assertEquals(AttemptStatus.CANCELED,
                    terminal.currentAttempt().status());
            assertEquals(0, master.activeStageAssignmentCount());
        }
    }

    @Test
    void missingTerminalEvidenceKeepsRunCancelingAndQuarantinesWorker()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger assignments = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    "127.0.0.1", 61_193, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            assignments.incrementAndGet();
                            accept(master, request, "inc-unresponsive-cancel");
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
                        "w", "127.0.0.1", 61_193, 8, 62_193,
                        "inc-unresponsive-cancel"));
                Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                        RpcMessage.submitRun(envelope(
                                "run-cancel-unresponsive", "attempt-unresponsive",
                                "publication-unresponsive", genericDiscardGraph()))));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (assignments.get() == 0 && System.nanoTime() < deadline) {
                    Thread.sleep(10L);
                }
                assertTrue(assignments.get() > 0);

                assertCancellation(master.handler().apply(RpcMessage.cancelRun(
                        "run-cancel-unresponsive", "request-unresponsive")),
                        RunCancellationResult.Disposition.ACCEPTED);
                RpcMessage result = submitted.get(10, TimeUnit.SECONDS);

                assertFalse(result.success());
                assertEquals(io.rowweave.run.DurableRunDisposition.ACCEPTED_NON_TERMINAL,
                        result.durableRunDisposition());
                assertEquals(RunStatus.CANCELING,
                        registry.runStateStore().get("run-cancel-unresponsive").status());
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
    void manifestCancellationRecordsAbortBeforeRollbackAndNeverPublishes(
            @TempDir Path temp) throws Exception {
        resetBlockingInput();
        InProcessRegistry registry = new InProcessRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Path domain = temp.resolve("manifest-domain");
        File spill = temp.resolve("manifest-worker").toFile();
        assertTrue(spill.mkdirs());
        RunAttemptFence fence = new RunAttemptFence(
                "run-cancel-manifest", "attempt-manifest", 1L);
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "manifest-worker", 8, "127.0.0.1", master.port(), spill)) {
            Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                    RpcMessage.submitRun(envelope(
                            fence.runId(), fence.attemptId(), "publication-manifest",
                            blockingGraph(new ManifestFileSink(
                                    META, domain.toString(), "ledger"))))));
            assertTrue(inputOpened.await(5, TimeUnit.SECONDS));

            assertCancellation(master.handler().apply(RpcMessage.cancelRun(
                    fence.runId(), "request-manifest")),
                    RunCancellationResult.Disposition.ACCEPTED);
            assertFalse(submitted.get(10, TimeUnit.SECONDS).success());

            BatchSinkAbortRecord abort = registry.durableStageAssignmentAuthority()
                    .batchSinkAbort(fence);
            assertEquals(BatchSinkAbortRecord.State.ABORTED, abort.state());
            assertNull(new ManifestFileStore(domain).latest());
            assertEquals(RunStatus.CANCELED,
                    registry.runStateStore().get(fence.runId()).status());
            assertEquals(0, worker.preparedOutputActiveCount());
        } finally {
            releaseInput.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void committingWinsCancellationRaceAndPublicationRemainsMonotonic(
            @TempDir Path temp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch beforePublish = new CountDownLatch(1);
        CountDownLatch releasePublish = new CountDownLatch(1);
        Path domain = temp.resolve("committing-domain");
        File spill = temp.resolve("committing-worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "committing-worker", 8, "127.0.0.1", master.port(), spill)) {
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
                            "run-commit-wins", "attempt-commit", "publication-commit",
                            manifestGraph(domain)))));
            assertTrue(beforePublish.await(5, TimeUnit.SECONDS));

            RpcMessage cancellation = master.handler().apply(
                    RpcMessage.cancelRun("run-commit-wins", "request-too-late"));
            assertCancellation(cancellation, RunCancellationResult.Disposition.TOO_LATE);
            releasePublish.countDown();

            assertTrue(submitted.get(10, TimeUnit.SECONDS).success());
            assertEquals(RunStatus.SUCCEEDED,
                    registry.runStateStore().get("run-commit-wins").status());
            assertEquals("publication-commit",
                    new ManifestFileStore(domain).latest().attemptId());
        } finally {
            releasePublish.countDown();
            executor.shutdownNow();
        }
    }

    private static void resetBlockingInput() {
        inputOpened = new CountDownLatch(1);
        releaseInput = new CountDownLatch(1);
    }

    private static void assertCancellation(
            RpcMessage message, RunCancellationResult.Disposition disposition) {
        assertTrue(message.success(), message.errorMessage());
        assertEquals(disposition.name(), message.runCancellationDisposition());
    }

    private static byte[] envelope(
            String runId, String attemptId, String publicationId, PlanGraph graph) {
        RunSubmission submission = new RunSubmission(
                runId, attemptId, publicationId, 1, 0L, 10L);
        return RunSubmissionCodec.encode(submission, PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap("active-run-cancellation", graph)));
    }

    private static PlanGraph blockingGraph(io.rowweave.Sink output) {
        return new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META,
                                Collections.singletonList(new BlockingInput()))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, output).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
    }

    private static PlanGraph genericDiscardGraph() {
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.emptyList(), "dispatch-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.unary("distinct", META, 0)
                        .listParam("keys", Collections.singletonList("id")).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "distinct"),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), 1);
    }

    private static PlanGraph manifestGraph(Path domain) {
        DataRow row = new DataRow(1);
        row.set(0, 7);
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.singletonList(row), "commit-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0,
                                new ManifestFileSink(META, domain.toString(), "ledger"))
                        .build().withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
    }

    private static void accept(MasterServer master, RpcMessage assignment) {
        accept(master, assignment, "inc-dispatch-cancel");
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

    private static void reportCanceled(MasterServer master, RpcMessage assignment) {
        try {
            StageAssignmentToken token = StageAssignmentManifestCodec.decodeToken(
                    assignment.stageAssignmentTokenBytes());
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(assignment.stageCommandBlob());
            RpcMessage terminal = master.handler().apply(RpcMessage.durableStageReport(
                    assignment.workerId(), StageTerminalReceipt.canceled(
                            token, digest, 0L, 0L, "CANCELED",
                            MessageDigest.getInstance("SHA-256").digest(
                                    "cancel".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                            System.currentTimeMillis())));
            assertEquals(RpcMessage.Type.ACK, terminal.type(), terminal.errorMessage());
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte element : digest) {
                hex.append(Character.forDigit((element >>> 4) & 0xf, 16));
                hex.append(Character.forDigit(element & 0xf, 16));
            }
            return hex.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}

