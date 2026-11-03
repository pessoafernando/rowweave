package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.monitor.BatchSinkView;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.ExecutionTelemetrySummary;
import io.rowweave.plan.ExecutionTelemetrySummaryCodec;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunResultSummary;
import io.rowweave.run.RunStateCodec;
import io.rowweave.run.InMemoryRunStateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RealClusterJobClientDurableRunTest {
    private static final String WORKER_HOST = "durable-client-worker";
    private static final int WORKER_PORT = 61_168;
    private static final String INCARNATION = "inc-durable-client";

    @Test
    void socketClientSubmitsStableRunAndQueriesAuthoritativeSnapshot() {
        InProcessRegistry registry = new InProcessRegistry();
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(10);
        try (MasterServer master = new MasterServer(60_000L, 0, registry, monitor)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    WORKER_HOST, WORKER_PORT, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            acceptAndReport(master, request, 3L, 3L);
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", WORKER_HOST, WORKER_PORT, 4, -1, INCARNATION));
                RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", master.port());
                RunSubmission submission = new RunSubmission(
                        "run-client", "attempt-client", "publication-client", 1, 0L, 10L);

                ExecutionReport report = client.submitRun(artifact(), submission);
                assertEquals(3L, report.rowsOut());
                assertEquals(TelemetryCoverage.COMPLETE,
                        report.resourceTelemetryCoverage());
                assertEquals(2L, report.peakReservedBytes());
                assertEquals(12L, report.spillPeakReservedBytes());
                RunSnapshot snapshot = client.queryRun("run-client");
                assertEquals(RunStatus.SUCCEEDED, snapshot.status());
                assertEquals("attempt-client", snapshot.currentAttempt().attemptId());
                RpcMessage queried = master.handler().apply(
                        RpcMessage.queryRun("run-client"));
                ExecutionReport recovered = queried.executionTelemetrySummary()
                        .toExecutionReport(snapshot.resultSummary().rowsOut());
                assertReportsEqual(report, recovered);
                ExecutionReport replayed = client.submitRun(artifact(), submission);
                assertReportsEqual(report, replayed);
                assertReportsEqual(report,
                        monitor.job("run-client").executionReport());
                BatchSinkOutcome discardOutcome =
                        client.queryBatchSinkOutcome("run-client");
                assertEquals(BatchSinkOutcome.State.COMMITTED,
                        discardOutcome.state());
                assertEquals("SIDE_EFFECT_FREE_COMMITTED",
                        discardOutcome.detailCode());
                assertNull(client.queryRun("missing-run"));
                assertEquals(BatchSinkOutcome.State.NOT_FOUND,
                        client.queryBatchSinkOutcome("missing-run").state());
            } finally {
                endpoint.close();
            }
        }
    }

    private static void acceptAndReport(MasterServer master, RpcMessage assignment,
                                        long inputRows, long outputRows) {
        try {
            io.rowweave.run.StageAssignmentToken token =
                    io.rowweave.run.StageAssignmentManifestCodec.decodeToken(
                            assignment.stageAssignmentTokenBytes());
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(assignment.stageCommandBlob());
            RpcMessage accepted = master.handler().apply(RpcMessage.assignmentAccepted(
                    assignment.workerId(), assignment.stageId(),
                    assignment.stageAssignmentTokenBytes(), digest,
                    INCARNATION, assignment.runAttemptFence()));
            assertEquals(RpcMessage.Type.ACK, accepted.type(), accepted.errorMessage());
            RpcMessage terminal = master.handler().apply(RpcMessage.durableStageReport(
                    assignment.workerId(), io.rowweave.run.StageTerminalReceipt.succeeded(
                            token, digest, inputRows, outputRows, System.currentTimeMillis(),
                            StageExecutionTelemetry.complete(1L, 2L, 3L, 4L, 5L, 6L))));
            assertEquals(RpcMessage.Type.ACK, terminal.type(), terminal.errorMessage());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    @Test
    void queryTransportOrAuthorityFailureIsNotReportedAsMissing() {
        RealClusterJobClient unavailable = new RealClusterJobClient("127.0.0.1", 1);
        assertThrows(RuntimeException.class, () -> unavailable.queryRun("run-unavailable"));
    }

    @Test
    void explicitPreAcceptanceRejectionDoesNotGuessFromTextOrQuery() {
        AtomicInteger queries = new AtomicInteger();
        try (RpcServer server = new RpcServer(request -> {
            if (request.type() == RpcMessage.Type.QUERY_RUN) queries.incrementAndGet();
            return RpcMessage.durableJobResult(false, 0L, "invalid durable request",
                    io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE);
        })) {
            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", server.port());
            DurableRunSubmitException failure = assertThrows(
                    DurableRunSubmitException.class,
                    () -> client.submitRun(artifact(), submission("run-rejected")));
            assertEquals(DurableRunSubmitException.Category.REJECTED, failure.category());
            assertEquals("run-rejected", failure.runId());
            assertNull(failure.lastSnapshot());
            assertEquals(0, queries.get());
        }
    }

    @Test
    void failedSubmitResponseUsesOneAuthoritativeQueryForTerminalOrUnknown() {
        assertSubmitFailureFromQuery(failedSnapshot("run-terminal-failed"),
                DurableRunSubmitException.Category.TERMINAL_FAILED);
        assertSubmitFailureFromQuery(runningSnapshot("run-unknown"),
                DurableRunSubmitException.Category.UNKNOWN);
    }

    @Test
    void lostSuccessResponseRecoversExactRowsFromSucceededSummary() {
        RunSnapshot succeeded = succeededSnapshot("run-success-ack-lost", 29L);
        ExecutionTelemetrySummary telemetry = new ExecutionTelemetrySummary(
                TelemetryCoverage.COMPLETE, 11L, 12L, 13L, 14L, 15L, 16L,
                TelemetryCoverage.NOT_APPLICABLE, Collections.emptyList());
        AtomicInteger queries = new AtomicInteger();
        try (RpcServer server = new RpcServer(request -> {
            if (request.type() == RpcMessage.Type.SUBMIT_RUN) {
                return RpcMessage.durableJobResult(false, 0L, "response outcome uncertain",
                        io.rowweave.run.DurableRunDisposition.UNKNOWN);
            }
            queries.incrementAndGet();
            return RpcMessage.runSnapshot(
                    io.rowweave.run.RunStateCodec.encodeSnapshot(succeeded),
                    ExecutionTelemetrySummaryCodec.encode(telemetry));
        })) {
            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", server.port());
            ExecutionReport recovered = client.submitRun(
                    artifact(), submission("run-success-ack-lost"));
            assertEquals(29L, recovered.rowsOut());
            assertEquals(TelemetryCoverage.COMPLETE,
                    recovered.resourceTelemetryCoverage());
            assertEquals(11L, recovered.peakReservedBytes());
            assertEquals(16L, recovered.spillPeakReservedBytes());
            assertEquals(1, queries.get());
        }
    }

    private static void assertReportsEqual(ExecutionReport expected, ExecutionReport actual) {
        assertEquals(expected.rowsOut(), actual.rowsOut());
        assertEquals(expected.resourceTelemetryCoverage(),
                actual.resourceTelemetryCoverage());
        assertEquals(expected.partitionTelemetryCoverage(),
                actual.partitionTelemetryCoverage());
        assertEquals(expected.peakReservedBytes(), actual.peakReservedBytes());
        assertEquals(expected.acquiredBytes(), actual.acquiredBytes());
        assertEquals(expected.pressureSpillCount(), actual.pressureSpillCount());
        assertEquals(expected.spillRunCount(), actual.spillRunCount());
        assertEquals(expected.spillLogicalBytesWritten(),
                actual.spillLogicalBytesWritten());
        assertEquals(expected.spillPeakReservedBytes(),
                actual.spillPeakReservedBytes());
        assertEquals(expected.partitionDistributions(),
                actual.partitionDistributions());
    }

    @Test
    void leaderLookupFailureIsAnAmbiguousSubmitAndQueriesAuthorityOnce() {
        RunSnapshot succeeded = succeededSnapshot("run-leader-lookup-lost", 31L);
        AtomicInteger lookups = new AtomicInteger();
        AtomicInteger queries = new AtomicInteger();
        try (RpcServer server = new RpcServer(request -> {
            assertEquals(RpcMessage.Type.QUERY_RUN, request.type());
            queries.incrementAndGet();
            return RpcMessage.runSnapshot(RunStateCodec.encodeSnapshot(succeeded));
        })) {
            RealClusterJobClient client = new RealClusterJobClient(() -> {
                if (lookups.getAndIncrement() == 0) {
                    throw new IllegalStateException("leader lookup outcome unavailable");
                }
                return new Endpoint("127.0.0.1", server.port());
            });

            assertEquals(31L, client.submitRun(
                    artifact(), submission("run-leader-lookup-lost")).rowsOut());
            assertEquals(2, lookups.get());
            assertEquals(1, queries.get());
        }
    }

    private static void assertSubmitFailureFromQuery(
            RunSnapshot snapshot, DurableRunSubmitException.Category expected) {
        AtomicInteger queries = new AtomicInteger();
        try (RpcServer server = new RpcServer(request -> {
            if (request.type() == RpcMessage.Type.SUBMIT_RUN) {
                return RpcMessage.durableJobResult(false, 0L, "accepted outcome",
                        io.rowweave.run.DurableRunDisposition.UNKNOWN);
            }
            queries.incrementAndGet();
            return RpcMessage.runSnapshot(io.rowweave.run.RunStateCodec.encodeSnapshot(snapshot));
        })) {
            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", server.port());
            DurableRunSubmitException failure = assertThrows(
                    DurableRunSubmitException.class,
                    () -> client.submitRun(artifact(), submission(snapshot.runId())));
            assertEquals(expected, failure.category());
            assertEquals(snapshot.runId(), failure.runId());
            assertEquals(snapshot.revision(), failure.lastSnapshot().revision());
            assertEquals(1, queries.get());
        }
    }

    private static RunSnapshot runningSnapshot(String runId) {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", 1, 0L, 1L)).snapshot();
        run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, run.revision(), "attempt", "publication", 2L)).snapshot();
        return store.apply(RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null)).snapshot();
    }

    private static RunSnapshot failedSnapshot(String runId) {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", 1, 0L, 1L)).snapshot();
        run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, run.revision(), "attempt", "publication", 2L)).snapshot();
        run = store.apply(RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null)).snapshot();
        return store.apply(RunCommand.advanceAttempt(
                "failed-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.FAILED, false, 4L, "FAILED", "failed")).snapshot();
    }

    private static RunSnapshot succeededSnapshot(String runId, long rowsOut) {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", 1, 0L, 1L)).snapshot();
        run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, run.revision(), "attempt", "publication", 2L)).snapshot();
        run = store.apply(RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null)).snapshot();
        run = store.apply(RunCommand.advanceAttempt(
                "running-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.RUNNING, false, 4L, null, null)).snapshot();
        run = store.apply(RunCommand.advanceAttempt(
                "committing-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.COMMITTING, false, 5L, null, null)).snapshot();
        return store.apply(RunCommand.advanceAttempt(
                "succeeded-" + runId, runId, run.revision(), "attempt", 1L,
                AttemptStatus.SUCCEEDED, false, 6L, null, null,
                RunResultSummary.discard(rowsOut))).snapshot();
    }

    @Test
    void unsupportedAttemptPolicyFailsBeforeLeaderLookup() {
        AtomicReference<Boolean> located = new AtomicReference<>(false);
        RealClusterJobClient client = new RealClusterJobClient(() -> {
            located.set(true);
            throw new AssertionError("leader lookup must not happen");
        });

        IllegalArgumentException attempts = assertThrows(IllegalArgumentException.class,
                () -> client.submitRun(artifact(), new RunSubmission(
                        "run-policy-attempts", "attempt", "publication", 4, 0L, 10L)));
        assertTrue(attempts.getMessage().startsWith("unsupported durable run policy:"));
        assertFalse(located.get());
    }

    @Test
    void nonZeroDeadlinePassesPolicyValidationAndReachesLeaderLookup() {
        AtomicInteger lookups = new AtomicInteger();
        RealClusterJobClient client = new RealClusterJobClient(() -> {
            lookups.incrementAndGet();
            throw new IllegalStateException("leader lookup reached");
        });

        DurableRunSubmitException lookup = assertThrows(DurableRunSubmitException.class,
                () -> client.submitRun(artifact(), new RunSubmission(
                        "run-policy-deadline", "attempt", "publication", 1, 20L, 10L)));
        assertTrue(lookup.getMessage().startsWith("durable submit transport outcome is unknown"));
        assertEquals("leader lookup reached", lookup.getCause().getMessage());
        assertEquals(2, lookups.get());
    }

    @Test
    void nonReconcilableOutputsFailBeforeLeaderLookup(@TempDir Path tmp) {
        AtomicInteger lookups = new AtomicInteger();
        RealClusterJobClient client = new RealClusterJobClient(() -> {
            lookups.incrementAndGet();
            throw new AssertionError("invalid durable output must not locate a leader");
        });

        IllegalArgumentException file = assertThrows(IllegalArgumentException.class,
                () -> client.submitRun(
                        fileOutputArtifact(tmp.resolve("plain.tsv")),
                        submission("run-client-plain-file")));
        assertTrue(file.getMessage().startsWith(
                "durable run requires crash-reconcilable output:"), file.getMessage());
        assertEquals(0, lookups.get());

        IllegalArgumentException custom = assertThrows(IllegalArgumentException.class,
                () -> client.submitRun(
                        customOutputArtifact(), submission("run-client-custom")));
        assertTrue(custom.getMessage().startsWith(
                "durable run requires crash-reconcilable output:"), custom.getMessage());
        assertEquals(0, lookups.get());
    }

    @Test
    void graphDurableEntryRejectsBeforeOutputSerializationAndLeaderLookup()
            throws Exception {
        AtomicBoolean located = new AtomicBoolean();
        AtomicBoolean serialized = new AtomicBoolean();
        RealClusterJobClient client = new RealClusterJobClient(() -> {
            located.set(true);
            throw new AssertionError("leader lookup must not happen");
        });
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.placeholderSource(outputMeta()),
                PlanNode.sink(outputMeta(), 0,
                        new HookedDurableOutput(serialized)).build()));

        java.lang.reflect.Method graphEntry = RealClusterJobClient.class.getMethod(
                "submitRun", PlanGraph.class, String.class, RunSubmission.class);
        java.lang.reflect.InvocationTargetException rejected = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> graphEntry.invoke(client, graph, "durable-graph",
                        submission("run-durable-graph-hook")));
        assertTrue(rejected.getCause() instanceof IllegalArgumentException);
        assertFalse(serialized.get());
        assertFalse(located.get());
    }

    @Test
    void durableRunCommitsManifestOutputWithAssignmentFence(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(10);
        Path domain = tmp.resolve("manifest");
        File spill = tmp.resolve("spill").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(
                     60_000L, 0, registry, monitor);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "durable-output-worker", 8, "127.0.0.1", master.port(), spill)) {
            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", master.port());
            RunSubmission manifestSubmission = new RunSubmission(
                    "run-manifest-output", "attempt-manifest-output",
                    "publication-manifest-output", 1, 0L, 10L);
            AtomicReference<AttemptStatus> manifestPublishStatus = new AtomicReference<>();
            AtomicReference<Boolean> durableIntentBeforePublish = new AtomicReference<>(false);
            AtomicReference<Boolean> durableLocatorBeforePublish = new AtomicReference<>(false);
            AtomicReference<String> commonOutcomeBeforePublish = new AtomicReference<>();
            AtomicReference<byte[]> identityBeforePublish = new AtomicReference<>();
            master.setManifestPublishHooks(() -> {
                RunSnapshot run = registry.runStateStore().get("run-manifest-output");
                manifestPublishStatus.set(run.currentAttempt().status());
                io.rowweave.run.RunAttemptFence fence = new io.rowweave.run.RunAttemptFence(
                        run.runId(), run.currentAttempt().attemptId(),
                        run.currentAttempt().fenceToken());
                io.rowweave.run.ManifestCommitJournalSnapshot journal =
                        registry.durableStageAssignmentAuthority()
                                .manifestCommitJournal(fence);
                durableIntentBeforePublish.set(journal != null
                        && journal.allExpectedMembersSealed()
                        && journal.intent() != null);
                durableLocatorBeforePublish.set(journal != null
                        && journal.memberCount() == 1
                        && journal.member(1).hasPreparedLocator());
                RpcMessage outcome = master.handler().apply(
                        RpcMessage.queryBatchSinkOutcome("run-manifest-output"));
                commonOutcomeBeforePublish.set(outcome.commitOutcome());
                identityBeforePublish.set(outcome.commitPayload());
            }, null);
            ExecutionReport manifestReport = client.submitRun(
                    manifestOutputArtifact(domain), manifestSubmission);

            assertEquals(2L, manifestReport.rowsOut());
            assertEquals(AttemptStatus.COMMITTING, manifestPublishStatus.get());
            assertTrue(durableIntentBeforePublish.get(),
                    "normal publish must be preceded by sealed receipts and durable intent");
            assertTrue(durableLocatorBeforePublish.get(),
                    "normal publish must be preceded by a durable prepared locator");
            assertEquals(BatchSinkOutcome.State.COMMIT_DECIDED.name(),
                    commonOutcomeBeforePublish.get(),
                    "durable intent must be queryable before external visibility");
            try (io.rowweave.FileSource.Reader reader =
                         new ManifestFileSource(outputMeta(), domain.toString(), "ledger")
                                 .openReader()) {
                assertEquals(7, reader.next().get(0));
                assertEquals(8, reader.next().get(0));
                assertFalse(reader.hasNext());
            }
            assertEquals(RunStatus.SUCCEEDED, client.queryRun("run-manifest-output").status());
            BatchSinkOutcome sinkOutcome =
                    client.queryBatchSinkOutcome("run-manifest-output");
            assertEquals(BatchSinkOutcome.State.COMMITTED, sinkOutcome.state());
            assertTrue(sinkOutcome.generation() > 0L);
            assertEquals(32, sinkOutcome.identityDigest().length);
            assertEquals("MANIFEST_VISIBLE_VERIFIED", sinkOutcome.detailCode());
            assertArrayEquals(identityBeforePublish.get(), sinkOutcome.identityDigest(),
                    "Sink group identity must remain stable after terminal journal cleanup");
            BatchSinkView monitored = monitor.job("run-manifest-output")
                    .batchSink();
            assertEquals(1, monitored.sinkCount());
            assertEquals(Collections.singletonList(
                    "MANIFEST_ATOMIC_GROUP_V1"), monitored.capabilityProfiles());
            assertEquals(BatchSinkView.DOMAIN_SAME,
                    monitored.commitDomainDisposition());
            assertEquals(1, monitored.descriptorDigestPrefixes().size());
            assertEquals(16,
                    monitored.descriptorDigestPrefixes().get(0).length());
            assertEquals(1, monitored.locatorClosureCount());
            assertEquals(BatchSinkOutcome.State.COMMITTED.name(),
                    monitored.outcomeState());
            assertFalse(monitored.descriptorDigestPrefixes().get(0)
                    .contains(domain.toString()));
            io.rowweave.commit.ManifestCommitOutcome legacyOutcome =
                    client.queryManifestOutcome(
                            domain.toString(), "publication-manifest-output");
            assertEquals(io.rowweave.commit.ManifestCommitOutcome.State.COMMITTED,
                    legacyOutcome.state());
            assertEquals(legacyOutcome.generation(), sinkOutcome.generation());
            Files.write(domain.resolve("manifests").resolve(
                            sinkOutcome.generation() + ".manifest"),
                    new byte[] {0x01, 0x02, 0x03},
                    StandardOpenOption.TRUNCATE_EXISTING);
            BatchSinkOutcome corrupt =
                    client.queryBatchSinkOutcome("run-manifest-output");
            assertEquals(BatchSinkOutcome.State.CORRUPT_OR_CONFLICT,
                    corrupt.state(),
                    "public QUERY must bound Manifest corruption through Worker/provider");
            assertEquals(0, worker.preparedOutputActiveCount());
        }
    }

    @Test
    void durableRunAtomicallyCommitsTwoToFiveManifestMembersAcrossWorkers(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        File spill0 = tmp.resolve("multi-spill-0").toFile();
        File spill1 = tmp.resolve("multi-spill-1").toFile();
        assertTrue(spill0.mkdirs());
        assertTrue(spill1.mkdirs());
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "durable-multi-output-worker-0", 8,
                     "127.0.0.1", master.port(), spill0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "durable-multi-output-worker-1", 8,
                     "127.0.0.1", master.port(), spill1)) {
            RealClusterJobClient client = new RealClusterJobClient(
                    "127.0.0.1", master.port());

            for (int sinkCount = 2; sinkCount <= 5; sinkCount++) {
                Path domain = tmp.resolve("multi-manifest-" + sinkCount);
                String runId = "run-multi-manifest-" + sinkCount;
                master.setManifestPublishHooks(() -> assertNull(
                        new io.rowweave.commit.ManifestFileStore(domain).latest(),
                        "no member may become visible before the group manifest"), null);

                PlanArtifact artifact = multiManifestOutputArtifact(domain, sinkCount);
                ExecutionReport report = client.submitRun(artifact, submission(runId));

                io.rowweave.commit.CommitManifest visible =
                        new io.rowweave.commit.ManifestFileStore(domain).latest();
                assertEquals(sinkCount * 2L, report.rowsOut());
                assertEquals(sinkCount, visible.members().size());
                for (int member = 0; member < sinkCount; member++) {
                    assertManifestRows(domain, "member-" + member, 7, 8);
                }
                assertEquals(RunStatus.SUCCEEDED, client.queryRun(runId).status());
                assertEquals(BatchSinkOutcome.State.COMMITTED,
                        client.queryBatchSinkOutcome(runId).state());
                if (sinkCount == 5) {
                    long generation = visible.generation();
                    ExecutionReport replay = client.submitRun(
                            artifact, submission(runId));
                    assertEquals(report.rowsOut(), replay.rowsOut());
                    assertEquals(generation,
                            new io.rowweave.commit.ManifestFileStore(domain)
                                    .latest().generation(),
                            "duplicate submit RPC must not create a new publication");
                }
            }

            assertEquals(0, worker0.preparedOutputActiveCount());
            assertEquals(0, worker1.preparedOutputActiveCount());
        }
    }

    private static PlanArtifact artifact() {
        DataSchema meta = outputMeta();
        PlanGraph graph = new PlanGraph(Arrays.asList(
                DurableTestSources.source(meta, Collections.emptyList(), "client-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(meta, 0, new DiscardSink(meta)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return LegacyPlanArtifactAdapter.wrap("durable-client", graph);
    }

    private static PlanArtifact fileOutputArtifact(Path file) {
        DataSchema meta = outputMeta();
        PlanGraph graph = new PlanGraph(Arrays.asList(
                DurableTestSources.source(meta, Arrays.asList(row(7), row(8)),
                                "file-output-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(meta, 0, new FileSink(meta, file.toString())).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "file-output")), 1);
        return LegacyPlanArtifactAdapter.wrap("durable-file-output", graph);
    }

    private static PlanArtifact customOutputArtifact() {
        DataSchema meta = outputMeta();
        PlanGraph graph = new PlanGraph(Arrays.asList(
                DurableTestSources.source(meta, Collections.emptyList(), "custom-output-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(meta, 0, new SelfReportedCoordinatedOutput(meta)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "custom-output")), 1);
        return LegacyPlanArtifactAdapter.wrap("durable-custom-output", graph);
    }

    private static RunSubmission submission(String runId) {
        return new RunSubmission(
                runId, "attempt-1", "publication-1", 1, 0L, 10L);
    }

    private static PlanArtifact manifestOutputArtifact(Path domain) {
        DataSchema meta = outputMeta();
        PlanGraph graph = new PlanGraph(Arrays.asList(
                DurableTestSources.source(meta, Arrays.asList(row(7), row(8)),
                                "manifest-output-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(meta, 0,
                                new ManifestFileSink(meta, domain.toString(), "ledger"))
                        .build().withParam(PlanNode.OPERATOR_UID_PARAM, "manifest-output")), 1);
        return LegacyPlanArtifactAdapter.wrap("durable-manifest-output", graph);
    }

    private static PlanArtifact multiManifestOutputArtifact(
            Path domain, int sinkCount) {
        DataSchema meta = outputMeta();
        PlanNode source = DurableTestSources.source(
                meta, Arrays.asList(row(7), row(8)), "multi-manifest-source")
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source");
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(source);
        for (int member = 0; member < sinkCount; member++) {
            nodes.add(PlanNode.sink(meta, 0, new ManifestFileSink(
                            meta, domain.toString(), "member-" + member))
                    .build().withParam(
                            PlanNode.OPERATOR_UID_PARAM, "output-" + member));
        }
        return LegacyPlanArtifactAdapter.wrap("durable-multi-manifest-output",
                new PlanGraph(nodes, 2));
    }

    private static void assertManifestRows(
            Path domain, String member, int first, int second) throws Exception {
        try (io.rowweave.FileSource.Reader reader =
                     new ManifestFileSource(outputMeta(), domain.toString(), member)
                             .openReader()) {
            assertEquals(first, reader.next().get(0));
            assertEquals(second, reader.next().get(0));
            assertFalse(reader.hasNext());
        }
    }

    private static DataSchema outputMeta() {
        return new DataSchema(new Field("id", DataType.IntegerType));
    }

    private static DataRow row(int id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static final class SelfReportedCoordinatedOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final DataSchema meta;

        private SelfReportedCoordinatedOutput(DataSchema meta) {
            this.meta = meta;
        }

        @Override public DataSchema getSchema() { return meta; }
        @Override public void collect(DataRow row) { }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { }
        @Override public void rollbackCommit() { }
        @Override public void finalizeCommit() { }

        @Override public void commitPrepared() { }
    }

    private static final class HookedDurableOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final AtomicBoolean serialized;

        private HookedDurableOutput(AtomicBoolean serialized) {
            this.serialized = serialized;
        }

        @Override public DataSchema getSchema() { return outputMeta(); }
        @Override public void collect(DataRow row) { }
        @Override public String exclusiveDestinationId() {
            return "test:hooked-durable";
        }

        private Object writeReplace() {
            serialized.set(true);
            return this;
        }
    }
}
