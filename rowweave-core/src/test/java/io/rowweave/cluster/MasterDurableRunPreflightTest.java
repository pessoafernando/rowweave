package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.CollectionSource;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.PrintSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.ArtifactReferenceAuthorityProvider;
import io.rowweave.artifact.ArtifactRepositoryProvider;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.DurableExecutionEvidenceAuthorityProvider;
import io.rowweave.run.FileRunStateStore;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunResultSummary;
import io.rowweave.run.RunStateCodec;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterDurableRunPreflightTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));

    @Test
    void corruptEnvelopeAndArtifactFailBeforeAuthorityMutationOrWorkerLookup() {
        CountingAuthorityRegistry registry = new CountingAuthorityRegistry();
        RunSubmission submission = submission("run-preflight");
        byte[] corruptEnvelope = envelope(submission);
        corruptEnvelope[corruptEnvelope.length - 1] ^= 1;
        byte[] badArtifactEnvelope = RunSubmissionCodec.encode(submission, new byte[]{1, 2, 3});

        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage corrupt = master.handler().apply(RpcMessage.submitRun(corruptEnvelope));
            assertFalse(corrupt.success());
            assertEquals(io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE,
                    corrupt.durableRunDisposition());
            assertTrue(corrupt.errorMessage().contains("checksum"), corrupt.errorMessage());
            assertNull(registry.store.get(submission.runId()));
            assertEquals(0, registry.liveWorkerReads.get());

            RpcMessage badArtifact = master.handler().apply(RpcMessage.submitRun(badArtifactEnvelope));
            assertFalse(badArtifact.success());
            assertEquals(io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE,
                    badArtifact.durableRunDisposition());
            assertTrue(badArtifact.errorMessage().contains("artifact")
                    || badArtifact.errorMessage().contains("frame"), badArtifact.errorMessage());
            assertNull(registry.store.get(submission.runId()));
            assertEquals(0, registry.liveWorkerReads.get());
        }
    }

    @Test
    void queryReturnsAuthoritySnapshotAndDistinguishesMissingRun() {
        CountingAuthorityRegistry registry = new CountingAuthorityRegistry();
        registry.store.apply(RunCommand.createRun(
                "create-query", "run-query", "definition", "artifact", 1, 0L, 1L));

        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage found = master.handler().apply(RpcMessage.queryRun("run-query"));
            assertEquals(RpcMessage.Type.RUN_SNAPSHOT, found.type());
            assertTrue(found.success());
            RunSnapshot snapshot = RunStateCodec.decodeSnapshot(found.planGraphBytes());
            assertEquals("run-query", snapshot.runId());
            assertEquals("artifact", snapshot.artifactId());
            assertEquals(io.rowweave.plan.TelemetryCoverage.UNAVAILABLE,
                    found.executionTelemetrySummary().resourceCoverage());

            RpcMessage missing = master.handler().apply(RpcMessage.queryRun("missing"));
            assertEquals(RpcMessage.Type.RUN_SNAPSHOT, missing.type());
            assertFalse(missing.success());
            assertNull(missing.errorMessage(), "missing is distinct from authority failure");
            assertEquals(0, registry.liveWorkerReads.get());
        }
    }

    @Test
    void fileRunStoreReturnsDecimalSnapshotWithUnavailableTelemetry(
            @TempDir Path temporary) {
        FileRunStateStore store = new FileRunStateStore(temporary);
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-file-query", "file-query", "definition", "artifact",
                1, 0L, 1L));
        run = accepted(store, RunCommand.createAttempt(
                "attempt-file-query", run.runId(), run.revision(),
                "attempt", "publication", 2L));
        for (AttemptStatus status : new AttemptStatus[] {
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                AttemptStatus.COMMITTING }) {
            run = accepted(store, RunCommand.advanceAttempt(
                    "advance-file-query-" + status, run.runId(), run.revision(),
                    "attempt", 1L, status, false, 3L, null, null));
        }
        RunSnapshot terminal = accepted(store, RunCommand.advanceAttempt(
                "succeed-file-query", run.runId(), run.revision(),
                "attempt", 1L, AttemptStatus.SUCCEEDED, false,
                4L, null, null, RunResultSummary.discard(17L)));
        CountingAuthorityRegistry registry = new CountingAuthorityRegistry(store);

        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage query = master.handler().apply(RpcMessage.queryRun("file-query"));

            assertTrue(query.success());
            assertArrayEquals(RunStateCodec.encodeSnapshot(terminal),
                    RunStateCodec.encodeSnapshot(
                            RunStateCodec.decodeSnapshot(query.planGraphBytes())));
            assertEquals(io.rowweave.plan.TelemetryCoverage.UNAVAILABLE,
                    query.executionTelemetrySummary().resourceCoverage());
            assertFalse(io.rowweave.run.DurableExecutionEvidenceAuthority.class
                    .isAssignableFrom(FileRunStateStore.class));
        }
    }

    @Test
    void runStateStoreCapabilityCannotReplaceMissingRegistryEvidenceProvider() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-provider-boundary", "provider-boundary", "definition",
                "artifact", 1, 0L, 1L));
        run = accepted(store, RunCommand.createAttempt(
                "attempt-provider-boundary", run.runId(), run.revision(),
                "attempt", "publication", 2L));
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 100L, 3, 0, StageAssignmentRole.KEYED_OP,
                "worker", "localhost", 7001, -1, "incarnation");
        StageAssignmentSnapshot assignments = store.publish(
                new StageAssignmentManifest(fence, run.artifactId(), 1L,
                        Collections.singletonList(record))).snapshot();
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                assignments.token(0), new byte[32], 3L);
        store.accept(acceptance);
        store.recordTerminal(StageTerminalReceipt.succeeded(
                acceptance.token(), acceptance.commandDigest(), 17L, 17L, 4L,
                StageExecutionTelemetry.complete(1L, 2L, 3L, 4L, 5L, 6L)));
        for (AttemptStatus status : new AttemptStatus[] {
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                AttemptStatus.COMMITTING }) {
            run = accepted(store, RunCommand.advanceAttempt(
                    "advance-provider-boundary-" + status, run.runId(), run.revision(),
                    "attempt", 1L, status, false, 5L, null, null));
        }
        RunSnapshot terminal = accepted(store, RunCommand.advanceAttempt(
                "succeed-provider-boundary", run.runId(), run.revision(),
                "attempt", 1L, AttemptStatus.SUCCEEDED, false,
                6L, null, null, RunResultSummary.discard(17L)));
        CountingAuthorityRegistry registry = new CountingAuthorityRegistry(store);

        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage query = master.handler().apply(
                    RpcMessage.queryRun(terminal.runId()));

            assertTrue(query.success());
            assertEquals(17L,
                    RunStateCodec.decodeSnapshot(query.planGraphBytes())
                            .resultSummary().rowsOut());
            assertEquals(TelemetryCoverage.UNAVAILABLE,
                    query.executionTelemetrySummary().resourceCoverage());
            assertEquals(TelemetryCoverage.UNAVAILABLE,
                    query.executionTelemetrySummary().partitionCoverage());
        }
    }

    @Test
    void durableApisFailClosedWhenRegistryHasNoAuthorityProvider() {
        CountingRegistryWithoutAuthority registry = new CountingRegistryWithoutAuthority();
        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage submit = master.handler().apply(
                    RpcMessage.submitRun(envelope(submission("run-no-provider"))));
            assertFalse(submit.success());
            assertTrue(submit.errorMessage().contains("run state authority unavailable"),
                    submit.errorMessage());
            assertEquals(0, registry.liveWorkerReads.get());

            RpcMessage query = master.handler().apply(RpcMessage.queryRun("run-no-provider"));
            assertEquals(RpcMessage.Type.RUN_SNAPSHOT, query.type());
            assertFalse(query.success());
            assertTrue(query.errorMessage().contains("run state authority unavailable"),
                    query.errorMessage());
        }
    }

    @Test
    void durableSingleSinkWithoutCrashReconciliationFailsBeforeMutationOrWorkerLookup() {
        CountingAuthorityRegistry registry = new CountingAuthorityRegistry();
        RunSubmission submission = submission("run-print-output");
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList())
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new PrintSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "print-output")));
        byte[] request = RunSubmissionCodec.encode(submission, PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap("durable-print-output", graph)));

        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage response = master.handler().apply(RpcMessage.submitRun(request));

            assertFalse(response.success());
            assertTrue(response.errorMessage().contains(
                    "durable run requires crash-reconcilable output:"),
                    response.errorMessage());
            assertNull(registry.store.get(submission.runId()));
            assertEquals(0, registry.liveWorkerReads.get());
        }
    }

    @Test
    void acceptedReplayDoesNotFallBackWhenCoherentEvidenceAuthorityIsCorrupt() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSubmission submission = submission("run-corrupt-evidence-replay");
        PlanArtifact artifact = validArtifact();
        byte[] request = RunSubmissionCodec.encode(
                submission, PlanArtifactCodec.encode(artifact));
        String seed = sha256Hex(request);

        RunSnapshot run = accepted(store, RunCommand.createRun(
                commandId(seed, "create-run"), submission.runId(),
                artifact.definitionId(), artifact.artifactId(),
                submission.maxAttempts(), submission.deadlineEpochMillis(),
                submission.submittedAtMillis()));
        run = accepted(store, RunCommand.createAttempt(
                commandId(seed, "create-attempt"), run.runId(), run.revision(),
                submission.attemptId(), submission.publicationId(),
                submission.submittedAtMillis()));
        run = accepted(store, RunCommand.advanceAttempt(
                commandId(seed, "dispatch"), run.runId(), run.revision(),
                submission.attemptId(), run.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, submission.submittedAtMillis(),
                null, null));
        for (AttemptStatus status : new AttemptStatus[] {
                AttemptStatus.RUNNING, AttemptStatus.COMMITTING }) {
            run = accepted(store, RunCommand.advanceAttempt(
                    "advance-corrupt-evidence-" + status, run.runId(), run.revision(),
                    submission.attemptId(), run.currentAttempt().fenceToken(),
                    status, false, submission.submittedAtMillis(), null, null));
        }
        accepted(store, RunCommand.advanceAttempt(
                "succeed-corrupt-evidence", run.runId(), run.revision(),
                submission.attemptId(), run.currentAttempt().fenceToken(),
                AttemptStatus.SUCCEEDED, false, submission.submittedAtMillis(),
                null, null, RunResultSummary.discard(1L)));

        EvidenceProviderRegistry registry = EvidenceProviderRegistry.throwing(store);
        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage replay = master.handler().apply(RpcMessage.submitRun(request));

            assertFalse(replay.success(),
                    "coherent evidence corruption must not be hidden by a second authority read");
            assertEquals(io.rowweave.run.DurableRunDisposition.UNKNOWN,
                    replay.durableRunDisposition(), replay.errorMessage());
            assertTrue(replay.errorMessage().contains("corrupt coherent evidence"),
                    replay.errorMessage());
        }
    }

    @Test
    void postPublicationFailureDoesNotFallBackWhenCoherentEvidenceAuthorityIsCorrupt() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSubmission submission = submission("run-corrupt-evidence-after-publication");
        EvidenceProviderRegistry registry = EvidenceProviderRegistry.throwing(store);

        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage response = master.handler().apply(
                    RpcMessage.submitRun(envelope(submission)));

            assertFalse(response.success());
            assertEquals(io.rowweave.run.DurableRunDisposition.UNKNOWN,
                    response.durableRunDisposition(), response.errorMessage());
            assertTrue(response.errorMessage().contains("corrupt coherent evidence"),
                    response.errorMessage());
            assertTrue(store.get(submission.runId()) != null,
                    "the failure must occur after durable publication");
        }
    }

    @Test
    void postPublicationFailureDoesNotFallBackWhenEvidenceProviderReturnsNull() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSubmission submission = submission("run-null-evidence-provider");
        EvidenceProviderRegistry registry = EvidenceProviderRegistry.returningNull(store);

        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage response = master.handler().apply(
                    RpcMessage.submitRun(envelope(submission)));

            assertFalse(response.success());
            assertEquals(io.rowweave.run.DurableRunDisposition.UNKNOWN,
                    response.durableRunDisposition(), response.errorMessage());
            assertTrue(response.errorMessage().contains("provider returned null"),
                    response.errorMessage());
            assertTrue(store.get(submission.runId()) != null,
                    "the failure must occur after durable publication");

            RpcMessage query = master.handler().apply(
                    RpcMessage.queryRun(submission.runId()));
            assertFalse(query.success());
            assertTrue(query.errorMessage().contains("provider returned null"),
                    query.errorMessage());
        }
    }

    @Test
    void legacySourceReplayFailsBeforeRunMutationOrWorkerLookup() {
        CountingAuthorityRegistry registry = new CountingAuthorityRegistry();
        RunSubmission submission = submission("run-legacy-source");
        DataRow row = new DataRow(1);
        row.set(0, 1);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(
                                new CollectionSource(META, Collections.singletonList(row))))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        byte[] request = RunSubmissionCodec.encode(submission, PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap("legacy-source", graph)));

        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage response = master.handler().apply(RpcMessage.submitRun(request));

            assertFalse(response.success());
            assertEquals(io.rowweave.run.DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE,
                    response.durableRunDisposition());
            assertTrue(response.errorMessage().contains("UNSUPPORTED_SOURCE_REPLAY"),
                    response.errorMessage());
            assertNull(registry.store.get(submission.runId()));
            assertEquals(0, registry.liveWorkerReads.get());
        }
    }

    private static RunSubmission submission(String runId) {
        return new RunSubmission(runId, "attempt-1", "publication-1", 1, 0L, 10L);
    }

    private static byte[] envelope(RunSubmission submission) {
        return RunSubmissionCodec.encode(submission,
                PlanArtifactCodec.encode(validArtifact()));
    }

    private static PlanArtifact validArtifact() {
        DataRow row = new DataRow(1);
        row.set(0, 1);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(
                                new CollectionSource(META, "durable-preflight-source",
                                        Collections.singletonList(row))))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return LegacyPlanArtifactAdapter.wrap("durable-preflight", graph);
    }

    private static String commandId(String seed, String phase) {
        return "run-submit-v1-" + phase + '-' + seed;
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                hex.append(Character.forDigit((value >>> 4) & 0xf, 16));
                hex.append(Character.forDigit(value & 0xf, 16));
            }
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static final class CountingAuthorityRegistry
            implements ServiceRegistry, RunStateStoreProvider {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final RunStateStore store;
        private final AtomicInteger liveWorkerReads = new AtomicInteger();

        private CountingAuthorityRegistry() {
            this(new InMemoryRunStateStore());
        }

        private CountingAuthorityRegistry(RunStateStore store) {
            this.store = store;
        }

        @Override public RunStateStore runStateStore() { return store; }
        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() {
            liveWorkerReads.incrementAndGet();
            return delegate.liveWorkers();
        }
        @Override public RegistryWorker worker(String workerId) { return delegate.worker(workerId); }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }

    private static final class EvidenceProviderRegistry
            implements ServiceRegistry, RunStateStoreProvider,
            DurableExecutionEvidenceAuthorityProvider,
            ArtifactRepositoryProvider, ArtifactReferenceAuthorityProvider {
        private final CountingAuthorityRegistry delegate;
        private final InMemoryArtifactRepository artifacts =
                new InMemoryArtifactRepository(16, 16L * 1024L * 1024L);
        private final boolean returnsNull;

        private EvidenceProviderRegistry(RunStateStore store, boolean returnsNull) {
            this.delegate = new CountingAuthorityRegistry(store);
            this.returnsNull = returnsNull;
        }

        private static EvidenceProviderRegistry throwing(RunStateStore store) {
            return new EvidenceProviderRegistry(store, false);
        }

        private static EvidenceProviderRegistry returningNull(RunStateStore store) {
            return new EvidenceProviderRegistry(store, true);
        }

        @Override public RunStateStore runStateStore() { return delegate.runStateStore(); }
        @Override public io.rowweave.run.DurableExecutionEvidenceAuthority
        durableExecutionEvidenceAuthority() {
            if (returnsNull) return null;
            return runId -> {
                throw new IllegalStateException("corrupt coherent evidence for " + runId);
            };
        }
        @Override public io.rowweave.artifact.ArtifactRepository artifactRepository() {
            return artifacts;
        }
        @Override public io.rowweave.artifact.ArtifactReferenceAuthority
        artifactReferenceAuthority() {
            return (io.rowweave.artifact.ArtifactReferenceAuthority) runStateStore();
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
            return delegate.worker(workerId);
        }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }

    private static RunSnapshot accepted(RunStateStore store, RunCommand command) {
        io.rowweave.run.RunDecision decision = store.apply(command);
        assertEquals(io.rowweave.run.RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static final class CountingRegistryWithoutAuthority implements ServiceRegistry {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final AtomicInteger liveWorkerReads = new AtomicInteger();

        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() {
            liveWorkerReads.incrementAndGet();
            return delegate.liveWorkers();
        }
        @Override public RegistryWorker worker(String workerId) { return delegate.worker(workerId); }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }
}
