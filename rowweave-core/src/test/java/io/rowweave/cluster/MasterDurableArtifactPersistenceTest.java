package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.ArtifactRepositoryProvider;
import io.rowweave.artifact.ArtifactBackfillStatus;
import io.rowweave.artifact.ArtifactCandidate;
import io.rowweave.artifact.ArtifactCandidatePage;
import io.rowweave.artifact.ArtifactCollectResult;
import io.rowweave.artifact.ArtifactGcToken;
import io.rowweave.artifact.ArtifactLifecycleRepository;
import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.artifact.ArtifactReferenceAuthorityProvider;
import io.rowweave.artifact.ArtifactReferencePublisher;
import io.rowweave.artifact.ArtifactReferenceView;
import io.rowweave.artifact.ArtifactRecoveryPage;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.DurableStageAssignmentAuthorityProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterDurableArtifactPersistenceTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));

    @Test
    void unsupportedPolicyRejectsBeforeArtifactRunOrWorkerSideEffects() {
        List<String> order = new ArrayList<>();
        RecordingRegistry registry = new RecordingRegistry(order);
        ArtifactRepository repository = recordingRepository(
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L), order);
        Submission fixture = submission("run-unsupported-policy", 4, 0L);

        try (MasterServer master = new MasterServer(
                5_000L, 0, registry, io.rowweave.monitor.JobMonitorRegistry.NOOP, repository)) {
            RpcMessage response = master.handler().apply(RpcMessage.submitRun(fixture.envelope));

            assertFalse(response.success());
            assertTrue(response.errorMessage().contains("unsupported durable run policy:"),
                    response.errorMessage());
            assertTrue(order.isEmpty(), "policy rejection must precede Artifact put and Run mutation");
            assertNull(registry.store.get(fixture.runId));
            assertEquals(0, registry.liveWorkerReads.get());
        }
    }

    @Test
    void retryPolicyRequiresCombinedAuthorityBeforeArtifactRunOrWorkerSideEffects() {
        List<String> order = new ArrayList<>();
        RecordingRegistry registry = new RecordingRegistry(order);
        ArtifactRepository repository = recordingRepository(
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L), order);
        Submission fixture = submission("run-missing-retry-authority", 2, 0L);

        try (MasterServer master = new MasterServer(
                5_000L, 0, registry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP, repository)) {
            RpcMessage response = master.handler().apply(
                    RpcMessage.submitRun(fixture.envelope));

            assertFalse(response.success());
            assertTrue(response.errorMessage().contains(
                    "whole-attempt retry authority capability unavailable"),
                    response.errorMessage());
            assertTrue(order.isEmpty(),
                    "retry capability rejection must precede Artifact put and Run mutation");
            assertNull(registry.store.get(fixture.runId));
            assertEquals(0, registry.liveWorkerReads.get());
        }
    }

    @Test
    void nonReconcilableOutputRejectsBeforeArtifactRunOrWorkerSideEffects(
            @TempDir Path tmp) {
        List<String> order = new ArrayList<>();
        RecordingRegistry registry = new RecordingRegistry(order);
        ArtifactRepository repository = recordingRepository(
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L), order);
        Submission fixture = submission("run-plain-file", 1, 0L,
                new FileSink(META, tmp.resolve("plain.tsv").toString()));

        try (MasterServer master = new MasterServer(
                5_000L, 0, registry, io.rowweave.monitor.JobMonitorRegistry.NOOP, repository)) {
            RpcMessage response = master.handler().apply(RpcMessage.submitRun(fixture.envelope));

            assertFalse(response.success());
            assertTrue(response.errorMessage().contains(
                    "durable run requires crash-reconcilable output:"),
                    response.errorMessage());
            assertTrue(order.isEmpty(),
                    "sink rejection must precede Artifact put and Run mutation");
            assertNull(registry.store.get(fixture.runId));
            assertEquals(0, registry.liveWorkerReads.get());
        }
    }

    @Test
    void missingRepositoryRejectsBeforeRunMutationOrWorkerLookup() {
        RecordingRegistry registry = new RecordingRegistry(new ArrayList<>());
        Submission fixture = submission("run-missing-repository");

        try (MasterServer master = new MasterServer(5_000L, 0, registry)) {
            RpcMessage response = master.handler().apply(RpcMessage.submitRun(fixture.envelope));

            assertFalse(response.success());
            assertTrue(response.errorMessage().contains(
                    "artifact lifecycle capability unavailable"),
                    response.errorMessage());
            assertNull(registry.store.get(fixture.runId));
            assertEquals(0, registry.liveWorkerReads.get());
        }
    }

    @Test
    void artifactPutAndIdentityVerificationPrecedeFirstRunMutation() {
        List<String> order = new ArrayList<>();
        RecordingRegistry registry = new RecordingRegistry(order);
        InMemoryArtifactRepository delegate =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        ArtifactRepository repository = recordingRepository(delegate, order);
        Submission fixture = submission("run-order");

        try (MasterServer master = new MasterServer(
                5_000L, 0, registry, io.rowweave.monitor.JobMonitorRegistry.NOOP, repository)) {
            RpcMessage response = master.handler().apply(RpcMessage.submitRun(fixture.envelope));

            assertFalse(response.success());
            assertTrue(response.errorMessage().contains("no registered worker"),
                    response.errorMessage());
            assertEquals("artifact-guard", order.get(0));
            assertEquals("run-apply", order.get(1));
            assertArrayEquals(fixture.artifactBytes, repository.get(fixture.artifactId));
            assertNotNull(registry.store.get(fixture.runId));
        }
    }

    @Test
    void putFailureAndReturnedIdentityMismatchLeaveNoRun() {
        Submission throwFixture = submission("run-put-throw");
        RecordingRegistry throwRegistry = new RecordingRegistry(new ArrayList<>());
        ArtifactLifecycleRepository throwing = new LifecycleRepositoryAdapter(
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L)) {
            @Override public <T> T publishReferenced(
                    byte[] encodedArtifact, java.util.function.LongConsumer reservation,
                    ArtifactReferencePublisher<T> publisher) {
                throw new IllegalStateException("repository unavailable");
            }
        };
        try (MasterServer master = new MasterServer(
                5_000L, 0, throwRegistry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP, throwing)) {
            RpcMessage response = master.handler().apply(RpcMessage.submitRun(throwFixture.envelope));
            assertFalse(response.success());
            assertNull(throwRegistry.store.get(throwFixture.runId));
            assertEquals(0, throwRegistry.liveWorkerReads.get());
        }

        Submission mismatchFixture = submission("run-put-mismatch");
        RecordingRegistry mismatchRegistry = new RecordingRegistry(new ArrayList<>());
        ArtifactLifecycleRepository mismatch = new LifecycleRepositoryAdapter(
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L)) {
            @Override public <T> T publishReferenced(
                    byte[] encodedArtifact, java.util.function.LongConsumer reservation,
                    ArtifactReferencePublisher<T> publisher) {
                return publisher.publish(
                        String.join("", Collections.nCopies(64, "0")));
            }
        };
        try (MasterServer master = new MasterServer(
                5_000L, 0, mismatchRegistry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP, mismatch)) {
            RpcMessage response = master.handler().apply(RpcMessage.submitRun(mismatchFixture.envelope));
            assertFalse(response.success());
            assertTrue(response.errorMessage().contains("identity"), response.errorMessage());
            assertNull(mismatchRegistry.store.get(mismatchFixture.runId));
            assertEquals(0, mismatchRegistry.liveWorkerReads.get());
        }
    }

    @Test
    void guardedCallbackAckLossLeavesReferencedRunAndSameEnvelopeReplayContinues() {
        Submission fixture = submission("run-artifact-ack-loss");
        RecordingRegistry registry = new RecordingRegistry(new ArrayList<>());
        InMemoryArtifactRepository durable =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        AtomicBoolean loseFirstAck = new AtomicBoolean(true);
        ArtifactLifecycleRepository repository = new LifecycleRepositoryAdapter(durable) {
            @Override public <T> T publishReferenced(
                    byte[] encodedArtifact, java.util.function.LongConsumer reservation,
                    ArtifactReferencePublisher<T> publisher) {
                T result = super.publishReferenced(
                        encodedArtifact, reservation, publisher);
                if (loseFirstAck.compareAndSet(true, false)) {
                    throw new IllegalStateException("artifact publication ACK lost");
                }
                return result;
            }
        };

        try (MasterServer master = new MasterServer(
                5_000L, 0, registry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP, repository)) {
            RpcMessage first = master.handler().apply(RpcMessage.submitRun(fixture.envelope));
            assertFalse(first.success());
            assertArrayEquals(fixture.artifactBytes, durable.get(fixture.artifactId));
            assertNotNull(registry.store.get(fixture.runId));
            assertNull(registry.store.get(fixture.runId).currentAttempt());
            assertEquals(0, registry.liveWorkerReads.get());

            RpcMessage replay = master.handler().apply(RpcMessage.submitRun(fixture.envelope));
            assertFalse(replay.success());
            assertTrue(replay.errorMessage().contains("no registered worker"),
                    replay.errorMessage());
            assertNotNull(registry.store.get(fixture.runId));
            assertEquals(1, registry.liveWorkerReads.get());
        }
    }

    @Test
    void inProcessRegistryProvidesOneSharedRepositoryToBothMasters() {
        InProcessRegistry registry = new InProcessRegistry();
        ArtifactRepository repository =
                ((ArtifactRepositoryProvider) registry).artifactRepository();
        Submission fixture = submission("run-shared-artifact");

        try (MasterServer first = new MasterServer(5_000L, 0, registry);
             MasterServer second = new MasterServer(5_000L, 0, registry)) {
            RpcMessage response = first.handler().apply(RpcMessage.submitRun(fixture.envelope));
            assertFalse(response.success());
            assertArrayEquals(fixture.artifactBytes, repository.get(fixture.artifactId));
            assertArrayEquals(fixture.artifactBytes,
                    ((ArtifactRepositoryProvider) registry).artifactRepository()
                            .get(fixture.artifactId));
        }
    }

    private static ArtifactLifecycleRepository recordingRepository(
            ArtifactLifecycleRepository delegate, List<String> order) {
        return new LifecycleRepositoryAdapter(delegate) {
            @Override public <T> T publishReferenced(
                    byte[] encodedArtifact, java.util.function.LongConsumer reservation,
                    ArtifactReferencePublisher<T> publisher) {
                order.add("artifact-guard");
                return super.publishReferenced(encodedArtifact, reservation, publisher);
            }
        };
    }

    private static Submission submission(String runId) {
        return submission(runId, 1, 0L, new DiscardSink(META));
    }

    private static Submission submission(String runId, int maxAttempts, long deadlineEpochMillis) {
        return submission(runId, maxAttempts, deadlineEpochMillis, new DiscardSink(META));
    }

    private static Submission submission(
            String runId, int maxAttempts, long deadlineEpochMillis, Sink output) {
        PlanGraph graph = new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.emptyList(), "artifact-persistence-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, output).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        byte[] artifactBytes = PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap("durable-artifact-persistence", graph));
        RunSubmission submission = new RunSubmission(
                runId, "attempt-1", "publication-1",
                maxAttempts, deadlineEpochMillis, 10L);
        return new Submission(runId, artifactBytes,
                PlanArtifactCodec.artifactId(artifactBytes),
                RunSubmissionCodec.encode(submission, artifactBytes));
    }

    private static final class Submission {
        final String runId;
        final byte[] artifactBytes;
        final String artifactId;
        final byte[] envelope;

        Submission(String runId, byte[] artifactBytes, String artifactId, byte[] envelope) {
            this.runId = runId;
            this.artifactBytes = artifactBytes;
            this.artifactId = artifactId;
            this.envelope = envelope;
        }
    }

    private static final class RecordingRegistry
            implements ServiceRegistry, RunStateStoreProvider,
            DurableStageAssignmentAuthorityProvider,
            ArtifactReferenceAuthorityProvider {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final InMemoryRunStateStore rawStore = new InMemoryRunStateStore();
        private final RecordingAuthority store;
        private final AtomicInteger liveWorkerReads = new AtomicInteger();

        RecordingRegistry(List<String> order) {
            this.store = new RecordingAuthority(rawStore, order);
        }

        @Override public RunStateStore runStateStore() { return store; }
        @Override public DurableStageAssignmentAuthority durableStageAssignmentAuthority() {
            return rawStore;
        }
        @Override public ArtifactReferenceAuthority artifactReferenceAuthority() {
            return store;
        }
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

    private static final class RecordingAuthority
            implements RunStateStore, ArtifactReferenceAuthority {
        private final InMemoryRunStateStore delegate;
        private final List<String> order;

        private RecordingAuthority(InMemoryRunStateStore delegate, List<String> order) {
            this.delegate = delegate;
            this.order = order;
        }

        @Override public RunDecision apply(RunCommand command) {
            order.add("run-apply");
            return delegate.apply(command);
        }
        @Override public RunSnapshot get(String runId) { return delegate.get(runId); }
        @Override public List<io.rowweave.run.ExecutionEvent> events(
                String runId, long afterSequence, int limit) {
            return delegate.events(runId, afterSequence, limit);
        }
        @Override public ArtifactReferenceView inspect(String artifactId) {
            return delegate.inspect(artifactId);
        }
        @Override public ArtifactBackfillStatus backfillStatus() {
            return delegate.backfillStatus();
        }
        @Override public ArtifactBackfillStatus advanceBackfill(int maxEntries) {
            return delegate.advanceBackfill(maxEntries);
        }
    }

    private static class LifecycleRepositoryAdapter
            implements ArtifactLifecycleRepository {
        private final ArtifactLifecycleRepository delegate;

        private LifecycleRepositoryAdapter(ArtifactLifecycleRepository delegate) {
            this.delegate = delegate;
        }

        @Override public String put(byte[] encodedArtifact) {
            return delegate.put(encodedArtifact);
        }
        @Override public String put(byte[] encodedArtifact,
                                    java.util.function.LongConsumer reservation) {
            return delegate.put(encodedArtifact, reservation);
        }
        @Override public <T> T publishReferenced(
                byte[] encodedArtifact, java.util.function.LongConsumer reservation,
                ArtifactReferencePublisher<T> publisher) {
            return delegate.publishReferenced(encodedArtifact, reservation, publisher);
        }
        @Override public byte[] get(String artifactId) { return delegate.get(artifactId); }
        @Override public byte[] get(String artifactId,
                                    java.util.function.LongConsumer reservation) {
            return delegate.get(artifactId, reservation);
        }
        @Override public ArtifactBackfillStatus inventoryStatus() {
            return delegate.inventoryStatus();
        }
        @Override public ArtifactBackfillStatus advanceInventoryBackfill(int maxEntries) {
            return delegate.advanceInventoryBackfill(maxEntries);
        }
        @Override public int inventoryUsageShardCount() {
            return delegate.inventoryUsageShardCount();
        }
        @Override public io.rowweave.artifact.ArtifactInventoryUsage inventoryUsage(
                int shard) {
            return delegate.inventoryUsage(shard);
        }
        @Override public ArtifactCandidatePage scanCandidates(
                String cursor, int limit, long cutoffMillis) {
            return delegate.scanCandidates(cursor, limit, cutoffMillis);
        }
        @Override public ArtifactRecoveryPage scanRecovering(
                String cursor, int limit) {
            return delegate.scanRecovering(cursor, limit);
        }
        @Override public ArtifactCollectResult collect(
                ArtifactCandidate candidate, ArtifactGcToken token,
                ArtifactReferenceAuthority references) {
            return delegate.collect(candidate, token, references);
        }
    }
}
