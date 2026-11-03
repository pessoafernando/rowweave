package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
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
import io.rowweave.run.DurableRunDisposition;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.DurableStageAssignmentAuthorityProvider;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterArtifactReferencePublicationTest {
    private static final DataSchema META =
            new DataSchema(new Field("id", DataType.IntegerType));

    @Test
    void guardedPublicationCreatesReferenceBeforeGuardReturns() {
        InProcessRegistry registry = new InProcessRegistry();
        ArtifactReferenceAuthority references =
                ((ArtifactReferenceAuthorityProvider) registry).artifactReferenceAuthority();
        GuardedProbeRepository repository = new GuardedProbeRepository(true);
        Submission fixture = submission("run-guarded", "definition-guarded");
        repository.afterCallback = () -> assertEquals(
                ArtifactReferenceView.Code.READY_REFERENCED,
                references.inspect(fixture.artifactId).code());

        try (MasterServer master = master(registry, repository)) {
            RpcMessage response = master.handler().apply(
                    RpcMessage.submitRun(fixture.envelope));

            assertFalse(response.success());
            assertEquals(1, repository.guardedCalls.get());
            assertEquals(0, repository.unguardedCalls.get());
            assertNotNull(runStore(registry).get(fixture.runId));
            assertEquals(1L, references.inspect(fixture.artifactId).referenceCount());
        }
    }

    @Test
    void repositoryFailureBeforeCallbackLeavesNoVisibleRun() {
        InProcessRegistry registry = new InProcessRegistry();
        GuardedProbeRepository repository = new GuardedProbeRepository(false);
        repository.failBeforeCallback.set(true);
        Submission fixture = submission("run-quota-failure", "definition-quota");

        try (MasterServer master = master(registry, repository)) {
            RpcMessage response = master.handler().apply(
                    RpcMessage.submitRun(fixture.envelope));

            assertFalse(response.success());
            assertEquals(DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE,
                    response.durableRunDisposition());
            assertEquals(1, repository.guardedCalls.get());
            assertNull(runStore(registry).get(fixture.runId));
        }
    }

    @Test
    void callbackAckLossLeavesReferencedRunAndReplayDoesNotDuplicateReference() {
        InProcessRegistry registry = new InProcessRegistry();
        ArtifactReferenceAuthority references =
                ((ArtifactReferenceAuthorityProvider) registry).artifactReferenceAuthority();
        GuardedProbeRepository repository = new GuardedProbeRepository(false);
        repository.loseFirstCallbackAck.set(true);
        Submission fixture = submission("run-callback-ack-loss", "definition-ack-loss");

        try (MasterServer master = master(registry, repository)) {
            RpcMessage first = master.handler().apply(
                    RpcMessage.submitRun(fixture.envelope));
            RunSnapshot afterUnknownAck = runStore(registry).get(fixture.runId);

            assertEquals(DurableRunDisposition.ACCEPTED_NON_TERMINAL,
                    first.durableRunDisposition());
            assertNotNull(afterUnknownAck);
            assertNull(afterUnknownAck.currentAttempt(),
                    "attempt creation must happen only after guarded CREATE/ref returns");
            assertEquals(1L, references.inspect(fixture.artifactId).referenceCount());

            master.handler().apply(RpcMessage.submitRun(fixture.envelope));
            assertEquals(1L, references.inspect(fixture.artifactId).referenceCount());
            assertEquals(2, repository.guardedCalls.get());
        }
    }

    @Test
    void sharedArtifactAndTwoMastersRetainOneExactReferencePerRun() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        ArtifactReferenceAuthority references =
                ((ArtifactReferenceAuthorityProvider) registry).artifactReferenceAuthority();
        GuardedProbeRepository repository = new GuardedProbeRepository(true);
        Submission first = submission("run-shared-a", "definition-shared");
        Submission second = submission("run-shared-b", "definition-shared");
        CountDownLatch start = new CountDownLatch(1);
        RpcMessage[] responses = new RpcMessage[2];

        try (MasterServer masterA = master(registry, repository);
             MasterServer masterB = master(registry, repository)) {
            Thread a = new Thread(() -> responses[0] = submitAfter(start, masterA, first),
                    "arl6-master-a");
            Thread b = new Thread(() -> responses[1] = submitAfter(start, masterB, second),
                    "arl6-master-b");
            a.start();
            b.start();
            start.countDown();
            a.join(TimeUnit.SECONDS.toMillis(10));
            b.join(TimeUnit.SECONDS.toMillis(10));

            assertFalse(a.isAlive());
            assertFalse(b.isAlive());
            assertNotNull(responses[0]);
            assertNotNull(responses[1]);
            assertNotNull(runStore(registry).get(first.runId));
            assertNotNull(runStore(registry).get(second.runId));
            assertEquals(first.artifactId, second.artifactId);
            assertEquals(2L, references.inspect(first.artifactId).referenceCount());
            assertEquals(2, repository.guardedCalls.get());
            assertEquals(1, repository.maxCallbacksInFlight.get());
        }
    }

    @Test
    void capabilityIdentityMismatchRejectsBeforePublicationOrRunVisibility() {
        MismatchedAuthorityRegistry registry = new MismatchedAuthorityRegistry();
        GuardedProbeRepository repository = new GuardedProbeRepository(false);
        Submission fixture = submission("run-mismatched-authority", "definition-mismatch");

        try (MasterServer master = master(registry, repository)) {
            RpcMessage response = master.handler().apply(
                    RpcMessage.submitRun(fixture.envelope));

            assertFalse(response.success());
            assertEquals(DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE,
                    response.durableRunDisposition());
            assertTrue(response.errorMessage().contains(
                    "artifact lifecycle capability unavailable"), response.errorMessage());
            assertEquals(0, repository.guardedCalls.get());
            assertNull(registry.runStateStore().get(fixture.runId));
        }
    }

    @Test
    void conflictingEnvelopeCannotBorrowAnExistingRunAcceptance() {
        InProcessRegistry registry = new InProcessRegistry();
        ArtifactReferenceAuthority references =
                ((ArtifactReferenceAuthorityProvider) registry).artifactReferenceAuthority();
        GuardedProbeRepository repository = new GuardedProbeRepository(false);
        Submission accepted = submission("run-envelope-conflict", "definition-accepted");
        Submission conflict = submission("run-envelope-conflict", "definition-conflict");

        try (MasterServer master = master(registry, repository)) {
            master.handler().apply(RpcMessage.submitRun(accepted.envelope));
            RpcMessage rejected = master.handler().apply(
                    RpcMessage.submitRun(conflict.envelope));

            assertEquals(DurableRunDisposition.REJECTED_BEFORE_ACCEPTANCE,
                    rejected.durableRunDisposition());
            assertTrue(rejected.errorMessage().contains("create run rejected")
                    || rejected.errorMessage().contains("run identity conflicts"),
                    rejected.errorMessage());
            assertEquals(accepted.artifactId,
                    runStore(registry).get(accepted.runId).artifactId());
            assertEquals(1L, references.inspect(accepted.artifactId).referenceCount());
            assertEquals(ArtifactReferenceView.Code.READY_UNREFERENCED,
                    references.inspect(conflict.artifactId).code());
        }
    }

    private static MasterServer master(
            ServiceRegistry registry, ArtifactLifecycleRepository repository) {
        return new MasterServer(5_000L, 0, registry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
    }

    private static RpcMessage submitAfter(
            CountDownLatch start, MasterServer master, Submission submission) {
        try {
            assertTrue(start.await(10, TimeUnit.SECONDS));
            return master.handler().apply(RpcMessage.submitRun(submission.envelope));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static RunStateStore runStore(InProcessRegistry registry) {
        return ((RunStateStoreProvider) registry).runStateStore();
    }

    private static Submission submission(String runId, String definitionId) {
        PlanGraph graph = new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.emptyList(), "artifact-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        byte[] artifact = PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap(definitionId, graph));
        RunSubmission submission = new RunSubmission(
                runId, "attempt-1", "publication-1", 1, 0L, 10L);
        return new Submission(runId, PlanArtifactCodec.artifactId(artifact),
                RunSubmissionCodec.encode(submission, artifact));
    }

    private static final class Submission {
        final String runId;
        final String artifactId;
        final byte[] envelope;

        private Submission(String runId, String artifactId, byte[] envelope) {
            this.runId = runId;
            this.artifactId = artifactId;
            this.envelope = envelope;
        }
    }

    private static final class GuardedProbeRepository
            implements ArtifactLifecycleRepository, AutoCloseable {
        private final InMemoryArtifactRepository delegate =
                new InMemoryArtifactRepository(32, 32L * 1024L * 1024L);
        private final boolean rejectUnguarded;
        private final AtomicInteger guardedCalls = new AtomicInteger();
        private final AtomicInteger unguardedCalls = new AtomicInteger();
        private final AtomicInteger callbacksInFlight = new AtomicInteger();
        private final AtomicInteger maxCallbacksInFlight = new AtomicInteger();
        private final AtomicBoolean failBeforeCallback = new AtomicBoolean();
        private final AtomicBoolean loseFirstCallbackAck = new AtomicBoolean();
        private volatile Runnable afterCallback = () -> { };

        private GuardedProbeRepository(boolean rejectUnguarded) {
            this.rejectUnguarded = rejectUnguarded;
        }

        @Override
        public String put(byte[] encodedArtifact) {
            return put(encodedArtifact, ignored -> { });
        }

        @Override
        public String put(byte[] encodedArtifact, LongConsumer reservation) {
            unguardedCalls.incrementAndGet();
            if (rejectUnguarded) {
                throw new IllegalStateException("unguarded Artifact put forbidden");
            }
            return delegate.put(encodedArtifact, reservation);
        }

        @Override
        public <T> T publishReferenced(
                byte[] encodedArtifact, LongConsumer reservation,
                ArtifactReferencePublisher<T> publisher) {
            guardedCalls.incrementAndGet();
            if (failBeforeCallback.get()) {
                throw new IllegalStateException("injected Artifact quota failure");
            }
            T result = delegate.publishReferenced(encodedArtifact, reservation, artifactId -> {
                int active = callbacksInFlight.incrementAndGet();
                maxCallbacksInFlight.accumulateAndGet(active, Math::max);
                try {
                    return publisher.publish(artifactId);
                } finally {
                    callbacksInFlight.decrementAndGet();
                }
            });
            afterCallback.run();
            if (loseFirstCallbackAck.compareAndSet(true, false)) {
                throw new IllegalStateException("injected callback ACK loss");
            }
            return result;
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
        @Override public byte[] get(String artifactId) { return delegate.get(artifactId); }
        @Override public byte[] get(String artifactId, LongConsumer reservation) {
            return delegate.get(artifactId, reservation);
        }
        @Override public void close() { delegate.close(); }
    }

    private static final class MismatchedAuthorityRegistry
            implements ServiceRegistry, RunStateStoreProvider,
            DurableStageAssignmentAuthorityProvider, ArtifactReferenceAuthorityProvider {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final ArtifactReferenceAuthority wrongAuthority =
                new InMemoryRunStateStore();

        @Override public RunStateStore runStateStore() {
            return ((RunStateStoreProvider) delegate).runStateStore();
        }
        @Override public DurableStageAssignmentAuthority durableStageAssignmentAuthority() {
            return ((DurableStageAssignmentAuthorityProvider) delegate)
                    .durableStageAssignmentAuthority();
        }
        @Override public ArtifactReferenceAuthority artifactReferenceAuthority() {
            return wrongAuthority;
        }
        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() { return delegate.liveWorkers(); }
        @Override public RegistryWorker worker(String workerId) { return delegate.worker(workerId); }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }
}

