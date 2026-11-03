package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.ArtifactRepositoryProvider;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RecoverableRunCatalog;
import io.rowweave.run.RecoverableRunCatalogProvider;
import io.rowweave.run.RunRecoveryOwnership;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunRecoveryOwnershipProvider;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterRecoveryInventoryBarrierTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));

    @Test
    void emptyInitialInventoryOpensDurableSubmitBeforeAnyRunMutation() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        Submission submission = submission("new-run", "definition-new");
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());

            RpcMessage response = master.handler().apply(
                    RpcMessage.submitRun(submission.envelope));
            assertFalse(response.success());
            assertTrue(response.errorMessage().contains("no registered worker"),
                    response.errorMessage());
            assertNotNull(registry.runStateStore().get("new-run"));
        }
    }

    @Test
    void inheritedRunIsClaimedAndBlocksUnrelatedDurableAdmission() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        createUnsafeInherited(registry, "inherited", "definition-old");
        Submission submission = submission("must-not-start", "definition-new");
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_CORRUPT,
                    master.recoveryInventorySnapshot().state());

            RpcMessage response = master.handler().apply(
                    RpcMessage.submitRun(submission.envelope));
            assertFalse(response.success());
            assertTrue(response.errorMessage().contains("recovery inventory"),
                    response.errorMessage());
            assertNull(registry.runStateStore().get("must-not-start"));
            RecoveryToken owner = registry.runRecoveryOwnership().ownership("inherited");
            assertNotNull(owner);
            assertEquals(election.termToken(), owner.owner().leaderTermToken());
        }
    }

    @Test
    void failoverLeaderReplacesObservedOwnershipEpochWithoutStandbyClaim() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        createUnsafeInherited(registry, "failover", "definition-old");
        MasterServer first = new MasterServer(5_000L, 0, registry);
        MasterServer second = new MasterServer(5_000L, 0, registry);
        MasterElection firstElection = new MasterElection(
                registry, new Endpoint("127.0.0.1", first.port()));
        MasterElection secondElection = new MasterElection(
                registry, new Endpoint("127.0.0.1", second.port()));
        try {
            first.bindLeadership(firstElection);
            second.bindLeadership(secondElection);
            firstElection.start();
            secondElection.start();
            assertTrue(firstElection.isLeader());
            assertFalse(secondElection.isLeader());
            long firstGeneration = first.recoveryInventoryGeneration();
            assertTrue(first.awaitRecoveryInventory(firstGeneration, 5, TimeUnit.SECONDS));
            RecoveryToken firstOwner = registry.runRecoveryOwnership().ownership("failover");
            assertEquals(1L, firstOwner.ownershipEpoch());
            assertEquals(firstElection.termToken(), firstOwner.owner().leaderTermToken());
            assertEquals(0L, second.recoveryInventoryGeneration());

            firstElection.close();
            assertTrue(secondElection.isLeader());
            long secondGeneration = second.recoveryInventoryGeneration();
            assertTrue(second.awaitRecoveryInventory(secondGeneration, 5, TimeUnit.SECONDS));
            RecoveryToken replacement = registry.runRecoveryOwnership().ownership("failover");
            assertEquals(2L, replacement.ownershipEpoch());
            assertEquals(secondElection.termToken(), replacement.owner().leaderTermToken());
            assertFalse(registry.runRecoveryOwnership().current(firstOwner));
        } finally {
            secondElection.close();
            firstElection.close();
            second.close();
            first.close();
        }
    }

    @Test
    void missingRecoveryCapabilityBlocksBeforeArtifactOrRunMutation() {
        CapabilityMissingRegistry registry = new CapabilityMissingRegistry();
        Submission submission = submission("capability-blocked", "definition");
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            RpcMessage response = master.handler().apply(
                    RpcMessage.submitRun(submission.envelope));
            assertFalse(response.success());
            assertTrue(response.errorMessage().contains("capability unavailable"),
                    response.errorMessage());
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_CAPABILITY,
                    master.recoveryInventorySnapshot().state());
            assertNull(registry.store.get("capability-blocked"));
        }
    }

    @Test
    void leadershipListenerNeverBlocksOnCatalogIo() throws Exception {
        BlockingRecoveryRegistry registry = new BlockingRecoveryRegistry();
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            long startedAt = System.nanoTime();
            election.start();
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startedAt);
            assertTrue(elapsedMillis < 500L,
                    "leadership listener blocked for " + elapsedMillis + "ms");
            assertTrue(registry.scanEntered.await(5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.SCANNING,
                    master.recoveryInventorySnapshot().state());
            registry.releaseScan.countDown();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    private static void createInherited(
            InProcessRegistry registry, String runId, String definitionId) {
        byte[] artifact = artifact(definitionId);
        ArtifactRepository repository =
                ((ArtifactRepositoryProvider) registry).artifactRepository();
        String artifactId = repository.put(artifact);
        RunStateStore store = registry.runStateStore();
        assertEquals(RunDecision.Code.ACCEPTED, store.apply(RunCommand.createRun(
                "create-" + runId, runId, definitionId, artifactId,
                1, 0L, 1L)).code());
    }

    private static void createUnsafeInherited(
            InProcessRegistry registry, String runId, String definitionId) {
        createInherited(registry, runId, definitionId);
        RunStateStore store = registry.runStateStore();
        RunDecision attempted = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, 1L,
                "attempt-" + runId, "publication-" + runId, 2L));
        assertEquals(RunDecision.Code.ACCEPTED, attempted.code());
        RunDecision dispatched = store.apply(RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, attempted.snapshot().revision(),
                "attempt-" + runId,
                attempted.snapshot().currentAttempt().fenceToken(),
                io.rowweave.run.AttemptStatus.DISPATCHING,
                false, 3L, null, null));
        assertEquals(RunDecision.Code.ACCEPTED, dispatched.code());
    }

    private static Submission submission(String runId, String definitionId) {
        byte[] artifact = artifact(definitionId);
        RunSubmission submission = new RunSubmission(
                runId, "attempt-1", "publication-1", 1, 0L, 10L);
        return new Submission(RunSubmissionCodec.encode(submission, artifact));
    }

    private static byte[] artifact(String definitionId) {
        PlanGraph graph = new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.emptyList(), "inventory-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return PlanArtifactCodec.encode(LegacyPlanArtifactAdapter.wrap(definitionId, graph));
    }

    private static final class Submission {
        final byte[] envelope;
        Submission(byte[] envelope) { this.envelope = envelope; }
    }

    private static final class CapabilityMissingRegistry
            implements ServiceRegistry, RunStateStoreProvider, ArtifactRepositoryProvider {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final io.rowweave.run.InMemoryRunStateStore store =
                new io.rowweave.run.InMemoryRunStateStore();
        private final ArtifactRepository artifacts =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);

        @Override public RunStateStore runStateStore() { return store; }
        @Override public ArtifactRepository artifactRepository() { return artifacts; }
        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public java.util.List<RegistryWorker> liveWorkers() {
            return delegate.liveWorkers();
        }
        @Override public RegistryWorker worker(String workerId) {
            return delegate.worker(workerId);
        }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }

    private static final class BlockingRecoveryRegistry implements ServiceRegistry,
            RunStateStoreProvider, ArtifactRepositoryProvider,
            RecoverableRunCatalogProvider, RunRecoveryOwnershipProvider {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final io.rowweave.run.InMemoryRunStateStore store =
                new io.rowweave.run.InMemoryRunStateStore();
        private final ArtifactRepository artifacts =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        final CountDownLatch scanEntered = new CountDownLatch(1);
        final CountDownLatch releaseScan = new CountDownLatch(1);

        @Override public RunStateStore runStateStore() { return store; }
        @Override public ArtifactRepository artifactRepository() { return artifacts; }
        @Override public RecoverableRunCatalog recoverableRunCatalog() {
            return (cursor, limit) -> {
                scanEntered.countDown();
                try {
                    releaseScan.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("scan interrupted", interrupted);
                }
                return store.scan(cursor, limit);
            };
        }
        @Override public RunRecoveryOwnership runRecoveryOwnership() { return store; }
        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public java.util.List<RegistryWorker> liveWorkers() {
            return delegate.liveWorkers();
        }
        @Override public RegistryWorker worker(String workerId) {
            return delegate.worker(workerId);
        }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }
}
