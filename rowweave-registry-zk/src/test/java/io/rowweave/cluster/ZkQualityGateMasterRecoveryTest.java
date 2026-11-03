package io.rowweave.cluster;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.BatchSession;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.QualityEvidenceReadGuard;
import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGatePolicy;
import io.rowweave.QualityGatePolicyCodec;
import io.rowweave.QualityPreparedEvidence;
import io.rowweave.QualityPreparedEvidenceReader;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.artifact.FileArtifactRepository;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.zk.HookedZkServiceRegistry;
import io.rowweave.cluster.registry.zk.ZkRemoteJobClient;
import io.rowweave.cluster.registry.zk.ZkServiceRegistry;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.ops.RowValidation;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.QualityGateAssignmentBinding;
import io.rowweave.run.RecoveryFencedRunStateStore;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RetryAttemptIdentity;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunRecoveryOwnershipProvider;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.WholeAttemptRetryClass;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** fresh Master recovery cuts over the real ZK authority. */
class ZkQualityGateMasterRecoveryTest {
    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));

    @TempDir
    Path temporary;

    @Test
    @Timeout(value = 150, unit = TimeUnit.SECONDS)
    void decisionBeforeCrashRecomputesThroughZkAndReconcilesAckLoss()
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry setup = new ZkServiceRegistry(zk.getConnectString());
             HookedZkServiceRegistry masterRegistry =
                     new HookedZkServiceRegistry(
                             zk.getConnectString(), () -> { }, () -> { }, true)) {
            FileArtifactRepository repository = repository("before-decision");
            Path domain = temporary.resolve("before-decision-domain");
            Evidence evidence = prepare(setup, repository,
                    "zk-quality-before-decision", domain,
                    QualityGatePolicy.allowAll(), false, false);

            try (MasterServer master = master(masterRegistry, repository);
                 MasterElection election = election(masterRegistry, master)) {
                master.bindLeadership(election);
                election.start();
                awaitReadyRecovery(master);

                assertSucceeded(masterRegistry.runStateStore(),
                        masterRegistry.durableStageAssignmentAuthority(),
                        evidence, domain);
            }
        }
    }

    @Test
    @Timeout(value = 150, unit = TimeUnit.SECONDS)
    void recoveredDecisionOperationTimeoutReconcilesCommittedBytes()
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry setup = new ZkServiceRegistry(zk.getConnectString());
             HookedZkServiceRegistry masterRegistry =
                     new HookedZkServiceRegistry(
                             zk.getConnectString(), () -> { }, () -> { },
                             true, true)) {
            FileArtifactRepository repository = repository("operation-timeout");
            Path domain = temporary.resolve("operation-timeout-domain");
            Evidence evidence = prepare(setup, repository,
                    "zk-quality-operation-timeout", domain,
                    QualityGatePolicy.allowAll(), false, false);

            try (MasterServer master = master(masterRegistry, repository);
                 MasterElection election = election(masterRegistry, master)) {
                master.bindLeadership(election);
                election.start();
                awaitReadyRecovery(master);

                assertSucceeded(masterRegistry.runStateStore(),
                        masterRegistry.durableStageAssignmentAuthority(),
                        evidence, domain);
            }
        }
    }

    @Test
    @Timeout(value = 150, unit = TimeUnit.SECONDS)
    void passDecisionBeforeIntentContinuesThroughZkWithoutReevaluation()
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry setup = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry masterRegistry =
                     new ZkServiceRegistry(zk.getConnectString())) {
            FileArtifactRepository repository = repository("pass-decision");
            Path domain = temporary.resolve("pass-decision-domain");
            Evidence evidence = prepare(setup, repository,
                    "zk-quality-pass-decision", domain,
                    QualityGatePolicy.allowAll(), true, false);

            try (MasterServer master = master(masterRegistry, repository);
                 MasterElection election = election(masterRegistry, master)) {
                master.bindLeadership(election);
                election.start();
                awaitReadyRecovery(master);

                assertSucceeded(masterRegistry.runStateStore(),
                        masterRegistry.durableStageAssignmentAuthority(),
                        evidence, domain);
            }
        }
    }

    @Test
    @Timeout(value = 150, unit = TimeUnit.SECONDS)
    void policyMismatchBeforeDecisionBlocksRealZkRecovery()
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry setup = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry masterRegistry =
                     new ZkServiceRegistry(zk.getConnectString())) {
            FileArtifactRepository repository = repository("policy-mismatch");
            Path domain = temporary.resolve("policy-mismatch-domain");
            Evidence evidence = prepare(setup, repository,
                    "zk-quality-policy-mismatch", domain,
                    QualityGatePolicy.builder().maxRejectedRows(0L).build(),
                    QualityGatePolicy.allowAll(), false, false);

            try (MasterServer master = master(masterRegistry, repository);
                 MasterElection election = election(masterRegistry, master)) {
                master.bindLeadership(election);
                election.start();
                awaitRecovery(master);

                RunSnapshot unresolved = masterRegistry.runStateStore()
                        .get(evidence.runId);
                assertEquals(RunStatus.RUNNING, unresolved.status());
                assertEquals(AttemptStatus.COMMITTING,
                        unresolved.currentAttempt().status());
                assertNull(masterRegistry.durableStageAssignmentAuthority()
                        .qualityGateDecision(evidence.fence));
                assertNull(new ManifestFileStore(domain).latest());
                assertEquals(LeaderRecoveryInventory.State.BLOCKED_CORRUPT,
                        master.recoveryInventorySnapshot().state());
            }
        }
    }

    @Test
    @Timeout(value = 150, unit = TimeUnit.SECONDS)
    void failDecisionBeforeAbortCompletesZkAbortAndNeverPublishes()
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry setup = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry masterRegistry =
                     new ZkServiceRegistry(zk.getConnectString())) {
            FileArtifactRepository repository = repository("fail-decision");
            Path domain = temporary.resolve("fail-decision-domain");
            Evidence evidence = prepare(setup, repository,
                    "zk-quality-fail-decision", domain,
                    QualityGatePolicy.builder().maxRejectedRows(0L).build(),
                    true, false);

            try (MasterServer master = master(masterRegistry, repository);
                 MasterElection election = election(masterRegistry, master)) {
                master.bindLeadership(election);
                election.start();
                awaitReadyRecovery(master);

                RunSnapshot terminal = masterRegistry.runStateStore()
                        .get(evidence.runId);
                assertEquals(RunStatus.FAILED, terminal.status());
                assertEquals("QUALITY_GATE_REJECTED",
                        terminal.currentAttempt().reasonCode());
                BatchSinkAbortRecord abort = masterRegistry
                        .durableStageAssignmentAuthority()
                        .batchSinkAbort(evidence.fence);
                assertEquals(BatchSinkAbortRecord.State.ABORTED, abort.state());
                assertNull(new ManifestFileStore(domain).latest());
            }
        }
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void intentBeforeCrashIsPublishedOnlyByReplacementRecoveryOwner()
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry setup = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry firstRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry secondRegistry =
                     new ZkServiceRegistry(zk.getConnectString())) {
            FileArtifactRepository repository = repository("intent-replacement");
            Path domain = temporary.resolve("intent-replacement-domain");
            Evidence evidence = prepare(setup, repository,
                    "zk-quality-intent-replacement", domain,
                    QualityGatePolicy.builder().maxRejectedRows(0L).build(),
                    QualityGatePolicy.allowAll(), true, true);
            CountDownLatch standbyStarted = new CountDownLatch(1);
            CountDownLatch publishCutReached = new CountDownLatch(1);
            try (MasterServer first = master(firstRegistry, repository);
                 MasterServer second = master(secondRegistry, repository);
                 MasterElection firstElection = election(firstRegistry, first);
                 MasterElection secondElection = election(secondRegistry, second)) {
                first.bindLeadership(firstElection);
                second.bindLeadership(secondElection);
                first.setManifestPublishHooks(() -> {
                    publishCutReached.countDown();
                    try {
                        assertTrue(standbyStarted.await(20L, TimeUnit.SECONDS),
                                "standby election did not start");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(interrupted);
                    }
                    Thread leaderLoss = new Thread(firstElection::close,
                            "zk-quality-recovery-leader-loss");
                    leaderLoss.setDaemon(true);
                    leaderLoss.start();
                    throw new RuntimeException(
                            "injected leader loss before recovered publish");
                }, null);
                firstElection.start();
                assertTrue(firstElection.isLeader());
                secondElection.start();
                assertTrue(!secondElection.isLeader());
                standbyStarted.countDown();
                assertTrue(publishCutReached.await(20L, TimeUnit.SECONDS),
                        "first Master did not reach publish cut: "
                                + first.recoveryInventorySnapshot().state());

                awaitLeader(secondElection);
                awaitReadyRecovery(second);

                assertSucceeded(secondRegistry.runStateStore(),
                        secondRegistry.durableStageAssignmentAuthority(),
                        evidence, domain);
                assertEquals(2L, ((RunRecoveryOwnershipProvider) secondRegistry)
                        .runRecoveryOwnership().ownership(evidence.runId)
                        .ownershipEpoch());
            }
        }
    }

    @Test
    @Timeout(value = 150, unit = TimeUnit.SECONDS)
    void publicQueryFollowsCurrentAttemptAcrossMasterReplacement()
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry setup = new ZkServiceRegistry(zk.getConnectString());
             ZkRemoteJobClient queryClient =
                     new ZkRemoteJobClient(zk.getConnectString())) {
            FileArtifactRepository repository = repository("public-rollover");
            Path domain = temporary.resolve("public-rollover-domain");
            Evidence evidence = prepare(setup, repository,
                    "zk-quality-public-rollover", domain,
                    QualityGatePolicy.builder().maxRejectedRows(0L).build(),
                    QualityGatePolicy.allowAll(), true, false, 2);
            RecoveryToken recoveryToken;

            try (ZkServiceRegistry firstRegistry =
                         new ZkServiceRegistry(zk.getConnectString());
                 MasterServer first = master(firstRegistry, repository);
                 MasterElection firstElection = election(firstRegistry, first)) {
                first.bindLeadership(firstElection);
                firstElection.start();
                awaitRecovery(first);

                QualityGateDecision previous =
                        queryClient.queryQualityGateDecision(evidence.runId);
                assertNotNull(previous);
                assertEquals(evidence.fence, previous.fence());
                recoveryToken = ((RunRecoveryOwnershipProvider) firstRegistry)
                        .runRecoveryOwnership().ownership(evidence.runId);
                assertNotNull(recoveryToken);
            }

            RunSnapshot current = setup.runStateStore().get(evidence.runId);
            current = acceptedRecovered(setup, recoveryToken,
                    RunCommand.advanceAttempt(
                            "reconciling-public-rollover", current.runId(),
                            current.revision(),
                            current.currentAttempt().attemptId(),
                            current.currentAttempt().fenceToken(),
                            AttemptStatus.RECONCILING, false, 80L,
                            "COMMIT_ACK_LOST", "retry current attempt"));
            current = acceptedRecovered(setup, recoveryToken,
                    retryAuthorityAdvance(current,
                            WholeAttemptRetryClass.WORKER_LOST, 81L));
            RetryAttemptIdentity retry = RetryAttemptIdentity.derive(
                    current.runId(), current.artifactId(), 2);
            current = acceptedRecovered(setup, recoveryToken,
                    retryAuthorityCreateAttempt(current, retry, 82L));
            RunAttemptFence replacementFence = new RunAttemptFence(
                    current.runId(), current.currentAttempt().attemptId(),
                    current.currentAttempt().fenceToken());
            assertTrue(!evidence.fence.equals(replacementFence));

            try (ZkServiceRegistry secondRegistry =
                         new ZkServiceRegistry(zk.getConnectString());
                 MasterServer second = master(secondRegistry, repository);
                 MasterElection secondElection = election(secondRegistry, second)) {
                second.bindLeadership(secondElection);
                secondElection.start();
                awaitLeader(secondElection);

                assertNull(queryClient.queryQualityGateDecision(evidence.runId));
            }
        }
    }

    private FileArtifactRepository repository(String name) {
        return new FileArtifactRepository(temporary.resolve(name + "-artifacts"));
    }

    private static MasterServer master(
            io.rowweave.cluster.registry.ServiceRegistry registry,
            FileArtifactRepository repository) {
        return new MasterServer(5_000L, 0, registry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
    }

    private static MasterElection election(
            io.rowweave.cluster.registry.ServiceRegistry registry,
            MasterServer master) {
        return new MasterElection(
                registry, new Endpoint("127.0.0.1", master.port()));
    }

    private static void awaitRecovery(MasterServer master)
            throws InterruptedException {
        // This fault-injection suite asserts the recovered authority, not a
        // recovery-time SLA on a shared host running many isolated ZK JVMs.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60L);
        long generation;
        do {
            generation = master.recoveryInventoryGeneration();
            if (generation > 0L) break;
            Thread.sleep(20L);
        } while (System.nanoTime() < deadline);
        assertTrue(generation > 0L, "recovery inventory did not start");
        assertTrue(master.awaitRecoveryInventory(
                generation, 60L, TimeUnit.SECONDS));
    }

    private static void awaitReadyRecovery(MasterServer master)
            throws InterruptedException {
        awaitRecovery(master);
        long generation = master.recoveryInventoryGeneration();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60L);
        LeaderRecoveryInventory.Snapshot snapshot;
        do {
            snapshot = master.recoveryInventorySnapshot();
            if (snapshot.generation() == generation
                    && snapshot.state()
                    == LeaderRecoveryInventory.State.EMPTY_READY) {
                return;
            }
            Thread.sleep(20L);
        } while (System.nanoTime() < deadline);
        assertEquals(generation, snapshot.generation(),
                "recovery inventory generation changed");
        assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                snapshot.state(), "recovery remains unsettled: activeRuns="
                        + snapshot.activeRuns() + " failure="
                        + snapshot.failure());
    }

    private static void awaitLeader(MasterElection election)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
        while (!election.isLeader() && System.nanoTime() < deadline) {
            Thread.sleep(20L);
        }
        assertTrue(election.isLeader(), "replacement Master did not become leader");
    }

    private static void assertSucceeded(
            RunStateStore store, DurableStageAssignmentAuthority authority,
            Evidence evidence, Path domain) {
        RunSnapshot terminal = store.get(evidence.runId);
        assertEquals(RunStatus.SUCCEEDED, terminal.status());
        QualityGateDecision decision = authority.qualityGateDecision(evidence.fence);
        assertNotNull(decision);
        assertEquals(QualityGateDecision.Outcome.PASS, decision.outcome());
        assertEquals("publication-" + evidence.runId,
                new ManifestFileStore(domain).latest().attemptId());
    }

    private static Evidence prepare(
            ZkServiceRegistry registry, FileArtifactRepository repository,
            String runId, Path domain, QualityGatePolicy policy,
            boolean recordDecision, boolean commitIntent) throws Exception {
        return prepare(registry, repository, runId, domain, policy, policy,
                recordDecision, commitIntent);
    }

    private static Evidence prepare(
            ZkServiceRegistry registry, FileArtifactRepository repository,
            String runId, Path domain, QualityGatePolicy artifactPolicy,
            QualityGatePolicy assignmentPolicy,
            boolean recordDecision, boolean commitIntent) throws Exception {
        return prepare(registry, repository, runId, domain, artifactPolicy,
                assignmentPolicy, recordDecision, commitIntent, 1);
    }

    private static Evidence prepare(
            ZkServiceRegistry registry, FileArtifactRepository repository,
            String runId, Path domain, QualityGatePolicy artifactPolicy,
            QualityGatePolicy assignmentPolicy,
            boolean recordDecision, boolean commitIntent,
            int maxAttempts) throws Exception {
        String definitionId = "definition-" + runId;
        String publicationId = "publication-" + runId;
        Path seed = domain.resolveSibling(domain.getFileName() + "-seed");
        new io.rowweave.local.LocalJobClient().submit(
                qualityArtifact(definitionId + "-seed", seed,
                        QualityGatePolicy.allowAll()));
        PlanArtifact artifact = qualityArtifact(
                definitionId, domain, artifactPolicy);
        PlanGraph graph = LegacyPlanArtifactAdapter.unwrapValidated(artifact);
        String artifactId = repository.put(PlanArtifactCodec.encode(artifact));
        RunSnapshot run = accepted(registry, RunCommand.createRun(
                "create-" + runId, runId, definitionId, artifactId,
                maxAttempts, 0L, 1L));
        run = accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, run.revision(),
                "attempt-" + runId, publicationId, 2L));
        run = accepted(registry, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, run.revision(),
                run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        RunAttemptFence fence = new RunAttemptFence(
                runId, run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());

        ExecutionGraph execution = ExecutionGraphBuilder.buildWithSinkBoundary(graph);
        List<StageAssignmentRecord> records = new ArrayList<>();
        List<QualityGateAssignmentBinding.MemberBinding> bindings =
                new ArrayList<>();
        List<ManifestFileSink> outputs = new ArrayList<>();
        for (String role : QualityGateAssignmentBinding.canonicalRoles()) {
            int nodeIndex = qualitySinkNode(graph, role);
            Stage stage = stageContaining(execution, nodeIndex);
            int ordinal = records.size();
            StageAssignmentRecord record = StageAssignmentRecord.create(
                    fence, 1L, ordinal, 900L + ordinal,
                    stage.index(), 0, StageAssignmentRole.SINK,
                    "departed-zk-quality-" + ordinal, "127.0.0.1",
                    63_100 + ordinal, -1, "departed-zk-inc-" + ordinal);
            records.add(record);
            PlanNode node = graph.node(nodeIndex);
            outputs.add((ManifestFileSink) node.output());
            bindings.add(new QualityGateAssignmentBinding.MemberBinding(
                    ordinal, role, BatchSinkDescriptorCodec.digest(
                    node.batchSinkDescriptor())));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, artifactId, 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1, records,
                new QualityGateAssignmentBinding(
                        QualityGatePolicyCodec.digest(assignmentPolicy), bindings));
        DurableStageAssignmentAuthority authority =
                registry.durableStageAssignmentAuthority();
        StageAssignmentSnapshot assignments = authority.publish(manifest).snapshot();
        List<byte[]> commandDigests = new ArrayList<>();
        for (int ordinal = 0; ordinal < records.size(); ordinal++) {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    ("zk-quality-command-" + runId + '-' + ordinal)
                            .getBytes(StandardCharsets.UTF_8));
            commandDigests.add(digest);
            authority.accept(new StageAssignmentAcceptance(
                    assignments.token(ordinal), digest, 4L));
        }
        run = accepted(registry, RunCommand.advanceAttempt(
                "running-" + runId, runId, run.revision(),
                run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken(),
                AttemptStatus.RUNNING, false, 5L, null, null));
        for (int ordinal = 0; ordinal < records.size(); ordinal++) {
            authority.recordTerminal(StageTerminalReceipt.succeeded(
                    assignments.token(ordinal), commandDigests.get(ordinal),
                    1L, 1L, 6L));
        }
        accepted(registry, RunCommand.advanceAttempt(
                "committing-" + runId, runId, run.revision(),
                run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken(),
                AttemptStatus.COMMITTING, false, 7L, null, null));

        long generation = registry.manifestCommitAuthority()
                .reserve(domain, publicationId);
        for (int ordinal = 0; ordinal < outputs.size(); ordinal++) {
            ManifestFileSink output = outputs.get(ordinal);
            output.bindManifestAttempt(publicationId, generation);
            try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                    output.getSchema(), seed.toString(),
                    output.memberName()).openReader()) {
                while (reader.hasNext()) output.collect(reader.next());
            }
            output.prepareCommit();
            output.commitPrepared();
            output.sealInstalledMember();
            io.rowweave.commit.CommitManifest.Member member =
                    output.installedMember();
            SinkAttemptIdentity identity =
                    DurableStageAssignmentPlan.sinkAttemptIdentity(
                            manifest, records.get(ordinal), graph);
            long sealedAt = 8L + ordinal;
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    output.domainId(), ManifestCommitWire.encodeMember(member),
                    member.length(), member.sha256(), sealedAt);
            authority.recordManifestMember(ManifestMemberReceipt.sealed(
                    assignments.token(ordinal), commandDigests.get(ordinal),
                    output.domainId(), generation, publicationId,
                    member, sealedAt, locator));
        }
        if (recordDecision) {
            ManifestCommitJournalSnapshot journal =
                    authority.manifestCommitJournal(fence);
            QualityPreparedEvidence prepared = QualityPreparedEvidenceReader.read(
                    graph, journal, QualityEvidenceReadGuard.until(
                            System.nanoTime() + TimeUnit.SECONDS.toNanos(5L),
                            () -> false));
            authority.recordQualityGateDecision(QualityGateDecision.evaluate(
                    artifactId, fence, publicationId,
                    assignmentPolicy, prepared));
        }
        if (commitIntent) {
            authority.commitManifestIntent(
                    authority.manifestCommitJournal(fence).buildIntent());
        }
        return new Evidence(runId, fence);
    }

    private static PlanArtifact qualityArtifact(
            String definitionId, Path domain, QualityGatePolicy policy) {
        BatchSession session = new BatchSession("zk-quality-recovery", "recovery");
        session.setDefinitionId(definitionId);
        session.setParallelism(2);
        DataRowClassification classified = session.fromSource(new CollectionSource(
                        META, "zk-quality-split", Arrays.asList(
                        row("A", 1), row(null, 2), row("B", 13))))
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(RowValidation.builder(META)
                        .notNull("adaptor.required", 0)
                        .integerRange("period.range", 1, 1, 12)
                        .build());
        classified.writeToManifest(TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary")
                .withQualityGate(policy));
        return session.toPlanArtifact();
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static int qualitySinkNode(PlanGraph graph, String role) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.SINK
                    && node.output() instanceof ManifestFileSink
                    && role.equals(((ManifestFileSink) node.output())
                    .qualityGateRole())) return i;
        }
        throw new AssertionError("missing quality Sink role " + role);
    }

    private static Stage stageContaining(ExecutionGraph graph, int nodeIndex) {
        for (Stage stage : graph.stages()) {
            if (stage.members().contains(nodeIndex)) return stage;
        }
        throw new AssertionError("missing stage for node " + nodeIndex);
    }

    private static RunSnapshot accepted(
            ZkServiceRegistry registry, RunCommand command) {
        RunDecision decision = registry.runStateStore().apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static RunSnapshot acceptedRecovered(
            ZkServiceRegistry registry, RecoveryToken token,
            RunCommand command) {
        RunDecision decision = ((RecoveryFencedRunStateStore)
                registry.runStateStore()).applyRecovered(token, command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static RunCommand retryAuthorityAdvance(
            RunSnapshot current, WholeAttemptRetryClass retryClass,
            long eventTimeMillis) throws Exception {
        java.lang.reflect.Method method = RunCommand.class.getDeclaredMethod(
                "retryAuthorityAdvance", String.class, String.class,
                long.class, String.class, long.class, long.class,
                WholeAttemptRetryClass.class, boolean.class);
        method.setAccessible(true);
        return (RunCommand) method.invoke(null, "ready-public-rollover",
                current.runId(), current.revision(),
                current.currentAttempt().attemptId(),
                current.currentAttempt().fenceToken(), eventTimeMillis,
                retryClass, false);
    }

    private static RunCommand retryAuthorityCreateAttempt(
            RunSnapshot current, RetryAttemptIdentity retry,
            long eventTimeMillis) throws Exception {
        java.lang.reflect.Method method = RunCommand.class.getDeclaredMethod(
                "retryAuthorityCreateAttempt", String.class, String.class,
                long.class, RetryAttemptIdentity.class, long.class);
        method.setAccessible(true);
        return (RunCommand) method.invoke(null, retry.commandId("create"),
                current.runId(), current.revision(), retry, eventTimeMillis);
    }

    private static final class Evidence {
        final String runId;
        final RunAttemptFence fence;

        Evidence(String runId, RunAttemptFence fence) {
            this.runId = runId;
            this.fence = fence;
        }
    }
}
