package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.SignedAdversarialProviderFixture;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** signed provider must cross the actual client/Master/Worker control path. */
class SignedProviderRealClusterTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void signedProviderCommitsThroughRealClusterControlPath(@TempDir Path tmp)
            throws Exception {
        Files.createDirectories(tmp.resolve("deployment"));
        SignedAdversarialProviderFixture.Session session =
                SignedAdversarialProviderFixture.open(tmp.resolve("deployment"));
        DeploymentRuntime runtime = session.workerRuntime();
        InProcessRegistry registry = new InProcessRegistry();
        AdversarialDurableSinkAuthorityAdapter authority =
                new AdversarialDurableSinkAuthorityAdapter();
        Path spill = tmp.resolve("spill");
        Files.createDirectories(spill);
        Path output = tmp.resolve("output");
        try (MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     registry.artifactRepository(), runtime, authority);
             WorkerServer worker = new WorkerServer(
                     "signed-provider-worker", 4,
                     new FixedMasterLocator(new Endpoint("127.0.0.1", master.port())),
                     spill.toFile(), runtime);
             RealClusterJobClient client = new RealClusterJobClient(
                     new FixedMasterLocator(new Endpoint("127.0.0.1", master.port())),
                     runtime)) {
            DataRow first = new DataRow(1);
            first.set(0, 7);
            DataRow second = new DataRow(1);
            second.set(0, 8);
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    DurableTestSources.source(META, Arrays.asList(first, second),
                                    "signed-provider-source")
                            .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                    PlanNode.sink(META, 0,
                                    SignedAdversarialProviderFixture.output(
                                            META, output, "ledger"))
                            .build().withParam(
                                    PlanNode.OPERATOR_UID_PARAM, "output")), 1);
            RunSubmission submission = new RunSubmission(
                    "signed-provider-run", "attempt-1", "publication-1",
                    1, 0L, 10L);

            ExecutionReport report = client.submitRun(
                    graph, "signed-provider-definition", submission);

            assertEquals(2L, report.rowsOut());
            assertEquals(RunStatus.SUCCEEDED,
                    client.queryRun("signed-provider-run").status());
            BatchSinkOutcome outcome =
                    client.queryBatchSinkOutcome("signed-provider-run");
            assertEquals(BatchSinkOutcome.State.COMMITTED, outcome.state());
            assertTrue(outcome.generation() > 0L);
            assertEquals(32, outcome.identityDigest().length);
            assertTrue(Files.exists(output.resolve("publications")
                    .resolve(outcome.generation() + ".visible")));
            assertEquals(0, worker.preparedOutputActiveCount());
        } finally {
            runtime.close();
        }
    }

    @Test
    void publicQueryProjectsProviderConflictAndWrongIdentity(@TempDir Path tmp)
            throws Exception {
        Files.createDirectories(tmp.resolve("deployment"));
        SignedAdversarialProviderFixture.Session session =
                SignedAdversarialProviderFixture.open(tmp.resolve("deployment"));
        DeploymentRuntime runtime = session.workerRuntime();
        InProcessRegistry registry = new InProcessRegistry();
        AdversarialDurableSinkAuthorityAdapter authority =
                new AdversarialDurableSinkAuthorityAdapter();
        Path spill = tmp.resolve("spill");
        Files.createDirectories(spill);
        try (MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     registry.artifactRepository(), runtime, authority);
             WorkerServer worker = new WorkerServer(
                     "signed-provider-query-worker", 4,
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())),
                     spill.toFile(), runtime);
             RealClusterJobClient client = new RealClusterJobClient(
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), runtime)) {
            assertTamperedPublicQuery(client, master,
                    tmp.resolve("conflict-output"), "query-conflict-run",
                    "CORRUPT", "PROVIDER_CONFLICT");
            assertTamperedPublicQuery(client, master,
                    tmp.resolve("wrong-identity-output"),
                    "query-wrong-identity-run", "WRONG",
                    "PROVIDER_QUERY_IDENTITY_CONFLICT");
        } finally {
            runtime.close();
        }
    }

    private static void assertTamperedPublicQuery(
            RealClusterJobClient client, MasterServer master, Path output,
            String runId, String markerValue, String expectedDetail)
            throws Exception {
        java.util.concurrent.CountDownLatch decided =
                new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release =
                new java.util.concurrent.CountDownLatch(1);
        master.setDurableDecisionHooksForTesting(null, () -> {
            decided.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        CompletableFuture<Throwable> submitted = CompletableFuture.supplyAsync(
                () -> {
                    try {
                        client.submitRun(graph(output), "definition-" + runId,
                                new RunSubmission(runId, "attempt-1",
                                        "publication-1", 1, 0L, 10L));
                        return null;
                    } catch (Throwable failure) {
                        return failure;
                    }
                });
        assertTrue(decided.await(10, TimeUnit.SECONDS));
        BatchSinkOutcome authority = client.queryBatchSinkOutcome(runId);
        assertEquals(BatchSinkOutcome.State.COMMIT_DECIDED,
                authority.state());
        Path publications = output.resolve("publications");
        Files.createDirectories(publications);
        Files.write(publications.resolve(
                authority.generation() + ".visible"),
                markerValue.getBytes(StandardCharsets.UTF_8));
        BatchSinkOutcome observed = client.queryBatchSinkOutcome(runId);
        assertEquals(BatchSinkOutcome.State.CORRUPT_OR_CONFLICT,
                observed.state());
        assertEquals(expectedDetail, observed.detailCode());
        release.countDown();
        submitted.get(10, TimeUnit.SECONDS);
        master.setDurableDecisionHooksForTesting(null, null);
    }

    @Test
    void replacementLeaderRebuildsGenericCoordinatorAndPublishesPreparedGroup(
            @TempDir Path tmp) throws Exception {
        Files.createDirectories(tmp.resolve("deployment"));
        SignedAdversarialProviderFixture.Session session =
                SignedAdversarialProviderFixture.open(tmp.resolve("deployment"));
        DeploymentRuntime runtime = session.workerRuntime();
        InProcessRegistry registry = new InProcessRegistry();
        AdversarialDurableSinkAuthorityAdapter authority =
                new AdversarialDurableSinkAuthorityAdapter();
        Path spill = tmp.resolve("spill");
        Path output = tmp.resolve("output");
        Files.createDirectories(spill);
        try (MasterServer first = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     registry.artifactRepository(), runtime, authority);
             MasterServer replacement = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     registry.artifactRepository(), runtime, authority);
             WorkerServer worker = new WorkerServer(
                     "signed-provider-recovery-worker", 4,
                     () -> {
                         io.rowweave.cluster.registry.RegistryWorker leader =
                                 registry.worker(
                                         MasterServer.MASTER_LEADER_ROLE_ID);
                         return leader == null
                                 ? new Endpoint("127.0.0.1", first.port())
                                 : new Endpoint(leader.host(), leader.port());
                     },
                     spill.toFile(), runtime);
             MasterElection firstElection = new MasterElection(
                     registry, new Endpoint("127.0.0.1", first.port()));
             MasterElection replacementElection = new MasterElection(
                     registry, new Endpoint(
                             "127.0.0.1", replacement.port()));
             RealClusterJobClient client = new RealClusterJobClient(
                     new RegistryMasterLocator(registry), runtime)) {
            first.bindLeadership(firstElection);
            replacement.bindLeadership(replacementElection);
            firstElection.start();
            long leaderDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5);
            while (!firstElection.isLeader()
                    && System.nanoTime() < leaderDeadline) {
                Thread.sleep(10L);
            }
            assertTrue(firstElection.isLeader());

            java.util.concurrent.CountDownLatch cut =
                    new java.util.concurrent.CountDownLatch(1);
            first.setDurableDecisionHooksForTesting(null, () -> {
                firstElection.close();
                cut.countDown();
                throw new IllegalStateException(
                        "injected leader loss after provider decision");
            });
            PlanGraph graph = graph(output);
            CompletableFuture<Throwable> submit = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            client.submitRun(graph, "signed-provider-recovery-definition",
                                    new RunSubmission(
                                            "signed-provider-recovery-run",
                                            "attempt-1", "publication-1",
                                            1, 0L, 10L));
                            return null;
                        } catch (Throwable failure) {
                            return failure;
                        }
                    });
            assertTrue(cut.await(10, TimeUnit.SECONDS), () ->
                    "submit=" + (submit.isDone() ? submit.join() : "still-running"));
            replacementElection.start();
            leaderDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!replacementElection.isLeader()
                    && System.nanoTime() < leaderDeadline) {
                Thread.sleep(10L);
            }
            assertTrue(replacementElection.isLeader());
            long inventory = replacement.recoveryInventoryGeneration();
            assertTrue(replacement.awaitRecoveryInventory(
                    inventory, 10, TimeUnit.SECONDS));

            long recoveryDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(10);
            io.rowweave.run.RunSnapshot recovered = client.queryRun(
                    "signed-provider-recovery-run");
            while (recovered.status() != RunStatus.SUCCEEDED
                    && System.nanoTime() < recoveryDeadline) {
                Thread.sleep(20L);
                recovered = client.queryRun("signed-provider-recovery-run");
            }
            assertEquals(RunStatus.SUCCEEDED, recovered.status());
            BatchSinkOutcome outcome = client.queryBatchSinkOutcome(
                    "signed-provider-recovery-run");
            assertEquals(BatchSinkOutcome.State.COMMITTED, outcome.state());
            assertTrue(Files.exists(output.resolve("publications")
                    .resolve(outcome.generation() + ".visible")));
            assertEquals(0, worker.preparedOutputActiveCount());
            submit.get(10, TimeUnit.SECONDS);
        } finally {
            runtime.close();
        }
    }

    @Test
    void genericTerminalWithoutLocatorDurablyDecidesAbortAndFailsRun(
            @TempDir Path tmp) throws Exception {
        Files.createDirectories(tmp.resolve("deployment"));
        SignedAdversarialProviderFixture.Session session =
                SignedAdversarialProviderFixture.open(tmp.resolve("deployment"));
        DeploymentRuntime runtime = session.workerRuntime();
        InProcessRegistry registry = new InProcessRegistry();
        AdversarialDurableSinkAuthorityAdapter authority =
                new AdversarialDurableSinkAuthorityAdapter();
        String runId = "signed-provider-missing-locator";
        PlanGraph graph = graph(tmp.resolve("output"));
        io.rowweave.artifact.PlanArtifact artifact =
                io.rowweave.artifact.LegacyPlanArtifactAdapter.wrap(
                        "definition-" + runId, graph);
        String artifactId = registry.artifactRepository().put(
                io.rowweave.artifact.PlanArtifactCodec.encode(artifact));
        io.rowweave.run.RunSnapshot snapshot = accepted(
                registry, io.rowweave.run.RunCommand.createRun(
                        "create-" + runId, runId, artifact.definitionId(),
                        artifactId, 1, 0L, 1L));
        snapshot = accepted(registry,
                io.rowweave.run.RunCommand.createAttempt(
                        "attempt-" + runId, runId, snapshot.revision(),
                        "attempt-1", "publication-1", 2L));
        snapshot = accepted(registry,
                io.rowweave.run.RunCommand.advanceAttempt(
                        "dispatch-" + runId, runId, snapshot.revision(),
                        "attempt-1", snapshot.currentAttempt().fenceToken(),
                        io.rowweave.run.AttemptStatus.DISPATCHING,
                        false, 3L, null, null));
        io.rowweave.run.RunAttemptFence fence =
                new io.rowweave.run.RunAttemptFence(
                        runId, "attempt-1",
                        snapshot.currentAttempt().fenceToken());
        io.rowweave.run.StageAssignmentRecord record =
                io.rowweave.run.StageAssignmentRecord.create(
                        fence, 1L, 0, 901L, 0, 0,
                        io.rowweave.run.StageAssignmentRole.WHOLE_GRAPH,
                        "departed-provider-worker", "127.0.0.1", 61_111,
                        -1, "departed-provider-incarnation");
        io.rowweave.run.DurableStageAssignmentAuthority assignments =
                registry.durableStageAssignmentAuthority();
        io.rowweave.run.StageAssignmentSnapshot evidence =
                assignments.publish(new io.rowweave.run.StageAssignmentManifest(
                        fence, artifactId, 1L,
                        io.rowweave.run.AttemptOutputProfile
                                .MANIFEST_ATOMIC_GROUP_V1,
                        Collections.singletonList(record))).snapshot();
        byte[] commandDigest = MessageDigest.getInstance("SHA-256").digest(
                "provider-command".getBytes(StandardCharsets.UTF_8));
        assignments.accept(new io.rowweave.run.StageAssignmentAcceptance(
                evidence.token(0), commandDigest, 4L));
        snapshot = accepted(registry,
                io.rowweave.run.RunCommand.advanceAttempt(
                        "running-" + runId, runId, snapshot.revision(),
                        "attempt-1", snapshot.currentAttempt().fenceToken(),
                        io.rowweave.run.AttemptStatus.RUNNING,
                        false, 5L, null, null));
        assignments.recordTerminal(
                io.rowweave.run.StageTerminalReceipt.succeeded(
                        evidence.token(0), commandDigest, 1L, 1L, 6L));

        try (MasterServer master = new MasterServer(
                     5_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     registry.artifactRepository(), runtime, authority);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()));
             RealClusterJobClient client = new RealClusterJobClient(
                     new RegistryMasterLocator(registry), runtime)) {
            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    inventory, 10, TimeUnit.SECONDS));

            io.rowweave.run.RunSnapshot failed = client.queryRun(runId);
            assertEquals(RunStatus.FAILED, failed.status());
            assertEquals("PREPARED_LOCATOR_RECOVERY_FAILED",
                    failed.currentAttempt().reasonCode());
            BatchSinkOutcome outcome = client.queryBatchSinkOutcome(runId);
            assertEquals(BatchSinkOutcome.State.ABORT_DECIDED,
                    outcome.state());
            assertTrue(!Files.exists(tmp.resolve("output")
                    .resolve("publications")));
        } finally {
            runtime.close();
        }
    }

    private static io.rowweave.run.RunSnapshot accepted(
            InProcessRegistry registry, io.rowweave.run.RunCommand command) {
        io.rowweave.run.RunDecision decision =
                registry.runStateStore().apply(command);
        assertEquals(io.rowweave.run.RunDecision.Code.ACCEPTED,
                decision.code());
        return decision.snapshot();
    }

    private static PlanGraph graph(Path output) {
        DataRow first = new DataRow(1);
        first.set(0, 7);
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Arrays.asList(first),
                                "signed-provider-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0,
                                SignedAdversarialProviderFixture.output(
                                        META, output, "ledger"))
                        .build().withParam(
                                PlanNode.OPERATOR_UID_PARAM, "output")), 1);
    }
}
