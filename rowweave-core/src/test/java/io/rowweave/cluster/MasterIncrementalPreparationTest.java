package io.rowweave.cluster;

import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.artifact.ArtifactRepositoryProvider;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.SignedIncrementalSourceProviderFixture;
import io.rowweave.incremental.IncrementalAuthorityDecision;
import io.rowweave.incremental.IncrementalAuthorityGateway;
import io.rowweave.incremental.IncrementalAuthorityGatewayProvider;
import io.rowweave.incremental.IncrementalDefinition;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierAuthorityState;
import io.rowweave.incremental.IncrementalFrontierView;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalIntervalPreparationDecision;
import io.rowweave.incremental.IncrementalPlanDescriptor;
import io.rowweave.incremental.IncrementalPlanTemplateFingerprint;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.IncrementalRunSubmission;
import io.rowweave.incremental.IncrementalSourceConfig;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RunStatus;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** F5 Master preparation: signed Worker actions end in one gateway joint claim. */
class MasterIncrementalPreparationTest {

    @Test
    void clientBootstrapsFeedIdempotentlyThroughTheMasterAuthority(
            @TempDir Path root) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        try (SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             DeploymentRuntime masterRuntime = deployments.staticRuntime();
             MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     ((ArtifactRepositoryProvider) registry).artifactRepository(),
                     masterRuntime);
             ClusterJobClient client = new ClusterJobClient(
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), masterRuntime)) {
            IncrementalRunSubmission submission = submission(root);

            IncrementalFrontierView accepted =
                    client.initializeIncrementalFeed(
                            "initialize-feed", 1L, submission);
            IncrementalFrontierView replayed =
                    client.initializeIncrementalFeed(
                            "initialize-feed", 1L, submission);

            assertEquals(accepted, replayed);
            assertEquals("feed", accepted.feedId());
            assertEquals(1L, accepted.feedIncarnation());
            assertEquals(IncrementalFrontierAuthorityState.READY.name(),
                    accepted.state());
            assertEquals(IncrementalFrontier.begin(),
                    ((IncrementalAuthorityGatewayProvider) registry)
                            .incrementalAuthorityGateway().record("feed")
                            .committedFrontier());
        }
    }

    @Test
    void terminalStageFailureClosesRunAndFrontierThroughJointGateway(
            @TempDir Path root) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        try (SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             DeploymentRuntime masterRuntime = deployments.staticRuntime();
             DeploymentRuntime workerRuntime = deployments.workerRuntime();
             MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     ((ArtifactRepositoryProvider) registry).artifactRepository(),
                     masterRuntime);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "failing-source-worker", 8,
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())),
                     root.resolve("worker").toFile(), workerRuntime);
             ClusterJobClient client = new ClusterJobClient(
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), masterRuntime)) {
            IncrementalRunSubmission submission = submission(
                    root, 1, "fail-read");
            IncrementalAuthorityGateway gateway =
                    ((IncrementalAuthorityGatewayProvider) registry)
                            .incrementalAuthorityGateway();
            initialize(gateway, submission);

            CompletableFuture<Throwable> failed =
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            client.submitIncrementalRun(submission);
                            return null;
                        } catch (Throwable failure) {
                            return failure;
                        }
                    });
            String runId = null;
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(10L);
            while (runId == null && !failed.isDone()
                    && System.nanoTime() < deadline) {
                io.rowweave.incremental.IncrementalAuthorityRecord record =
                        gateway.record(submission.feedId());
                if (record.activeInterval() != null) {
                    runId = record.activeInterval().fence().runId();
                }
                Thread.sleep(1L);
            }
            assertNotNull(runId, "the active Run must be observable before joint closure");
            assertNotNull(failed.get(10L, TimeUnit.SECONDS));

            IncrementalFrontier frontier =
                    gateway.record(submission.feedId()).committedFrontier();
            assertEquals(IncrementalFrontier.begin(), frontier);
            assertEquals(IncrementalFrontierAuthorityState.READY,
                    gateway.record(submission.feedId()).state());
            assertEquals(RunStatus.FAILED,
                    gateway.runSnapshot(runId).status());
            assertNull(new io.rowweave.commit.ManifestFileStore(
                    root.resolve("manifest")).latest());
        }
    }

    @Test
    void prepublicationFailureClosesRunAndFrontierOnlyThroughJointGateway(
            @TempDir Path root) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        try (SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             DeploymentRuntime masterRuntime = deployments.staticRuntime();
             DeploymentRuntime workerRuntime = deployments.workerRuntime();
             MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     ((ArtifactRepositoryProvider) registry).artifactRepository(),
                     masterRuntime);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "source-worker", 8,
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())),
                     root.resolve("worker").toFile(), workerRuntime);
             ClusterJobClient client = new ClusterJobClient(
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), masterRuntime)) {
            IncrementalRunSubmission submission = submission(root, 1);
            IncrementalAuthorityGateway gateway =
                    ((IncrementalAuthorityGatewayProvider) registry)
                            .incrementalAuthorityGateway();
            initialize(gateway, submission);
            AtomicReference<String> runId = new AtomicReference<>();
            master.setDurableDecisionHooksForTesting(
                    () -> {
                        runId.set(gateway.record(submission.feedId())
                                .activeInterval().fence().runId());
                        throw new IllegalStateException("before-intent");
                    },
                    null);

            assertThrows(RuntimeException.class,
                    () -> client.submitIncrementalRun(submission));

            assertEquals(IncrementalFrontierAuthorityState.READY,
                    gateway.record(submission.feedId()).state());
            assertEquals(RunStatus.FAILED,
                    gateway.runSnapshot(runId.get()).status());
            assertNull(((RunStateStoreProvider) registry).runStateStore()
                    .get(runId.get()));
            assertNull(new io.rowweave.commit.ManifestFileStore(
                    root.resolve("manifest")).latest());
        }
    }

    @Test
    void clusterExecutionKeepsAllRunAndSinkEvidenceInTheJointGateway(
            @TempDir Path root) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        try (SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             DeploymentRuntime masterRuntime = deployments.staticRuntime();
             DeploymentRuntime workerRuntime = deployments.workerRuntime();
             MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     ((ArtifactRepositoryProvider) registry).artifactRepository(),
                     masterRuntime);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "source-worker", 8,
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())),
                     root.resolve("worker").toFile(), workerRuntime);
             ClusterJobClient client = new ClusterJobClient(
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())), masterRuntime)) {
            IncrementalRunSubmission submission = submission(root);
            IncrementalAuthorityGateway gateway =
                    ((IncrementalAuthorityGatewayProvider) registry)
                            .incrementalAuthorityGateway();
            initialize(gateway, submission);

            assertEquals(2L,
                    client.submitIncrementalRun(submission).rowsOut());
            assertEquals(RunStatus.SUCCEEDED,
                    gateway.runSnapshot(
                            gateway.record(submission.feedId())
                                    .lastCommitReceipt().fence().runId()).status());
            assertNull(((RunStateStoreProvider) registry).runStateStore()
                    .get(gateway.record(submission.feedId())
                            .lastCommitReceipt().fence().runId()));
        }
    }

    @Test
    void masterCapturesMaterializesPublishesAndClaimsThroughGateway(
            @TempDir Path root) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        try (SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             DeploymentRuntime masterRuntime = deployments.staticRuntime();
             DeploymentRuntime workerRuntime = deployments.workerRuntime();
             MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     ((ArtifactRepositoryProvider) registry).artifactRepository(),
                     masterRuntime);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "source-worker", 8,
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())),
                     root.resolve("worker").toFile(), workerRuntime)) {
            IncrementalRunSubmission submission = submission(root);
            IncrementalAuthorityGateway gateway =
                    ((IncrementalAuthorityGatewayProvider) registry)
                            .incrementalAuthorityGateway();
            initialize(gateway, submission);

            IncrementalIntervalPreparationDecision prepared =
                    master.prepareIncrementalSubmissionControl(
                            submission,
                            System.nanoTime() + 5_000_000_000L,
                            ignored -> { });

            assertEquals(IncrementalIntervalPreparationDecision.Code.CLAIMED,
                    prepared.code());
            assertEquals(prepared.claimRequest().runId(),
                    prepared.runSnapshot().runId());
            MasterServer.IncrementalExecutionClaim claim =
                    master.claimIncrementalExecutionControl(
                            submission, prepared, ignored -> { });
            assertTrue(gateway.currentlyOwns(
                    claim.frontierToken(), claim.runToken()));
            assertEquals(prepared.artifactId(), claim.artifactId());
            assertEquals(2, claim.graph().nodeCount());
            StageAssignmentSnapshot assignments =
                    master.publishIncrementalAssignmentsControl(claim);
            assertEquals(StageAssignmentRole.SYSTEM_CONTROL,
                    assignments.manifest().assignment(
                            assignments.manifest().assignmentCount() - 1).role());
            assertNull(((RunStateStoreProvider) registry).runStateStore()
                    .get(prepared.claimRequest().runId()),
                    "incremental physical intent must not leak into generic Run authority");
            assertTrue(new String(
                    gateway.record(submission.feedId()).activeInterval()
                            .capturedTarget().snapshotIdentity(),
                    StandardCharsets.UTF_8)
                    .startsWith("rowweave-provider-action-"));
        }
    }

    private static IncrementalRunSubmission submission(Path root)
            throws Exception {
        return submission(root, 3);
    }

    private static IncrementalRunSubmission submission(
            Path root, int maxAttempts) throws Exception {
        return submission(root, maxAttempts, "config");
    }

    private static IncrementalRunSubmission submission(
            Path root, int maxAttempts, String configurationBytes)
            throws Exception {
        DataSchema meta = SignedIncrementalSourceProviderFixture.sourceDataSchema();
        byte[] schema = hexBytes(
                io.rowweave.commit.SchemaFingerprint.of(meta));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "source", digest("frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schema, false,
                digest("partitions"),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(meta)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source")
                .withIncrementalPlanDescriptor(descriptor);
        ManifestFileSink output = new ManifestFileSink(
                meta, root.resolve("manifest").toString(), "changes");
        PlanNode sink = PlanNode.sink(meta, 0, output.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 1);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "definition", template, trust, ignored -> { });
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrapDurable(
                definition.definitionId(), template, trust, ignored -> { });
        IncrementalSourceProviderContract contract =
                SignedIncrementalSourceProviderFixture.contract();
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                configurationBytes.getBytes(StandardCharsets.UTF_8));
        return IncrementalRunSubmission.normal(
                "feed", "trigger", IncrementalRunMode.SNAPSHOT_V1,
                "attempt-1", "publication-1", maxAttempts, 0L, 10L,
                definition, artifact, contract, configuration);
    }

    private static void initialize(
            IncrementalAuthorityGateway gateway,
            IncrementalRunSubmission submission) {
        IncrementalInitializeRequest initialize =
                new IncrementalInitializeRequest(
                        submission.feedId(), 1L,
                        submission.definition().fingerprint(),
                        IncrementalSourceProviderContractCodec.digest(
                                submission.sourceContract()),
                        submission.definition().planDescriptor()
                                .frontierContractDigest(),
                        submission.sourceContract().frontierKind());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                gateway.initialize("initialize-feed", initialize).code());
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] hexBytes(String value) {
        byte[] result = new byte[value.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(
                    value.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }
}
