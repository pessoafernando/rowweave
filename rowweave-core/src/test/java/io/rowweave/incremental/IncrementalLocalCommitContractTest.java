package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.IncrementalSourceProvider;
import io.rowweave.IncrementalSourceSplit;
import io.rowweave.SourceExecutor;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.cluster.JobClient;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.local.LocalJobClient;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RetryAttemptIdentity;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** local visibility precedes the joint frontier/Run terminal. */
class IncrementalLocalCommitContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("op", DataType.StringType),
            new Field("sequence", DataType.LongType),
            new Field("payload", DataType.StringType));

    @Test
    void localNormalIntervalPublishesDataAndControlBeforeJointSuccess(
            @TempDir Path root) throws Exception {
        byte[] partitions = digest("partitions");
        IncrementalSourceProviderContract contract = contract();
        EmbeddedProvider provider = new EmbeddedProvider(contract, partitions);
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("config"));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "ledger-source", digest("frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), false,
                partitions,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source")
                .withIncrementalPlanDescriptor(descriptor);
        ManifestFileSink zOutput = new ManifestFileSink(
                META, root.resolve("manifest").toString(), "z-changes");
        ManifestFileSink aOutput = new ManifestFileSink(
                META, root.resolve("manifest").toString(), "a-changes");
        PlanNode zSink = PlanNode.sink(META, 0, zOutput.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "ledger-z-sink").build();
        PlanNode aSink = PlanNode.sink(META, 0, aOutput.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "ledger-a-sink").build();
        PlanGraph template = new PlanGraph(
                Arrays.asList(source, zSink, aSink), 1);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "ledger-definition", template, trust, ignored -> { });
        InMemoryIncrementalAuthorityStore authority = initialize(
                definition, contract);
        FrontierRecoveryToken token = authority.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("local-master", 1L)).token();
        InMemoryArtifactRepository artifacts =
                new InMemoryArtifactRepository(16, 16L * 1024L * 1024L);
        TrackingJobClient execution = new TrackingJobClient();
        IncrementalJobClient client = new IncrementalJobClient(
                definition, template, contract, configuration, provider,
                artifacts, authority, execution, trust,
                ProviderActionSupervisor.processWide(4));

        IncrementalJobResult result = client.submitNormal(
                "feed", token, "trigger-1", IncrementalRunMode.SNAPSHOT_V1,
                "attempt-1", "publication-1", 3, 0L, 10L,
                System.nanoTime() + 10_000_000_000L, ignored -> { });

        assertEquals(IncrementalJobResult.Code.SUCCEEDED, result.code());
        assertEquals(4L, result.executionReport().rowsOut());
        assertEquals(2L, authority.record("feed")
                .committedFrontier().sequence());
        assertEquals(IncrementalFrontierAuthorityState.READY,
                authority.record("feed").state());
        assertNull(authority.record("feed").activeInterval());
        assertEquals(RunStatus.SUCCEEDED,
                authority.runSnapshot(result.runId()).status());

        CommitManifest visible = new ManifestFileStore(
                root.resolve("manifest")).latest();
        assertNotNull(visible);
        assertEquals("publication-1", visible.attemptId());
        assertEquals(3, visible.members().size());
        assertTrue(visible.members().stream().anyMatch(
                member -> "a-changes".equals(member.name())));
        assertTrue(visible.members().stream().anyMatch(
                member -> "z-changes".equals(member.name())));
        CommitManifest.Member control = visible.members().stream()
                .filter(member -> IncrementalControlMemberCodec.MEMBER_NAME
                        .equals(member.name()))
                .findFirst().orElseThrow(AssertionError::new);
        byte[] controlFrame = java.nio.file.Files.readAllBytes(
                root.resolve("manifest").resolve(control.relativeDataPath()));
        IncrementalControlMember metadata =
                IncrementalControlMemberCodec.decode(controlFrame);
        assertEquals(result.runId(), metadata.runId());
        assertEquals(result.intervalId(), metadata.intervalId());
        assertEquals(result.artifactId(), metadata.artifactId());
        assertEquals(32, metadata.dataMembersDigest().length);
        assertEquals(32, metadata.decisionInputDigest().length);
        assertTrue(Arrays.equals(controlFrame,
                IncrementalControlMemberCodec.encode(metadata)));
        byte[] corruptControl = controlFrame.clone();
        corruptControl[corruptControl.length / 2] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalControlMemberCodec.decode(corruptControl));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalControlMemberCodec.decode(
                        Arrays.copyOf(controlFrame, controlFrame.length - 1)));
        assertEquals(1, provider.captureCalls.get());
        assertEquals(1, provider.materializeCalls.get());
        assertEquals(1, provider.openCalls.get());
        assertEquals(1, provider.closeCalls.get());
        assertEquals(1, execution.submissions.get());
        assertFalse(artifacts.get(result.artifactId()).length == 0);

        IncrementalJobResult backfill = client.submitBackfill(
                "feed", token, 1L,
                IncrementalFrontier.int64Sequence(0L),
                IncrementalFrontier.int64Sequence(2L),
                "backfill-request-1", "backfill-attempt-1",
                "backfill-publication-1", 3, 0L, 30L,
                System.nanoTime() + 10_000_000_000L, ignored -> { });

        assertEquals(IncrementalJobResult.Code.SUCCEEDED, backfill.code());
        assertEquals(2L, authority.record("feed")
                .committedFrontier().sequence());
        assertEquals(1L, authority.record("feed")
                .lastCompletedBackfillOrdinal());
        assertEquals(IncrementalRunMode.BACKFILL_V1,
                authority.record("feed").lastBackfillReceipt().mode());
        CommitManifest backfillManifest = new ManifestFileStore(
                root.resolve("manifest")).latest();
        assertEquals("backfill-publication-1", backfillManifest.attemptId());
        CommitManifest.Member backfillControl = backfillManifest.members()
                .stream().filter(member -> IncrementalControlMemberCodec
                        .MEMBER_NAME.equals(member.name()))
                .findFirst().orElseThrow(AssertionError::new);
        assertEquals(IncrementalRunMode.BACKFILL_V1,
                IncrementalControlMemberCodec.decode(
                        java.nio.file.Files.readAllBytes(
                                root.resolve("manifest").resolve(
                                        backfillControl.relativeDataPath())))
                        .mode());
        assertEquals(2, provider.captureCalls.get());
        assertEquals(2, provider.materializeCalls.get());
        assertEquals(2, provider.openCalls.get());
        assertEquals(2, provider.closeCalls.get());

        IncrementalJobResult replay = client.submitBackfill(
                "feed", token, 1L,
                IncrementalFrontier.int64Sequence(0L),
                IncrementalFrontier.int64Sequence(2L),
                "backfill-request-1", "backfill-attempt-1",
                "backfill-publication-1", 3, 0L, 40L,
                System.nanoTime() + 10_000_000_000L, ignored -> { });
        assertEquals(IncrementalJobResult.Code.BACKFILL_REPLAY, replay.code());
        assertEquals(backfill.runId(), replay.runId());
        assertEquals(backfill.artifactId(), replay.artifactId());
        assertEquals(backfill.intervalId(), replay.intervalId());
        assertEquals(backfillManifest.generation(), new ManifestFileStore(
                root.resolve("manifest")).latest().generation());
        assertEquals(2, provider.captureCalls.get());
        assertEquals(2, provider.materializeCalls.get());
        assertEquals(2, provider.openCalls.get());
    }

    @Test
    void preIntentFailureRetriesTheFrozenArtifactFromSplitStart(
            @TempDir Path root) throws Exception {
        byte[] partitions = digest("retry-partitions");
        IncrementalSourceProviderContract contract = contract();
        EmbeddedProvider provider = new EmbeddedProvider(
                contract, partitions, true);
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("retry-config"));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "retry-source", digest("retry-frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), false,
                partitions,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "retry-source")
                .withIncrementalPlanDescriptor(descriptor);
        ManifestFileSink output = new ManifestFileSink(
                META, root.resolve("retry-manifest").toString(), "changes");
        PlanNode sink = PlanNode.sink(META, 0, output.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "retry-sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 1);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "retry-definition", template, trust, ignored -> { });
        InMemoryIncrementalAuthorityStore authority = initialize(
                definition, contract);
        FrontierRecoveryToken token = authority.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("local-master", 1L)).token();
        InMemoryArtifactRepository artifacts =
                new InMemoryArtifactRepository(16, 16L * 1024L * 1024L);
        IncrementalJobClient client = new IncrementalJobClient(
                definition, template, contract, configuration, provider,
                artifacts, authority, trust,
                ProviderActionSupervisor.processWide(4));

        IncrementalJobResult result = client.submitNormal(
                "feed", token, "retry-trigger", IncrementalRunMode.SNAPSHOT_V1,
                "attempt-1", "publication-1", 3, 0L, 10L,
                System.nanoTime() + 10_000_000_000L, ignored -> { });

        assertEquals(IncrementalJobResult.Code.SUCCEEDED, result.code());
        assertEquals(1, provider.captureCalls.get());
        assertEquals(1, provider.materializeCalls.get());
        assertEquals(2, provider.openCalls.get());
        assertEquals(2, provider.closeCalls.get());
        assertEquals(2, authority.runSnapshot(result.runId()).attemptCount());
        RetryAttemptIdentity retry = RetryAttemptIdentity.derive(
                result.runId(), result.artifactId(), 2);
        assertEquals(retry.publicationId(), new ManifestFileStore(
                root.resolve("retry-manifest")).latest().attemptId());
        assertEquals(2L, authority.record("feed")
                .committedFrontier().sequence());
    }

    @Test
    void exhaustedLocalRetriesJointlyCloseRunWithoutVisibility(
            @TempDir Path root) throws Exception {
        byte[] partitions = digest("exhausted-partitions");
        IncrementalSourceProviderContract contract = contract();
        EmbeddedProvider provider = new EmbeddedProvider(
                contract, partitions, 2);
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("exhausted-config"));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "exhausted-source", digest("exhausted-frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), false,
                partitions,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "exhausted-source")
                .withIncrementalPlanDescriptor(descriptor);
        Path manifestRoot = root.resolve("exhausted-manifest");
        ManifestFileSink output = new ManifestFileSink(
                META, manifestRoot.toString(), "changes");
        PlanNode sink = PlanNode.sink(META, 0, output.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "exhausted-sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 1);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "exhausted-definition", template, trust,
                        ignored -> { });
        InMemoryIncrementalAuthorityStore authority = initialize(
                definition, contract);
        FrontierRecoveryToken token = authority.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("local-master", 1L)).token();
        IncrementalJobClient client = new IncrementalJobClient(
                definition, template, contract, configuration, provider,
                new InMemoryArtifactRepository(
                        16, 16L * 1024L * 1024L),
                authority, trust, ProviderActionSupervisor.processWide(4));

        assertThrows(RuntimeException.class, () -> client.submitNormal(
                "feed", token, "exhausted-trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                "attempt-1", "publication-1", 2, 0L, 10L,
                System.nanoTime() + 10_000_000_000L, ignored -> { }));

        String runId = IncrementalIntervalIdentity.from(
                provider.lastReceipt).candidateRunId();
        assertEquals(RunStatus.FAILED,
                authority.runSnapshot(runId).status());
        assertEquals(2, authority.runSnapshot(runId).attemptCount());
        assertEquals(IncrementalFrontierAuthorityState.READY,
                authority.record("feed").state());
        assertNull(authority.record("feed").activeInterval());
        assertEquals(IncrementalFrontier.begin(),
                authority.record("feed").committedFrontier());
        assertNull(new ManifestFileStore(manifestRoot).latest());
        assertEquals(1, provider.captureCalls.get());
        assertEquals(1, provider.materializeCalls.get());
        assertEquals(2, provider.openCalls.get());
        assertEquals(2, provider.closeCalls.get());
    }

    @Test
    void authorityDeadlineClosesRunningIntervalWithoutRetryOrVisibility(
            @TempDir Path root) throws Exception {
        AtomicLong clock = new AtomicLong(50L);
        byte[] partitions = digest("deadline-partitions");
        IncrementalSourceProviderContract contract = contract();
        EmbeddedProvider provider = new EmbeddedProvider(
                contract, partitions, 0, () -> clock.set(200L));
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("deadline-config"));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "deadline-source", digest("deadline-frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), false,
                partitions,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "deadline-source")
                .withIncrementalPlanDescriptor(descriptor);
        Path manifestRoot = root.resolve("deadline-manifest");
        ManifestFileSink output = new ManifestFileSink(
                META, manifestRoot.toString(), "changes");
        PlanNode sink = PlanNode.sink(META, 0, output.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "deadline-sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 1);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "deadline-definition", template, trust,
                        ignored -> { });
        InMemoryIncrementalAuthorityStore authority =
                new InMemoryIncrementalAuthorityStore(clock::get);
        initialize(definition, contract, authority);
        FrontierRecoveryToken token = authority.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("local-master", 1L)).token();
        IncrementalJobClient client = new IncrementalJobClient(
                definition, template, contract, configuration, provider,
                new InMemoryArtifactRepository(
                        16, 16L * 1024L * 1024L),
                authority, trust, ProviderActionSupervisor.processWide(4));

        IncrementalJobResult result = client.submitNormal(
                "feed", token, "deadline-trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                "attempt-1", "publication-1", 3, 100L, 10L,
                System.nanoTime() + 10_000_000_000L, ignored -> { });

        assertEquals(IncrementalJobResult.Code.TIMED_OUT, result.code());
        assertEquals(RunStatus.FAILED,
                authority.runSnapshot(result.runId()).status());
        assertEquals(1, authority.runSnapshot(result.runId()).attemptCount());
        assertEquals(IncrementalFrontierAuthorityState.READY,
                authority.record("feed").state());
        assertEquals(IncrementalFrontier.begin(),
                authority.record("feed").committedFrontier());
        assertNull(new ManifestFileStore(manifestRoot).latest());
        assertEquals(1, provider.openCalls.get());
        assertEquals(1, provider.closeCalls.get());
    }

    @Test
    void userCancellationClosesRunningIntervalWithoutRetryOrVisibility(
            @TempDir Path root) throws Exception {
        byte[] partitions = digest("cancel-partitions");
        IncrementalSourceProviderContract contract = contract();
        AtomicReference<IncrementalJobClient> clientRef = new AtomicReference<>();
        AtomicReference<EmbeddedProvider> providerRef = new AtomicReference<>();
        AtomicReference<FrontierRecoveryToken> tokenRef = new AtomicReference<>();
        AtomicReference<RunCancellationResult> cancelRef = new AtomicReference<>();
        EmbeddedProvider provider = new EmbeddedProvider(
                contract, partitions, 0, () -> {
            String runId = IncrementalIntervalIdentity.from(
                    providerRef.get().lastReceipt).candidateRunId();
            cancelRef.set(clientRef.get().cancel(
                    tokenRef.get(), runId, "cancel-request-1"));
        });
        providerRef.set(provider);
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("cancel-config"));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "cancel-source", digest("cancel-frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), false,
                partitions,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "cancel-source")
                .withIncrementalPlanDescriptor(descriptor);
        Path manifestRoot = root.resolve("cancel-manifest");
        ManifestFileSink output = new ManifestFileSink(
                META, manifestRoot.toString(), "changes");
        PlanNode sink = PlanNode.sink(META, 0, output.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "cancel-sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 1);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "cancel-definition", template, trust,
                        ignored -> { });
        InMemoryIncrementalAuthorityStore authority = initialize(
                definition, contract);
        FrontierRecoveryToken token = authority.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("local-master", 1L)).token();
        tokenRef.set(token);
        IncrementalJobClient client = new IncrementalJobClient(
                definition, template, contract, configuration, provider,
                new InMemoryArtifactRepository(
                        16, 16L * 1024L * 1024L),
                authority, trust, ProviderActionSupervisor.processWide(4));
        clientRef.set(client);

        IncrementalJobResult result = client.submitNormal(
                "feed", token, "cancel-trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                "attempt-1", "publication-1", 3, 0L, 10L,
                System.nanoTime() + 10_000_000_000L, ignored -> { });

        assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                cancelRef.get().disposition());
        assertEquals(IncrementalJobResult.Code.CANCELED, result.code());
        assertEquals(RunStatus.CANCELED,
                authority.runSnapshot(result.runId()).status());
        assertEquals(1, authority.runSnapshot(result.runId()).attemptCount());
        assertEquals(IncrementalFrontierAuthorityState.READY,
                authority.record("feed").state());
        assertEquals(IncrementalFrontier.begin(),
                authority.record("feed").committedFrontier());
        assertNull(new ManifestFileStore(manifestRoot).latest());
        assertEquals(1, provider.openCalls.get());
        assertEquals(1, provider.closeCalls.get());
    }

    @Test
    void replacementReconcilesVisibleManifestWithoutReexecutingSource(
            @TempDir Path root) throws Exception {
        byte[] partitions = digest("recovery-partitions");
        IncrementalSourceProviderContract contract = contract();
        EmbeddedProvider provider = new EmbeddedProvider(contract, partitions);
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("recovery-config"));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "recovery-source", digest("recovery-frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), false,
                partitions,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "recovery-source")
                .withIncrementalPlanDescriptor(descriptor);
        Path manifestRoot = root.resolve("recovery-manifest");
        ManifestFileSink output = new ManifestFileSink(
                META, manifestRoot.toString(), "changes");
        PlanNode sink = PlanNode.sink(META, 0, output.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "recovery-sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 1);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "recovery-definition", template, trust,
                        ignored -> { });
        AtomicInteger terminalFault = new AtomicInteger();
        InMemoryIncrementalAuthorityStore authority =
                new InMemoryIncrementalAuthorityStore(phase -> {
                    if (phase == IncrementalJointSuccessPhase.BEFORE_COMMIT
                            && terminalFault.incrementAndGet() == 1) {
                        throw new InjectedTerminalFault();
                    }
                });
        initialize(definition, contract, authority);
        FrontierRecoveryToken first = authority.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("local-master", 1L)).token();
        InMemoryArtifactRepository artifacts =
                new InMemoryArtifactRepository(16, 16L * 1024L * 1024L);
        IncrementalJobClient client = new IncrementalJobClient(
                definition, template, contract, configuration, provider,
                artifacts, authority, trust,
                ProviderActionSupervisor.processWide(4));

        assertThrows(InjectedTerminalFault.class, () -> client.submitNormal(
                "feed", first, "recovery-trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                "attempt-1", "publication-1", 3, 0L, 10L,
                System.nanoTime() + 10_000_000_000L, ignored -> { }));
        assertNotNull(new ManifestFileStore(manifestRoot).latest());
        assertEquals(IncrementalFrontier.begin(),
                authority.record("feed").committedFrontier());
        assertEquals(IncrementalAttemptCommitPhase.COMMIT_DECIDED,
                authority.record("feed").activeInterval().phase());

        FrontierRecoveryToken replacement = authority.claimOwnership(
                "feed", authority.record("feed").revision(),
                first.ownershipEpoch(),
                new RecoveryOwner("replacement-master", 2L)).token();
        IncrementalJobResult recovered = client.recover(
                "feed", replacement, ignored -> { });

        assertEquals(IncrementalJobResult.Code.RECOVERED, recovered.code());
        assertEquals(RunStatus.SUCCEEDED,
                authority.runSnapshot(recovered.runId()).status());
        assertEquals(2L, authority.record("feed")
                .committedFrontier().sequence());
        assertNull(authority.record("feed").activeInterval());
        assertEquals(1, provider.captureCalls.get());
        assertEquals(1, provider.materializeCalls.get());
        assertEquals(1, provider.openCalls.get());
    }

    @Test
    void fileAuthorityReopensAndReconcilesVisibleManifest(
            @TempDir Path root) throws Exception {
        byte[] partitions = digest("file-recovery-partitions");
        IncrementalSourceProviderContract contract = contract();
        EmbeddedProvider provider = new EmbeddedProvider(contract, partitions);
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("file-recovery-config"));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "file-recovery-source", digest("file-frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), false,
                partitions,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM,
                        "file-recovery-source")
                .withIncrementalPlanDescriptor(descriptor);
        Path manifestRoot = root.resolve("file-recovery-manifest");
        ManifestFileSink output = new ManifestFileSink(
                META, manifestRoot.toString(), "changes");
        PlanNode sink = PlanNode.sink(META, 0, output.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM,
                        "file-recovery-sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 1);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "file-recovery-definition", template, trust,
                        ignored -> { });
        Path authorityRoot = root.resolve("authority");
        AtomicInteger terminalFault = new AtomicInteger();
        FileIncrementalAuthorityStore firstAuthority =
                new FileIncrementalAuthorityStore(authorityRoot, phase -> {
                    if (phase == IncrementalJointSuccessPhase.BEFORE_COMMIT
                            && terminalFault.incrementAndGet() == 1) {
                        throw new InjectedTerminalFault();
                    }
                });
        initialize(definition, contract, firstAuthority);
        FrontierRecoveryToken first = firstAuthority.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("local-master", 1L)).token();
        InMemoryArtifactRepository artifacts =
                new InMemoryArtifactRepository(16, 16L * 1024L * 1024L);
        IncrementalJobClient firstClient = new IncrementalJobClient(
                definition, template, contract, configuration, provider,
                artifacts, firstAuthority, trust,
                ProviderActionSupervisor.processWide(4));

        assertThrows(InjectedTerminalFault.class,
                () -> firstClient.submitNormal(
                        "feed", first, "file-recovery-trigger",
                        IncrementalRunMode.SNAPSHOT_V1,
                        "attempt-1", "publication-1", 3, 0L, 10L,
                        System.nanoTime() + 10_000_000_000L,
                        ignored -> { }));
        assertNotNull(new ManifestFileStore(manifestRoot).latest());

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(authorityRoot);
        assertEquals(IncrementalAttemptCommitPhase.COMMIT_DECIDED,
                reopened.record("feed").activeInterval().phase());
        FrontierRecoveryToken replacement = reopened.claimOwnership(
                "feed", reopened.record("feed").revision(),
                first.ownershipEpoch(),
                new RecoveryOwner("replacement-master", 2L)).token();
        IncrementalJobClient replacementClient = new IncrementalJobClient(
                definition, template, contract, configuration, provider,
                artifacts, reopened, trust,
                ProviderActionSupervisor.processWide(4));
        IncrementalJobResult recovered = replacementClient.recover(
                "feed", replacement, ignored -> { });

        assertEquals(IncrementalJobResult.Code.RECOVERED, recovered.code());
        assertEquals(2L, reopened.record("feed")
                .committedFrontier().sequence());
        assertEquals(RunStatus.SUCCEEDED,
                reopened.runSnapshot(recovered.runId()).status());
        assertEquals(1, provider.captureCalls.get());
        assertEquals(1, provider.materializeCalls.get());
        assertEquals(1, provider.openCalls.get());
    }

    @Test
    void localClientRejectsUnsupportedSinkGroupsAtConstruction(
            @TempDir Path root) throws Exception {
        byte[] partitions = digest("preflight-partitions");
        IncrementalSourceProviderContract contract = contract();
        EmbeddedProvider provider = new EmbeddedProvider(contract, partitions);
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("preflight-config"));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "preflight-source", digest("preflight-frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), false,
                partitions,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "preflight-source")
                .withIncrementalPlanDescriptor(descriptor);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        InMemoryArtifactRepository artifacts =
                new InMemoryArtifactRepository(16, 16L * 1024L * 1024L);
        InMemoryIncrementalAuthorityStore authority =
                new InMemoryIncrementalAuthorityStore();

        PlanNode discard = PlanNode.sink(
                META, 0, new DiscardSink(META))
                .param(PlanNode.OPERATOR_UID_PARAM, "discard-sink").build();
        assertClientConstructionRejected(
                new PlanGraph(Arrays.asList(source, discard), 1),
                "discard-definition", contract, configuration, provider,
                artifacts, authority, trust);

        ManifestFileSink first = new ManifestFileSink(
                META, root.resolve("domain-a").toString(), "changes-a");
        ManifestFileSink second = new ManifestFileSink(
                META, root.resolve("domain-b").toString(), "changes-b");
        PlanNode firstSink = PlanNode.sink(
                META, 0, first.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "first-sink").build();
        PlanNode secondSink = PlanNode.sink(
                META, 0, second.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "second-sink").build();
        assertClientConstructionRejected(
                new PlanGraph(Arrays.asList(
                        source, firstSink, secondSink), 1),
                "mixed-domain-definition", contract, configuration, provider,
                artifacts, authority, trust);

        ManifestFileSink reserved = new ManifestFileSink(
                META, root.resolve("domain-a").toString(),
                IncrementalControlMemberCodec.MEMBER_NAME);
        PlanNode reservedSink = PlanNode.sink(
                META, 0, reserved.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "reserved-sink").build();
        assertClientConstructionRejected(
                new PlanGraph(Arrays.asList(source, reservedSink), 1),
                "reserved-definition", contract, configuration, provider,
                artifacts, authority, trust);
    }

    private static void assertClientConstructionRejected(
            PlanGraph template, String definitionId,
            IncrementalSourceProviderContract contract,
            IncrementalSourceConfig configuration,
            IncrementalSourceProvider provider,
            InMemoryArtifactRepository artifacts,
            InMemoryIncrementalAuthorityStore authority,
            ObjectTrustContext trust) {
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        definitionId, template, trust, ignored -> { });
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalJobClient(
                        definition, template, contract, configuration,
                        provider, artifacts, authority, trust,
                        ProviderActionSupervisor.processWide(4)));
    }

    private static InMemoryIncrementalAuthorityStore initialize(
            IncrementalDefinition definition,
            IncrementalSourceProviderContract contract) {
        InMemoryIncrementalAuthorityStore store =
                new InMemoryIncrementalAuthorityStore();
        initialize(definition, contract, store);
        return store;
    }

    private static void initialize(
            IncrementalDefinition definition,
            IncrementalSourceProviderContract contract,
            InMemoryIncrementalAuthorityStore store) {
        IncrementalInitializeRequest request = new IncrementalInitializeRequest(
                "feed", 1L, definition.fingerprint(),
                IncrementalSourceProviderContractCodec.digest(contract),
                definition.planDescriptor().frontierContractDigest(),
                contract.frontierKind());
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.INITIALIZE, "feed", "initialize",
                0L, 1L, null, null, request.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.initialize(
                        command, request)).code());
    }

    private static void initialize(
            IncrementalDefinition definition,
            IncrementalSourceProviderContract contract,
            FileIncrementalAuthorityStore store) {
        IncrementalInitializeRequest request = new IncrementalInitializeRequest(
                "feed", 1L, definition.fingerprint(),
                IncrementalSourceProviderContractCodec.digest(contract),
                definition.planDescriptor().frontierContractDigest(),
                contract.frontierKind());
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.INITIALIZE, "feed", "initialize",
                0L, 1L, null, null, request.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.initialize(
                        command, request)).code());
    }

    private static IncrementalSourceProviderContract contract() {
        return new IncrementalSourceProviderContract(
                IncrementalSourceProviderContract.CURRENT_SPI_VERSION,
                new IncrementalSourceProviderKey("test.embedded", 1),
                new IncrementalSourceCodecDescriptor("test.config", 1, 64),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.CAPTURE_CODEC_ID, 1,
                        IncrementalCaptureResponseCodec.MAX_FRAME_BYTES),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.MATERIALIZATION_CODEC_ID,
                        1, IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES),
                IncrementalFrontierCodec.MAX_FRAME_BYTES,
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                IncrementalSourceProviderContract.OrderingGuarantee
                        .TOTAL_ORDER_NO_LATE_LEFT_V1,
                IncrementalSourceProviderContract.SnapshotGuarantee
                        .EXACT_REOPENABLE_V1,
                false, new IncrementalSourceActionBudget(
                        1024L * 1024L, 24L * 1024L * 1024L, 1, 60_000L),
                IncrementalSourceProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
    }

    private static byte[] schemaDigest() {
        String fingerprint = SchemaFingerprint.of(META);
        byte[] decoded = new byte[fingerprint.length() / 2];
        for (int index = 0; index < decoded.length; index++) {
            decoded[index] = (byte) Integer.parseInt(
                    fingerprint.substring(index * 2, index * 2 + 2), 16);
        }
        return decoded;
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte b : value) {
            result.append(Character.forDigit((b >>> 4) & 0xf, 16));
            result.append(Character.forDigit(b & 0xf, 16));
        }
        return result.toString();
    }

    private static final class EmbeddedProvider
            implements IncrementalSourceProvider {
        private static final long serialVersionUID = 1L;
        final IncrementalSourceProviderContract contract;
        final byte[] partitions;
        final AtomicInteger captureCalls = new AtomicInteger();
        final AtomicInteger materializeCalls = new AtomicInteger();
        final AtomicInteger openCalls = new AtomicInteger();
        final AtomicInteger closeCalls = new AtomicInteger();
        final int failingReads;
        final Runnable onOpen;
        volatile CapturedTargetReceipt lastReceipt;

        EmbeddedProvider(IncrementalSourceProviderContract contract,
                         byte[] partitions) {
            this(contract, partitions, 0);
        }

        EmbeddedProvider(IncrementalSourceProviderContract contract,
                         byte[] partitions, boolean failFirstRead) {
            this(contract, partitions, failFirstRead ? 1 : 0);
        }

        EmbeddedProvider(IncrementalSourceProviderContract contract,
                         byte[] partitions, int failingReads) {
            this(contract, partitions, failingReads, () -> { });
        }

        EmbeddedProvider(IncrementalSourceProviderContract contract,
                         byte[] partitions, int failingReads,
                         Runnable onOpen) {
            this.contract = contract;
            this.partitions = partitions.clone();
            this.failingReads = failingReads;
            this.onOpen = onOpen;
        }

        @Override public IncrementalSourceProviderContract contract() {
            return contract;
        }

        @Override public byte[] canonicalizeConfiguration(
                IncrementalSourceConfig configuration) {
            return configuration.configurationBytes();
        }

        @Override public IncrementalCaptureResponse captureTarget(
                IncrementalSourceConfig configuration,
                IncrementalCaptureRequest request) {
            captureCalls.incrementAndGet();
            return new IncrementalCaptureResponse(
                    request.mode() == IncrementalRunMode.BACKFILL_V1
                            ? request.requestedTarget()
                            : IncrementalFrontier.int64Sequence(2L),
                    bytes("snapshot"));
        }

        @Override public IncrementalMaterializationResponse materializeInterval(
                IncrementalSourceConfig configuration,
                CapturedTargetReceipt receipt) {
            materializeCalls.incrementAndGet();
            lastReceipt = receipt;
            try {
                BatchSourceSplitDescriptor split =
                        new BatchSourceSplitDescriptor(
                                BatchSourceSplitDescriptor.CURRENT_VERSION,
                                contract.providerKey().connectorType(),
                                contract.providerKey().connectorVersion(), "p0",
                                SchemaFingerprint.of(META),
                                hex(digest("snapshot")),
                                BatchSourceSplitDescriptor.ReplayGuarantee
                                        .EXACT_ORDERED_FROM_START);
                return new IncrementalMaterializationResponse(
                        contract.providerKey(), receipt, partitions,
                        Collections.singletonList(
                                new IncrementalMaterializedSplit(
                                        split, bytes("open"))));
            } catch (Exception impossible) {
                throw new IllegalStateException(impossible);
            }
        }

        @Override public SourceExecutor<IncrementalSourceSplit> openSplit(
                IncrementalSourceSplit input) {
            int ordinal = openCalls.incrementAndGet();
            onOpen.run();
            return new Reader(input, this, ordinal <= failingReads);
        }
    }

    private static final class TrackingJobClient implements JobClient {
        private final AtomicInteger submissions = new AtomicInteger();
        private final LocalJobClient delegate = new LocalJobClient(false);

        @Override public long submitToSink(PlanGraph graph) {
            return submit(graph).rowsOut();
        }

        @Override public ExecutionReport submit(PlanGraph graph) {
            submissions.incrementAndGet();
            return delegate.submit(graph);
        }
    }

    private static final class Reader
            extends SourceExecutor<IncrementalSourceSplit> {
        private final EmbeddedProvider owner;
        private final boolean failRead;
        private final CapturedTargetReceipt capturedTarget;
        private int index;
        private boolean closed;

        Reader(IncrementalSourceSplit input, EmbeddedProvider owner,
               boolean failRead) {
            super(input, input.getSchema());
            this.owner = owner;
            this.failRead = failRead;
            this.capturedTarget = input.capturedTarget();
        }

        @Override public void open() { }

        @Override public boolean hasNext() { return index < 2; }

        @Override public DataRow next() {
            if (failRead) {
                throw new IllegalStateException("injected local read failure");
            }
            if (index >= 2) throw new NoSuchElementException();
            long sequence = ++index;
            DataRow row = new DataRow(4);
            row.set(0, sequence);
            row.set(1, "UPSERT");
            row.set(2, capturedTarget.mode() == IncrementalRunMode.SNAPSHOT_V1
                    ? sequence - 1L
                    : capturedTarget.from().sequence() + sequence);
            row.set(3, "value-" + sequence);
            return row;
        }

        @Override public void close() {
            if (!closed) {
                closed = true;
                owner.closeCalls.incrementAndGet();
            }
        }
    }

    private static final class InjectedTerminalFault extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
