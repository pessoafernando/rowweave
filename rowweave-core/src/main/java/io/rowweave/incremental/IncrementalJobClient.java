package io.rowweave.incremental;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSink;
import io.rowweave.BatchRuntimes;
import io.rowweave.BatchExecutableSource;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.IncrementalSourceProvider;
import io.rowweave.IncrementalSourceRuntime;
import io.rowweave.IncrementalMapFunction;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.ArtifactLifecycleRepository;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.FileCommitGenerationAuthority;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.cluster.JobClient;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.WholeAttemptRetryDecision;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongConsumer;

/** Frontier-aware single-JVM executor for one captured bounded interval. */
public final class IncrementalJobClient {
    private final IncrementalDefinition definition;
    private final PlanGraph template;
    private final IncrementalSourceProviderContract sourceContract;
    private final IncrementalSourceConfig configuration;
    private final IncrementalSourceProvider sourceProvider;
    private final ArtifactLifecycleRepository artifacts;
    private final IncrementalLocalAuthority authority;
    private final JobClient executionClient;
    private final ObjectTrustContext trustContext;
    private final ProviderActionSupervisor supervisor;
    private final IncrementalIntervalPreparer preparer;

    public IncrementalJobClient(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceProvider sourceProvider,
            ArtifactLifecycleRepository artifacts,
            InMemoryIncrementalAuthorityStore authority,
            ObjectTrustContext trustContext,
            ProviderActionSupervisor supervisor) {
        this(definition, template, sourceContract, configuration,
                sourceProvider, artifacts,
                (IncrementalLocalAuthority) authority,
                BatchRuntimes.processWideInProcess(),
                trustContext, supervisor, true);
    }

    public IncrementalJobClient(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceProvider sourceProvider,
            ArtifactLifecycleRepository artifacts,
            InMemoryIncrementalAuthorityStore authority,
            JobClient executionClient,
            ObjectTrustContext trustContext,
            ProviderActionSupervisor supervisor) {
        this(definition, template, sourceContract, configuration,
                sourceProvider, artifacts,
                (IncrementalLocalAuthority) authority, executionClient,
                trustContext, supervisor, true);
    }

    public IncrementalJobClient(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceProvider sourceProvider,
            ArtifactLifecycleRepository artifacts,
            FileIncrementalAuthorityStore authority,
            ObjectTrustContext trustContext,
            ProviderActionSupervisor supervisor) {
        this(definition, template, sourceContract, configuration,
                sourceProvider, artifacts,
                (IncrementalLocalAuthority) authority,
                BatchRuntimes.processWideInProcess(),
                trustContext, supervisor, true);
    }

    public IncrementalJobClient(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceProvider sourceProvider,
            ArtifactLifecycleRepository artifacts,
            FileIncrementalAuthorityStore authority,
            JobClient executionClient,
            ObjectTrustContext trustContext,
            ProviderActionSupervisor supervisor) {
        this(definition, template, sourceContract, configuration,
                sourceProvider, artifacts,
                (IncrementalLocalAuthority) authority, executionClient,
                trustContext, supervisor, true);
    }

    private IncrementalJobClient(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceProvider sourceProvider,
            ArtifactLifecycleRepository artifacts,
            IncrementalLocalAuthority authority,
            JobClient executionClient,
            ObjectTrustContext trustContext,
            ProviderActionSupervisor supervisor,
            boolean backendChecked) {
        if (definition == null || template == null || sourceContract == null
                || configuration == null || sourceProvider == null
                || artifacts == null || authority == null
                || executionClient == null
                || trustContext == null || supervisor == null) {
            throw new IllegalArgumentException(
                    "incremental local client fields are required");
        }
        validateLocalTemplate(template);
        IncrementalDefinition expected =
                IncrementalPlanTemplateFingerprint.derive(
                        definition.definitionId(), template, trustContext,
                        ignored -> { });
        if (!definition.equals(expected)) {
            throw new IllegalArgumentException(
                    "incremental definition differs from template");
        }
        this.definition = definition;
        this.template = template;
        this.sourceContract = sourceContract;
        this.configuration = configuration;
        this.sourceProvider = sourceProvider;
        this.artifacts = artifacts;
        this.authority = authority;
        this.executionClient = executionClient;
        this.trustContext = trustContext;
        this.supervisor = supervisor;
        this.preparer = new IncrementalIntervalPreparer(
                definition, template, sourceContract, configuration,
                new IncrementalSourceActionExecutor(
                        sourceContract, configuration, sourceProvider,
                        supervisor),
                artifacts, authority, trustContext);
    }

    private static void validateLocalTemplate(PlanGraph template) {
        int sinkCount = 0;
        String domainId = null;
        Set<String> memberNames = new HashSet<>();
        for (int index = 0; index < template.nodeCount(); index++) {
            PlanNode node = template.node(index);
            if (node.kind() != PlanNode.Kind.SINK) continue;
            sinkCount++;
            if (node.output() == null
                    || node.output().getClass()
                    != ProviderBatchSink.class) {
                throw new IllegalArgumentException(
                        "incremental local runtime requires Manifest provider Sinks");
            }
            ManifestFileSink manifest = ManifestFileSink.fromProviderSink(
                    (ProviderBatchSink) node.output());
            if (manifest.hasQualityGateBinding()) {
                throw new IllegalArgumentException(
                        "incremental local runtime does not permit quality Sink bindings");
            }
            if (IncrementalControlMemberCodec.MEMBER_NAME.equals(
                    manifest.memberName())) {
                throw new IllegalArgumentException(
                        "reserved incremental control member is engine-owned");
            }
            if (!memberNames.add(manifest.memberName())) {
                throw new IllegalArgumentException(
                        "incremental Manifest member names must be unique");
            }
            if (domainId == null) domainId = manifest.domainId();
            else if (!domainId.equals(manifest.domainId())) {
                throw new IllegalArgumentException(
                        "incremental Sinks must share one Manifest domain");
            }
        }
        if (sinkCount < 1
                || sinkCount > StageAssignmentManifest.MAX_ASSIGNMENTS - 1) {
            throw new IllegalArgumentException(
                    "incremental runtime requires 1-4094 user Sinks");
        }
    }

    public IncrementalJobResult submitNormal(
            String feedId, FrontierRecoveryToken recoveryToken,
            String triggerId, IncrementalRunMode mode,
            String attemptId, String publicationId, int maxAttempts,
            long deadlineEpochMillis, long eventTimeMillis,
            long actionDeadlineNanos, LongConsumer reservation) {
        IncrementalIntervalPreparationDecision prepared =
                preparer.prepareNormal(
                        feedId, recoveryToken, triggerId, mode,
                        attemptId, publicationId, maxAttempts,
                        deadlineEpochMillis, eventTimeMillis,
                        actionDeadlineNanos, reservation);
        if (prepared.code()
                != IncrementalIntervalPreparationDecision.Code.CLAIMED) {
            return IncrementalJobResult.prepared(prepared);
        }
        return executeClaimed(
                feedId, recoveryToken, prepared,
                actionDeadlineNanos, reservation);
    }

    public IncrementalJobResult submitBackfill(
            String feedId, FrontierRecoveryToken recoveryToken,
            long expectedBackfillOrdinal, IncrementalFrontier from,
            IncrementalFrontier target, String requestKey,
            String attemptId, String publicationId, int maxAttempts,
            long deadlineEpochMillis, long eventTimeMillis,
            long actionDeadlineNanos, LongConsumer reservation) {
        IncrementalIntervalPreparationDecision prepared =
                preparer.prepareBackfill(
                        feedId, recoveryToken, expectedBackfillOrdinal,
                        from, target, requestKey, attemptId, publicationId,
                        maxAttempts, deadlineEpochMillis, eventTimeMillis,
                        actionDeadlineNanos, reservation);
        if (prepared.code()
                != IncrementalIntervalPreparationDecision.Code.CLAIMED) {
            return IncrementalJobResult.prepared(prepared);
        }
        return executeClaimed(
                feedId, recoveryToken, prepared,
                actionDeadlineNanos, reservation);
    }

    public RunCancellationResult cancel(
            FrontierRecoveryToken frontierToken,
            String runId, String requestId) {
        if (frontierToken == null) {
            throw new IllegalArgumentException(
                    "frontier recovery token is required");
        }
        RecoveryToken runToken = claimRun(frontierToken, runId);
        return authority.requestUserCancellation(
                frontierToken, runToken, runId, requestId);
    }

    public IncrementalJobResult recover(
            String feedId, FrontierRecoveryToken frontierToken,
            LongConsumer reservation) {
        if (frontierToken == null || reservation == null) {
            throw new IllegalArgumentException(
                    "recovery token and reservation are required");
        }
        IncrementalAuthorityRecord record = authority.record(feedId);
        require(record != null && record.activeInterval() != null
                        && record.activeInterval().phase()
                        == IncrementalAttemptCommitPhase.COMMIT_DECIDED,
                "local recovery requires a commit-decided active interval");
        IncrementalActiveInterval active = record.activeInterval();
        byte[] encodedArtifact = artifacts.get(active.artifactId(), reservation);
        require(encodedArtifact != null,
                "incremental recovery Artifact is unavailable");
        PlanArtifact artifact = PlanArtifactCodec.decode(
                encodedArtifact, reservation);
        require(active.artifactId().equals(artifact.artifactId())
                        && IncrementalPlanArtifactMaterializer
                        .definitionIdentity(definition.fingerprint()).equals(
                                artifact.definitionId()),
                "incremental recovery Artifact identity differs");
        PlanGraph materialized =
                LegacyPlanArtifactAdapter.unwrapIncrementalValidated(
                        artifact, trustContext, reservation);
        RecoveryToken runToken = claimRunReplacement(
                frontierToken, active.fence().runId());
        RuntimeGraph runtime = bindRuntimeGraph(
                materialized, active, active.fence().runId(),
                frontierToken, runToken, Long.MAX_VALUE);
        VerifiedIncrementalSinkCommitDecision sinkDecision =
                IncrementalSinkIntentStrictAdapter.verify(active, authority);
        VerifiedIncrementalPublicationProof publication =
                IncrementalManifestStrictAdapter.verify(
                        sinkDecision, runtime.store,
                        frontierToken, runToken);
        VerifiedIncrementalJointCommitProof proof =
                IncrementalJointCommitProofAssembler.join(
                        active, sinkDecision, publication);
        IncrementalAuthorityCommandKind kind = active.capturedTarget().mode()
                == IncrementalRunMode.BACKFILL_V1
                ? IncrementalAuthorityCommandKind.COMPLETE_BACKFILL
                : IncrementalAuthorityCommandKind.COMMIT;
        IncrementalAuthorityCommand terminal =
                new IncrementalAuthorityCommand(
                        kind, feedId,
                        "local-terminal-" + active.identity().intervalId(),
                        authority.record(feedId).revision(),
                        record.feedIncarnation(),
                        active.identity().intervalId(), active.fence(),
                        proof.proofDigest());
        IncrementalAuthorityDecision committed = authority.apply(
                IncrementalAuthorityMutation.jointTerminal(
                        terminal, frontierToken, runToken, proof));
        require(committed.code() == IncrementalAuthorityDecision.Code.ACCEPTED
                        || committed.code()
                        == IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                "incremental recovery joint terminal was rejected: "
                        + committed.code());
        return IncrementalJobResult.recovered(active, committed);
    }

    private IncrementalJobResult executeClaimed(
            String feedId, FrontierRecoveryToken frontierToken,
            IncrementalIntervalPreparationDecision prepared,
            long actionDeadlineNanos, LongConsumer reservation) {
        IncrementalClaimRequest request = prepared.claimRequest();
        byte[] encodedArtifact = artifacts.get(
                prepared.artifactId(), reservation);
        if (encodedArtifact == null) {
            throw new IllegalStateException(
                    "incremental Artifact is unavailable");
        }
        PlanArtifact artifact = PlanArtifactCodec.decode(
                encodedArtifact, reservation);
        if (!prepared.artifactId().equals(artifact.artifactId())
                || !IncrementalPlanArtifactMaterializer
                .definitionIdentity(definition.fingerprint()).equals(
                        artifact.definitionId())) {
            throw new IllegalArgumentException(
                    "incremental Artifact identity differs");
        }
        PlanGraph materialized =
                LegacyPlanArtifactAdapter.unwrapIncrementalValidated(
                        artifact, trustContext, reservation);
        RecoveryToken runToken = claimRun(frontierToken, request.runId());
        while (true) {
            IncrementalAuthorityRecord record = authority.record(feedId);
            requireFrozen(prepared, record);
            try {
                return executeAttempt(
                        feedId, frontierToken, runToken, prepared,
                        materialized, record, actionDeadlineNanos);
            } catch (RetrySignal retry) {
                runToken = retry.runToken;
            }
        }
    }

    private IncrementalJobResult executeAttempt(
            String feedId, FrontierRecoveryToken frontierToken,
            RecoveryToken recoveryToken,
            IncrementalIntervalPreparationDecision prepared,
            PlanGraph materialized, IncrementalAuthorityRecord record,
            long actionDeadlineNanos) {
        IncrementalClaimRequest request = prepared.claimRequest();
        IncrementalActiveInterval active = record.activeInterval();
        RunSnapshot run = authority.runSnapshot(request.runId());
        require(run != null && run.currentAttempt() != null
                        && active.fence().attemptId().equals(
                        run.currentAttempt().attemptId())
                        && active.fence().fenceToken()
                        == run.currentAttempt().fenceToken(),
                "incremental Run attempt differs from active interval");
        String publicationId = run.currentAttempt().publicationId();
        RuntimeGraph runtime = bindRuntimeGraph(
                materialized, active, request.runId(),
                frontierToken, recoveryToken, actionDeadlineNanos);
        long generation = new FileCommitGenerationAuthority().reserve(
                runtime.store.domainPath(), publicationId);
        runtime.bindOutputs(publicationId, generation);

        StageAssignmentManifest assignments = assignmentManifest(
                runtime, active);
        StageAssignmentSnapshot assignmentSnapshot = publishAssignments(
                assignments, frontierToken, recoveryToken);
        List<byte[]> commandDigests = acceptAssignments(
                assignmentSnapshot, frontierToken, recoveryToken);
        advance(frontierToken, recoveryToken, request,
                AttemptStatus.DISPATCHING,
                "local-dispatch");
        advance(frontierToken, recoveryToken, request, AttemptStatus.RUNNING,
                "local-running");

        final ExecutionReport report;
        try {
            report = executionClient.submit(runtime.graph);
        } catch (RuntimeException | Error failure) {
            AttemptStatus cancellation = cancellationTerminal(
                    request.runId());
            if (cancellation != null) {
                return finishCancellation(
                        frontierToken, recoveryToken, prepared, request,
                        runtime, assignments, commandDigests, generation,
                        publicationId, cancellation);
            }
            RecoveryToken retryToken;
            try {
                retryToken = prepareRetry(
                        frontierToken, recoveryToken, request, runtime,
                        assignments, commandDigests,
                        generation, publicationId, actionDeadlineNanos);
            } catch (RuntimeException | Error reconciliationFailure) {
                failure.addSuppressed(reconciliationFailure);
                throw failure;
            }
            if (retryToken == null) throw failure;
            throw new RetrySignal(retryToken);
        }
        long userRows = perUserRows(report, runtime.sinks.size());
        List<CommitManifest.Member> dataMembers =
                runtime.dataMembers();
        CommitManifest.Member controlMember = installControlMember(
                feedId, record.activeInterval(),
                publicationId, generation, dataMembers, runtime.store);
        List<CommitManifest.Member> allMembers =
                new ArrayList<>(dataMembers);
        allMembers.add(controlMember);

        recordOutputEvidence(
                runtime, assignments, assignmentSnapshot, commandDigests,
                generation, userRows, dataMembers, controlMember,
                frontierToken, recoveryToken);
        advance(frontierToken, recoveryToken, request,
                AttemptStatus.COMMITTING,
                "local-committing");
        ManifestCommitIntent intent = authority.manifestCommitJournal(
                active.fence()).buildIntent();
        ManifestIntentDecision intentDecision = recoveryToken == null
                ? authority.commitManifestIntent(intent)
                : authority.commitManifestIntentRecovered(
                frontierToken, recoveryToken, intent);
        require(intentDecision.code() == ManifestIntentDecision.Code.COMMITTED
                        || intentDecision.code()
                        == ManifestIntentDecision.Code.ALREADY_COMMITTED,
                "incremental Manifest intent was rejected: "
                        + intentDecision.code());

        CommitManifest manifest = new CommitManifest(
                runtime.store.domainId(), generation,
                publicationId, allMembers);
        runtime.store.publish(manifest);
        RecoveryToken runToken = recoveryToken == null
                ? claimRun(frontierToken, request.runId()) : recoveryToken;
        IncrementalActiveInterval decided =
                authority.record(feedId).activeInterval();
        VerifiedIncrementalSinkCommitDecision sinkDecision =
                IncrementalSinkIntentStrictAdapter.verify(decided, authority);
        VerifiedIncrementalPublicationProof publication =
                IncrementalManifestStrictAdapter.verify(
                        sinkDecision, runtime.store,
                        frontierToken, runToken);
        VerifiedIncrementalJointCommitProof proof =
                IncrementalJointCommitProofAssembler.join(
                        decided, sinkDecision, publication);
        IncrementalAuthorityCommandKind kind = request.capturedTarget().mode()
                == IncrementalRunMode.BACKFILL_V1
                ? IncrementalAuthorityCommandKind.COMPLETE_BACKFILL
                : IncrementalAuthorityCommandKind.COMMIT;
        IncrementalAuthorityCommand terminal =
                new IncrementalAuthorityCommand(
                        kind, feedId,
                        "local-terminal-" + request.intervalId(),
                        authority.record(feedId).revision(),
                        record.feedIncarnation(), request.intervalId(),
                        decided.fence(), proof.proofDigest());
        IncrementalAuthorityDecision committed = authority.apply(
                IncrementalAuthorityMutation.jointTerminal(
                        terminal, frontierToken, runToken, proof));
        require(committed.code() == IncrementalAuthorityDecision.Code.ACCEPTED
                        || committed.code()
                        == IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                "incremental joint terminal was rejected: "
                        + committed.code());
        runtime.finalizeOutputs();
        return IncrementalJobResult.succeeded(prepared, report, committed);
    }

    private RecoveryToken prepareRetry(
            FrontierRecoveryToken frontierToken,
            RecoveryToken currentRunToken,
            IncrementalClaimRequest request, RuntimeGraph runtime,
            StageAssignmentManifest assignments,
            List<byte[]> commandDigests, long generation,
            String publicationId, long actionDeadlineNanos) {
        RecoveryToken runToken = currentRunToken == null
                ? claimRun(frontierToken, request.runId()) : currentRunToken;
        long eventTime = nextRunEventTime(request.runId());
        byte[] failureDigest = sha256(
                "LOCAL_EXECUTION_LOST".getBytes(StandardCharsets.US_ASCII));
        for (int ordinal = 0;
             ordinal < assignments.assignmentCount(); ordinal++) {
            StageAssignmentSnapshot current = authority.assignmentSnapshot(
                    assignments.fence());
            if (current == null || current.acceptance(ordinal) == null) {
                throw new IllegalStateException(
                        "incremental retry assignment evidence is unavailable");
            }
            if (current.terminal(ordinal) != null) continue;
            StageTerminalDecision terminal = authority.recordTerminalRecovered(
                    frontierToken, runToken, StageTerminalReceipt.lost(
                            current.acceptance(ordinal).token(),
                            commandDigests.get(ordinal),
                            "LOCAL_EXECUTION_LOST", failureDigest, 0L));
            require(terminal.code() == StageTerminalDecision.Code.RECORDED
                            || terminal.code()
                            == StageTerminalDecision.Code.ALREADY_RECORDED,
                    "incremental retry terminal was rejected: "
                            + terminal.code());
        }

        BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                assignments.fence(), assignments.generation(),
                BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1,
                runtime.store.domainId(), generation, publicationId,
                runtime.sinkGroupIdentity(assignments),
                "LOCAL_EXECUTION_LOST", eventTime);
        BatchSinkAbortDecision decided = authority.recordBatchSinkAbortRecovered(
                frontierToken, runToken, abort);
        require(decided.code() == BatchSinkAbortDecision.Code.RECORDED
                        || decided.code()
                        == BatchSinkAbortDecision.Code.ALREADY_RECORDED,
                "incremental abort decision was rejected: "
                        + decided.code());
        runtime.rollbackOutputs();
        BatchSinkAbortRecord retained = decided.record();
        if (retained.state() == BatchSinkAbortRecord.State.ABORT_DECIDED) {
            BatchSinkAbortDecision completed =
                    authority.recordBatchSinkAbortRecovered(
                            frontierToken, runToken,
                            retained.completed(eventTime));
            require(completed.code() == BatchSinkAbortDecision.Code.RECORDED
                            || completed.code()
                            == BatchSinkAbortDecision.Code.ALREADY_RECORDED,
                    "incremental abort completion was rejected: "
                            + completed.code());
        }

        WholeAttemptRetryDecision scheduled = authority.scheduleRetry(
                frontierToken, runToken, assignments.fence());
        if (scheduled.code() == WholeAttemptRetryDecision.Code.EXHAUSTED) {
            return null;
        }
        require(scheduled.code() == WholeAttemptRetryDecision.Code.SCHEDULED
                        || scheduled.code()
                        == WholeAttemptRetryDecision.Code.ALREADY_SCHEDULED,
                "incremental retry schedule was rejected: "
                        + scheduled.code());
        waitUntilRetryDue(
                frontierToken, runToken, request.runId(), actionDeadlineNanos);
        RetryAttemptClaim claimed = authority.claimDueRetry(
                frontierToken, runToken, request.runId());
        require(claimed.code() == RetryAttemptClaim.Code.CLAIMED
                        || claimed.code()
                        == RetryAttemptClaim.Code.ALREADY_CLAIMED,
                "incremental retry claim was rejected: " + claimed.code());
        IncrementalActiveInterval retried = authority.record(
                request.feedId()).activeInterval();
        require(retried != null
                        && retried.identity().intervalId().equals(
                        request.intervalId())
                        && retried.artifactId().equals(request.artifactId())
                        && retried.capturedTarget().equals(
                        request.capturedTarget())
                        && claimed.identity() != null
                        && retried.fence().attemptId().equals(
                        claimed.identity().attemptId())
                        && retried.fence().fenceToken()
                        == claimed.identity().fenceToken(),
                "incremental retry changed the frozen interval");
        return runToken;
    }

    private IncrementalJobResult finishCancellation(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalIntervalPreparationDecision prepared,
            IncrementalClaimRequest request, RuntimeGraph runtime,
            StageAssignmentManifest assignments,
            List<byte[]> commandDigests, long generation,
            String publicationId, AttemptStatus terminalStatus) {
        String reasonCode = terminalStatus == AttemptStatus.TIMED_OUT
                ? "RUN_DEADLINE" : "USER_CANCELED";
        long eventTime = nextRunEventTime(request.runId());
        byte[] failureDigest = sha256(
                reasonCode.getBytes(StandardCharsets.US_ASCII));
        for (int ordinal = 0;
             ordinal < assignments.assignmentCount(); ordinal++) {
            StageAssignmentSnapshot current = authority.assignmentSnapshot(
                    assignments.fence());
            require(current != null && current.acceptance(ordinal) != null,
                    "incremental cancellation assignment evidence is unavailable");
            if (current.terminal(ordinal) != null) continue;
            StageTerminalDecision terminal = authority.recordTerminalRecovered(
                    frontierToken, runToken, StageTerminalReceipt.canceled(
                            current.acceptance(ordinal).token(),
                            commandDigests.get(ordinal), 0L, 0L,
                            reasonCode, failureDigest, eventTime));
            require(terminal.code() == StageTerminalDecision.Code.RECORDED
                            || terminal.code()
                            == StageTerminalDecision.Code.ALREADY_RECORDED,
                    "incremental cancellation terminal was rejected: "
                            + terminal.code());
        }
        BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                assignments.fence(), assignments.generation(),
                BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1,
                runtime.store.domainId(), generation, publicationId,
                runtime.sinkGroupIdentity(assignments), reasonCode, eventTime);
        BatchSinkAbortDecision decided = authority.recordBatchSinkAbortRecovered(
                frontierToken, runToken, abort);
        require(decided.code() == BatchSinkAbortDecision.Code.RECORDED
                        || decided.code()
                        == BatchSinkAbortDecision.Code.ALREADY_RECORDED,
                "incremental cancellation abort was rejected: "
                        + decided.code());
        runtime.rollbackOutputs();
        BatchSinkAbortRecord retained = decided.record();
        if (retained.state() == BatchSinkAbortRecord.State.ABORT_DECIDED) {
            BatchSinkAbortDecision completed =
                    authority.recordBatchSinkAbortRecovered(
                            frontierToken, runToken,
                            retained.completed(eventTime));
            require(completed.code() == BatchSinkAbortDecision.Code.RECORDED
                            || completed.code()
                            == BatchSinkAbortDecision.Code.ALREADY_RECORDED,
                    "incremental cancellation cleanup was rejected: "
                            + completed.code());
        }
        VerifiedIncrementalAbortClosureProof proof =
                authority.verifyAbortClosure(
                        frontierToken, runToken, assignments.fence(),
                        terminalStatus);
        IncrementalAuthorityRecord record = authority.record(request.feedId());
        IncrementalAuthorityCommand close = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN,
                request.feedId(),
                "local-close-" + terminalStatus.name() + '-'
                        + request.intervalId(),
                record.revision(), record.feedIncarnation(),
                request.intervalId(), assignments.fence(),
                proof.proofDigest());
        IncrementalAuthorityDecision closed = authority.apply(
                IncrementalAuthorityMutation.closeAborted(
                        close, frontierToken, runToken, proof));
        require(closed.code() == IncrementalAuthorityDecision.Code.ACCEPTED
                        || closed.code()
                        == IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                "incremental cancellation close was rejected: "
                        + closed.code());
        return IncrementalJobResult.terminal(
                prepared, terminalStatus, closed);
    }

    private AttemptStatus cancellationTerminal(String runId) {
        RunSnapshot run = authority.runSnapshot(runId);
        if (run == null || run.currentAttempt() == null
                || run.currentAttempt().cancellationCause() == null) {
            return null;
        }
        return run.currentAttempt().cancellationCause()
                == CancellationCause.TIMEOUT
                ? AttemptStatus.TIMED_OUT : AttemptStatus.CANCELED;
    }

    private void waitUntilRetryDue(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId, long actionDeadlineNanos) {
        while (true) {
            RetryReadyEvaluation readiness = authority.evaluateRetryReady(
                    frontierToken, runToken, runId);
            if (readiness.code() == RetryReadyEvaluation.Code.DUE) return;
            require(readiness.code() == RetryReadyEvaluation.Code.NOT_DUE,
                    "incremental retry readiness was rejected: "
                            + readiness.code());
            long remainingNanos = actionDeadlineNanos - System.nanoTime();
            require(remainingNanos > 0L,
                    "incremental retry exceeded its action deadline");
            long waitMillis = Math.max(1L, Math.min(50L,
                    readiness.notBeforeMillis()
                            - readiness.observedAtMillis()));
            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "incremental retry wait was interrupted", interrupted);
            }
        }
    }

    private RecoveryToken claimRun(
            FrontierRecoveryToken frontierToken, String runId) {
        RunSnapshot run = authority.runSnapshot(runId);
        RecoveryClaim ownership = authority.claim(
                runId, run.revision(), 0L, frontierToken.owner());
        require(ownership.code() == RecoveryClaim.Code.ACQUIRED
                        || ownership.code() == RecoveryClaim.Code.REPLAYED,
                "incremental Run ownership was rejected: "
                        + ownership.code());
        return ownership.token();
    }

    private RecoveryToken claimRunReplacement(
            FrontierRecoveryToken frontierToken, String runId) {
        RunSnapshot run = authority.runSnapshot(runId);
        RecoveryToken previous = authority.ownership(runId);
        RecoveryClaim ownership = authority.claim(
                runId, run.revision(),
                previous == null ? 0L : previous.ownershipEpoch(),
                frontierToken.owner());
        require(ownership.code() == RecoveryClaim.Code.ACQUIRED
                        || ownership.code() == RecoveryClaim.Code.REPLAYED,
                "incremental replacement Run ownership was rejected: "
                        + ownership.code());
        return ownership.token();
    }

    private boolean cancellationRequested(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        RunDeadlineResult deadline = authority.evaluateDeadline(
                frontierToken, runToken, runId);
        require(deadline.disposition() != RunDeadlineResult.Disposition.NOT_FOUND
                        && deadline.disposition()
                        != RunDeadlineResult.Disposition.RETIRING,
                "incremental Run disappeared during deadline evaluation");
        RunSnapshot run = authority.runSnapshot(runId);
        return run == null || run.currentAttempt() == null
                || run.currentAttempt().cancellationCause() != null;
    }

    private long nextRunEventTime(String runId) {
        RunSnapshot run = authority.runSnapshot(runId);
        return run.updatedAtMillis() == Long.MAX_VALUE
                ? Long.MAX_VALUE : run.updatedAtMillis() + 1L;
    }

    private RuntimeGraph bindRuntimeGraph(
            PlanGraph materialized, IncrementalActiveInterval active,
            String runId,
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            long actionDeadlineNanos) {
        PlanGraph graph = materialized;
        int sourceIndex = -1;
        List<RuntimeSink> sinks = new ArrayList<>();
        ManifestFileStore store = null;
        for (int index = 0; index < materialized.nodeCount(); index++) {
            PlanNode node = materialized.node(index);
            if (node.kind() == PlanNode.Kind.SOURCE) {
                if (sourceIndex >= 0 || !node.hasIncrementalPlanDescriptor()) {
                    throw new IllegalArgumentException(
                            "incremental runtime requires one SOURCE");
                }
                sourceIndex = index;
                List<io.rowweave.Source> runtimeInputs = new ArrayList<>();
                List<BatchSourceSplitBinding> bindings =
                        node.sourceSplitBindings();
                for (int split = 0; split < bindings.size(); split++) {
                    IncrementalChangelogRowValidator validator =
                            new IncrementalChangelogRowValidator(
                                    node.outMeta(), definition.planDescriptor(),
                                    active.capturedTarget(),
                                    definition.planDescriptor()
                                            .partitionSetDigest(), split);
                    BatchExecutableSource runtimeInput =
                            IncrementalSourceRuntime.bind(
                                    bindings.get(split), sourceContract,
                                    configuration, active.capturedTarget(),
                                    definition.planDescriptor()
                                            .partitionSetDigest(),
                                    sourceProvider, supervisor,
                                    actionDeadlineNanos,
                                    () -> cancellationRequested(
                                            frontierToken, runToken,
                                            runId),
                                    validator);
                    runtimeInputs.add(runtimeInput);
                }
                PlanNode replacement = PlanNode.sourceFromInputs(
                        node.outMeta(), runtimeInputs);
                for (java.util.Map.Entry<String, String> entry
                        : node.params().entrySet()) {
                    replacement = replacement.withParam(
                            entry.getKey(), entry.getValue());
                }
                for (java.util.Map.Entry<String, List<String>> entry
                        : node.listParams().entrySet()) {
                    if (PlanNode.INCREMENTAL_PLAN_DESCRIPTOR_PARAM.equals(
                            entry.getKey())
                            || PlanNode.SOURCE_SPLIT_DESCRIPTORS_PARAM.equals(
                            entry.getKey())) {
                        continue;
                    }
                    replacement = replacement.withListParam(
                            entry.getKey(), entry.getValue());
                }
                graph = graph.withNodeReplaced(index, replacement);
            } else if (node.kind() == PlanNode.Kind.SINK) {
                if (node.output() == null
                        || node.output().getClass()
                        != ProviderBatchSink.class) {
                    throw new IllegalArgumentException(
                            "incremental local runtime requires provider Sink proxies");
                }
                ProviderBatchSink proxy =
                        (ProviderBatchSink) node.output();
                ManifestFileSink manifest =
                        ManifestFileSink.fromProviderSink(proxy);
                if (IncrementalControlMemberCodec.MEMBER_NAME.equals(
                        manifest.memberName())) {
                    throw new IllegalArgumentException(
                            "reserved incremental control member is engine-owned");
                }
                ManifestFileStore candidate = new ManifestFileStore(
                        java.nio.file.Paths.get(manifest.domainPath()));
                if (store == null) store = candidate;
                else if (!store.domainId().equals(candidate.domainId())) {
                    throw new IllegalArgumentException(
                            "incremental Sinks must share one Manifest domain");
                }
                PreparedManifestOutput output =
                        new PreparedManifestOutput(manifest, proxy);
                PlanNode.Builder builder = PlanNode.sink(
                        node.outMeta(), node.child(0), output);
                for (java.util.Map.Entry<String, String> entry
                        : node.params().entrySet()) {
                    builder.param(entry.getKey(), entry.getValue());
                }
                for (java.util.Map.Entry<String, List<String>> entry
                        : node.listParams().entrySet()) {
                    builder.listParam(entry.getKey(), entry.getValue());
                }
                graph = graph.withNodeReplaced(index, builder.build());
                sinks.add(new RuntimeSink(index, output));
            } else if (node.kind() == PlanNode.Kind.UNARY) {
                if (!"incrementalMap".equals(node.typeId())
                        || !(node.userFunction()
                        instanceof IncrementalMapFunction)) {
                    throw new IllegalArgumentException(
                            "incremental runtime only permits IncrementalMapFunction");
                }
                IncrementalMapFunction guarded =
                        IncrementalMapIdentityGuard.guard(
                                (IncrementalMapFunction) node.userFunction(),
                                node.outMeta(), definition.planDescriptor());
                graph = graph.withNodeReplaced(index,
                        PlanNode.unaryFnWithParams(
                                node.typeId(), node.outMeta(), node.child(0),
                                node.params(), node.listParams(), guarded));
            }
        }
        if (sourceIndex < 0 || sinks.isEmpty() || store == null
                || sinks.size() + 1
                > StageAssignmentManifest.MAX_ASSIGNMENTS) {
            throw new IllegalArgumentException(
                    "incremental runtime graph shape is invalid");
        }
        sinks.sort(Comparator.comparing(RuntimeSink::memberName));
        return new RuntimeGraph(graph, sinks, store);
    }

    private StageAssignmentManifest assignmentManifest(
            RuntimeGraph runtime, IncrementalActiveInterval active) {
        List<StageAssignmentRecord> records = new ArrayList<>();
        long assignmentGeneration = active.attemptOrdinal();
        for (int ordinal = 0; ordinal < runtime.sinks.size(); ordinal++) {
            RuntimeSink sink = runtime.sinks.get(ordinal);
            records.add(StageAssignmentRecord.create(
                    active.fence(), assignmentGeneration, ordinal,
                    ordinal + 1L, sink.nodeIndex, 0,
                    StageAssignmentRole.SINK, "local-worker", "localhost",
                    1, -1, "local-incarnation"));
        }
        int controlOrdinal = records.size();
        records.add(StageAssignmentRecord.create(
                active.fence(), assignmentGeneration, controlOrdinal,
                controlOrdinal + 1L, runtime.graph.nodeCount(), 0,
                StageAssignmentRole.SYSTEM_CONTROL,
                "local-worker", "localhost",
                1, -1, "local-incarnation"));
        return new StageAssignmentManifest(
                active.fence(), active.artifactId(), assignmentGeneration,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1, records);
    }

    private StageAssignmentSnapshot publishAssignments(
            StageAssignmentManifest manifest,
            FrontierRecoveryToken frontierToken,
            RecoveryToken runToken) {
        StageManifestPublication publication = runToken == null
                ? authority.publish(manifest)
                : authority.publishRecovered(
                frontierToken, runToken, manifest);
        require(publication.code() == StageManifestPublication.Code.PUBLISHED
                        || publication.code()
                        == StageManifestPublication.Code.ALREADY_PUBLISHED,
                "incremental assignments were rejected: "
                        + publication.code());
        return publication.snapshot();
    }

    private List<byte[]> acceptAssignments(
            StageAssignmentSnapshot snapshot,
            FrontierRecoveryToken frontierToken,
            RecoveryToken runToken) {
        int assignmentCount = snapshot.manifest().assignmentCount();
        List<byte[]> digests = new ArrayList<>(assignmentCount);
        long eventTime = Math.max(1L, System.currentTimeMillis());
        for (int ordinal = 0; ordinal < assignmentCount; ordinal++) {
            byte[] digest = sha256(snapshot.token(ordinal)
                    .stageExecutionId().getBytes(StandardCharsets.US_ASCII));
            StageAssignmentAcceptance acceptance =
                    new StageAssignmentAcceptance(
                            snapshot.token(ordinal), digest, eventTime);
            StageAcceptanceDecision accepted = runToken == null
                    ? authority.accept(acceptance)
                    : authority.acceptRecovered(
                    frontierToken, runToken, acceptance);
            require(accepted.code() == StageAcceptanceDecision.Code.ACCEPTED
                            || accepted.code()
                            == StageAcceptanceDecision.Code.ALREADY_ACCEPTED,
                    "incremental assignment acceptance was rejected: "
                            + accepted.code());
            digests.add(digest);
        }
        return digests;
    }

    private void recordOutputEvidence(
            RuntimeGraph runtime, StageAssignmentManifest assignments,
            StageAssignmentSnapshot snapshot, List<byte[]> commandDigests,
            long generation, long userRows,
            List<CommitManifest.Member> dataMembers,
            CommitManifest.Member controlMember,
            FrontierRecoveryToken frontierToken,
            RecoveryToken runToken) {
        long eventTime = Math.max(1L, System.currentTimeMillis());
        List<CommitManifest.Member> all = new ArrayList<>(dataMembers);
        all.add(controlMember);
        for (int ordinal = 0; ordinal < all.size(); ordinal++) {
            CommitManifest.Member member = all.get(ordinal);
            StageTerminalReceipt terminalReceipt =
                    StageTerminalReceipt.succeeded(
                            snapshot.token(ordinal),
                            commandDigests.get(ordinal),
                            ordinal < dataMembers.size() ? userRows : 0L,
                            ordinal < dataMembers.size() ? userRows : 0L,
                            eventTime);
            StageTerminalDecision terminal = runToken == null
                    ? authority.recordTerminal(terminalReceipt)
                    : authority.recordTerminalRecovered(
                    frontierToken, runToken, terminalReceipt);
            require(terminal.code() == StageTerminalDecision.Code.RECORDED
                            || terminal.code()
                            == StageTerminalDecision.Code.ALREADY_RECORDED,
                    "incremental terminal evidence was rejected: "
                            + terminal.code());
            BatchSinkDescriptor descriptor = ordinal < runtime.sinks.size()
                    ? runtime.sinks.get(ordinal).output.batchSinkDescriptor()
                    : controlDescriptor(runtime.store);
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    assignments, assignments.assignment(ordinal),
                    ordinal < runtime.sinks.size()
                            ? runtime.graph.node(
                            runtime.sinks.get(ordinal).nodeIndex).param(
                            PlanNode.OPERATOR_UID_PARAM)
                            : IncrementalControlMemberCodec.MEMBER_NAME,
                    descriptor);
            byte[] memberFrame = ManifestCommitWire.encodeMember(member);
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.LEGACY_MANIFEST_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    runtime.store.domainId(), memberFrame,
                    member.length(), member.sha256(), eventTime);
            PreparedSinkReceipt preparedReceipt = new PreparedSinkReceipt(
                    snapshot.token(ordinal), commandDigests.get(ordinal), locator);
            PreparedSinkDecision prepared = runToken == null
                    ? authority.recordPreparedSink(preparedReceipt)
                    : authority.recordPreparedSinkRecovered(
                    frontierToken, runToken, preparedReceipt);
            require(prepared.code() == PreparedSinkDecision.Code.RECORDED
                            || prepared.code()
                            == PreparedSinkDecision.Code.ALREADY_RECORDED,
                    "incremental prepared Sink was rejected: "
                            + prepared.code());
            ManifestMemberReceipt memberReceipt = ManifestMemberReceipt.sealed(
                    snapshot.token(ordinal), commandDigests.get(ordinal),
                    runtime.store.domainId(), generation,
                    runtime.publicationId(), member, eventTime, locator);
            ManifestMemberDecision sealed = runToken == null
                    ? authority.recordManifestMember(memberReceipt)
                    : authority.recordManifestMemberRecovered(
                    frontierToken, runToken, memberReceipt);
            require(sealed.code() == ManifestMemberDecision.Code.RECORDED
                            || sealed.code()
                            == ManifestMemberDecision.Code.ALREADY_RECORDED,
                    "incremental Manifest member was rejected: "
                            + sealed.code());
        }
    }

    private CommitManifest.Member installControlMember(
            String feedId, IncrementalActiveInterval active,
            String publicationId, long generation,
            List<CommitManifest.Member> dataMembers,
            ManifestFileStore store) {
        IncrementalControlMember control =
                IncrementalControlMemberCodec.create(
                        feedId, definition.fingerprint(), active,
                        definition.planDescriptor(), publicationId,
                        generation, dataMembers);
        byte[] frame = IncrementalControlMemberCodec.encode(control);
        Path staged = store.stagingPath(
                publicationId,
                IncrementalControlMemberCodec.MEMBER_NAME);
        try {
            Files.write(staged, frame,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "failed to stage incremental control member", failure);
        }
        store.forceStaged(staged);
        return store.installMember(
                generation, IncrementalControlMemberCodec.MEMBER_NAME,
                IncrementalControlMemberCodec.SCHEMA_FINGERPRINT,
                staged);
    }

    private void advance(
            FrontierRecoveryToken token, RecoveryToken runToken,
            IncrementalClaimRequest request,
            AttemptStatus target, String commandSuffix) {
        RunSnapshot run = authority.runSnapshot(request.runId());
        IncrementalAuthorityRecord record = authority.record(request.feedId());
        long eventTime = run.updatedAtMillis() == Long.MAX_VALUE
                ? Long.MAX_VALUE : run.updatedAtMillis() + 1L;
        IncrementalAttemptProgressCommand command =
                new IncrementalAttemptProgressCommand(
                        request.feedId(), request.intervalId(),
                        record.revision(), run.revision(),
                        record.activeInterval().fence(), target,
                        commandSuffix + '-' + request.intervalId() + '-'
                                + record.activeInterval().fence().attemptId(),
                        eventTime);
        RunDecision decision = runToken == null
                ? authority.advanceProgress(token, command)
                : authority.advanceProgress(token, runToken, command);
        require(decision.code() == RunDecision.Code.ACCEPTED
                        || decision.code() == RunDecision.Code.ALREADY_APPLIED,
                "incremental Run progress was rejected: "
                        + decision.code());
    }

    private static long perUserRows(
            ExecutionReport report, int userSinkCount) {
        if (report == null || userSinkCount <= 0
                || report.rowsOut() % userSinkCount != 0L) {
            throw new IllegalStateException(
                    "incremental one-to-one Sink row accounting is invalid");
        }
        return report.rowsOut() / userSinkCount;
    }

    private static BatchSinkDescriptor controlDescriptor(
            ManifestFileStore store) {
        return new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION,
                "rowweave.manifest-file", 1,
                hex(sha256(IncrementalControlMemberCodec.SCHEMA_FINGERPRINT
                        .getBytes(StandardCharsets.US_ASCII))),
                store.domainId() + "#member:"
                        + IncrementalControlMemberCodec.MEMBER_NAME,
                store.domainId(),
                BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1);
    }

    private static void requireFrozen(
            IncrementalIntervalPreparationDecision prepared,
            IncrementalAuthorityRecord record) {
        if (record == null || record.state()
                != IncrementalFrontierAuthorityState.ACTIVE
                || record.activeInterval() == null
                || !prepared.artifactId().equals(
                record.activeInterval().artifactId())
                || !prepared.claimRequest().intervalId().equals(
                record.activeInterval().identity().intervalId())
                || !prepared.claimRequest().capturedTarget().equals(
                record.activeInterval().capturedTarget())) {
            throw new IllegalArgumentException(
                    "incremental claimed interval differs from authority");
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String hex(byte[] value) {
        char[] chars = new char[value.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int index = 0; index < value.length; index++) {
            int unsigned = value[index] & 0xff;
            chars[index * 2] = digits[unsigned >>> 4];
            chars[index * 2 + 1] = digits[unsigned & 0xf];
        }
        return new String(chars);
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private static final class RuntimeGraph {
        private PlanGraph graph;
        private final List<RuntimeSink> sinks;
        private final ManifestFileStore store;

        private RuntimeGraph(
                PlanGraph graph, List<RuntimeSink> sinks,
                ManifestFileStore store) {
            this.graph = graph;
            this.sinks = sinks;
            this.store = store;
        }

        private void bindOutputs(String publicationId, long generation) {
            for (RuntimeSink sink : sinks) {
                sink.output.bind(publicationId, generation);
            }
        }

        private String publicationId() {
            return sinks.get(0).output.publicationId;
        }

        private List<CommitManifest.Member> dataMembers() {
            List<CommitManifest.Member> members = new ArrayList<>();
            for (RuntimeSink sink : sinks) {
                members.add(sink.output.installedMember());
            }
            return members;
        }

        private byte[] sinkGroupIdentity(
                StageAssignmentManifest assignments) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                for (int ordinal = 0;
                     ordinal < assignments.assignmentCount(); ordinal++) {
                    BatchSinkDescriptor descriptor = ordinal < sinks.size()
                            ? sinks.get(ordinal).output.batchSinkDescriptor()
                            : controlDescriptor(store);
                    String operatorId = ordinal < sinks.size()
                            ? graph.node(sinks.get(ordinal).nodeIndex).param(
                            PlanNode.OPERATOR_UID_PARAM)
                            : IncrementalControlMemberCodec.MEMBER_NAME;
                    digest.update(SinkAttemptIdentity.forAssignment(
                            assignments, assignments.assignment(ordinal),
                            operatorId, descriptor).identityDigest());
                }
                return digest.digest();
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException(
                        "SHA-256 is unavailable", impossible);
            }
        }

        private void rollbackOutputs() {
            Throwable failure = null;
            for (int index = sinks.size() - 1; index >= 0; index--) {
                try {
                    sinks.get(index).output.rollbackCommit();
                } catch (Throwable rollbackFailure) {
                    if (failure == null) failure = rollbackFailure;
                    else failure.addSuppressed(rollbackFailure);
                }
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) throw (Error) failure;
            if (failure != null) {
                throw new IllegalStateException(
                        "incremental output rollback failed", failure);
            }
        }

        private void finalizeOutputs() {
            for (RuntimeSink sink : sinks) {
                sink.output.finishVisible();
            }
        }
    }

    private static final class RuntimeSink {
        private final int nodeIndex;
        private final PreparedManifestOutput output;

        private RuntimeSink(int nodeIndex, PreparedManifestOutput output) {
            this.nodeIndex = nodeIndex;
            this.output = output;
        }

        private String memberName() { return output.memberName(); }
    }

    private static final class RetrySignal extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final RecoveryToken runToken;

        private RetrySignal(RecoveryToken runToken) {
            super(null, null, false, false);
            this.runToken = runToken;
        }
    }

    /** Runtime-only Sink: stage close prepares, authority publishes the group. */
    private static final class PreparedManifestOutput
            implements BatchSink {
        private static final long serialVersionUID = 1L;
        private final ManifestFileSink delegate;
        private final ProviderBatchSink proxy;
        private transient String publicationId;
        private transient boolean prepared;

        private PreparedManifestOutput(
                ManifestFileSink delegate,
                ProviderBatchSink proxy) {
            this.delegate = delegate;
            this.proxy = proxy;
        }

        private void bind(String publicationId, long generation) {
            this.publicationId = publicationId;
            delegate.bindManifestAttempt(publicationId, generation);
        }

        @Override public DataSchema getSchema() {
            return delegate.getSchema();
        }

        @Override public BatchSinkDescriptor batchSinkDescriptor() {
            return proxy.batchSinkDescriptor();
        }

        @Override public void collect(DataRow row) { delegate.collect(row); }

        @Override public void close() { prepareCommit(); }

        @Override public void abort() { rollbackCommit(); }

        @Override public boolean supportsCoordinatedCommit() { return true; }

        @Override public void prepareCommit() {
            if (prepared) return;
            delegate.prepareCommit();
            delegate.commitPrepared();
            delegate.sealInstalledMember();
            prepared = true;
        }

        @Override public void commitPrepared() {
            if (!prepared) {
                throw new IllegalStateException(
                        "incremental Manifest output is not prepared");
            }
        }

        @Override public void rollbackCommit() {
            delegate.rollbackCommit();
        }

        @Override public void finalizeCommit() {
            if (!prepared) {
                throw new IllegalStateException(
                        "incremental Manifest output is not prepared");
            }
        }

        @Override public boolean isWorkerWritable() { return true; }

        @Override public String exclusiveDestinationId() {
            return delegate.exclusiveDestinationId();
        }

        private CommitManifest.Member installedMember() {
            return delegate.installedMember();
        }

        private String memberName() { return delegate.memberName(); }

        private void finishVisible() { delegate.finalizeCommit(); }
    }
}
