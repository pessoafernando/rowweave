package io.rowweave.incremental;

import io.rowweave.artifact.ArtifactLifecycleRepository;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;

import java.security.MessageDigest;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/** Embedded capture-to-claim workflow. It deliberately stops before dispatch. */
public final class IncrementalIntervalPreparer {
    private final IncrementalDefinition definition;
    private final PlanGraph template;
    private final IncrementalSourceProviderContract sourceContract;
    private final IncrementalSourceConfig configuration;
    private final IncrementalSourceActions sourceActions;
    private final ArtifactLifecycleRepository artifacts;
    private final IncrementalPreparationAuthority authority;
    private final ObjectTrustContext trustContext;
    private final String deploymentSetId;
    private final Consumer<String> pendingRunReservation;

    IncrementalIntervalPreparer(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceActions sourceActions,
            ArtifactLifecycleRepository artifacts,
            IncrementalPreparationAuthority authority,
            ObjectTrustContext trustContext) {
        this(definition, template, sourceContract, configuration,
                sourceActions, artifacts, authority, trustContext, null,
                ignored -> { });
    }

    IncrementalIntervalPreparer(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceActions sourceActions,
            ArtifactLifecycleRepository artifacts,
            IncrementalPreparationAuthority authority,
            ObjectTrustContext trustContext, String deploymentSetId) {
        this(definition, template, sourceContract, configuration,
                sourceActions, artifacts, authority, trustContext,
                deploymentSetId, ignored -> { });
    }

    private IncrementalIntervalPreparer(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceActions sourceActions,
            ArtifactLifecycleRepository artifacts,
            IncrementalPreparationAuthority authority,
            ObjectTrustContext trustContext, String deploymentSetId,
            Consumer<String> pendingRunReservation) {
        if (definition == null || template == null || sourceContract == null
                || configuration == null || sourceActions == null
                || artifacts == null || authority == null
                || trustContext == null || pendingRunReservation == null) {
            throw new IllegalArgumentException(
                    "incremental interval preparer fields are required");
        }
        this.definition = definition;
        this.template = template;
        this.sourceContract = sourceContract;
        this.configuration = configuration;
        this.sourceActions = sourceActions;
        this.artifacts = artifacts;
        this.authority = authority;
        this.trustContext = trustContext;
        this.deploymentSetId = deploymentSetId;
        this.pendingRunReservation = pendingRunReservation;
    }

    /** Cluster preparation reuses the same capture-to-claim algorithm through one gateway. */
    public static IncrementalIntervalPreparer forCluster(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceActions sourceActions,
            ArtifactLifecycleRepository artifacts,
            IncrementalAuthorityGateway gateway,
            ObjectTrustContext trustContext) {
        return forCluster(
                definition, template, sourceContract, configuration,
                sourceActions, artifacts, gateway, trustContext, null,
                ignored -> { });
    }

    public static IncrementalIntervalPreparer forCluster(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceActions sourceActions,
            ArtifactLifecycleRepository artifacts,
            IncrementalAuthorityGateway gateway,
            ObjectTrustContext trustContext, String deploymentSetId) {
        return forCluster(definition, template, sourceContract, configuration,
                sourceActions, artifacts, gateway, trustContext,
                deploymentSetId, ignored -> { });
    }

    public static IncrementalIntervalPreparer forCluster(
            IncrementalDefinition definition, PlanGraph template,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig configuration,
            IncrementalSourceActions sourceActions,
            ArtifactLifecycleRepository artifacts,
            IncrementalAuthorityGateway gateway,
            ObjectTrustContext trustContext, String deploymentSetId,
            Consumer<String> pendingRunReservation) {
        if (gateway == null) {
            throw new IllegalArgumentException(
                    "incremental authority gateway is required");
        }
        IncrementalPreparationAuthority authority =
                new IncrementalPreparationAuthority() {
                    @Override
                    public IncrementalAuthorityRecord record(String feedId) {
                        return gateway.record(feedId);
                    }

                    @Override
                    public IncrementalPendingRunDecision publishPending(
                            FrontierRecoveryToken token,
                            IncrementalClaimRequest request) {
                        return gateway.publishPending(token, request);
                    }

                    @Override
                    public IncrementalAuthorityDecision apply(
                            IncrementalAuthorityMutation mutation) {
                        IncrementalClaimRequest request = mutation.claimRequest();
                        if (request == null
                                || mutation.command().kind()
                                != IncrementalAuthorityCommandKind.CLAIM) {
                            throw new IllegalArgumentException(
                                    "cluster preparation only permits CLAIM");
                        }
                        return gateway.claimInterval(
                                mutation.command().commandId(),
                                mutation.recoveryToken(), request);
                    }
                };
        return new IncrementalIntervalPreparer(
                definition, template, sourceContract, configuration,
                sourceActions, artifacts, authority, trustContext,
                deploymentSetId, pendingRunReservation);
    }

    public IncrementalIntervalPreparationDecision prepareNormal(
            String feedId, FrontierRecoveryToken recoveryToken,
            String triggerId, IncrementalRunMode mode,
            String attemptId, String publicationId, int maxAttempts,
            long deadlineEpochMillis, long eventTimeMillis,
            long actionDeadlineNanos, LongConsumer reservation) {
        if (mode != IncrementalRunMode.SNAPSHOT_V1
                && mode != IncrementalRunMode.INCREMENTAL_V1) {
            throw new IllegalArgumentException(
                    "normal preparation requires snapshot or incremental mode");
        }
        if (reservation == null) {
            throw new IllegalArgumentException("reservation is required");
        }
        IncrementalAuthorityRecord before = authority.record(feedId);
        requireFrozenBinding(feedId, before);
        if (before.state() != IncrementalFrontierAuthorityState.READY
                || before.activeInterval() != null) {
            return IncrementalIntervalPreparationDecision.lostRace(
                    null, null, before, null);
        }
        IncrementalCaptureRequest captureRequest =
                IncrementalCaptureRequest.normal(
                        before.feedIncarnation(), definition.fingerprint(),
                        before.revision(), before.committedFrontier(),
                        triggerId, mode);
        CapturedTargetReceipt captured = sourceActions.capture(
                captureRequest, actionDeadlineNanos);
        if (mode == IncrementalRunMode.INCREMENTAL_V1
                && captured.from().equals(captured.target())) {
            IncrementalAuthorityRecord confirmed = authority.record(feedId);
            return before.equals(confirmed)
                    ? IncrementalIntervalPreparationDecision.noOp(confirmed)
                    : IncrementalIntervalPreparationDecision.lostRace(
                    null, null, confirmed, null);
        }
        return materializeAndClaim(
                feedId, recoveryToken, before, captured,
                attemptId, publicationId, maxAttempts,
                deadlineEpochMillis, eventTimeMillis,
                actionDeadlineNanos, reservation);
    }

    private IncrementalIntervalPreparationDecision retainedBackfill(
            IncrementalAuthorityRecord record, long expectedOrdinal,
            IncrementalFrontier from, IncrementalFrontier target,
            String requestKey) {
        long completed = record.lastCompletedBackfillOrdinal();
        if (expectedOrdinal > completed) {
            if (completed == Long.MAX_VALUE
                    || expectedOrdinal != completed + 1L) {
                throw new IllegalArgumentException(
                        "backfill ordinal must be the next authority ordinal");
            }
            return null;
        }
        if (expectedOrdinal < completed) {
            return IncrementalIntervalPreparationDecision.staleBackfill(record);
        }
        IncrementalCommitReceipt receipt = record.lastBackfillReceipt();
        if (receipt == null) {
            throw new IllegalArgumentException(
                    "backfill replay has no retained authority receipt");
        }
        CapturedTargetReceipt identity = CapturedTargetReceipt.backfill(
                record.feedIncarnation(), definition.fingerprint(),
                IncrementalSourceProviderContractCodec.digest(sourceContract),
                expectedOrdinal, from, target, requestKey, new byte[]{1});
        if (!receipt.intervalId().equals(
                IncrementalIntervalIdentity.from(identity).intervalId())) {
            throw new IllegalArgumentException(
                    "backfill replay identity conflicts with retained receipt");
        }
        return IncrementalIntervalPreparationDecision.backfillReplay(record);
    }

    public IncrementalIntervalPreparationDecision prepareBackfill(
            String feedId, FrontierRecoveryToken recoveryToken,
            long expectedBackfillOrdinal, IncrementalFrontier from,
            IncrementalFrontier target, String requestKey,
            String attemptId, String publicationId, int maxAttempts,
            long deadlineEpochMillis, long eventTimeMillis,
            long actionDeadlineNanos, LongConsumer reservation) {
        if (reservation == null) {
            throw new IllegalArgumentException("reservation is required");
        }
        IncrementalAuthorityRecord before = authority.record(feedId);
        requireFrozenBinding(feedId, before);
        if (before.state() != IncrementalFrontierAuthorityState.READY
                || before.activeInterval() != null) {
            return IncrementalIntervalPreparationDecision.lostRace(
                    null, null, before, null);
        }
        IncrementalIntervalPreparationDecision retained =
                retainedBackfill(before, expectedBackfillOrdinal,
                        from, target, requestKey);
        if (retained != null) return retained;
        IncrementalCaptureRequest captureRequest =
                IncrementalCaptureRequest.backfill(
                        before.feedIncarnation(), definition.fingerprint(),
                        expectedBackfillOrdinal, from, target, requestKey);
        CapturedTargetReceipt captured = sourceActions.capture(
                captureRequest, actionDeadlineNanos);
        return materializeAndClaim(
                feedId, recoveryToken, before, captured,
                attemptId, publicationId, maxAttempts,
                deadlineEpochMillis, eventTimeMillis,
                actionDeadlineNanos, reservation);
    }

    private IncrementalIntervalPreparationDecision materializeAndClaim(
            String feedId, FrontierRecoveryToken recoveryToken,
            IncrementalAuthorityRecord before,
            CapturedTargetReceipt captured,
            String attemptId, String publicationId, int maxAttempts,
            long deadlineEpochMillis, long eventTimeMillis,
            long actionDeadlineNanos, LongConsumer reservation) {
        IncrementalMaterializationResponse materialized =
                sourceActions.materialize(
                        captured, actionDeadlineNanos, reservation);
        PlanArtifact artifact = IncrementalPlanArtifactMaterializer.materialize(
                definition, template, sourceContract, configuration,
                materialized, deploymentSetId, trustContext, reservation);
        byte[] encodedArtifact = PlanArtifactCodec.encode(
                artifact, reservation);
        IncrementalClaimRequest claimRequest = new IncrementalClaimRequest(
                feedId, captured,
                definition.planDescriptor().frontierContractDigest(),
                artifact.artifactId(), attemptId, publicationId,
                maxAttempts, deadlineEpochMillis, eventTimeMillis);
        pendingRunReservation.accept(claimRequest.runId());
        IncrementalPendingRunDecision pending =
                artifacts.publishReferencedIncrementalPending(
                        encodedArtifact, reservation, claimRequest,
                        request -> authority.publishPending(
                                recoveryToken, request));
        if (pending.code() != IncrementalPendingRunDecision.Code.PUBLISHED
                && pending.code()
                != IncrementalPendingRunDecision.Code.ALREADY_PUBLISHED) {
            return IncrementalIntervalPreparationDecision.lostRace(
                    artifact.artifactId(), claimRequest,
                    authority.record(feedId), pending.runSnapshot());
        }
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, feedId,
                "incremental-claim-" + claimRequest.intervalId(),
                before.revision(), before.feedIncarnation(),
                claimRequest.intervalId(), claimRequest.firstAttemptFence(),
                claimRequest.transitionDigest());
        IncrementalAuthorityDecision claimed = authority.apply(
                IncrementalAuthorityMutation.claim(
                        command, recoveryToken, claimRequest));
        if (claimed.code() == IncrementalAuthorityDecision.Code.ACCEPTED
                || claimed.code()
                == IncrementalAuthorityDecision.Code.ALREADY_APPLIED) {
            return IncrementalIntervalPreparationDecision.claimed(
                    artifact.artifactId(), claimRequest, claimed);
        }
        return IncrementalIntervalPreparationDecision.lostRace(
                artifact.artifactId(), claimRequest,
                claimed.record(), claimed.runSnapshot());
    }

    private void requireFrozenBinding(
            String feedId, IncrementalAuthorityRecord record) {
        byte[] contractDigest =
                IncrementalSourceProviderContractCodec.digest(sourceContract);
        if (record == null || !record.feedId().equals(feedId)
                || !MessageDigest.isEqual(
                record.definitionDigest(), definition.fingerprint())
                || !MessageDigest.isEqual(
                record.sourceContractDigest(), contractDigest)
                || !MessageDigest.isEqual(
                record.frontierContractDigest(),
                definition.planDescriptor().frontierContractDigest())
                || record.frontierKind() != sourceContract.frontierKind()) {
            throw new IllegalArgumentException(
                    "incremental authority differs from preparation binding");
        }
    }
}
