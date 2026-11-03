package io.rowweave.incremental;

import io.rowweave.artifact.PlanArtifact;

import java.security.MessageDigest;
import java.util.Objects;

/** Canonical client intent for one bounded incremental interval submission. */
public final class IncrementalRunSubmission {
    private final String feedId;
    private final String requestIdentity;
    private final IncrementalRunMode mode;
    private final long expectedBackfillOrdinal;
    private final IncrementalFrontier backfillFrom;
    private final IncrementalFrontier backfillTarget;
    private final String attemptId;
    private final String publicationId;
    private final int maxAttempts;
    private final long deadlineEpochMillis;
    private final long submittedAtMillis;
    private final IncrementalDefinition definition;
    private final PlanArtifact templateArtifact;
    private final IncrementalSourceProviderContract sourceContract;
    private final IncrementalSourceConfig sourceConfiguration;

    private IncrementalRunSubmission(
            String feedId, String requestIdentity, IncrementalRunMode mode,
            long expectedBackfillOrdinal, IncrementalFrontier backfillFrom,
            IncrementalFrontier backfillTarget, String attemptId,
            String publicationId, int maxAttempts, long deadlineEpochMillis,
            long submittedAtMillis, IncrementalDefinition definition,
            PlanArtifact templateArtifact,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig sourceConfiguration) {
        this.feedId = IncrementalAuthorityValue.text(feedId, "feed id");
        this.requestIdentity = IncrementalAuthorityValue.text(
                requestIdentity, "request identity");
        this.attemptId = IncrementalAuthorityValue.text(
                attemptId, "attempt id");
        this.publicationId = IncrementalAuthorityValue.text(
                publicationId, "publication id");
        if (mode == null || definition == null || templateArtifact == null
                || sourceContract == null || sourceConfiguration == null) {
            throw new IllegalArgumentException(
                    "incremental submission fields are required");
        }
        if (maxAttempts < 1
                || maxAttempts > IncrementalActiveInterval.MAX_ATTEMPTS
                || submittedAtMillis < 0L || deadlineEpochMillis < 0L
                || deadlineEpochMillis != 0L
                && deadlineEpochMillis < submittedAtMillis) {
            throw new IllegalArgumentException(
                    "incremental submission retry or deadline policy is invalid");
        }
        if (mode == IncrementalRunMode.BACKFILL_V1) {
            if (expectedBackfillOrdinal <= 0L || backfillFrom == null
                    || backfillTarget == null
                    || backfillFrom.kind() == IncrementalFrontier.Kind.BEGIN_V1
                    || backfillFrom.kind() != backfillTarget.kind()
                    || IncrementalFrontierOrder.compareConcrete(
                    backfillFrom, backfillTarget) >= 0) {
                throw new IllegalArgumentException(
                        "backfill submission requires exact positive-width bounds");
            }
        } else if ((mode != IncrementalRunMode.SNAPSHOT_V1
                && mode != IncrementalRunMode.INCREMENTAL_V1)
                || expectedBackfillOrdinal != 0L
                || backfillFrom != null || backfillTarget != null) {
            throw new IllegalArgumentException(
                    "normal submission cannot contain backfill bounds");
        }
        byte[] contractDigest =
                IncrementalSourceProviderContractCodec.digest(sourceContract);
        if (!definition.definitionId().equals(templateArtifact.definitionId())
                || templateArtifact.runtimeTarget()
                != PlanArtifact.RuntimeTarget.BATCH
                || templateArtifact.boundedness()
                != PlanArtifact.Boundedness.BOUNDED
                || templateArtifact.changelogMode()
                != PlanArtifact.ChangelogMode.UPSERT
                || !templateArtifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER)
                || !sourceContract.providerKey().equals(
                sourceConfiguration.providerKey())
                || !MessageDigest.isEqual(
                contractDigest, sourceConfiguration.contractDigest())
                || sourceConfiguration.configurationSize()
                > sourceContract.configurationCodec().maxPayloadBytes()) {
            throw new IllegalArgumentException(
                    "incremental submission static binding differs");
        }
        this.mode = mode;
        this.expectedBackfillOrdinal = expectedBackfillOrdinal;
        this.backfillFrom = backfillFrom;
        this.backfillTarget = backfillTarget;
        this.maxAttempts = maxAttempts;
        this.deadlineEpochMillis = deadlineEpochMillis;
        this.submittedAtMillis = submittedAtMillis;
        this.definition = definition;
        this.templateArtifact = templateArtifact;
        this.sourceContract = sourceContract;
        this.sourceConfiguration = sourceConfiguration;
    }

    public static IncrementalRunSubmission normal(
            String feedId, String triggerId, IncrementalRunMode mode,
            String attemptId, String publicationId, int maxAttempts,
            long deadlineEpochMillis, long submittedAtMillis,
            IncrementalDefinition definition, PlanArtifact templateArtifact,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig sourceConfiguration) {
        return new IncrementalRunSubmission(
                feedId, triggerId, mode, 0L, null, null,
                attemptId, publicationId, maxAttempts, deadlineEpochMillis,
                submittedAtMillis, definition, templateArtifact,
                sourceContract, sourceConfiguration);
    }

    public static IncrementalRunSubmission backfill(
            String feedId, long expectedBackfillOrdinal,
            IncrementalFrontier from, IncrementalFrontier target,
            String requestKey, String attemptId, String publicationId,
            int maxAttempts, long deadlineEpochMillis,
            long submittedAtMillis, IncrementalDefinition definition,
            PlanArtifact templateArtifact,
            IncrementalSourceProviderContract sourceContract,
            IncrementalSourceConfig sourceConfiguration) {
        return new IncrementalRunSubmission(
                feedId, requestKey, IncrementalRunMode.BACKFILL_V1,
                expectedBackfillOrdinal, from, target, attemptId,
                publicationId, maxAttempts, deadlineEpochMillis,
                submittedAtMillis, definition, templateArtifact,
                sourceContract, sourceConfiguration);
    }

    public String feedId() { return feedId; }
    public String requestIdentity() { return requestIdentity; }
    public IncrementalRunMode mode() { return mode; }
    public long expectedBackfillOrdinal() { return expectedBackfillOrdinal; }
    public IncrementalFrontier backfillFrom() { return backfillFrom; }
    public IncrementalFrontier backfillTarget() { return backfillTarget; }
    public String attemptId() { return attemptId; }
    public String publicationId() { return publicationId; }
    public int maxAttempts() { return maxAttempts; }
    public long deadlineEpochMillis() { return deadlineEpochMillis; }
    public long submittedAtMillis() { return submittedAtMillis; }
    public IncrementalDefinition definition() { return definition; }
    public PlanArtifact templateArtifact() { return templateArtifact; }
    public IncrementalSourceProviderContract sourceContract() {
        return sourceContract;
    }
    public IncrementalSourceConfig sourceConfiguration() {
        return sourceConfiguration;
    }

    public IncrementalRunSubmission withTemplateArtifact(
            PlanArtifact replacement) {
        return new IncrementalRunSubmission(
                feedId, requestIdentity, mode, expectedBackfillOrdinal,
                backfillFrom, backfillTarget, attemptId, publicationId,
                maxAttempts, deadlineEpochMillis, submittedAtMillis,
                definition, replacement, sourceContract, sourceConfiguration);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalRunSubmission)) return false;
        IncrementalRunSubmission that = (IncrementalRunSubmission) other;
        return expectedBackfillOrdinal == that.expectedBackfillOrdinal
                && maxAttempts == that.maxAttempts
                && deadlineEpochMillis == that.deadlineEpochMillis
                && submittedAtMillis == that.submittedAtMillis
                && feedId.equals(that.feedId)
                && requestIdentity.equals(that.requestIdentity)
                && mode == that.mode
                && Objects.equals(backfillFrom, that.backfillFrom)
                && Objects.equals(backfillTarget, that.backfillTarget)
                && attemptId.equals(that.attemptId)
                && publicationId.equals(that.publicationId)
                && definition.equals(that.definition)
                && templateArtifact.artifactId().equals(
                that.templateArtifact.artifactId())
                && sourceContract.equals(that.sourceContract)
                && sourceConfiguration.equals(that.sourceConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(feedId, requestIdentity, mode,
                expectedBackfillOrdinal, backfillFrom, backfillTarget,
                attemptId, publicationId, maxAttempts, deadlineEpochMillis,
                submittedAtMillis, definition, templateArtifact.artifactId(),
                sourceContract, sourceConfiguration);
    }
}
