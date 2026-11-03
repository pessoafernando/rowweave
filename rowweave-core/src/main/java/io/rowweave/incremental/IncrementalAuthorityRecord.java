package io.rowweave.incremental;

import java.util.Arrays;
import java.util.Objects;

/** Bounded O(1) durable projection for one feed frontier authority. */
public final class IncrementalAuthorityRecord {
    public static final int SCHEMA_VERSION = 1;

    private final int schemaVersion;
    private final long revision;
    private final String feedId;
    private final long feedIncarnation;
    private final byte[] definitionDigest;
    private final byte[] sourceContractDigest;
    private final byte[] frontierContractDigest;
    private final IncrementalFrontier.Kind frontierKind;
    private final IncrementalFrontier committedFrontier;
    private final IncrementalActiveInterval activeInterval;
    private final IncrementalCommitReceipt lastCommitReceipt;
    private final long lastCompletedBackfillOrdinal;
    private final IncrementalCommitReceipt lastBackfillReceipt;
    private final IncrementalFrontierAuthorityState state;
    private final byte[] conflictDigest;

    public IncrementalAuthorityRecord(
            long revision, String feedId, long feedIncarnation,
            byte[] definitionDigest, byte[] sourceContractDigest,
            byte[] frontierContractDigest,
            IncrementalFrontier.Kind frontierKind,
            IncrementalFrontier committedFrontier,
            IncrementalActiveInterval activeInterval,
            IncrementalCommitReceipt lastCommitReceipt,
            long lastCompletedBackfillOrdinal,
            IncrementalCommitReceipt lastBackfillReceipt,
            IncrementalFrontierAuthorityState state, byte[] conflictDigest) {
        if (revision < 0L || feedIncarnation <= 0L) {
            throw new IllegalArgumentException("revision must be non-negative and incarnation positive");
        }
        if (frontierKind == null || frontierKind == IncrementalFrontier.Kind.BEGIN_V1
                || committedFrontier == null || state == null) {
            throw new IllegalArgumentException("frontier contract, committed frontier and state are required");
        }
        if (committedFrontier.kind() != IncrementalFrontier.Kind.BEGIN_V1
                && committedFrontier.kind() != frontierKind) {
            throw new IllegalArgumentException("committed frontier kind violates feed contract");
        }
        this.schemaVersion = SCHEMA_VERSION;
        this.revision = revision;
        this.feedId = IncrementalAuthorityValue.text(feedId, "feed id");
        this.feedIncarnation = feedIncarnation;
        this.definitionDigest = IncrementalAuthorityValue.digest(
                definitionDigest, "definition digest");
        this.sourceContractDigest = IncrementalAuthorityValue.digest(
                sourceContractDigest, "source contract digest");
        this.frontierContractDigest = IncrementalAuthorityValue.digest(
                frontierContractDigest, "frontier contract digest");
        this.frontierKind = frontierKind;
        this.committedFrontier = committedFrontier;
        this.activeInterval = activeInterval;
        this.lastCommitReceipt = lastCommitReceipt;
        this.lastCompletedBackfillOrdinal = lastCompletedBackfillOrdinal;
        this.lastBackfillReceipt = lastBackfillReceipt;
        this.state = state;
        this.conflictDigest = conflictDigest == null ? null
                : IncrementalAuthorityValue.digest(conflictDigest, "conflict digest");
        validate();
    }

    private void validate() {
        if (state == IncrementalFrontierAuthorityState.ACTIVE && activeInterval == null) {
            throw new IllegalArgumentException("ACTIVE record requires an active interval");
        }
        if (state == IncrementalFrontierAuthorityState.READY && activeInterval != null) {
            throw new IllegalArgumentException("READY record cannot retain an active interval");
        }
        if (state == IncrementalFrontierAuthorityState.CORRUPT_OR_CONFLICT) {
            if (conflictDigest == null) {
                throw new IllegalArgumentException("conflict record requires conflict digest");
            }
        } else if (conflictDigest != null) {
            throw new IllegalArgumentException("non-conflict record cannot carry conflict digest");
        }
        validateActive(activeInterval);
        validateReceipt(lastCommitReceipt, false);
        validateReceipt(lastBackfillReceipt, true);
        if ((lastBackfillReceipt == null) != (lastCompletedBackfillOrdinal == 0L)) {
            throw new IllegalArgumentException("backfill ordinal and receipt must appear together");
        }
        if (lastCompletedBackfillOrdinal < 0L
                || (lastBackfillReceipt != null
                && lastBackfillReceipt.expectedSequence() != lastCompletedBackfillOrdinal)) {
            throw new IllegalArgumentException("backfill ordinal does not match receipt");
        }
        if (lastCommitReceipt != null && !lastCommitReceipt.to().equals(committedFrontier)) {
            throw new IllegalArgumentException("last normal receipt does not match committed frontier");
        }
    }

    private void validateActive(IncrementalActiveInterval active) {
        if (active == null) return;
        CapturedTargetReceipt capture = active.capturedTarget();
        if (capture.feedIncarnation() != feedIncarnation
                || !Arrays.equals(capture.definitionDigest(), definitionDigest)
                || !Arrays.equals(capture.sourceContractDigest(), sourceContractDigest)) {
            throw new IllegalArgumentException("active interval feed contract does not match record");
        }
        if (state == IncrementalFrontierAuthorityState.ACTIVE
                && active.phase() == IncrementalAttemptCommitPhase.CLAIMED
                && active.attemptOrdinal() == 1) {
            long claimedRecordRevision;
            try {
                claimedRecordRevision = Math.addExact(active.claimedRevision(), 1L);
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("claimed revision is exhausted", overflow);
            }
            if (revision != claimedRecordRevision) {
                throw new IllegalArgumentException(
                        "CLAIMED record must be the immediate claim CAS revision");
            }
        } else if (active.claimedRevision() >= revision) {
            throw new IllegalArgumentException("active claim revision is not older than record revision");
        }
        requireBoundKind(capture.from());
        requireBoundKind(capture.target());
        if (capture.mode() == IncrementalRunMode.BACKFILL_V1) {
            long expected;
            try {
                expected = Math.addExact(lastCompletedBackfillOrdinal, 1L);
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("backfill ordinal is exhausted", overflow);
            }
            if (capture.expectedSequence() != expected) {
                throw new IllegalArgumentException("active backfill ordinal is not the next ordinal");
            }
        } else {
            if (!capture.from().equals(committedFrontier)) {
                throw new IllegalArgumentException("normal active interval does not start at committed frontier");
            }
            if (capture.mode() == IncrementalRunMode.INCREMENTAL_V1
                    && IncrementalFrontierOrder.compareConcrete(
                    capture.from(), capture.target()) == 0) {
                throw new IllegalArgumentException("normal no-op must not create an active interval");
            }
        }
    }

    private void validateReceipt(IncrementalCommitReceipt receipt, boolean backfill) {
        if (receipt == null) return;
        if ((receipt.mode() == IncrementalRunMode.BACKFILL_V1) != backfill) {
            throw new IllegalArgumentException("commit receipt is in the wrong retained slot");
        }
        if (!Arrays.equals(receipt.definitionFingerprint(), definitionDigest)) {
            throw new IllegalArgumentException("commit receipt definition does not match record");
        }
        if (receipt.claimedRevision() >= revision) {
            throw new IllegalArgumentException("commit receipt claim revision is not older than record");
        }
        requireBoundKind(receipt.from());
        requireBoundKind(receipt.to());
    }

    private void requireBoundKind(IncrementalFrontier value) {
        if (value.kind() != IncrementalFrontier.Kind.BEGIN_V1
                && value.kind() != frontierKind) {
            throw new IllegalArgumentException("interval frontier kind violates feed contract");
        }
    }

    public int schemaVersion() { return schemaVersion; }
    public long revision() { return revision; }
    public String feedId() { return feedId; }
    public long feedIncarnation() { return feedIncarnation; }
    public byte[] definitionDigest() { return definitionDigest.clone(); }
    public byte[] sourceContractDigest() { return sourceContractDigest.clone(); }
    public byte[] frontierContractDigest() { return frontierContractDigest.clone(); }
    public IncrementalFrontier.Kind frontierKind() { return frontierKind; }
    public IncrementalFrontier committedFrontier() { return committedFrontier; }
    public IncrementalActiveInterval activeInterval() { return activeInterval; }
    public IncrementalCommitReceipt lastCommitReceipt() { return lastCommitReceipt; }
    public long lastCompletedBackfillOrdinal() { return lastCompletedBackfillOrdinal; }
    public IncrementalCommitReceipt lastBackfillReceipt() { return lastBackfillReceipt; }
    public IncrementalFrontierAuthorityState state() { return state; }
    public byte[] conflictDigest() {
        return conflictDigest == null ? null : conflictDigest.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalAuthorityRecord)) return false;
        IncrementalAuthorityRecord that = (IncrementalAuthorityRecord) other;
        return schemaVersion == that.schemaVersion && revision == that.revision
                && feedIncarnation == that.feedIncarnation
                && lastCompletedBackfillOrdinal == that.lastCompletedBackfillOrdinal
                && feedId.equals(that.feedId) && frontierKind == that.frontierKind
                && committedFrontier.equals(that.committedFrontier)
                && Objects.equals(activeInterval, that.activeInterval)
                && Objects.equals(lastCommitReceipt, that.lastCommitReceipt)
                && Objects.equals(lastBackfillReceipt, that.lastBackfillReceipt)
                && state == that.state
                && Arrays.equals(definitionDigest, that.definitionDigest)
                && Arrays.equals(sourceContractDigest, that.sourceContractDigest)
                && Arrays.equals(frontierContractDigest, that.frontierContractDigest)
                && Arrays.equals(conflictDigest, that.conflictDigest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(schemaVersion, revision, feedId, feedIncarnation,
                frontierKind, committedFrontier, activeInterval, lastCommitReceipt,
                lastCompletedBackfillOrdinal, lastBackfillReceipt, state);
        result = 31 * result + Arrays.hashCode(definitionDigest);
        result = 31 * result + Arrays.hashCode(sourceContractDigest);
        result = 31 * result + Arrays.hashCode(frontierContractDigest);
        return 31 * result + Arrays.hashCode(conflictDigest);
    }
}
