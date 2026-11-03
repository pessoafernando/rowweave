package io.rowweave.pipeline;

import java.util.Objects;

/** First durable visible winner for a handoff key. */
public final class VersionedDatasetVisibleWinner {
    private final VersionedDatasetCommitEvidence evidence;
    private final long visibleAtEpochMillis;

    public VersionedDatasetVisibleWinner(
            VersionedDatasetCommitEvidence evidence, long visibleAtEpochMillis) {
        if (evidence == null) {
            throw PipelineText.failure("INVALID_EVIDENCE", "winner.evidence",
                    "commit evidence is required");
        }
        this.evidence = evidence;
        this.visibleAtEpochMillis = VersionedDatasetHandoffSupport.positive(
                visibleAtEpochMillis, "winner.visibleAtEpochMillis",
                "INVALID_TIMESTAMP");
    }

    public VersionedDatasetCommitEvidence evidence() { return evidence; }
    public long visibleAtEpochMillis() { return visibleAtEpochMillis; }
    public String winnerDigest() { return evidence.evidenceDigest(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof VersionedDatasetVisibleWinner)) return false;
        VersionedDatasetVisibleWinner that = (VersionedDatasetVisibleWinner) other;
        return visibleAtEpochMillis == that.visibleAtEpochMillis
                && evidence.equals(that.evidence);
    }

    @Override
    public int hashCode() { return Objects.hash(evidence, visibleAtEpochMillis); }
}
