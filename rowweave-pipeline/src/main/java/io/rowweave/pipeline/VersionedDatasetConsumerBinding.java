package io.rowweave.pipeline;

import java.util.Objects;

/** Durable read permission binding a consumer attempt to one visible winner. */
public final class VersionedDatasetConsumerBinding {
    private final VersionedDatasetConsumerRequest request;
    private final VersionedDatasetVisibleWinner winner;
    private final long boundAtEpochMillis;

    public VersionedDatasetConsumerBinding(
            VersionedDatasetConsumerRequest request,
            VersionedDatasetVisibleWinner winner,
            long boundAtEpochMillis) {
        if (request == null || winner == null) {
            throw PipelineText.failure("INVALID_BINDING", "binding",
                    "request and winner are required");
        }
        if (!request.key().equals(winner.evidence().attempt().key())
                || !request.expectedWinnerDigest().equals(winner.winnerDigest())) {
            throw PipelineText.failure("BINDING_MISMATCH", "binding",
                    "request does not identify the supplied winner");
        }
        this.request = request;
        this.winner = winner;
        this.boundAtEpochMillis = VersionedDatasetHandoffSupport.positive(
                boundAtEpochMillis, "binding.boundAtEpochMillis",
                "INVALID_TIMESTAMP");
    }

    public VersionedDatasetConsumerRequest request() { return request; }
    public VersionedDatasetVisibleWinner winner() { return winner; }
    public long boundAtEpochMillis() { return boundAtEpochMillis; }
    public String bindingId() {
        return VersionedDatasetHandoffCodec.sha256Hex(
                VersionedDatasetHandoffCodec.encodeConsumerBinding(this));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof VersionedDatasetConsumerBinding)) return false;
        VersionedDatasetConsumerBinding that = (VersionedDatasetConsumerBinding) other;
        return boundAtEpochMillis == that.boundAtEpochMillis
                && request.equals(that.request) && winner.equals(that.winner);
    }

    @Override
    public int hashCode() { return Objects.hash(request, winner, boundAtEpochMillis); }
}
