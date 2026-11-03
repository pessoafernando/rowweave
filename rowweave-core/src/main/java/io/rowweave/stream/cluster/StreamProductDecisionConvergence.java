package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamCdcPhase;

/** A durable cut that was fully published without requiring an active run. */
final class StreamProductDecisionConvergence {
    private final String referenceId;
    private final StreamCdcPhase cdcPhase;

    StreamProductDecisionConvergence(
            String referenceId, StreamCdcPhase cdcPhase) {
        if (referenceId == null || referenceId.isEmpty() || cdcPhase == null) {
            throw new IllegalArgumentException(
                    "Stream decision convergence is incomplete");
        }
        this.referenceId = referenceId;
        this.cdcPhase = cdcPhase;
    }

    String referenceId() { return referenceId; }
    StreamCdcPhase cdcPhase() { return cdcPhase; }
}
