package io.rowweave.stream.cluster;

/** Creates a cluster execution from the immutable durable product snapshot. */
public interface StreamProductRunFactory {
    StreamProductRun create(StreamProductJobSnapshot snapshot);

    default StreamProductRun restart(
            StreamProductJobSnapshot snapshot, Throwable failure) {
        throw new UnsupportedOperationException(
                "Stream run factory does not support whole-graph restart");
    }

    /** Publishes an irreversible cut before restart or terminal transition. */
    default StreamProductDecisionConvergence convergeDurableDecision(
            StreamProductJobSnapshot snapshot) {
        return null;
    }

    /** Releases run-authority roots after the product job is durably terminal. */
    default void retire(StreamProductJobSnapshot snapshot) { }
}
