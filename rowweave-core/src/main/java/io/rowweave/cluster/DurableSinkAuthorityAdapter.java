package io.rowweave.cluster;

import io.rowweave.commit.BatchSinkOutcome;

/** Internal authority projection; providers cannot implement or replace this seam. */
interface DurableSinkAuthorityAdapter {
    /** Returns the exact durable decision digest that must fence visibility. */
    byte[] requireCommitDecision();

    /** Reads the durable authority projection without executing provider code. */
    BatchSinkOutcome authorityOutcome();

    /** Returns the exact durable abort decision digest. */
    default byte[] requireAbortDecision(String reasonCode) {
        throw new UnsupportedOperationException(
                "durable Sink abort authority is unavailable");
    }

    default void completeAbort() {
        throw new UnsupportedOperationException(
                "durable Sink abort completion is unavailable");
    }
}
