package io.rowweave.incremental;

/** Package-private fault seam around the indivisible in-memory joint-success mutation. */
interface IncrementalJointSuccessObserver {
    void observe(IncrementalJointSuccessPhase phase);
}

enum IncrementalJointSuccessPhase {
    BEFORE_COMMIT,
    AFTER_COMMIT_BEFORE_ACK
}
