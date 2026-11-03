package io.rowweave.mem.spill;

/** stable, closed failure categories for spill disk admission and lifecycle. */
public enum SpillFailureCode {
    SPILL_CONFIG_INVALID,
    SPILL_WORKSPACE_UNAVAILABLE,
    SPILL_WORKER_QUOTA_EXCEEDED,
    SPILL_JOB_QUOTA_EXCEEDED,
    SPILL_MIN_FREE_SPACE,
    SPILL_IO_WRITE_FAILED,
    SPILL_CLEANUP_FAILED
}
