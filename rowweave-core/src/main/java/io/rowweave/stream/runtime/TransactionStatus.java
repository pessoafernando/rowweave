package io.rowweave.stream.runtime;

/** Exact provider observation for one checkpoint transaction identity. */
public enum TransactionStatus {
    UNKNOWN, PREPARED, VISIBLE, ABORTED
}
