package io.rowweave.stream.spi;

/** Durable observation for one checkpoint transaction identity. */
public enum StreamTransactionStatus {
    UNKNOWN,
    PREPARED,
    VISIBLE,
    ABORTED
}
