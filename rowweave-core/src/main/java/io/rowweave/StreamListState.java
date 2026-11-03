package io.rowweave;

/** Canonical append-only values scoped to the current key. */
public interface StreamListState {

    void add(byte[] value);

    StreamStateCursor values();

    void clear();
}
