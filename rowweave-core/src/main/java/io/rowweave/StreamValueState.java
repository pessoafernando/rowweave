package io.rowweave;

/** Canonical replacement value scoped to the current key. */
public interface StreamValueState {

    byte[] value();

    void update(byte[] value);

    void clear();
}
