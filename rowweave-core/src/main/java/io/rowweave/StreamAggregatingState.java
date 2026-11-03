package io.rowweave;

/** Canonical aggregate accumulator scoped to the current key. */
public interface StreamAggregatingState {

    byte[] value();

    void update(byte[] accumulator);

    void clear();
}
