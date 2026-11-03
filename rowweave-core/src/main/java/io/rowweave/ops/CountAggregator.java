package io.rowweave.ops;

import io.rowweave.mem.Accumulator;

import java.nio.ByteBuffer;

/**
 * The count aggregator, with COUNT(*) semantics: every row counts, null values included. An empty set returns 0.
 * Implements {@link Accumulator}: spill serializes the count (8 bytes) and a merge adds them.
 */
public final class CountAggregator implements Accumulator {

    private long count;

    public void add(Object value) {
        count++;
    }

    public void merge(CountAggregator other) {
        if (other == null) {
            return;
        }
        count += other.count;
    }

    public long result() {
        return count;
    }

    @Override
    public byte[] writeState() {
        return ByteBuffer.allocate(8).putLong(count).array();
    }

    @Override
    public void mergeState(byte[] state) {
        if (state == null || state.length == 0) {
            return;
        }
        count += ByteBuffer.wrap(state).getLong();
    }
}
