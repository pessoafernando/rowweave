package io.rowweave.ops;

import io.rowweave.mem.Accumulator;

import java.nio.ByteBuffer;

/** Exact non-negative count state used only by quality summaries. */
final class TypedQualityCountAccumulator implements Accumulator {

    private long count;

    void increment() {
        count = Math.addExact(count, 1L);
    }

    long result() {
        return count;
    }

    @Override
    public byte[] writeState() {
        return ByteBuffer.allocate(Long.BYTES).putLong(count).array();
    }

    @Override
    public void mergeState(byte[] state) {
        if (state == null || state.length != Long.BYTES) {
            throw new IllegalArgumentException(
                    "QUALITY_SUMMARY_RUNTIME_INVALID: count state must be exactly 8 bytes");
        }
        long other = ByteBuffer.wrap(state).getLong();
        if (other < 0L) {
            throw new IllegalArgumentException(
                    "QUALITY_SUMMARY_RUNTIME_INVALID: count state must be non-negative");
        }
        count = Math.addExact(count, other);
    }
}
