package io.rowweave.mem.spill;

/**
 * One-frame reader. The returned frame and decode upper bound must be reserved before allocation;
 * an implementation may escrow the descriptor maximum for the reader lifetime. Readers never prefetch.
 */
public interface BudgetedFrameReader extends AutoCloseable {

    byte[] nextFrame();

    @Override
    void close();
}
