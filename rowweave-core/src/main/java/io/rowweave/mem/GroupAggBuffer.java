package io.rowweave.mem;

import java.util.function.Supplier;

/**
 * Keyed accumulator table abstraction for grouped aggregation.
 *
 * <p>Operators program against this interface rather than a bare {@code HashMap}: a key is identified by the binary {@code byte[]} produced by RowSerializer
 * serializing the grouping key, and accumulators are created by a factory and updated in place. The heap implementation uses a HashMap; a segmented one puts the
 * accumulator table into off-heap segments plus recursive partitioning once the budget is exceeded, while this interface and the operator code stay as they are.
 *
 * <p>Order contract: a heap implementation without spill keeps first-seen group order; a spilling implementation is deterministic and iterates in the same order every time, but hash
 * partitioning changes the global first-seen order. Output that depends on order must state it explicitly with {@code orderBy}.
 */
public interface GroupAggBuffer extends Iterable<GroupAggBuffer.Group>, AutoCloseable {

    /**
     * Returns the accumulator array for this key, creating and registering one through the factory when it is absent.
     * The array is the same reference held inside the buffer, so accumulation the operator does in place is persisted.
     */
    Object[] getOrCreate(byte[] keyBytes);

    long groupCount();

    /** Release the working set; a resource-backed implementation must override this and be idempotent. */
    @Override
    default void close() {
        // heap-only/stateless compatibility implementations may have no external resource
    }

    /** One group: its binary key plus the accumulator array (the same reference). */
    interface Group {
        byte[] keyBytes();

        Object[] accumulators();
    }

    /** Factory for accumulator arrays, called once per new group. */
    interface AccumulatorFactory extends Supplier<Object[]> {
    }
}
