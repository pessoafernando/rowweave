package io.rowweave;

/**
 * Single-pass rows of one physical Batch operator task. The cursor is valid
 * only while its {@link MapPartitionFunction} invocation is active.
 */
@FunctionalInterface
public interface PartitionDataRowCursor {
    /** Returns the next row, or {@code null} when this partition is exhausted. */
    DataRow next();
}
