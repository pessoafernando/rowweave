package io.rowweave.mem;

import io.rowweave.DataRow;

/**
 * Row storage abstraction for the sort operator.
 * putRow serializes into the buffer, sort orders it in place under a RowComparator, and iterate replays the ordered result.
 * The heap implementation stores byte[] values; a segmented one uses a normalized key, fixed-width and byte-comparable, plus a variable-length tail, leaving this interface and the operator unchanged.
 */
public interface SortBuffer extends Iterable<DataRow>, AutoCloseable {

    void putRow(DataRow row);

    /**
     * Zero-serialization entry point: accepts rows that are already serialized, as a network shuffle receiver gets them, which avoids
     * a deserialize-to-DataRow then re-serialize round trip inside the operator. An implementation that can take bytes directly, such as {@link PointerSortBuffer},
     * overrides this method and stores them as they arrive; the default throws UnsupportedOperation and the caller picks a byte-native buffer. The bytes must be in the
     * {@link RowSerializer} encoding.
     */
    default void putBytes(byte[] rowBytes) {
        throw new UnsupportedOperationException("putBytes not supported by " + getClass().getSimpleName()
                + "; use a byte-native SortBuffer (PointerSortBuffer)");
    }

    void sort();

    long size();

    /**
     * Credit intentionally retained after the final sort cursor reaches EOF so a grouped replay consumer can
     * serialize one row without racing another elastic owner. The SortBuffer keeps ownership until close.
     */
    default long retainedReplayScratchBytes() {
        return 0L;
    }

    /** Wakes internal workers and waiters without releasing any resource; the owner joins and closes afterwards. */
    default void cancel(Throwable cause) {
        // synchronous/stateless implementations have no worker to wake
    }

    /** Release the resident reservation and segments; a resource-backed implementation overrides this and must be idempotent. */
    @Override
    default void close() {
        // heap-only/stateless implementations have no external resource
    }
}
