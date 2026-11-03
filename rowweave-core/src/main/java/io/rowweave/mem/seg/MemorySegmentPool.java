package io.rowweave.mem.seg;

/**
 * Segment pool abstraction: operators acquire and return fixed-length segments.
 * Deterministic return is the rule (a freed segment is reclaimed and reused at once), with outstanding() as the backstop for leak auditing.
 * HeapPool is the heap implementation; an off-heap implementation over pooled buffers substitutes behind the same interface.
 */
public interface MemorySegmentPool {

    MemorySegment allocate(int size);

    void free(MemorySegment segment);

    /** Segments not yet returned (leak audit: expected 0 once the job ends). */
    int outstanding();

    /** Releases every idle buffer held by the pool; active segments are unaffected. */
    default void trim() {
        // non-caching/off-heap implementations may have nothing to trim
    }
}
