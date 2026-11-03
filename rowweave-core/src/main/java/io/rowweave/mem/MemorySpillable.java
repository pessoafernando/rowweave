package io.rowweave.mem;

/**
 * A memory holder that the MemoryManager can ask to spill, under a cooperative yielding protocol.
 * spill() writes the memory it holds to disk and releases it back to the MemoryManager, returning the bytes freed; 0 means nothing could be yielded.
 */
@FunctionalInterface
public interface MemorySpillable {

    long spill();
}
