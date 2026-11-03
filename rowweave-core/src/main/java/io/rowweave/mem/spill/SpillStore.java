package io.rowweave.mem.spill;

/**
 * Spill backend abstraction: creates runs and releases the underlying resources on close (a disk implementation deletes its temporary files).
 * MemorySpillStore suits fast unit tests; DiskSpillStore writes real files under rowweave.io.tmp.dirs to prevent OOM.
 */
public interface SpillStore extends AutoCloseable {

    SpillRun createRun();

    @Override
    void close();
}
