package io.rowweave.mem.spill;

import java.util.Iterator;

/**
 * One spill run: byte frames written in append order, rereadable through read() once finishWriting has been called.
 * Frame boundaries are preserved by the implementation through a length prefix; an empty frame round-trips unchanged.
 */
public interface SpillRun {

    void append(byte[] frame);

    /** Marks the end of writing (a disk implementation flushes and switches to read mode). */
    void finishWriting();

    /** Rereads every frame in append order; may be called several times, each pass starting from the beginning. */
    Iterator<byte[]> read();

    /** Aborts an unfinished write and releases its resources; idempotent. A no-op on a finished run. */
    default void abort() {
        // Stateless/custom in-memory runs have nothing to release. Resource-owning runs override.
    }

    /** Releases all payload of a finished or unfinished run: closes the reader and invalidates the run permanently. Must be idempotent. */
    default void dispose() {
        abort();
    }
}
