package io.rowweave;

/**
 * Closeable, one-pass view over managed state without a collection snapshot.
 */
public interface StreamStateCursor extends AutoCloseable {

    boolean advance();

    /** Whether the current entry has a map key; list entries do not. */
    boolean hasKey();

    byte[] key();

    byte[] value();

    @Override
    void close();
}
