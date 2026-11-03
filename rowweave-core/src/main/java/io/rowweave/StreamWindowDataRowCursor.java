package io.rowweave;

/** Closeable one-pass view over spill-backed full-window contents. */
public interface StreamWindowDataRowCursor extends AutoCloseable {

    boolean advance();

    /** Returns the current defensive row; valid only after a successful advance. */
    DataRow row();

    @Override
    void close();
}
