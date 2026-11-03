package io.rowweave.stream.runtime;

import java.io.InputStream;
import java.io.OutputStream;

/** Signed deterministic byte-stream adapter for one explicit state mapping. */
public interface StreamStateMigrationAdapter {
    void migrate(InputStream source, OutputStream target) throws Exception;
}
