package io.rowweave.incremental;

/** Core JDBC provider for timestamp plus a stable canonical tie-break key. */
public final class JdbcTimestampKeyIncrementalSourceProvider
        extends CoreJdbcIncrementalSourceProvider {
    private static final long serialVersionUID = 1L;

    public JdbcTimestampKeyIncrementalSourceProvider() {
        super(IncrementalFrontier.Kind.TIMESTAMP_KEY_V1, null);
    }

    public JdbcTimestampKeyIncrementalSourceProvider(
            JdbcIncrementalSourceAccess access) {
        super(IncrementalFrontier.Kind.TIMESTAMP_KEY_V1, access);
    }
}
