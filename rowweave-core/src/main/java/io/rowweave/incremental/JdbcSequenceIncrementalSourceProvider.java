package io.rowweave.incremental;

/** Core JDBC provider for a non-negative immutable sequence frontier. */
public final class JdbcSequenceIncrementalSourceProvider
        extends CoreJdbcIncrementalSourceProvider {
    private static final long serialVersionUID = 1L;

    public JdbcSequenceIncrementalSourceProvider() {
        super(IncrementalFrontier.Kind.INT64_SEQUENCE_V1, null);
    }

    public JdbcSequenceIncrementalSourceProvider(
            JdbcIncrementalSourceAccess access) {
        super(IncrementalFrontier.Kind.INT64_SEQUENCE_V1, access);
    }
}
