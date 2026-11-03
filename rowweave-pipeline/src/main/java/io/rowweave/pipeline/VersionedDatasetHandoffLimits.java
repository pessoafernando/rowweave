package io.rowweave.pipeline;

/** Public allocation limits for VERSIONED_DATASET handoff records. */
public final class VersionedDatasetHandoffLimits {
    public static final int MAX_ID_BYTES = 256;
    public static final int MAX_READER_BINDING_BYTES = 64 * 1024;
    public static final int MAX_RECORD_BYTES = 256 * 1024;
    public static final int SHA256_BYTES = 32;

    private VersionedDatasetHandoffLimits() { }
}
