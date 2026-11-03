package io.rowweave.pipeline;

/** Public hard limits for the Pipeline composition contract. */
public final class PipelineLimits {
    public static final int MAX_ID_BYTES = 256;
    public static final int MAX_FEATURE_BYTES = 256;
    public static final int MAX_FEATURES = 256;
    public static final int MAX_STEPS = 4096;
    public static final int MAX_EDGES = 16384;
    public static final int MAX_OUTPUTS = 256;
    public static final int MAX_BODY_BYTES = 96 * 1024 * 1024;
    public static final int SHA256_BYTES = 32;

    private PipelineLimits() { }
}
