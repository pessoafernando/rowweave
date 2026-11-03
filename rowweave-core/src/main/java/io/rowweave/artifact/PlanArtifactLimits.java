package io.rowweave.artifact;

/** Fixed decode ceilings for the version 1 Artifact envelope. */
public final class PlanArtifactLimits {

    public static final int MAX_TEXT_BYTES = 256;
    public static final int MAX_SCHEMA_FINGERPRINT_BYTES = 128;
    public static final int MAX_FEATURES = 1_024;
    public static final int MAX_OPERATORS = 100_000;
    public static final int MAX_OPERATOR_INPUTS = 1_024;
    public static final int MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;
    public static final int MAX_BODY_BYTES = 72 * 1024 * 1024;
    static final int SHA256_BYTES = 32;

    private PlanArtifactLimits() {
    }
}
