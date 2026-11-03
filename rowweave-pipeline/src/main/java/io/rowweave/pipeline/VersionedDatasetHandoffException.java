package io.rowweave.pipeline;

/** Stable state-transition failure from a VERSIONED_DATASET handoff authority. */
public final class VersionedDatasetHandoffException extends IllegalStateException {
    private final String code;

    VersionedDatasetHandoffException(String code, String detail) {
        super(code + ": " + detail);
        this.code = code;
    }

    public String code() { return code; }
}
