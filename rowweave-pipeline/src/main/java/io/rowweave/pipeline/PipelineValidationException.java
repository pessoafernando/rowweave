package io.rowweave.pipeline;

/** Stable validation failure raised before Pipeline execution side effects. */
public final class PipelineValidationException extends IllegalArgumentException {
    private final String code;
    private final String path;

    PipelineValidationException(String code, String path, String detail) {
        super(code + " at " + path + ": " + detail);
        this.code = code;
        this.path = path;
    }

    public String code() { return code; }
    public String path() { return path; }
}
