package io.rowweave.release;

/** Stable bounded validation failure for production release contracts. */
public final class ReleaseContractException extends IllegalArgumentException {
    private final String code;
    private final String path;

    public ReleaseContractException(String code, String path, String message) {
        super(message);
        this.code = code;
        this.path = path;
    }

    public ReleaseContractException(
            String code, String path, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.path = path;
    }

    public String code() { return code; }
    public String path() { return path; }
}
