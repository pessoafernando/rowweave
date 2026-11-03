package io.rowweave.portable;

/** Stable fail-closed validation error for the portable public plan contract. */
public final class PortablePlanException extends IllegalArgumentException {
    private final String code;
    private final String path;

    public PortablePlanException(String code, String path, String message) {
        super(code + ": " + message);
        this.code = code;
        this.path = path;
    }

    public PortablePlanException(
            String code, String path, String message, Throwable cause) {
        super(code + ": " + message, cause);
        this.code = code;
        this.path = path;
    }

    public String code() { return code; }
    public String path() { return path; }
}
