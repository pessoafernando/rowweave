package io.rowweave.stream.contract;

/** Stable Stream contract failure; code/path are machine-readable. */
public final class StreamContractException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    private final String code;
    private final String path;

    StreamContractException(String code, String path, String message) {
        super(message);
        this.code = code;
        this.path = path;
    }

    public String code() { return code; }
    public String path() { return path; }
}
