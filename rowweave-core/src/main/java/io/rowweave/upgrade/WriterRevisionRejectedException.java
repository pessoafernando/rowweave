package io.rowweave.upgrade;

/** Fail-closed writer revision admission error. */
public final class WriterRevisionRejectedException extends IllegalStateException {
    private final String code;

    WriterRevisionRejectedException(String code, String message) {
        super(code + ": " + message);
        this.code = code;
    }

    public String code() { return code; }
}
