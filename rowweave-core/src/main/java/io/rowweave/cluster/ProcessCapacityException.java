package io.rowweave.cluster;

/** Stable, non-secret startup failure for the process-capacity gate. */
public final class ProcessCapacityException extends IllegalArgumentException {

    public enum Code {
        JVM_OPTION("ROWWEAVE-CAP-001"),
        CONFIGURATION("ROWWEAVE-CAP-002"),
        HEAP_CAPACITY("ROWWEAVE-CAP-003"),
        RSS_CAPACITY("ROWWEAVE-CAP-004"),
        STAGE_THREADS("ROWWEAVE-CAP-005"),
        CERTIFICATION("ROWWEAVE-CAP-006");

        private final String stableCode;

        Code(String stableCode) {
            this.stableCode = stableCode;
        }

        public String stableCode() {
            return stableCode;
        }
    }

    private final Code code;

    ProcessCapacityException(Code code, String message) {
        super(prefix(code, message));
        this.code = requireCode(code);
    }

    ProcessCapacityException(Code code, String message, Throwable cause) {
        super(prefix(code, message), cause);
        this.code = requireCode(code);
    }

    public Code code() {
        return code;
    }

    public String stableCode() {
        return code.stableCode();
    }

    private static String prefix(Code code, String message) {
        Code checked = requireCode(code);
        String detail = message == null || message.trim().isEmpty()
                ? "process capacity validation failed" : message;
        return checked.stableCode() + " " + detail;
    }

    private static Code requireCode(Code code) {
        if (code == null) {
            throw new IllegalArgumentException("process capacity failure code is required");
        }
        return code;
    }
}
