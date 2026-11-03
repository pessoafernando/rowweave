package io.rowweave.run;

/** Retry reasons for a whole attempt that only a shared authority can establish. */
public enum WholeAttemptRetryClass {
    PRE_DISPATCH_ABSENT("RETRY_PRE_DISPATCH_ABSENT"),
    WORKER_LOST("RETRY_WORKER_LOST");

    private final String reasonCode;

    WholeAttemptRetryClass(String reasonCode) {
        this.reasonCode = reasonCode;
    }

    public String reasonCode() {
        return reasonCode;
    }

    public static WholeAttemptRetryClass fromReasonCode(String reasonCode) {
        for (WholeAttemptRetryClass value : values()) {
            if (value.reasonCode.equals(reasonCode)) return value;
        }
        return null;
    }
}
