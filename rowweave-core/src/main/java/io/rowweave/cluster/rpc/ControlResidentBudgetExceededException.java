package io.rowweave.cluster.rpc;

/** The control-plane resident budget is exhausted; the caller must reject the request before any side effect. */
public final class ControlResidentBudgetExceededException extends RuntimeException {
    public static final String CODE = "CONTROL_RESIDENT_BUDGET_EXCEEDED";

    ControlResidentBudgetExceededException(long requested, long used, long limit) {
        super(CODE + ": requested=" + requested + ", used=" + used + ", limit=" + limit);
    }

    public String code() {
        return CODE;
    }
}
