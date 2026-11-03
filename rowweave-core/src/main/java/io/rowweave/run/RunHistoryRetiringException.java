package io.rowweave.run;

/** Terminal history has entered fenced deletion and can no longer pass as a complete event log. */
public final class RunHistoryRetiringException extends IllegalStateException {
    public RunHistoryRetiringException(String runId) {
        super("run history is retiring: "
                + RunText.required(runId, "runId", RunText.MAX_ID_BYTES));
    }
}
