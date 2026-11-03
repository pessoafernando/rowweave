package io.rowweave.run;

/** Internal narrow authority that evaluates a frozen Run deadline using backend time. */
public interface RunDeadlineAuthority {
    RunDeadlineResult evaluateDeadline(String runId);
}
