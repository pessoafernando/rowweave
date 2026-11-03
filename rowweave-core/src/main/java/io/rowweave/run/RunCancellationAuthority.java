package io.rowweave.run;

/** Narrow authority that permits USER cancellation only; revision, attempt and fence stay hidden. */
public interface RunCancellationAuthority {
    RunCancellationResult requestUserCancellation(String runId, String requestId);
}
