package io.rowweave.run;

import java.io.Serializable;
import java.util.Objects;

/**
 * One immutable execution-attempt authority carried from Master assignment through every Worker report.
 * The fence token is monotonic within a run and prevents a superseded attempt from mutating current state.
 */
public final class RunAttemptFence implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String runId;
    private final String attemptId;
    private final long fenceToken;

    public RunAttemptFence(String runId, String attemptId, long fenceToken) {
        this.runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        this.attemptId = RunText.required(attemptId, "attemptId", RunText.MAX_ID_BYTES);
        if (fenceToken <= 0L) {
            throw new IllegalArgumentException("fenceToken must be positive");
        }
        this.fenceToken = fenceToken;
    }

    public String runId() {
        return runId;
    }

    public String attemptId() {
        return attemptId;
    }

    public long fenceToken() {
        return fenceToken;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof RunAttemptFence)) return false;
        RunAttemptFence that = (RunAttemptFence) other;
        return fenceToken == that.fenceToken
                && runId.equals(that.runId)
                && attemptId.equals(that.attemptId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runId, attemptId, fenceToken);
    }

    @Override
    public String toString() {
        return "RunAttemptFence{" + runId + '/' + attemptId + ", token=" + fenceToken + '}';
    }
}
