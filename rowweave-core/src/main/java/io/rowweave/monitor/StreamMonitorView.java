package io.rowweave.monitor;

import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;

import java.util.Objects;

/**
 * Immutable monitor projection of one Stream operational evidence snapshot.
 * Attempt and lifetime progress are kept separately so restart metrics remain
 * meaningful while completed checkpoints and record totals stay cumulative.
 */
public final class StreamMonitorView {

    private final StreamJobEvidence evidence;
    private final StreamJobProgress attemptProgress;
    private final StreamJobProgress lifetimeProgress;

    public StreamMonitorView(StreamJobEvidence evidence) {
        this.evidence = Objects.requireNonNull(evidence, "evidence");
        this.attemptProgress = evidence.attemptProgress();
        this.lifetimeProgress = evidence.lifetimeProgress();
    }

    public StreamJobProgress attemptProgress() { return attemptProgress; }
    public StreamJobProgress lifetimeProgress() { return lifetimeProgress; }
    public boolean available() { return evidence.available(); }
    public boolean complete() { return evidence.complete(); }
    public String runId() { return evidence.runId(); }
    public String attemptId() { return evidence.attemptId(); }
    public int attemptOrdinal() { return evidence.attemptOrdinal(); }
    public String latestCompletedCheckpointId() {
        return evidence.latestCompletedCheckpointId();
    }
    public long latestCheckpointDurationMillis() {
        return evidence.latestCheckpointDurationMillis();
    }
    public long latestCheckpointBytes() {
        return evidence.latestCheckpointBytes();
    }
    public int placementTaskCount() { return evidence.placementTaskCount(); }
    public int reportedTaskCount() { return evidence.reportedTaskCount(); }
    public long observedAtEpochMillis() { return evidence.observedAtEpochMillis(); }

    @Override public boolean equals(Object other) {
        return other instanceof StreamMonitorView
                && evidence.equals(((StreamMonitorView) other).evidence);
    }

    @Override public int hashCode() { return evidence.hashCode(); }
}
