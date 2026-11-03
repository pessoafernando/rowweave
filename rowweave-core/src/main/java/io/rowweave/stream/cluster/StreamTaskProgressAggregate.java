package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamJobProgress;

/** Immutable job-attempt projection of the latest accepted task samples. */
final class StreamTaskProgressAggregate {
    private final StreamJobProgress progress;
    private final int placementTaskCount;
    private final int reportedTaskCount;

    StreamTaskProgressAggregate(StreamJobProgress progress,
            int placementTaskCount, int reportedTaskCount) {
        if (progress == null || placementTaskCount <= 0
                || reportedTaskCount < 0
                || reportedTaskCount > placementTaskCount) {
            throw new IllegalArgumentException(
                    "Stream task progress aggregate is invalid");
        }
        this.progress = progress;
        this.placementTaskCount = placementTaskCount;
        this.reportedTaskCount = reportedTaskCount;
    }

    StreamJobProgress progress() { return progress; }
    int placementTaskCount() { return placementTaskCount; }
    int reportedTaskCount() { return reportedTaskCount; }
    boolean complete() { return reportedTaskCount == placementTaskCount; }
}
