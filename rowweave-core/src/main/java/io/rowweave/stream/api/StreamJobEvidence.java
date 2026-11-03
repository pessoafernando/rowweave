package io.rowweave.stream.api;

import java.util.Objects;

/** Durable, data-free operational evidence for one logical Stream job. */
public final class StreamJobEvidence {
    private final StreamJobId jobId;
    private final String runId;
    private final String attemptId;
    private final int attemptOrdinal;
    private final long fenceToken;
    private final long coordinatorTerm;
    private final long assignmentGeneration;
    private final String latestCompletedCheckpointId;
    private final long latestCompletedCheckpointSequence;
    private final long latestCheckpointDurationMillis;
    private final long latestCheckpointBytes;
    private final StreamJobProgress attemptProgress;
    private final StreamJobProgress lifetimeProgress;
    private final int placementTaskCount;
    private final int reportedTaskCount;
    private final long observedAtEpochMillis;

    public StreamJobEvidence(StreamJobId jobId, String runId,
            String attemptId, int attemptOrdinal, long fenceToken,
            long coordinatorTerm, long assignmentGeneration,
            String latestCompletedCheckpointId,
            long latestCompletedCheckpointSequence,
            long latestCheckpointDurationMillis,
            long latestCheckpointBytes, StreamJobProgress attemptProgress,
            StreamJobProgress lifetimeProgress, int placementTaskCount,
            int reportedTaskCount, long observedAtEpochMillis) {
        this(jobId, runId, attemptId, attemptOrdinal, fenceToken,
                coordinatorTerm, assignmentGeneration,
                latestCompletedCheckpointId,
                latestCompletedCheckpointSequence,
                latestCheckpointDurationMillis, latestCheckpointBytes,
                attemptProgress, lifetimeProgress, placementTaskCount,
                reportedTaskCount, observedAtEpochMillis, false);
    }

    private StreamJobEvidence(StreamJobId jobId, String runId,
            String attemptId, int attemptOrdinal, long fenceToken,
            long coordinatorTerm, long assignmentGeneration,
            String latestCompletedCheckpointId,
            long latestCompletedCheckpointSequence,
            long latestCheckpointDurationMillis,
            long latestCheckpointBytes, StreamJobProgress attemptProgress,
            StreamJobProgress lifetimeProgress, int placementTaskCount,
            int reportedTaskCount, long observedAtEpochMillis,
            boolean unavailable) {
        if (jobId == null || attemptProgress == null
                || lifetimeProgress == null || attemptOrdinal < 0
                || fenceToken < 0L || coordinatorTerm < 0L
                || assignmentGeneration < 0L
                || latestCompletedCheckpointSequence < 0L
                || latestCheckpointDurationMillis < 0L
                || latestCheckpointBytes < 0L || placementTaskCount < 0
                || reportedTaskCount < 0
                || reportedTaskCount > placementTaskCount
                || observedAtEpochMillis < 0L) {
            throw new IllegalArgumentException(
                    "Stream operational evidence is invalid");
        }
        if (unavailable) {
            if (runId != null || attemptId != null || attemptOrdinal != 0
                    || fenceToken != 0L || coordinatorTerm != 0L
                    || assignmentGeneration != 0L
                    || latestCompletedCheckpointId != null
                    || latestCompletedCheckpointSequence != 0L
                    || latestCheckpointDurationMillis != 0L
                    || latestCheckpointBytes != 0L
                    || placementTaskCount != 0 || reportedTaskCount != 0) {
                throw new IllegalArgumentException(
                        "unavailable evidence carries runtime identity");
            }
        } else {
            requireText(runId, "run ID");
            requireText(attemptId, "attempt ID");
            if (fenceToken == 0L || coordinatorTerm == 0L
                    || assignmentGeneration == 0L
                    || placementTaskCount == 0
                    || attemptProgress.attempt() != attemptOrdinal
                    || lifetimeProgress.attempt() != attemptOrdinal) {
                throw new IllegalArgumentException(
                        "available evidence lacks fenced runtime identity");
            }
            boolean hasCheckpoint = latestCompletedCheckpointId != null;
            if (hasCheckpoint != (latestCompletedCheckpointSequence > 0L)
                    || !hasCheckpoint && (latestCheckpointDurationMillis != 0L
                    || latestCheckpointBytes != 0L)
                    || hasCheckpoint && !latestCompletedCheckpointId.matches(
                    "stream-checkpoint:[0-9a-f]{64}")) {
                throw new IllegalArgumentException(
                        "latest completed checkpoint evidence is invalid");
            }
        }
        this.jobId = jobId;
        this.runId = runId;
        this.attemptId = attemptId;
        this.attemptOrdinal = attemptOrdinal;
        this.fenceToken = fenceToken;
        this.coordinatorTerm = coordinatorTerm;
        this.assignmentGeneration = assignmentGeneration;
        this.latestCompletedCheckpointId = latestCompletedCheckpointId;
        this.latestCompletedCheckpointSequence =
                latestCompletedCheckpointSequence;
        this.latestCheckpointDurationMillis = latestCheckpointDurationMillis;
        this.latestCheckpointBytes = latestCheckpointBytes;
        this.attemptProgress = attemptProgress;
        this.lifetimeProgress = lifetimeProgress;
        this.placementTaskCount = placementTaskCount;
        this.reportedTaskCount = reportedTaskCount;
        this.observedAtEpochMillis = observedAtEpochMillis;
    }

    public static StreamJobEvidence unavailable(StreamJobId jobId,
            StreamJobProgress progress, long observedAtEpochMillis) {
        return new StreamJobEvidence(jobId, null, null, 0, 0L, 0L, 0L,
                null, 0L, 0L, 0L, progress, progress, 0, 0,
                observedAtEpochMillis, true);
    }

    public StreamJobId jobId() { return jobId; }
    public boolean available() { return runId != null; }
    public boolean complete() {
        return available() && reportedTaskCount == placementTaskCount;
    }
    public String runId() { return runId; }
    public String attemptId() { return attemptId; }
    public int attemptOrdinal() { return attemptOrdinal; }
    public long fenceToken() { return fenceToken; }
    public long coordinatorTerm() { return coordinatorTerm; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public String latestCompletedCheckpointId() {
        return latestCompletedCheckpointId;
    }
    public long latestCompletedCheckpointSequence() {
        return latestCompletedCheckpointSequence;
    }
    public long latestCheckpointDurationMillis() {
        return latestCheckpointDurationMillis;
    }
    public long latestCheckpointBytes() { return latestCheckpointBytes; }
    public StreamJobProgress attemptProgress() { return attemptProgress; }
    public StreamJobProgress lifetimeProgress() { return lifetimeProgress; }
    public int placementTaskCount() { return placementTaskCount; }
    public int reportedTaskCount() { return reportedTaskCount; }
    public long observedAtEpochMillis() { return observedAtEpochMillis; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamJobEvidence)) return false;
        StreamJobEvidence that = (StreamJobEvidence) other;
        return attemptOrdinal == that.attemptOrdinal
                && fenceToken == that.fenceToken
                && coordinatorTerm == that.coordinatorTerm
                && assignmentGeneration == that.assignmentGeneration
                && latestCompletedCheckpointSequence
                == that.latestCompletedCheckpointSequence
                && latestCheckpointDurationMillis
                == that.latestCheckpointDurationMillis
                && latestCheckpointBytes == that.latestCheckpointBytes
                && placementTaskCount == that.placementTaskCount
                && reportedTaskCount == that.reportedTaskCount
                && observedAtEpochMillis == that.observedAtEpochMillis
                && jobId.equals(that.jobId)
                && Objects.equals(runId, that.runId)
                && Objects.equals(attemptId, that.attemptId)
                && Objects.equals(latestCompletedCheckpointId,
                that.latestCompletedCheckpointId)
                && attemptProgress.equals(that.attemptProgress)
                && lifetimeProgress.equals(that.lifetimeProgress);
    }

    @Override public int hashCode() {
        return Objects.hash(jobId, runId, attemptId, attemptOrdinal,
                fenceToken, coordinatorTerm, assignmentGeneration,
                latestCompletedCheckpointId,
                latestCompletedCheckpointSequence,
                latestCheckpointDurationMillis, latestCheckpointBytes,
                attemptProgress, lifetimeProgress, placementTaskCount,
                reportedTaskCount, observedAtEpochMillis);
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isEmpty() || value.length() > 256) {
            throw new IllegalArgumentException(name + " is invalid");
        }
    }
}
