package io.rowweave.stream.contract;

import java.util.Objects;

/** Durable split ownership fence for one job attempt and Worker incarnation. */
public final class StreamSourceAssignment {
    private final String jobId;
    private final String attemptId;
    private final String sourceOperatorUid;
    private final String splitId;
    private final long splitGeneration;
    private final int subtaskIndex;
    private final String workerIncarnation;
    private final long assignmentGeneration;

    public StreamSourceAssignment(String jobId, String attemptId,
            String sourceOperatorUid, String splitId, long splitGeneration,
            int subtaskIndex, String workerIncarnation,
            long assignmentGeneration) {
        this.jobId = text(jobId, "jobId");
        this.attemptId = text(attemptId, "attemptId");
        this.sourceOperatorUid = text(sourceOperatorUid, "sourceOperatorUid");
        this.splitId = StreamContractSupport.digest(
                splitId, "source.assignment.splitId");
        if (splitGeneration < 0L || subtaskIndex < 0
                || subtaskIndex >= 4_096 || assignmentGeneration <= 0L) {
            throw new IllegalArgumentException(
                    "source assignment generation or subtask is invalid");
        }
        this.splitGeneration = splitGeneration;
        this.subtaskIndex = subtaskIndex;
        this.workerIncarnation = text(
                workerIncarnation, "workerIncarnation");
        this.assignmentGeneration = assignmentGeneration;
    }

    public String jobId() { return jobId; }
    public String attemptId() { return attemptId; }
    public String sourceOperatorUid() { return sourceOperatorUid; }
    public String splitId() { return splitId; }
    public long splitGeneration() { return splitGeneration; }
    public int subtaskIndex() { return subtaskIndex; }
    public String workerIncarnation() { return workerIncarnation; }
    public long assignmentGeneration() { return assignmentGeneration; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamSourceAssignment)) return false;
        StreamSourceAssignment that = (StreamSourceAssignment) other;
        return splitGeneration == that.splitGeneration
                && subtaskIndex == that.subtaskIndex
                && assignmentGeneration == that.assignmentGeneration
                && jobId.equals(that.jobId)
                && attemptId.equals(that.attemptId)
                && sourceOperatorUid.equals(that.sourceOperatorUid)
                && splitId.equals(that.splitId)
                && workerIncarnation.equals(that.workerIncarnation);
    }

    @Override public int hashCode() {
        return Objects.hash(jobId, attemptId, sourceOperatorUid, splitId,
                splitGeneration, subtaskIndex, workerIncarnation,
                assignmentGeneration);
    }

    private static String text(String value, String field) {
        return StreamContractSupport.text(value,
                "source.assignment." + field, 256);
    }
}
