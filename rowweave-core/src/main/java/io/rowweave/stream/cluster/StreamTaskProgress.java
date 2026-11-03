package io.rowweave.stream.cluster;

import java.util.Objects;

/** One bounded, cumulative observation sample for one fenced task attempt. */
final class StreamTaskProgress {
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private final long coordinatorTerm;
    private final long assignmentGeneration;
    private final StreamTaskIdentity taskIdentity;
    private final long sampleSequence;
    private final boolean complete;
    private final long recordsRead;
    private final long recordsWritten;
    private final long bytesRead;
    private final long bytesWritten;
    private final long operatorWatermark;
    private final boolean watermarkIdle;
    private final long backlogEvents;
    private final long backlogBytes;
    private final int mailboxHighWaterEvents;
    private final long channelHighWaterBytes;
    private final long managedStateBytes;
    private final int timerHighWaterCount;
    private final int windowHighWaterCount;
    private final long backpressuredNanos;

    StreamTaskProgress(String runId, String attemptId, long fenceToken,
            long coordinatorTerm, long assignmentGeneration,
            StreamTaskIdentity taskIdentity, long sampleSequence,
            boolean complete, long recordsRead, long recordsWritten,
            long bytesRead, long bytesWritten, long operatorWatermark,
            boolean watermarkIdle, long backlogEvents, long backlogBytes,
            int mailboxHighWaterEvents, long channelHighWaterBytes,
            long managedStateBytes, int timerHighWaterCount,
            int windowHighWaterCount, long backpressuredNanos) {
        this.runId = StreamControlMessage.text(runId, "run ID");
        this.attemptId = StreamControlMessage.text(attemptId, "attempt ID");
        if (fenceToken <= 0L || coordinatorTerm <= 0L
                || assignmentGeneration <= 0L || taskIdentity == null
                || sampleSequence <= 0L || recordsRead < 0L
                || recordsWritten < 0L || bytesRead < 0L
                || bytesWritten < 0L || backlogEvents < 0L
                || backlogBytes < 0L || mailboxHighWaterEvents < 0
                || channelHighWaterBytes < 0L || managedStateBytes < 0L
                || timerHighWaterCount < 0 || windowHighWaterCount < 0
                || backpressuredNanos < 0L) {
            throw new IllegalArgumentException(
                    "Stream task progress is invalid");
        }
        this.fenceToken = fenceToken;
        this.coordinatorTerm = coordinatorTerm;
        this.assignmentGeneration = assignmentGeneration;
        this.taskIdentity = taskIdentity;
        this.sampleSequence = sampleSequence;
        this.complete = complete;
        this.recordsRead = recordsRead;
        this.recordsWritten = recordsWritten;
        this.bytesRead = bytesRead;
        this.bytesWritten = bytesWritten;
        this.operatorWatermark = operatorWatermark;
        this.watermarkIdle = watermarkIdle;
        this.backlogEvents = backlogEvents;
        this.backlogBytes = backlogBytes;
        this.mailboxHighWaterEvents = mailboxHighWaterEvents;
        this.channelHighWaterBytes = channelHighWaterBytes;
        this.managedStateBytes = managedStateBytes;
        this.timerHighWaterCount = timerHighWaterCount;
        this.windowHighWaterCount = windowHighWaterCount;
        this.backpressuredNanos = backpressuredNanos;
    }

    String runId() { return runId; }
    String attemptId() { return attemptId; }
    long fenceToken() { return fenceToken; }
    long coordinatorTerm() { return coordinatorTerm; }
    long assignmentGeneration() { return assignmentGeneration; }
    StreamTaskIdentity taskIdentity() { return taskIdentity; }
    long sampleSequence() { return sampleSequence; }
    boolean complete() { return complete; }
    long recordsRead() { return recordsRead; }
    long recordsWritten() { return recordsWritten; }
    long bytesRead() { return bytesRead; }
    long bytesWritten() { return bytesWritten; }
    long operatorWatermark() { return operatorWatermark; }
    boolean watermarkIdle() { return watermarkIdle; }
    long backlogEvents() { return backlogEvents; }
    long backlogBytes() { return backlogBytes; }
    int mailboxHighWaterEvents() { return mailboxHighWaterEvents; }
    long channelHighWaterBytes() { return channelHighWaterBytes; }
    long managedStateBytes() { return managedStateBytes; }
    int timerHighWaterCount() { return timerHighWaterCount; }
    int windowHighWaterCount() { return windowHighWaterCount; }
    long backpressuredNanos() { return backpressuredNanos; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamTaskProgress)) return false;
        StreamTaskProgress that = (StreamTaskProgress) other;
        return fenceToken == that.fenceToken
                && coordinatorTerm == that.coordinatorTerm
                && assignmentGeneration == that.assignmentGeneration
                && sampleSequence == that.sampleSequence
                && complete == that.complete
                && recordsRead == that.recordsRead
                && recordsWritten == that.recordsWritten
                && bytesRead == that.bytesRead
                && bytesWritten == that.bytesWritten
                && operatorWatermark == that.operatorWatermark
                && watermarkIdle == that.watermarkIdle
                && backlogEvents == that.backlogEvents
                && backlogBytes == that.backlogBytes
                && mailboxHighWaterEvents == that.mailboxHighWaterEvents
                && channelHighWaterBytes == that.channelHighWaterBytes
                && managedStateBytes == that.managedStateBytes
                && timerHighWaterCount == that.timerHighWaterCount
                && windowHighWaterCount == that.windowHighWaterCount
                && backpressuredNanos == that.backpressuredNanos
                && runId.equals(that.runId)
                && attemptId.equals(that.attemptId)
                && taskIdentity.equals(that.taskIdentity);
    }

    @Override public int hashCode() {
        return Objects.hash(runId, attemptId, fenceToken, coordinatorTerm,
                assignmentGeneration, taskIdentity, sampleSequence, complete,
                recordsRead, recordsWritten, bytesRead, bytesWritten,
                operatorWatermark, watermarkIdle, backlogEvents, backlogBytes,
                mailboxHighWaterEvents, channelHighWaterBytes,
                managedStateBytes, timerHighWaterCount, windowHighWaterCount,
                backpressuredNanos);
    }
}
