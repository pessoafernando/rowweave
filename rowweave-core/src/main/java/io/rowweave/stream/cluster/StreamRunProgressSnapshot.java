package io.rowweave.stream.cluster;

/** One run-attempt identity paired with its current task aggregate. */
final class StreamRunProgressSnapshot {
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private final long coordinatorTerm;
    private final long assignmentGeneration;
    private final StreamTaskProgressAggregate aggregate;
    private final java.util.List<io.rowweave.monitor.StageView> stages;

    StreamRunProgressSnapshot(String runId, String attemptId,
            long fenceToken, long coordinatorTerm,
            long assignmentGeneration,
            StreamTaskProgressAggregate aggregate) {
        this(runId, attemptId, fenceToken, coordinatorTerm, assignmentGeneration,
                aggregate, java.util.Collections.emptyList());
    }

    StreamRunProgressSnapshot(String runId, String attemptId,
            long fenceToken, long coordinatorTerm, long assignmentGeneration,
            StreamTaskProgressAggregate aggregate,
            java.util.List<io.rowweave.monitor.StageView> stages) {
        this.runId = StreamControlMessage.text(runId, "run ID");
        this.attemptId = StreamControlMessage.text(attemptId, "attempt ID");
        if (fenceToken <= 0L || coordinatorTerm <= 0L
                || assignmentGeneration <= 0L || aggregate == null) {
            throw new IllegalArgumentException(
                    "Stream run progress identity is invalid");
        }
        this.fenceToken = fenceToken;
        this.coordinatorTerm = coordinatorTerm;
        this.assignmentGeneration = assignmentGeneration;
        this.aggregate = aggregate;
        this.stages = java.util.Collections.unmodifiableList(new java.util.ArrayList<>(stages));
    }

    String runId() { return runId; }
    String attemptId() { return attemptId; }
    long fenceToken() { return fenceToken; }
    long coordinatorTerm() { return coordinatorTerm; }
    long assignmentGeneration() { return assignmentGeneration; }
    StreamTaskProgressAggregate aggregate() { return aggregate; }
    java.util.List<io.rowweave.monitor.StageView> stages() { return stages; }
}
