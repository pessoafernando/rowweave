package io.rowweave.stream.cluster;

/** Exact assigned Worker incarnation that became unreachable during a run. */
final class StreamWorkerLossException extends IllegalStateException {
    private final String workerId;
    private final String workerIncarnation;

    StreamWorkerLossException(
            StreamWorkerPlacement.Assignment assignment, Throwable cause) {
        super("assigned Stream Worker became unavailable", cause);
        if (assignment == null) {
            throw new IllegalArgumentException(
                    "lost Stream Worker assignment is required");
        }
        this.workerId = assignment.workerId();
        this.workerIncarnation = assignment.workerIncarnation();
    }

    String workerId() { return workerId; }
    String workerIncarnation() { return workerIncarnation; }
}
