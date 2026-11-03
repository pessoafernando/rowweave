package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;

import java.util.List;
import java.util.Objects;

/** Immutable durable authority record for one active Stream run attempt. */
public final class StreamClusterRunState {
    private final long coordinatorTerm;
    private final String coordinatorIncarnation;
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private final long assignmentGeneration;
    private final long channelEpoch;
    private final StreamWorkerPlacement placement;
    private final CheckpointIdentity latestCompletedCheckpoint;

    public static StreamClusterRunState initial(
            long coordinatorTerm, String coordinatorIncarnation,
            String runId, String attemptId, long fenceToken,
            long assignmentGeneration, long channelEpoch,
            StreamWorkerPlacement placement) {
        return new StreamClusterRunState(
                coordinatorTerm, coordinatorIncarnation, runId, attemptId,
                fenceToken, assignmentGeneration, channelEpoch,
                placement, null);
    }

    StreamClusterRunState(
            long coordinatorTerm, String coordinatorIncarnation,
            String runId, String attemptId, long fenceToken,
            long assignmentGeneration, long channelEpoch,
            StreamWorkerPlacement placement,
            CheckpointIdentity latestCompletedCheckpoint) {
        if (coordinatorTerm <= 0L || fenceToken <= 0L
                || assignmentGeneration <= 0L || channelEpoch <= 0L
                || placement == null) {
            throw new IllegalArgumentException(
                    "Stream run state generations and placement are required");
        }
        this.coordinatorTerm = coordinatorTerm;
        this.coordinatorIncarnation = StreamControlMessage.text(
                coordinatorIncarnation, "coordinator incarnation");
        this.runId = StreamControlMessage.text(runId, "run ID");
        this.attemptId = StreamControlMessage.text(attemptId, "attempt ID");
        validatePlacement(placement);
        if (latestCompletedCheckpoint != null
                && !runId.equals(latestCompletedCheckpoint.runId())) {
            throw new IllegalArgumentException(
                    "latest checkpoint belongs to another Stream run");
        }
        this.fenceToken = fenceToken;
        this.assignmentGeneration = assignmentGeneration;
        this.channelEpoch = channelEpoch;
        this.placement = placement;
        this.latestCompletedCheckpoint = latestCompletedCheckpoint;
    }

    public long coordinatorTerm() { return coordinatorTerm; }
    public String coordinatorIncarnation() { return coordinatorIncarnation; }
    public String runId() { return runId; }
    public String attemptId() { return attemptId; }
    public long fenceToken() { return fenceToken; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public long channelEpoch() { return channelEpoch; }
    public StreamWorkerPlacement placement() { return placement; }
    public CheckpointIdentity latestCompletedCheckpoint() {
        return latestCompletedCheckpoint;
    }

    StreamClusterRunState withCoordinator(long term, String incarnation) {
        return new StreamClusterRunState(
                term, incarnation, runId, attemptId, fenceToken,
                assignmentGeneration, channelEpoch, placement,
                latestCompletedCheckpoint);
    }

    StreamClusterRunState withCheckpoint(CheckpointIdentity checkpoint) {
        return new StreamClusterRunState(
                coordinatorTerm, coordinatorIncarnation, runId, attemptId,
                fenceToken, assignmentGeneration, channelEpoch, placement,
                checkpoint);
    }

    StreamClusterRunState restart(
            String newAttemptId, long newFenceToken,
            long newAssignmentGeneration, long newChannelEpoch,
            StreamWorkerPlacement newPlacement) {
        return new StreamClusterRunState(
                coordinatorTerm, coordinatorIncarnation, runId, newAttemptId,
                newFenceToken, newAssignmentGeneration, newChannelEpoch,
                newPlacement, latestCompletedCheckpoint);
    }

    boolean containsWorker(String workerId, String workerIncarnation) {
        for (StreamWorkerPlacement.Assignment assignment
                : placement.assignments()) {
            if (assignment.workerId().equals(workerId)
                    && assignment.workerIncarnation().equals(
                            workerIncarnation)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamClusterRunState)) return false;
        StreamClusterRunState that = (StreamClusterRunState) other;
        return coordinatorTerm == that.coordinatorTerm
                && fenceToken == that.fenceToken
                && assignmentGeneration == that.assignmentGeneration
                && channelEpoch == that.channelEpoch
                && coordinatorIncarnation.equals(that.coordinatorIncarnation)
                && runId.equals(that.runId)
                && attemptId.equals(that.attemptId)
                && Objects.equals(latestCompletedCheckpoint,
                        that.latestCompletedCheckpoint)
                && samePlacement(placement, that.placement);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                coordinatorTerm, coordinatorIncarnation, runId, attemptId,
                fenceToken, assignmentGeneration, channelEpoch,
                latestCompletedCheckpoint);
        for (StreamWorkerPlacement.Assignment assignment
                : placement.assignments()) {
            result = 31 * result + Objects.hash(
                    assignment.taskOrdinal(), assignment.workerId(),
                    assignment.workerIncarnation(), assignment.operatorUid(),
                    assignment.subtaskIndex());
        }
        return result;
    }

    private static void validatePlacement(StreamWorkerPlacement placement) {
        List<StreamWorkerPlacement.Assignment> assignments =
                placement.assignments();
        if (assignments.isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream run placement must not be empty");
        }
        for (int index = 0; index < assignments.size(); index++) {
            StreamWorkerPlacement.Assignment assignment = assignments.get(index);
            if (assignment == null || assignment.taskOrdinal() != index) {
                throw new IllegalArgumentException(
                        "Stream run placement task ordinals are not canonical");
            }
            StreamControlMessage.text(assignment.workerId(), "worker ID");
            StreamControlMessage.text(
                    assignment.workerIncarnation(), "worker incarnation");
            for (int previous = 0; previous < index; previous++) {
                if (assignments.get(previous).identity().equals(
                        assignment.identity())) {
                    throw new IllegalArgumentException(
                            "Stream placement task identities are duplicated");
                }
            }
        }
    }

    private static boolean samePlacement(
            StreamWorkerPlacement left, StreamWorkerPlacement right) {
        List<StreamWorkerPlacement.Assignment> a = left.assignments();
        List<StreamWorkerPlacement.Assignment> b = right.assignments();
        if (a.size() != b.size()) return false;
        for (int index = 0; index < a.size(); index++) {
            StreamWorkerPlacement.Assignment x = a.get(index);
            StreamWorkerPlacement.Assignment y = b.get(index);
            if (x.taskOrdinal() != y.taskOrdinal()
                    || !x.workerId().equals(y.workerId())
                    || !x.workerIncarnation().equals(
                            y.workerIncarnation())
                    || !x.identity().equals(y.identity())) {
                return false;
            }
        }
        return true;
    }
}
