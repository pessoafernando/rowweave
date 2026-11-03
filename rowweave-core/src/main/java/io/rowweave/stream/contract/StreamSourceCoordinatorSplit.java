package io.rowweave.stream.contract;

import java.util.Arrays;
import java.util.Objects;

/** Immutable coordinator state for one split identity and ABA generation. */
public final class StreamSourceCoordinatorSplit
        implements Comparable<StreamSourceCoordinatorSplit> {
    public enum Status {
        PENDING(1), ASSIGNED(2), FINISHED(3), REMOVED(4);
        private final int wireId;
        Status(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static Status fromWire(int id) {
            for (Status value : values()) if (value.wireId == id) return value;
            throw new IllegalArgumentException("unknown source split status");
        }
    }

    private final String splitId;
    private final long splitGeneration;
    private final StreamSourceSplit split;
    private final Status status;
    private final StreamSourceAssignment assignment;
    private final long lastAssignmentGeneration;
    private final long logicalPosition;
    private final byte[] positionFrame;

    public StreamSourceCoordinatorSplit(String splitId,
            long splitGeneration, StreamSourceSplit split, Status status,
            StreamSourceAssignment assignment,
            long lastAssignmentGeneration, long logicalPosition,
            byte[] positionFrame) {
        this.splitId = StreamContractSupport.digest(
                splitId, "source.coordinator.splitId");
        if (splitGeneration < 0L || status == null
                || (split != null && !splitId.equals(split.splitId()))
                || (status != Status.REMOVED && split == null)
                || ((status == Status.ASSIGNED || status == Status.FINISHED)
                != (assignment != null))
                || (assignment != null
                && (assignment.splitGeneration() != splitGeneration
                || !splitId.equals(assignment.splitId())))
                || lastAssignmentGeneration < 0L
                || (assignment != null && lastAssignmentGeneration
                != assignment.assignmentGeneration())
                || (positionFrame == null) != (logicalPosition < 0L)
                || (positionFrame != null
                && (positionFrame.length < 1
                || positionFrame.length > 4 * 1024 * 1024))) {
            throw new IllegalArgumentException(
                    "source coordinator split state is invalid");
        }
        this.splitGeneration = splitGeneration;
        this.split = split;
        this.status = status;
        this.assignment = assignment;
        this.lastAssignmentGeneration = lastAssignmentGeneration;
        this.logicalPosition = logicalPosition;
        this.positionFrame = positionFrame == null ? null
                : positionFrame.clone();
    }

    public String splitId() { return splitId; }
    public long splitGeneration() { return splitGeneration; }
    public boolean hasSplitDescriptor() { return split != null; }
    public StreamSourceSplit split() {
        if (split == null) throw new IllegalStateException("split was removed");
        return split;
    }
    public Status status() { return status; }
    public StreamSourceAssignment assignment() {
        if (assignment == null) {
            throw new IllegalStateException("split is not assigned");
        }
        return assignment;
    }
    public boolean hasAssignment() { return assignment != null; }
    public long lastAssignmentGeneration() {
        return lastAssignmentGeneration;
    }
    public boolean hasPosition() { return positionFrame != null; }
    public long logicalPosition() {
        if (positionFrame == null) {
            throw new IllegalStateException("split has no position");
        }
        return logicalPosition;
    }
    public byte[] positionFrame() {
        if (positionFrame == null) {
            throw new IllegalStateException("split has no position");
        }
        return positionFrame.clone();
    }

    @Override public int compareTo(StreamSourceCoordinatorSplit other) {
        return splitId.compareTo(other.splitId);
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamSourceCoordinatorSplit)) return false;
        StreamSourceCoordinatorSplit that =
                (StreamSourceCoordinatorSplit) other;
        return splitGeneration == that.splitGeneration
                && lastAssignmentGeneration == that.lastAssignmentGeneration
                && logicalPosition == that.logicalPosition
                && splitId.equals(that.splitId)
                && Objects.equals(split, that.split)
                && status == that.status
                && Objects.equals(assignment, that.assignment)
                && Arrays.equals(positionFrame, that.positionFrame);
    }

    @Override public int hashCode() {
        return Objects.hash(splitId, splitGeneration, split, status,
                assignment, lastAssignmentGeneration, logicalPosition,
                Arrays.hashCode(positionFrame));
    }
}
