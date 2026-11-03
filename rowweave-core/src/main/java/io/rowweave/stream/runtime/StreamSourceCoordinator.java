package io.rowweave.stream.runtime;

import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.StreamSourceAssignment;
import io.rowweave.stream.contract.StreamSourceCoordinatorSplit;
import io.rowweave.stream.contract.StreamSourceCoordinatorState;
import io.rowweave.stream.contract.StreamSourceDiscoveryDescriptor;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Single-owner transition engine for fixed and dynamic Source coordination. */
public final class StreamSourceCoordinator {
    private final String jobId;
    private final String attemptId;
    private final String sourceOperatorUid;
    private final StreamSourceDiscoveryDescriptor limits;
    private final TreeMap<String, StreamSourceCoordinatorSplit> splits;
    private long discoveryEpoch;
    private StreamDiscoveryCursor cursor;

    private StreamSourceCoordinator(String jobId, String attemptId,
            String sourceOperatorUid,
            StreamSourceDiscoveryDescriptor limits,
            long discoveryEpoch, StreamDiscoveryCursor cursor,
            List<StreamSourceCoordinatorSplit> splits) {
        if (limits == null || cursor == null) {
            throw new IllegalArgumentException(
                    "source coordinator limits and cursor are required");
        }
        this.jobId = jobId;
        this.attemptId = attemptId;
        this.sourceOperatorUid = sourceOperatorUid;
        this.limits = limits;
        this.discoveryEpoch = discoveryEpoch;
        this.cursor = cursor;
        this.splits = new TreeMap<>();
        for (StreamSourceCoordinatorSplit split : splits) {
            if (this.splits.put(split.splitId(), split) != null) {
                throw new IllegalArgumentException(
                        "source coordinator split is duplicated");
            }
        }
        validateState();
    }

    public static StreamSourceCoordinator initial(String jobId,
            String attemptId, String sourceOperatorUid,
            StreamSourceDiscoveryDescriptor limits) {
        StreamSourceCoordinatorState checked =
                new StreamSourceCoordinatorState(jobId, attemptId,
                        sourceOperatorUid, -1L,
                        StreamDiscoveryCursor.initial(),
                        java.util.Collections
                                .<StreamSourceCoordinatorSplit>emptyList());
        return new StreamSourceCoordinator(checked.jobId(),
                checked.attemptId(), checked.sourceOperatorUid(), limits,
                checked.discoveryEpoch(), checked.cursor(), checked.splits());
    }

    public static StreamSourceCoordinator restore(
            StreamSourceCoordinatorState state,
            StreamSourceDiscoveryDescriptor limits) {
        if (state == null) {
            throw new IllegalArgumentException(
                    "source coordinator restore state is required");
        }
        return new StreamSourceCoordinator(state.jobId(), state.attemptId(),
                state.sourceOperatorUid(), limits, state.discoveryEpoch(),
                state.cursor(), state.splits());
    }

    /** Rebinds a durable cut to a new attempt and revokes old task owners. */
    public static StreamSourceCoordinator recoverAttempt(
            StreamSourceCoordinatorState state, String jobId,
            String attemptId, StreamSourceDiscoveryDescriptor limits) {
        if (state == null) {
            throw new IllegalArgumentException(
                    "source coordinator recovery state is required");
        }
        List<StreamSourceCoordinatorSplit> recovered = new ArrayList<>();
        for (StreamSourceCoordinatorSplit split : state.splits()) {
            StreamSourceCoordinatorSplit.Status status = split.status();
            StreamSourceAssignment assignment = null;
            long lastAssignmentGeneration = 0L;
            if (status == StreamSourceCoordinatorSplit.Status.ASSIGNED) {
                status = StreamSourceCoordinatorSplit.Status.PENDING;
            } else if (status
                    == StreamSourceCoordinatorSplit.Status.FINISHED) {
                StreamSourceAssignment old = split.assignment();
                assignment = new StreamSourceAssignment(jobId, attemptId,
                        state.sourceOperatorUid(), split.splitId(),
                        split.splitGeneration(), old.subtaskIndex(),
                        old.workerIncarnation(),
                        old.assignmentGeneration());
                lastAssignmentGeneration = old.assignmentGeneration();
            }
            recovered.add(new StreamSourceCoordinatorSplit(split.splitId(),
                    split.splitGeneration(), split.hasSplitDescriptor()
                    ? split.split() : null, status, assignment,
                    lastAssignmentGeneration,
                    split.hasPosition() ? split.logicalPosition() : -1L,
                    split.hasPosition() ? split.positionFrame() : null));
        }
        return new StreamSourceCoordinator(jobId, attemptId,
                state.sourceOperatorUid(), limits, state.discoveryEpoch(),
                state.cursor(), recovered);
    }

    public StreamSourceCoordinatorState state() {
        return new StreamSourceCoordinatorState(jobId, attemptId,
                sourceOperatorUid, discoveryEpoch, cursor,
                new ArrayList<>(splits.values()));
    }

    public void applyDiscovery(StreamSplitDiscovery discovery) {
        if (discovery == null
                || discovery.changes().size()
                > limits.maximumChangesPerDiscovery()
                || discovery.nextCursor().frame().length
                > limits.maximumCursorBytes()) {
            throw new IllegalArgumentException(
                    "split discovery exceeds coordinator limits");
        }
        if (discovery.discoveryEpoch() < discoveryEpoch) return;
        if (discovery.discoveryEpoch() == discoveryEpoch
                && !cursor.equals(discovery.nextCursor())) {
            throw new IllegalStateException(
                    "discovery epoch was reused with a different cursor");
        }
        Set<String> responseIdentities = new HashSet<>();
        TreeMap<String, StreamSourceCoordinatorSplit> candidate =
                new TreeMap<>(splits);
        for (StreamSplitChange change : discovery.changes()) {
            String identity = change.splitId() + ':' + change.generation();
            if (!responseIdentities.add(identity)) {
                throw new IllegalArgumentException(
                        "discovery contains conflicting split changes");
            }
            apply(candidate, change);
        }
        validate(candidate, discovery.nextCursor());
        splits.clear();
        splits.putAll(candidate);
        discoveryEpoch = discovery.discoveryEpoch();
        cursor = discovery.nextCursor();
    }

    public StreamSourceAssignment assign(String splitId,
            long splitGeneration, int subtaskIndex,
            String workerIncarnation, long assignmentGeneration) {
        StreamSourceCoordinatorSplit current = requireGeneration(
                splitId, splitGeneration);
        StreamSourceAssignment assignment = new StreamSourceAssignment(
                jobId, attemptId, sourceOperatorUid, splitId,
                splitGeneration, subtaskIndex, workerIncarnation,
                assignmentGeneration);
        if (current.status() == StreamSourceCoordinatorSplit.Status.ASSIGNED) {
            if (current.assignment().equals(assignment)) {
                return current.assignment();
            }
            throw new IllegalStateException(
                    "split is already owned by another assignment");
        }
        if (current.status() != StreamSourceCoordinatorSplit.Status.PENDING
                || assignmentGeneration
                <= current.lastAssignmentGeneration()) {
            throw new IllegalStateException(
                    "split is not pending for this assignment generation");
        }
        StreamSourceCoordinatorSplit next = copy(current,
                StreamSourceCoordinatorSplit.Status.ASSIGNED, assignment,
                assignmentGeneration, current.hasPosition()
                        ? current.logicalPosition() : -1L,
                current.hasPosition() ? current.positionFrame() : null);
        splits.put(splitId, next);
        return assignment;
    }

    public void recordPosition(String splitId, long splitGeneration,
            long logicalPosition, byte[] positionFrame) {
        StreamSourceCoordinatorSplit current = requireGeneration(
                splitId, splitGeneration);
        if ((current.status()
                != StreamSourceCoordinatorSplit.Status.ASSIGNED
                && current.status()
                != StreamSourceCoordinatorSplit.Status.FINISHED)
                || logicalPosition < 0L || positionFrame == null
                || positionFrame.length < 1
                || positionFrame.length > 4 * 1024 * 1024) {
            throw new IllegalStateException(
                    "position requires an active split and bounded frame");
        }
        splits.put(splitId, copy(current, current.status(),
                current.assignment(), current.lastAssignmentGeneration(),
                logicalPosition, positionFrame));
    }

    public void finish(String splitId, long splitGeneration,
            long assignmentGeneration) {
        StreamSourceCoordinatorSplit current = requireGeneration(
                splitId, splitGeneration);
        if (current.status()
                == StreamSourceCoordinatorSplit.Status.FINISHED
                && current.assignment().assignmentGeneration()
                == assignmentGeneration) {
            return;
        }
        if (current.status() != StreamSourceCoordinatorSplit.Status.ASSIGNED
                || current.assignment().assignmentGeneration()
                != assignmentGeneration) {
            throw new IllegalStateException(
                    "finish does not match the active split assignment");
        }
        splits.put(splitId, copy(current,
                StreamSourceCoordinatorSplit.Status.FINISHED,
                current.assignment(), current.lastAssignmentGeneration(),
                current.hasPosition() ? current.logicalPosition() : -1L,
                current.hasPosition() ? current.positionFrame() : null));
    }

    public void revokeWorker(String workerIncarnation) {
        if (workerIncarnation == null || workerIncarnation.isEmpty()) {
            throw new IllegalArgumentException(
                    "worker incarnation is required");
        }
        for (Map.Entry<String, StreamSourceCoordinatorSplit> entry
                : new ArrayList<>(splits.entrySet())) {
            StreamSourceCoordinatorSplit current = entry.getValue();
            if (current.status()
                    == StreamSourceCoordinatorSplit.Status.ASSIGNED
                    && current.assignment().workerIncarnation().equals(
                    workerIncarnation)) {
                splits.put(entry.getKey(), copy(current,
                        StreamSourceCoordinatorSplit.Status.PENDING, null,
                        current.lastAssignmentGeneration(),
                        current.hasPosition() ? current.logicalPosition() : -1L,
                        current.hasPosition()
                                ? current.positionFrame() : null));
            }
        }
    }

    private void apply(TreeMap<String, StreamSourceCoordinatorSplit> target,
            StreamSplitChange change) {
        StreamSourceCoordinatorSplit current = target.get(change.splitId());
        if (current != null
                && change.generation() < current.splitGeneration()) return;
        if (change.kind() == StreamSplitChange.Kind.ADD) {
            StreamSourceSplit added = contract(change.split());
            if (added.descriptorFrame().length
                    > limits.maximumDescriptorBytes()) {
                throw new IllegalArgumentException(
                        "split descriptor exceeds discovery limit");
            }
            if (current == null) {
                target.put(change.splitId(), pending(
                        added, change.generation()));
                return;
            }
            if (change.generation() == current.splitGeneration()) {
                if (current.status()
                        == StreamSourceCoordinatorSplit.Status.REMOVED
                        || !current.hasSplitDescriptor()
                        || !current.split().equals(added)) {
                    throw new IllegalStateException(
                            "split generation was reused after removal or drift");
                }
                return;
            }
            if (current.status()
                    != StreamSourceCoordinatorSplit.Status.REMOVED) {
                throw new IllegalStateException(
                        "new split generation overlaps an active generation");
            }
            target.put(change.splitId(), pending(added,
                    change.generation()));
            return;
        }
        if (current == null || change.generation()
                > current.splitGeneration()) {
            target.put(change.splitId(), removed(change.splitId(),
                    change.generation(), null, 0L));
        } else if (current.status()
                != StreamSourceCoordinatorSplit.Status.REMOVED) {
            target.put(change.splitId(), removed(change.splitId(),
                    current.splitGeneration(), current.hasSplitDescriptor()
                            ? current.split() : null,
                    current.lastAssignmentGeneration()));
        }
    }

    private void validateState() {
        validate(splits, cursor);
        for (StreamSourceCoordinatorSplit split : splits.values()) {
            if (split.hasAssignment()) {
                StreamSourceAssignment assignment = split.assignment();
                if (!jobId.equals(assignment.jobId())
                        || !attemptId.equals(assignment.attemptId())
                        || !sourceOperatorUid.equals(
                        assignment.sourceOperatorUid())) {
                    throw new IllegalArgumentException(
                            "restored split assignment belongs to another attempt");
                }
            }
        }
    }

    private void validate(Map<String, StreamSourceCoordinatorSplit> values,
            StreamDiscoveryCursor candidateCursor) {
        if (values.size() > limits.maximumKnownSplits()
                || candidateCursor.frame().length
                > limits.maximumCursorBytes()) {
            throw new IllegalArgumentException(
                    "source coordinator state exceeds discovery limits");
        }
        int pending = 0;
        for (StreamSourceCoordinatorSplit split : values.values()) {
            if (split.status()
                    == StreamSourceCoordinatorSplit.Status.PENDING) pending++;
            if (split.hasSplitDescriptor()
                    && split.split().descriptorFrame().length
                    > limits.maximumDescriptorBytes()) {
                throw new IllegalArgumentException(
                        "restored split descriptor exceeds discovery limit");
            }
        }
        if (pending > limits.maximumPendingSplits()) {
            throw new IllegalArgumentException(
                    "source pending queue exceeds discovery limit");
        }
    }

    private StreamSourceCoordinatorSplit requireGeneration(String splitId,
            long splitGeneration) {
        StreamSourceCoordinatorSplit current = splits.get(splitId);
        if (current == null
                || current.splitGeneration() != splitGeneration) {
            throw new IllegalStateException(
                    "split generation is stale or unknown");
        }
        return current;
    }

    private static StreamSourceSplit contract(StreamSplitSpec split) {
        return new StreamSourceSplit(split.splitId(), split.positionCodecId(),
                split.descriptorFrame());
    }

    private static StreamSourceCoordinatorSplit pending(
            StreamSourceSplit split, long generation) {
        return new StreamSourceCoordinatorSplit(split.splitId(), generation,
                split, StreamSourceCoordinatorSplit.Status.PENDING, null,
                0L, -1L, null);
    }

    private static StreamSourceCoordinatorSplit removed(String splitId,
            long generation, StreamSourceSplit split,
            long lastAssignmentGeneration) {
        return new StreamSourceCoordinatorSplit(splitId, generation, split,
                StreamSourceCoordinatorSplit.Status.REMOVED, null,
                lastAssignmentGeneration, -1L, null);
    }

    private static StreamSourceCoordinatorSplit copy(
            StreamSourceCoordinatorSplit current,
            StreamSourceCoordinatorSplit.Status status,
            StreamSourceAssignment assignment,
            long lastAssignmentGeneration, long logicalPosition,
            byte[] positionFrame) {
        return new StreamSourceCoordinatorSplit(current.splitId(),
                current.splitGeneration(), current.hasSplitDescriptor()
                        ? current.split() : null,
                status, assignment, lastAssignmentGeneration,
                logicalPosition, positionFrame);
    }
}
