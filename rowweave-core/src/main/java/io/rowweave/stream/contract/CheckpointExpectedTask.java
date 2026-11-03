package io.rowweave.stream.contract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/** Exact task/evidence closure expected before a checkpoint may commit. */
public final class CheckpointExpectedTask implements Comparable<CheckpointExpectedTask> {
    private final String taskId;
    private final long assignmentGeneration;
    private final String workerIncarnation;
    private final List<CheckpointEvidenceKey> evidenceKeys;

    public CheckpointExpectedTask(
            String taskId, long assignmentGeneration, String workerIncarnation,
            List<CheckpointEvidenceKey> evidenceKeys) {
        this.taskId = StreamContractSupport.text(
                taskId, "checkpoint.expectedTask.taskId", 512);
        if (assignmentGeneration <= 0L) {
            throw StreamContractSupport.failure("INVALID_EXPECTED_TASK",
                    "checkpoint.expectedTask.assignmentGeneration",
                    "positive assignment generation is required");
        }
        this.assignmentGeneration = assignmentGeneration;
        this.workerIncarnation = StreamContractSupport.text(workerIncarnation,
                "checkpoint.expectedTask.workerIncarnation", 256);
        if (evidenceKeys == null || evidenceKeys.isEmpty() || evidenceKeys.size() > 256) {
            throw StreamContractSupport.failure("INVALID_EVIDENCE_SET",
                    "checkpoint.expectedTask.evidence", "bounded evidence set is required");
        }
        TreeSet<CheckpointEvidenceKey> canonical = new TreeSet<>();
        for (CheckpointEvidenceKey key : evidenceKeys) {
            if (key == null || !canonical.add(key)) {
                throw StreamContractSupport.failure("DUPLICATE_EVIDENCE",
                        "checkpoint.expectedTask.evidence",
                        "evidence keys must be non-null and unique");
            }
        }
        this.evidenceKeys = Collections.unmodifiableList(new ArrayList<>(canonical));
    }

    public String taskId() { return taskId; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public String workerIncarnation() { return workerIncarnation; }
    public List<CheckpointEvidenceKey> evidenceKeys() { return evidenceKeys; }

    @Override
    public int compareTo(CheckpointExpectedTask other) {
        return taskId.compareTo(other.taskId);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CheckpointExpectedTask)) return false;
        CheckpointExpectedTask that = (CheckpointExpectedTask) other;
        return assignmentGeneration == that.assignmentGeneration
                && taskId.equals(that.taskId)
                && workerIncarnation.equals(that.workerIncarnation)
                && evidenceKeys.equals(that.evidenceKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, assignmentGeneration,
                workerIncarnation, evidenceKeys);
    }
}
