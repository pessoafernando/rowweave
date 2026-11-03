package io.rowweave.stream.contract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** One fenced task ACK with canonical digest-only evidence. */
public final class CheckpointTaskReceipt {
    private final CheckpointIdentity checkpointIdentity;
    private final String taskId;
    private final long assignmentGeneration;
    private final String workerIncarnation;
    private final List<CheckpointEvidence> evidence;

    public CheckpointTaskReceipt(
            CheckpointIdentity checkpointIdentity, String taskId,
            long assignmentGeneration, String workerIncarnation,
            List<CheckpointEvidence> evidence) {
        if (checkpointIdentity == null || assignmentGeneration <= 0L) {
            throw StreamContractSupport.failure("INVALID_TASK_RECEIPT",
                    "checkpoint.receipt", "checkpoint and assignment are required");
        }
        this.checkpointIdentity = checkpointIdentity;
        this.taskId = StreamContractSupport.text(
                taskId, "checkpoint.receipt.taskId", 512);
        this.workerIncarnation = StreamContractSupport.text(
                workerIncarnation, "checkpoint.receipt.workerIncarnation", 256);
        if (evidence == null || evidence.isEmpty() || evidence.size() > 256) {
            throw StreamContractSupport.failure("INVALID_EVIDENCE_SET",
                    "checkpoint.receipt.evidence", "bounded evidence is required");
        }
        TreeMap<CheckpointEvidenceKey, CheckpointEvidence> canonical = new TreeMap<>();
        for (CheckpointEvidence member : evidence) {
            if (member == null || canonical.put(member.key(), member) != null) {
                throw StreamContractSupport.failure("DUPLICATE_EVIDENCE",
                        "checkpoint.receipt.evidence", "evidence keys must be unique");
            }
        }
        this.assignmentGeneration = assignmentGeneration;
        this.evidence = Collections.unmodifiableList(
                new ArrayList<>(canonical.values()));
    }

    public CheckpointIdentity checkpointIdentity() { return checkpointIdentity; }
    public String taskId() { return taskId; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public String workerIncarnation() { return workerIncarnation; }
    public List<CheckpointEvidence> evidence() { return evidence; }

    List<CheckpointEvidenceKey> evidenceKeys() {
        List<CheckpointEvidenceKey> keys = new ArrayList<>(evidence.size());
        for (CheckpointEvidence member : evidence) keys.add(member.key());
        return keys;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CheckpointTaskReceipt)) return false;
        CheckpointTaskReceipt that = (CheckpointTaskReceipt) other;
        return assignmentGeneration == that.assignmentGeneration
                && checkpointIdentity.equals(that.checkpointIdentity)
                && taskId.equals(that.taskId)
                && workerIncarnation.equals(that.workerIncarnation)
                && evidence.equals(that.evidence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpointIdentity, taskId, assignmentGeneration,
                workerIncarnation, evidence);
    }
}
