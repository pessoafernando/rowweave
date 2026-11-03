package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.CheckpointEvidenceKey;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamCheckpointMember;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A rescaled cut is committed with the same per-task closure as a live cut. */
final class StreamSyntheticCheckpointPlan {
    private static final String WORKER = "offline-rescale-authority";
    private static final int MAX_TASK_EVIDENCE = 256;

    private final CheckpointExpectation expectation;
    private final List<CheckpointTaskReceipt> receipts;

    private StreamSyntheticCheckpointPlan(CheckpointExpectation expectation,
            List<CheckpointTaskReceipt> receipts) {
        this.expectation = expectation;
        this.receipts = Collections.unmodifiableList(receipts);
    }

    static StreamSyntheticCheckpointPlan from(CheckpointIdentity checkpoint,
            List<StreamCheckpointMember> members) {
        if (checkpoint == null || members == null || members.isEmpty()) {
            throw new IllegalArgumentException(
                    "synthetic checkpoint requires members and identity");
        }
        Map<String, List<CheckpointEvidence>> byTask = new LinkedHashMap<>();
        for (StreamCheckpointMember member : members) {
            if (member == null) {
                throw new IllegalArgumentException(
                        "synthetic checkpoint has a null member");
            }
            String taskId = member.operatorUid().length() + ":"
                    + member.operatorUid() + ':' + member.subtaskIndex();
            byTask.computeIfAbsent(taskId,
                    ignored -> new ArrayList<>()).add(member.toEvidence());
        }
        List<CheckpointExpectedTask> expected = new ArrayList<>();
        List<CheckpointTaskReceipt> receipts = new ArrayList<>();
        for (Map.Entry<String, List<CheckpointEvidence>> entry
                : byTask.entrySet()) {
            List<CheckpointEvidence> evidence = entry.getValue();
            if (evidence.size() > MAX_TASK_EVIDENCE) {
                throw new IllegalArgumentException(
                        "synthetic task checkpoint evidence exceeds the per-task lease");
            }
            List<CheckpointEvidenceKey> keys = new ArrayList<>(
                    evidence.size());
            for (CheckpointEvidence item : evidence) keys.add(item.key());
            expected.add(new CheckpointExpectedTask(
                    entry.getKey(), 1L, WORKER, keys));
            receipts.add(new CheckpointTaskReceipt(
                    checkpoint, entry.getKey(), 1L, WORKER, evidence));
        }
        return new StreamSyntheticCheckpointPlan(
                new CheckpointExpectation(checkpoint, expected), receipts);
    }

    CheckpointExpectation expectation() { return expectation; }
    List<CheckpointTaskReceipt> receipts() { return receipts; }
}
