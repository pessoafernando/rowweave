package io.rowweave.stream.contract;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/** Pure deterministic first-decision-wins checkpoint authority reference model. */
public final class CheckpointAuthorityReference {
    private final CheckpointExpectation expectation;
    private final Map<String, CheckpointTaskReceipt> receipts = new TreeMap<>();
    private CheckpointDecision decision;
    private CheckpointManifest manifest;
    private CheckpointDecision commitDecision;

    public CheckpointAuthorityReference(CheckpointExpectation expectation) {
        if (expectation == null) throw new IllegalArgumentException("expectation is required");
        this.expectation = expectation;
        this.decision = new CheckpointDecision(expectation.checkpointIdentity(),
                CheckpointDecision.Status.COLLECTING, null);
    }

    public synchronized void acknowledge(CheckpointTaskReceipt receipt) {
        if (receipt == null || !expectation.checkpointIdentity().equals(
                receipt.checkpointIdentity())) {
            throw failure("STALE_CHECKPOINT", "checkpoint.receipt",
                    "receipt belongs to another checkpoint");
        }
        if (decision.status() != CheckpointDecision.Status.COLLECTING) {
            throw failure("CHECKPOINT_ALREADY_DECIDED", "checkpoint.decision",
                    "checkpoint no longer accepts receipts");
        }
        CheckpointExpectedTask expected = expectation.task(receipt.taskId());
        if (expected == null) {
            throw failure("UNEXPECTED_TASK", "checkpoint.receipt.taskId",
                    "task is outside the frozen checkpoint closure");
        }
        if (expected.assignmentGeneration() != receipt.assignmentGeneration()
                || !expected.workerIncarnation().equals(
                receipt.workerIncarnation())) {
            throw failure("STALE_TASK_ASSIGNMENT", "checkpoint.receipt.assignment",
                    "receipt does not match the frozen task assignment");
        }
        if (!expected.evidenceKeys().equals(receipt.evidenceKeys())) {
            throw failure("EVIDENCE_SET_MISMATCH", "checkpoint.receipt.evidence",
                    "receipt evidence does not equal its frozen closure");
        }
        CheckpointTaskReceipt previous = receipts.get(receipt.taskId());
        if (previous != null && !previous.equals(receipt)) {
            throw failure("TASK_RECEIPT_CONFLICT", "checkpoint.receipt",
                    "same task ACK drifted");
        }
        if (previous == null) receipts.put(receipt.taskId(), receipt);
    }

    public synchronized CheckpointDecision decideCommit() {
        if (decision.status() == CheckpointDecision.Status.ABORTED) {
            throw failure("DECISION_ALREADY_ABORTED", "checkpoint.decision",
                    "aborted checkpoint cannot commit");
        }
        if (decision.status() == CheckpointDecision.Status.COMMIT_DECIDED) {
            return decision;
        }
        if (decision.status() == CheckpointDecision.Status.COMPLETED) {
            return commitDecision;
        }
        if (receipts.size() != expectation.tasks().size()) {
            throw failure("CHECKPOINT_INCOMPLETE", "checkpoint.receipts",
                    "not all expected tasks have acknowledged");
        }
        manifest = new CheckpointManifest(expectation.checkpointIdentity(),
                new ArrayList<>(receipts.values()));
        commitDecision = new CheckpointDecision(expectation.checkpointIdentity(),
                CheckpointDecision.Status.COMMIT_DECIDED,
                manifest.manifestDigest());
        decision = commitDecision;
        return decision;
    }

    public synchronized CheckpointDecision abort() {
        if (decision.status() == CheckpointDecision.Status.COMMIT_DECIDED
                || decision.status() == CheckpointDecision.Status.COMPLETED) {
            throw failure("DECISION_ALREADY_COMMITTED", "checkpoint.decision",
                    "committed checkpoint cannot abort");
        }
        if (decision.status() == CheckpointDecision.Status.ABORTED) return decision;
        decision = new CheckpointDecision(expectation.checkpointIdentity(),
                CheckpointDecision.Status.ABORTED, null);
        return decision;
    }

    public synchronized void complete(CheckpointDecision commitDecision) {
        if (decision.status() == CheckpointDecision.Status.ABORTED) {
            throw failure("CHECKPOINT_NOT_COMMITTED", "checkpoint.decision",
                    "aborted checkpoint cannot complete");
        }
        if (commitDecision == null
                || commitDecision.status() != CheckpointDecision.Status.COMMIT_DECIDED
                || !expectation.checkpointIdentity().equals(
                commitDecision.checkpointIdentity())
                || !commitDecision.manifestDigest().equals(decision.manifestDigest())) {
            throw failure("STALE_CHECKPOINT_DECISION", "checkpoint.decision",
                    "completion requires the exact commit decision");
        }
        decision = new CheckpointDecision(expectation.checkpointIdentity(),
                CheckpointDecision.Status.COMPLETED, decision.manifestDigest());
    }

    public synchronized CheckpointDecision snapshot() { return decision; }

    public synchronized CheckpointManifest manifest() {
        if (manifest == null) {
            throw failure("CHECKPOINT_NOT_COMMITTED", "checkpoint.manifest",
                    "manifest exists only after the commit decision");
        }
        return manifest;
    }

    private String manifestDigest() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeCheckpoint(output, expectation.checkpointIdentity());
                output.writeInt(receipts.size());
                for (CheckpointTaskReceipt receipt : receipts.values()) {
                    writeText(output, receipt.taskId());
                    output.writeLong(receipt.assignmentGeneration());
                    writeText(output, receipt.workerIncarnation());
                    output.writeInt(receipt.evidence().size());
                    for (CheckpointEvidence evidence : receipt.evidence()) {
                        output.writeInt(evidence.kind().wireId());
                        writeText(output, evidence.memberId());
                        writeText(output, evidence.contentDigest());
                        output.writeLong(evidence.contentBytes());
                    }
                }
            }
            return hex(sha256(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory checkpoint digest failed", impossible);
        }
    }

    private static void writeCheckpoint(
            DataOutputStream output, CheckpointIdentity checkpoint) throws IOException {
        writeText(output, checkpoint.definitionId());
        writeText(output, checkpoint.artifactId());
        writeText(output, checkpoint.runId());
        writeText(output, checkpoint.attemptId());
        output.writeLong(checkpoint.fenceToken());
        output.writeLong(checkpoint.coordinatorTerm());
        writeText(output, checkpoint.coordinatorIncarnation());
        output.writeLong(checkpoint.checkpointSequence());
        writeText(output, checkpoint.planDigest());
        writeText(output, checkpoint.deploymentDigest());
        output.writeInt(checkpoint.dataProtocolVersion());
        output.writeInt(checkpoint.stateFormatVersion());
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int current = value[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }

    private static StreamContractException failure(
            String code, String path, String message) {
        return StreamContractSupport.failure(code, path, message);
    }
}
