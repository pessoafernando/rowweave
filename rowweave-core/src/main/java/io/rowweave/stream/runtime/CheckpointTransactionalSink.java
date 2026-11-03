package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointManifest;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamSinkBinding;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Task-local transactional Sink lifecycle with durable decision proof checks. */
public final class CheckpointTransactionalSink implements AutoCloseable {
    private enum Phase { OPEN, PREPARED, FINALIZED, ABORTED, CLOSED }

    private final String operatorUid;
    private final int subtaskIndex;
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private final long assignmentGeneration;
    private final StreamSinkBinding binding;
    private final TransactionalStreamSinkWriter writer;
    private final Thread ownerThread;

    private Phase phase = Phase.OPEN;
    private CheckpointIdentity preparedCheckpoint;
    private StreamCheckpointMember preparedMember;
    private StreamCheckpointException terminalFailure;

    public CheckpointTransactionalSink(
            String operatorUid, int subtaskIndex,
            String runId, String attemptId, long fenceToken,
            long assignmentGeneration, StreamSinkBinding binding,
            TransactionalStreamSinkProvider provider) {
        validateBase(operatorUid, subtaskIndex, runId, attemptId,
                fenceToken, assignmentGeneration, binding, provider);
        this.operatorUid = operatorUid;
        this.subtaskIndex = subtaskIndex;
        this.runId = runId;
        this.attemptId = attemptId;
        this.fenceToken = fenceToken;
        this.assignmentGeneration = assignmentGeneration;
        this.binding = binding;
        TransactionalStreamSinkWriter opened = provider.open(
                new StreamSinkTransactionOpenRequest(
                        operatorUid, subtaskIndex, runId, attemptId,
                        fenceToken, assignmentGeneration, binding));
        if (opened == null) {
            throw failure(StreamCheckpointException.Code.SINK_FAILURE,
                    "transactional Stream Sink provider returned no writer");
        }
        writer = opened;
        ownerThread = Thread.currentThread();
    }

    public static CheckpointTransactionalSink recover(
            String operatorUid, int subtaskIndex,
            String runId, String attemptId, long fenceToken,
            long assignmentGeneration, StreamSinkBinding binding,
            TransactionalStreamSinkProvider provider,
            CheckpointIdentity checkpoint,
            StreamCheckpointMember preparedMember) {
        validateBase(operatorUid, subtaskIndex, runId, attemptId,
                fenceToken, assignmentGeneration, binding, provider);
        validateScope(runId, attemptId, fenceToken, checkpoint);
        String expectedTransaction = transactionIdentity(
                operatorUid, subtaskIndex, assignmentGeneration,
                binding, checkpoint);
        if (preparedMember == null
                || preparedMember.kind()
                != StreamCheckpointMember.Kind.TRANSACTIONAL_SINK
                || !operatorUid.equals(preparedMember.operatorUid())
                || subtaskIndex != preparedMember.subtaskIndex()
                || !expectedTransaction.equals(preparedMember.subjectId())
                || !binding.providerContractDigest().equals(
                        preparedMember.contractDigest())) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "prepared Sink member does not match the fenced transaction");
        }
        CheckpointTransactionalSink recovered =
                new CheckpointTransactionalSink(
                        operatorUid, subtaskIndex, runId, attemptId,
                        fenceToken, assignmentGeneration, binding, provider);
        recovered.preparedCheckpoint = checkpoint;
        recovered.preparedMember = preparedMember;
        recovered.phase = Phase.PREPARED;
        return recovered;
    }

    public void write(byte[] recordFrame) {
        checkOwnerAndHealthy();
        requirePhase(Phase.OPEN);
        if (recordFrame == null || recordFrame.length == 0
                || recordFrame.length > StreamEdge.MAX_FRAME_BYTES) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "transactional Sink record frame is invalid");
        }
        invoke(() -> writer.write(recordFrame.clone()));
    }

    public StreamCheckpointMember prepare(CheckpointIdentity checkpoint) {
        checkOwnerAndHealthy();
        requirePhase(Phase.OPEN);
        validateScope(runId, attemptId, fenceToken, checkpoint);
        String transaction = transactionIdentity(
                operatorUid, subtaskIndex, assignmentGeneration,
                binding, checkpoint);
        byte[] receipt = invokeResult(() -> writer.prepare(transaction));
        if (receipt == null || receipt.length == 0
                || receipt.length > StreamEdge.MAX_FRAME_BYTES) {
            throw poison(StreamCheckpointException.Code.SINK_FAILURE,
                    "prepared Sink receipt frame is invalid", null);
        }
        StreamCheckpointMember member =
                StreamCheckpointMember.transactionalSink(
                        operatorUid, subtaskIndex, transaction,
                        binding.providerContractDigest(), sha256Hex(receipt),
                        receipt.length);
        preparedCheckpoint = checkpoint;
        preparedMember = member;
        phase = Phase.PREPARED;
        return member;
    }

    public void reconcile(
            CheckpointDecision commitDecision,
            StreamCheckpointAuthority authority) {
        checkOwnerAndHealthy();
        if (phase == Phase.FINALIZED) {
            requireCommitProof(commitDecision, authority);
            return;
        }
        requirePhase(Phase.PREPARED);
        requireCommitProof(commitDecision, authority);
        String transaction = preparedMember.subjectId();
        TransactionStatus status = invokeResult(
                () -> writer.query(transaction));
        if (status == TransactionStatus.PREPARED) {
            invoke(() -> writer.commit(transaction, commitDecision));
            status = invokeResult(() -> writer.query(transaction));
        }
        if (status != TransactionStatus.VISIBLE) {
            throw poison(StreamCheckpointException.Code.SINK_FAILURE,
                    "transactional Sink did not become exactly visible", null);
        }
        invoke(() -> writer.finalizeTransaction(transaction));
        phase = Phase.FINALIZED;
    }

    public void reconcile(
            CheckpointDecision commitDecision,
            FileCheckpointAuthority authority) {
        reconcile(commitDecision, (StreamCheckpointAuthority) authority);
    }

    public void abort(StreamCheckpointAuthority authority) {
        checkOwnerAndHealthy();
        if (authority == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint authority is required");
        }
        if (phase == Phase.ABORTED) return;
        requirePhase(Phase.PREPARED);
        CheckpointDecision decision = authority.firstDecision(
                preparedCheckpoint);
        if (decision != null
                && decision.status()
                == CheckpointDecision.Status.COMMIT_DECIDED) {
            throw failure(
                    StreamCheckpointException.Code.DECISION_ALREADY_COMMITTED,
                    "committed Sink transaction cannot abort");
        }
        invoke(() -> writer.abort(preparedMember.subjectId()));
        phase = Phase.ABORTED;
    }

    public void abort(FileCheckpointAuthority authority) {
        abort((StreamCheckpointAuthority) authority);
    }

    @Override
    public void close() {
        if (Thread.currentThread() != ownerThread) {
            throw failure(StreamCheckpointException.Code.THREAD_OWNERSHIP,
                    "transactional Sink is owned by its mailbox thread");
        }
        if (phase == Phase.CLOSED) return;
        try {
            writer.close();
        } catch (Throwable closeFailure) {
            if (terminalFailure != null) {
                terminalFailure.addSuppressed(closeFailure);
            } else {
                throw poison(StreamCheckpointException.Code.SINK_FAILURE,
                        "transactional Sink close failed", closeFailure);
            }
        } finally {
            phase = Phase.CLOSED;
        }
    }

    private void requireCommitProof(
            CheckpointDecision proposed,
            StreamCheckpointAuthority authority) {
        if (authority == null || proposed == null
                || proposed.status()
                != CheckpointDecision.Status.COMMIT_DECIDED
                || !preparedCheckpoint.equals(proposed.checkpointIdentity())) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "exact checkpoint commit decision is required");
        }
        CheckpointDecision durable = authority.firstDecision(
                preparedCheckpoint);
        if (!proposed.equals(durable)) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "checkpoint commit decision is not authoritative");
        }
        CheckpointManifest manifest = authority.manifest(
                proposed.manifestDigest());
        CheckpointEvidence expected = preparedMember.toEvidence();
        boolean found = false;
        for (CheckpointTaskReceipt receipt : manifest.taskReceipts()) {
            for (CheckpointEvidence evidence : receipt.evidence()) {
                if (expected.equals(evidence)) found = true;
            }
        }
        if (!found) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "checkpoint manifest excludes the prepared Sink receipt");
        }
    }

    private void requirePhase(Phase expected) {
        if (phase != expected) {
            throw failure(StreamCheckpointException.Code.SINK_FAILURE,
                    "transactional Sink lifecycle is not in " + expected);
        }
    }

    private void checkOwnerAndHealthy() {
        if (Thread.currentThread() != ownerThread) {
            throw failure(StreamCheckpointException.Code.THREAD_OWNERSHIP,
                    "transactional Sink is owned by its mailbox thread");
        }
        if (terminalFailure != null) throw terminalFailure;
    }

    private void invoke(Action action) {
        try {
            action.run();
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (Throwable providerFailure) {
            throw poison(StreamCheckpointException.Code.SINK_FAILURE,
                    "transactional Sink provider action failed",
                    providerFailure);
        }
    }

    private <T> T invokeResult(ResultAction<T> action) {
        try {
            return action.run();
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (Throwable providerFailure) {
            throw poison(StreamCheckpointException.Code.SINK_FAILURE,
                    "transactional Sink provider action failed",
                    providerFailure);
        }
    }

    private StreamCheckpointException poison(
            StreamCheckpointException.Code code, String message,
            Throwable cause) {
        StreamCheckpointException poisoned = cause == null
                ? failure(code, message) : failure(code, message, cause);
        terminalFailure = poisoned;
        return poisoned;
    }

    private static void validateBase(
            String operatorUid, int subtaskIndex,
            String runId, String attemptId, long fenceToken,
            long assignmentGeneration, StreamSinkBinding binding,
            TransactionalStreamSinkProvider provider) {
        if (operatorUid == null || operatorUid.isEmpty()
                || subtaskIndex < 0 || runId == null || runId.isEmpty()
                || attemptId == null || attemptId.isEmpty()
                || fenceToken <= 0L || assignmentGeneration <= 0L
                || binding == null || provider == null
                || binding.visibilityProfile()
                != StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL
                || !binding.providerKey().equals(provider.providerKey())
                || !binding.providerContractDigest().equals(
                        provider.contractDigest())) {
            throw new IllegalArgumentException(
                    "transactional Sink identity and provider contract mismatch");
        }
    }

    private static void validateScope(
            String runId, String attemptId, long fenceToken,
            CheckpointIdentity checkpoint) {
        if (checkpoint == null || !runId.equals(checkpoint.runId())
                || !attemptId.equals(checkpoint.attemptId())
                || fenceToken != checkpoint.fenceToken()) {
            throw failure(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                    "checkpoint is outside the Sink task attempt");
        }
    }

    private static String transactionIdentity(
            String operatorUid, int subtaskIndex, long assignmentGeneration,
            StreamSinkBinding binding, CheckpointIdentity checkpoint) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeText(output, checkpoint.artifactId());
                writeText(output, operatorUid);
                output.writeInt(subtaskIndex);
                writeText(output, checkpoint.runId());
                writeText(output, checkpoint.attemptId());
                output.writeLong(checkpoint.fenceToken());
                output.writeLong(assignmentGeneration);
                output.writeLong(checkpoint.coordinatorTerm());
                writeText(output, checkpoint.coordinatorIncarnation());
                output.writeLong(checkpoint.checkpointSequence());
                writeText(output, binding.providerKey());
                writeText(output, binding.providerContractDigest());
                writeText(output, binding.configurationDigest());
            }
            return "stream-tx:" + sha256Hex(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory transaction identity failed", impossible);
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            char[] alphabet = "0123456789abcdef".toCharArray();
            char[] result = new char[digest.length * 2];
            for (int index = 0; index < digest.length; index++) {
                int current = digest[index] & 0xff;
                result[index * 2] = alphabet[current >>> 4];
                result[index * 2 + 1] = alphabet[current & 0x0f];
            }
            return new String(result);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message,
            Throwable cause) {
        return new StreamCheckpointException(code, message, cause);
    }

    private interface Action {
        void run();
    }

    private interface ResultAction<T> {
        T run();
    }
}
