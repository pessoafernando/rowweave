package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidenceKey;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSinkBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** checkpoint-transactional Sink prepare and reconcile. */
class CheckpointTransactionalSinkTest {
    private static final String A = digest('a');
    private static final String B = digest('b');
    private static final String C = digest('c');

    @Test
    void prepareIsPrivateAndDurableDecisionReconcilesExactlyOnce(
            @TempDir Path root) {
        RecordingProvider provider = new RecordingProvider();
        CheckpointIdentity checkpoint = checkpoint();
        CheckpointTransactionalSink sink = sink(provider);
        sink.write(bytes("row-1"));
        sink.write(bytes("row-2"));

        StreamCheckpointMember member = sink.prepare(checkpoint);
        assertEquals(StreamCheckpointMember.Kind.TRANSACTIONAL_SINK,
                member.kind());
        assertFalse(provider.visible(member.subjectId()));

        FileCheckpointAuthority authority = new FileCheckpointAuthority(root);
        StreamCheckpointCoordinator coordinator = coordinator(member, authority);
        coordinator.acknowledge(receipt(member));
        CheckpointDecision commit = coordinator.decideCommit();

        sink.reconcile(commit, authority);
        assertTrue(provider.visible(member.subjectId()));
        assertEquals(Arrays.asList("row-1", "row-2"),
                provider.visibleRows(member.subjectId()));
        assertEquals(1, provider.commitCalls);
        assertEquals(1, provider.finalizeCalls);

        sink.reconcile(commit, authority);
        assertEquals(1, provider.commitCalls);
        assertEquals(1, provider.finalizeCalls);
        StreamCheckpointException staleProof = assertThrows(
                StreamCheckpointException.class,
                () -> sink.reconcile(null, authority));
        assertEquals(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                staleProof.code());

        CheckpointTransactionalSink replacement =
                CheckpointTransactionalSink.recover(
                        "sink", 0, "run-1", "attempt-1", 1L, 2L,
                        binding(), provider, checkpoint, member);
        replacement.reconcile(commit, authority);
        assertEquals(1, provider.commitCalls,
                "ACK-loss replacement must query visible transaction");
        assertEquals(1, provider.finalizeCalls,
                "provider finalize is exact-idempotent by transaction");
    }

    @Test
    void missingDecisionOrManifestMembershipRejectsBeforeCommit(
            @TempDir Path root) {
        RecordingProvider provider = new RecordingProvider();
        CheckpointTransactionalSink sink = sink(provider);
        StreamCheckpointMember member = sink.prepare(checkpoint());
        FileCheckpointAuthority authority = new FileCheckpointAuthority(root);

        StreamCheckpointException absent = assertThrows(
                StreamCheckpointException.class,
                () -> sink.reconcile(unpublishedCommit(member), authority));
        assertEquals(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                absent.code());
        assertEquals(0, provider.commitCalls);

        StreamCheckpointMember other = StreamCheckpointMember.transactionalSink(
                "sink", 0, "other-transaction", A, B, 1L);
        StreamCheckpointCoordinator wrongCoordinator =
                coordinator(other, authority);
        wrongCoordinator.acknowledge(receipt(other));
        CheckpointDecision wrongManifest = wrongCoordinator.decideCommit();
        StreamCheckpointException missingMember = assertThrows(
                StreamCheckpointException.class,
                () -> sink.reconcile(wrongManifest, authority));
        assertEquals(StreamCheckpointException.Code.COMMIT_PROOF_INVALID,
                missingMember.code());
        assertEquals(0, provider.commitCalls);
    }

    @Test
    void abortWorksOnlyBeforeCommitDecision(@TempDir Path root) {
        RecordingProvider provider = new RecordingProvider();
        CheckpointTransactionalSink abortable = sink(provider);
        StreamCheckpointMember first = abortable.prepare(checkpoint());
        FileCheckpointAuthority abortAuthority =
                new FileCheckpointAuthority(root.resolve("abort"));
        abortable.abort(abortAuthority);
        assertTrue(provider.aborted(first.subjectId()));

        RecordingProvider committedProvider = new RecordingProvider();
        CheckpointTransactionalSink committed = sink(committedProvider);
        StreamCheckpointMember second = committed.prepare(checkpoint());
        FileCheckpointAuthority commitAuthority =
                new FileCheckpointAuthority(root.resolve("commit"));
        StreamCheckpointCoordinator coordinator =
                coordinator(second, commitAuthority);
        coordinator.acknowledge(receipt(second));
        CheckpointDecision decision = coordinator.decideCommit();

        StreamCheckpointException rejected = assertThrows(
                StreamCheckpointException.class,
                () -> committed.abort(commitAuthority));
        assertEquals(StreamCheckpointException.Code.DECISION_ALREADY_COMMITTED,
                rejected.code());
        assertFalse(committedProvider.aborted(second.subjectId()));
        committed.reconcile(decision, commitAuthority);
        assertTrue(committedProvider.visible(second.subjectId()));
    }

    @Test
    void providerAndVisibilityMismatchRejectBeforeOpen() {
        RecordingProvider provider = new RecordingProvider();
        StreamSinkBinding eager = new StreamSinkBinding(
                "transactional-test", A, sha256Hex(new byte[] {7}),
                new byte[] {7},
                StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE);
        assertThrows(IllegalArgumentException.class,
                () -> new CheckpointTransactionalSink(
                        "sink", 0, "run-1", "attempt-1", 1L, 2L,
                        eager, provider));
        assertEquals(0, provider.openCalls);

        provider.contractDigest = B;
        assertThrows(IllegalArgumentException.class,
                () -> sink(provider));
        assertEquals(0, provider.openCalls);
    }

    @Test
    void providerFailurePoisonsButCloseStillReleasesWriter() {
        RecordingProvider provider = new RecordingProvider();
        RuntimeException writeBoom = new RuntimeException("write boom");
        provider.writeFailure = writeBoom;
        CheckpointTransactionalSink sink = sink(provider);

        StreamCheckpointException failure = assertThrows(
                StreamCheckpointException.class,
                () -> sink.write(bytes("row")));
        assertEquals(StreamCheckpointException.Code.SINK_FAILURE,
                failure.code());
        assertEquals(writeBoom, failure.getCause());

        sink.close();
        assertEquals(1, provider.closeCalls);
    }

    private static CheckpointTransactionalSink sink(
            RecordingProvider provider) {
        return new CheckpointTransactionalSink(
                "sink", 0, "run-1", "attempt-1", 1L, 2L,
                binding(), provider);
    }

    private static StreamSinkBinding binding() {
        byte[] configuration = new byte[] {7};
        return new StreamSinkBinding(
                "transactional-test", A, sha256Hex(configuration),
                configuration,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
    }

    private static StreamCheckpointCoordinator coordinator(
            StreamCheckpointMember member,
            FileCheckpointAuthority authority) {
        CheckpointExpectedTask task = new CheckpointExpectedTask(
                "sink#0", 2L, "worker-1",
                Collections.singletonList(new CheckpointEvidenceKey(
                        member.kind().evidenceKind(), member.memberId())));
        return new StreamCheckpointCoordinator(
                new CheckpointExpectation(
                        checkpoint(), Collections.singletonList(task)),
                authority);
    }

    private static CheckpointTaskReceipt receipt(
            StreamCheckpointMember member) {
        return new CheckpointTaskReceipt(
                checkpoint(), "sink#0", 2L, "worker-1",
                Collections.singletonList(member.toEvidence()));
    }

    private static CheckpointDecision unpublishedCommit(
            StreamCheckpointMember member) {
        io.rowweave.stream.contract.CheckpointAuthorityReference reference =
                new io.rowweave.stream.contract.CheckpointAuthorityReference(
                        new CheckpointExpectation(
                                checkpoint(), Collections.singletonList(
                                new CheckpointExpectedTask(
                                        "sink#0", 2L, "worker-1",
                                        Collections.singletonList(
                                                new CheckpointEvidenceKey(
                                                        member.kind().evidenceKind(),
                                                        member.memberId()))))));
        reference.acknowledge(receipt(member));
        return reference.decideCommit();
    }

    private static CheckpointIdentity checkpoint() {
        return new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                4L, "coordinator-1", 1L, B, C, 1, 1);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder hex = new StringBuilder();
            for (byte current : digest) {
                hex.append(String.format("%02x", current & 0xff));
            }
            return hex.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class RecordingProvider
            implements TransactionalStreamSinkProvider {
        private String contractDigest = A;
        private final Map<String, Transaction> transactions =
                new LinkedHashMap<>();
        private int openCalls;
        private int commitCalls;
        private int finalizeCalls;
        private int closeCalls;
        private RuntimeException writeFailure;

        @Override public String providerKey() { return "transactional-test"; }
        @Override public String contractDigest() { return contractDigest; }

        @Override
        public TransactionalStreamSinkWriter open(
                StreamSinkTransactionOpenRequest request) {
            openCalls++;
            return new TransactionalStreamSinkWriter() {
                private final List<String> pending = new ArrayList<>();

                @Override public void write(byte[] recordFrame) {
                    if (writeFailure != null) throw writeFailure;
                    pending.add(new String(recordFrame, StandardCharsets.UTF_8));
                }

                @Override public byte[] prepare(String transactionIdentity) {
                    Transaction transaction = transactions.computeIfAbsent(
                            transactionIdentity, ignored ->
                                    new Transaction(new ArrayList<>(pending)));
                    return bytes("receipt:" + transactionIdentity + ':'
                            + transaction.rows.size());
                }

                @Override public TransactionStatus query(
                        String transactionIdentity) {
                    Transaction transaction = transactions.get(transactionIdentity);
                    return transaction == null ? TransactionStatus.UNKNOWN
                            : transaction.status;
                }

                @Override public void commit(
                        String transactionIdentity,
                        CheckpointDecision decision) {
                    Transaction transaction = transactions.get(transactionIdentity);
                    if (transaction.status != TransactionStatus.VISIBLE) {
                        transaction.status = TransactionStatus.VISIBLE;
                        commitCalls++;
                    }
                }

                @Override public void finalizeTransaction(
                        String transactionIdentity) {
                    Transaction transaction = transactions.get(transactionIdentity);
                    if (!transaction.finalized) {
                        transaction.finalized = true;
                        finalizeCalls++;
                    }
                }

                @Override public void abort(String transactionIdentity) {
                    transactions.get(transactionIdentity).status =
                            TransactionStatus.ABORTED;
                }

                @Override public void close() { closeCalls++; }
            };
        }

        private boolean visible(String transactionIdentity) {
            Transaction transaction = transactions.get(transactionIdentity);
            return transaction != null
                    && transaction.status == TransactionStatus.VISIBLE;
        }

        private boolean aborted(String transactionIdentity) {
            Transaction transaction = transactions.get(transactionIdentity);
            return transaction != null
                    && transaction.status == TransactionStatus.ABORTED;
        }

        private List<String> visibleRows(String transactionIdentity) {
            return transactions.get(transactionIdentity).rows;
        }
    }

    private static final class Transaction {
        private final List<String> rows;
        private TransactionStatus status = TransactionStatus.PREPARED;
        private boolean finalized;

        private Transaction(List<String> rows) {
            this.rows = rows;
        }
    }
}
