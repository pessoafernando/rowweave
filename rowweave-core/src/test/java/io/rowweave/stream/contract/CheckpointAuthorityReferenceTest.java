package io.rowweave.stream.contract;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** pure reference model for the checkpoint decision linearization. */
class CheckpointAuthorityReferenceTest {
    private static final String A = digest('a');
    private static final String B = digest('b');
    private static final String C = digest('c');

    @Test
    void physicalTaskClosureCanExceedLogicalNodeLimit() {
        CheckpointIdentity checkpoint = checkpoint(1L);
        List<CheckpointExpectedTask> expected = new ArrayList<>();
        List<CheckpointTaskReceipt> receipts = new ArrayList<>();
        CheckpointEvidence evidence = new CheckpointEvidence(
                CheckpointEvidence.Kind.STATE, "state", A, 1L);
        CheckpointEvidenceKey key = new CheckpointEvidenceKey(
                CheckpointEvidence.Kind.STATE, "state");
        for (int index = 0; index < 4098; index++) {
            String task = "task#" + index;
            expected.add(new CheckpointExpectedTask(task, 2L, "worker-1",
                    Collections.singletonList(key)));
            receipts.add(new CheckpointTaskReceipt(checkpoint, task, 2L,
                    "worker-1", Collections.singletonList(evidence)));
        }
        CheckpointExpectation expectation = new CheckpointExpectation(
                checkpoint, expected);
        assertEquals(4098, expectation.tasks().size());
        CheckpointManifest manifest = new CheckpointManifest(checkpoint,
                receipts);
        assertEquals(manifest, CheckpointManifestCodec.decode(
                CheckpointManifestCodec.encode(manifest)));
    }

    @Test
    void commitRequiresExactTaskAndEvidenceClosureAndIsAckLossIdempotent() {
        CheckpointIdentity checkpoint = checkpoint(1L);
        CheckpointExpectation expectation = new CheckpointExpectation(
                checkpoint,
                Arrays.asList(
                        new CheckpointExpectedTask("source#0", 2L, "worker-1", Arrays.asList(
                                new CheckpointEvidenceKey(
                                        CheckpointEvidence.Kind.SOURCE_POSITION,
                                        "source/split-0"),
                                new CheckpointEvidenceKey(
                                        CheckpointEvidence.Kind.STATE,
                                        "source/operator-state"))),
                        new CheckpointExpectedTask("sink#0", 2L, "worker-1", Collections.singletonList(
                                new CheckpointEvidenceKey(
                                        CheckpointEvidence.Kind.TRANSACTIONAL_SINK,
                                        "sink/transaction")))));
        CheckpointAuthorityReference authority =
                new CheckpointAuthorityReference(expectation);

        CheckpointTaskReceipt source = receipt(checkpoint, "source#0",
                new CheckpointEvidence(CheckpointEvidence.Kind.SOURCE_POSITION,
                        "source/split-0", A, 8L),
                new CheckpointEvidence(CheckpointEvidence.Kind.STATE,
                        "source/operator-state", B, 1024L));
        authority.acknowledge(source);
        assertCode("CHECKPOINT_INCOMPLETE", authority::decideCommit);

        CheckpointTaskReceipt sink = receipt(checkpoint, "sink#0",
                new CheckpointEvidence(CheckpointEvidence.Kind.TRANSACTIONAL_SINK,
                        "sink/transaction", C, 64L));
        authority.acknowledge(sink);
        authority.acknowledge(source);
        CheckpointDecision first = authority.decideCommit();
        CheckpointDecision replay = authority.decideCommit();

        assertEquals(CheckpointDecision.Status.COMMIT_DECIDED, first.status());
        assertEquals(first, replay);
        assertEquals(first.manifestDigest(), authority.snapshot().manifestDigest());

        authority.complete(first);
        authority.complete(first);
        assertEquals(CheckpointDecision.Status.COMPLETED,
                authority.snapshot().status());
        assertEquals(first, authority.decideCommit());
        assertCode("DECISION_ALREADY_COMMITTED", authority::abort);
    }

    @Test
    void driftStaleCheckpointAndAbortAreFailClosed() {
        CheckpointIdentity checkpoint = checkpoint(1L);
        CheckpointExpectation expectation = new CheckpointExpectation(
                checkpoint, Collections.singletonList(
                        new CheckpointExpectedTask("task#0", 2L, "worker-1", Collections.singletonList(
                                new CheckpointEvidenceKey(
                                        CheckpointEvidence.Kind.STATE, "state")))));
        CheckpointAuthorityReference authority =
                new CheckpointAuthorityReference(expectation);
        CheckpointTaskReceipt accepted = receipt(checkpoint, "task#0",
                new CheckpointEvidence(CheckpointEvidence.Kind.STATE, "state", A, 1L));

        assertCode("STALE_TASK_ASSIGNMENT", () -> authority.acknowledge(
                new CheckpointTaskReceipt(checkpoint, "task#0", 3L, "worker-1",
                        accepted.evidence())));
        assertCode("STALE_TASK_ASSIGNMENT", () -> authority.acknowledge(
                new CheckpointTaskReceipt(checkpoint, "task#0", 2L, "worker-old",
                        accepted.evidence())));
        assertEquals(CheckpointDecision.Status.COLLECTING,
                authority.snapshot().status());
        authority.acknowledge(accepted);

        assertCode("TASK_RECEIPT_CONFLICT", () -> authority.acknowledge(
                receipt(checkpoint, "task#0",
                        new CheckpointEvidence(CheckpointEvidence.Kind.STATE,
                                "state", B, 1L))));
        assertCode("STALE_CHECKPOINT", () -> authority.acknowledge(
                receipt(checkpoint(2L), "task#0",
                        new CheckpointEvidence(CheckpointEvidence.Kind.STATE,
                                "state", A, 1L))));

        CheckpointAuthorityReference aborted =
                new CheckpointAuthorityReference(expectation);
        CheckpointDecision abort = aborted.abort();
        assertEquals(CheckpointDecision.Status.ABORTED, abort.status());
        assertEquals(abort, aborted.abort());
        assertCode("DECISION_ALREADY_ABORTED", aborted::decideCommit);
        assertCode("CHECKPOINT_NOT_COMMITTED", () -> aborted.complete(abort));
    }

    @Test
    void runAllowsOneCheckpointInFlightAndRestoresOnlyCompletedCuts() {
        StreamCheckpointRunReference run = new StreamCheckpointRunReference(
                "run-1", "attempt-1", 1L);
        CheckpointExpectation first = expectation(checkpoint(1L));
        run.trigger(first);

        assertCode("CHECKPOINT_IN_FLIGHT", () ->
                run.trigger(expectation(checkpoint(2L))));
        run.acknowledge(receipt(first.checkpointIdentity(), "task#0",
                new CheckpointEvidence(CheckpointEvidence.Kind.STATE,
                        "state", A, 1L)));
        CheckpointDecision commit = run.decideCommit();
        assertEquals(null, run.latestRestorePoint());
        assertCode("CHECKPOINT_IN_FLIGHT", () ->
                run.trigger(expectation(checkpoint(2L))));

        run.complete(commit);
        assertEquals(CheckpointDecision.Status.COMPLETED,
                run.latestRestorePoint().status());
        run.trigger(expectation(checkpoint(2L)));
        assertCode("CHECKPOINT_SEQUENCE_REPLAY", () ->
                run.trigger(expectation(checkpoint(1L))));

        StreamCheckpointRunReference fenced = new StreamCheckpointRunReference(
                "run-1", "attempt-2", 2L);
        assertCode("STALE_CHECKPOINT", () -> fenced.trigger(first));
    }

    @Test
    void committedManifestAndDecisionHaveCanonicalBoundedWire() {
        CheckpointExpectation expectation = expectation(checkpoint(1L));
        CheckpointAuthorityReference authority =
                new CheckpointAuthorityReference(expectation);
        authority.acknowledge(receipt(expectation.checkpointIdentity(), "task#0",
                new CheckpointEvidence(CheckpointEvidence.Kind.STATE,
                        "state", A, 4096L)));
        CheckpointDecision decision = authority.decideCommit();
        CheckpointManifest manifest = authority.manifest();

        byte[] manifestFrame = CheckpointManifestCodec.encode(manifest);
        assertArrayEquals(manifestFrame, CheckpointManifestCodec.encode(
                CheckpointManifestCodec.decode(manifestFrame)));
        assertEquals(manifest, CheckpointManifestCodec.decode(manifestFrame));
        assertEquals(manifest.manifestDigest(), decision.manifestDigest());

        byte[] decisionFrame = CheckpointDecisionCodec.encode(decision);
        assertArrayEquals(decisionFrame, CheckpointDecisionCodec.encode(
                CheckpointDecisionCodec.decode(decisionFrame)));
        assertEquals(decision, CheckpointDecisionCodec.decode(decisionFrame));

        byte[] corrupt = manifestFrame.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertCode("CHECKSUM_MISMATCH", () ->
                CheckpointManifestCodec.decode(corrupt));
        for (int length = 0; length < decisionFrame.length; length++) {
            byte[] truncated = Arrays.copyOf(decisionFrame, length);
            assertCode("FRAME_LENGTH_MISMATCH", () ->
                    CheckpointDecisionCodec.decode(truncated));
        }
    }

    private static CheckpointExpectation expectation(CheckpointIdentity identity) {
        return new CheckpointExpectation(identity, Collections.singletonList(
                new CheckpointExpectedTask("task#0", 2L, "worker-1",
                        Collections.singletonList(new CheckpointEvidenceKey(
                                CheckpointEvidence.Kind.STATE, "state")))));
    }

    private static CheckpointTaskReceipt receipt(
            CheckpointIdentity checkpoint, String taskId,
            CheckpointEvidence... evidence) {
        return new CheckpointTaskReceipt(checkpoint, taskId, 2L, "worker-1",
                Arrays.asList(evidence));
    }

    private static CheckpointIdentity checkpoint(long sequence) {
        return new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                4L, "coordinator-1", sequence, A, B, 1, 1);
    }

    private static void assertCode(String code,
                                   org.junit.jupiter.api.function.Executable action) {
        StreamContractException failure = assertThrows(StreamContractException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
