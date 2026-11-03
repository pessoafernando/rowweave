package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.CheckpointEvidenceKey;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamContractException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** joint source/state/multi-Sink checkpoint decision. */
class StreamCheckpointCoordinatorTest {
    private static final String A = digest('a');
    private static final String B = digest('b');
    private static final String C = digest('c');

    @Test
    void exactJointClosureDecidesAndReplacementSurvivesAckLoss(
            @TempDir Path root) {
        CheckpointExpectation expectation = expectation();
        StreamCheckpointCoordinator coordinator =
                new StreamCheckpointCoordinator(expectation,
                        new FileCheckpointAuthority(root));

        coordinator.acknowledge(sourceReceipt());
        coordinator.acknowledge(sinkReceipt("sink-a#0", "sink-a", A));
        StreamContractException incomplete = assertThrows(
                StreamContractException.class, coordinator::decideCommit);
        assertEquals("CHECKPOINT_INCOMPLETE", incomplete.code());
        assertNull(coordinator.currentDecision());

        coordinator.acknowledge(sinkReceipt("sink-b#0", "sink-b", B));
        CheckpointDecision commit = coordinator.decideCommit();
        assertEquals(CheckpointDecision.Status.COMMIT_DECIDED,
                commit.status());

        StreamCheckpointCoordinator replacement =
                new StreamCheckpointCoordinator(expectation,
                        new FileCheckpointAuthority(root));
        assertEquals(commit, replacement.currentDecision());
        assertEquals(commit, replacement.decideCommit(),
                "ACK loss must reread the exact first decision");
        CheckpointDecision completed = replacement.complete(commit);
        assertEquals(CheckpointDecision.Status.COMPLETED,
                completed.status());
        assertEquals(completed,
                new StreamCheckpointCoordinator(expectation,
                        new FileCheckpointAuthority(root))
                        .currentDecision());
    }

    @Test
    void replacementBeforeDecisionCanAbortButCannotCommitPartialClosure(
            @TempDir Path root) {
        CheckpointExpectation expectation = expectation();
        StreamCheckpointCoordinator first =
                new StreamCheckpointCoordinator(expectation,
                        new FileCheckpointAuthority(root));
        first.acknowledge(sourceReceipt());

        StreamCheckpointCoordinator replacement =
                new StreamCheckpointCoordinator(expectation,
                        new FileCheckpointAuthority(root));
        StreamContractException incomplete = assertThrows(
                StreamContractException.class, replacement::decideCommit);
        assertEquals("CHECKPOINT_INCOMPLETE", incomplete.code());

        CheckpointDecision aborted = replacement.abort();
        assertEquals(CheckpointDecision.Status.ABORTED, aborted.status());
        assertEquals(aborted, replacement.abort());
        assertEquals(aborted,
                new StreamCheckpointCoordinator(expectation,
                        new FileCheckpointAuthority(root))
                        .currentDecision());
        assertThrows(StreamCheckpointException.class,
                replacement::decideCommit);
        assertThrows(StreamCheckpointException.class,
                () -> replacement.acknowledge(sourceReceipt()));
    }

    private static CheckpointExpectation expectation() {
        return new CheckpointExpectation(checkpoint(), Arrays.asList(
                new CheckpointExpectedTask(
                        "source#0", 2L, "worker-1", Arrays.asList(
                        new CheckpointEvidenceKey(
                                CheckpointEvidence.Kind.SOURCE_POSITION,
                                "source-position"),
                        new CheckpointEvidenceKey(
                                CheckpointEvidence.Kind.STATE,
                                "operator-state"))),
                expectedSink("sink-a#0", "sink-a"),
                expectedSink("sink-b#0", "sink-b")));
    }

    private static CheckpointExpectedTask expectedSink(
            String taskId, String memberId) {
        return new CheckpointExpectedTask(
                taskId, 2L, "worker-1",
                Collections.singletonList(new CheckpointEvidenceKey(
                        CheckpointEvidence.Kind.TRANSACTIONAL_SINK,
                        memberId)));
    }

    private static CheckpointTaskReceipt sourceReceipt() {
        return new CheckpointTaskReceipt(
                checkpoint(), "source#0", 2L, "worker-1", Arrays.asList(
                new CheckpointEvidence(
                        CheckpointEvidence.Kind.SOURCE_POSITION,
                        "source-position", A, 8L),
                new CheckpointEvidence(
                        CheckpointEvidence.Kind.STATE,
                        "operator-state", B, 4096L)));
    }

    private static CheckpointTaskReceipt sinkReceipt(
            String taskId, String memberId, String digest) {
        return new CheckpointTaskReceipt(
                checkpoint(), taskId, 2L, "worker-1",
                Collections.singletonList(new CheckpointEvidence(
                        CheckpointEvidence.Kind.TRANSACTIONAL_SINK,
                        memberId, digest, 64L)));
    }

    private static CheckpointIdentity checkpoint() {
        return new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                4L, "coordinator-1", 1L, B, C, 1, 1);
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
