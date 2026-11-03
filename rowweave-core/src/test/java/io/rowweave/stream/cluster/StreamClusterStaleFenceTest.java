package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointAuthorityReference;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.CheckpointEvidenceKey;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamContractException;
import io.rowweave.stream.contract.StreamEnvelope;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** old channel, assignment and commit identities have zero mutation. */
class StreamClusterStaleFenceTest {
    private static final String DIGEST = repeat('a');

    @Test
    void restartedAttemptRejectsOldBarrierReceiptAndCommitProof()
            throws Exception {
        StreamChannelIdentity activeChannel = new StreamChannelIdentity(
                "run-1", "attempt-2", 12L, 4L, "lane-1", 2L);
        StreamChannelIdentity staleChannel = new StreamChannelIdentity(
                "run-1", "attempt-1", 11L, 3L, "lane-1", 1L);
        try (StreamSocketChannel.Pair pair = StreamSocketChannel.openLoopback(
                activeChannel, 1, 4_096, 4_096L)) {
            StreamContractException staleBarrier = assertThrows(
                    StreamContractException.class,
                    () -> pair.sender().send(StreamEnvelope.checkpointBarrier(
                            staleChannel, 1L, checkpoint(
                                    "attempt-1", 11L, 7L,
                                    "master-inc-7", 3L))));
            assertEquals("STALE_CHANNEL", staleBarrier.code());
            assertEquals(0, pair.sender().outstandingEvents());
        }

        CheckpointIdentity active = checkpoint(
                "attempt-2", 12L, 8L, "master-inc-8", 4L);
        CheckpointIdentity stale = checkpoint(
                "attempt-1", 11L, 7L, "master-inc-7", 3L);
        CheckpointAuthorityReference activeAuthority = authority(
                active, "inc-c", 4L);
        StreamContractException staleReceipt = assertThrows(
                StreamContractException.class,
                () -> activeAuthority.acknowledge(receipt(
                        stale, "inc-a", 3L)));
        assertEquals("STALE_CHECKPOINT", staleReceipt.code());

        activeAuthority.acknowledge(receipt(active, "inc-c", 4L));
        activeAuthority.decideCommit();
        CheckpointAuthorityReference oldAuthority = authority(
                stale, "inc-a", 3L);
        oldAuthority.acknowledge(receipt(stale, "inc-a", 3L));
        CheckpointDecision oldCommit = oldAuthority.decideCommit();
        StreamContractException staleCommit = assertThrows(
                StreamContractException.class,
                () -> activeAuthority.complete(oldCommit));
        assertEquals("STALE_CHECKPOINT_DECISION", staleCommit.code());
    }

    private static CheckpointAuthorityReference authority(
            CheckpointIdentity checkpoint, String workerIncarnation,
            long assignmentGeneration) {
        return new CheckpointAuthorityReference(new CheckpointExpectation(
                checkpoint, Collections.singletonList(
                        new CheckpointExpectedTask(
                                "sink#0", assignmentGeneration,
                                workerIncarnation,
                                Collections.singletonList(key())))));
    }

    private static CheckpointTaskReceipt receipt(
            CheckpointIdentity checkpoint, String workerIncarnation,
            long assignmentGeneration) {
        return new CheckpointTaskReceipt(
                checkpoint, "sink#0", assignmentGeneration,
                workerIncarnation, Collections.singletonList(
                        new CheckpointEvidence(
                                CheckpointEvidence.Kind.TRANSACTIONAL_SINK,
                                "sink-member", DIGEST, 1L)));
    }

    private static CheckpointEvidenceKey key() {
        return new CheckpointEvidenceKey(
                CheckpointEvidence.Kind.TRANSACTIONAL_SINK, "sink-member");
    }

    private static CheckpointIdentity checkpoint(
            String attemptId, long fenceToken, long term,
            String coordinator, long sequence) {
        return new CheckpointIdentity(
                "definition-1", DIGEST, "run-1", attemptId,
                fenceToken, term, coordinator, sequence,
                DIGEST, DIGEST, 1, 1);
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
