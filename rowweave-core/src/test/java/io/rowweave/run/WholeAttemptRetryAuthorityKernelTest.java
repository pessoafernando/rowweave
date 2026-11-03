package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WholeAttemptRetryAuthorityKernelTest {

    @Test
    void mismatchedAssignmentJournalFailsClosedAsCorruptEvidence() {
        RunSnapshot run = runningRun();
        StageAssignmentSnapshot assignments = lostAssignments(run, 1L);
        ManifestCommitJournalSnapshot exact =
                ManifestCommitJournalSnapshot.initial(assignments);
        assertEquals(WholeAttemptRetryClass.WORKER_LOST,
                WholeAttemptRetryAuthorityKernel.classify(
                        run, assignments, exact, null).retryClass());

        StageAssignmentSnapshot otherGeneration = lostAssignments(run, 2L);
        ManifestCommitJournalSnapshot mismatched =
                ManifestCommitJournalSnapshot.initial(otherGeneration);
        assertEquals(WholeAttemptRetryDecision.Code.NOT_RETRYABLE,
                WholeAttemptRetryAuthorityKernel.classify(
                        run, assignments, mismatched, null).code());
    }

    @Test
    void canceledAndAbsentDispatchClosuresRemainRetryableAfterAbort() {
        RunSnapshot run = runningRun();
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord first = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0,
                StageAssignmentRole.SOURCE_STAGE,
                "worker-a", "localhost", 7001, -1, "inc-a");
        StageAssignmentRecord second = StageAssignmentRecord.create(
                fence, 1L, 1, 102L, 1, 0,
                StageAssignmentRole.SINK,
                "worker-b", "localhost", 7002, -1, "inc-b");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                Arrays.asList(first, second));
        StageAssignmentSnapshot initial =
                StageAssignmentSnapshot.initial(manifest);
        StageAssignmentAcceptance accepted = new StageAssignmentAcceptance(
                initial.token(0), digest(3), 10L);
        StageAssignmentSnapshot partial = initial.withAcceptance(accepted)
                .withTerminal(StageTerminalReceipt.canceled(
                        accepted.token(), accepted.commandDigest(),
                        0L, 0L, "CANCELED", digest(4), 20L));
        BatchSinkAbortRecord aborted = BatchSinkAbortRecord.decided(
                fence, 1L,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", 1L, run.currentAttempt().publicationId(),
                digest(5), "ASSIGNMENT_DISPATCH_FAILED", 21L)
                .completed(22L);

        assertEquals(WholeAttemptRetryClass.PRE_DISPATCH_ABSENT,
                WholeAttemptRetryAuthorityKernel.classify(
                        run, partial,
                        ManifestCommitJournalSnapshot.initial(partial),
                        aborted).retryClass());

        StageAssignmentAcceptance secondAccepted =
                new StageAssignmentAcceptance(
                        partial.token(1), digest(6), 11L);
        StageAssignmentSnapshot allCanceled = partial
                .withAcceptance(secondAccepted)
                .withTerminal(StageTerminalReceipt.canceled(
                        secondAccepted.token(),
                        secondAccepted.commandDigest(), 0L, 0L,
                        "CANCELED", digest(7), 20L));
        assertEquals(WholeAttemptRetryClass.WORKER_LOST,
                WholeAttemptRetryAuthorityKernel.classify(
                        run, allCanceled,
                        ManifestCommitJournalSnapshot.initial(allCanceled),
                        aborted).retryClass());
    }

    private static RunSnapshot runningRun() {
        AttemptSnapshot attempt = new AttemptSnapshot(
                1, "attempt-1", 1L, "publication-1",
                AttemptStatus.RUNNING, null, 2L, 4L, null, null);
        return new RunSnapshot("kernel-corrupt", "definition", "artifact",
                2, 1, 4L, RunStatus.RUNNING, 0L, 1L, 4L, attempt);
    }

    private static StageAssignmentSnapshot lostAssignments(
            RunSnapshot run, long generation) {
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, generation, 0, 101L, 0, 0,
                StageAssignmentRole.WHOLE_GRAPH,
                "worker", "localhost", 7001, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), generation,
                AttemptOutputProfile.DISCARD,
                Collections.singletonList(record));
        StageAssignmentSnapshot initial =
                StageAssignmentSnapshot.initial(manifest);
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                initial.token(0), digest(1), 10L);
        return initial.withAcceptance(acceptance).withTerminal(
                StageTerminalReceipt.lost(
                        acceptance.token(), acceptance.commandDigest(),
                        "HOST_TERMINATED", digest(2), 0L));
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) value);
        return digest;
    }
}
