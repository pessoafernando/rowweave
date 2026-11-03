package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointAuthorityReference;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.CheckpointEvidenceKey;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.runtime.FileCheckpointAuthority;
import io.rowweave.stream.runtime.FileCheckpointMemberStore;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.FileCheckpointRetention;
import io.rowweave.stream.runtime.FileStreamSavepointStore;
import io.rowweave.stream.runtime.StreamCheckpointCoordinator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** replacement and worker loss must advance one durable exact fence. */
class FileStreamClusterRunAuthorityTest {
    private static final String DIGEST = repeat('a');

    @Test
    void durablePlacementAcceptsMorePhysicalTasksThanLogicalNodes() {
        List<StreamWorkerPlacement.Assignment> assignments =
                new ArrayList<>();
        for (int index = 0; index < 2049; index++) {
            assignments.add(new StreamWorkerPlacement.Assignment(
                    assignments.size(), "source", index, "worker", "inc"));
            assignments.add(new StreamWorkerPlacement.Assignment(
                    assignments.size(), "sink", index, "worker", "inc"));
        }
        StreamClusterRunState state = StreamClusterRunState.initial(
                7L, "master", "run", "attempt", 11L, 3L, 1L,
                new StreamWorkerPlacement(assignments));
        assertEquals(4098, FileStreamClusterRunAuthority.decode(
                FileStreamClusterRunAuthority.encode(state))
                .placement().assignments().size());
    }

    @Test
    void oversizedPlacementStopsAtTheDurableFrameLease() {
        List<StreamWorkerPlacement.Assignment> assignments =
                new ArrayList<>();
        char[] chars = new char[256];
        Arrays.fill(chars, 'x');
        String incarnation = new String(chars);
        for (int index = 0; index < 1000; index++) {
            assignments.add(new StreamWorkerPlacement.Assignment(
                    assignments.size(), "source", index,
                    "worker", incarnation));
            assignments.add(new StreamWorkerPlacement.Assignment(
                    assignments.size(), "sink", index,
                    "worker", incarnation));
        }
        StreamClusterRunState state = StreamClusterRunState.initial(
                7L, "master", "run", "attempt", 11L, 3L, 1L,
                new StreamWorkerPlacement(assignments));
        StreamClusterProtocolException failure = assertThrows(
                StreamClusterProtocolException.class,
                () -> FileStreamClusterRunAuthority.encode(state));
        assertEquals(StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                failure.code());
    }

    @Test
    void liveRescaleAtomicallyAdvancesGenerationFromCompletedCut(
            @TempDir Path root) {
        FileStreamClusterRunAuthority runs =
                new FileStreamClusterRunAuthority(root);
        StreamWorkerPlacement initialPlacement = placement(
                "worker-a", "inc-a", "worker-b", "inc-b");
        runs.create(StreamClusterRunState.initial(
                7L, "master-inc-7", "run-1", "attempt-1",
                11L, 3L, 5L, initialPlacement));
        CheckpointIdentity cut = checkpoint(
                "attempt-1", 11L, 7L, "master-inc-7", 9L);
        runs.recordCompletedCheckpoint("run-1", 7L, "master-inc-7",
                completedDecision(cut));
        StreamWorkerPlacement targetPlacement = new StreamWorkerPlacement(
                Arrays.asList(
                        new StreamWorkerPlacement.Assignment(
                                0, "source", 0, "worker-a", "inc-a"),
                        new StreamWorkerPlacement.Assignment(
                                1, "source", 1, "worker-b", "inc-b"),
                        new StreamWorkerPlacement.Assignment(
                                2, "sink", 0, "worker-a", "inc-a"),
                        new StreamWorkerPlacement.Assignment(
                                3, "sink", 1, "worker-b", "inc-b")));

        StreamClusterRunState target = runs.liveRescaleCutover(
                "run-1", 7L, "master-inc-7", "attempt-1",
                11L, 3L, cut, "attempt-4", 12L, 4L, 6L,
                targetPlacement);

        assertEquals("attempt-4", target.attemptId());
        assertEquals(12L, target.fenceToken());
        assertEquals(4L, target.assignmentGeneration());
        assertEquals(6L, target.channelEpoch());
        assertEquals(cut, target.latestCompletedCheckpoint());
        assertEquals(4, target.placement().assignments().size());
        assertEquals(target, runs.liveRescaleCutover(
                "run-1", 7L, "master-inc-7", "attempt-1",
                11L, 3L, cut, "attempt-4", 12L, 4L, 6L,
                targetPlacement));
        assertCode(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                () -> runs.liveRescaleCutover(
                        "run-1", 7L, "master-inc-7", "attempt-1",
                        11L, 3L, cut, "attempt-5", 13L, 5L, 7L,
                        targetPlacement));
    }

    @Test
    void replacementRecoversCheckpointAndWorkerLossAtomicallyRestartsWholeGraph(
            @TempDir Path root) throws Exception {
        StreamWorkerPlacement firstPlacement = placement(
                "worker-a", "inc-a", "worker-b", "inc-b");
        StreamClusterRunState initial = StreamClusterRunState.initial(
                7L, "master-inc-7", "run-1", "attempt-1",
                11L, 3L, 1L, firstPlacement);
        FileStreamClusterRunAuthority first =
                new FileStreamClusterRunAuthority(root);
        assertEquals(initial, first.create(initial));

        CheckpointIdentity completed = checkpoint(
                "attempt-1", 11L, 7L, "master-inc-7", 4L);
        assertThrows(IllegalArgumentException.class,
                () -> first.recordCompletedCheckpoint(
                        "run-1", 7L, "master-inc-7",
                        CheckpointDecision.aborted(completed)));
        StreamClusterRunState checkpointed = first.recordCompletedCheckpoint(
                "run-1", 7L, "master-inc-7",
                completedDecision(completed));
        assertEquals(completed, checkpointed.latestCompletedCheckpoint());

        FileStreamClusterRunAuthority replacement =
                new FileStreamClusterRunAuthority(root);
        StreamClusterRunState recovered = replacement.replaceCoordinator(
                "run-1", 7L, "master-inc-7", 8L, "master-inc-8");
        assertEquals(8L, recovered.coordinatorTerm());
        assertEquals("master-inc-8", recovered.coordinatorIncarnation());
        assertEquals(completed, recovered.latestCompletedCheckpoint());
        assertEquals("attempt-1", recovered.attemptId());

        StreamWorkerPlacement replacementPlacement = placement(
                "worker-a", "inc-a", "worker-c", "inc-c");
        StreamClusterRunState restarted = replacement.restartAfterWorkerLoss(
                "run-1", 8L, "master-inc-8", "worker-b", "inc-b",
                "attempt-2", 12L, 4L, 2L, replacementPlacement);

        assertEquals("attempt-2", restarted.attemptId());
        assertEquals(12L, restarted.fenceToken());
        assertEquals(4L, restarted.assignmentGeneration());
        assertEquals(2L, restarted.channelEpoch());
        assertEquals(completed, restarted.latestCompletedCheckpoint());
        assertEquals("worker-c", restarted.placement().assignments()
                .get(1).workerId());
        assertEquals("source", restarted.placement().assignments()
                .get(0).operatorUid());
        assertEquals(0, restarted.placement().assignments()
                .get(0).subtaskIndex());
        assertEquals("sink", restarted.placement().assignments()
                .get(1).operatorUid());
        assertEquals(restarted,
                new FileStreamClusterRunAuthority(root).current("run-1"));

        assertCode(StreamClusterProtocolException.Code.STALE_COORDINATOR,
                () -> first.recordCompletedCheckpoint(
                        "run-1", 7L, "master-inc-7",
                        completedDecision(checkpoint(
                                "attempt-1", 11L, 7L,
                                "master-inc-7", 5L))));
        assertCode(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                () -> replacement.restartAfterWorkerLoss(
                        "run-1", 8L, "master-inc-8",
                        "worker-b", "inc-b", "attempt-3",
                        13L, 5L, 3L, replacementPlacement));

        byte[] corrupt = Files.readAllBytes(replacement.statePath("run-1"));
        corrupt[corrupt.length - 1] ^= 1;
        Files.write(replacement.statePath("run-1"), corrupt);
        assertCode(StreamClusterProtocolException.Code.CHECKSUM_MISMATCH,
                () -> new FileStreamClusterRunAuthority(root).current("run-1"));
    }

    @Test
    void operatorFailureRestartAdvancesWholeGraphWithoutInventingWorkerLoss(
            @TempDir Path root) {
        StreamWorkerPlacement placement = placement(
                "worker-a", "inc-a", "worker-b", "inc-b");
        FileStreamClusterRunAuthority runs =
                new FileStreamClusterRunAuthority(root);
        runs.create(StreamClusterRunState.initial(
                7L, "master-inc-7", "run-operator", "attempt-1",
                11L, 3L, 1L, placement));
        CheckpointIdentity completed = checkpointForRun(
                "run-operator", "attempt-1", 11L, 7L,
                "master-inc-7", 4L);
        runs.recordCompletedCheckpoint("run-operator", 7L,
                "master-inc-7", completedDecision(completed));

        StreamClusterRunState restarted = runs.restartAfterFailure(
                "run-operator", 7L, "master-inc-7", "attempt-2",
                12L, 4L, 2L, placement);

        assertEquals("attempt-2", restarted.attemptId());
        assertEquals(12L, restarted.fenceToken());
        assertEquals(4L, restarted.assignmentGeneration());
        assertEquals(2L, restarted.channelEpoch());
        assertEquals(completed, restarted.latestCompletedCheckpoint());
        assertEquals(2, restarted.placement().assignments().size());
        assertEquals("worker-a", restarted.placement().assignments()
                .get(0).workerId());
        assertEquals("worker-b", restarted.placement().assignments()
                .get(1).workerId());
        assertEquals(restarted, new FileStreamClusterRunAuthority(root)
                .current("run-operator"));
        assertCode(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                () -> runs.restartAfterFailure(
                        "run-operator", 7L, "master-inc-7", "attempt-3",
                        12L, 5L, 3L, placement));
    }

    @Test
    void workerRejectsOldCoordinatorAttemptReplayAndWrongIncarnation() {
        StreamClusterWorker worker = new StreamClusterWorker(
                profile("worker-a", "inc-a"),
                java.util.Collections.emptyMap(),
                java.util.Collections.emptyMap());
        worker.acceptControl(encoded(command(
                StreamControlMessage.Kind.START, 7L, "master-inc-7",
                "attempt-1", 11L, 3L, "inc-a", 1L)));
        worker.acceptControl(encoded(command(
                StreamControlMessage.Kind.START, 8L, "master-inc-8",
                "attempt-1", 11L, 3L, "inc-a", 1L)));

        assertCode(StreamClusterProtocolException.Code.STALE_COORDINATOR,
                () -> worker.acceptControl(encoded(command(
                        StreamControlMessage.Kind.CANCEL,
                        7L, "master-inc-7", "attempt-1",
                        11L, 3L, "inc-a", 2L))));
        assertCode(StreamClusterProtocolException.Code.COMMAND_REPLAY,
                () -> worker.acceptControl(encoded(command(
                        StreamControlMessage.Kind.START,
                        8L, "master-inc-8", "attempt-1",
                        11L, 3L, "inc-a", 1L))));

        worker.acceptControl(encoded(command(
                StreamControlMessage.Kind.RESTORE, 8L, "master-inc-8",
                "attempt-2", 12L, 4L, "inc-a", 2L)));
        assertCode(StreamClusterProtocolException.Code.STALE_ATTEMPT,
                () -> worker.acceptControl(encoded(command(
                        StreamControlMessage.Kind.START,
                        8L, "master-inc-8", "attempt-1",
                        11L, 3L, "inc-a", 3L))));
        assertCode(StreamClusterProtocolException.Code.WORKER_INCARNATION_MISMATCH,
                () -> worker.acceptControl(encoded(command(
                        StreamControlMessage.Kind.START,
                        9L, "master-inc-9", "attempt-3",
                        13L, 5L, "inc-old", 1L))));
    }

    @Test
    void decisionRecoveryIsIdempotentOutsideTheOrdinaryCommandSequence() {
        StreamClusterWorker worker = new StreamClusterWorker(
                profile("worker-a", "inc-a"),
                java.util.Collections.emptyMap(),
                java.util.Collections.emptyMap());
        worker.acceptControl(encoded(command(
                StreamControlMessage.Kind.START, 7L, "master-inc-7",
                "attempt-1", 11L, 3L, "inc-a", 9L)));

        worker.acceptRecoveryControl(encoded(command(
                StreamControlMessage.Kind.CHECKPOINT_TRIGGER,
                7L, "master-inc-7", "attempt-1",
                11L, 3L, "inc-a", 2L)));
        worker.acceptControl(encoded(command(
                StreamControlMessage.Kind.RESTORE, 7L, "master-inc-7",
                "attempt-2", 12L, 4L, "inc-a", 1L)));
    }

    @Test
    void workerFencesIndependentRunsWithoutRewritingEitherAttempt() {
        StreamClusterWorker worker = new StreamClusterWorker(
                profile("worker-a", "inc-a"),
                java.util.Collections.emptyMap(),
                java.util.Collections.emptyMap());
        worker.acceptControl(encoded(commandForRun(
                "run-a", StreamControlMessage.Kind.START,
                7L, "master-a", "attempt-1", 11L, 3L,
                "inc-a", 1L)));
        worker.acceptControl(encoded(commandForRun(
                "run-b", StreamControlMessage.Kind.START,
                4L, "master-b", "attempt-1", 9L, 2L,
                "inc-a", 1L)));
        worker.acceptControl(encoded(commandForRun(
                "run-a", StreamControlMessage.Kind.CANCEL,
                7L, "master-a", "attempt-1", 11L, 3L,
                "inc-a", 2L)));
        worker.acceptControl(encoded(commandForRun(
                "run-b", StreamControlMessage.Kind.CANCEL,
                4L, "master-b", "attempt-1", 9L, 2L,
                "inc-a", 2L)));
    }

    @Test
    void retentionKeepsCheckpointReferencedOnlyByDurableRunAuthority(
            @TempDir Path root) throws Exception {
        Path checkpointRoot = root.resolve("checkpoints");
        FileCheckpointAuthority checkpointAuthority =
                new FileCheckpointAuthority(
                        checkpointRoot.resolve("authority"));
        FileCheckpointMemberStore members =
                new FileCheckpointMemberStore(checkpointRoot);
        FileCheckpointPayloadStore payloads =
                new FileCheckpointPayloadStore(checkpointRoot, 1_024L);
        FileStreamSavepointStore savepoints =
                new FileStreamSavepointStore(checkpointRoot);
        Path staged = root.resolve("staged");
        Files.write(staged, new byte[] {1, 2, 3});
        FileCheckpointPayloadStore.StoredPayload payload =
                payloads.store(staged);
        StreamCheckpointMember member = StreamCheckpointMember.stateHandle(
                "aggregate", 0, 0, 127, "readout", "decimal-v1", 1,
                payload.digest(), payload.contentBytes());
        members.publish(member);
        CheckpointIdentity identity = checkpoint(
                "attempt-1", 11L, 7L, "master-inc-7", 1L);
        CheckpointEvidenceKey key = new CheckpointEvidenceKey(
                member.kind().evidenceKind(), member.memberId());
        CheckpointExpectation expectation = new CheckpointExpectation(
                identity, java.util.Collections.singletonList(
                new CheckpointExpectedTask(
                        "aggregate#0", 3L, "inc-a",
                        java.util.Collections.singletonList(key))));
        StreamCheckpointCoordinator coordinator =
                new StreamCheckpointCoordinator(
                        expectation, checkpointAuthority, members);
        coordinator.acknowledge(new CheckpointTaskReceipt(
                identity, "aggregate#0", 3L, "inc-a",
                java.util.Collections.singletonList(member.toEvidence())));
        CheckpointDecision completed = coordinator.complete(
                coordinator.decideCommit());

        FileStreamClusterRunAuthority runs =
                new FileStreamClusterRunAuthority(root.resolve("runs"));
        runs.create(StreamClusterRunState.initial(
                7L, "master-inc-7", "run-1", "attempt-1",
                11L, 3L, 1L,
                placement("worker-a", "inc-a", "worker-b", "inc-b")));
        runs.recordCompletedCheckpoint(
                "run-1", 7L, "master-inc-7", completed);

        CheckpointIdentity beforePointer = checkpoint(
                "attempt-1", 11L, 7L, "master-inc-7", 2L);
        StreamCheckpointCoordinator nextCoordinator =
                new StreamCheckpointCoordinator(
                        new CheckpointExpectation(
                                beforePointer,
                                java.util.Collections.singletonList(
                                        new CheckpointExpectedTask(
                                                "aggregate#0", 3L, "inc-a",
                                                java.util.Collections
                                                        .singletonList(key)))),
                        checkpointAuthority, members);
        nextCoordinator.acknowledge(new CheckpointTaskReceipt(
                beforePointer, "aggregate#0", 3L, "inc-a",
                java.util.Collections.singletonList(member.toEvidence())));
        CheckpointDecision completedBeforePointer = nextCoordinator.complete(
                nextCoordinator.decideCommit());

        new FileCheckpointRetention(
                checkpointAuthority, members, payloads, savepoints,
                runs.completedCheckpointRoots(), runs.runIds())
                .retainLatest("run-2", 1);

        assertNotNull(checkpointAuthority.decision(identity),
                "run recovery root must survive another run's sweep");
        assertNotNull(checkpointAuthority.decision(beforePointer),
                "completed cut must survive the crash window before run pointer publication");
        payloads.verifyStored(payload);
        assertEquals(beforePointer, runs.recordCompletedCheckpoint(
                "run-1", 7L, "master-inc-7", completedBeforePointer)
                .latestCompletedCheckpoint());
    }

    @Test
    void retiringTerminalRunRemovesItFromRetentionRoots(
            @TempDir Path root) {
        FileStreamClusterRunAuthority runs =
                new FileStreamClusterRunAuthority(root);
        runs.create(StreamClusterRunState.initial(
                7L, "master-inc-7", "terminal-run", "attempt-1",
                11L, 3L, 1L,
                placement("worker-a", "inc-a", "worker-b", "inc-b")));

        runs.retire("terminal-run");

        assertFalse(runs.runIds().contains("terminal-run"));
        assertFalse(Files.exists(runs.statePath("terminal-run")));
    }

    private static StreamControlMessage command(
            StreamControlMessage.Kind kind, long term, String coordinator,
            String attempt, long fence, long generation,
            String workerIncarnation, long sequence) {
        return commandForRun("run-1", kind, term, coordinator, attempt,
                fence, generation, workerIncarnation, sequence);
    }

    private static StreamControlMessage commandForRun(
            String runId, StreamControlMessage.Kind kind,
            long term, String coordinator, String attempt,
            long fence, long generation,
            String workerIncarnation, long sequence) {
        return new StreamControlMessage(
                kind, term, coordinator, runId, attempt,
                fence, generation, workerIncarnation, sequence,
                DIGEST, new byte[0]);
    }

    private static byte[] encoded(StreamControlMessage command) {
        return StreamControlMessageCodec.encode(command);
    }

    private static StreamWorkerPlacement placement(
            String firstWorker, String firstIncarnation,
            String secondWorker, String secondIncarnation) {
        return new StreamWorkerPlacement(Arrays.asList(
                new StreamWorkerPlacement.Assignment(
                        0, "source", 0, firstWorker, firstIncarnation),
                new StreamWorkerPlacement.Assignment(
                        1, "sink", 0, secondWorker, secondIncarnation)));
    }

    private static StreamWorkerCapabilityProfile profile(
            String workerId, String incarnation) {
        return new StreamWorkerCapabilityProfile(
                workerId, incarnation, 1, 1,
                Arrays.asList(
                        "rowweave.runtime.stream.v1",
                        StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                        StreamWorkerCapabilityProfile.DATA_CAPABILITY),
                1, 1, 4_096, 8_192L, 2);
    }

    private static CheckpointIdentity checkpoint(
            String attemptId, long fenceToken, long term,
            String coordinator, long sequence) {
        return checkpointForRun("run-1", attemptId, fenceToken, term,
                coordinator, sequence);
    }

    private static CheckpointIdentity checkpointForRun(
            String runId, String attemptId, long fenceToken, long term,
            String coordinator, long sequence) {
        return new CheckpointIdentity(
                "definition-1", DIGEST, runId, attemptId,
                fenceToken, term, coordinator, sequence,
                DIGEST, DIGEST, 1, 1);
    }

    private static CheckpointDecision completedDecision(
            CheckpointIdentity checkpoint) {
        CheckpointEvidenceKey key = new CheckpointEvidenceKey(
                CheckpointEvidence.Kind.SOURCE_POSITION, "source-position");
        CheckpointExpectation expectation = new CheckpointExpectation(
                checkpoint, java.util.Collections.singletonList(
                        new CheckpointExpectedTask(
                                "source#0", 3L, "inc-a",
                                java.util.Collections.singletonList(key))));
        CheckpointAuthorityReference reference =
                new CheckpointAuthorityReference(expectation);
        reference.acknowledge(new CheckpointTaskReceipt(
                checkpoint, "source#0", 3L, "inc-a",
                java.util.Collections.singletonList(new CheckpointEvidence(
                        CheckpointEvidence.Kind.SOURCE_POSITION,
                        "source-position", DIGEST, 1L))));
        return CheckpointDecision.completed(reference.decideCommit());
    }

    private static void assertCode(
            StreamClusterProtocolException.Code code,
            org.junit.jupiter.api.function.Executable executable) {
        StreamClusterProtocolException failure = assertThrows(
                StreamClusterProtocolException.class, executable);
        assertEquals(code, failure.code());
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
