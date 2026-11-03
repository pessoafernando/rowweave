package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** product submission and cancel identities are durable Master authority. */
class StreamProductJobAuthorityContractTest {

    @Test
    void sharedStateStorePreservesJobAcrossAuthorityReplacement() {
        StreamProductJobStateStore store =
                new InMemoryStreamProductJobStateStore();
        FileStreamProductJobAuthority first =
                new FileStreamProductJobAuthority(store);
        StreamSubmissionId submission = StreamSubmissionId.of(
                "shared-store-submission");
        PlanArtifact artifact = artifact("shared-store-job");
        StreamProductJobSnapshot created = first.publish(
                submission, artifact);
        first.transition(created.jobId(), StreamJobStatus.STARTING);

        FileStreamProductJobAuthority replacement =
                new FileStreamProductJobAuthority(store);
        StreamProductJobSnapshot recovered =
                replacement.bySubmission(submission);
        assertEquals(StreamJobStatus.STARTING, recovered.status());
        assertEquals(artifact, recovered.artifact());
        assertEquals(Collections.singletonList(recovered),
                replacement.all());
    }

    @Test
    void submissionRetryAndMasterReplacementConvergeOnOneDurableJob(
            @TempDir Path root) {
        FileStreamProductJobAuthority first =
                new FileStreamProductJobAuthority(root);
        StreamSubmissionId submission = StreamSubmissionId.of("submission-1");
        PlanArtifact artifact = artifact("job-a");

        StreamProductJobSnapshot created = first.publish(submission, artifact);
        StreamProductJobSnapshot retried = first.publish(submission, artifact);

        assertEquals(created, retried);
        assertEquals(StreamJobStatus.SUBMITTED, created.status());
        assertEquals(artifact.artifactId(), created.artifactId());
        assertEquals(artifact, created.artifact());

        FileStreamProductJobAuthority replacement =
                new FileStreamProductJobAuthority(root);
        assertEquals(created, replacement.bySubmission(submission));
        assertEquals(created, replacement.byJob(created.jobId()));

        StreamProductControlException conflict = assertThrows(
                StreamProductControlException.class,
                () -> replacement.publish(submission, artifact("job-b")));
        assertEquals(StreamProductControlException.Code.SUBMISSION_CONFLICT,
                conflict.code());
        assertEquals(created, replacement.bySubmission(submission));
    }

    @Test
    void cancelCommandIsIdempotentAndFirstTerminalStatusWins(
            @TempDir Path root) {
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        StreamProductJobSnapshot created = authority.publish(
                StreamSubmissionId.of("submission-cancel"), artifact("job-c"));
        authority.transition(created.jobId(), StreamJobStatus.STARTING);
        authority.transition(created.jobId(), StreamJobStatus.RUNNING);
        StreamCommandId command = StreamCommandId.of("cancel-1");

        StreamProductJobSnapshot accepted =
                authority.acceptCancel(created.jobId(), command);
        assertEquals(StreamJobStatus.STOPPING, accepted.status());
        assertEquals(StreamOperationStatus.ACCEPTED,
                accepted.cancelStatus(command));
        assertEquals(accepted,
                authority.acceptCancel(created.jobId(), command));

        StreamProductJobSnapshot canceled =
                authority.completeCancel(created.jobId(), command);
        assertEquals(StreamJobStatus.CANCELED, canceled.status());
        assertEquals(StreamOperationStatus.SUCCEEDED,
                canceled.cancelStatus(command));
        assertEquals(canceled,
                authority.transition(created.jobId(), StreamJobStatus.FAILED));

        StreamProductControlException commandConflict = assertThrows(
                StreamProductControlException.class,
                () -> authority.acceptCancel(created.jobId(),
                        StreamCommandId.of("cancel-2")));
        assertEquals(StreamProductControlException.Code.COMMAND_CONFLICT,
                commandConflict.code());
    }

    @Test
    void finishedStatusIsDurableAndCannotBeRewritten(@TempDir Path root) {
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        StreamProductJobSnapshot created = authority.publish(
                StreamSubmissionId.of("submission-finished"),
                artifact("job-finished"));
        authority.transition(created.jobId(), StreamJobStatus.STARTING);
        authority.transition(created.jobId(), StreamJobStatus.RUNNING);
        StreamProductJobSnapshot finished = authority.transition(
                created.jobId(), StreamJobStatus.FINISHED);
        assertEquals(StreamJobStatus.FINISHED, finished.status());

        FileStreamProductJobAuthority replacement =
                new FileStreamProductJobAuthority(root);
        assertEquals(StreamJobStatus.FINISHED,
                replacement.byJob(created.jobId()).status());
        assertEquals(finished, replacement.transition(
                created.jobId(), StreamJobStatus.FAILED));
        assertThrows(StreamProductControlException.class, () ->
                replacement.acceptCancel(created.jobId(),
                        StreamCommandId.of("cancel-finished")));
    }

    @Test
    void unknownSubmissionAndJobFailWithStableCodes(@TempDir Path root) {
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        StreamProductControlException submission = assertThrows(
                StreamProductControlException.class,
                () -> authority.bySubmission(
                        StreamSubmissionId.of("missing-submission")));
        assertEquals(StreamProductControlException.Code.JOB_NOT_FOUND,
                submission.code());
        StreamProductControlException job = assertThrows(
                StreamProductControlException.class,
                () -> authority.byJob(
                        io.rowweave.stream.api.StreamJobId.of("missing-job")));
        assertEquals(StreamProductControlException.Code.JOB_NOT_FOUND,
                job.code());
    }

    @Test
    void checkpointAuthorityAllowsOneInFlightAndCountsOnlyCompletion(
            @TempDir Path root) {
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        StreamProductJobSnapshot job = authority.publish(
                StreamSubmissionId.of("checkpoint-authority"),
                artifact("checkpoint-authority-job"));
        authority.transition(job.jobId(), StreamJobStatus.STARTING);
        authority.transition(job.jobId(), StreamJobStatus.RUNNING);
        StreamCommandId first = StreamCommandId.of("checkpoint-first");
        StreamCommandId second = StreamCommandId.of("checkpoint-second");

        StreamProductJobSnapshot accepted =
                authority.acceptCheckpoint(job.jobId(), first);
        assertEquals(StreamOperationStatus.ACCEPTED,
                accepted.checkpoint(first).status());
        long acceptedAt = accepted.checkpoint(first)
                .acceptedAtEpochMillis();
        assertTrue(acceptedAt > 0L);
        FileStreamProductJobAuthority replacement =
                new FileStreamProductJobAuthority(root);
        assertEquals(acceptedAt, replacement.byJob(job.jobId())
                .checkpoint(first).acceptedAtEpochMillis());
        assertEquals(acceptedAt, replacement.startCheckpoint(
                job.jobId(), first).checkpoint(first)
                .acceptedAtEpochMillis());
        StreamProductControlException inFlight = assertThrows(
                StreamProductControlException.class,
                () -> authority.acceptCheckpoint(job.jobId(), second));
        assertEquals(StreamProductControlException.Code.INVALID_STATE,
                inFlight.code());
        authority.startCheckpoint(job.jobId(), first);
        StreamProductJobSnapshot completed =
                authority.completeCheckpoint(job.jobId(), first);
        assertEquals(1L, completed.progress().completedCheckpoints());
        assertTrue(completed.checkpoint(first).completedAtEpochMillis()
                >= acceptedAt);
        assertEquals(completed.checkpoint(first).completedAtEpochMillis(),
                new FileStreamProductJobAuthority(root).byJob(job.jobId())
                        .checkpoint(first).completedAtEpochMillis());
        assertEquals(completed,
                authority.completeCheckpoint(job.jobId(), first));

        authority.acceptCheckpoint(job.jobId(), second);
        StreamProductJobSnapshot failed =
                authority.failCheckpoint(job.jobId(), second);
        assertEquals(StreamOperationStatus.FAILED,
                failed.checkpoint(second).status());
        assertEquals(1L, failed.progress().completedCheckpoints());
    }

    @Test
    void versionSevenAuthorityFrameRemainsReadableWithoutInventingStartTime(
            @TempDir Path root) {
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        StreamProductJobSnapshot job = authority.publish(
                StreamSubmissionId.of("authority-v7"),
                artifact("authority-v7-job"));
        authority.transition(job.jobId(), StreamJobStatus.STARTING);
        authority.transition(job.jobId(), StreamJobStatus.RUNNING);
        StreamCommandId command = StreamCommandId.of("checkpoint-v7");
        StreamProductJobSnapshot accepted = authority.acceptCheckpoint(
                job.jobId(), command);

        StreamProductJobSnapshot decoded = FileStreamProductJobAuthority
                .decode(FileStreamProductJobAuthority.encode(accepted, 7));
        assertEquals(StreamOperationStatus.ACCEPTED,
                decoded.checkpoint(command).status());
        assertEquals(0L,
                decoded.checkpoint(command).acceptedAtEpochMillis());
        assertEquals(accepted.evidence(), decoded.evidence());
        assertEquals(accepted.artifactId(), decoded.artifactId());
    }

    @Test
    void operationalEvidenceIsDurableAndFencedToItsJob(@TempDir Path root) {
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        StreamProductJobSnapshot created = authority.publish(
                StreamSubmissionId.of("evidence-authority"),
                artifact("evidence-authority-job"));
        StreamJobProgress attempt = new StreamJobProgress(
                1, 0L, 10L,
                io.rowweave.stream.api.StreamCdcPhase.SNAPSHOT, 0,
                3L, 2L, 30L, 20L, 1L, 10L,
                2, 12L, 14L, 1, 1, 4L);
        StreamJobEvidence evidence = new StreamJobEvidence(
                created.jobId(), "run-evidence", "attempt-evidence",
                1, 2L, 3L, 4L, null, 0L, 0L, 0L,
                attempt, attempt, 2, 2, 1_000L);

        StreamProductJobSnapshot updated = authority.updateEvidence(
                created.jobId(), evidence);
        assertEquals(evidence, updated.evidence());
        assertEquals(attempt, updated.progress());

        FileStreamProductJobAuthority replacement =
                new FileStreamProductJobAuthority(root);
        assertEquals(evidence,
                replacement.byJob(created.jobId()).evidence());
        assertThrows(IllegalArgumentException.class,
                () -> replacement.updateEvidence(created.jobId(),
                        StreamJobEvidence.unavailable(
                                io.rowweave.stream.api.StreamJobId.of(
                                        "another-job"),
                                StreamJobProgress.empty(), 2_000L)));
    }

    @Test
    void staleMasterAndAttemptEvidenceCannotOverwriteNewerAuthority(
            @TempDir Path root) {
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        StreamProductJobSnapshot created = authority.publish(
                StreamSubmissionId.of("evidence-fencing"),
                artifact("evidence-fencing-job"));
        StreamJobProgress first = progress(1, 3L);
        StreamJobEvidence initial = new StreamJobEvidence(
                created.jobId(), "run-fenced", "attempt-1", 1,
                2L, 3L, 4L, null, 0L, 0L, 0L,
                first, first, 1, 1, 1_000L);
        authority.updateEvidence(created.jobId(), initial);

        StreamJobProgress advanced = progress(1, 5L);
        StreamJobEvidence latest = new StreamJobEvidence(
                created.jobId(), "run-fenced", "attempt-1", 1,
                2L, 3L, 4L,
                "stream-checkpoint:" + sha256("checkpoint-2"),
                2L, 20L, 200L, advanced, advanced,
                1, 1, 2_000L);
        assertEquals(latest, authority.updateEvidence(
                created.jobId(), latest).evidence());

        assertEquals(latest, authority.updateEvidence(created.jobId(),
                initial).evidence());
        StreamJobEvidence oldTerm = new StreamJobEvidence(
                created.jobId(), "run-fenced", "attempt-old-master", 2,
                3L, 2L, 5L, null, 0L, 0L, 0L,
                progress(2, 9L), progress(2, 14L),
                1, 1, 3_000L);
        assertEquals(latest, authority.updateEvidence(
                created.jobId(), oldTerm).evidence());

        StreamJobProgress restartedAttempt = progress(2, 1L);
        StreamJobProgress restartedLifetime = progress(2, 6L);
        StreamJobEvidence restarted = new StreamJobEvidence(
                created.jobId(), "run-fenced", "attempt-2", 2,
                3L, 4L, 5L,
                "stream-checkpoint:" + sha256("checkpoint-2"),
                2L, 20L, 200L, restartedAttempt, restartedLifetime,
                1, 1, 4_000L);
        assertEquals(restarted, authority.updateEvidence(
                created.jobId(), restarted).evidence());
        assertEquals(restarted, authority.updateEvidence(
                created.jobId(), latest).evidence());
    }

    @Test
    void restartPreservesLastPublishedCheckpointEvidenceUntilReplacementReports(
            @TempDir Path root) {
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        StreamProductJobSnapshot created = authority.publish(
                StreamSubmissionId.of("restart-evidence"),
                artifact("restart-evidence-job"));
        authority.transition(created.jobId(), StreamJobStatus.STARTING);
        authority.transition(created.jobId(), StreamJobStatus.RUNNING);
        StreamJobProgress first = progress(1, 5L);
        StreamJobEvidence published = new StreamJobEvidence(
                created.jobId(), "run-restart", "attempt-1", 1,
                2L, 3L, 4L,
                "stream-checkpoint:" + sha256("restart-checkpoint"),
                14L, 20L, 200L, first, first,
                1, 1, 1_000L);
        authority.updateEvidence(created.jobId(), published);

        StreamProductJobSnapshot restarting =
                authority.beginRestart(created.jobId());

        assertEquals(StreamJobStatus.RESTARTING, restarting.status());
        assertTrue(restarting.evidence().available());
        assertEquals(published, restarting.evidence());
        assertEquals(14L, restarting.evidence()
                .latestCompletedCheckpointSequence());

        StreamJobProgress replacementAttempt = progress(2, 1L);
        StreamJobProgress replacementLifetime = progress(2, 6L);
        StreamJobEvidence replacement = new StreamJobEvidence(
                created.jobId(), "run-restart", "attempt-2", 2,
                3L, 3L, 5L,
                published.latestCompletedCheckpointId(),
                published.latestCompletedCheckpointSequence(),
                published.latestCheckpointDurationMillis(),
                published.latestCheckpointBytes(), replacementAttempt,
                replacementLifetime, 1, 1, 2_000L);
        assertEquals(replacement, authority.updateEvidence(
                created.jobId(), replacement).evidence());
    }

    private static PlanArtifact artifact(String definitionId) {
        StreamSession session = RowWeave.createStreamSession(definitionId);
        session.fromSource(source()).uid("source").sinkTo("sink", sink());
        return session.toPlanArtifact();
    }

    private static StreamJobProgress progress(int attempt, long records) {
        return new StreamJobProgress(attempt, 0L, 10L,
                io.rowweave.stream.api.StreamCdcPhase.NOT_APPLICABLE,
                attempt, records, records, records * 10L, records * 10L,
                0L, 0L, 0, 0L, 0L, 0, 0, 0L);
    }

    private static StreamSourceSpec source() {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false)));
        return StreamSourceSpec.exactOrdered(schema, "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
