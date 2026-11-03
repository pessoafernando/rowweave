package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** submission IDs and reconnectable job handles are public contracts. */
class StreamPublicJobClientContractTest {

    @Test
    void sessionCompilesWholeJobAndSubmitsWithCallerStableIdentity() {
        RecordingClient client = new RecordingClient();
        StreamSession session = RowWeave.createStreamSession(
                "submitted-stream", "Submitted stream", client);
        session.fromSource(source()).uid("source").sinkTo("sink", sink());
        StreamSubmissionId submissionId = StreamSubmissionId.of("submission-001");

        StreamJobHandle handle = session.submit(submissionId);

        assertSame(client.handle, handle);
        assertEquals(submissionId, client.submissionId);
        assertEquals(PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1,
                client.artifact.payloadFormat());
        assertEquals(client.artifact.artifactId(), handle.artifactId());
        assertEquals(StreamJobStatus.SUBMITTED, handle.status());
        assertEquals(StreamJobProgress.empty(), handle.progress());
    }

    @Test
    void sessionWithoutClientFailsBeforeAnyExecutionSideEffect() {
        StreamSession session = RowWeave.createStreamSession("no-client");
        session.fromSource(source()).uid("source").sinkTo("sink", sink());
        StreamApiException failure = assertThrows(StreamApiException.class,
                () -> session.submit(StreamSubmissionId.of("submission-002")));
        assertEquals(StreamApiException.Code.CLIENT_REQUIRED, failure.code());
    }

    @Test
    void stableIdentityValuesRejectBlankAndRemainValueObjects() {
        assertEquals(StreamSubmissionId.of("same"), StreamSubmissionId.of("same"));
        assertEquals(StreamJobId.of("job-1"), StreamJobId.of("job-1"));
        assertEquals(StreamCommandId.of("command-1"), StreamCommandId.of("command-1"));
        assertThrows(IllegalArgumentException.class, () -> StreamSubmissionId.of(""));
        assertThrows(IllegalArgumentException.class, () -> StreamJobId.of(" "));
        assertThrows(IllegalArgumentException.class, () -> StreamCommandId.of(null));
    }

    @Test
    void lifecycleStatusAndRecoveryRequestsAreStableAndBounded() {
        assertEquals(1, StreamJobStatus.SUBMITTED.wireId());
        assertEquals(5, StreamJobStatus.STOPPING.wireId());
        assertEquals(8, StreamJobStatus.STOPPED_WITH_SAVEPOINT.wireId());
        assertEquals(false, StreamJobStatus.RUNNING.terminal());
        assertEquals(true, StreamJobStatus.FAILED.terminal());

        StreamRecoveryPointId point = StreamRecoveryPointId.savepoint("savepoint-1");
        StreamRestoreRequest restore = new StreamRestoreRequest(
                StreamJobId.of("old-job"), point, 4, 128);
        StreamOfflineRescaleRequest rescale = new StreamOfflineRescaleRequest(
                StreamJobId.of("old-job"), point, 8, 128);
        assertEquals(point, restore.recoveryPointId());
        assertEquals(4, restore.targetParallelism());
        assertEquals(8, rescale.targetParallelism());
        assertThrows(IllegalArgumentException.class,
                () -> new StreamRestoreRequest(StreamJobId.of("old-job"),
                        point, 129, 128));
    }

    @Test
    void progressViewContainsOnlyBoundedAggregateCounters() {
        StreamJobProgress progress = new StreamJobProgress(
                2, 7L, 123_456L, StreamCdcPhase.LOG,
                1, 1_000L, 900L, 8_000L, 7_000L, 12L, 4_096L,
                20, 32_768L, 65_536L, 40, 8, 5_000L);
        assertEquals(2, progress.attempt());
        assertEquals(7L, progress.completedCheckpoints());
        assertEquals(123_456L, progress.operatorWatermark());
        assertEquals(StreamCdcPhase.LOG, progress.cdcPhase());
        assertEquals(1_000L, progress.recordsRead());
        assertEquals(900L, progress.recordsWritten());
        assertEquals(12L, progress.backlogEvents());
        assertEquals(4_096L, progress.backlogBytes());
        assertEquals(65_536L, progress.managedStateBytes());
        assertEquals(40, progress.timerHighWaterCount());
        assertEquals(8, progress.windowHighWaterCount());
        assertThrows(IllegalArgumentException.class,
                () -> new StreamJobProgress(0, 0L, Long.MIN_VALUE,
                        StreamCdcPhase.NOT_APPLICABLE, 0, -1L, 0L, 0L, 0L,
                        0, 0L, 0L, 0, 0, 0L));
    }

    @Test
    void operationalEvidenceCarriesFencedIdentityCoverageAndProgress() {
        StreamJobProgress attempt = new StreamJobProgress(
                3, 1L, 50L, StreamCdcPhase.LOG, 2,
                10L, 8L, 100L, 80L, 2L, 20L,
                4, 40L, 60L, 6, 2, 7L);
        StreamJobProgress lifetime = new StreamJobProgress(
                3, 5L, 50L, StreamCdcPhase.LOG, 2,
                110L, 108L, 1_100L, 1_080L, 2L, 20L,
                8, 80L, 120L, 12, 4, 17L);
        StreamJobEvidence evidence = new StreamJobEvidence(
                StreamJobId.of("job-evidence"), "run-1", "attempt-3",
                3, 9L, 4L, 7L,
                "stream-checkpoint:" + repeat('a', 64), 5L,
                123L, 4_096L, attempt, lifetime,
                6, 6, 1_000L);

        assertTrue(evidence.available());
        assertTrue(evidence.complete());
        assertEquals("run-1", evidence.runId());
        assertEquals("attempt-3", evidence.attemptId());
        assertEquals(5L, evidence.latestCompletedCheckpointSequence());
        assertEquals(123L, evidence.latestCheckpointDurationMillis());
        assertEquals(attempt, evidence.attemptProgress());
        assertEquals(lifetime, evidence.lifetimeProgress());

        StreamJobEvidence unavailable = StreamJobEvidence.unavailable(
                StreamJobId.of("job-pending"), StreamJobProgress.empty(),
                2_000L);
        assertFalse(unavailable.available());
        assertFalse(unavailable.complete());
        assertThrows(IllegalArgumentException.class,
                () -> new StreamJobEvidence(StreamJobId.of("job-evidence"),
                        "run-1", "attempt-3", 3, 9L, 4L, 7L,
                        null, 1L, 0L, 0L, attempt, lifetime,
                        6, 7, 1_000L));
    }

    private static String repeat(char value, int count) {
        StringBuilder result = new StringBuilder(count);
        for (int index = 0; index < count; index++) result.append(value);
        return result.toString();
    }

    private static final class RecordingClient implements StreamJobClient {
        private final Handle handle = new Handle();
        private StreamSubmissionId submissionId;
        private PlanArtifact artifact;

        @Override
        public StreamJobHandle submit(StreamSubmissionId id, PlanArtifact plan) {
            submissionId = id;
            artifact = plan;
            handle.artifactId = plan.artifactId();
            return handle;
        }

        @Override public StreamJobHandle attachSubmission(StreamSubmissionId id) {
            return handle;
        }

        @Override public StreamJobHandle attach(StreamJobId id) { return handle; }
        @Override public StreamCommandHandle attachCancel(
                StreamJobId jobId, StreamCommandId commandId) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamCheckpointHandle attachCheckpoint(
                StreamJobId jobId, StreamCommandId commandId) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamSavepointHandle attachSavepoint(
                StreamJobId jobId, StreamCommandId commandId) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamJobHandle restore(StreamSubmissionId id,
                PlanArtifact artifact, StreamRestoreRequest request) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamJobHandle offlineRescale(StreamSubmissionId id,
                PlanArtifact artifact, StreamOfflineRescaleRequest request) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class Handle implements StreamJobHandle {
        private String artifactId;
        @Override public StreamJobId id() { return StreamJobId.of("job-1"); }
        @Override public String artifactId() { return artifactId; }
        @Override public StreamJobStatus status() { return StreamJobStatus.SUBMITTED; }
        @Override public StreamJobProgress progress() { return StreamJobProgress.empty(); }
        @Override public StreamCommandHandle cancel(StreamCommandId id) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamCheckpointHandle triggerCheckpoint(StreamCommandId id) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamSavepointHandle stopWithSavepoint(
                StreamCommandId id, StreamSavepointRequest request) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamJobStatus awaitRunning(long timeoutMillis) {
            return status();
        }
        @Override public StreamJobStatus awaitTermination(long timeoutMillis) {
            return status();
        }
    }

    private static StreamSourceSpec source() {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.StringType, false)));
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
            for (byte current : digest) result.append(String.format("%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
