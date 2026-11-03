package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamSubmissionId;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Backend-neutral black-box recovery contract for every Stream product mode. */
public final class StreamJobClientRecoveryContract {
    private static final long WAIT_MILLIS = 5_000L;

    private StreamJobClientRecoveryContract() { }

    @FunctionalInterface
    public interface CheckedAction {
        void run() throws Exception;
    }

    public static void verifyExactCheckpointRestore(
            StreamJobClient client, StreamSession definition, String prefix,
            int parallelism, int maxParallelism,
            CheckedAction awaitInitialResult,
            CheckedAction prepareForRecovery,
            CheckedAction assertRecoveredResult) throws Exception {
        validate(client, definition, definition, prefix, parallelism,
                maxParallelism, awaitInitialResult, prepareForRecovery,
                assertRecoveredResult);
        PlanArtifact artifact = definition.toPlanArtifact();
        StreamJobHandle source = definition.submit(StreamSubmissionId.of(
                prefix + "-source"));
        assertEquals(StreamJobStatus.RUNNING,
                source.awaitRunning(WAIT_MILLIS));
        awaitInitialResult.run();

        StreamCommandId checkpointCommand = StreamCommandId.of(
                prefix + "-checkpoint");
        StreamCheckpointHandle checkpoint = source.triggerCheckpoint(
                checkpointCommand);
        assertEquals(StreamOperationStatus.SUCCEEDED,
                checkpoint.awaitCompletion(WAIT_MILLIS));
        assertEquals(checkpoint.checkpointId(), client.attachCheckpoint(
                source.id(), checkpointCommand).checkpointId());
        source.cancel(StreamCommandId.of(prefix + "-stop-source"))
                .awaitCompletion(WAIT_MILLIS);
        assertEquals(StreamJobStatus.CANCELED,
                source.awaitTermination(WAIT_MILLIS));

        prepareForRecovery.run();
        StreamSubmissionId targetSubmission = StreamSubmissionId.of(
                prefix + "-target");
        StreamRestoreRequest request = new StreamRestoreRequest(source.id(),
                StreamRecoveryPointId.checkpoint(checkpoint.checkpointId()),
                parallelism, maxParallelism);
        StreamJobHandle target = client.restore(
                targetSubmission, artifact, request);
        assertNotEquals(source.id(), target.id());
        assertEquals(target.id(), client.restore(
                targetSubmission, artifact, request).id());
        assertEquals(target.id(),
                client.attachSubmission(targetSubmission).id());
        assertEquals(StreamJobStatus.RUNNING,
                target.awaitRunning(WAIT_MILLIS));
        assertRecoveredResult.run();
        cancel(target, prefix + "-cancel-target");
    }

    public static String verifySavepointOfflineRescale(
            StreamJobClient client, StreamSession sourceDefinition,
            StreamSession targetDefinition, String prefix,
            int targetParallelism, int targetMaxParallelism,
            CheckedAction awaitInitialResult,
            CheckedAction prepareForRecovery,
            CheckedAction assertRecoveredResult) throws Exception {
        validate(client, sourceDefinition, targetDefinition, prefix,
                targetParallelism, targetMaxParallelism, awaitInitialResult,
                prepareForRecovery, assertRecoveredResult);
        StreamJobHandle source = sourceDefinition.submit(
                StreamSubmissionId.of(prefix + "-source"));
        assertEquals(StreamJobStatus.RUNNING,
                source.awaitRunning(WAIT_MILLIS));
        awaitInitialResult.run();

        StreamCommandId savepointCommand = StreamCommandId.of(
                prefix + "-savepoint");
        StreamSavepointRequest savepointRequest = new StreamSavepointRequest(
                "file-savepoint-v1", new byte[0]);
        StreamSavepointHandle savepoint = source.stopWithSavepoint(
                savepointCommand, savepointRequest);
        assertEquals(StreamOperationStatus.SUCCEEDED,
                savepoint.awaitCompletion(WAIT_MILLIS));
        assertEquals(savepoint.savepointId(), client.attachSavepoint(
                source.id(), savepointCommand).savepointId());
        assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                source.awaitTermination(WAIT_MILLIS));

        prepareForRecovery.run();
        PlanArtifact targetArtifact = targetDefinition.toPlanArtifact();
        StreamSubmissionId targetSubmission = StreamSubmissionId.of(
                prefix + "-target");
        StreamOfflineRescaleRequest request =
                new StreamOfflineRescaleRequest(source.id(),
                        StreamRecoveryPointId.savepoint(
                                savepoint.savepointId()),
                        targetParallelism, targetMaxParallelism);
        StreamJobHandle target = client.offlineRescale(
                targetSubmission, targetArtifact, request);
        assertNotEquals(source.id(), target.id());
        assertEquals(target.id(), client.offlineRescale(
                targetSubmission, targetArtifact, request).id());
        assertEquals(target.id(),
                client.attachSubmission(targetSubmission).id());
        assertEquals(StreamJobStatus.RUNNING,
                target.awaitRunning(WAIT_MILLIS));
        assertRecoveredResult.run();

        StreamCommandId checkpointCommand = StreamCommandId.of(
                prefix + "-target-checkpoint");
        StreamCheckpointHandle checkpoint = target.triggerCheckpoint(
                checkpointCommand);
        assertEquals(StreamOperationStatus.SUCCEEDED,
                checkpoint.awaitCompletion(WAIT_MILLIS));
        assertEquals(checkpoint.checkpointId(), client.attachCheckpoint(
                target.id(), checkpointCommand).checkpointId());
        cancel(target, prefix + "-cancel-target");
        return checkpoint.checkpointId();
    }

    private static void cancel(StreamJobHandle job, String commandId) {
        assertEquals(StreamOperationStatus.SUCCEEDED,
                job.cancel(StreamCommandId.of(commandId))
                        .awaitCompletion(WAIT_MILLIS));
        assertEquals(StreamJobStatus.CANCELED,
                job.awaitTermination(WAIT_MILLIS));
    }

    private static void validate(
            StreamJobClient client, StreamSession sourceDefinition,
            StreamSession targetDefinition, String prefix,
            int targetParallelism, int targetMaxParallelism,
            CheckedAction awaitInitialResult,
            CheckedAction prepareForRecovery,
            CheckedAction assertRecoveredResult) {
        if (client == null || sourceDefinition == null
                || targetDefinition == null || prefix == null
                || prefix.isEmpty() || targetParallelism < 1
                || targetMaxParallelism < targetParallelism
                || awaitInitialResult == null || prepareForRecovery == null
                || assertRecoveredResult == null) {
            throw new IllegalArgumentException(
                    "Stream recovery contract inputs are required");
        }
    }

}
