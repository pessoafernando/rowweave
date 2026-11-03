package io.rowweave;

import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSubmissionId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Backend-neutral black-box contract for every Stream product client mode. */
public final class StreamJobClientLifecycleContract {
    private StreamJobClientLifecycleContract() { }

    public static void verify(
            StreamJobClient client, StreamSession definition, String prefix) {
        if (client == null || definition == null || prefix == null
                || prefix.isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream lifecycle contract inputs are required");
        }
        StreamControlException missingJob = assertThrows(
                StreamControlException.class,
                () -> client.attach(StreamJobId.of(
                        prefix + "-contract-missing-job")));
        assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                missingJob.code());
        StreamSubmissionId submission = StreamSubmissionId.of(
                prefix + "-contract-submission");
        StreamJobHandle submitted = definition.submit(submission);
        assertEquals(StreamJobStatus.RUNNING,
                submitted.awaitRunning(5_000L));
        assertEquals(submitted.id(), definition.submit(submission).id());
        assertEquals(submitted.id(),
                client.attachSubmission(submission).id());
        assertEquals(submitted.id(), client.attach(submitted.id()).id());

        StreamControlException timeout = assertThrows(
                StreamControlException.class,
                () -> submitted.awaitTermination(10L));
        assertEquals(StreamControlException.Code.WAIT_TIMEOUT,
                timeout.code());
        assertEquals(StreamJobStatus.RUNNING, submitted.status());

        StreamCommandId checkpointCommand = StreamCommandId.of(
                prefix + "-contract-checkpoint");
        StreamCheckpointHandle checkpoint = submitted.triggerCheckpoint(
                checkpointCommand);
        assertEquals(StreamOperationStatus.SUCCEEDED,
                checkpoint.awaitCompletion(5_000L));
        assertEquals(checkpoint.checkpointId(),
                client.attachCheckpoint(
                        submitted.id(), checkpointCommand).checkpointId());
        assertEquals(checkpoint.checkpointId(), submitted.triggerCheckpoint(
                checkpointCommand).checkpointId());
        assertEquals(1L, submitted.progress().completedCheckpoints());
        StreamControlException commandConflict = assertThrows(
                StreamControlException.class,
                () -> submitted.cancel(checkpointCommand));
        assertEquals(StreamControlException.Code.COMMAND_CONFLICT,
                commandConflict.code());
        StreamControlException savepointConflict = assertThrows(
                StreamControlException.class,
                () -> submitted.stopWithSavepoint(checkpointCommand,
                        new StreamSavepointRequest(
                                "file-savepoint-v1", new byte[0])));
        assertEquals(StreamControlException.Code.COMMAND_CONFLICT,
                savepointConflict.code());
        assertEquals(StreamJobStatus.RUNNING, submitted.status());

        StreamCommandId cancelCommand = StreamCommandId.of(
                prefix + "-contract-cancel");
        StreamCommandHandle cancel = submitted.cancel(cancelCommand);
        assertEquals(StreamOperationStatus.SUCCEEDED,
                cancel.awaitCompletion(5_000L));
        assertEquals(StreamOperationStatus.SUCCEEDED,
                client.attachCancel(
                        submitted.id(), cancelCommand).status());
        assertEquals(StreamJobStatus.CANCELED,
                submitted.awaitTermination(5_000L));
    }

}
