package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamRescaleHandle;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** live rescale is a durable reconnectable operation. */
class StreamLiveRescaleAuthorityTest {

    @Test
    void publicApiExposesReconnectableLiveRescale() throws Exception {
        Method trigger = io.rowweave.StreamJobHandle.class.getMethod(
                "rescale", StreamCommandId.class, int.class);
        Method attach = io.rowweave.StreamJobClient.class.getMethod(
                "attachRescale", io.rowweave.stream.api.StreamJobId.class,
                StreamCommandId.class);

        assertEquals(StreamRescaleHandle.class, trigger.getReturnType());
        assertEquals(StreamRescaleHandle.class, attach.getReturnType());
        assertEquals(StreamOperationStatus.class,
                StreamRescaleHandle.class.getMethod("status").getReturnType());
        assertEquals(int.class, StreamRescaleHandle.class.getMethod(
                "targetParallelism").getReturnType());
    }

    @Test
    void intentAndCutoverSurviveAuthorityReplacement(@TempDir Path root) {
        FileStreamProductJobAuthority first =
                new FileStreamProductJobAuthority(root);
        StreamProductJobSnapshot running = running(first, "live-rescale");
        StreamCommandId command = StreamCommandId.of("rescale-command");

        StreamProductJobSnapshot accepted = first.acceptRescale(
                running.jobId(), command, 2, 7L);
        StreamProductRescaleOperation intent = accepted.rescale(command);
        assertEquals(StreamOperationStatus.ACCEPTED, intent.status());
        assertEquals(StreamProductRescaleOperation.Phase.INTENT_DURABLE,
                intent.phase());
        assertEquals(7L, intent.sourceGeneration());
        assertEquals(8L, intent.targetGeneration());
        assertEquals(2, intent.targetParallelism());
        assertEquals(accepted, first.acceptRescale(
                running.jobId(), command, 2, 7L));

        FileStreamProductJobAuthority replacement =
                new FileStreamProductJobAuthority(root);
        replacement.startRescale(running.jobId(), command);
        replacement.decideRescaleCutover(running.jobId(), command);
        StreamProductJobSnapshot completed = replacement.completeRescale(
                running.jobId(), command);
        assertEquals(StreamOperationStatus.SUCCEEDED,
                completed.rescale(command).status());
        assertEquals(StreamProductRescaleOperation.Phase.CUTOVER_COMPLETE,
                completed.rescale(command).phase());
        assertEquals(completed,
                new FileStreamProductJobAuthority(root).byJob(running.jobId()));
    }

    @Test
    void commandIdentityAndOperationSerializationFailClosed(
            @TempDir Path root) {
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        StreamProductJobSnapshot running = running(authority, "serialization");
        StreamCommandId command = StreamCommandId.of("rescale-command");
        authority.acceptRescale(running.jobId(), command, 2, 1L);

        StreamProductControlException rebound = assertThrows(
                StreamProductControlException.class,
                () -> authority.acceptRescale(
                        running.jobId(), command, 3, 1L));
        assertEquals(StreamProductControlException.Code.COMMAND_CONFLICT,
                rebound.code());
        StreamProductControlException checkpoint = assertThrows(
                StreamProductControlException.class,
                () -> authority.acceptCheckpoint(running.jobId(),
                        StreamCommandId.of("checkpoint-during-rescale")));
        assertEquals(StreamProductControlException.Code.INVALID_STATE,
                checkpoint.code());
        StreamProductControlException cancel = assertThrows(
                StreamProductControlException.class,
                () -> authority.acceptCancel(running.jobId(),
                        StreamCommandId.of("cancel-during-rescale")));
        assertEquals(StreamProductControlException.Code.INVALID_STATE,
                cancel.code());

        authority.startRescale(running.jobId(), command);
        StreamProductJobSnapshot failed = authority.failRescale(
                running.jobId(), command);
        assertEquals(StreamOperationStatus.FAILED,
                failed.rescale(command).status());
        assertEquals(StreamJobStatus.RUNNING, failed.status());
    }

    @Test
    void durableCutoverCannotRollBackToOldGeneration(@TempDir Path root) {
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        StreamProductJobSnapshot running = running(authority, "cutover-fence");
        StreamCommandId command = StreamCommandId.of("rescale-command");
        authority.acceptRescale(running.jobId(), command, 2, 4L);
        authority.startRescale(running.jobId(), command);
        authority.decideRescaleCutover(running.jobId(), command);

        StreamProductControlException rollback = assertThrows(
                StreamProductControlException.class,
                () -> authority.failRescale(running.jobId(), command));
        assertEquals(StreamProductControlException.Code.INVALID_STATE,
                rollback.code());
        assertEquals(StreamProductRescaleOperation.Phase.CUTOVER_DURABLE,
                authority.byJob(running.jobId()).rescale(command).phase());
    }

    private static StreamProductJobSnapshot running(
            FileStreamProductJobAuthority authority, String identity) {
        StreamProductJobSnapshot created = authority.publish(
                StreamSubmissionId.of(identity), artifact(identity));
        authority.transition(created.jobId(), StreamJobStatus.STARTING);
        return authority.transition(created.jobId(), StreamJobStatus.RUNNING);
    }

    private static PlanArtifact artifact(String identity) {
        StreamSession session = RowWeave.createStreamSession(identity);
        session.setParallelism(1);
        session.setMaxParallelism(4);
        session.fromSource(source()).uid("source").sinkTo("sink", sink());
        return session.toPlanArtifact();
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
