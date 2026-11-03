package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamJobClientLifecycleContract;
import io.rowweave.StreamSession;
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** in-process control reuses the durable product lifecycle. */
class InProcessStreamJobClientContractTest {

    @Test
    void submitAttachCheckpointAndCancelNeedNoControlListener(
            @TempDir Path authorityRoot) {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (InProcessStreamJobClient client =
                     new InProcessStreamJobClient(authorityRoot, runs)) {
            assertEquals(0, client.controlListenerCountForDiagnostics());
            StreamJobClientLifecycleContract.verify(
                    client, session(client), "in-process");
            assertEquals(1, runs.starts.get());
            assertEquals(1, runs.checkpoints.get());
            assertEquals(1, runs.cancels.get());
        }
    }

    private static StreamSession session(io.rowweave.StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "in-process-job", "in-process-job", client);
        session.fromSource(source()).uid("source").sinkTo("sink", sink());
        return session;
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

    private static final class RecordingRunFactory
            implements StreamProductRunFactory {
        private final AtomicInteger starts = new AtomicInteger();
        private final AtomicInteger cancels = new AtomicInteger();
        private final AtomicInteger checkpoints = new AtomicInteger();

        @Override
        public StreamProductRun create(StreamProductJobSnapshot snapshot) {
            return new StreamProductRun() {
                @Override public void start() { starts.incrementAndGet(); }
                @Override public void cancel() { cancels.incrementAndGet(); }
                @Override public void triggerCheckpoint(
                        StreamCommandId commandId, String checkpointId) {
                    checkpoints.incrementAndGet();
                }
                @Override public void close() { }
            };
        }
    }
}
