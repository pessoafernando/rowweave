package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamRescaleHandle;
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

/** Public control path for checkpoint-driven live rescale. */
class StreamLiveRescaleControlTest {

    @Test
    void inProcessClientTriggersAndReattachesSameDurableCommand(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("live-control-submit"));
            StreamCommandId command = StreamCommandId.of("scale-to-two");

            StreamRescaleHandle triggered = job.rescale(command, 2);
            StreamRescaleHandle attached = client.attachRescale(
                    job.id(), command);

            assertEquals(triggered.rescaleId(), attached.rescaleId());
            assertEquals(2, attached.targetParallelism());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    attached.awaitCompletion(1_000L));
            assertEquals(1, runs.rescales.get());
            assertEquals(1L, runs.sourceGeneration);
            assertEquals(2L, runs.targetGeneration);
        }
    }

    @Test
    void durableRunCutoverRecoversTargetGenerationBeforeReply(
            @TempDir Path root) {
        AtomicInteger creates = new AtomicInteger();
        StreamProductRunFactory factory = snapshot -> {
            int invocation = creates.incrementAndGet();
            return new StreamProductRun() {
                @Override public void start() { }
                @Override public void cancel() { }
                @Override public long assignmentGeneration() {
                    return invocation == 1 ? 1L : 2L;
                }
                @Override public StreamProductRun liveRescale(
                        StreamCommandId commandId, String rescaleId,
                        int targetParallelism,
                        long expectedSourceGeneration,
                        long expectedTargetGeneration) {
                    throw new StreamLiveRescaleCutoverException(
                            new IllegalStateException("lost launch ACK"));
                }
                @Override public void close() { }
            };
        };
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, factory)) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("live-cutover-recovery"));

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of("scale-recovery"), 2)
                            .awaitCompletion(1_000L));
            assertEquals(2, creates.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    client.attachRescale(job.id(),
                            StreamCommandId.of("scale-recovery")).status());
        }
    }

    @Test
    void replacementMasterResumesCheckpointingRescale(
            @TempDir Path root) {
        io.rowweave.stream.api.StreamJobId jobId;
        try (StreamJobClient first = new InProcessStreamJobClient(
                root, new RecordingRunFactory())) {
            StreamJobHandle job = session(first).submit(
                    StreamSubmissionId.of("replacement-checkpointing"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(1_000L));
            jobId = job.id();
        }
        StreamCommandId command = StreamCommandId.of(
                "replacement-checkpointing-rescale");
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        authority.acceptRescale(jobId, command, 2, 1L);
        authority.startRescale(jobId, command);

        RecordingRunFactory replacementRuns = new RecordingRunFactory();
        try (StreamJobClient replacement = new InProcessStreamJobClient(
                root, replacementRuns)) {
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    replacement.attachRescale(jobId, command)
                            .awaitCompletion(5_000L));
            assertEquals(1, replacementRuns.rescales.get());
            assertEquals(1L, replacementRuns.sourceGeneration);
            assertEquals(2L, replacementRuns.targetGeneration);
        }
    }

    @Test
    void replacementMasterCompletesDurableCutoverWithoutReplayingOldCut(
            @TempDir Path root) {
        io.rowweave.stream.api.StreamJobId jobId;
        try (StreamJobClient first = new InProcessStreamJobClient(
                root, new RecordingRunFactory())) {
            StreamJobHandle job = session(first).submit(
                    StreamSubmissionId.of("replacement-cutover"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(1_000L));
            jobId = job.id();
        }
        StreamCommandId command = StreamCommandId.of(
                "replacement-cutover-rescale");
        FileStreamProductJobAuthority authority =
                new FileStreamProductJobAuthority(root);
        authority.acceptRescale(jobId, command, 2, 1L);
        authority.startRescale(jobId, command);
        authority.decideRescaleCutover(jobId, command);
        AtomicInteger replayedCuts = new AtomicInteger();
        StreamProductRunFactory replacementRuns = snapshot ->
                new StreamProductRun() {
                    @Override public void start() { }
                    @Override public void cancel() { }
                    @Override public long assignmentGeneration() {
                        return 2L;
                    }
                    @Override public StreamProductRun liveRescale(
                            StreamCommandId commandId, String rescaleId,
                            int targetParallelism,
                            long expectedSourceGeneration,
                            long expectedTargetGeneration) {
                        replayedCuts.incrementAndGet();
                        return this;
                    }
                    @Override public void close() { }
                };
        try (StreamJobClient replacement = new InProcessStreamJobClient(
                root, replacementRuns)) {
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    replacement.attachRescale(jobId, command)
                            .awaitCompletion(5_000L));
            assertEquals(0, replayedCuts.get());
        }
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "live-control", "Live control", client);
        session.setParallelism(1);
        session.setMaxParallelism(4);
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false)));
        StreamSourceSpec source = StreamSourceSpec.exactOrdered(
                schema, "source-v1", sha256("source-contract"),
                new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(sha256("split"),
                                "sequence-v1", new byte[] {2})));
        session.fromSource(source).uid("source").sinkTo("sink",
                StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", sha256("sink-contract"),
                        new byte[] {3}));
        return session;
    }

    private static final class RecordingRunFactory
            implements StreamProductRunFactory {
        private final AtomicInteger rescales = new AtomicInteger();
        private volatile long sourceGeneration;
        private volatile long targetGeneration;

        @Override public StreamProductRun create(
                StreamProductJobSnapshot snapshot) {
            return new StreamProductRun() {
                @Override public void start() { }
                @Override public void cancel() { }
                @Override public long assignmentGeneration() { return 1L; }
                @Override public StreamProductRun liveRescale(
                        StreamCommandId commandId, String rescaleId,
                        int targetParallelism, long expectedSourceGeneration,
                        long expectedTargetGeneration) {
                    rescales.incrementAndGet();
                    sourceGeneration = expectedSourceGeneration;
                    targetGeneration = expectedTargetGeneration;
                    return this;
                }
                @Override public void close() { }
            };
        }
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
