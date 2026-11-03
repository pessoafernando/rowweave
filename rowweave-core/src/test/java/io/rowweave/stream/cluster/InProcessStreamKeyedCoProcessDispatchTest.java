package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.StreamBroadcastProcessContext;
import io.rowweave.StreamCollector;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamKeyedCoProcessFunction;
import io.rowweave.StreamKeyedBroadcastProcessContext;
import io.rowweave.StreamKeyedBroadcastProcessFunction;
import io.rowweave.StreamProcessContext;
import io.rowweave.StreamSession;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import io.rowweave.stream.runtime.StreamSourceEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public co-process runs through same-JVM cluster roles. */
class InProcessStreamKeyedCoProcessDispatchTest {

    @Test
    void liveRescaleReplicatesBroadcastStateToEveryTargetTask(
            @TempDir Path root) throws Exception {
        io.rowweave.artifact.PlanArtifact definition =
                StreamProductKeyedCoProcessPlanAdmissionTest
                        .broadcastSession().toPlanArtifact();
        RescalableLeftSource left = new RescalableLeftSource(
                serializer(definition, "left").serialize(
                        row(7L, "before")),
                serializer(definition, "left").serialize(
                        row(7L, "after")));
        RestorableBroadcastSource right = new RestorableBroadcastSource(
                serializer(definition, "right").serialize(
                        row(7L, "approved")));
        MultiRecordingSink sink = new MultiRecordingSink();
        Map<String, DurableStreamSourceProvider> sources =
                new LinkedHashMap<>();
        sources.put(left.providerKey(), left);
        sources.put(right.providerKey(), right);
        Map<String, EagerStreamSinkProvider> sinks = new LinkedHashMap<>();
        sinks.put(sink.providerKey(), sink);

        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, sources, sinks, ignored -> new BroadcastFunction(),
                Arrays.asList(digest("deployment-set")))) {
            StreamSession session =
                    StreamProductKeyedCoProcessPlanAdmissionTest
                            .broadcastSession(cluster, 2);
            sink.serializer = serializer(session.toPlanArtifact(),
                    "co-process");
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("broadcast-live-rescale"));

            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            sink.awaitCount(1);
            assertEquals(Collections.singletonList("approved:before"),
                    sink.values());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "broadcast-live-scale-to-three"), 3)
                            .awaitCompletion(10_000L));
            left.releaseAfterRescale();
            sink.awaitCount(2);
            assertEquals(Arrays.asList(
                    "approved:before", "approved:after"), sink.values());
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("broadcast-live-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void immutableBroadcastSnapshotFansOutBeforeParallelKeyedReads(
            @TempDir Path root) throws Exception {
        io.rowweave.artifact.PlanArtifact definition =
                StreamProductKeyedCoProcessPlanAdmissionTest
                        .broadcastSession().toPlanArtifact();
        FiniteSource left = new FiniteSource("left-source-v1",
                digest("left-contract"), serializer(definition, "left")
                .serialize(row(7L, "gallery")));
        BoundedSource right = new BoundedSource("right-source-v1",
                digest("right-contract"), serializer(definition, "right")
                .serialize(row(7L, "approved")));
        RecordingSink sink = new RecordingSink();
        Map<String, DurableStreamSourceProvider> sources =
                new LinkedHashMap<>();
        sources.put(left.providerKey(), left);
        sources.put(right.providerKey(), right);
        Map<String, EagerStreamSinkProvider> sinks = new LinkedHashMap<>();
        sinks.put(sink.providerKey(), sink);

        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, sources, sinks,
                ignored -> new BroadcastFunction(),
                Arrays.asList(digest("deployment-set")))) {
            StreamSession session =
                    StreamProductKeyedCoProcessPlanAdmissionTest
                            .broadcastSession(cluster, 2);
            sink.serializer = serializer(session.toPlanArtifact(),
                    "co-process");
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("broadcast-submit"));

            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals("approved:gallery", sink.value.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "broadcast-checkpoint"))
                            .awaitCompletion(5_000L));
            assertEquals(1L, job.progress().completedCheckpoints());
            job.cancel(StreamCommandId.of("broadcast-cancel"))
                    .awaitCompletion(5_000L);
            assertEquals(StreamJobStatus.CANCELED,
                    job.awaitTermination(5_000L));
        }
    }

    @Test
    void twoSourcesShareStateCheckpointAndWriteTheSink(@TempDir Path root)
            throws Exception {
        RowSerializer leftSerializer = serializer(
                StreamProductKeyedCoProcessPlanAdmissionTest.session()
                        .toPlanArtifact(), "left");
        RowSerializer rightSerializer = serializer(
                StreamProductKeyedCoProcessPlanAdmissionTest.session()
                        .toPlanArtifact(), "right");
        FiniteSource left = new FiniteSource("left-source-v1",
                digest("left-contract"), leftSerializer.serialize(
                row(7L, "L")));
        FiniteSource right = new FiniteSource("right-source-v1",
                digest("right-contract"), rightSerializer.serialize(
                row(7L, "R")));
        RecordingSink sink = new RecordingSink();
        Map<String, DurableStreamSourceProvider> sources =
                new LinkedHashMap<>();
        sources.put(left.providerKey(), left);
        sources.put(right.providerKey(), right);
        Map<String, EagerStreamSinkProvider> sinks = new LinkedHashMap<>();
        sinks.put(sink.providerKey(), sink);
        StreamKeyedCoProcessFunction function = new SharedStateFunction();

        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, sources, sinks, ignored -> function,
                Arrays.asList(digest("deployment-set")))) {
            StreamSession session =
                    StreamProductKeyedCoProcessPlanAdmissionTest
                            .session(cluster);
            sink.serializer = serializer(session.toPlanArtifact(),
                    "co-process");
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("co-process-submit"));

            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals("L:R", sink.value.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "co-process-checkpoint"))
                            .awaitCompletion(5_000L));
            assertEquals(1L, job.progress().completedCheckpoints());
            job.cancel(StreamCommandId.of("co-process-cancel"))
                    .awaitCompletion(5_000L);
            assertEquals(StreamJobStatus.CANCELED,
                    job.awaitTermination(5_000L));
        }
    }

    private static final class SharedStateFunction
            implements StreamKeyedCoProcessFunction {
        @Override public void processLeft(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            context.mapState("shared").put(bytes("left"),
                    bytes(value.getString(1)));
            emitIfComplete(value.getLong(0), context, output);
        }

        @Override public void processRight(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            context.mapState("shared").put(bytes("right"),
                    bytes(value.getString(1)));
            emitIfComplete(value.getLong(0), context, output);
        }

        private void emitIfComplete(long key, StreamProcessContext context,
                StreamCollector output) {
            byte[] left = context.mapState("shared").get(bytes("left"));
            byte[] right = context.mapState("shared").get(bytes("right"));
            if (left == null || right == null) return;
            DataRow result = new DataRow(2);
            result.set(0, key);
            result.set(1, text(left) + ':' + text(right));
            output.collect(result);
        }
    }

    private static final class BroadcastFunction
            implements StreamKeyedBroadcastProcessFunction {
        @Override public void processKeyed(DataRow value,
                StreamKeyedBroadcastProcessContext context,
                StreamCollector output) {
            DataRow result = new DataRow(2);
            result.set(0, value.getLong(0));
            byte[] rule = context.broadcastState("rules").get(
                    ByteBuffer.allocate(Long.BYTES)
                            .putLong(value.getLong(0)).array());
            result.set(1, (rule == null ? "missing" : text(rule))
                    + ':' + value.getString(1));
            output.collect(result);
        }

        @Override public void processBroadcast(DataRow value,
                StreamBroadcastProcessContext context,
                StreamCollector output) {
            context.broadcastState("rules").put(
                    ByteBuffer.allocate(Long.BYTES)
                            .putLong(value.getLong(0)).array(),
                    bytes(value.getString(1)));
        }
    }

    private static final class FiniteSource
            implements DurableStreamSourceProvider {
        private final String providerKey;
        private final String contractDigest;
        private final byte[] frame;

        private FiniteSource(String providerKey, String contractDigest,
                byte[] frame) {
            this.providerKey = providerKey;
            this.contractDigest = contractDigest;
            this.frame = frame;
        }

        @Override public String providerKey() { return providerKey; }

        @Override public String contractDigest() { return contractDigest; }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private boolean emitted;

                @Override public StreamSourceRecord next() throws Exception {
                    if (!emitted) {
                        emitted = true;
                        return StreamSourceRecord.of(frame, position(1));
                    }
                    wakeups.acquire();
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position(emitted ? 1 : 0);
                }

                @Override public void wakeUp() { wakeups.release(); }

                @Override public void cancel() { wakeups.release(); }

                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class RecordingSink
            implements EagerStreamSinkProvider {
        private final CountDownLatch received = new CountDownLatch(1);
        private final AtomicReference<String> value = new AtomicReference<>();
        private volatile RowSerializer serializer;

        @Override public String providerKey() { return "sink-v1"; }

        @Override public String contractDigest() {
            return digest("sink-contract");
        }

        @Override public EagerStreamSinkWriter open(
                StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    DataRow row = serializer.deserialize(recordFrame);
                    value.set(row.getString(1));
                    received.countDown();
                }

                @Override public void cancel() { }

                @Override public void close() { }
            };
        }
    }

    private static final class RescalableLeftSource
            implements DurableStreamSourceProvider {
        private final byte[] first;
        private final byte[] second;
        private final Object monitor = new Object();
        private boolean released;

        private RescalableLeftSource(byte[] first, byte[] second) {
            this.first = first;
            this.second = second;
        }

        @Override public String providerKey() { return "left-source-v1"; }

        @Override public String contractDigest() {
            return digest("left-contract");
        }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                private int next = request.hasRestoredPosition()
                        ? ByteBuffer.wrap(request.restoredPositionFrame())
                                .getInt() : 0;
                private boolean canceled;

                @Override public StreamSourceRecord next() throws Exception {
                    if (next == 0) {
                        next = 1;
                        return StreamSourceRecord.of(first, position(next));
                    }
                    if (next == 1) {
                        synchronized (monitor) {
                            while (!released && !canceled) monitor.wait();
                        }
                        if (canceled) return null;
                        next = 2;
                        return StreamSourceRecord.of(second, position(next));
                    }
                    synchronized (monitor) {
                        while (!canceled) monitor.wait();
                    }
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }

                @Override public void wakeUp() {
                    synchronized (monitor) {
                        canceled = true;
                        monitor.notifyAll();
                    }
                }

                @Override public void cancel() { wakeUp(); }

                @Override public void close() { wakeUp(); }
            };
        }

        void releaseAfterRescale() {
            synchronized (monitor) {
                released = true;
                monitor.notifyAll();
            }
        }
    }

    private static final class RestorableBroadcastSource
            implements DurableStreamSourceProvider {
        private final byte[] frame;

        private RestorableBroadcastSource(byte[] frame) {
            this.frame = frame;
        }

        @Override public String providerKey() { return "right-source-v1"; }

        @Override public String contractDigest() {
            return digest("right-contract");
        }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                private int next = request.hasRestoredPosition()
                        ? ByteBuffer.wrap(request.restoredPositionFrame())
                                .getInt() : 0;

                @Override public StreamSourceRecord next() {
                    throw new AssertionError(
                            "bounded source must use nextEvent");
                }

                @Override public StreamSourceEvent nextEvent() {
                    if (next++ == 0) {
                        return StreamSourceEvent.record(
                                StreamSourceRecord.of(frame, position(1)));
                    }
                    return StreamSourceEvent.endOfInput();
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position(Math.min(next, 1));
                }

                @Override public void cancel() { }

                @Override public void close() { }
            };
        }
    }

    private static final class MultiRecordingSink
            implements EagerStreamSinkProvider {
        private final CopyOnWriteArrayList<String> values =
                new CopyOnWriteArrayList<>();
        private volatile RowSerializer serializer;

        @Override public String providerKey() { return "sink-v1"; }

        @Override public String contractDigest() {
            return digest("sink-contract");
        }

        @Override public EagerStreamSinkWriter open(
                StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    values.add(serializer.deserialize(recordFrame)
                            .getString(1));
                    synchronized (values) { values.notifyAll(); }
                }

                @Override public void cancel() { }

                @Override public void close() { }
            };
        }

        void awaitCount(int count) throws InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            synchronized (values) {
                while (values.size() < count) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0L) {
                        throw new AssertionError(
                                "timed out waiting for broadcast output: "
                                        + values);
                    }
                    values.wait(Math.max(1L,
                            TimeUnit.NANOSECONDS.toMillis(remaining)));
                }
            }
        }

        List<String> values() { return Arrays.asList(values.toArray(
                new String[values.size()])); }
    }

    private static final class BoundedSource
            implements DurableStreamSourceProvider {
        private final String providerKey;
        private final String contractDigest;
        private final byte[] frame;

        private BoundedSource(String providerKey, String contractDigest,
                byte[] frame) {
            this.providerKey = providerKey;
            this.contractDigest = contractDigest;
            this.frame = frame;
        }

        @Override public String providerKey() { return providerKey; }

        @Override public String contractDigest() { return contractDigest; }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                private int next;

                @Override public StreamSourceRecord next() {
                    throw new AssertionError(
                            "bounded source must use nextEvent");
                }

                @Override public StreamSourceEvent nextEvent() {
                    if (next++ == 0) {
                        return StreamSourceEvent.record(
                                StreamSourceRecord.of(frame, position(1)));
                    }
                    return StreamSourceEvent.endOfInput();
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position(Math.min(next, 1));
                }

                @Override public void cancel() { }

                @Override public void close() { }
            };
        }
    }

    private static RowSerializer serializer(
            io.rowweave.artifact.PlanArtifact artifact, String uid) {
        io.rowweave.stream.contract.PortableStreamJob job =
                io.rowweave.stream.contract.PortableStreamJobArtifact
                        .decodeValidated(artifact);
        for (io.rowweave.stream.contract.PortableStreamNode node
                : job.plan().nodes()) {
            if (node.operatorUid().equals(uid)) {
                return new RowSerializer(PortablePlanCompiler.toDataSchema(
                        node.outputSchema()));
            }
        }
        throw new AssertionError("missing node " + uid);
    }

    private static DataRow row(long key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static String digest(String value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(hash.length * 2);
            for (byte current : hash) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
