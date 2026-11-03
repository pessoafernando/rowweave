package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.mem.RowSerializer;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.FileCheckpointAuthority;
import io.rowweave.stream.runtime.FileStreamCheckpointCatalog;
import io.rowweave.stream.runtime.StreamSinkTransactionOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import io.rowweave.stream.runtime.TransactionStatus;
import io.rowweave.stream.runtime.TransactionalStreamSinkProvider;
import io.rowweave.stream.runtime.TransactionalStreamSinkWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Transactional visibility remains exactly-once across a live generation cut. */
class InProcessStreamTransactionalLiveRescaleTest {
    private static final String SOURCE_DIGEST = digest("source");
    private static final String SINK_DIGEST = digest("sink");

    @Test
    void liveRescaleCommitsOldGenerationBeforeNewGenerationWrites(
            @TempDir Path root) throws Exception {
        StreamSchema schema = schema();
        RowSerializer serializer = new RowSerializer(
                io.rowweave.portable.PortablePlanCompiler.toDataSchema(
                        schema.toPortableSchema()));
        RescalableSource source = new RescalableSource(
                serializer.serialize(row("before")),
                serializer.serialize(row("after")));
        RecordingTransactionalSink sink =
                new RecordingTransactionalSink(serializer);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap("source", source),
                Collections.emptyMap(),
                Collections.singletonMap("sink", sink),
                reference -> {
                    throw new UnsupportedOperationException(
                            "transactional rescale has no functions");
                }, Collections.emptyList())) {
            StreamJobHandle job = session(cluster, schema).submit(
                    StreamSubmissionId.of("transactional-live-rescale"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            source.awaitFirstRead();

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "transactional-scale-to-two"), 2)
                            .awaitCompletion(10_000L));
            assertTargetCutStartsFreshTransactions(root,
                    job.id().value());
            assertEquals(1, source.maximumActiveReaders(),
                    "old and target source generations must not overlap");
            awaitVisible(sink, 1);
            assertEquals(Collections.singletonList("before"),
                    sink.visibleValues());

            source.releaseSecond();
            source.awaitSecondRead();
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "transactional-post-rescale-cut"))
                            .awaitCompletion(10_000L));
            awaitVisible(sink, 2);
            assertEquals(java.util.Arrays.asList("before", "after"),
                    sink.visibleValues());
            assertEquals(2, sink.nonEmptyCommittedTransactions());
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("transactional-rescale-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    private static StreamSession session(
            InProcessStreamCluster cluster, StreamSchema schema) {
        StreamSession session = RowWeave.createStreamSession(
                "transactional-rescale", "transactional-rescale", cluster);
        session.setParallelism(1);
        session.setMaxParallelism(4);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                60_000L, 5_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                131_072L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.exactOrdered(
                        schema, "source", SOURCE_DIGEST, new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                digest("split"), "int32-v1",
                                new byte[] {2}))))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        "sink", SINK_DIGEST, new byte[] {3}));
        return session;
    }

    private static StreamSchema schema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("value", "value", "value",
                        DataType.StringType, false)));
    }

    private static DataRow row(String value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static void awaitVisible(
            RecordingTransactionalSink sink, int count) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (sink.visibleValues().size() < count
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(count, sink.visibleValues().size());
    }

    private static void assertTargetCutStartsFreshTransactions(
            Path root, String runId) {
        Path checkpoints = root.resolve("checkpoints");
        FileCheckpointAuthority authority = new FileCheckpointAuthority(
                checkpoints.resolve("authority"));
        FileStreamCheckpointCatalog catalog =
                new FileStreamCheckpointCatalog(checkpoints);
        CheckpointDecision target = null;
        for (CheckpointDecision decision : authority.decisionsForRun(runId)) {
            if ("attempt-2".equals(
                    decision.checkpointIdentity().attemptId())) {
                target = decision;
            }
        }
        if (target == null) {
            throw new AssertionError(
                    "live rescale target checkpoint was not published");
        }
        long inherited = catalog.resolve(target, authority).stream()
                .filter(member -> member.kind()
                        == StreamCheckpointMember.Kind.TRANSACTIONAL_SINK)
                .count();
        assertEquals(0L, inherited,
                "the target generation must open fresh sink transactions");
    }

    private static final class RescalableSource
            implements DurableStreamSourceProvider {
        private final byte[] first;
        private final byte[] second;
        private final Object monitor = new Object();
        private int reads;
        private int activeReaders;
        private int maximumActiveReaders;
        private boolean secondReleased;

        private RescalableSource(byte[] first, byte[] second) {
            this.first = first;
            this.second = second;
        }

        @Override public String providerKey() { return "source"; }
        @Override public String contractDigest() { return SOURCE_DIGEST; }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            synchronized (monitor) {
                activeReaders++;
                maximumActiveReaders = Math.max(
                        maximumActiveReaders, activeReaders);
            }
            return new StreamSourceReader() {
                private int next = request.hasRestoredPosition()
                        ? ByteBuffer.wrap(request.restoredPositionFrame())
                        .getInt() : 0;
                private boolean canceled;
                private boolean closed;
                private boolean woken;

                @Override public StreamSourceRecord next() throws Exception {
                    if (next == 0) {
                        next = 1;
                        observed();
                        return StreamSourceRecord.of(first, position(next));
                    }
                    if (next == 1) {
                        synchronized (monitor) {
                            while (!secondReleased && !canceled && !woken) {
                                monitor.wait();
                            }
                            if (woken) {
                                woken = false;
                                return null;
                            }
                        }
                        if (canceled) return null;
                        next = 2;
                        observed();
                        return StreamSourceRecord.of(second, position(next));
                    }
                    synchronized (monitor) {
                        while (!canceled && !woken) monitor.wait();
                        woken = false;
                    }
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }

                @Override public void wakeUp() {
                    synchronized (monitor) {
                        woken = true;
                        monitor.notifyAll();
                    }
                }
                @Override public void cancel() { cancelReader(); }
                @Override public void close() { cancelReader(); }

                private void cancelReader() {
                    synchronized (monitor) {
                        if (closed) return;
                        closed = true;
                        canceled = true;
                        activeReaders--;
                        monitor.notifyAll();
                    }
                }
            };
        }

        private void observed() {
            synchronized (monitor) {
                reads++;
                monitor.notifyAll();
            }
        }

        void releaseSecond() {
            synchronized (monitor) {
                secondReleased = true;
                monitor.notifyAll();
            }
        }

        void awaitFirstRead() throws Exception { awaitReads(1); }
        void awaitSecondRead() throws Exception { awaitReads(2); }

        int maximumActiveReaders() {
            synchronized (monitor) {
                return maximumActiveReaders;
            }
        }

        private void awaitReads(int expected) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            synchronized (monitor) {
                while (reads < expected) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0L) {
                        throw new AssertionError("source read timed out");
                    }
                    monitor.wait(Math.max(1L,
                            TimeUnit.NANOSECONDS.toMillis(remaining)));
                }
            }
        }
    }

    private static final class RecordingTransactionalSink
            implements TransactionalStreamSinkProvider {
        private final RowSerializer serializer;
        private final Map<String, List<byte[]>> prepared =
                new LinkedHashMap<>();
        private final Map<String, List<byte[]>> committed =
                new LinkedHashMap<>();

        private RecordingTransactionalSink(RowSerializer serializer) {
            this.serializer = serializer;
        }

        @Override public String providerKey() { return "sink"; }
        @Override public String contractDigest() { return SINK_DIGEST; }

        @Override public TransactionalStreamSinkWriter open(
                StreamSinkTransactionOpenRequest request) {
            List<byte[]> records = new ArrayList<>();
            return new TransactionalStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    records.add(recordFrame.clone());
                }

                @Override public byte[] prepare(String identity) {
                    synchronized (prepared) {
                        prepared.put(identity, copy(records));
                    }
                    return identity.getBytes(StandardCharsets.UTF_8);
                }

                @Override public TransactionStatus query(String identity) {
                    synchronized (prepared) {
                        return committed.containsKey(identity)
                                ? TransactionStatus.VISIBLE
                                : TransactionStatus.PREPARED;
                    }
                }

                @Override public void commit(
                        String identity, CheckpointDecision decision) {
                    synchronized (prepared) {
                        committed.putIfAbsent(identity,
                                copy(prepared.get(identity)));
                    }
                }

                @Override public void finalizeTransaction(String identity) { }

                @Override public void abort(String identity) {
                    synchronized (prepared) { prepared.remove(identity); }
                }

                @Override public void close() { }
            };
        }

        List<String> visibleValues() {
            synchronized (prepared) {
                List<String> values = new ArrayList<>();
                for (List<byte[]> transaction : committed.values()) {
                    for (byte[] frame : transaction) {
                        values.add(serializer.deserialize(frame).getString(0));
                    }
                }
                return values;
            }
        }

        int nonEmptyCommittedTransactions() {
            synchronized (prepared) {
                int count = 0;
                for (List<byte[]> records : committed.values()) {
                    if (!records.isEmpty()) count++;
                }
                return count;
            }
        }

        private static List<byte[]> copy(List<byte[]> values) {
            List<byte[]> copied = new ArrayList<>();
            if (values != null) {
                for (byte[] value : values) copied.add(value.clone());
            }
            return copied;
        }
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte current : bytes) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
