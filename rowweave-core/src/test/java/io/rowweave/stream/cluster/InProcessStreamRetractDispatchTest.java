package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.FileStreamSavepointStore;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RETRACT uses the ordinary InProcess product roles. */
class InProcessStreamRetractDispatchTest {

    @Test
    void twoRetractSourcesUnionIntoOneSharedSubgraphAndTwoSinks(
            @TempDir Path root) throws Exception {
        SingleInsertSource first = new SingleInsertSource("source-a", 1L);
        SingleInsertSource second = new SingleInsertSource("source-b", 2L);
        IdSink left = new IdSink("sink-a");
        IdSink right = new IdSink("sink-b");
        Map<String, DurableStreamSourceProvider> sources = new HashMap<>();
        sources.put(first.providerKey(), first);
        sources.put(second.providerKey(), second);
        Map<String, EagerStreamSinkProvider> sinks = new HashMap<>();
        sinks.put(left.providerKey(), left);
        sinks.put(right.providerKey(), right);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, sources, sinks)) {
            StreamSession session = RowWeave.createStreamSession(
                    "retract-union", "retract-union", cluster);
            session.setChangelogMode(StreamChangelogMode.RETRACT);
            session.setCheckpointConfig(
                    StreamCheckpointConfig.manualOnlyUnaligned(
                            5_000L, 2));
            session.setResourceProfile(new StreamResourceProfile(
                    16, 4_096, 65_536, 0L, 0, 0,
                    65_536L, 6, 32, 0L));
            io.rowweave.DataStream one = session.fromSource(
                    retractSource(first.providerKey(), "split-a"))
                    .uid("source-a");
            io.rowweave.DataStream two = session.fromSource(
                    retractSource(second.providerKey(), "split-b"))
                    .uid("source-b");
            io.rowweave.DataStream union = one.union(two).uid("union");
            union.sinkTo("sink-a", retractSink(left.providerKey()));
            union.sinkTo("sink-b", retractSink(right.providerKey()));
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("retract-union-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(left.received.await(5L, TimeUnit.SECONDS));
            assertTrue(right.received.await(5L, TimeUnit.SECONDS));
            assertEquals(Arrays.asList(1L, 2L), sorted(left.ids));
            assertEquals(Arrays.asList(1L, 2L), sorted(right.ids));
            io.rowweave.StreamCheckpointHandle checkpoint =
                    job.triggerCheckpoint(
                            io.rowweave.stream.api.StreamCommandId.of(
                                    "retract-unaligned-cut"));
            assertEquals(io.rowweave.stream.api.StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(4L, new FileStreamSavepointStore(
                    root.resolve("checkpoints"))
                    .get(checkpoint.checkpointId()).checkpointManifest()
                    .taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(evidence -> evidence.kind()
                            == CheckpointEvidence.Kind.CHANNEL_STATE)
                    .count());
            job.cancel(io.rowweave.stream.api.StreamCommandId.of(
                    "cancel-retract-union")).awaitCompletion(5_000L);
        }
    }

    @Test
    void sourceAndSinkExchangeCanonicalChangesThroughTheGenericDag(
            @TempDir Path root) throws Exception {
        ChangeSource source = new ChangeSource();
        ChangeSink sink = new ChangeSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink))) {
            StreamJobHandle job = session(cluster).submit(
                    StreamSubmissionId.of("retract-dispatch-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(Arrays.asList(
                            StreamChangeRecord.Kind.INSERT,
                            StreamChangeRecord.Kind.UPDATE_BEFORE,
                            StreamChangeRecord.Kind.UPDATE_AFTER,
                            StreamChangeRecord.Kind.DELETE),
                    sink.kinds);
            job.cancel(io.rowweave.stream.api.StreamCommandId.of(
                    "cancel-retract-dispatch")).awaitCompletion(5_000L);
        }
    }

    private static StreamSession session(InProcessStreamCluster client) {
        StreamSession session = RowWeave.createStreamSession(
                "retract-dispatch", "retract-dispatch", client);
        session.setChangelogMode(StreamChangelogMode.RETRACT);
        session.setParallelism(1);
        session.setMaxParallelism(1);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 65_536, 0L, 0, 0,
                65_536L, 4, 16, 0L));
        session.fromSource(StreamSourceSpec.retractExactOrdered(
                schema(), Collections.singletonList("id"), "change-source",
                digest("change-source"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("split"), "position-v1", new byte[] {2}))))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.retractEagerAtLeastOnce(
                        "change-sink", digest("change-sink"),
                        new byte[] {3}));
        return session;
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.LongType, false)));
    }

    private static StreamSourceSpec retractSource(
            String providerKey, String splitId) {
        return StreamSourceSpec.retractExactOrdered(
                schema(), Collections.singletonList("id"), providerKey,
                digest(providerKey), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest(splitId), "position-v1", new byte[] {2})));
    }

    private static StreamSinkSpec retractSink(String providerKey) {
        return StreamSinkSpec.retractEagerAtLeastOnce(
                providerKey, digest(providerKey), new byte[] {3});
    }

    private static List<Long> sorted(List<Long> values) {
        List<Long> copy = new ArrayList<>(values);
        Collections.sort(copy);
        return copy;
    }

    private static final class SingleInsertSource
            implements DurableStreamSourceProvider {
        private final String providerKey;
        private final long id;

        private SingleInsertSource(String providerKey, long id) {
            this.providerKey = providerKey;
            this.id = id;
        }

        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() {
            return digest(providerKey);
        }
        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            byte[] frame = StreamChangeRecordCodec.encode(
                    StreamChangeRecord.insert(new RowSerializer(
                            PortablePlanCompiler.toDataSchema(
                                    schema().toPortableSchema()))
                            .serialize(row(id, id * 10L))));
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private boolean emitted;
                @Override public StreamSourceRecord next() throws Exception {
                    if (!emitted) {
                        emitted = true;
                        return StreamSourceRecord.of(frame, new byte[] {1});
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return new byte[] {(byte) (emitted ? 1 : 0)};
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class IdSink implements EagerStreamSinkProvider {
        private final String providerKey;
        private final CountDownLatch received = new CountDownLatch(2);
        private final List<Long> ids =
                Collections.synchronizedList(new ArrayList<>());

        private IdSink(String providerKey) {
            this.providerKey = providerKey;
        }

        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() {
            return digest(providerKey);
        }
        @Override public EagerStreamSinkWriter open(
                StreamSinkOpenRequest request) {
            RowSerializer serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            schema().toPortableSchema()));
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    StreamChangeRecord change =
                            StreamChangeRecordCodec.decode(
                                    recordFrame, 4_096, 4_096, 4_096);
                    ids.add(serializer.deserialize(
                            change.afterImageFrame()).getLong(0));
                    received.countDown();
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class ChangeSource
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return "change-source"; }
        @Override public String contractDigest() {
            return digest("change-source");
        }

        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            PortableRowSchema schema = schema().toPortableSchema();
            RowSerializer rows = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(schema));
            RowSerializer keys = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(new PortableRowSchema(
                            Collections.singletonList(schema.field(0)))));
            DataRow before = row(5L, 10L);
            DataRow after = row(5L, 20L);
            byte[] key = keys.serializeProjected(before, new int[] {0});
            List<byte[]> frames = Arrays.asList(
                    encode(StreamChangeRecord.insert(rows.serialize(before))),
                    encode(StreamChangeRecord.updateBefore(
                            key, rows.serialize(before))),
                    encode(StreamChangeRecord.updateAfter(
                            key, rows.serialize(after))),
                    encode(StreamChangeRecord.delete(
                            key, rows.serialize(after))));
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next < frames.size()) {
                        return StreamSourceRecord.of(
                                frames.get(next++), new byte[] {(byte) next});
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return new byte[] {(byte) next};
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class ChangeSink implements EagerStreamSinkProvider {
        private final CountDownLatch received = new CountDownLatch(4);
        private final List<StreamChangeRecord.Kind> kinds =
                Collections.synchronizedList(new ArrayList<>());

        @Override public String providerKey() { return "change-sink"; }
        @Override public String contractDigest() {
            return digest("change-sink");
        }

        @Override public EagerStreamSinkWriter open(
                StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    kinds.add(StreamChangeRecordCodec.decode(
                            recordFrame, 4_096, 4_096, 4_096).kind());
                    received.countDown();
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static byte[] encode(StreamChangeRecord record) {
        return StreamChangeRecordCodec.encode(record);
    }

    private static DataRow row(long id, long amount) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, amount);
        return row;
    }

    private static String digest(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder text = new StringBuilder(64);
            for (byte current : digest) {
                text.append(Character.forDigit((current >>> 4) & 0xf, 16));
                text.append(Character.forDigit(current & 0xf, 16));
            }
            return text.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
