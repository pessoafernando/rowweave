package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataRow;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.StreamWindowAggregateFunction;
import io.rowweave.RetractableStreamWindowAggregateFunction;
import io.rowweave.StreamWindowProcessFunction;
import io.rowweave.StreamWindowContext;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.api.StreamAccumulationMode;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamGenericWindowSpec;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.api.StreamWindowAssignerSpec;
import io.rowweave.stream.api.StreamWindowResourceSpec;
import io.rowweave.stream.api.StreamWindowTriggerSpec;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceEvent;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public generic windows execute through shared roles. */
class InProcessStreamGenericWindowDispatchTest {

    @Test
    void retractAggregateWindowRemovesBeforeImageBeforeFinalTrigger(
            @TempDir Path root) throws Exception {
        RetractWindowSource source = new RetractWindowSource();
        RetractWindowSink sink = new RetractWindowSink();
        FunctionDeploymentReference function = retractFunction();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> new RetractDecimalAggregate(),
                Collections.singletonList(function.deploymentSetId()))) {
            StreamJobHandle job = retractSession(cluster, function).submit(
                    StreamSubmissionId.of("retract-window-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals(StreamChangeRecord.Kind.INSERT, sink.kind);
            assertEquals("A:0:100:10.0000000000", sink.value);
            job.cancel(StreamCommandId.of("retract-window-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void publicAggregateWindowRunsAndCheckpointsThroughMasterWorkerRoles(
            @TempDir Path root) throws Exception {
        WindowSource source = new WindowSource();
        WindowSink sink = new WindowSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> new DecimalAggregate(), Collections.singletonList(
                StreamGenericWindowProductAdmissionTest.function()
                        .deploymentSetId()))) {
            StreamJobHandle job = session(cluster).submit(
                    StreamSubmissionId.of("generic-window-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals("A:0:100:3.0000000001", sink.value);
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "generic-window-checkpoint"))
                            .awaitCompletion(5_000L));
            job.cancel(StreamCommandId.of("generic-window-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void publicProcessWindowRunsThroughMasterWorkerRoles(
            @TempDir Path root) throws Exception {
        WindowSource source = new WindowSource();
        WindowSink sink = new WindowSink();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, Collections.singletonMap(source.providerKey(), source),
                Collections.singletonMap(sink.providerKey(), sink),
                ignored -> new SumProcess(), Collections.singletonList(
                StreamGenericWindowProductAdmissionTest.processFunction()
                        .deploymentSetId()))) {
            StreamJobHandle job = processSession(cluster).submit(
                    StreamSubmissionId.of("generic-process-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            assertTrue(sink.received.await(5L, TimeUnit.SECONDS));
            assertEquals("A:0:100:3.0000000001", sink.value);
            job.cancel(StreamCommandId.of("generic-process-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    private static StreamSession session(InProcessStreamCluster cluster) {
        StreamSession session = RowWeave.createStreamSession(
                "generic-window", "generic-window", cluster);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 16_384L, 32, 32,
                65_536L, 5, 32, 1_048_576L));
        DataStream keyed = session.fromSource(
                StreamSourceSpec.exactOrderedEventTime(
                        StreamGenericWindowProductAdmissionTest.inputSchema(),
                        "source-v1",
                        StreamGenericWindowProductAdmissionTest
                                .digest("source"), new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                StreamGenericWindowProductAdmissionTest
                                        .digest("split"),
                                "sequence-v1", new byte[] {2})),
                        StreamWatermarkPolicy.providerEvents(1_000L)))
                .uid("source").keyBy("adaptor").uid("key");
        keyed.genericWindow(StreamGenericWindowSpec.aggregate(
                        StreamWindowAssignerSpec.eventTimeTumbling(
                                "event-time", 100L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING, 0L,
                        StreamGenericWindowProductAdmissionTest.function(),
                        StreamGenericWindowProductAdmissionTest.outputSchema(),
                        Collections.singletonList("adaptor"),
                        new StreamStateSpec("window-state",
                                StreamStateSpec.Kind.KEYED_AGGREGATING,
                                "generic-window-state-v1", 1,
                                StreamStateSpec.TtlMode.NONE, 0L),
                        StreamWindowResourceSpec.of(8, 1_000L,
                                8_192L, 32_768L, 8,
                                StreamEmissionLimit.of(1, 4_096L))))
                .uid("window").sinkTo("sink",
                        StreamSinkSpec.eagerAtLeastOnce("sink-v1",
                                StreamGenericWindowProductAdmissionTest
                                        .digest("sink"), new byte[] {3}));
        return session;
    }

    private static StreamSession processSession(
            InProcessStreamCluster cluster) {
        StreamSession session = RowWeave.createStreamSession(
                "generic-process", "generic-process", cluster);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 16_384L, 32, 32,
                65_536L, 5, 32, 1_048_576L));
        DataStream keyed = session.fromSource(
                StreamSourceSpec.exactOrderedEventTime(
                        StreamGenericWindowProductAdmissionTest.inputSchema(),
                        "source-v1",
                        StreamGenericWindowProductAdmissionTest
                                .digest("source"), new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                StreamGenericWindowProductAdmissionTest
                                        .digest("split"),
                                "sequence-v1", new byte[] {2})),
                        StreamWatermarkPolicy.providerEvents(1_000L)))
                .uid("source").keyBy("adaptor").uid("key");
        keyed.genericWindow(StreamGenericWindowSpec.process(
                        StreamWindowAssignerSpec.eventTimeTumbling(
                                "event-time", 100L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING, 0L,
                        StreamGenericWindowProductAdmissionTest
                                .processFunction(),
                        StreamGenericWindowProductAdmissionTest.outputSchema(),
                        Collections.singletonList("adaptor"),
                        new StreamStateSpec("window-rows",
                                StreamStateSpec.Kind.KEYED_LIST,
                                "generic-window-rows-v1", 1,
                                StreamStateSpec.TtlMode.NONE, 0L),
                        StreamWindowResourceSpec.of(8, 1_000L,
                                8_192L, 32_768L, 8,
                                StreamEmissionLimit.of(1, 4_096L))))
                .uid("window").sinkTo("sink",
                        StreamSinkSpec.eagerAtLeastOnce("sink-v1",
                                StreamGenericWindowProductAdmissionTest
                                        .digest("sink"), new byte[] {3}));
        return session;
    }

    private static StreamSession retractSession(
            InProcessStreamCluster cluster,
            FunctionDeploymentReference function) {
        StreamSession session = RowWeave.createStreamSession(
                "retract-window", "retract-window", cluster);
        session.setChangelogMode(StreamChangelogMode.RETRACT);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 8_192, 16_384L, 32, 32,
                65_536L, 5, 32, 1_048_576L));
        DataStream keyed = session.fromSource(
                StreamSourceSpec.retractExactOrderedEventTime(
                        StreamGenericWindowProductAdmissionTest.inputSchema(),
                        Collections.singletonList("adaptor"),
                        "retract-window-source",
                        StreamGenericWindowProductAdmissionTest
                                .digest("retract-window-source"),
                        new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(
                                StreamGenericWindowProductAdmissionTest
                                        .digest("retract-window-split"),
                                "sequence-v2", new byte[] {2})),
                        StreamWatermarkPolicy.providerEvents(1_000L)))
                .uid("source").keyBy("adaptor").uid("key");
        keyed.genericWindow(StreamGenericWindowSpec.aggregate(
                        StreamWindowAssignerSpec.eventTimeTumbling(
                                "event-time", 100L),
                        StreamWindowTriggerSpec.eventTimeFinal().purging(),
                        StreamAccumulationMode.DISCARDING, 0L, function,
                        StreamGenericWindowProductAdmissionTest.outputSchema(),
                        Collections.singletonList("adaptor"),
                        new StreamStateSpec("retract-window-state",
                                StreamStateSpec.Kind.KEYED_AGGREGATING,
                                "generic-retract-window-state-v2", 2,
                                StreamStateSpec.TtlMode.NONE, 0L),
                        StreamWindowResourceSpec.of(8, 1_000L,
                                8_192L, 32_768L, 8,
                                StreamEmissionLimit.of(1, 4_096L))))
                .uid("window").sinkTo("sink",
                        StreamSinkSpec.retractEagerAtLeastOnce(
                                "retract-window-sink",
                                StreamGenericWindowProductAdmissionTest
                                        .digest("retract-window-sink"),
                                new byte[] {3}));
        return session;
    }

    private static FunctionDeploymentReference retractFunction() {
        byte[] configuration = new byte[] {6};
        return new FunctionDeploymentReference(
                StreamGenericWindowProductAdmissionTest
                        .digest("retract-window-deployment"),
                "retract-window-functions",
                StreamGenericWindowProductAdmissionTest
                        .digest("retract-window-jar"),
                "example.RetractWindowAggregate",
                FunctionAbi.STREAM_RETRACT_WINDOW_AGGREGATE_V2,
                "retract-window-config-v2",
                StreamGenericWindowProductAdmissionTest.digest(configuration),
                configuration, new FunctionResourceProfile(
                1_048_576L, 0L, 1));
    }

    private static final class WindowSource
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return "source-v1"; }
        @Override public String contractDigest() {
            return StreamGenericWindowProductAdmissionTest.digest("source");
        }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            StreamGenericWindowProductAdmissionTest
                                    .inputSchema().toPortableSchema()));
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next;
                @Override public StreamSourceRecord next() { return null; }
                @Override public StreamSourceEvent nextEvent() throws Exception {
                    if (next == 0) return record(serializer, ++next,
                            new BigDecimal("1.00000000006"), 10L);
                    if (next == 1) return record(serializer, ++next,
                            new BigDecimal("2.00000000005"), 20L);
                    if (next == 2) {
                        next++;
                        return StreamSourceEvent.watermark(100L);
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4).putInt(next).array();
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class RetractWindowSource
            implements DurableStreamSourceProvider {
        @Override public String providerKey() {
            return "retract-window-source";
        }
        @Override public String contractDigest() {
            return StreamGenericWindowProductAdmissionTest
                    .digest("retract-window-source");
        }
        @Override public StreamSourceReader open(
                StreamSourceOpenRequest request) {
            RowSerializer rows = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            StreamGenericWindowProductAdmissionTest
                                    .inputSchema().toPortableSchema()));
            RowSerializer keys = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            new io.rowweave.portable.PortableRowSchema(
                                    Collections.singletonList(
                                            StreamGenericWindowProductAdmissionTest
                                                    .inputSchema()
                                                    .toPortableSchema()
                                                    .field(0)))));
            DataRow ten = inputRow(new BigDecimal("10"), 10L);
            DataRow five = inputRow(new BigDecimal("5"), 20L);
            byte[] key = keys.serializeProjected(five, new int[] {0});
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next;
                @Override public StreamSourceRecord next() { return null; }
                @Override public StreamSourceEvent nextEvent()
                        throws Exception {
                    next++;
                    if (next == 1) return change(rows, next, 10L,
                            StreamChangeRecord.insert(rows.serialize(ten)));
                    if (next == 2) return change(rows, next, 20L,
                            StreamChangeRecord.insert(rows.serialize(five)));
                    if (next == 3) return change(rows, next, 20L,
                            StreamChangeRecord.delete(
                                    key, rows.serialize(five)));
                    if (next == 4) return StreamSourceEvent.watermark(100L);
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4).putInt(next).array();
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static StreamSourceEvent change(RowSerializer ignored,
            int position, long timestamp, StreamChangeRecord record) {
        return StreamSourceEvent.record(StreamSourceRecord.of(
                StreamChangeRecordCodec.encode(record),
                ByteBuffer.allocate(4).putInt(position).array(), timestamp));
    }

    private static DataRow inputRow(BigDecimal amount, long timestamp) {
        DataRow row = new DataRow(3);
        row.set(0, "A");
        row.set(1, new Timestamp(timestamp));
        row.set(2, amount);
        return row;
    }

    private static StreamSourceEvent record(RowSerializer serializer,
            int position, BigDecimal amount, long timestamp) {
        DataRow row = new DataRow(3);
        row.set(0, "A");
        row.set(1, new Timestamp(timestamp));
        row.set(2, amount);
        return StreamSourceEvent.record(StreamSourceRecord.of(
                serializer.serialize(row),
                ByteBuffer.allocate(4).putInt(position).array(), timestamp));
    }

    private static final class WindowSink
            implements EagerStreamSinkProvider {
        private final CountDownLatch received = new CountDownLatch(1);
        private volatile String value;
        @Override public String providerKey() { return "sink-v1"; }
        @Override public String contractDigest() {
            return StreamGenericWindowProductAdmissionTest.digest("sink");
        }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            RowSerializer serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            StreamGenericWindowProductAdmissionTest
                                    .outputSchema().toPortableSchema()));
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] frame) {
                    DataRow row = serializer.deserialize(frame);
                    value = row.getString(0) + ':' + row.getLong(1) + ':'
                            + row.getLong(2) + ':'
                            + row.getBigDecimal(3).toPlainString();
                    received.countDown();
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class RetractWindowSink
            implements EagerStreamSinkProvider {
        private final CountDownLatch received = new CountDownLatch(1);
        private volatile StreamChangeRecord.Kind kind;
        private volatile String value;
        @Override public String providerKey() {
            return "retract-window-sink";
        }
        @Override public String contractDigest() {
            return StreamGenericWindowProductAdmissionTest
                    .digest("retract-window-sink");
        }
        @Override public EagerStreamSinkWriter open(
                StreamSinkOpenRequest request) {
            RowSerializer serializer = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            StreamGenericWindowProductAdmissionTest
                                    .outputSchema().toPortableSchema()));
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] frame) {
                    StreamChangeRecord change = StreamChangeRecordCodec.decode(
                            frame, 4_096, 4_096, 4_096);
                    kind = change.kind();
                    DataRow row = serializer.deserialize(
                            change.afterImageFrame());
                    value = row.getString(0) + ':' + row.getLong(1) + ':'
                            + row.getLong(2) + ':'
                            + row.getBigDecimal(3).toPlainString();
                    received.countDown();
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class DecimalAggregate
            implements StreamWindowAggregateFunction {
        @Override public byte[] createAccumulator() {
            return bytes(BigDecimal.ZERO);
        }
        @Override public byte[] add(DataRow value, byte[] accumulator) {
            BigDecimal amount = value.getBigDecimal(2);
            return bytes(decimal(accumulator).add(amount));
        }
        @Override public byte[] merge(byte[] left, byte[] right) {
            return bytes(decimal(left).add(decimal(right)));
        }
        @Override public DataRow getResult(byte[] accumulator,
                StreamWindowContext context) {
            DataRow row = new DataRow(4);
            row.set(0, context.currentKey().get(0));
            row.set(1, context.windowStart());
            row.set(2, context.windowEnd());
            row.set(3, decimal(accumulator).setScale(
                    10, java.math.RoundingMode.HALF_UP));
            return row;
        }
        private static BigDecimal decimal(byte[] value) {
            return new BigDecimal(new String(value, StandardCharsets.UTF_8));
        }
        private static byte[] bytes(BigDecimal value) {
            return value.toPlainString().getBytes(StandardCharsets.UTF_8);
        }
    }

    private static final class RetractDecimalAggregate
            implements RetractableStreamWindowAggregateFunction {
        @Override public byte[] createAccumulator() {
            return bytes(BigDecimal.ZERO, 0L);
        }
        @Override public byte[] add(DataRow value, byte[] accumulator) {
            Accumulator current = accumulator(accumulator);
            return bytes(current.sum.add(value.getBigDecimal(2)),
                    current.count + 1L);
        }
        @Override public byte[] retract(DataRow value, byte[] accumulator) {
            Accumulator current = accumulator(accumulator);
            if (current.count < 1L) {
                throw new IllegalArgumentException("empty window retract");
            }
            return bytes(current.sum.subtract(value.getBigDecimal(2)),
                    current.count - 1L);
        }
        @Override public boolean isEmpty(byte[] accumulator) {
            return accumulator(accumulator).count == 0L;
        }
        @Override public byte[] merge(byte[] left, byte[] right) {
            Accumulator first = accumulator(left);
            Accumulator second = accumulator(right);
            return bytes(first.sum.add(second.sum),
                    first.count + second.count);
        }
        @Override public DataRow getResult(byte[] accumulator,
                StreamWindowContext context) {
            Accumulator value = accumulator(accumulator);
            DataRow row = new DataRow(4);
            row.set(0, context.currentKey().get(0));
            row.set(1, context.windowStart());
            row.set(2, context.windowEnd());
            row.set(3, value.sum.setScale(
                    10, java.math.RoundingMode.HALF_UP));
            return row;
        }
        private static byte[] bytes(BigDecimal value, long count) {
            return (value.toPlainString() + '|' + Long.toString(count))
                    .getBytes(StandardCharsets.UTF_8);
        }
        private static Accumulator accumulator(byte[] value) {
            String text = new String(value, StandardCharsets.UTF_8);
            int split = text.lastIndexOf('|');
            return new Accumulator(new BigDecimal(text.substring(0, split)),
                    Long.parseLong(text.substring(split + 1)));
        }
    }

    private static final class Accumulator {
        private final BigDecimal sum;
        private final long count;
        private Accumulator(BigDecimal sum, long count) {
            this.sum = sum;
            this.count = count;
        }
    }

    private static final class SumProcess
            implements StreamWindowProcessFunction {
        @Override public void process(StreamWindowContext context,
                io.rowweave.StreamWindowDataRowCursor elements,
                io.rowweave.StreamWindowCollector output) {
            BigDecimal sum = BigDecimal.ZERO;
            while (elements.advance()) {
                sum = sum.add(elements.row().getBigDecimal(2));
            }
            DataRow row = new DataRow(4);
            row.set(0, context.currentKey().get(0));
            row.set(1, context.windowStart());
            row.set(2, context.windowEnd());
            row.set(3, sum.setScale(10,
                    java.math.RoundingMode.HALF_UP));
            output.collect(row);
        }
    }
}
