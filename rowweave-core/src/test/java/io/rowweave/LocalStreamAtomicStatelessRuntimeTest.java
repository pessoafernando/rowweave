package io.rowweave;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamEventTimeSpec;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.spi.StreamFunctionProvider;
import io.rowweave.stream.spi.StreamSinkContext;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSinkWriter;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Local reuses the cluster atomic operator graph. */
class LocalStreamAtomicStatelessRuntimeTest {
    private static final String SOURCE = "atomic-local-source-v1";
    private static final String SINK = "atomic-local-sink-v1";
    private static final String UNION_LEFT = "atomic-local-union-left-v1";
    private static final String UNION_RIGHT = "atomic-local-union-right-v1";
    private static final String UNION_SINK = "atomic-local-union-sink-v1";
    private static final String SOURCE_CONTRACT = digest("source-contract");
    private static final String SINK_CONTRACT = digest("sink-contract");
    private static final String DEPLOYMENT = digest("deployment");
    private static final String JAR = digest("jar");

    @Test
    void localExecutesTheSameCalcTimeAndFlatMapGraph(
            @TempDir Path root) throws Exception {
        Source source = new Source();
        Sink sink = new Sink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.singletonList(sink),
                Collections.singletonList(new Functions()))) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("atomic-local-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            sink.await(4, 5_000L);
            assertEquals(Arrays.asList(6L, 60L, 8L, 80L), sink.values());
            job.cancel(StreamCommandId.of("atomic-local-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void localExecutesTheSameFairUnionInputGate(@TempDir Path root)
            throws Exception {
        Source left = new Source(UNION_LEFT, 0, 5);
        Source right = new Source(UNION_RIGHT, 100, 5);
        IntegerSink sink = new IntegerSink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Arrays.asList(left, right),
                Collections.singletonList(sink), Collections.emptyList())) {
            StreamJobHandle job = unionSession(client).submit(
                    StreamSubmissionId.of("atomic-local-union-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            sink.await(10, 5_000L);
            assertEquals(Arrays.asList(0, 1, 2, 3, 4),
                    sink.valuesBelow(100));
            assertEquals(Arrays.asList(100, 101, 102, 103, 104),
                    sink.valuesAtLeast(100));
            job.cancel(StreamCommandId.of("atomic-local-union-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "atomic-local", "atomic-local", client);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0, 0L, 4, 16, 0L));
        StreamExpression value = StreamExpression.input(
                "value", PortableDataType.INTEGER, false);
        StreamCalcSpec timestamped = StreamCalcSpec.of(
                StreamExpression.call(StreamExpression.Function
                                .GREATER_OR_EQUAL,
                        value, StreamExpression.integerLiteral(5)),
                Arrays.asList(StreamCalcSpec.projection("value", value),
                        StreamCalcSpec.projection("event-time",
                                StreamExpression.call(StreamExpression.Function
                                                .EPOCH_MILLIS_TO_TIMESTAMP,
                                        StreamExpression.call(StreamExpression
                                                .Function.CAST_LONG, value)))));
        StreamCalcSpec valueOnly = StreamCalcSpec.of(null,
                Collections.singletonList(StreamCalcSpec.projection(
                        "value", value)));
        session.fromSource(StreamSourceSpec.exactOrdered(
                        integerSchema(), SOURCE, SOURCE_CONTRACT,
                        new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(digest("split"), "sequence-v1",
                                new byte[] {2}))))
                .uid("source")
                .calc(timestamped, timestampedSchema()).uid("add-time")
                .assignEventTime(StreamEventTimeSpec.monotonic(
                        StreamExpression.input("event-time",
                                PortableDataType.TIMESTAMP, false),
                        30_000L,
                        StreamEventTimeSpec.InvalidTimestampPolicy.FAIL))
                .uid("assign-time")
                .calc(valueOnly, integerSchema()).uid("drop-time")
                .flatMap(function(), longSchema(),
                        StreamEmissionLimit.of(2, 64L)).uid("flat-map")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        SINK, SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static StreamSession unionSession(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "atomic-local-union", "atomic-local-union", client);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0, 0L, 5, 16, 0L));
        DataStream left = session.fromSource(source(UNION_LEFT, "left"))
                .uid("left");
        DataStream right = session.fromSource(source(UNION_RIGHT, "right"))
                .uid("right");
        left.union(right).uid("union").sinkTo("sink",
                StreamSinkSpec.eagerAtLeastOnce(
                        UNION_SINK, SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static StreamSourceSpec source(String provider, String split) {
        return StreamSourceSpec.exactOrdered(
                integerSchema(), provider, SOURCE_CONTRACT,
                new byte[] {1}, Collections.singletonList(
                new StreamSplitSpec(digest(split), "sequence-v1",
                        new byte[] {2})));
    }

    private static FunctionDeploymentReference function() {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(
                DEPLOYMENT, "flat-map", JAR, DuplicateEven.class.getName(),
                FunctionAbi.FLAT_MAP_V1, "stream-function-config-v1",
                digest(config), config,
                new FunctionResourceProfile(1_024L, 0L, 2));
    }

    private static StreamSchema integerSchema() {
        return new StreamSchema(Collections.singletonList(new StreamField(
                "value", "value", "value", DataType.IntegerType, false)));
    }

    private static StreamSchema longSchema() {
        return new StreamSchema(Collections.singletonList(new StreamField(
                "value", "value", "value", DataType.LongType, false)));
    }

    private static StreamSchema timestampedSchema() {
        return new StreamSchema(Arrays.asList(
                integerSchema().field(0),
                new StreamField("event-time", "eventTime", "eventTime",
                        DataType.TimestampType, false)));
    }

    public static final class DuplicateEven implements FlatMapFunction {
        @Override public void flatMap(DataRow row, Collector collector) {
            int value = row.getInteger(0);
            if ((value & 1) != 0) return;
            DataRow first = new DataRow(1);
            first.set(0, (long) value);
            collector.collect(first);
            DataRow second = new DataRow(1);
            second.set(0, value * 10L);
            collector.collect(second);
        }
    }

    private static final class Functions implements StreamFunctionProvider {
        @Override public String deploymentSetId() { return DEPLOYMENT; }
        @Override public Serializable resolve(
                FunctionDeploymentReference reference) {
            if (function().equals(reference)) return new DuplicateEven();
            throw new IllegalArgumentException("unknown function");
        }
    }

    private static final class Source implements StreamSourceProvider {
        private final String providerKey;
        private final int start;
        private final int count;

        Source() { this(SOURCE, 0, 10); }

        Source(String providerKey, int start, int count) {
            this.providerKey = providerKey;
            this.start = start;
            this.count = count;
        }

        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            StreamDataRowCodec codec = new StreamDataRowCodec(integerSchema());
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next;
                private volatile boolean canceled;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next < count) {
                        DataRow row = new DataRow(1);
                        row.set(0, start + next++);
                        return StreamSourceRecord.of(codec.encode(row),
                                ByteBuffer.allocate(4).putInt(next).array());
                    }
                    while (!canceled) {
                        if (wakeups.tryAcquire(10L,
                                TimeUnit.MILLISECONDS)) return null;
                    }
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4).putInt(next).array();
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    canceled = true; wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }
    }

    private static final class Sink implements StreamSinkProvider {
        private final List<Long> values = new ArrayList<>();
        private final StreamDataRowCodec codec = new StreamDataRowCodec(longSchema());
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamSinkWriter open(StreamSinkContext context) {
            return frame -> {
                synchronized (values) {
                    values.add(codec.decode(frame).getLong(0));
                    values.notifyAll();
                }
            };
        }
        void await(int count, long timeoutMillis) throws InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            synchronized (values) {
                while (values.size() < count
                        && System.nanoTime() < deadline) {
                    values.wait(10L);
                }
                assertEquals(count, values.size());
            }
        }
        List<Long> values() {
            synchronized (values) {
                return new ArrayList<>(values);
            }
        }
    }

    private static final class IntegerSink implements StreamSinkProvider {
        private final List<Integer> values = new ArrayList<>();
        private final StreamDataRowCodec codec = new StreamDataRowCodec(
                integerSchema());
        @Override public String providerKey() { return UNION_SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamSinkWriter open(StreamSinkContext context) {
            return frame -> {
                synchronized (values) {
                    values.add(codec.decode(frame).getInteger(0));
                    values.notifyAll();
                }
            };
        }
        void await(int count, long timeoutMillis) throws InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            synchronized (values) {
                while (values.size() < count
                        && System.nanoTime() < deadline) values.wait(10L);
                assertEquals(count, values.size());
            }
        }
        List<Integer> valuesBelow(int boundary) {
            return valuesByBoundary(boundary, false);
        }
        List<Integer> valuesAtLeast(int boundary) {
            return valuesByBoundary(boundary, true);
        }
        private List<Integer> valuesByBoundary(
                int boundary, boolean atLeast) {
            List<Integer> result = new ArrayList<>();
            synchronized (values) {
                for (Integer value : values) {
                    if ((value >= boundary) == atLeast) result.add(value);
                }
            }
            return result;
        }
    }

    private static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) {
                result.append(String.format("%02x", one & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
