package io.rowweave.examples;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.FilterFunction;
import io.rowweave.MapFunction;
import io.rowweave.DataRow;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.spi.StreamFunctionProvider;
import io.rowweave.stream.spi.StreamSinkContext;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSinkWriter;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;

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

/** Runnable public-SDK APPEND map/filter job on the no-listener Local runtime. */
public final class StreamLocalAppendExample {
    private static final String SOURCE = "example-source-v1";
    private static final String SINK = "example-sink-v1";
    private static final String SOURCE_CONTRACT = digest("example-source");
    private static final String SINK_CONTRACT = digest("example-sink");
    private static final String DEPLOYMENT = digest("example-functions");
    private static final String JAR = digest("example-functions-jar");
    private static final StreamSchema SCHEMA = new StreamSchema(Arrays.asList(
            new StreamField("segment", "segment", "segment",
                    DataType.StringType, false),
            new StreamField("amount", "amount", "amount",
                    DataType.IntegerType, false)));

    private StreamLocalAppendExample() { }

    public static List<String> run(Path stateRoot) throws Exception {
        ExampleSource source = new ExampleSource();
        ExampleSink sink = new ExampleSink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                stateRoot, Collections.singletonList(source),
                Collections.singletonList(sink),
                Collections.singletonList(new Functions()))) {
            StreamSession session = RowWeave.createStreamSession(
                    "stream-local-append", "stream-local-append", client);
            session.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 0L, 0, 0,
                    32_768L, 8, 32, 0L));
            DataStream result = session.fromSource(StreamSourceSpec.exactOrdered(
                            SCHEMA, SOURCE, SOURCE_CONTRACT, new byte[] {1},
                            Collections.singletonList(new StreamSplitSpec(
                                    digest("example-split"), "int32-v1",
                                    new byte[] {2}))))
                    .uid("source")
                    .filter(function("minimum", MinimumAmount.class,
                            FunctionAbi.FILTER_V1)).uid("minimum")
                    .map(function("normalize", NormalizeSegment.class,
                            FunctionAbi.MAP_V1), SCHEMA).uid("normalize");
            result.sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                    SINK, SINK_CONTRACT, new byte[] {3}));

            StreamJobHandle handle = session.submit(
                    StreamSubmissionId.of("stream-local-append-example"));
            handle.awaitRunning(5_000L);
            sink.awaitWrites(2, 5_000L);
            if (handle.cancel(StreamCommandId.of("example-cancel"))
                    .awaitCompletion(5_000L)
                    != StreamOperationStatus.SUCCEEDED) {
                throw new IllegalStateException("Local Stream cancel failed");
            }
            return sink.snapshot();
        }
    }

    public static final class MinimumAmount implements FilterFunction {
        @Override public boolean filter(DataRow row) {
            return row.getInteger(1) >= 20;
        }
    }

    public static final class NormalizeSegment implements MapFunction {
        @Override public DataRow map(DataRow row) {
            DataRow result = row.copy();
            result.set(0, row.getString(0).toUpperCase(java.util.Locale.ROOT));
            return result;
        }
    }

    private static FunctionDeploymentReference function(
            String id, Class<?> implementation, FunctionAbi abi) {
        byte[] configuration = new byte[] {1};
        return new FunctionDeploymentReference(
                DEPLOYMENT, id, JAR, implementation.getName(), abi,
                "example-config-v1", digest(configuration), configuration,
                new FunctionResourceProfile(2_048L, 0L, 1));
    }

    private static final class Functions implements StreamFunctionProvider {
        @Override public String deploymentSetId() { return DEPLOYMENT; }
        @Override public Serializable resolve(
                FunctionDeploymentReference reference) {
            if ("minimum".equals(reference.deploymentId())) {
                return new MinimumAmount();
            }
            if ("normalize".equals(reference.deploymentId())) {
                return new NormalizeSegment();
            }
            throw new IllegalArgumentException("unknown example function");
        }
    }

    private static final class ExampleSource implements StreamSourceProvider {
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(context.restoredPositionFrame()).getInt()
                        : 0;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next < 3) {
                        int value = next++;
                        DataRow row = new DataRow(2);
                        row.set(0, String.valueOf((char) ('a' + value)));
                        row.set(1, (value + 1) * 10);
                        return StreamSourceRecord.of(codec.encode(row), position(next));
                    }
                    while (!canceled && !wakeups.tryAcquire(
                            20L, TimeUnit.MILLISECONDS)) { }
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    canceled = true;
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }
        private static byte[] position(int value) {
            return ByteBuffer.allocate(4).putInt(value).array();
        }
    }

    private static final class ExampleSink implements StreamSinkProvider {
        private final List<String> rows = new ArrayList<>();
        private final StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamSinkWriter open(StreamSinkContext context) {
            return frame -> {
                DataRow row = codec.decode(frame);
                synchronized (rows) {
                    rows.add(row.getString(0) + '-' + row.getInteger(1));
                    rows.notifyAll();
                }
            };
        }
        void awaitWrites(int count, long timeoutMillis)
                throws InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            synchronized (rows) {
                while (rows.size() < count && System.nanoTime() < deadline) {
                    TimeUnit.NANOSECONDS.timedWait(
                            rows, deadline - System.nanoTime());
                }
                if (rows.size() < count) {
                    throw new IllegalStateException(
                            "Local Stream example Sink timed out");
                }
            }
        }
        List<String> snapshot() {
            synchronized (rows) {
                return Collections.unmodifiableList(new ArrayList<>(rows));
            }
        }
    }

    private static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) result.append(String.format("%02x", one));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
