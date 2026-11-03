package io.rowweave;

import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.MonitorRuntime;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamUpsertCodec;
import io.rowweave.stream.api.StreamUpsertValue;
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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Local is the public no-listener product runtime. */
class LocalStreamPublicRuntimeTest {
    private static final String SOURCE = "public-local-source-v1";
    private static final String SINK = "public-local-sink-v1";
    private static final String SOURCE_CONTRACT = digest("source-contract");
    private static final String SINK_CONTRACT = digest("sink-contract");
    private static final String DEPLOYMENT = digest("deployment");
    private static final String JAR = digest("jar");

    @Test
    void publicLocalClientRunsSavepointOfflineRescaleAndCheckpoint(
            @TempDir Path root) throws Exception {
        TestSourceProvider source = new TestSourceProvider();
        TestSinkProvider sink = new TestSinkProvider();
        try (StreamJobClient client = local(root, source, sink)) {
            StreamJobClientRecoveryContract.verifySavepointOfflineRescale(
                    client, session(client, 1), session(client, 2),
                    "public-local-rescale", 2, 2,
                    () -> sink.awaitWrites(2, 5_000L),
                    () -> { },
                    () -> {
                        sink.awaitWrites(4, 5_000L);
                        assertEquals(Arrays.asList(
                                "A=1.0000000000", "B=2.0000000000",
                                "A=4.0000000000", "B=6.0000000000"),
                                sink.canonicalWrites());
                    });
        }
    }

    @Test
    void publicLocalClientPassesUnifiedLifecycleContract(
            @TempDir Path root) {
        TestSourceProvider source = new TestSourceProvider();
        TestSinkProvider sink = new TestSinkProvider();
        try (StreamJobClient client = local(root, source, sink)) {
            StreamJobClientLifecycleContract.verify(
                    client, session(client, 1), "local");
        }
    }

    @Test
    void publicLocalClientFeedsMonitorRuntime(@TempDir Path root) {
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(4);
        MonitorRuntime.useForTesting(monitor);
        TestSourceProvider source = new TestSourceProvider();
        TestSinkProvider sink = new TestSinkProvider();
        try (StreamJobClient client = local(root, source, sink)) {
            StreamJobHandle job = session(client, 1).submit(
                    StreamSubmissionId.of("public-local-monitor"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            JobView view = monitor.job(job.id().value());
            assertTrue(view != null, "submitted Stream job was not monitored");
            assertEquals("RUNNING", view.state());
            assertTrue(view.stream() != null, "Stream typed view was absent");

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.cancel(StreamCommandId.of("public-local-monitor-cancel"))
                            .awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.CANCELED, job.status());
            assertEquals("CANCELED", monitor.job(job.id().value()).state());
        } finally {
            MonitorRuntime.resetForTesting();
        }
    }

    @Test
    void publicLocalClientRestoresANewJobFromExactCheckpoint(
            @TempDir Path root) throws Exception {
        TestSourceProvider source = new TestSourceProvider();
        TestSinkProvider sink = new TestSinkProvider();
        try (StreamJobClient client = local(root, source, sink)) {
            StreamJobClientRecoveryContract.verifyExactCheckpointRestore(
                    client, session(client, 1), "public-local-exact", 1, 2,
                    () -> sink.awaitWrites(2, 5_000L),
                    () -> { },
                    () -> {
                        sink.awaitWrites(4, 5_000L);
                        assertEquals(Arrays.asList(
                                "A=1.0000000000", "B=2.0000000000",
                                "A=4.0000000000", "B=6.0000000000"),
                                sink.canonicalWrites());
                    });
        }
    }

    @Test
    void localStateRootIsExclusiveAndReopensFromLatestCheckpoint(
            @TempDir Path root) throws Exception {
        TestSourceProvider source = new TestSourceProvider();
        TestSinkProvider sink = new TestSinkProvider();
        StreamJobClient first = local(root, source, sink);
        StreamJobHandle original;
        try {
            original = session(first, 1).submit(
                    StreamSubmissionId.of("public-local-reopen"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            sink.awaitWrites(2, 5_000L);
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    original.triggerCheckpoint(StreamCommandId.of(
                            "public-local-before-reopen"))
                            .awaitCompletion(5_000L));
            StreamJobClient duplicate = null;
            try {
                duplicate = local(root, source, sink);
                fail("the same Local Stream state root was opened twice");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("already owned"));
            } finally {
                if (duplicate != null) duplicate.close();
            }
        } finally {
            first.close();
        }

        try (StreamJobClient replacement = local(root, source, sink)) {
            StreamJobHandle attached = replacement.attach(original.id());
            assertEquals(StreamJobStatus.RUNNING,
                    attached.awaitRunning(5_000L));
            sink.awaitWrites(4, 5_000L);
            assertEquals(Arrays.asList(
                    "A=1.0000000000", "B=2.0000000000",
                    "A=4.0000000000", "B=6.0000000000"),
                    sink.canonicalWrites());
            attached.cancel(StreamCommandId.of("public-local-reopen-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    private static StreamJobClient local(Path root,
            TestSourceProvider source, TestSinkProvider sink) {
        return RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.singletonList(sink),
                Collections.singletonList(new TestFunctionProvider()));
    }

    private static StreamSession session(
            StreamJobClient client, int parallelism) {
        StreamSession session = RowWeave.createStreamSession(
                "public-local", "public-local", client);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setParallelism(parallelism);
        session.setMaxParallelism(2);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 8_192L, 0, 0,
                32_768L, 8, 32, 1_048_576L));
        DataStream keyed = session.fromSource(StreamSourceSpec.exactOrdered(
                        schema(), SOURCE, SOURCE_CONTRACT, new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                digest("split"), "sequence-v1",
                                new byte[] {2}))))
                .uid("source").keyBy("adaptor").uid("adaptor-key");
        keyed.keyedAggregate(function(), schema(), new StreamStateSpec(
                        "readout", StreamStateSpec.Kind.KEYED_AGGREGATING,
                        "aggregate-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L))
                .uid("aggregate")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        SINK, SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, false)));
    }

    private static FunctionDeploymentReference function() {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(
                DEPLOYMENT, "sum", JAR, Sum.class.getName(),
                FunctionAbi.STREAM_AGGREGATE_V1,
                "stream-function-config-v1", digest(config), config,
                new FunctionResourceProfile(2_048L, 0L, 1));
    }

    private static final class TestFunctionProvider
            implements StreamFunctionProvider {
        @Override public String deploymentSetId() { return DEPLOYMENT; }
        @Override public Serializable resolve(
                FunctionDeploymentReference reference) {
            if (function().equals(reference)) return new Sum();
            throw new IllegalArgumentException("unknown local function");
        }
    }

    public static final class Sum implements StreamAggregateFunction {
        private final StreamDataRowCodec codec = new StreamDataRowCodec(schema());
        @Override public byte[] createAccumulator() { return new byte[] {0}; }
        @Override public byte[] add(DataRow row, byte[] accumulator) {
            DataRow result = row.copy();
            if (accumulator.length != 1 || accumulator[0] != 0) {
                DataRow current = codec.decode(accumulator);
                result.set(1, current.getBigDecimal(1).add(
                        row.getBigDecimal(1)));
            }
            return codec.encode(result);
        }
        @Override public DataRow getResult(byte[] accumulator) {
            DataRow result = codec.decode(accumulator);
            result.set(1, result.getBigDecimal(1).setScale(
                    10, RoundingMode.HALF_UP));
            return result;
        }
    }

    private static final class TestSourceProvider
            implements StreamSourceProvider {
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            StreamDataRowCodec codec = new StreamDataRowCodec(schema());
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private volatile boolean canceled;
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(
                        context.restoredPositionFrame()).getInt() : 0;
                @Override public StreamSourceRecord next() throws Exception {
                    int limit = context.hasRestoredPosition() ? 4 : 2;
                    if (next < limit) {
                        int index = next++;
                        DataRow row = new DataRow(2);
                        row.set(0, index % 2 == 0 ? "A" : "B");
                        row.set(1, BigDecimal.valueOf(index + 1L));
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
                    canceled = true;
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }
    }

    private static final class TestSinkProvider
            implements StreamSinkProvider {
        private final List<String> writes = new ArrayList<>();
        private final StreamUpsertCodec codec = new StreamUpsertCodec(
                new StreamSchema(Collections.singletonList(schema().field(0))),
                schema());
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamSinkWriter open(StreamSinkContext context) {
            return frame -> {
                StreamUpsertValue value = codec.decode(frame);
                DataRow row = value.afterImage();
                synchronized (writes) {
                    writes.add(row.getString(0) + '='
                            + row.getBigDecimal(1).toPlainString());
                    writes.notifyAll();
                }
            };
        }
        void awaitWrites(int count, long timeoutMillis)
                throws InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            synchronized (writes) {
                while (writes.size() < count && System.nanoTime() < deadline) {
                    TimeUnit.NANOSECONDS.timedWait(
                            writes, deadline - System.nanoTime());
                }
                assertTrue(writes.size() >= count,
                        "local Sink did not receive " + count + " writes");
            }
        }
        List<String> canonicalWrites() {
            synchronized (writes) {
                List<String> copy = new ArrayList<>(writes);
                Collections.sort(copy.subList(0, 2));
                Collections.sort(copy.subList(2, 4));
                return copy;
            }
        }
    }

    private static String digest(String value) {
        return digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder hex = new StringBuilder(64);
            for (byte one : bytes) hex.append(String.format("%02x", one));
            return hex.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
