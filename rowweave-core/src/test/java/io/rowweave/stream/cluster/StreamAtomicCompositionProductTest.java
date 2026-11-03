package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.Collector;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.FlatMapFunction;
import io.rowweave.DataRow;
import io.rowweave.StreamAsyncFunction;
import io.rowweave.StreamAsyncRequest;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamCollector;
import io.rowweave.StreamKeyedCoProcessFunction;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamKeyedProcessFunction;
import io.rowweave.StreamProcessContext;
import io.rowweave.StreamProcessResult;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamSession;
import io.rowweave.StreamWindowAggregateFunction;
import io.rowweave.StreamWindowContext;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamAsyncOrdering;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.api.StreamAsyncResourceSpec;
import io.rowweave.stream.api.StreamAsyncWaitSpec;
import io.rowweave.stream.api.StreamAccumulationMode;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamCoProcessSpec;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamGenericWindowSpec;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamManagedStateSpec;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamWindowAssignerSpec;
import io.rowweave.stream.api.StreamWindowResourceSpec;
import io.rowweave.stream.api.StreamWindowTriggerSpec;
import io.rowweave.stream.contract.StreamOperatorCode;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** product execution is assembled by graph, not shape. */
class StreamAtomicCompositionProductTest {
    static final String SOURCE = "atomic-composition-source-v1";
    static final String SOURCE_RIGHT =
            "atomic-composition-source-right-v1";
    static final String SOURCE_CONTRACT = digest("source-contract");
    static final String SINK_CONTRACT = digest("sink-contract");
    static final String DEPLOYMENT = digest("deployment");
    static final StreamOutputTag AUDIT_TAG = StreamOutputTag.of(
            "audit", schema(), StreamChangelogMode.APPEND,
            Collections.emptyList(), StreamEmissionLimit.of(1, 1_024L),
            StreamEmissionLimit.of(1, 1_024L),
            StreamEmissionLimit.of(1, 1_024L));

    @Test
    void runtimePublishesNodeProgressWithoutClientPolling(@TempDir Path root)
            throws Exception {
        io.rowweave.monitor.InMemoryJobMonitor monitor =
                new io.rowweave.monitor.InMemoryJobMonitor(8);
        io.rowweave.monitor.MonitorRuntime.useForTesting(monitor);
        Source source = new Source(SOURCE, 1L, 2L);
        Source right = new Source(SOURCE_RIGHT, 3L, 4L);
        Sink first = new Sink("atomic-composition-sink-1");
        Sink second = new Sink("atomic-composition-sink-2");
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Arrays.asList(source, right), Arrays.asList(first, second),
                Collections.singletonList(new Functions()))) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("runtime-monitor"));
            first.await(4, 5_000L);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            io.rowweave.monitor.JobView view;
            do {
                view = monitor.job(job.id().value());
                if (view != null && !view.stages().isEmpty()
                        && view.stream().lifetimeProgress().recordsWritten() == 8L
                        && view.stages().stream().allMatch(stage ->
                                stage.inputRows() > 0 && stage.outputRows() > 0)) break;
                Thread.sleep(10L);
            } while (System.nanoTime() < deadline);
            assertTrue(view != null && !view.stages().isEmpty());
            assertTrue(view.dagJson().contains("edges"));
            assertTrue(view.stages().stream().allMatch(stage ->
                    stage.inputRows() > 0 && stage.outputRows() > 0),
                    "every active node reports its own input and output");
            assertEquals(4L, view.stream().lifetimeProgress().recordsRead());
            assertEquals(8L, view.stream().lifetimeProgress().recordsWritten());
            assertTrue(view.stages().stream().allMatch(stage ->
                    stage.parallelism() == stage.instances().size()));
            long submitted = view.submitTime();
            job.cancel(StreamCommandId.of("runtime-monitor-cancel")).awaitCompletion(5_000L);
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!"CANCELED".equals(monitor.job(job.id().value()).state())
                    && System.nanoTime() < deadline) Thread.sleep(10L);
            assertEquals("CANCELED", monitor.job(job.id().value()).state());
            assertEquals(submitted, monitor.job(job.id().value()).submitTime());
        } finally {
            io.rowweave.monitor.MonitorRuntime.resetForTesting();
        }
    }

    @Test
    void admissionAcceptsStatelessBeforeAndAfterKeyedState() {
        StreamTestGraph admitted =
                StreamTestGraph.from(session(null)
                        .toPlanArtifact());

        assertEquals(1, admitted.product.nodes(
                StreamOperatorCode.KEYED_PROCESS).size());
        assertEquals(2, admitted.product.nodes(
                StreamOperatorCode.GENERIC_WINDOW).size());
        assertEquals(1, admitted.product.nodes(
                StreamOperatorCode.KEYED_CO_PROCESS).size());
        for (String uid : Arrays.asList(
                "process", "window", "second-window", "co-process")) {
            StreamExecutionGraph.Node node = admitted.product.executionGraph
                    .node(uid);
            assertEquals(2, node.parallelism());
            assertTrue(node.resourceRequest().maximumResidentBytes()
                            >= 65_536L,
                    uid + " must reserve its node-local state ceiling");
        }
        assertEquals(1, admitted.product.nodes(
                StreamOperatorCode.ASYNC_WAIT).size());
        assertEquals(2, admitted.sinks.size());
        assertTrue(admitted.plan.nodes().stream().anyMatch(node ->
                node.opcode() == StreamOperatorCode.CALC));
        assertTrue(admitted.plan.nodes().stream().anyMatch(node ->
                node.opcode() == StreamOperatorCode.FLAT_MAP));
        assertTrue(admitted.plan.nodes().stream().anyMatch(node ->
                node.opcode() == StreamOperatorCode.UNION));
    }

    @Test
    void localRunsMixedGraphOnceBeforeTwoSinks(@TempDir Path root)
            throws Exception {
        Source source = new Source(SOURCE, 1L, 2L);
        Source right = new Source(SOURCE_RIGHT, 3L, 4L);
        Sink first = new Sink("atomic-composition-sink-1");
        Sink second = new Sink("atomic-composition-sink-2");
        Functions functions = new Functions();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Arrays.asList(source, right),
                Arrays.asList(first, second),
                Collections.singletonList(functions))) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("atomic-composition-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            first.await(4, 5_000L);
            second.await(4, 5_000L);
            assertEquals(Arrays.asList(1L, 2L, 3L, 4L),
                    first.sortedValues());
            assertEquals(first.sortedValues(), second.sortedValues());

            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("atomic-composition-checkpoint"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(1L, job.progress().completedCheckpoints());
            assertEquals(2, functions.flatMapCalls.get());
            assertEquals(4, functions.processCalls.get());
            assertEquals(8, functions.windowCalls.get());
            assertEquals(4, functions.coLeftCalls.get());
            assertEquals(4, functions.coRightCalls.get());
            assertEquals(4, functions.asyncCalls.get());
            StreamSavepointHandle savepoint = job.stopWithSavepoint(
                    StreamCommandId.of("atomic-composition-savepoint"),
                    new StreamSavepointRequest(
                            "file-savepoint-v1", new byte[0]));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    savepoint.awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                    job.awaitTermination(5_000L));
        }
    }

    @Test
    void localRestoresWholeMixedGraphWithoutReplayingSources(
            @TempDir Path root) throws Exception {
        Source source = new Source(SOURCE, 1L, 2L);
        Source right = new Source(SOURCE_RIGHT, 3L, 4L);
        Sink first = new Sink("atomic-composition-sink-1");
        Sink second = new Sink("atomic-composition-sink-2");
        Functions functions = new Functions();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Arrays.asList(source, right),
                Arrays.asList(first, second),
                Collections.singletonList(functions))) {
            StreamSession definition = session(client);
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of(
                            "atomic-composition-recovery-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            first.await(4, 5_000L);
            second.await(4, 5_000L);

            StreamSavepointHandle savepoint = original.stopWithSavepoint(
                    StreamCommandId.of(
                            "atomic-composition-recovery-savepoint"),
                    new StreamSavepointRequest(
                            "file-savepoint-v1", new byte[0]));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    savepoint.awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                    original.awaitTermination(5_000L));

            StreamJobHandle restored = client.restore(
                    StreamSubmissionId.of(
                            "atomic-composition-recovery-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.savepoint(
                                    savepoint.savepointId()), 2, 128));
            assertEquals(StreamJobStatus.RUNNING,
                    restored.awaitRunning(5_000L));
            StreamCheckpointHandle checkpoint = restored.triggerCheckpoint(
                    StreamCommandId.of(
                            "atomic-composition-recovery-checkpoint"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(Arrays.asList(1L, 2L, 3L, 4L),
                    first.sortedValues());
            assertEquals(first.sortedValues(), second.sortedValues());
            assertEquals(1, source.restoredOpens.get());
            assertEquals(1, right.restoredOpens.get());
            restored.cancel(StreamCommandId.of(
                    "atomic-composition-recovery-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void localLiveRescalesWholeMixedGraphWithoutShapeAdmission(
            @TempDir Path root) throws Exception {
        Source source = new Source(SOURCE, 1L, 2L);
        Source right = new Source(SOURCE_RIGHT, 3L, 4L);
        Sink first = new Sink("atomic-composition-sink-1");
        Sink second = new Sink("atomic-composition-sink-2");
        Functions functions = new Functions();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Arrays.asList(source, right),
                Arrays.asList(first, second),
                Collections.singletonList(functions))) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of(
                            "atomic-composition-live-rescale-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            first.await(4, 5_000L);
            second.await(4, 5_000L);

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                            "atomic-composition-live-scale-to-three"), 3)
                            .awaitCompletion(10_000L));
            assertEquals(StreamJobStatus.RUNNING, job.status());
            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of(
                            "atomic-composition-post-rescale-checkpoint"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(10_000L));
            assertEquals(Arrays.asList(1L, 2L, 3L, 4L),
                    first.sortedValues());
            assertEquals(first.sortedValues(), second.sortedValues());
            assertEquals(1, source.restoredOpens.get());
            assertEquals(1, right.restoredOpens.get());
            job.cancel(StreamCommandId.of(
                    "atomic-composition-live-rescale-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    static StreamSession session(StreamJobClient client) {
        StreamSession session = client == null
                ? RowWeave.createStreamSession("atomic-composition")
                : RowWeave.createStreamSession("atomic-composition",
                "atomic-composition", client);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 8_192, 65_536L, 16, 16,
                65_536L, 10, 256, 65_536L));
        session.setParallelism(2);
        StreamExpression value = StreamExpression.input(
                "value", PortableDataType.LONG, false);
        StreamCalcSpec calc = StreamCalcSpec.of(null,
                Collections.singletonList(
                        StreamCalcSpec.projection("value", value)));
        StreamManagedStateSpec state = StreamManagedStateSpec.bounded(
                new StreamStateSpec("seen",
                        StreamStateSpec.Kind.KEYED_VALUE,
                        "signed-long-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                1, 16L, 8_192L, Long.BYTES);
        StreamProcessSpec process = StreamProcessSpec.keyed(
                function("process", FunctionAbi.KEYED_PROCESS_V1,
                        CountProcess.class), schema(),
                Collections.singletonList("value"),
                Collections.singletonList(state), Collections.emptyList(),
                StreamEmissionLimit.of(1, 1_024L),
                StreamEmissionLimit.of(1, 1_024L))
                .withOutputTags(Collections.singletonList(AUDIT_TAG));
        StreamAsyncWaitSpec async = StreamAsyncWaitSpec.of(
                function("async", FunctionAbi.STREAM_ASYNC_V1,
                        PassAsync.class), schema(),
                Collections.singletonList("value"),
                StreamAsyncOrdering.ORDERED,
                StreamAsyncReplaySpec.atLeastOnce(),
                StreamAsyncResourceSpec.of(2, 4_096L,
                        1_000L, 2, 1L, 10L,
                        StreamEmissionLimit.of(1, 1_024L)));
        StreamGenericWindowSpec window = StreamGenericWindowSpec.aggregate(
                StreamWindowAssignerSpec.countTumbling(1L),
                StreamWindowTriggerSpec.count(1L),
                StreamAccumulationMode.DISCARDING, 0L,
                function("window", FunctionAbi.STREAM_WINDOW_AGGREGATE_V1,
                        PassWindow.class), schema(),
                Collections.singletonList("value"),
                new StreamStateSpec("window-state",
                        StreamStateSpec.Kind.KEYED_AGGREGATING,
                        "atomic-window-v1", 1,
                        StreamStateSpec.TtlMode.NONE, 0L),
                StreamWindowResourceSpec.of(8, 1_000L,
                        8_192L, 32_768L, 8,
                        StreamEmissionLimit.of(1, 1_024L)));
        StreamCoProcessSpec coProcess = StreamCoProcessSpec.keyed(
                function("co-process", FunctionAbi.KEYED_CO_PROCESS_V1,
                        LeftOnlyCoProcess.class), schema(),
                Collections.singletonList("value"),
                Collections.singletonList(StreamManagedStateSpec.bounded(
                        new StreamStateSpec("co-seen",
                                StreamStateSpec.Kind.KEYED_VALUE,
                                "signed-long-v1", 1,
                                StreamStateSpec.TtlMode.PROCESSING_TIME,
                                60_000L),
                        1, 16L, 8_192L, Long.BYTES)),
                Collections.emptyList(),
                StreamEmissionLimit.of(1, 1_024L),
                StreamEmissionLimit.of(1, 1_024L),
                StreamEmissionLimit.of(1, 1_024L));
        DataStream left = session.fromSource(source(SOURCE, "left"))
                .uid("source")
                .calc(calc, schema()).uid("calc")
                .flatMap(function("flat-map", FunctionAbi.FLAT_MAP_V1,
                                PassFlatMap.class), schema(),
                        StreamEmissionLimit.of(1, 1_024L))
                .uid("flat-map")
                .keyBy("value").uid("left-key");
        DataStream right = session.fromSource(
                source(SOURCE_RIGHT, "right")).uid("source-right")
                .keyBy("value").uid("right-key");
        StreamProcessResult processed = left.union(right).uid("union")
                .keyedProcessWithOutputs(process, "process");
        DataStream windowed = processed.main()
                .calc(calc, schema()).uid("stateful-calc")
                .genericWindow(window).uid("window")
                .genericWindow(window).uid("second-window");
        DataStream coRight = processed.output("audit")
                .keyBy("value").uid("co-right-key");
        windowed.keyedCoProcess(coRight, coProcess).uid("co-process")
                .asyncWait(async).uid("async")
                .sinkTo("sink-1", StreamSinkSpec.eagerAtLeastOnce(
                "atomic-composition-sink-1", SINK_CONTRACT,
                new byte[] {3}));
        processed.output("audit").calc(calc, schema()).uid("audit-calc")
                .sinkTo("sink-2", StreamSinkSpec.eagerAtLeastOnce(
                "atomic-composition-sink-2", SINK_CONTRACT,
                new byte[] {4}));
        return session;
    }

    private static StreamSourceSpec source(String provider, String split) {
        return StreamSourceSpec.exactOrdered(
                schema(), provider, SOURCE_CONTRACT, new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest(split), "sequence-v1", new byte[] {2})));
    }

    static StreamSchema schema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("value", "value", "value",
                        DataType.LongType, false)));
    }

    static FunctionDeploymentReference function(
            String id, FunctionAbi abi, Class<?> implementation) {
        byte[] config = id.getBytes(StandardCharsets.UTF_8);
        return new FunctionDeploymentReference(
                DEPLOYMENT, id, digest("jar-" + id),
                implementation.getName(), abi, "config-v1",
                digest(config), config,
                new FunctionResourceProfile(1_024L, 0L, 1));
    }

    public static final class PassFlatMap implements FlatMapFunction {
        private final AtomicInteger calls;
        PassFlatMap(AtomicInteger calls) { this.calls = calls; }
        @Override public void flatMap(DataRow row, Collector output) {
            calls.incrementAndGet();
            output.collect(row.copy());
        }
    }

    public static final class CountProcess
            implements StreamKeyedProcessFunction {
        private final AtomicInteger calls;
        CountProcess(AtomicInteger calls) { this.calls = calls; }
        @Override public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            calls.incrementAndGet();
            byte[] current = context.valueState("seen").value();
            long count = current == null ? 1L
                    : ByteBuffer.wrap(current).getLong() + 1L;
            context.valueState("seen").update(
                    ByteBuffer.allocate(Long.BYTES).putLong(count).array());
            output.collect(value.copy());
            output.collect(AUDIT_TAG, value.copy());
        }
    }

    public static final class PassAsync implements StreamAsyncFunction {
        private final AtomicInteger calls;
        PassAsync(AtomicInteger calls) { this.calls = calls; }
        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }
        @Override public byte[] invoke(StreamAsyncRequest request) {
            calls.incrementAndGet();
            return request.requestFrame();
        }
        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow row = new DataRow(1);
            row.set(0, ByteBuffer.wrap(responseFrame).getLong());
            output.collect(row);
        }
    }

    public static final class PassWindow
            implements StreamWindowAggregateFunction {
        private final AtomicInteger calls;
        PassWindow(AtomicInteger calls) { this.calls = calls; }
        @Override public byte[] createAccumulator() {
            return new byte[Long.BYTES];
        }
        @Override public byte[] add(DataRow value, byte[] accumulator) {
            calls.incrementAndGet();
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(value.getLong(0)).array();
        }
        @Override public byte[] merge(byte[] left, byte[] right) {
            return left;
        }
        @Override public DataRow getResult(byte[] accumulator,
                StreamWindowContext context) {
            DataRow result = new DataRow(1);
            result.set(0, ByteBuffer.wrap(accumulator).getLong());
            return result;
        }
    }

    public static final class LeftOnlyCoProcess
            implements StreamKeyedCoProcessFunction {
        private final AtomicInteger leftCalls;
        private final AtomicInteger rightCalls;
        LeftOnlyCoProcess(AtomicInteger leftCalls,
                AtomicInteger rightCalls) {
            this.leftCalls = leftCalls;
            this.rightCalls = rightCalls;
        }
        @Override public void processLeft(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            leftCalls.incrementAndGet();
            output.collect(value.copy());
        }
        @Override public void processRight(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            rightCalls.incrementAndGet();
        }
    }

    private static final class Functions implements StreamFunctionProvider {
        final AtomicInteger flatMapCalls = new AtomicInteger();
        final AtomicInteger processCalls = new AtomicInteger();
        final AtomicInteger windowCalls = new AtomicInteger();
        final AtomicInteger coLeftCalls = new AtomicInteger();
        final AtomicInteger coRightCalls = new AtomicInteger();
        final AtomicInteger asyncCalls = new AtomicInteger();
        @Override public String deploymentSetId() { return DEPLOYMENT; }
        @Override public Serializable resolve(
                FunctionDeploymentReference reference) {
            if (reference.equals(function("flat-map",
                    FunctionAbi.FLAT_MAP_V1, PassFlatMap.class))) {
                return new PassFlatMap(flatMapCalls);
            }
            if (reference.equals(function("process",
                    FunctionAbi.KEYED_PROCESS_V1, CountProcess.class))) {
                return new CountProcess(processCalls);
            }
            if (reference.equals(function("async",
                    FunctionAbi.STREAM_ASYNC_V1, PassAsync.class))) {
                return new PassAsync(asyncCalls);
            }
            if (reference.equals(function("window",
                    FunctionAbi.STREAM_WINDOW_AGGREGATE_V1,
                    PassWindow.class))) {
                return new PassWindow(windowCalls);
            }
            if (reference.equals(function("co-process",
                    FunctionAbi.KEYED_CO_PROCESS_V1,
                    LeftOnlyCoProcess.class))) {
                return new LeftOnlyCoProcess(coLeftCalls, coRightCalls);
            }
            throw new IllegalArgumentException("unknown function");
        }
    }

    static final class Source implements StreamSourceProvider {
        private final String providerKey;
        private final long[] values;
        private final AtomicInteger restoredOpens = new AtomicInteger();
        Source(String providerKey, long... values) {
            this.providerKey = providerKey;
            this.values = values.clone();
        }
        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            if (context.hasRestoredPosition()) restoredOpens.incrementAndGet();
            StreamDataRowCodec codec = new StreamDataRowCodec(schema());
            Semaphore wakeups = new Semaphore(0);
            return new StreamSourceReader() {
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(context.restoredPositionFrame())
                        .getInt() : 0;
                private volatile boolean canceled;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next < values.length) {
                        DataRow row = new DataRow(1);
                        row.set(0, values[next++]);
                        return StreamSourceRecord.of(
                                codec.encode(row), position(next));
                    }
                    while (!canceled) {
                        if (wakeups.tryAcquire(20L,
                                TimeUnit.MILLISECONDS)) return null;
                    }
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
    }

    private static final class Sink implements StreamSinkProvider {
        private final String providerKey;
        private final List<Long> values = new ArrayList<>();
        private final StreamDataRowCodec codec = new StreamDataRowCodec(schema());
        Sink(String providerKey) { this.providerKey = providerKey; }
        @Override public String providerKey() { return providerKey; }
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
                        && System.nanoTime() < deadline) values.wait(10L);
                assertEquals(count, values.size());
            }
        }
        List<Long> values() {
            synchronized (values) {
                return new ArrayList<>(values);
            }
        }
        List<Long> sortedValues() {
            List<Long> result = values();
            Collections.sort(result);
            return result;
        }
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(hash.length * 2);
            for (byte current : hash) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
