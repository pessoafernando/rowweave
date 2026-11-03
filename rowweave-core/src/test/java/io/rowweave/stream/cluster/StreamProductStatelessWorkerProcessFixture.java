package io.rowweave.stream.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FilterFunction;
import io.rowweave.FlatMapFunction;
import io.rowweave.Collector;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.StreamCollector;
import io.rowweave.StreamBroadcastProcessContext;
import io.rowweave.StreamKeyedBroadcastProcessContext;
import io.rowweave.StreamKeyedBroadcastProcessFunction;
import io.rowweave.StreamKeyedProcessFunction;
import io.rowweave.StreamProcessContext;
import io.rowweave.StreamTimerContext;
import io.rowweave.StreamValueState;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import io.rowweave.stream.runtime.StreamSourceEvent;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Independent Worker fixture for the public MAP/FILTER cluster product path. */
public final class StreamProductStatelessWorkerProcessFixture {
    static final int RECORDS = 1_000;
    static final String SOURCE_PROVIDER = "stateless-source-v1";
    static final String SINK_PROVIDER = "stateless-sink-v1";
    static final String SINK_PROVIDER_A = "stateless-sink-a-v1";
    static final String SINK_PROVIDER_B = "stateless-sink-b-v1";
    static final String UNION_SOURCE_LEFT = "union-source-left-v1";
    static final String UNION_SOURCE_RIGHT = "union-source-right-v1";
    static final String UNION_SINK_PROVIDER = "union-sink-v1";
    static final String SELF_UNION_SINK_PROVIDER = "self-union-sink-v1";
    static final String PROCESS_SOURCE_PROVIDER = "process-source-v1";
    static final String PROCESS_MAIN_SINK_PROVIDER = "process-main-sink-v1";
    static final String PROCESS_TAG_SINK_PROVIDER = "process-tag-sink-v1";
    static final String BROADCAST_LEFT_SOURCE_PROVIDER = "left-source-v1";
    static final String BROADCAST_RIGHT_SOURCE_PROVIDER = "right-source-v1";
    static final String BROADCAST_SINK_PROVIDER = "sink-v1";
    static final String SOURCE_CONTRACT = digestText("stateless-source-contract");
    static final String SINK_CONTRACT = digestText("stateless-sink-contract");
    static final String DEPLOYMENT_SET = digestText("stateless-deployment-set");
    static final String JAR_DIGEST = digestText("stateless-functions-jar");

    private StreamProductStatelessWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("role and exchange directory required");
        }
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        List<String> capabilities = new ArrayList<>(Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.UPSERT_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                PortableStreamPlan.CALC_CAPABILITY,
                PortableStreamPlan.FLAT_MAP_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY,
                PortableStreamPlan.UNION_CAPABILITY,
                PortableStreamPlan.KEYED_PROCESS_CAPABILITY,
                PortableStreamPlan.KEYED_CO_PROCESS_CAPABILITY,
                PortableStreamPlan.BROADCAST_STATE_CAPABILITY,
                PortableStreamPlan.SOURCE_END_OF_INPUT_CAPABILITY,
                PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY,
                PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                StreamWorkerCapabilityProfile
                        .CHECKPOINT_TRANSFER_CAPABILITY));
        boolean sourceRole = "source".equals(role)
                || "process-source".equals(role)
                || "broadcast-source".equals(role);
        capabilities.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                + DEPLOYMENT_SET);
        capabilities.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                + digestText("deployment-set"));
        StreamWorkerCapabilityProfile profile = new StreamWorkerCapabilityProfile(
                "stateless-worker-" + role, "stateless-incarnation-" + role,
                2, 1, capabilities,
                8, 64, 4_096, 1_048_576L, 32);
        java.util.Map<String, DurableStreamSourceProvider> sources =
                new java.util.LinkedHashMap<>();
        if ("source".equals(role)) {
            DurableStreamSourceProvider source = new IntegerSourceProvider(
                    SOURCE_PROVIDER, 0, RECORDS);
            sources.put(source.providerKey(), source);
        } else if ("union-source".equals(role)) {
            DurableStreamSourceProvider left = new IntegerSourceProvider(
                    UNION_SOURCE_LEFT, 0, RECORDS / 4);
            DurableStreamSourceProvider right = new IntegerSourceProvider(
                    UNION_SOURCE_RIGHT, RECORDS / 4, RECORDS / 4);
            sources.put(left.providerKey(), left);
            sources.put(right.providerKey(), right);
        } else if ("process-source".equals(role)) {
            DurableStreamSourceProvider source =
                    new RepeatingIntegerSourceProvider();
            sources.put(source.providerKey(), source);
        } else if ("broadcast-source".equals(role)) {
            DurableStreamSourceProvider left =
                    new BroadcastLeftSourceProvider();
            DurableStreamSourceProvider right =
                    new BroadcastRightSourceProvider();
            sources.put(left.providerKey(), left);
            sources.put(right.providerKey(), right);
        }
        EagerStreamSinkProvider sink = sink(role, exchange);
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile,
                sources,
                sink == null ? Collections.emptyMap()
                        : Collections.singletonMap(sink.providerKey(), sink),
                reference -> resolveFunction(reference, exchange),
                exchange.resolve(
                        "stateless-checkpoint-store-" + role))) {
            Files.write(exchange.resolve("stateless-" + role + ".port"),
                    Integer.toString(server.port()).getBytes(StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("stateless-shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    static DataSchema integerMeta() {
        return new DataSchema(new Field[] {
                new Field("value", "value", DataType.IntegerType)});
    }

    static DataSchema longMeta() {
        return new DataSchema(new Field[] {
                new Field("value", "value", DataType.LongType)});
    }

    static FunctionDeploymentReference function(
            String deploymentId, FunctionAbi abi, String configuration) {
        byte[] frame = configuration.getBytes(StandardCharsets.UTF_8);
        return new FunctionDeploymentReference(
                DEPLOYMENT_SET, deploymentId, JAR_DIGEST,
                functionClass(abi),
                abi, "stream-function-config-v1", digestText(configuration),
                frame, new FunctionResourceProfile(1_024L, 0L,
                abi == FunctionAbi.FLAT_MAP_V1 ? 2 : 1));
    }

    static java.io.Serializable resolveFunction(
            FunctionDeploymentReference reference, Path exchange) {
        if (function("filter-even", FunctionAbi.FILTER_V1,
                "filter-even").equals(reference)) {
            return new EvenFilter();
        }
        if (function("map-times-ten", FunctionAbi.MAP_V1,
                "map-times-ten").equals(reference)) {
            return new TimesTenMap(exchange.resolve("map-invocations"));
        }
        if (function("flat-map-even", FunctionAbi.FLAT_MAP_V1,
                "flat-map-even").equals(reference)) {
            return new DuplicateEvenFlatMap();
        }
        if (function("keyed-count", FunctionAbi.KEYED_PROCESS_V1,
                "keyed-count").equals(reference)) {
            return new KeyedCountProcess();
        }
        if (function("tag-filter", FunctionAbi.FILTER_V1,
                "tag-filter").equals(reference)) {
            return new TagFilter();
        }
        if (reference.functionAbi() == FunctionAbi.KEYED_CO_PROCESS_V1
                && digestText("deployment-set").equals(
                        reference.deploymentSetId())) {
            return new BroadcastProcess();
        }
        throw new IllegalArgumentException(
                "signed Stream function deployment is unavailable");
    }

    public static final class EvenFilter implements FilterFunction {
        @Override public boolean filter(DataRow row) {
            return row.getInteger(0) % 2 == 0;
        }
    }

    public static final class TimesTenMap implements MapFunction {
        private final Path invocationCount;
        private final AtomicInteger invocations = new AtomicInteger();

        public TimesTenMap(Path invocationCount) {
            this.invocationCount = invocationCount;
        }

        @Override public DataRow map(DataRow row) {
            int current = invocations.incrementAndGet();
            try {
                Files.write(invocationCount, Integer.toString(current)
                        .getBytes(StandardCharsets.US_ASCII));
            } catch (java.io.IOException failure) {
                throw new IllegalStateException(failure);
            }
            DataRow mapped = new DataRow(1);
            mapped.set(0, row.getInteger(0) * 10L);
            return mapped;
        }
    }

    public static final class DuplicateEvenFlatMap implements FlatMapFunction {
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

    public static final class KeyedCountProcess
            implements StreamKeyedProcessFunction {
        @Override public void processElement(DataRow row,
                StreamProcessContext context, StreamCollector output) {
            StreamValueState state = context.valueState("count");
            byte[] prior = state.value();
            long count = prior == null ? 1L
                    : ByteBuffer.wrap(prior).getLong() + 1L;
            state.update(ByteBuffer.allocate(Long.BYTES).putLong(count).array());
            DataRow result = new DataRow(2);
            result.set(0, row.getInteger(0));
            result.set(1, count);
            output.collect(result);
            if (count == 2L) {
                DataRow tagged = new DataRow(2);
                tagged.set(0, "K" + row.getInteger(0));
                tagged.set(1, count);
                output.collect(processTag(), tagged);
            }
            if (count == 2L) {
                context.timerService().registerTimer(
                        "emit-latest", new byte[0],
                        context.currentProcessingTime() + 50L);
            }
        }

        @Override public void onTimer(long timestamp,
                StreamTimerContext context, StreamCollector output) {
            byte[] current = context.valueState("count").value();
            DataRow result = new DataRow(2);
            result.set(0, context.currentKey().getInteger(0));
            result.set(1, ByteBuffer.wrap(current).getLong());
            output.collect(result);
        }
    }

    public static final class TagFilter implements FilterFunction {
        @Override public boolean filter(DataRow row) {
            return row.getString(0).startsWith("K");
        }
    }

    private static String functionClass(FunctionAbi abi) {
        if (abi == FunctionAbi.FILTER_V1) return EvenFilter.class.getName();
        if (abi == FunctionAbi.MAP_V1) return TimesTenMap.class.getName();
        if (abi == FunctionAbi.FLAT_MAP_V1) {
            return DuplicateEvenFlatMap.class.getName();
        }
        if (abi == FunctionAbi.KEYED_PROCESS_V1) {
            return KeyedCountProcess.class.getName();
        }
        if (abi == FunctionAbi.KEYED_CO_PROCESS_V1) {
            return BroadcastProcess.class.getName();
        }
        throw new IllegalArgumentException("unsupported test function ABI");
    }

    static StreamSchema processOutputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("value", "value", "value",
                        DataType.IntegerType, false),
                new StreamField("count", "count", "count",
                        DataType.LongType, false)));
    }

    static StreamOutputTag processTag() {
        return StreamOutputTag.of("second-seen", processTagSchema(),
                StreamChangelogMode.UPSERT,
                Collections.singletonList("code"),
                StreamEmissionLimit.of(1, 1_024L),
                StreamEmissionLimit.of(1, 1_024L),
                StreamEmissionLimit.of(1, 1_024L));
    }

    static StreamSchema processTagSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("code", "code", "code",
                        DataType.StringType, false),
                new StreamField("total", "total", "total",
                        DataType.LongType, false)));
    }

    static DataSchema processOutputMeta() {
        return new DataSchema(new Field[] {
                new Field("value", "value", DataType.IntegerType),
                new Field("count", "count", DataType.LongType)});
    }

    static DataSchema processTagMeta() {
        return new DataSchema(new Field[] {
                new Field("code", "code", DataType.StringType),
                new Field("total", "total", DataType.LongType)});
    }

    static DurableStreamSourceProvider processSourceProvider() {
        return new RepeatingIntegerSourceProvider();
    }

    static EagerStreamSinkProvider processMainSinkProvider(Path result) {
        return new ProcessSummarySinkProvider(PROCESS_MAIN_SINK_PROVIDER,
                result, 30, processOutputMeta(), false);
    }

    static EagerStreamSinkProvider processTagSinkProvider(Path result) {
        return new ProcessSummarySinkProvider(PROCESS_TAG_SINK_PROVIDER,
                result, 10, processTagMeta(), true);
    }

    private static EagerStreamSinkProvider sink(String role, Path exchange) {
        if ("sink".equals(role)) {
            return new LongDigestSinkProvider(SINK_PROVIDER,
                    exchange.resolve("stateless-result"));
        }
        if ("sink-a".equals(role)) {
            return new LongDigestSinkProvider(SINK_PROVIDER_A,
                    exchange.resolve("stateless-result-a"));
        }
        if ("sink-b".equals(role)) {
            return new LongDigestSinkProvider(SINK_PROVIDER_B,
                    exchange.resolve("stateless-result-b"));
        }
        if ("union-sink".equals(role)) {
            return new IntegerSumSinkProvider(
                    UNION_SINK_PROVIDER,
                    exchange.resolve("stateless-union-result"), RECORDS / 2);
        }
        if ("self-union-sink".equals(role)) {
            return new IntegerSumSinkProvider(
                    SELF_UNION_SINK_PROVIDER,
                    exchange.resolve("stateless-self-union-result"),
                    RECORDS * 2);
        }
        if ("process-main-sink".equals(role)) {
            return new ProcessSummarySinkProvider(
                    PROCESS_MAIN_SINK_PROVIDER,
                    exchange.resolve("process-main-result"), 30,
                    processOutputMeta(), false);
        }
        if ("process-tag-sink".equals(role)) {
            return new ProcessSummarySinkProvider(
                    PROCESS_TAG_SINK_PROVIDER,
                    exchange.resolve("process-tag-result"), 10,
                    processTagMeta(), true);
        }
        if ("broadcast-sink".equals(role)) {
            return new BroadcastSinkProvider(
                    exchange.resolve("broadcast-result"));
        }
        return null;
    }

    public static final class BroadcastProcess
            implements StreamKeyedBroadcastProcessFunction {
        @Override public void processKeyed(DataRow value,
                StreamKeyedBroadcastProcessContext context,
                StreamCollector output) {
            byte[] rule = context.broadcastState("rules").get(
                    ByteBuffer.allocate(Long.BYTES)
                            .putLong(value.getLong(0)).array());
            DataRow result = new DataRow(2);
            result.set(0, value.getLong(0));
            result.set(1, (rule == null ? "missing"
                    : new String(rule, StandardCharsets.UTF_8))
                    + ':' + value.getString(1));
            output.collect(result);
        }

        @Override public void processBroadcast(DataRow value,
                StreamBroadcastProcessContext context,
                StreamCollector output) {
            context.broadcastState("rules").put(
                    ByteBuffer.allocate(Long.BYTES)
                            .putLong(value.getLong(0)).array(),
                    value.getString(1).getBytes(StandardCharsets.UTF_8));
        }
    }

    private static DataSchema broadcastMeta() {
        return new DataSchema(new Field[] {
                new Field("id", "id", DataType.LongType),
                new Field("value", "value", DataType.StringType)});
    }

    private static final class BroadcastLeftSourceProvider
            implements DurableStreamSourceProvider {
        @Override public String providerKey() {
            return BROADCAST_LEFT_SOURCE_PROVIDER;
        }
        @Override public String contractDigest() {
            return digestText("left-contract");
        }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(broadcastMeta());
            return new StreamSourceReader() {
                private boolean emitted;
                private boolean cancelled;
                private boolean wakeup;
                @Override public synchronized StreamSourceRecord next()
                        throws Exception {
                    if (!emitted) {
                        emitted = true;
                        DataRow row = new DataRow(2);
                        row.set(0, 7L);
                        row.set(1, "gallery");
                        return StreamSourceRecord.of(serializer.serialize(row),
                                broadcastPosition(1));
                    }
                    while (!cancelled && !wakeup) wait();
                    wakeup = false;
                    return null;
                }
                @Override public synchronized byte[] checkpointPositionFrame() {
                    return broadcastPosition(emitted ? 1 : 0);
                }
                @Override public synchronized void wakeUp() {
                    wakeup = true;
                    notifyAll();
                }
                @Override public synchronized void cancel() {
                    cancelled = true;
                    notifyAll();
                }
                @Override public synchronized void close() { cancel(); }
            };
        }
    }

    private static final class BroadcastRightSourceProvider
            implements DurableStreamSourceProvider {
        @Override public String providerKey() {
            return BROADCAST_RIGHT_SOURCE_PROVIDER;
        }
        @Override public String contractDigest() {
            return digestText("right-contract");
        }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(broadcastMeta());
            return new StreamSourceReader() {
                private int next;
                @Override public StreamSourceRecord next() {
                    throw new AssertionError("bounded broadcast uses nextEvent");
                }
                @Override public StreamSourceEvent nextEvent() {
                    if (next++ == 0) {
                        DataRow row = new DataRow(2);
                        row.set(0, 7L);
                        row.set(1, "approved");
                        return StreamSourceEvent.record(StreamSourceRecord.of(
                                serializer.serialize(row),
                                broadcastPosition(1)));
                    }
                    return StreamSourceEvent.endOfInput();
                }
                @Override public byte[] checkpointPositionFrame() {
                    return broadcastPosition(Math.min(next, 1));
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class BroadcastSinkProvider
            implements EagerStreamSinkProvider {
        private final Path result;
        private BroadcastSinkProvider(Path result) { this.result = result; }
        @Override public String providerKey() { return BROADCAST_SINK_PROVIDER; }
        @Override public String contractDigest() {
            return digestText("sink-contract");
        }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                private final RowSerializer serializer =
                        new RowSerializer(broadcastMeta());
                @Override public void write(byte[] recordFrame)
                        throws Exception {
                    Files.write(result, serializer.deserialize(recordFrame)
                            .getString(1).getBytes(StandardCharsets.UTF_8));
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static byte[] broadcastPosition(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private static final class RepeatingIntegerSourceProvider
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return PROCESS_SOURCE_PROVIDER; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(integerMeta());
            return new StreamSourceReader() {
                private boolean cancelled;
                private boolean wakeUp;
                private int next = request.hasRestoredPosition()
                        ? ByteBuffer.wrap(request.restoredPositionFrame()).getInt()
                        : 0;
                @Override public synchronized StreamSourceRecord next()
                        throws Exception {
                    if (next < 20) {
                        DataRow row = new DataRow(1);
                        row.set(0, next++ % 10);
                        return StreamSourceRecord.of(serializer.serialize(row),
                                ByteBuffer.allocate(Integer.BYTES)
                                        .putInt(next).array());
                    }
                    while (!cancelled && !wakeUp) wait();
                    wakeUp = false;
                    return null;
                }
                @Override public synchronized byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(Integer.BYTES)
                            .putInt(next).array();
                }
                @Override public synchronized void wakeUp() {
                    wakeUp = true;
                    notifyAll();
                }
                @Override public synchronized void cancel() {
                    cancelled = true;
                    notifyAll();
                }
                @Override public synchronized void close() { cancel(); }
            };
        }
    }

    private static final class ProcessSummarySinkProvider
            implements EagerStreamSinkProvider {
        private final String providerKey;
        private final Path result;
        private final int expectedCount;
        private final DataSchema dataSchema;
        private final boolean upsert;
        private int count;
        private long sum;

        private ProcessSummarySinkProvider(String providerKey, Path result,
                int expectedCount, DataSchema dataSchema, boolean upsert) {
            this.providerKey = providerKey;
            this.result = result;
            this.expectedCount = expectedCount;
            this.dataSchema = dataSchema;
            this.upsert = upsert;
        }

        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                private final RowSerializer serializer =
                        new RowSerializer(dataSchema);
                @Override public void write(byte[] recordFrame)
                        throws Exception {
                    byte[] rowFrame = recordFrame;
                    if (upsert) {
                        StreamUpsertRecord record = StreamUpsertRecordCodec
                                .decode(recordFrame, 4_096, 4_096);
                        if (!record.hasAfterImage()) return;
                        rowFrame = record.afterImageFrame();
                    }
                    record(serializer.deserialize(rowFrame).getLong(1));
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }

        private synchronized void record(long value) throws Exception {
            sum += value;
            if (++count == expectedCount) {
                Files.write(result, (count + ":" + sum).getBytes(
                        StandardCharsets.US_ASCII));
            }
        }
    }

    private static final class IntegerSourceProvider
            implements DurableStreamSourceProvider {
        private final String providerKey;
        private final int start;
        private final int count;
        private IntegerSourceProvider(String providerKey, int start,
                int count) {
            this.providerKey = providerKey;
            this.start = start;
            this.count = count;
        }
        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            RowSerializer serializer = new RowSerializer(integerMeta());
            return new StreamSourceReader() {
                private boolean cancelled;
                private boolean wakeUp;
                private int next = request.hasRestoredPosition()
                        ? restoredNext(request.restoredPositionFrame()) : 0;
                @Override public synchronized StreamSourceRecord next()
                        throws Exception {
                    if (next < count) {
                        DataRow row = new DataRow(1);
                        row.set(0, start + next++);
                        byte[] frame = serializer.serialize(row);
                        return StreamSourceRecord.of(frame, position(next));
                    }
                    while (!cancelled && !wakeUp) wait();
                    wakeUp = false;
                    return null;
                }
                @Override public synchronized byte[] checkpointPositionFrame() {
                    return position(next);
                }
                @Override public synchronized void wakeUp() {
                    wakeUp = true;
                    notifyAll();
                }
                @Override public synchronized void cancel() {
                    cancelled = true;
                    notifyAll();
                }
                @Override public synchronized void close() {
                    cancelled = true;
                    notifyAll();
                }
            };
        }

        private int restoredNext(byte[] frame) {
            if (frame.length != Integer.BYTES) {
                throw new IllegalArgumentException(
                        "stateless source position is invalid");
            }
            int restored = ByteBuffer.wrap(frame).getInt();
            if (restored < 0 || restored > count) {
                throw new IllegalArgumentException(
                        "stateless source position is out of range");
            }
            return restored;
        }

        private byte[] position(int next) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(next).array();
        }
    }

    private static final class IntegerSumSinkProvider
            implements EagerStreamSinkProvider {
        private final Path result;
        private final String providerKey;
        private final int expectedCount;
        private IntegerSumSinkProvider(String providerKey, Path result,
                int expectedCount) {
            this.providerKey = providerKey;
            this.result = result;
            this.expectedCount = expectedCount;
        }
        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                private final RowSerializer serializer =
                        new RowSerializer(integerMeta());
                private int count;
                private long sum;
                @Override public synchronized void write(byte[] recordFrame)
                        throws Exception {
                    sum += serializer.deserialize(recordFrame).getInteger(0);
                    if (++count == expectedCount) {
                        Files.write(result, (count + ":" + sum).getBytes(
                                StandardCharsets.US_ASCII));
                    }
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class LongDigestSinkProvider
            implements EagerStreamSinkProvider {
        private final Path result;
        private final String providerKey;
        private LongDigestSinkProvider(String providerKey, Path result) {
            this.providerKey = providerKey;
            this.result = result;
        }
        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                private final MessageDigest digest = digest();
                private int count;
                @Override public synchronized void write(byte[] recordFrame)
                        throws Exception {
                    digest.update(recordFrame);
                    if (++count == RECORDS / 2) {
                        Files.write(result, (count + ":" + hex(digest.digest()))
                                .getBytes(StandardCharsets.US_ASCII));
                    }
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    static String digestText(String value) {
        return hex(digest().digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int current = value[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }
}
