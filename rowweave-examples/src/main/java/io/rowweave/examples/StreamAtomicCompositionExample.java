package io.rowweave.examples;

import io.rowweave.RowWeave;
import io.rowweave.Collector;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.FlatMapFunction;
import io.rowweave.DataRow;
import io.rowweave.StreamAsyncFunction;
import io.rowweave.StreamAsyncRequest;
import io.rowweave.StreamCollector;
import io.rowweave.StreamKeyedProcessFunction;
import io.rowweave.StreamProcessContext;
import io.rowweave.StreamProcessResult;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamAsyncOrdering;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.api.StreamAsyncResourceSpec;
import io.rowweave.stream.api.StreamAsyncWaitSpec;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamManagedStateSpec;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Public-SDK example compiling a mixed atomic Stream graph. */
public final class StreamAtomicCompositionExample {
    private static final String DEPLOYMENT = digest("atomic-example-functions");
    private static final String SOURCE_CONTRACT = digest("atomic-example-source");
    private static final String SINK_CONTRACT = digest("atomic-example-sink");
    private static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "value", "value", "value", DataType.LongType, false)));
    private static final StreamOutputTag AUDIT = StreamOutputTag.of(
            "audit", SCHEMA, StreamChangelogMode.APPEND,
            Collections.singletonList("value"),
            StreamEmissionLimit.of(1, 1_024L),
            StreamEmissionLimit.of(1, 1_024L),
            StreamEmissionLimit.of(1, 1_024L));

    private StreamAtomicCompositionExample() { }

    public static List<String> run() {
        StreamSession session = RowWeave.createStreamSession(
                "stream-atomic-composition-example");
        session.setParallelism(2);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 8_192, 65_536L, 16, 16,
                65_536L, 10, 32, 65_536L));

        StreamExpression value = StreamExpression.input(
                "value", PortableDataType.LONG, false);
        StreamCalcSpec identity = StreamCalcSpec.of(null,
                Collections.singletonList(
                        StreamCalcSpec.projection("value", value)));
        DataStream left = session.fromSource(source("left-source", "left"))
                .uid("left-source")
                .calc(identity, SCHEMA).uid("calc")
                .flatMap(function("flat-map", FunctionAbi.FLAT_MAP_V1,
                                PassFlatMap.class), SCHEMA,
                        StreamEmissionLimit.of(1, 1_024L))
                .uid("flat-map");
        DataStream right = session.fromSource(
                source("right-source", "right")).uid("right-source");

        StreamManagedStateSpec state = StreamManagedStateSpec.bounded(
                new StreamStateSpec("seen",
                        StreamStateSpec.Kind.KEYED_VALUE,
                        "signed-long-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                1, 16L, 8_192L, Long.BYTES);
        StreamProcessSpec process = StreamProcessSpec.keyed(
                function("process", FunctionAbi.KEYED_PROCESS_V1,
                        CountProcess.class), SCHEMA,
                Collections.singletonList("value"),
                Collections.singletonList(state), Collections.emptyList(),
                StreamEmissionLimit.of(1, 1_024L),
                StreamEmissionLimit.of(1, 1_024L))
                .withOutputTags(Collections.singletonList(AUDIT));
        StreamProcessResult processed = left.union(right).uid("union")
                .keyBy("value").uid("key-by")
                .keyedProcessWithOutputs(process, "keyed-process");

        StreamAsyncWaitSpec async = StreamAsyncWaitSpec.of(
                function("async", FunctionAbi.STREAM_ASYNC_V1,
                        PassAsync.class), SCHEMA,
                Collections.singletonList("value"),
                StreamAsyncOrdering.ORDERED,
                StreamAsyncReplaySpec.atLeastOnce(),
                StreamAsyncResourceSpec.of(2, 4_096L, 1_000L,
                        2, 1L, 10L,
                        StreamEmissionLimit.of(1, 1_024L)));
        processed.main().asyncWait(async).uid("async-wait")
                .sinkTo("main-sink", StreamSinkSpec.eagerAtLeastOnce(
                        "main-sink", SINK_CONTRACT, new byte[] {3}));
        processed.output("audit").calc(identity, SCHEMA).uid("audit-calc")
                .sinkTo("audit-sink", StreamSinkSpec.eagerAtLeastOnce(
                        "audit-sink", SINK_CONTRACT, new byte[] {4}));

        PlanArtifact artifact = session.toPlanArtifact();
        requireFeature(artifact, PlanArtifact.FEATURE_STREAM_CALC_V1);
        requireFeature(artifact, PlanArtifact.FEATURE_STREAM_FLAT_MAP_V1);
        requireFeature(artifact, PlanArtifact.FEATURE_STREAM_UNION_V1);
        requireFeature(artifact, PlanArtifact.FEATURE_STREAM_KEYED_PROCESS_V1);
        requireFeature(artifact, PlanArtifact.FEATURE_STREAM_TAGGED_OUTPUT_V1);
        requireFeature(artifact, PlanArtifact.FEATURE_STREAM_ASYNC_WAIT_V1);
        return Arrays.asList("calc", "flat-map", "union", "keyed-process",
                "tagged-output", "async-wait");
    }

    public static final class PassFlatMap implements FlatMapFunction {
        @Override public void flatMap(DataRow row, Collector output) {
            output.collect(row.copy());
        }
    }

    public static final class CountProcess
            implements StreamKeyedProcessFunction {
        @Override public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            byte[] previous = context.valueState("seen").value();
            long count = previous == null ? 1L
                    : ByteBuffer.wrap(previous).getLong() + 1L;
            context.valueState("seen").update(
                    ByteBuffer.allocate(Long.BYTES).putLong(count).array());
            output.collect(value.copy());
            output.collect(AUDIT, value.copy());
        }
    }

    public static final class PassAsync implements StreamAsyncFunction {
        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }
        @Override public byte[] invoke(StreamAsyncRequest request) {
            return request.requestFrame();
        }
        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow result = new DataRow(1);
            result.set(0, ByteBuffer.wrap(responseFrame).getLong());
            output.collect(result);
        }
    }

    private static StreamSourceSpec source(String provider, String split) {
        return StreamSourceSpec.exactOrdered(
                SCHEMA, provider, SOURCE_CONTRACT, new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest(split), "sequence-v1", new byte[] {2})));
    }

    private static FunctionDeploymentReference function(
            String id, FunctionAbi abi, Class<?> implementation) {
        byte[] configuration = id.getBytes(StandardCharsets.UTF_8);
        return new FunctionDeploymentReference(
                DEPLOYMENT, id, digest("jar-" + id),
                implementation.getName(), abi, "config-v1",
                digest(configuration), configuration,
                new FunctionResourceProfile(1_024L, 0L, 1));
    }

    private static void requireFeature(PlanArtifact artifact, String feature) {
        if (!artifact.requiredFeatures().contains(feature)) {
            throw new IllegalStateException(
                    "compiled plan is missing feature: " + feature);
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
                result.append(String.format("%02x", one));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
