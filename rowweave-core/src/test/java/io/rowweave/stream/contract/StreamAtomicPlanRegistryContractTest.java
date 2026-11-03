package io.rowweave.stream.contract;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** additive atomic opcode, ABI, capability and arity registry. */
class StreamAtomicPlanRegistryContractTest {

    @Test
    void atomicOpcodesAndFunctionAbisHavePermanentAdditiveWireIds() {
        assertOpcode(StreamOperatorCode.CALC, 9, 1, 1,
                StreamOperatorCode.BindingKind.CALC);
        assertOpcode(StreamOperatorCode.FLAT_MAP, 10, 1, 1,
                StreamOperatorCode.BindingKind.FUNCTION);
        assertOpcode(StreamOperatorCode.UNION, 11, 2, 2,
                StreamOperatorCode.BindingKind.NONE);
        assertOpcode(StreamOperatorCode.ASSIGN_EVENT_TIME, 12, 1, 1,
                StreamOperatorCode.BindingKind.TIME);
        assertOpcode(StreamOperatorCode.KEYED_PROCESS, 13, 1, 1,
                StreamOperatorCode.BindingKind.FUNCTION);
        assertOpcode(StreamOperatorCode.KEYED_CO_PROCESS, 14, 2, 2,
                StreamOperatorCode.BindingKind.FUNCTION);
        assertOpcode(StreamOperatorCode.GENERIC_WINDOW, 15, 1, 1,
                StreamOperatorCode.BindingKind.WINDOW);
        assertOpcode(StreamOperatorCode.ASYNC_WAIT, 16, 1, 1,
                StreamOperatorCode.BindingKind.ASYNC);

        assertEquals(9, FunctionAbi.KEYED_PROCESS_V1.wireId());
        assertEquals(10, FunctionAbi.KEYED_CO_PROCESS_V1.wireId());
        assertEquals(11, FunctionAbi.STREAM_WINDOW_AGGREGATE_V1.wireId());
        assertEquals(12, FunctionAbi.STREAM_WINDOW_PROCESS_V1.wireId());
        assertEquals(13, FunctionAbi.STREAM_ASYNC_V1.wireId());
        assertEquals(14, FunctionAbi.STREAM_RETRACT_AGGREGATE_V2.wireId());
        assertEquals(15,
                FunctionAbi.STREAM_RETRACT_WINDOW_AGGREGATE_V2.wireId());
        assertEquals(16, FunctionAbi.MAP_PARTITION_V1.wireId());
        assertEquals(5, StreamStateDescriptor.Kind.KEYED_LIST.wireId());

        assertEquals(StreamOperatorCode.UNION,
                StreamOperatorCode.fromWire(11));
        assertEquals(FunctionAbi.STREAM_ASYNC_V1,
                FunctionAbi.fromWireId(13));
        assertEquals(StreamStateDescriptor.Kind.KEYED_LIST,
                StreamStateDescriptor.Kind.fromWire(5));
        assertFailure("UNKNOWN_OPCODE", () -> StreamOperatorCode.fromWire(17));
        assertEquals(FunctionAbi.STREAM_RETRACT_AGGREGATE_V2,
                FunctionAbi.fromWireId(14));
        assertEquals(FunctionAbi.STREAM_RETRACT_WINDOW_AGGREGATE_V2,
                FunctionAbi.fromWireId(15));
        assertEquals(FunctionAbi.MAP_PARTITION_V1,
                FunctionAbi.fromWireId(16));
        assertThrows(IllegalArgumentException.class,
                () -> FunctionAbi.fromWireId(17));
    }

    @Test
    void unionIsCanonicalBinaryAndRequiresItsCapability() {
        PortableStreamPlan plan = unionPlan(true);

        assertEquals(plan, PortableStreamPlanCodec.decode(
                PortableStreamPlanCodec.encode(plan)));
        assertEquals(2, plan.nodes().get(2).inputOperatorUids().size());
        assertEquals(Arrays.asList(
                        PortableStreamPlan.UNION_CAPABILITY,
                        PortableStreamPlan.STREAM_CAPABILITY),
                plan.requiredCapabilities());

        assertFailure("MISSING_CAPABILITY", () -> unionPlan(false));
    }

    @Test
    void portableUnionRemainsBinaryToPreserveTheArtifactEnvelope() {
        PortableRowSchema schema = schema();
        List<String> inputs = new ArrayList<>();
        List<String> fingerprints = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            inputs.add("source-" + index);
            fingerprints.add(schema.fingerprint());
        }
        assertFailure("INVALID_TOPOLOGY", () -> new PortableStreamNode(
                "union", StreamOperatorCode.UNION, inputs, fingerprints,
                schema, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), null));

        assertFailure("INVALID_EDGE_INPUT", () -> new StreamEdge(
                "source", "union", 2, "channel-2",
                StreamEdge.Partitioning.FORWARD, 1024, 4096));
    }

    @Test
    void everyAtomicCapabilityIsProjectedToTheOuterArtifact() {
        PortableStreamPlan base = unionPlan(true);
        List<String> atomic = Arrays.asList(
                PortableStreamPlan.CALC_CAPABILITY,
                PortableStreamPlan.FLAT_MAP_CAPABILITY,
                PortableStreamPlan.UNION_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY,
                PortableStreamPlan.KEYED_PROCESS_CAPABILITY,
                PortableStreamPlan.KEYED_CO_PROCESS_CAPABILITY,
                PortableStreamPlan.BROADCAST_STATE_CAPABILITY,
                PortableStreamPlan.GENERIC_WINDOW_CAPABILITY,
                PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY,
                PortableStreamPlan.ASYNC_WAIT_CAPABILITY,
                PortableStreamPlan.STREAM_CAPABILITY);
        PortableStreamPlan plan = new PortableStreamPlan(
                base.planVersion(), base.deliveryGuarantee(), base.parallelism(),
                base.maxParallelism(), base.changelogMode(), base.timeMode(),
                base.rootSinkOperatorUids(), base.nodes(), base.edges(), atomic);

        PlanArtifact artifact = PortableStreamPlanArtifact.wrap(
                "atomic-capabilities", plan);
        PlanArtifactValidator.validateReadable(artifact);
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_CALC_V1));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_FLAT_MAP_V1));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_UNION_V1));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_EVENT_TIME_ASSIGNMENT_V1));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_KEYED_PROCESS_V1));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_KEYED_CO_PROCESS_V1));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_BROADCAST_STATE_V1));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_GENERIC_WINDOW_V1));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_TAGGED_OUTPUT_V1));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_ASYNC_WAIT_V1));
    }

    private static void assertOpcode(StreamOperatorCode opcode, int wireId,
            int minimumInputs, int maximumInputs,
            StreamOperatorCode.BindingKind bindingKind) {
        assertEquals(wireId, opcode.wireId());
        assertEquals(minimumInputs, opcode.minimumInputCount());
        assertEquals(maximumInputs, opcode.maximumInputCount());
        assertEquals(bindingKind, opcode.bindingKind());
    }

    private static PortableStreamPlan unionPlan(boolean withCapability) {
        PortableRowSchema schema = schema();
        PortableStreamNode left = source("left", "split-left", schema);
        PortableStreamNode right = source("right", "split-right", schema);
        PortableStreamNode union = new PortableStreamNode(
                "union", StreamOperatorCode.UNION,
                Arrays.asList("left", "right"),
                Arrays.asList(schema.fingerprint(), schema.fingerprint()),
                schema, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), null);
        byte[] sinkConfig = new byte[]{7};
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("union"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), new StreamSinkBinding(
                "sink-provider", repeat('d', 64), sha256(sinkConfig), sinkConfig,
                StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE));
        List<String> capabilities = withCapability
                ? Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan.UNION_CAPABILITY)
                : Collections.singletonList(PortableStreamPlan.STREAM_CAPABILITY);
        return new PortableStreamPlan(PortableStreamPlan.CURRENT_VERSION,
                PortableStreamPlan.DeliveryGuarantee.AT_LEAST_ONCE,
                1, 128, PortableStreamPlan.ChangelogMode.APPEND,
                PortableStreamPlan.TimeMode.PROCESSING_TIME,
                Collections.singletonList("sink"),
                Arrays.asList(left, right, union, sink),
                Arrays.asList(
                        edge("left", "union", 0),
                        edge("right", "union", 1),
                        edge("union", "sink", 0)),
                capabilities);
    }

    private static PortableStreamNode source(
            String uid, String splitId, PortableRowSchema schema) {
        byte[] config = new byte[]{(byte) uid.length()};
        return new PortableStreamNode(uid, StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), new StreamSourceBinding(
                "source-provider", repeat('a', 64), sha256(config), config,
                Collections.singletonList(new StreamSourceSplit(
                        sha256(splitId.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                        "position-v1", new byte[]{1})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED));
    }

    private static StreamEdge edge(String source, String target, int input) {
        return new StreamEdge(source, target, input,
                source + '-' + target + '-' + input,
                StreamEdge.Partitioning.FORWARD, 1024, 4096);
    }

    private static PortableRowSchema schema() {
        return new PortableRowSchema(Collections.singletonList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false)));
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder text = new StringBuilder(64);
            for (byte item : digest) {
                text.append(String.format(java.util.Locale.ROOT, "%02x", item & 0xff));
            }
            return text.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String repeat(char value, int count) {
        char[] result = new char[count];
        Arrays.fill(result, value);
        return new String(result);
    }

    private static void assertFailure(String code, Runnable action) {
        StreamContractException failure = assertThrows(
                StreamContractException.class, action::run);
        assertEquals(code, failure.code());
    }
}
