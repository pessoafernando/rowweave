package io.rowweave.portable;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.BatchSink;
import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.MapFunction;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.plan.PlanGraph;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortablePlanCompilerTest {

    private static final String DIGEST_A = repeat('a', 64);
    private static final String DIGEST_B = repeat('b', 64);
    private static final String CONFIG_DIGEST =
            "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d";

    @Test
    void verifiesEveryBindingBeforeResolvingAnyRuntimeObject() {
        PortableRowSchema schema = schema();
        PortableBatchPlan plan = plan(schema,
                functionNode(schema, Collections.emptyMap()));
        List<String> calls = new ArrayList<>();
        RecordingBindings bindings = new RecordingBindings(calls, schema);

        PlanGraph compiled = PortablePlanCompiler.compile(
                plan, bindings, bindings);

        assertEquals(Arrays.asList(
                "verify-source", "verify-function", "verify-sink",
                "resolve-source", "resolve-function", "resolve-sink"), calls);
        assertEquals(3, compiled.nodeCount());
        assertEquals(4, compiled.parallelism());
        assertEquals("source-orders", compiled.node(0).name());
        assertEquals("map", compiled.node(1).typeId());
        assertTrue(compiled.node(1).userFunction() instanceof MapFunction);
        assertEquals("sink-orders", compiled.node(2).name());
    }

    @Test
    void metadataPreflightNeverResolvesRuntimeObjects() {
        PortableRowSchema schema = schema();
        PortableBatchPlan plan = plan(schema,
                functionNode(schema, Collections.emptyMap()));
        List<String> calls = new ArrayList<>();
        RecordingBindings bindings = new RecordingBindings(calls, schema);

        PortablePlanCompiler.preflight(plan, bindings);

        assertEquals(Arrays.asList(
                "verify-source", "verify-function", "verify-sink"), calls);
    }

    @Test
    void metadataProjectionPreservesPlanningShapeWithoutRuntimeResolution() {
        PortableRowSchema schema = schema();
        PortableBatchPlan plan = plan(schema,
                functionNode(schema, Collections.emptyMap()));
        List<String> calls = new ArrayList<>();
        RecordingBindings bindings = new RecordingBindings(calls, schema);

        PlanGraph projected = PortablePlanCompiler.projectMetadata(
                plan, bindings);

        assertEquals(Arrays.asList(
                "verify-source", "verify-function", "verify-sink"), calls);
        assertEquals(plan.desiredParallelism(), projected.parallelism());
        assertEquals(plan.nodes().size(), projected.nodeCount());
        assertArrayEquals(sourceDescriptor(schemaRuntimeFingerprint(schema)),
                BatchSourceSplitDescriptorCodec.encode(
                        projected.node(0).sourceSplitDescriptors().get(0)));
        assertTrue(projected.node(0).inputs().get(0)
                instanceof BatchSource);
        assertTrue(projected.node(1).userFunction() instanceof MapFunction);
        assertTrue(projected.node(2).output() instanceof BatchSink);
        assertTrue(projected.node(2).output().isWorkerWritable());
        assertArrayEquals(sinkDescriptor(schema), BatchSinkDescriptorCodec.encode(
                projected.node(2).batchSinkDescriptor()));
        assertThrows(IllegalStateException.class,
                () -> projected.node(0).inputs().get(0).iterator());
    }

    @Test
    void invalidClosedParametersFailBeforeBindingVerificationOrResolution() {
        PortableRowSchema schema = schema();
        PortableBatchPlan plan = plan(schema,
                functionNode(schema, Collections.singletonMap("surprise", "x")));
        List<String> calls = new ArrayList<>();
        RecordingBindings bindings = new RecordingBindings(calls, schema);

        PortablePlanException failure = assertThrows(
                PortablePlanException.class,
                () -> PortablePlanCompiler.compile(plan, bindings, bindings));

        assertEquals("UNKNOWN_PARAMETER", failure.code());
        assertTrue(calls.isEmpty());
    }

    @Test
    void metadataVerifierFailurePreventsEveryRuntimeResolution() {
        PortableRowSchema schema = schema();
        PortableBatchPlan plan = plan(schema,
                functionNode(schema, Collections.emptyMap()));
        List<String> calls = new ArrayList<>();
        RecordingBindings bindings = new RecordingBindings(calls, schema) {
            @Override
            public void verifySink(PortableSinkBinding binding) {
                calls.add("verify-sink-failed");
                throw PortablePlanText.failure("CONTRACT_DIGEST_MISMATCH",
                        "sink.providerContractDigest",
                        "provider contract does not match the admitted catalog");
            }
        };

        assertCode("CONTRACT_DIGEST_MISMATCH",
                () -> PortablePlanCompiler.compile(plan, bindings, bindings));
        assertEquals(Arrays.asList("verify-source", "verify-function",
                "verify-sink-failed"), calls);
    }

    @Test
    void connectorDescriptorsMustBindThePortableRuntimeSchema() {
        PortableRowSchema schema = schema();
        PortableBatchPlan valid = plan(schema,
                functionNode(schema, Collections.emptyMap()));
        PortablePlanNode source = valid.nodes().get(0);
        PortablePlanNode wrongSource = new PortablePlanNode(
                source.operatorId(), source.opcode(),
                source.inputOperatorIds(), source.inputSchemaFingerprints(),
                source.outputSchema(), source.scalarParameters(),
                source.listParameters(), new PortableSourceBinding(
                "jdbc-v1", DIGEST_A, new byte[]{1},
                Collections.singletonList(sourceDescriptor(
                        repeat('f', 64)))));
        PortableBatchPlan drift = new PortableBatchPlan(
                valid.planVersion(), valid.desiredParallelism(),
                valid.rootSinkOperatorIds(), Arrays.asList(
                wrongSource, valid.nodes().get(1), valid.nodes().get(2)),
                valid.requiredCapabilities());
        List<String> calls = new ArrayList<>();

        assertCode("SOURCE_DESCRIPTOR_SCHEMA_MISMATCH", () ->
                PortablePlanCompiler.preflight(
                        drift, new RecordingBindings(calls, schema)));
        assertTrue(calls.isEmpty());

        byte[] duplicateFrame = sourceDescriptor(
                schemaRuntimeFingerprint(schema));
        PortablePlanNode duplicateSource = new PortablePlanNode(
                source.operatorId(), source.opcode(),
                source.inputOperatorIds(), source.inputSchemaFingerprints(),
                source.outputSchema(), source.scalarParameters(),
                source.listParameters(), new PortableSourceBinding(
                "jdbc-v1", DIGEST_A, new byte[]{1},
                Arrays.asList(duplicateFrame, duplicateFrame)));
        PortableBatchPlan duplicate = new PortableBatchPlan(
                valid.planVersion(), valid.desiredParallelism(),
                valid.rootSinkOperatorIds(), Arrays.asList(
                duplicateSource, valid.nodes().get(1), valid.nodes().get(2)),
                valid.requiredCapabilities());
        assertCode("SOURCE_DESCRIPTOR_DUPLICATE", () ->
                PortablePlanCompiler.preflight(
                        duplicate, new RecordingBindings(
                                new ArrayList<>(), schema)));

        PortablePlanNode sink = valid.nodes().get(2);
        PortablePlanNode wrongSink = new PortablePlanNode(
                sink.operatorId(), sink.opcode(), sink.inputOperatorIds(),
                sink.inputSchemaFingerprints(), sink.outputSchema(),
                sink.scalarParameters(), sink.listParameters(),
                new PortableSinkBinding("manifest-file-v1", DIGEST_B,
                        sinkDescriptor(repeat('e', 64)), new byte[]{4}));
        PortableBatchPlan sinkDrift = new PortableBatchPlan(
                valid.planVersion(), valid.desiredParallelism(),
                valid.rootSinkOperatorIds(), Arrays.asList(
                valid.nodes().get(0), valid.nodes().get(1), wrongSink),
                valid.requiredCapabilities());
        assertCode("SINK_DESCRIPTOR_SCHEMA_MISMATCH", () ->
                PortablePlanCompiler.preflight(
                        sinkDrift, new RecordingBindings(
                                new ArrayList<>(), schema)));
    }

    @Test
    void durablePreflightRequiresExactSourcesAndSideEffectFreeSinks() {
        PortableRowSchema schema = schema();
        PortableBatchPlan valid = plan(schema,
                functionNode(schema, Collections.emptyMap()));
        PortablePlanCompiler.preflightDurable(
                valid, new RecordingBindings(new ArrayList<>(), schema));

        PortablePlanNode source = valid.nodes().get(0);
        PortablePlanNode legacySource = new PortablePlanNode(
                source.operatorId(), source.opcode(),
                source.inputOperatorIds(), source.inputSchemaFingerprints(),
                source.outputSchema(), source.scalarParameters(),
                source.listParameters(), new PortableSourceBinding(
                "jdbc-v1", DIGEST_A, new byte[]{1},
                Collections.singletonList(sourceDescriptor(
                        schemaRuntimeFingerprint(schema),
                        BatchSourceSplitDescriptor.ReplayGuarantee
                                .LEGACY_SINGLE_ATTEMPT))));
        PortableBatchPlan legacy = new PortableBatchPlan(
                valid.planVersion(), valid.desiredParallelism(),
                valid.rootSinkOperatorIds(), Arrays.asList(
                legacySource, valid.nodes().get(1), valid.nodes().get(2)),
                valid.requiredCapabilities());
        assertCode("DURABLE_SOURCE_REPLAY_UNSUPPORTED", () ->
                PortablePlanCompiler.preflightDurable(
                        legacy, new RecordingBindings(
                                new ArrayList<>(), schema)));

        PortablePlanNode sink = valid.nodes().get(2);
        PortablePlanNode manifestSink = new PortablePlanNode(
                sink.operatorId(), sink.opcode(), sink.inputOperatorIds(),
                sink.inputSchemaFingerprints(), sink.outputSchema(),
                sink.scalarParameters(), sink.listParameters(),
                new PortableSinkBinding("manifest-file-v1", DIGEST_B,
                        sinkDescriptor(schemaRuntimeFingerprint(schema),
                                BatchSinkDescriptor.CapabilityProfile
                                        .MANIFEST_ATOMIC_GROUP_V1),
                        new byte[]{4}));
        PortableBatchPlan manifest = new PortableBatchPlan(
                valid.planVersion(), valid.desiredParallelism(),
                valid.rootSinkOperatorIds(), Arrays.asList(
                valid.nodes().get(0), valid.nodes().get(1), manifestSink),
                valid.requiredCapabilities());
        assertCode("DURABLE_SINK_PROFILE_UNSUPPORTED", () ->
                PortablePlanCompiler.preflightDurable(
                        manifest, new RecordingBindings(
                                new ArrayList<>(), schema)));
    }

    @Test
    void wrongResolvedAbiOrSchemaFailsClosed() {
        PortableRowSchema schema = schema();
        PortableBatchPlan plan = plan(schema,
                functionNode(schema, Collections.emptyMap()));
        RecordingBindings wrongFunction = new RecordingBindings(
                new ArrayList<>(), schema) {
            @Override
            public Serializable resolveFunction(
                    FunctionDeploymentReference reference) {
                return (io.rowweave.FilterFunction) row -> true;
            }
        };
        assertCode("RESOLVED_FUNCTION_ABI_MISMATCH",
                () -> PortablePlanCompiler.compile(
                        plan, wrongFunction, wrongFunction));

        RecordingBindings wrongSource = new RecordingBindings(
                new ArrayList<>(), schema) {
            @Override
            public List<Source> resolveSource(PortableSourceBinding binding) {
                return Collections.singletonList(new TestInput(
                        new DataSchema(new Field("wrong", DataType.StringType))));
            }
        };
        assertCode("RESOLVED_SCHEMA_MISMATCH",
                () -> PortablePlanCompiler.compile(plan, wrongSource, wrongSource));
    }

    @Test
    void compiledProjectionMustRemainByteExactToPortableSemantics() {
        PortableRowSchema schema = schema();
        PortableBatchPlan plan = plan(schema,
                functionNode(schema, Collections.emptyMap()));
        RecordingBindings bindings = new RecordingBindings(
                new ArrayList<>(), schema);
        PlanGraph compiled = PortablePlanCompiler.compile(
                plan, bindings, bindings);
        List<io.rowweave.plan.PlanNode> nodes = new ArrayList<>(
                compiled.nodes());
        nodes.set(1, nodes.get(1).withParam(
                PortablePlanCompiler.OPCODE_PARAM, "999"));
        PlanGraph drift = new PlanGraph(nodes, compiled.parallelism());

        assertCode("PROJECTION_MISMATCH",
                () -> PortablePlanCompiler.requireExactProjection(plan, drift));
    }

    private static PortableBatchPlan plan(
            PortableRowSchema schema, PortablePlanNode function) {
        PortablePlanNode source = sourceNode(schema);
        PortablePlanNode sink = new PortablePlanNode(
                "sink-orders", PortableOperatorCode.SINK,
                Collections.singletonList(function.operatorId()),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSinkBinding(
                        "manifest-file-v1", DIGEST_B,
                        sinkDescriptor(schema), new byte[]{4}));
        return new PortableBatchPlan(1, 4,
                Collections.singletonList(sink.operatorId()),
                Arrays.asList(source, function, sink),
                Arrays.asList("rowweave.function-provider.v1",
                        PortableBatchPlan.BATCH_CAPABILITY));
    }

    private static PortablePlanNode sourceNode(PortableRowSchema schema) {
        return new PortablePlanNode(
                "source-orders", PortableOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSourceBinding(
                        "jdbc-v1", DIGEST_A, new byte[]{1},
                        Collections.singletonList(sourceDescriptor(
                                schemaRuntimeFingerprint(schema)))));
    }

    private static byte[] sourceDescriptor(String runtimeSchemaFingerprint) {
        return sourceDescriptor(runtimeSchemaFingerprint,
                BatchSourceSplitDescriptor.ReplayGuarantee
                        .EXACT_ORDERED_FROM_START);
    }

    private static byte[] sourceDescriptor(
            String runtimeSchemaFingerprint,
            BatchSourceSplitDescriptor.ReplayGuarantee replayGuarantee) {
        return BatchSourceSplitDescriptorCodec.encode(
                new BatchSourceSplitDescriptor(
                        1, "test-source", 1, "split-1",
                        runtimeSchemaFingerprint, "snapshot-1",
                        replayGuarantee));
    }

    private static byte[] sinkDescriptor(PortableRowSchema schema) {
        return sinkDescriptor(schemaRuntimeFingerprint(schema));
    }

    private static byte[] sinkDescriptor(String runtimeSchemaFingerprint) {
        return sinkDescriptor(runtimeSchemaFingerprint,
                BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1);
    }

    private static byte[] sinkDescriptor(
            String runtimeSchemaFingerprint,
            BatchSinkDescriptor.CapabilityProfile capabilityProfile) {
        return BatchSinkDescriptorCodec.encode(new BatchSinkDescriptor(
                1, "test-sink", 1, runtimeSchemaFingerprint,
                "destination", "domain",
                capabilityProfile));
    }

    private static String schemaRuntimeFingerprint(PortableRowSchema schema) {
        return SchemaFingerprint.of(PortablePlanCompiler.toDataSchema(schema));
    }

    private static PortablePlanNode functionNode(
            PortableRowSchema schema,
            java.util.Map<String, String> scalarParameters) {
        return new PortablePlanNode(
                "map-orders", PortableOperatorCode.FUNCTION_MAP,
                Collections.singletonList("source-orders"),
                Collections.singletonList(schema.fingerprint()), schema,
                scalarParameters, Collections.emptyMap(),
                new FunctionDeploymentReference(
                        DIGEST_A, "domain-functions", DIGEST_B,
                        "example.functions.NormalizeAmount",
                        FunctionAbi.MAP_V1, "empty-v1", CONFIG_DIGEST,
                        new byte[]{0},
                        new FunctionResourceProfile(1024, 0, 1)));
    }

    private static PortableRowSchema schema() {
        return new PortableRowSchema(Arrays.asList(
                new PortableField("customer-id", "customer_id", "customerId",
                        PortableDataType.STRING, false),
                new PortableField("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true)));
    }

    private static void assertCode(String code, Runnable action) {
        PortablePlanException failure = assertThrows(
                PortablePlanException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static class RecordingBindings
            implements PortableBindingVerifier, PortableRuntimeResolver {
        private final List<String> calls;
        private final DataSchema dataSchema;

        private RecordingBindings(
                List<String> calls, PortableRowSchema schema) {
            this.calls = calls;
            this.dataSchema = PortablePlanCompiler.toDataSchema(schema);
        }

        @Override
        public void verifySource(PortableSourceBinding binding) {
            calls.add("verify-source");
        }

        @Override
        public void verifyFunction(FunctionDeploymentReference reference) {
            calls.add("verify-function");
        }

        @Override
        public void verifySink(PortableSinkBinding binding) {
            calls.add("verify-sink");
        }

        @Override
        public List<Source> resolveSource(PortableSourceBinding binding) {
            calls.add("resolve-source");
            return Collections.singletonList(new TestInput(dataSchema));
        }

        @Override
        public Serializable resolveFunction(
                FunctionDeploymentReference reference) {
            calls.add("resolve-function");
            return (MapFunction) row -> row;
        }

        @Override
        public Sink resolveSink(PortableSinkBinding binding, DataSchema inputSchema) {
            calls.add("resolve-sink");
            return new TestOutput(inputSchema);
        }
    }

    private static final class TestInput implements BatchSource {
        private final DataSchema dataSchema;
        private final BatchSourceSplitDescriptor descriptor;

        private TestInput(DataSchema dataSchema) {
            this.dataSchema = dataSchema;
            this.descriptor = new BatchSourceSplitDescriptor(
                    1, "test-source", 1, "split-1",
                    SchemaFingerprint.of(dataSchema), "snapshot-1",
                    BatchSourceSplitDescriptor.ReplayGuarantee
                            .EXACT_ORDERED_FROM_START);
        }

        @Override public DataSchema getSchema() { return dataSchema; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() {
            return descriptor;
        }
        @Override public Iterator<DataRow> iterator() {
            return Collections.<DataRow>emptyList().iterator();
        }
    }

    private static final class TestOutput implements BatchSink {
        private final DataSchema dataSchema;
        private final BatchSinkDescriptor descriptor;

        private TestOutput(DataSchema dataSchema) {
            this.dataSchema = dataSchema;
            this.descriptor = new BatchSinkDescriptor(
                    1, "test-sink", 1, SchemaFingerprint.of(dataSchema),
                    "destination", "domain",
                    BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1);
        }

        @Override public DataSchema getSchema() { return dataSchema; }
        @Override public void collect(DataRow row) { }
        @Override public BatchSinkDescriptor batchSinkDescriptor() {
            return descriptor;
        }
    }
}
