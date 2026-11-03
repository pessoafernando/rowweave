package io.rowweave.portable;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.Collector;
import io.rowweave.Source;
import io.rowweave.MapPartitionEmissionLimit;
import io.rowweave.MapPartitionFunction;
import io.rowweave.Sink;
import io.rowweave.PartitionDataRowCursor;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PortableMapPartitionContractTest {
    private static final String DIGEST_A = repeat('a', 64);
    private static final String DIGEST_B = repeat('b', 64);
    private static final String CONFIG_DIGEST =
            "73907589101a7e8ab83178e7db2997aab7272cd02d364e8e3ecc2beccda4b631";

    @Test
    void opcodeAbiAndCapabilityAreStableAndRequiredTogether() {
        assertEquals(67,
                PortableOperatorCode.FUNCTION_MAP_PARTITION.wireId());
        assertEquals(PortableOperatorCode.FUNCTION_MAP_PARTITION,
                PortableOperatorCode.fromWireId(67));
        assertEquals(16, FunctionAbi.MAP_PARTITION_V1.wireId());
        assertEquals(FunctionAbi.MAP_PARTITION_V1,
                FunctionAbi.fromWireId(16));

        PortableRowSchema schema = schema();
        PortablePlanNode source = source(schema);
        PortablePlanNode mapPartition = new PortablePlanNode(
                "partition-map", PortableOperatorCode.FUNCTION_MAP_PARTITION,
                Collections.singletonList(source.operatorId()),
                Collections.singletonList(schema.fingerprint()), schema,
                limits(10L, 4096L), Collections.emptyMap(),
                function(FunctionAbi.MAP_PARTITION_V1));
        PortablePlanNode sink = sink(schema, mapPartition);

        PortablePlanException missing = assertThrows(
                PortablePlanException.class,
                () -> new PortableBatchPlan(1, 2,
                        Collections.singletonList(sink.operatorId()),
                        Arrays.asList(source, mapPartition, sink),
                        Collections.singletonList(
                                PortableBatchPlan.BATCH_CAPABILITY)));
        assertEquals("MISSING_CAPABILITY", missing.code());

        PortableBatchPlan plan = new PortableBatchPlan(1, 2,
                Collections.singletonList(sink.operatorId()),
                Arrays.asList(source, mapPartition, sink),
                Arrays.asList(PortableBatchPlan.BATCH_CAPABILITY,
                        PortableBatchPlan.MAP_PARTITION_CAPABILITY));
        assertEquals(PortableOperatorCode.FUNCTION_MAP_PARTITION,
                plan.nodes().get(1).opcode());
        byte[] frame = PortableBatchPlanCodec.encode(plan);
        PortableBatchPlan decoded = PortableBatchPlanCodec.decode(frame);
        assertEquals(plan, decoded);
        assertArrayEquals(frame, PortableBatchPlanCodec.encode(decoded));
    }

    @Test
    void mapPartitionRejectsWrongAbi() {
        PortableRowSchema schema = schema();
        PortablePlanNode source = source(schema);
        PortablePlanException abi = assertThrows(PortablePlanException.class,
                () -> new PortablePlanNode("partition-map",
                        PortableOperatorCode.FUNCTION_MAP_PARTITION,
                        Collections.singletonList(source.operatorId()),
                        Collections.singletonList(schema.fingerprint()), schema,
                        limits(1L, 1L), Collections.emptyMap(),
                        function(FunctionAbi.MAP_V1)));
        assertEquals("FUNCTION_ABI_MISMATCH", abi.code());
    }

    @Test
    void preflightAcceptsOnlyTheExactBoundedLimitContract() {
        PortableRowSchema schema = schema();
        Map<String, String> maximum = limits(
                MapPartitionEmissionLimit.MAX_OUTPUT_ROWS_PER_PARTITION,
                MapPartitionEmissionLimit.MAX_OUTPUT_BYTES_PER_PARTITION);
        PortablePlanCompiler.preflight(plan(schema, maximum), verifier());

        assertPreflightCode("INVALID_PARAMETER", schema, limits(0L, 1L));
        assertPreflightCode("INVALID_PARAMETER", schema, limits(1L, 0L));
        assertPreflightCode("LIMIT_EXCEEDED", schema, limits(
                MapPartitionEmissionLimit.MAX_OUTPUT_ROWS_PER_PARTITION + 1L,
                1L));
        assertPreflightCode("LIMIT_EXCEEDED", schema, limits(1L,
                MapPartitionEmissionLimit.MAX_OUTPUT_BYTES_PER_PARTITION + 1L));

        Map<String, String> unknown = limits(1L, 1L);
        unknown.put("bufferAllInput", "true");
        assertPreflightCode("UNKNOWN_PARAMETER", schema, unknown);

        Map<String, String> missing = new LinkedHashMap<>();
        missing.put("maxOutputRowsPerPartition", "1");
        assertPreflightCode("UNKNOWN_PARAMETER", schema, missing);
    }

    @Test
    void workerResolutionRejectsFunctionResultSchemaDrift() {
        PortableRowSchema schema = schema();
        PortableRowSchema wrongSchema = new PortableRowSchema(
                Collections.singletonList(new PortableField(
                        "value", "value", "value",
                        PortableDataType.STRING, true)));

        PortablePlanException mismatch = assertThrows(
                PortablePlanException.class,
                () -> PortablePlanCompiler.compile(
                        plan(schema, limits(1L, 1L)), verifier(),
                        resolver(schema, wrongSchema)));
        assertEquals("RESOLVED_SCHEMA_MISMATCH", mismatch.code());
    }

    private static Map<String, String> limits(long rows, long bytes) {
        Map<String, String> result = new LinkedHashMap<>();
        result.put("maxOutputRowsPerPartition", Long.toString(rows));
        result.put("maxOutputBytesPerPartition", Long.toString(bytes));
        return result;
    }

    private static PortableBatchPlan plan(
            PortableRowSchema schema, Map<String, String> parameters) {
        PortablePlanNode source = source(schema);
        PortablePlanNode mapPartition = new PortablePlanNode(
                "partition-map", PortableOperatorCode.FUNCTION_MAP_PARTITION,
                Collections.singletonList(source.operatorId()),
                Collections.singletonList(schema.fingerprint()), schema,
                parameters, Collections.emptyMap(),
                function(FunctionAbi.MAP_PARTITION_V1));
        PortablePlanNode sink = sink(schema, mapPartition);
        return new PortableBatchPlan(1, 2,
                Collections.singletonList(sink.operatorId()),
                Arrays.asList(source, mapPartition, sink),
                Arrays.asList(PortableBatchPlan.BATCH_CAPABILITY,
                        PortableBatchPlan.MAP_PARTITION_CAPABILITY));
    }

    private static void assertPreflightCode(
            String code, PortableRowSchema schema,
            Map<String, String> parameters) {
        PortablePlanException failure = assertThrows(
                PortablePlanException.class,
                () -> PortablePlanCompiler.preflight(
                        plan(schema, parameters), verifier()));
        assertEquals(code, failure.code());
    }

    private static PortableBindingVerifier verifier() {
        return new PortableBindingVerifier() {
            @Override public void verifySource(PortableSourceBinding binding) { }
            @Override public void verifyFunction(
                    FunctionDeploymentReference reference) { }
            @Override public void verifySink(PortableSinkBinding binding) { }
        };
    }

    private static PortableRuntimeResolver resolver(
            PortableRowSchema sourceSchema,
            PortableRowSchema functionResultSchema) {
        DataSchema sourceMeta = PortablePlanCompiler.toDataSchema(sourceSchema);
        DataSchema resultMeta = PortablePlanCompiler.toDataSchema(
                functionResultSchema);
        return new PortableRuntimeResolver() {
            @Override public List<Source> resolveSource(
                    PortableSourceBinding binding) {
                return Collections.singletonList(new Source() {
                    private static final long serialVersionUID = 1L;
                    @Override public DataSchema getSchema() { return sourceMeta; }
                    @Override public Iterator<DataRow> iterator() {
                        return Collections.<DataRow>emptyList().iterator();
                    }
                });
            }

            @Override public Serializable resolveFunction(
                    FunctionDeploymentReference reference) {
                return new MapPartitionFunction() {
                    private static final long serialVersionUID = 1L;
                    @Override public void mapPartition(
                            PartitionDataRowCursor rows, Collector collector) { }
                    @Override public DataSchema getResultSchema() {
                        return resultMeta;
                    }
                };
            }

            @Override public Sink resolveSink(
                    PortableSinkBinding binding, DataSchema inputSchema) {
                return new Sink() {
                    private static final long serialVersionUID = 1L;
                    @Override public DataSchema getSchema() {
                        return inputSchema;
                    }
                    @Override public void collect(DataRow row) { }
                };
            }
        };
    }

    private static PortablePlanNode source(PortableRowSchema schema) {
        String runtimeFingerprint = SchemaFingerprint.of(
                PortablePlanCompiler.toDataSchema(schema));
        byte[] descriptor = BatchSourceSplitDescriptorCodec.encode(
                new BatchSourceSplitDescriptor(
                        1, "test-source", 1, "split-1",
                        runtimeFingerprint, "snapshot-1",
                        BatchSourceSplitDescriptor.ReplayGuarantee
                                .EXACT_ORDERED_FROM_START));
        return new PortablePlanNode("source", PortableOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSourceBinding("source-v1", DIGEST_A, new byte[]{1},
                        Collections.singletonList(descriptor)));
    }

    private static PortablePlanNode sink(
            PortableRowSchema schema, PortablePlanNode input) {
        String runtimeFingerprint = SchemaFingerprint.of(
                PortablePlanCompiler.toDataSchema(schema));
        byte[] descriptor = BatchSinkDescriptorCodec.encode(
                new BatchSinkDescriptor(
                        1, "test-sink", 1, runtimeFingerprint,
                        "destination", "domain",
                        BatchSinkDescriptor.CapabilityProfile
                                .SIDE_EFFECT_FREE_V1));
        return new PortablePlanNode("sink", PortableOperatorCode.SINK,
                Collections.singletonList(input.operatorId()),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSinkBinding("sink-v1", DIGEST_B,
                        descriptor, new byte[]{4}));
    }

    private static FunctionDeploymentReference function(FunctionAbi abi) {
        return new FunctionDeploymentReference(
                DIGEST_A, "functions", DIGEST_B,
                "example.functions.PartitionMap", abi, "binary-v1",
                CONFIG_DIGEST, new byte[]{8, 9},
                new FunctionResourceProfile(1024L, 0L, 16));
    }

    private static PortableRowSchema schema() {
        return new PortableRowSchema(Collections.singletonList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false)));
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
