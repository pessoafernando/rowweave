package io.rowweave.portable;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PortableBatchPlanModelTest {

    private static final String DIGEST_A = repeat('a', 64);
    private static final String DIGEST_B = repeat('b', 64);
    private static final String CONFIG_DIGEST =
            "73907589101a7e8ab83178e7db2997aab7272cd02d364e8e3ecc2beccda4b631";

    @Test
    void canonicalPlanKeepsOnlyClosedMetadataAndTypedBindings() {
        PortableRowSchema schema = schema();
        PortablePlanNode source = source(schema);
        PortablePlanNode map = functionMap(schema, source);
        PortablePlanNode sink = sink(schema, map);
        PortableBatchPlan plan = new PortableBatchPlan(
                1, 4, Collections.singletonList("sink-orders"),
                Arrays.asList(source, map, sink),
                Arrays.asList("rowweave.function-provider.v1",
                        "rowweave.runtime.batch.v1"));

        assertEquals(1, plan.planVersion());
        assertEquals(4, plan.desiredParallelism());
        assertEquals(Arrays.asList(source, map, sink), plan.nodes());
        assertEquals(Collections.singletonList("sink-orders"),
                plan.rootSinkOperatorIds());
        assertEquals(Arrays.asList("rowweave.function-provider.v1",
                "rowweave.runtime.batch.v1"), plan.requiredCapabilities());
        assertEquals(Arrays.asList("alpha", "zeta"),
                Arrays.asList(map.scalarParameters().keySet().toArray()));
        assertEquals(schema.fingerprint(),
                map.inputSchemaFingerprints().get(0));
    }

    @Test
    void nodeRejectsWrongArityBindingOrFunctionAbi() {
        PortableRowSchema schema = schema();
        assertCode("INVALID_TOPOLOGY", () -> node(
                "bad-join", PortableOperatorCode.JOIN,
                Collections.singletonList("left"),
                Collections.singletonList(schema.fingerprint()),
                schema, null));
        assertCode("BINDING_KIND_MISMATCH", () -> node(
                "bad-source", PortableOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(),
                schema, null));
        assertCode("FUNCTION_ABI_MISMATCH", () -> node(
                "bad-function", PortableOperatorCode.FUNCTION_MAP,
                Collections.singletonList("source-orders"),
                Collections.singletonList(schema.fingerprint()),
                schema, function(FunctionAbi.FILTER_V1)));
        assertCode("BINDING_KIND_MISMATCH", () -> node(
                "fake-function", PortableOperatorCode.FUNCTION_MAP,
                Collections.singletonList("source-orders"),
                Collections.singletonList(schema.fingerprint()), schema,
                () -> PortableOperatorCode.BindingKind.FUNCTION));
    }

    @Test
    void planRejectsDuplicateForwardUnknownAndSchemaDriftEdges() {
        PortableRowSchema schema = schema();
        PortablePlanNode source = source(schema);
        PortablePlanNode map = functionMap(schema, source);
        PortablePlanNode sink = sink(schema, map);

        assertCode("DUPLICATE_OPERATOR_ID", () -> plan(
                Arrays.asList(source, source, sink)));
        assertCode("INVALID_TOPOLOGY", () -> plan(
                Arrays.asList(map, source, sink)));

        PortablePlanNode drift = node(
                "map-orders", PortableOperatorCode.FUNCTION_MAP,
                Collections.singletonList(source.operatorId()),
                Collections.singletonList(DIGEST_A),
                schema, function(FunctionAbi.MAP_V1));
        assertCode("SCHEMA_MISMATCH", () -> plan(
                Arrays.asList(source, drift, sink(schema, drift))));
    }

    @Test
    void everySinkMustBeADeclaredRootAndEveryRootMustBeASink() {
        PortableRowSchema schema = schema();
        PortablePlanNode source = source(schema);
        PortablePlanNode map = functionMap(schema, source);
        PortablePlanNode sink = sink(schema, map);

        assertCode("INVALID_ROOT_SET", () -> new PortableBatchPlan(
                1, 1, Collections.singletonList(map.operatorId()),
                Arrays.asList(source, map, sink),
                Collections.singletonList("rowweave.runtime.batch.v1")));
        assertCode("INVALID_ROOT_SET", () -> new PortableBatchPlan(
                1, 1, Collections.emptyList(),
                Arrays.asList(source, map, sink),
                Collections.singletonList("rowweave.runtime.batch.v1")));
    }

    private static PortableBatchPlan plan(List<PortablePlanNode> nodes) {
        return new PortableBatchPlan(
                1, 1, Collections.singletonList("sink-orders"), nodes,
                Collections.singletonList("rowweave.runtime.batch.v1"));
    }

    private static PortablePlanNode source(PortableRowSchema schema) {
        return node("source-orders", PortableOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                new PortableSourceBinding(
                        "jdbc-v1", DIGEST_A, new byte[]{1},
                        Collections.singletonList(new byte[]{2})));
    }

    private static PortablePlanNode functionMap(
            PortableRowSchema schema, PortablePlanNode input) {
        return node("map-orders", PortableOperatorCode.FUNCTION_MAP,
                Collections.singletonList(input.operatorId()),
                Collections.singletonList(input.outputSchema().fingerprint()),
                schema, function(FunctionAbi.MAP_V1));
    }

    private static PortablePlanNode sink(
            PortableRowSchema schema, PortablePlanNode input) {
        return node("sink-orders", PortableOperatorCode.SINK,
                Collections.singletonList(input.operatorId()),
                Collections.singletonList(input.outputSchema().fingerprint()),
                schema, new PortableSinkBinding(
                        "manifest-file-v1", DIGEST_B,
                        new byte[]{3}, new byte[]{4}));
    }

    private static FunctionDeploymentReference function(FunctionAbi abi) {
        return new FunctionDeploymentReference(
                DIGEST_A, "domain-functions", DIGEST_B,
                "example.functions.NormalizeAmount", abi, "json-v1",
                CONFIG_DIGEST, new byte[]{8, 9},
                new FunctionResourceProfile(1024, 0, 16));
    }

    private static PortablePlanNode node(
            String id, PortableOperatorCode code,
            List<String> inputs, List<String> inputSchemas,
            PortableRowSchema outputSchema, PortableNodeBinding binding) {
        Map<String, String> scalar = new LinkedHashMap<>();
        scalar.put("zeta", "last");
        scalar.put("alpha", "first");
        Map<String, List<String>> lists = new LinkedHashMap<>();
        lists.put("keys", Collections.singletonList("customer-id"));
        return new PortablePlanNode(
                id, code, inputs, inputSchemas, outputSchema,
                scalar, lists, binding);
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
}
