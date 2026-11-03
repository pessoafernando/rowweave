package io.rowweave.pipeline;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PortablePlanArtifact;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.PortableBatchPlan;
import io.rowweave.portable.PortableBindingVerifier;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableOperatorCode;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortablePlanNode;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.portable.PortableSinkBinding;
import io.rowweave.portable.PortableSourceBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the composition shapes a caller can build through the public builder,
 * including the shapes that a single-step or shared-upstream definition does
 * not exercise. Paired with the table in
 * {@code docs/product/reference/pipeline-boundary.md} section 2.
 */
class PipelineCompositionShapesTest {
    private static final String SCHEMA = repeat('a', 64);

    @Test
    void materializedFanInComposesTwoInputsIntoOneStep() {
        PipelineDefinition definition = fanIn(false);

        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(
                definition, Collections.singletonList("joined"));

        assertEquals(3, plan.orderedUnits().size());
        assertEquals(Arrays.asList("left", "right", "join"), stepIds(plan));
        assertEquals(Arrays.asList("to-left", "to-right"), edgeIds(plan));
        assertEquals(Collections.singletonList("joined"), outputIds(plan));
        PipelineExecutionUnit last = plan.orderedUnits().get(2);
        assertEquals("join", last.stepId());
        assertEquals(4, last.artifact().operators().size());
        assertEquals(PlanArtifact.RuntimeTarget.BATCH, last.runtimeTarget());
    }

    @Test
    void materializedFanInOrderIsCanonicalNotCallerOrdered() {
        PipelineExecutionPlan leftFirst = PipelineValidator.validateAndCompile(
                fanIn(false), Collections.singletonList("joined"));
        PipelineExecutionPlan rightFirst = PipelineValidator.validateAndCompile(
                fanIn(true), Collections.singletonList("joined"));

        assertEquals(stepIds(leftFirst), stepIds(rightFirst));
        assertEquals(edgeIds(leftFirst), edgeIds(rightFirst));
        assertEquals(leftFirst.definitionDigest(), rightFirst.definitionDigest());
    }

    @Test
    void fanInThroughASinkEndpointIsRefusedByTheArtifactContract() {
        io.rowweave.artifact.PlanArtifactValidationException failure = assertThrows(
                io.rowweave.artifact.PlanArtifactValidationException.class, () ->
                        new PipelineStep("join", artifactWithFanInSinkAsEndpoint("join")));
        assertEquals("INVALID_SEMANTICS", failure.code());
        assertTrue(failure.getMessage().contains("SINK requires 1 inputs"),
                failure.getMessage());
    }

    @Test
    void chainedBoundariesKeepUnitOrderAcrossThreeSteps() {
        PipelineDefinition definition = PipelineDefinition.builder("p", "r")
                .addBatchStep("a", step("a", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addBatchStep("b", step("b", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addStreamStep("c", step("c", PlanArtifact.RuntimeTarget.STREAM).artifact())
                .addVersionedDatasetEdge("ab", "a", "a-sink", "b", "b-source", "d-ab", SCHEMA)
                .addVersionedDatasetEdge("bc", "b", "b-sink", "c", "c-source", "d-bc", SCHEMA)
                .addOutput("final", "c", "c-sink")
                .build();

        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(
                definition, Collections.singletonList("final"));

        assertEquals(Arrays.asList("a", "b", "c"), stepIds(plan));
        assertEquals(Arrays.asList("ab", "bc"), edgeIds(plan));
        assertEquals(PlanArtifact.RuntimeTarget.BATCH, plan.orderedUnits().get(1).runtimeTarget());
        assertEquals(PlanArtifact.RuntimeTarget.STREAM, plan.orderedUnits().get(2).runtimeTarget());
    }

    @Test
    void streamProducerMaterializesIntoABatchConsumer() {
        PipelineDefinition definition = PipelineDefinition.builder("p", "r")
                .addStreamStep("s", step("s", PlanArtifact.RuntimeTarget.STREAM).artifact())
                .addBatchStep("b", step("b", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addVersionedDatasetEdge("sb", "s", "s-sink", "b", "b-source", "d-sb", SCHEMA)
                .addOutput("out", "b", "b-sink")
                .build();

        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(
                definition, Collections.singletonList("out"));

        assertEquals(Arrays.asList("s", "b"), stepIds(plan));
        assertEquals(PlanArtifact.RuntimeTarget.STREAM, plan.orderedUnits().get(0).runtimeTarget());
        assertEquals(PlanArtifact.RuntimeTarget.BATCH, plan.orderedUnits().get(1).runtimeTarget());
        assertEquals(PipelineEdge.BoundaryKind.VERSIONED_DATASET,
                plan.materializedEdges().get(0).boundaryKind());
    }

    @Test
    void disconnectedRootsComposeIntoIndependentUnits() {
        PipelineDefinition definition = PipelineDefinition.builder("p", "r")
                .addBatchStep("x", step("x", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addStreamStep("y", step("y", PlanArtifact.RuntimeTarget.STREAM).artifact())
                .addOutput("out-x", "x", "x-sink")
                .addOutput("out-y", "y", "y-sink")
                .build();

        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(definition,
                Arrays.asList("out-x", "out-y"));

        assertEquals(2, plan.orderedUnits().size());
        assertEquals(Arrays.asList("x", "y"), stepIds(plan));
        assertTrue(plan.materializedEdges().isEmpty());
        assertEquals(Arrays.asList("out-x", "out-y"), outputIds(plan));
    }

    @Test
    void oneProducerFansOutToThreeConsumers() {
        PipelineDefinition definition = PipelineDefinition.builder("p", "r")
                .addBatchStep("root", step("root", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addBatchStep("x", step("x", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addBatchStep("y", step("y", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addStreamStep("z", step("z", PlanArtifact.RuntimeTarget.STREAM).artifact())
                .addVersionedDatasetEdge("rx", "root", "root-sink", "x", "x-source", "d-x", SCHEMA)
                .addVersionedDatasetEdge("ry", "root", "root-sink", "y", "y-source", "d-y", SCHEMA)
                .addVersionedDatasetEdge("rz", "root", "root-sink", "z", "z-source", "d-z", SCHEMA)
                .addOutput("out-x", "x", "x-sink")
                .addOutput("out-y", "y", "y-sink")
                .addOutput("out-z", "z", "z-sink")
                .build();

        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(definition,
                Arrays.asList("out-x", "out-y", "out-z"));

        assertEquals(Arrays.asList("root", "x", "y", "z"), stepIds(plan));
        assertEquals(3, plan.materializedEdges().size());
        assertEquals(Arrays.asList("rx", "ry", "rz"), edgeIds(plan));
    }

    @Test
    void outputOnAPrunedStepStillCompilesToOneUnit() {
        PipelineDefinition definition = PipelineDefinition.builder("p", "r")
                .addBatchStep("a", step("a", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addBatchStep("b", step("b", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addVersionedDatasetEdge("ab", "a", "a-sink", "b", "b-source", "d-ab", SCHEMA)
                .addOutput("out-a", "a", "a-sink")
                .addOutput("out-b", "b", "b-sink")
                .build();

        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(
                definition, Collections.singletonList("out-a"));

        assertEquals(Collections.singletonList("a"), stepIds(plan));
        assertTrue(plan.materializedEdges().isEmpty());
        assertEquals(Collections.singletonList("out-a"), outputIds(plan));
    }

    /**
     * Two producers materialized into one step that declares a SOURCE operator
     * per input; {@code reversed} swaps only the declaration order.
     */
    private static PipelineDefinition fanIn(boolean reversed) {
        PipelineEdge leftEdge = new PipelineEdge("to-left", "left", "left-sink",
                "join", "join-left", "d-left", SCHEMA,
                PipelineEdge.BoundaryKind.VERSIONED_DATASET);
        PipelineEdge rightEdge = new PipelineEdge("to-right", "right", "right-sink",
                "join", "join-right", "d-right", SCHEMA,
                PipelineEdge.BoundaryKind.VERSIONED_DATASET);
        PipelineDefinitionBuilder builder = PipelineDefinition.builder("p", "r")
                .addBatchStep("left", step("left", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addBatchStep("right", step("right", PlanArtifact.RuntimeTarget.BATCH).artifact())
                .addBatchStep("join", fanInStep("join").artifact());
        if (reversed) {
            builder.addEdge(rightEdge).addEdge(leftEdge);
        } else {
            builder.addEdge(leftEdge).addEdge(rightEdge);
        }
        return builder.addOutput("joined", "join", "join-sink").build();
    }

    private static PipelineStep fanInStep(String id) {
        return new PipelineStep(id, new PlanArtifact(id,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION, PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.BATCH, PlanArtifact.Boundedness.BOUNDED,
                PlanArtifact.ChangelogMode.APPEND, PlanArtifact.TimeMode.NONE,
                Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                        PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1),
                Collections.<String>emptyList(),
                Arrays.asList(
                        new OperatorDescriptor(id + "-left", OperatorDescriptor.Kind.SOURCE,
                                "test.source", SCHEMA, Collections.<String>emptyList()),
                        new OperatorDescriptor(id + "-right", OperatorDescriptor.Kind.SOURCE,
                                "test.source", SCHEMA, Collections.<String>emptyList()),
                        new OperatorDescriptor(id + "-merge", OperatorDescriptor.Kind.BINARY,
                                "test.binary", SCHEMA,
                                Arrays.asList(id + "-left", id + "-right")),
                        new OperatorDescriptor(id + "-sink", OperatorDescriptor.Kind.SINK,
                                "test.sink", SCHEMA, Collections.singletonList(id + "-merge"))),
                PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1, new byte[] {1}));
    }

    private static PlanArtifact artifactWithFanInSinkAsEndpoint(String id) {
        return new PlanArtifact(id,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION, PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.BATCH, PlanArtifact.Boundedness.BOUNDED,
                PlanArtifact.ChangelogMode.APPEND, PlanArtifact.TimeMode.NONE,
                Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                        PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1),
                Collections.<String>emptyList(),
                Arrays.asList(
                        new OperatorDescriptor(id + "-left", OperatorDescriptor.Kind.SOURCE,
                                "test.source", SCHEMA, Collections.<String>emptyList()),
                        new OperatorDescriptor(id + "-right", OperatorDescriptor.Kind.SOURCE,
                                "test.source", SCHEMA, Collections.<String>emptyList()),
                        new OperatorDescriptor(id + "-sink", OperatorDescriptor.Kind.SINK,
                                "test.sink", SCHEMA,
                                Arrays.asList(id + "-left", id + "-right"))),
                PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1, new byte[] {1});
    }

    private static PipelineStep step(String id, PlanArtifact.RuntimeTarget runtime) {
        List<OperatorDescriptor> operators = Arrays.asList(
                new OperatorDescriptor(id + "-source", OperatorDescriptor.Kind.SOURCE,
                        "test.source", SCHEMA, Collections.<String>emptyList()),
                new OperatorDescriptor(id + "-sink", OperatorDescriptor.Kind.SINK,
                        "test.sink", SCHEMA, Collections.singletonList(id + "-source")));
        return new PipelineStep(id, new PlanArtifact(id,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION, PlanArtifact.CURRENT_READER_VERSION,
                runtime,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.Boundedness.BOUNDED : PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.TimeMode.NONE : PlanArtifact.TimeMode.PROCESSING_TIME,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                                PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1)
                        : Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                                PlanArtifact.FEATURE_PORTABLE_STREAM_JOB_V1,
                                PlanArtifact.FEATURE_RUNTIME_STREAM_V1),
                Collections.<String>emptyList(), operators,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1
                        : PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1,
                new byte[] {1}));
    }

    private static List<String> stepIds(PipelineExecutionPlan plan) {
        List<String> ids = new ArrayList<>();
        for (PipelineExecutionUnit unit : plan.orderedUnits()) ids.add(unit.stepId());
        return ids;
    }

    private static List<String> edgeIds(PipelineExecutionPlan plan) {
        List<String> ids = new ArrayList<>();
        for (PipelineEdge edge : plan.materializedEdges()) ids.add(edge.edgeId());
        return ids;
    }

    private static List<String> outputIds(PipelineExecutionPlan plan) {
        List<String> ids = new ArrayList<>();
        for (PipelineOutput output : plan.selectedOutputs()) ids.add(output.outputId());
        return ids;
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
    /**
     * The fan-in pinned above is described by hand-written operator metadata.
     * These cases take the same shape from the portable plan model the Batch
     * runtime compiles, so an accepted shape is not an artefact of the fixture.
     */
    @Test
    void fanInComposesFromPortableArtifactsTheBatchModelProduces() {
        PortableRowSchema schema = portableSchema();
        PortableBatchPlan fanInPlan = portableFanInPlan(schema, "join");
        PortablePlanCompiler.preflight(fanInPlan, ACCEPT_ALL_BINDINGS);

        PlanArtifact left = portableProducer(schema, "left");
        PlanArtifact right = portableProducer(schema, "right");
        PlanArtifact join = PortablePlanArtifact.wrap("join", fanInPlan);

        assertEquals(Arrays.asList("SOURCE", "SOURCE", "BINARY", "SINK"), kinds(join));
        assertEquals(PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1, join.payloadFormat());

        PipelineExecutionPlan forward = PipelineValidator.validateAndCompile(
                portableDefinition(left, right, join, schema, false, false),
                Collections.singletonList("joined"));
        PipelineExecutionPlan reversed = PipelineValidator.validateAndCompile(
                portableDefinition(left, right, join, schema, true, false),
                Collections.singletonList("joined"));

        assertEquals(Arrays.asList("left", "right", "join"), stepIds(forward));
        assertEquals(Arrays.asList("to-left", "to-right"), edgeIds(forward));
        assertEquals(stepIds(forward), stepIds(reversed));
        assertEquals(edgeIds(forward), edgeIds(reversed));
        assertEquals(forward.definitionDigest(), reversed.definitionDigest());
        assertEquals(Arrays.asList("SOURCE", "SOURCE", "BINARY", "SINK"),
                kinds(forward.orderedUnits().get(2).artifact()));
    }

    @Test
    void portableFanInStillRefusesTwoBoundariesOnOneSourceOperator() {
        PortableRowSchema schema = portableSchema();
        PlanArtifact left = portableProducer(schema, "left");
        PlanArtifact right = portableProducer(schema, "right");
        PlanArtifact join = PortablePlanArtifact.wrap("join",
                portableFanInPlan(schema, "join"));

        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class, () -> portableDefinition(
                        left, right, join, schema, false, true));
        assertEquals("DUPLICATE_SOURCE_BINDING", failure.code());
        assertEquals("pipeline.edges[1].consumerSourceOperatorId", failure.path());
    }

    /**
     * A Pipeline step takes a portable carrier only. The trusted in-process graph
     * that the BatchSession compiles is refused here, so composing stages cannot be
     * mistaken for submitting that graph to the Batch runtime.
     */
    @Test
    void trustedLegacyBatchArtifactIsRefusedAtTheStepSeam() {
        DataSchema meta = new DataSchema(new Field("id", DataType.LongType));
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(meta, Collections.<io.rowweave.DataRow>emptyList())
                .withParam(PlanNode.OPERATOR_UID_PARAM, "legacy-left"));
        nodes.add(PlanNode.source(meta, Collections.<io.rowweave.DataRow>emptyList())
                .withParam(PlanNode.OPERATOR_UID_PARAM, "legacy-right"));
        nodes.add(PlanNode.binary("join", meta, 0, 1).build()
                .withParam(PlanNode.OPERATOR_UID_PARAM, "legacy-merge"));
        nodes.add(PlanNode.sink(meta, 2, "collecting").build()
                .withParam(PlanNode.OPERATOR_UID_PARAM, "legacy-sink"));
        PlanArtifact legacy = LegacyPlanArtifactAdapter.wrap(
                "legacy-join-step", new PlanGraph(nodes, 1));

        assertEquals(PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH,
                legacy.payloadFormat());
        assertEquals(Arrays.asList("SOURCE", "SOURCE", "BINARY", "SINK"), kinds(legacy));

        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class, () ->
                        PipelineArtifactAdapter.forBatch("join", legacy));
        assertEquals("INVALID_ARTIFACT_METADATA", failure.code());
    }

    private static final PortableBindingVerifier ACCEPT_ALL_BINDINGS =
            new PortableBindingVerifier() {
                @Override public void verifySource(PortableSourceBinding binding) { }
                @Override public void verifyFunction(FunctionDeploymentReference reference) { }
                @Override public void verifySink(PortableSinkBinding binding) { }
            };

    private static PortableRowSchema portableSchema() {
        return new PortableRowSchema(Collections.singletonList(new PortableField(
                "id", "id", "id", PortableDataType.LONG, false)));
    }

    private static PortablePlanNode portableSourceNode(String id, PortableRowSchema schema) {
        return new PortablePlanNode(id, PortableOperatorCode.SOURCE,
                Collections.<String>emptyList(), Collections.<String>emptyList(), schema,
                Collections.<String, String>emptyMap(),
                Collections.<String, java.util.List<String>>emptyMap(),
                new PortableSourceBinding(id + "-provider-v1", repeat('a', 64),
                        new byte[] {1}, Collections.singletonList(
                                splitDescriptor(id, schema))));
    }

    private static PortablePlanNode portableSinkNode(
            String id, String inputId, PortableRowSchema schema) {
        return new PortablePlanNode(id, PortableOperatorCode.SINK,
                Collections.singletonList(inputId),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.<String, String>emptyMap(),
                Collections.<String, java.util.List<String>>emptyMap(),
                new PortableSinkBinding(id + "-provider-v1", repeat('b', 64),
                        sinkDescriptor(id, schema), new byte[] {7}));
    }

    private static PlanArtifact portableProducer(PortableRowSchema schema, String id) {
        PortableBatchPlan plan = new PortableBatchPlan(1, 1,
                Collections.singletonList(id + "-sink"),
                Arrays.asList(portableSourceNode(id + "-source", schema),
                        portableSinkNode(id + "-sink", id + "-source", schema)),
                Collections.singletonList(PortableBatchPlan.BATCH_CAPABILITY));
        return PortablePlanArtifact.wrap(plan.nodes().get(0).operatorId()
                .replace("-source", ""), plan);
    }

    private static PortableBatchPlan portableFanInPlan(PortableRowSchema schema, String id) {
        Map<String, String> joinParameters = new java.util.LinkedHashMap<>();
        joinParameters.put("joinType", "INNER");
        Map<String, java.util.List<String>> joinKeys = new java.util.LinkedHashMap<>();
        joinKeys.put("leftKeys", Collections.singletonList("id"));
        joinKeys.put("rightKeys", Collections.singletonList("id"));
        PortablePlanNode merge = new PortablePlanNode(id + "-merge",
                PortableOperatorCode.JOIN,
                Arrays.asList(id + "-left", id + "-right"),
                Arrays.asList(schema.fingerprint(), schema.fingerprint()), schema,
                joinParameters, joinKeys, null);
        return new PortableBatchPlan(1, 1,
                Collections.singletonList(id + "-sink"),
                Arrays.asList(portableSourceNode(id + "-left", schema),
                        portableSourceNode(id + "-right", schema), merge,
                        portableSinkNode(id + "-sink", id + "-merge", schema)),
                Collections.singletonList(PortableBatchPlan.BATCH_CAPABILITY));
    }

    private static PipelineDefinition portableDefinition(
            PlanArtifact left, PlanArtifact right, PlanArtifact join,
            PortableRowSchema schema, boolean reverseEdges, boolean bothOnOneSource) {
        String fingerprint = schema.fingerprint();
        PipelineEdge toLeft = new PipelineEdge("to-left", "left", "left-sink",
                "join", "join-left", "d-left", fingerprint,
                PipelineEdge.BoundaryKind.VERSIONED_DATASET);
        PipelineEdge toRight = new PipelineEdge("to-right", "right", "right-sink",
                "join", bothOnOneSource ? "join-left" : "join-right",
                "d-right", fingerprint, PipelineEdge.BoundaryKind.VERSIONED_DATASET);
        PipelineDefinitionBuilder builder = PipelineDefinition.builder("portable-fan-in", "r1")
                .addBatchStep("left", left)
                .addBatchStep("right", right)
                .addBatchStep("join", join);
        if (reverseEdges) {
            builder.addEdge(toRight).addEdge(toLeft);
        } else {
            builder.addEdge(toLeft).addEdge(toRight);
        }
        return builder.addOutput("joined", "join", "join-sink").build();
    }

    private static String runtimeFingerprint(PortableRowSchema schema) {
        return SchemaFingerprint.of(PortablePlanCompiler.toDataSchema(schema));
    }

    private static byte[] splitDescriptor(String id, PortableRowSchema schema) {
        return BatchSourceSplitDescriptorCodec.encode(new BatchSourceSplitDescriptor(
                1, "rowweave.test.source", 1, id + "-split-1",
                runtimeFingerprint(schema), "snapshot-1",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START));
    }

    private static byte[] sinkDescriptor(String id, PortableRowSchema schema) {
        return BatchSinkDescriptorCodec.encode(new BatchSinkDescriptor(
                1, "rowweave.test.sink", 1, runtimeFingerprint(schema),
                id + "-destination", id + "-domain",
                BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1));
    }

    private static List<String> kinds(PlanArtifact artifact) {
        List<String> kinds = new ArrayList<>();
        for (OperatorDescriptor operator : artifact.operators()) {
            kinds.add(operator.kind().name());
        }
        return kinds;
    }

}
