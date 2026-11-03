package io.rowweave.cluster;

import io.rowweave.AssignedSourceBatch;
import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.SourceExecutor;
import io.rowweave.SourceExecutorFactory;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.local.LocalJobClient;
import io.rowweave.ops.RowValidation;
import io.rowweave.ops.TypedRowClassification;
import io.rowweave.plan.OutputCommitValidator;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;
import io.rowweave.plan.TypedRowClassificationPlanValidator;
import io.rowweave.sched.net.ShuffleEndpoint;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** full-topology admission and fragment-local worker rebuild. */
class TypedRowClassificationPlanAdmissionTest {

    private static final String SOURCE_UID = "ledger.source";
    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final RowValidation VALIDATION = RowValidation.builder(META)
            .notNull("adaptor.required", 0)
            .integerRange("period.range", 1, 1, 12)
            .decimalRange("amount.range", 2,
                    new BigDecimal("-10"), new BigDecimal("10"))
            .build();
    private static final DataSchema REJECTED_META = new DataSchema(
            new Field("source_operator_uid", DataType.StringType),
            new Field("split_identity_sha256", DataType.StringType),
            new Field("split_local_ordinal", DataType.LongType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));
    private static final DataSchema CARRIER_META = carrierMeta();

    @BeforeAll
    static void registerProbeExecutor() {
        SourceExecutorFactory.register(ProbeInput.class, ProbeExecutor::new);
    }

    @Test
    void apiRoundTripKeepsCanonicalNodesAndLocalIrExecutesSharedClassifierOnce() {
        ExecutionEnvironment session = new ExecutionEnvironment("classification-plan", "classification plan");
        DataSet source = session.fromSource(exact("split-a", Arrays.asList(
                row("A", 1, BigDecimal.ZERO),
                row(null, 2, BigDecimal.ZERO)))).uid(SOURCE_UID);
        DataRowClassification classification = source.classifyRowsWithSourcePosition(VALIDATION);
        classification.validRows().sinkTo(new DiscardSink(META));
        classification.rejectedRows().sinkTo(new DiscardSink(REJECTED_META));

        PlanGraph graph = PlanGraph.fromBytes(session.toPlanGraph().toBytes());
        assertEquals(6, graph.nodeCount());
        assertEquals("rowClassify", graph.node(1).typeId());
        assertEquals("EXACT_SOURCE_V1", graph.node(1).param("sourcePositionMode"));
        assertEquals("1", graph.node(1).param("carrierVersion"));
        assertEquals("BOTH_V1", graph.node(1).param("routeContract"));
        assertEquals(3, graph.node(1).params().size());
        assertEquals(VALIDATION.ruleIds(), graph.node(1).listParam("ruleIds"));
        assertEquals(5, graph.node(1).listParams().size());
        assertEquals("qualityRoute", graph.node(2).typeId());
        assertEquals("VALID", graph.node(2).param("route"));
        assertEquals("qualityRoute", graph.node(4).typeId());
        assertEquals("REJECTED", graph.node(4).param("route"));
        assertEquals(1, graph.node(2).child(0));
        assertEquals(1, graph.node(4).child(0));

        assertTrue(PlanOps.supportsUnaryType("rowClassify"));
        assertTrue(PlanOps.supportsUnaryType("qualityRoute"));
        assertTrue(PlanOps.isPassThrough("rowClassify"));
        assertTrue(PlanOps.isPassThrough("qualityRoute"));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(graph));
        assertTrue(graph.toJson().contains("rowClassify"));
        assertTrue(graph.toJson().contains("qualityRoute"));
        assertEquals(2L, new LocalJobClient(false).submit(graph).rowsOut());
    }

    @Test
    void fullGraphRequiresReachableUniqueDualRoutesAndNoThirdConsumer() {
        assertInvalid(graph(source(), classifier(0, CARRIER_META, "EXACT_SOURCE_V1", "1", "BOTH_V1"),
                route(1, META, "VALID", "1"), sink(2, META)));

        assertInvalid(graph(source(), classifier(0, CARRIER_META, "EXACT_SOURCE_V1", "1", "BOTH_V1"),
                route(1, META, "VALID", "1"), sink(2, META),
                route(1, META, "VALID", "1"), sink(4, META),
                route(1, REJECTED_META, "REJECTED", "1"), sink(6, REJECTED_META)));

        PlanNode filter = PlanNode.unary("filter", CARRIER_META, 1)
                .param("expr", "period > 0").build();
        assertInvalid(graph(source(), classifier(0, CARRIER_META, "EXACT_SOURCE_V1", "1", "BOTH_V1"),
                route(1, META, "VALID", "1"), sink(2, META),
                route(1, REJECTED_META, "REJECTED", "1"), sink(4, REJECTED_META),
                filter, sink(6, CARRIER_META)));

        assertInvalid(graph(source(), classifier(0, CARRIER_META, "EXACT_SOURCE_V1", "1", "BOTH_V1"),
                route(1, META, "VALID", "1"), sink(2, META),
                route(1, REJECTED_META, "REJECTED", "1")));

        assertInvalid(graph(source(), classifier(0, CARRIER_META, "EXACT_SOURCE_V1", "1", "BOTH_V1"),
                route(0, META, "VALID", "1"), sink(2, META),
                route(1, REJECTED_META, "REJECTED", "1"), sink(4, REJECTED_META)));
    }

    @Test
    void fullAndWorkerAdmissionRejectDescriptorAndMetaTamper() {
        assertInvalid(canonicalGraph(classifier(0, CARRIER_META,
                "FUTURE_MODE", "1", "BOTH_V1")));
        assertInvalid(canonicalGraph(classifier(0, CARRIER_META,
                "EXACT_SOURCE_V1", "2", "BOTH_V1")));
        assertInvalid(canonicalGraph(classifier(0, CARRIER_META,
                "EXACT_SOURCE_V1", "1", "ONE_ONLY")));
        assertInvalid(canonicalGraph(classifier(0, META,
                "EXACT_SOURCE_V1", "1", "BOTH_V1")));
        assertInvalid(canonicalGraph(route(1, META, "FUTURE", "1"), 2));
        assertInvalid(canonicalGraph(route(1, META, "VALID", "2"), 2));
        assertInvalid(canonicalGraph(route(1, REJECTED_META, "VALID", "1"), 2));
        assertInvalid(canonicalGraph(route(1, META, "REJECTED", "1"), 4));

        PlanNode extraClassifier = classifier(0, CARRIER_META,
                "EXACT_SOURCE_V1", "1", "BOTH_V1")
                .withParam("unexpected", "value");
        assertInvalid(canonicalGraph(extraClassifier));
        PlanNode extraRoute = route(1, META, "VALID", "1")
                .withParam("unexpected", "value");
        assertInvalid(canonicalGraph(extraRoute, 2));

        PlanNode canonicalClassifier = classifier(0, CARRIER_META,
                "EXACT_SOURCE_V1", "1", "BOTH_V1");
        Map<String, List<String>> missingLists =
                new LinkedHashMap<>(canonicalClassifier.listParams());
        missingLists.remove("upperBounds");
        assertInvalid(canonicalGraph(PlanNode.unaryFnWithParams(
                "rowClassify", CARRIER_META, 0, canonicalClassifier.params(),
                missingLists, "forbidden-user-function")));

        PlanNode listRoute = PlanNode.unary("qualityRoute", META, 1)
                .param("carrierVersion", "1").param("route", "VALID")
                .listParam("unexpected", Collections.singletonList("value")).build();
        assertInvalid(canonicalGraph(listRoute, 2));
        assertInvalid(canonicalGraph(PlanNode.unaryFnWithParams(
                "qualityRoute", META, 1, route(1, META, "VALID", "1").params(),
                Collections.<String, List<String>>emptyMap(),
                "forbidden-user-function"), 2));
    }

    @Test
    void outputGateAndMasterRejectIncompleteTopologyBeforeSideEffects() {
        PlanGraph incomplete = graph(source(),
                classifier(0, CARRIER_META, "EXACT_SOURCE_V1", "1", "BOTH_V1"),
                route(1, META, "VALID", "1"), sink(2, META));
        assertThrows(IllegalArgumentException.class,
                () -> OutputCommitValidator.validate(incomplete));

        AtomicInteger assignments = new AtomicInteger();
        String host = "classification-worker";
        int port = 61_100;
        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration registration = InProcessRpcRegistry.register(
                    host, port, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            assignments.incrementAndGet();
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "quality-worker", host, port, 8, -1, "inc-classification"));
                RpcMessage result = master.handler().apply(
                        RpcMessage.submitJob(incomplete.toBytes()));
                assertFalse(result.success());
                assertTrue(result.errorMessage().contains("QUALITY_CLASSIFICATION_INVALID"),
                        result.errorMessage());
                assertEquals(0, assignments.get());
                assertEquals(0, master.activeStageTrackerCount());
                assertEquals(0, master.activeStageAssignmentCount());
            } finally {
                registration.close();
            }
        }
    }

    @Test
    void workerValidatesClassifierAndRouteFragmentsBeforeOpeningInputOrInbound() {
        AtomicInteger opens = new AtomicInteger();
        ProbeInput probe = new ProbeInput(opens);
        BatchSourceSplitBinding binding = new BatchSourceSplitBinding(
                probe, probe.batchSourceSplit());
        PlanNode placeholder = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, SOURCE_UID);
        PlanGraph classifierFragment = graph(placeholder,
                classifier(0, CARRIER_META, "EXACT_SOURCE_V1", "2", "BOTH_V1"));
        StageTask.StageResult classifierResult = new StageTask(
                StageCommand.sourceStage(
                        META, CARRIER_META, 8,
                        new AssignedSourceBatch(Collections.singletonList(binding)),
                        classifierFragment,
                        new ShuffleEndpoint("127.0.0.1", 61_101)),
                StageTask.PortListener.NONE).run();
        assertEquals(StageTask.StopReason.FAILED, classifierResult.reason());
        assertTrue(allMessages(classifierResult.failure())
                .contains("QUALITY_CLASSIFICATION_INVALID"));
        assertEquals(0, opens.get());

        PlanGraph validRouteFragment = graph(
                PlanNode.placeholderSource(CARRIER_META),
                route(0, REJECTED_META, "VALID", "1"));
        assertThrows(IllegalArgumentException.class,
                () -> TypedRowClassificationPlanValidator
                        .validateWorkerFragment(validRouteFragment));
    }

    private static PlanGraph canonicalGraph(PlanNode replacement) {
        return canonicalGraph(replacement, 1);
    }

    private static PlanGraph canonicalGraph(PlanNode replacement, int index) {
        List<PlanNode> nodes = new ArrayList<>(Arrays.asList(
                source(),
                classifier(0, CARRIER_META, "EXACT_SOURCE_V1", "1", "BOTH_V1"),
                route(1, META, "VALID", "1"), sink(2, META),
                route(1, REJECTED_META, "REJECTED", "1"), sink(4, REJECTED_META)));
        nodes.set(index, replacement);
        return new PlanGraph(nodes);
    }

    private static PlanNode source() {
        return PlanNode.sourceFromInputs(META,
                        Collections.<Source>singletonList(exact("split-a",
                                Collections.singletonList(row("A", 1, BigDecimal.ZERO)))))
                .withParam(PlanNode.OPERATOR_UID_PARAM, SOURCE_UID);
    }

    private static PlanNode classifier(
            int child, DataSchema outMeta, String mode, String version, String contract) {
        return PlanNode.unary("rowClassify", outMeta, child)
                .param("sourcePositionMode", mode)
                .param("carrierVersion", version)
                .param("routeContract", contract)
                .listParam("ruleIds", VALIDATION.ruleIds())
                .listParam("ruleKinds", VALIDATION.ruleKinds())
                .listParam("fieldIndices", VALIDATION.fieldIndices())
                .listParam("lowerBounds", VALIDATION.lowerBounds())
                .listParam("upperBounds", VALIDATION.upperBounds())
                .build();
    }

    private static PlanNode route(
            int child, DataSchema outMeta, String route, String version) {
        return PlanNode.unary("qualityRoute", outMeta, child)
                .param("carrierVersion", version)
                .param("route", route)
                .build();
    }

    private static PlanNode sink(int child, DataSchema meta) {
        return PlanNode.sink(meta, child, new DiscardSink(meta)).build();
    }

    private static PlanGraph graph(PlanNode... nodes) {
        return new PlanGraph(Arrays.asList(nodes));
    }

    private static void assertInvalid(PlanGraph graph) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ClusterPlanCapabilityValidator.validate(graph));
        assertTrue(failure.getMessage().contains("QUALITY_CLASSIFICATION_INVALID"),
                failure::getMessage);
    }

    private static CollectionSource exact(String splitId, List<DataRow> rows) {
        return new CollectionSource(META, splitId, rows);
    }

    private static DataSchema carrierMeta() {
        Field[] fields = new Field[META.getFieldCount() + 8];
        System.arraycopy(META.getFields(), 0, fields, 0, META.getFieldCount());
        int offset = META.getFieldCount();
        fields[offset] = new Field("__rowweave_quality_route_v1", DataType.IntegerType);
        System.arraycopy(REJECTED_META.getFields(), 0, fields, offset + 1, 7);
        return new DataSchema(fields);
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static String allMessages(Throwable failure) {
        StringBuilder result = new StringBuilder();
        Throwable cursor = failure;
        while (cursor != null) {
            if (cursor.getMessage() != null) result.append(cursor.getMessage()).append('\n');
            cursor = cursor.getCause();
        }
        return result.toString();
    }

    private static final class ProbeInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final transient AtomicInteger opens;
        private final BatchSourceSplitDescriptor descriptor;

        private ProbeInput(AtomicInteger opens) {
            this.opens = opens;
            this.descriptor = new BatchSourceSplitDescriptor(
                    1, "probe", 1, "probe-split", SchemaFingerprint.of(META),
                    "probe-snapshot",
                    BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
        }

        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() {
            throw new AssertionError("raw iterator fallback is forbidden");
        }
    }

    private static final class ProbeExecutor extends SourceExecutor<ProbeInput> {
        private ProbeExecutor(ProbeInput input) { super(input, META); }
        @Override public void open() { input.opens.incrementAndGet(); }
        @Override public boolean hasNext() { return false; }
        @Override public DataRow next() { throw new AssertionError("no rows"); }
        @Override public void close() { }
    }
}
