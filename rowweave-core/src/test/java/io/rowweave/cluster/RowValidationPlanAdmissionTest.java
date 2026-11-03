package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.local.LocalJobClient;
import io.rowweave.ops.RowValidation;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** describable execution and pre-side-effect validation admission. */
class RowValidationPlanAdmissionTest {

    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType),
            new Field("sequence", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));
    private static final RowValidation VALIDATION = RowValidation.builder(META)
            .notNull("adaptor.required", 0)
            .stringUtf8BytesRange("adaptor.length", 0, 1, 16)
            .integerRange("period.range", 1, 1, 12)
            .longRange("sequence.range", 2, 0L, 100L)
            .decimalRange("amount.range", 3,
                    new BigDecimal("-100.00"), new BigDecimal("100.00"))
            .build();

    @Test
    void planRoundTripKeepsDescriptionAndExecutesLocally(@TempDir Path temp) {
        Path output = temp.resolve("local.tsv");
        ExecutionEnvironment session = new ExecutionEnvironment("quality-plan", "quality plan");
        session.fromSource(new CollectionSource(META, rows()))
                .validateRows(VALIDATION)
                .sinkTo(new FileSink(META, output.toString()));

        PlanGraph graph = PlanGraph.fromBytes(session.toPlanGraph().toBytes());
        PlanNode node = graph.node(1);
        assertEquals("rowValidate", node.typeId());
        assertEquals(VALIDATION.ruleIds(), node.listParam("ruleIds"));
        assertEquals(VALIDATION.ruleKinds(), node.listParam("ruleKinds"));
        assertEquals(VALIDATION.fieldIndices(), node.listParam("fieldIndices"));
        assertEquals(VALIDATION.lowerBounds(), node.listParam("lowerBounds"));
        assertEquals(VALIDATION.upperBounds(), node.listParam("upperBounds"));

        assertEquals(2L, new LocalJobClient(2).submitToSink(graph));
        assertEquals(oracle(), read(output));
    }

    @Test
    void malformedDescriptionsFailCapabilityPreflightBeforeInputOpen() {
        AtomicInteger iteratorCalls = new AtomicInteger();
        Source source = new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public java.util.Iterator<DataRow> iterator() {
                iteratorCalls.incrementAndGet();
                return Collections.<DataRow>emptyList().iterator();
            }
        };
        List<String> future = new ArrayList<>(VALIDATION.ruleKinds());
        future.set(0, "FUTURE_RULE");
        List<String> nonCanonical = new ArrayList<>(VALIDATION.fieldIndices());
        nonCanonical.set(0, "00");

        assertInvalid(graph(source, Arrays.asList(
                Collections.singletonList("short"), VALIDATION.ruleKinds(),
                VALIDATION.fieldIndices(), VALIDATION.lowerBounds(), VALIDATION.upperBounds())));
        assertInvalid(graph(source, Arrays.asList(
                VALIDATION.ruleIds(), future, VALIDATION.fieldIndices(),
                VALIDATION.lowerBounds(), VALIDATION.upperBounds())));
        assertInvalid(graph(source, Arrays.asList(
                VALIDATION.ruleIds(), VALIDATION.ruleKinds(), nonCanonical,
                VALIDATION.lowerBounds(), VALIDATION.upperBounds())));
        assertEquals(0, iteratorCalls.get());
    }

    @Test
    void masterRejectsTamperBeforeWorkerAssignment() {
        AtomicInteger assignments = new AtomicInteger();
        String host = "row-validation-worker";
        int port = 61_098;
        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration registration = InProcessRpcRegistry.register(host, port, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) assignments.incrementAndGet();
                return RpcMessage.ack();
            });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "quality-worker", host, port, 8, -1, "inc-row-validation"));
                List<String> future = new ArrayList<>(VALIDATION.ruleKinds());
                future.set(0, "FUTURE_RULE");

                RpcMessage result = master.handler().apply(RpcMessage.submitJob(
                        graph(null, Arrays.asList(
                                VALIDATION.ruleIds(), future, VALIDATION.fieldIndices(),
                                VALIDATION.lowerBounds(), VALIDATION.upperBounds())).toBytes()));

                assertFalse(result.success());
                assertTrue(result.errorMessage().contains("QUALITY_VALIDATION_INVALID"),
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
    void localAndInProcessP1P2P4MatchOracle(@TempDir Path temp) throws Exception {
        assertEquals(oracle(), run(null, 1, temp.resolve("local-p1.tsv")));
        assertEquals(oracle(), run(null, 2, temp.resolve("local-p2.tsv")));
        assertEquals(oracle(), run(null, 4, temp.resolve("local-p4.tsv")));
        try (InProcessCluster cluster = new InProcessCluster(1, temp.resolve("cluster-p1").toFile())) {
            assertEquals(oracle(), run(cluster, 1, temp.resolve("cluster-p1.tsv")));
        }
        try (InProcessCluster cluster = new InProcessCluster(2, temp.resolve("cluster-p2").toFile())) {
            assertEquals(oracle(), run(cluster, 2, temp.resolve("cluster-p2.tsv")));
        }
        try (InProcessCluster cluster = new InProcessCluster(4, temp.resolve("cluster-p4").toFile())) {
            assertEquals(oracle(), run(cluster, 4, temp.resolve("cluster-p4.tsv")));
        }
    }

    @Test
    void validationRemainsVisibleAndFusible() {
        ExecutionEnvironment session = new ExecutionEnvironment("quality-visible", "quality visible");
        DataSet checked = session.fromSource(new CollectionSource(META, rows()))
                .validateRows(VALIDATION)
                .filter("period > 0");
        checked.sinkTo(new io.rowweave.DiscardSink(checked.getSchema()));
        PlanGraph graph = session.toPlanGraph();

        assertEquals("rowValidate", graph.node(1).typeId());
        assertEquals("filter", graph.node(2).typeId());
        assertTrue(io.rowweave.plan.PlanOps.isPassThrough("rowValidate"));
        assertTrue(graph.toJson().contains("rowValidate"));
    }

    private static PlanGraph graph(Source input, List<List<String>> params) {
        PlanNode validation = PlanNode.unary("rowValidate", META, 0)
                .listParam("ruleIds", params.get(0))
                .listParam("ruleKinds", params.get(1))
                .listParam("fieldIndices", params.get(2))
                .listParam("lowerBounds", params.get(3))
                .listParam("upperBounds", params.get(4))
                .build();
        return new PlanGraph(Arrays.asList(
                input == null
                        ? PlanNode.source(META, Collections.emptyList())
                        : PlanNode.sourceFromInputs(META, Collections.singletonList(input)),
                validation,
                PlanNode.sink(META, 1, new io.rowweave.DiscardSink(META)).build()));
    }

    private static void assertInvalid(PlanGraph graph) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ClusterPlanCapabilityValidator.validate(graph));
        assertTrue(failure.getMessage().contains("QUALITY_VALIDATION_INVALID"),
                failure::getMessage);
    }

    private static List<String> run(JobClient client, int parallelism, Path output) {
        ExecutionEnvironment session = client == null
                ? new ExecutionEnvironment("quality-runtime", "quality runtime")
                : new ExecutionEnvironment("quality-runtime", "quality runtime", client);
        session.setParallelism(parallelism);
        session.fromSource(new CollectionSource(META, rows()))
                .validateRows(VALIDATION)
                .sinkTo(new FileSink(META, output.toString()));
        session.execute();
        return read(output);
    }

    private static List<String> read(Path output) {
        List<String> result = new ArrayList<>();
        for (DataRow row : FileSink.read(META, output.toString())) {
            result.add(row.getString(0) + "|" + row.getInteger(1) + "|"
                    + row.getLong(2) + "|" + row.getBigDecimal(3).toPlainString());
        }
        Collections.sort(result);
        return result;
    }

    private static List<String> oracle() {
        return Arrays.asList("A001|1|1|10.000", "C003|12|100|-5.50");
    }

    private static List<DataRow> rows() {
        return Arrays.asList(
                row("C003", 12, 100L, new BigDecimal("-5.50")),
                row("A001", 1, 1L, new BigDecimal("10.000")));
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }
}
