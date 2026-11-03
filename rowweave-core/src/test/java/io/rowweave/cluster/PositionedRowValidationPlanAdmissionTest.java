package io.rowweave.cluster;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.SourceExecutor;
import io.rowweave.SourceExecutorFactory;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.local.LocalJobClient;
import io.rowweave.ops.RowValidation;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** API, admission and local/cluster assembly for positioned validation. */
class PositionedRowValidationPlanAdmissionTest {

    private static final String MODE_PARAM = "sourcePositionMode";
    private static final String EXACT_MODE = "EXACT_SOURCE_V1";
    private static final String SOURCE_UID = "ledger.source";
    private static final Pattern LOCATED = Pattern.compile(
            "source=ledger\\.source split=[0-9a-f]{64} ordinal=1");
    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final RowValidation VALIDATION = RowValidation.builder(META)
            .notNull("adaptor.required", 0)
            .integerRange("period.range", 1, 1, 12)
            .decimalRange("amount.range", 2,
                    new BigDecimal("-100"), new BigDecimal("100"))
            .build();

    @BeforeAll
    static void registerProbeExecutor() {
        SourceExecutorFactory.register(ProbeInput.class, ProbeExecutor::new);
    }

    @Test
    void apiRequiresExactUidSourceAndRoundTripKeepsSingleModeParam() {
        ExecutionEnvironment session = new ExecutionEnvironment("position-plan", "position plan");
        DataSet source = session.fromSource(exact("split-a", validRows())).uid(SOURCE_UID);
        source.validateRowsWithSourcePosition(VALIDATION)
                .sinkTo(new DiscardSink(META));

        PlanGraph graph = PlanGraph.fromBytes(session.toPlanGraph().toBytes());
        assertEquals(SOURCE_UID, graph.node(0).param(PlanNode.OPERATOR_UID_PARAM));
        assertEquals("rowValidate", graph.node(1).typeId());
        assertEquals(EXACT_MODE, graph.node(1).param(MODE_PARAM));
        assertEquals(1, graph.node(1).params().size());

        assertApiInvalid(() -> new ExecutionEnvironment("missing-uid", "missing uid")
                .fromSource(exact("split", validRows()))
                .validateRowsWithSourcePosition(VALIDATION));
        assertApiInvalid(() -> source.validateRows(VALIDATION)
                .validateRowsWithSourcePosition(VALIDATION));
        assertApiInvalid(() -> new ExecutionEnvironment("legacy", "legacy")
                .fromSource(new CollectionSource(META, validRows())).uid(SOURCE_UID)
                .validateRowsWithSourcePosition(VALIDATION));
    }

    @Test
    void capabilityRejectsFutureModeWrongTopologyMissingUidAndLegacySource() {
        PlanNode exactSource = PlanNode.sourceFromInputs(META,
                        Collections.<Source>singletonList(exact("split", validRows())))
                .withParam(PlanNode.OPERATOR_UID_PARAM, SOURCE_UID);
        assertPlanInvalid(plan(exactSource,
                validationNode(0, "FUTURE_SOURCE_MODE")));
        assertPlanInvalid(plan(PlanNode.sourceFromInputs(META,
                        Collections.<Source>singletonList(exact("split", validRows()))),
                validationNode(0, EXACT_MODE)));

        PlanNode filter = PlanNode.unary("filter", META, 0)
                .param("expr", "period > 0").build();
        assertPlanInvalid(new PlanGraph(Arrays.asList(
                exactSource, filter, validationNode(1, EXACT_MODE),
                PlanNode.sink(META, 2, new DiscardSink(META)).build())));

        PlanNode legacy = PlanNode.sourceFromInputs(META,
                        Collections.<Source>singletonList(
                                new CollectionSource(META, validRows())))
                .withParam(PlanNode.OPERATOR_UID_PARAM, SOURCE_UID);
        assertPlanInvalid(plan(legacy, validationNode(0, EXACT_MODE)));
    }

    @Test
    void masterRejectsModeTamperBeforeAnyWorkerAssignment() {
        AtomicInteger assignments = new AtomicInteger();
        String host = "position-worker";
        int port = 61_099;
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
                        "position-worker", host, port, 8, -1, "inc-position"));
                PlanNode source = PlanNode.sourceFromInputs(META,
                                Collections.<Source>singletonList(exact("split", validRows())))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, SOURCE_UID);
                RpcMessage result = master.handler().apply(RpcMessage.submitJob(
                        plan(source, validationNode(0, "FUTURE_SOURCE_MODE")).toBytes()));

                assertFalse(result.success());
                assertTrue(result.errorMessage().contains("QUALITY_SOURCE_POSITION_INVALID"),
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
    void workerRebuildRejectsTamperBeforeOpeningSource() {
        AtomicInteger opens = new AtomicInteger();
        ProbeInput input = new ProbeInput(opens);
        PlanNode source = PlanNode.sourceFromInputs(META,
                        Collections.<Source>singletonList(input))
                .withParam(PlanNode.OPERATOR_UID_PARAM, SOURCE_UID);
        StageTask.StageResult result = new StageTask(
                StageCommand.wholeGraph(plan(
                        source, validationNode(0, "FUTURE_SOURCE_MODE"))),
                StageTask.PortListener.NONE).run();

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertTrue(allMessages(result.failure()).contains("QUALITY_SOURCE_POSITION_INVALID"));
        assertEquals(0, opens.get());
    }

    @Test
    void localAndInProcessP1P2P4MatchOracle(@TempDir Path temp) throws Exception {
        List<String> oracle = Arrays.asList(
                "A001|1|10.000", "B002|2|0", "C003|12|-5.50");
        assertEquals(oracle, run(null, 1, temp.resolve("local-p1.tsv")));
        assertEquals(oracle, run(null, 2, temp.resolve("local-p2.tsv")));
        assertEquals(oracle, run(null, 4, temp.resolve("local-p4.tsv")));
        try (InProcessCluster cluster = new InProcessCluster(
                1, temp.resolve("cluster-p1").toFile())) {
            assertEquals(oracle, run(cluster, 1, temp.resolve("cluster-p1.tsv")));
        }
        try (InProcessCluster cluster = new InProcessCluster(
                2, temp.resolve("cluster-p2").toFile())) {
            assertEquals(oracle, run(cluster, 2, temp.resolve("cluster-p2.tsv")));
        }
        try (InProcessCluster cluster = new InProcessCluster(
                4, temp.resolve("cluster-p4").toFile())) {
            assertEquals(oracle, run(cluster, 4, temp.resolve("cluster-p4.tsv")));
        }
    }

    @Test
    void reopenProducesTheSameStableLocatedFailureWithoutRawSplitId() {
        String first = locatedFailure();
        String second = locatedFailure();
        assertEquals(first, second);
        assertTrue(first.startsWith("QUALITY_RULE_VIOLATION"), first);
        assertTrue(LOCATED.matcher(first).find(), first);
        assertFalse(first.contains("domain/private-split"), first);
        assertFalse(first.contains("bad-period-value"), first);
    }

    @Test
    void positionedValidationStaysInParallelSourceRouterBeforeKeyedShuffle() {
        io.rowweave.ops.ParallelGroupByAgg aggregate =
                io.rowweave.ops.ParallelGroupByAgg.of(META, "adaptor").count();
        DataSchema grouped = aggregate.getOutputSchema();
        PlanNode source = PlanNode.sourceFromInputs(META, Arrays.<Source>asList(
                        exact("split-a", Arrays.asList(
                                row("A", 1, BigDecimal.ZERO),
                                row("B", 2, BigDecimal.ZERO))),
                        exact("split-b", Collections.singletonList(
                                row("A", 3, BigDecimal.ZERO)))))
                .withParam(PlanNode.OPERATOR_UID_PARAM, SOURCE_UID);
        PlanNode validation = validationNode(0, EXACT_MODE);
        PlanNode groupedNode = PlanNode.unary("groupByCount", grouped, 1)
                .listParam("keys", Collections.singletonList("adaptor"))
                .build();
        PlanGraph graph = new PlanGraph(Arrays.asList(
                source, validation, groupedNode,
                PlanNode.sink(grouped, 2, new DiscardSink(grouped)).build()), 4);

        ExecutionReport report = new PushDagExecutor(4).runSinkGraphReported(graph);
        assertEquals(2L, report.rowsOut());
        assertEquals(4, report.partitionRowCounts().length);
        assertEquals(3L, Arrays.stream(report.partitionRowCounts()).sum());
    }

    private static List<String> run(JobClient client, int parallelism, Path output) {
        ExecutionEnvironment session = client == null
                ? new ExecutionEnvironment("position-runtime", "position runtime")
                : new ExecutionEnvironment("position-runtime", "position runtime", client);
        session.setParallelism(parallelism);
        DataSet source = session.fromSource(
                exact("split-a", Arrays.asList(
                        row("C003", 12, new BigDecimal("-5.50")),
                        row("A001", 1, new BigDecimal("10.000")))),
                exact("split-b", Collections.singletonList(
                        row("B002", 2, BigDecimal.ZERO))))
                .uid(SOURCE_UID);
        source.validateRowsWithSourcePosition(VALIDATION)
                .sinkTo(new FileSink(META, output.toString()));
        session.execute();
        List<String> actual = new ArrayList<>();
        for (DataRow row : FileSink.read(META, output.toString())) {
            actual.add(row.getString(0) + "|" + row.getInteger(1) + "|"
                    + row.getBigDecimal(2).toPlainString());
        }
        Collections.sort(actual);
        return actual;
    }

    private static String locatedFailure() {
        ExecutionEnvironment session = new ExecutionEnvironment("position-failure", "position failure");
        DataSet source = session.fromSource(exact("domain/private-split", Arrays.asList(
                row("A", 1, BigDecimal.ZERO),
                row("B", 99, BigDecimal.ZERO)))).uid(SOURCE_UID);
        source.validateRowsWithSourcePosition(VALIDATION)
                .sinkTo(new DiscardSink(META));
        RuntimeException failure = assertThrows(RuntimeException.class, session::execute);
        Matcher match = Pattern.compile(
                "QUALITY_RULE_VIOLATION[^\\r\\n]*(?:source=ledger\\.source)"
                        + "[^\\r\\n]*ordinal=1").matcher(allMessages(failure));
        assertTrue(match.find(), allMessages(failure));
        return match.group();
    }

    private static PlanGraph plan(PlanNode source, PlanNode validation) {
        return new PlanGraph(Arrays.asList(source, validation,
                PlanNode.sink(META, 1, new DiscardSink(META)).build()));
    }

    private static PlanNode validationNode(int child, String mode) {
        PlanNode.Builder builder = PlanNode.unary("rowValidate", META, child)
                .listParam("ruleIds", VALIDATION.ruleIds())
                .listParam("ruleKinds", VALIDATION.ruleKinds())
                .listParam("fieldIndices", VALIDATION.fieldIndices())
                .listParam("lowerBounds", VALIDATION.lowerBounds())
                .listParam("upperBounds", VALIDATION.upperBounds());
        if (mode != null) builder.param(MODE_PARAM, mode);
        return builder.build();
    }

    private static void assertPlanInvalid(PlanGraph graph) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ClusterPlanCapabilityValidator.validate(graph));
        assertTrue(failure.getMessage().contains("QUALITY_SOURCE_POSITION_INVALID"),
                failure::getMessage);
    }

    private static void assertApiInvalid(org.junit.jupiter.api.function.Executable executable) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(failure.getMessage().startsWith("QUALITY_SOURCE_POSITION_INVALID:"),
                failure::getMessage);
    }

    private static CollectionSource exact(String splitId, List<DataRow> rows) {
        return new CollectionSource(META, splitId, rows);
    }

    private static List<DataRow> validRows() {
        return Collections.singletonList(row("A", 1, BigDecimal.ZERO));
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
