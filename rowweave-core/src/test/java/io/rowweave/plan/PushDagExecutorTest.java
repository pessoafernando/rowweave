package io.rowweave.plan;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.SortField;
import io.rowweave.sched.PipelineExecutor;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PushDagExecutor runs a PlanGraph IR by pushing rows into the sink one at a time. The executor
 * returns no List and keeps no memo table; CollectingRowSink and ArrayList appear only here, to
 * gather rows for assertions.
 */
class PushDagExecutorTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("grp", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(Integer id, String grp, BigDecimal amt) {
        DataRow r = new DataRow(3);
        r.set(0, id);
        r.set(1, grp);
        r.set(2, amt);
        return r;
    }

    /** Test-only collector wrapping a Consumer of DataRow. */
    private static List<DataRow> run(PlanGraph g) {
        List<DataRow> out = new ArrayList<>();
        new PushDagExecutor().execute(g, out::add);
        return out;
    }

    /** Linear source -> filter -> select end to end. */
    @Test
    void linearFilterSelect() {
        List<DataRow> rows = Arrays.asList(
                row(10, "a", new BigDecimal("100")),
                row(1, "b", new BigDecimal("50")),
                row(8, "a", new BigDecimal("80")));
        PlanNode src = PlanNode.source(META, rows);
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "id > 5").build();
        // select id + 1 (computed) -- the meta comes from Select.of
        io.rowweave.ops.Select sel = io.rowweave.ops.Select.of(META, "id + 1 as idp");
        PlanNode select = PlanNode.unary("select", sel.getOutputSchema(), 1)
                .listParam("exprs", Arrays.asList("id + 1 as idp")).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, filter, select));

        List<DataRow> out = run(g);
        assertEquals(2, out.size()); // 10>5, 8>5 (1 dropped)
        assertEquals(new BigDecimal("11"), out.get(0).get(0));
        assertEquals(new BigDecimal("9"), out.get(1).get(0));
    }

    /** Breaker groupBySum: the sum is rounded to scale 10 and the meta is right. */
    @Test
    void breakerGroupBySum() {
        List<DataRow> rows = Arrays.asList(
                row(1, "a", new BigDecimal("10.5")),
                row(2, "b", new BigDecimal("20")),
                row(3, "a", new BigDecimal("4.5")));
        io.rowweave.ops.ParallelGroupByAgg agg =
                io.rowweave.ops.ParallelGroupByAgg.of(META, "grp").sum("amt");
        PlanNode src = PlanNode.source(META, rows);
        PlanNode gb = PlanNode.unary("groupBySum", agg.getOutputSchema(), 0)
                .listParam("keys", Arrays.asList("grp"))
                .param("measure", "amt").build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, gb));

        List<DataRow> out = run(g);
        assertEquals(2, out.size());
        // find the grp=a row: sum=15.0000000000 (scale 10)
        BigDecimal sumA = null, sumB = null;
        for (DataRow r : out) {
            if ("a".equals(r.get(0))) sumA = (BigDecimal) r.get(1);
            if ("b".equals(r.get(0))) sumB = (BigDecimal) r.get(1);
        }
        assertEquals(0, new BigDecimal("15.0000000000").compareTo(sumA));
        assertEquals(0, new BigDecimal("20.0000000000").compareTo(sumB));
    }

    @Test
    void reportedLinearFailureFreezesOwnedMemoryExactlyOnceBeforeClose() {
        long poolBytes = 1L << 20;
        RuntimeException terminalFailure = new RuntimeException("terminal failed");
        io.rowweave.ops.ParallelGroupByAgg aggregate =
                io.rowweave.ops.ParallelGroupByAgg.of(META, "grp").count();
        PlanGraph fragment = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.unary("groupByCount", aggregate.getOutputSchema(), 0)
                        .listParam("keys", Collections.singletonList("grp"))
                        .build()));
        Map<Integer, RowStream> bindings = Collections.singletonMap(0,
                RowStream.fromIterable(Arrays.asList(
                        row(1, "a", BigDecimal.ONE),
                        row(2, "b", BigDecimal.TEN))));
        PipelineExecutor.Stage failingTerminal = (in, ignored) -> {
            while (in.next() != null) { }
            throw terminalFailure;
        };
        StageExecutionTelemetryAccumulator telemetry =
                new StageExecutionTelemetryAccumulator();

        RuntimeException observed = assertThrows(RuntimeException.class,
                () -> new PushDagExecutor(1,
                        io.rowweave.mem.ManagedMemory.ofBytes(poolBytes))
                        .executeLinearBoundReported(
                                fragment, bindings, failingTerminal, telemetry));

        assertSame(terminalFailure, observed.getCause());
        StageExecutionTelemetry snapshot = telemetry.freeze();
        assertEquals(TelemetryCoverage.COMPLETE, snapshot.coverage());
        assertEquals(poolBytes, snapshot.managedAcquiredBytes(),
                "one operator owner must be merged exactly once");
        assertTrue(snapshot.managedPeakReservedBytes() > 0);
    }

    /** orderBy, top, distinct and wholeAgg are rebuilt and executed. */
    @Test
    void orderByTypeId() {
        List<DataRow> rows = Arrays.asList(
                row(3, "a", null), row(1, "b", null), row(2, "c", null));
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        PlanNode src = PlanNode.source(META, rows);
        PlanNode ob = PlanNode.unary("orderBy", META, 0)
                .listParam("sort", Collections.singletonList(PlanOps.encodeSortField(sf))).build();
        List<DataRow> out = run(new PlanGraph(Arrays.asList(src, ob)));
        assertEquals(Arrays.asList(1, 2, 3),
                Arrays.asList(out.get(0).get(0), out.get(1).get(0), out.get(2).get(0)));
    }

    @Test
    void distinctTypeId() {
        List<DataRow> rows = Arrays.asList(
                row(1, "a", null), row(1, "a", null), row(2, "b", null));
        PlanNode src = PlanNode.source(META, rows);
        PlanNode d = PlanNode.unary("distinct", META, 0)
                .listParam("keys", Collections.emptyList()).build();
        List<DataRow> out = run(new PlanGraph(Arrays.asList(src, d)));
        assertEquals(2, out.size());
    }

    @Test
    void wholeAggTypeId() {
        List<DataRow> rows = Arrays.asList(
                row(1, "a", new BigDecimal("10")), row(2, "b", new BigDecimal("20")));
        DataSchema outMeta = new DataSchema(new Field("sum0", DataType.BigDecimalType));
        PlanNode src = PlanNode.source(META, rows);
        PlanNode wa = PlanNode.unary("wholeAgg", outMeta, 0)
                .param("kind", "SUM").param("field", "amt").build();
        List<DataRow> out = run(new PlanGraph(Arrays.asList(src, wa)));
        assertEquals(1, out.size());
        assertEquals(0, new BigDecimal("30.0000000000").compareTo((BigDecimal) out.get(0).get(0)));
    }

    /** BINARY join with INNER, feeding a filter downstream. */
    @Test
    void binaryJoinInner() {
        DataSchema leftMeta = new DataSchema(
                new Field("k", DataType.IntegerType), new Field("lv", DataType.StringType));
        DataSchema rightMeta = new DataSchema(
                new Field("k", DataType.IntegerType), new Field("rv", DataType.StringType));
        List<DataRow> left = Arrays.asList(jrow(1, "L1"), jrow(2, "L2"));
        List<DataRow> right = Arrays.asList(jrow(1, "R1"), jrow(3, "R3"));

        io.rowweave.ops.join.SortMergeJoin smj = io.rowweave.ops.join.SortMergeJoin.of(
                leftMeta, rightMeta, new String[]{"k"}, new String[]{"k"},
                io.rowweave.ops.join.JoinType.INNER);
        PlanNode ls = PlanNode.source(leftMeta, left);
        PlanNode rs = PlanNode.source(rightMeta, right);
        PlanNode join = PlanNode.binary("join", smj.getOutputSchema(), 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
        PlanGraph g = new PlanGraph(Arrays.asList(ls, rs, join));

        List<DataRow> out = run(g);
        assertEquals(1, out.size()); // only k=1 matches
        assertEquals(1, out.get(0).get(0));
    }

    /** BINARY union concatenates and keeps the order. */
    @Test
    void binaryUnion() {
        List<DataRow> a = Arrays.asList(row(1, "a", null), row(2, "a", null));
        List<DataRow> b = Arrays.asList(row(3, "b", null));
        PlanNode ls = PlanNode.source(META, a);
        PlanNode rs = PlanNode.source(META, b);
        PlanNode u = PlanNode.binary("union", META, 0, 1).build();
        List<DataRow> out = run(new PlanGraph(Arrays.asList(ls, rs, u)));
        assertEquals(Arrays.asList(1, 2, 3),
                Arrays.asList(out.get(0).get(0), out.get(1).get(0), out.get(2).get(0)));
    }

    /** An unsupported typeId (map) throws. */
    @Test
    void unsupportedTypeIdThrows() {
        // An unknown typeId (no describable rebuild and no userFunction) makes PlanOps.stageFor throw IllegalArgumentException.
        // Note: map, flatMap and filterFn are supported code-lambda typeIds now (they carry a userFunction), so they are no longer unsupported.
        PlanNode src = PlanNode.source(META, Arrays.asList(row(1, "a", null)));
        PlanNode unknown = PlanNode.unary("bogusUnknownOp", META, 0).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, unknown));
        assertThrows(IllegalArgumentException.class, () -> run(g));
    }

    /** Execution after a serialization round-trip gives the same result, which requires the SOURCE Source to be Serializable. */
    @Test
    void serializationRoundTripThenExecute() {
        List<DataRow> rows = Arrays.asList(
                row(10, "a", new BigDecimal("1.2300000000")), row(2, "b", null));
        PlanNode src = PlanNode.source(META, rows);
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "id > 5").build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, filter));

        PlanGraph back = PlanGraph.fromBytes(g.toBytes());
        List<DataRow> out = run(back);
        assertEquals(1, out.size());
        assertEquals(10, out.get(0).get(0));
        assertEquals(new BigDecimal("1.2300000000"), out.get(0).get(2));
    }

    /** Empty input through a linear graph and a binary graph: the sink gets no rows and the run closes normally. */
    @Test
    void emptyInput() {
        PlanNode src = PlanNode.source(META, new ArrayList<>());
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "id > 5").build();
        assertEquals(0, run(new PlanGraph(Arrays.asList(src, filter))).size());

        PlanNode ls = PlanNode.source(META, new ArrayList<>());
        PlanNode rs = PlanNode.source(META, new ArrayList<>());
        PlanNode u = PlanNode.binary("union", META, 0, 1).build();
        assertEquals(0, run(new PlanGraph(Arrays.asList(ls, rs, u))).size());
    }

    /** An unsupported binary typeId (coGroup) throws. */
    @Test
    void unsupportedBinaryTypeIdThrows() {
        PlanNode ls = PlanNode.source(META, new ArrayList<>());
        PlanNode rs = PlanNode.source(META, new ArrayList<>());
        PlanNode cg = PlanNode.binary("coGroup", META, 0, 1).build();
        PlanGraph g = new PlanGraph(Arrays.asList(ls, rs, cg));
        assertThrows(IllegalArgumentException.class, () -> run(g));
    }

    /** An upstream source failing mid-iteration makes execution fail fast and propagate the error, never swallow it silently. */
    @Test
    void upstreamErrorPropagates() {
        io.rowweave.Source boom = new io.rowweave.Source() {
            @Override
            public DataSchema getSchema() {
                return META;
            }

            @Override
            public java.util.Iterator<DataRow> iterator() {
                return new java.util.Iterator<DataRow>() {
                    private int n = 0;

                    @Override
                    public boolean hasNext() {
                        return n++ < 2;
                    }

                    @Override
                    public DataRow next() {
                        throw new RuntimeException("source boom");
                    }
                };
            }
        };
        PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(boom));
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "id > 0").build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, filter));
        assertThrows(RuntimeException.class, () -> run(g));
    }

    /** A graph rooted at a SINK (cluster staging) raises an explicit UnsupportedOperationException, rather than being misread as binary. */
    @Test
    void sinkRootedGraphRejected() {
        PlanNode src = PlanNode.source(META, new ArrayList<>());
        PlanNode sink = PlanNode.sink(META, 0, new io.rowweave.CollectingSink(META)).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, sink));
        assertThrows(UnsupportedOperationException.class, () -> run(g));
    }

    private static DataRow jrow(Integer k, String v) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, v);
        return r;
    }

    /**
     * needsMemory has a single source of truth: it returns true for the breaker typeIds that draw on
     * the allowance and call newOperatorMemory inside stageFor, and false for pass-through operators,
     * wholeAgg (a scalar) and union (a concat). A new breaker typeId left out of needsMemory has to
     * be caught here or fail loudly, because a per-operator sub-allowance with a denominator that
     * misses it makes acquire decide on the wrong total.
     */
    @Test
    void needsMemoryClassifiesBreakersOnly() {
        for (String breaker : new String[]{
                "groupBySum", "groupByCount", "groupByMax", "groupByMin", "groupBy",
                "distinct", "orderBy", "top", "reduceGroup", "combineGroup", "combineReduceGroup",
                "join", "coGroup"}) {
            assertTrue(PlanOps.needsMemory(breaker), breaker + " is a breaker that draws from the budget");
        }
        for (String nonBreaker : new String[]{
                "filter", "filterFn", "select", "selectByIndex", "map", "flatMap",
                "wholeAgg", "union", "source", "sink"}) {
            assertFalse(PlanOps.needsMemory(nonBreaker), nonBreaker + " draws nothing from the budget (pass-through, bounded, scalar or concat)");
        }
    }

    @Test
    void memoryOwnerRequirementIsNodeLocalAndDeclaresJoinChildArity() {
        assertEquals(1, PlanOps.memoryOwnerRequirement("orderBy").childArity());
        assertEquals(1, PlanOps.memoryOwnerRequirement("groupBySum").childArity());
        assertEquals(2, PlanOps.memoryOwnerRequirement("join").childArity());
        assertEquals(2, PlanOps.memoryOwnerRequirement("coGroup").childArity());
        assertThrows(IllegalArgumentException.class,
                () -> PlanOps.memoryOwnerRequirement("filter"));
    }

    @Test
    void insufficientGuaranteesRejectBeforeSourceBodyOrPartialAdmission() {
        System.setProperty("rowweave.worker.operator.maxMemory", "80");
        System.setProperty("rowweave.worker.operator.minMemory", "60");
        try {
            AtomicInteger iteratorCalls = new AtomicInteger();
            io.rowweave.Source input = new io.rowweave.Source() {
                @Override public DataSchema getSchema() { return META; }
                @Override public java.util.Iterator<DataRow> iterator() {
                    iteratorCalls.incrementAndGet();
                    return Collections.emptyIterator();
                }
            };
            io.rowweave.ops.ParallelGroupByAgg aggregate =
                    io.rowweave.ops.ParallelGroupByAgg.of(META, "grp").sum("amt");
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.sourceFromInputs(META, Collections.singletonList(input)),
                    PlanNode.unary("groupBySum", aggregate.getOutputSchema(), 0)
                            .listParam("keys", Collections.singletonList("grp"))
                            .param("measure", "amt").build(),
                    PlanNode.unary("orderBy", aggregate.getOutputSchema(), 1)
                            .listParam("sort", Collections.singletonList(PlanOps.encodeSortField(
                                    new SortField(0, SortField.Direction.ASC,
                                            SortField.NullOrder.NULLS_LAST)))).build()));
            io.rowweave.mem.ManagedMemory pool = io.rowweave.mem.ManagedMemory.ofBytes(100);

            assertThrows(io.rowweave.mem.MemoryBudgetExceededException.class,
                    () -> new PushDagExecutor(1, pool).execute(graph, ignored -> { }));
            assertEquals(0, iteratorCalls.get(), "a resource refusal happens before the source body runs");
            assertEquals(100, pool.availableBytes(), "refusing the whole owner group leaves no partial guarantee behind");
            assertEquals(0, pool.snapshot().activeOwnerCount());
        } finally {
            System.clearProperty("rowweave.worker.operator.maxMemory");
            System.clearProperty("rowweave.worker.operator.minMemory");
        }
    }
}
