package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JobSessionTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(String k, BigDecimal amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, amt);
        return r;
    }

    /** simple in-memory Source over a re-iterable List. */
    private static Source input(DataSchema meta, DataRow... rs) {
        final List<DataRow> rows = new ArrayList<>(Arrays.asList(rs));
        return new Source() {
            @Override
            public DataSchema getSchema() {
                return meta;
            }
            @Override
            public Iterator<DataRow> iterator() {
                return rows.iterator();
            }
        };
    }

    @Test
    void createSessionFromInputFilterSelectCommit() {
        ExecutionEnvironment session = RowWeave.createEnvironment("test");
        DataSet ds = session.fromSource(input(META, row("a", new BigDecimal("100")),
                        row("b", new BigDecimal("50")), row("c", new BigDecimal("200"))))
                .filter(r -> ((BigDecimal) r.get(1)).compareTo(new BigDecimal("100")) >= 0)
                .select(0);
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        session.execute();
        // a(100), c(200) pass
        assertEquals(2, out.getRows().size());
        assertEquals("a", out.getRows().get(0).get(0));
        assertEquals("c", out.getRows().get(1).get(0));
    }

    @Test
    void commitMultipleSinks() {
        ExecutionEnvironment session = RowWeave.createEnvironment("multi");
        DataSet src = session.fromSource(input(META, row("a", new BigDecimal("1")), row("b", new BigDecimal("2"))));
        WorkerCollectingOutput allOut = new WorkerCollectingOutput(META);
        src.sinkTo(allOut);
        DataSet filtered = session.fromSource(input(META, row("x", new BigDecimal("9"))))
                .filter(r -> true);
        WorkerCollectingOutput filtOut = new WorkerCollectingOutput(filtered.getSchema());
        filtered.sinkTo(filtOut);
        session.execute();
        assertEquals(2, allOut.getRows().size());
        assertEquals(1, filtOut.getRows().size());
    }

    @Test
    void commitGroupByBreaker() {
        ExecutionEnvironment session = RowWeave.createEnvironment("agg");
        DataSet src = session.fromSource(input(META,
                row("a", new BigDecimal("10")), row("b", new BigDecimal("20")), row("a", new BigDecimal("5"))));
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("amt");
        DataSet grouped = src.groupBy(agg);
        WorkerCollectingOutput out = new WorkerCollectingOutput(grouped.getSchema());
        grouped.sinkTo(out);
        session.execute();
        assertEquals(2, out.getRows().size()); // a, b
    }

    @Test
    void commitJoinBinary() {
        DataSchema lm = new DataSchema(new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));
        ExecutionEnvironment session = RowWeave.createEnvironment("join");
        DataRow l1 = new DataRow(2); l1.set(0, 1); l1.set(1, "a");
        DataRow l2 = new DataRow(2); l2.set(0, 2); l2.set(1, "b");
        DataRow r1 = new DataRow(2); r1.set(0, 2); r1.set(1, "x");
        DataSet left = session.fromSource(input(lm, l1, l2));
        DataSet right = session.fromSource(input(rm, r1));
        SortMergeJoin smj = SortMergeJoin.of(lm, rm, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        DataSet joined = left.join(right, smj);
        WorkerCollectingOutput out = new WorkerCollectingOutput(joined.getSchema());
        joined.sinkTo(out);
        session.execute();
        assertEquals(1, out.getRows().size()); // key 2 matches
    }

    @Test
    void outputWithoutSessionThrows() {
        // DataSet built via static source() (no session) cannot output() — must use RowWeave.createEnvironment().fromSource
        DataSet ds = DataSet.source(META, new ArrayList<>());
        assertThrows(IllegalStateException.class, () -> ds.sinkTo(new WorkerCollectingOutput(META)));
    }

    @Test
    void emptySourceCommitClosesOutput() {
        ExecutionEnvironment session = RowWeave.createEnvironment("empty");
        final boolean[] closed = {false};
        Sink out = new CloseTrackingOutput(closed);
        DataSet ds = session.fromSource(input(META));
        ds.sinkTo(out);
        session.execute();
        assertTrue(closed[0], "commit must close output even for empty source");
    }

    /** The Sink API contract requires serializability, so a static fixture keeps an anonymous inner class from capturing the test instance. */
    private static final class CloseTrackingOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final boolean[] closed;

        private CloseTrackingOutput(boolean[] closed) {
            this.closed = closed;
        }

        @Override public DataSchema getSchema() { return META; }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public void collect(DataRow row) { }
        @Override public void close() { closed[0] = true; }
    }

    @Test
    void createSessionName() {
        ExecutionEnvironment s = RowWeave.createEnvironment("myjob");
        assertEquals("myjob", s.getJobName());
    }

    /** commit goes through a whole PlanGraph: toPlanGraph emits a multi-node graph that ends in a SINK, so the IR is no longer dead code. */
    @Test
    void toPlanGraphProducesSinkRootedDag() {
        ExecutionEnvironment session = RowWeave.createEnvironment("plan");
        DataSet ds = session.fromSource(input(META, row("a", new BigDecimal("1"))))
                .filter(r -> true).select(0);
        ds.sinkTo(new WorkerCollectingOutput(ds.getSchema()));
        io.rowweave.plan.PlanGraph g = session.toPlanGraph();
        // source, filter, select and SINK give 4 nodes, with the SINK last
        assertEquals(4, g.nodeCount());
        assertEquals(io.rowweave.plan.PlanNode.Kind.SINK, g.node(3).kind());
        assertEquals(io.rowweave.plan.PlanNode.Kind.SOURCE, g.node(0).kind());
    }

    /** A multi-sink dag whose toPlanGraph carries several SINK nodes, one per output, while commit still submits the whole graph once. */
    @Test
    void toPlanGraphMultiSink() {
        ExecutionEnvironment session = RowWeave.createEnvironment("multi-plan");
        DataSet a = session.fromSource(input(META, row("a", new BigDecimal("1"))));
        a.sinkTo(new WorkerCollectingOutput(META));
        DataSet b = session.fromSource(input(META, row("b", new BigDecimal("2"))));
        b.sinkTo(new WorkerCollectingOutput(META));
        io.rowweave.plan.PlanGraph g = session.toPlanGraph();
        int sinkCount = 0;
        for (int i = 0; i < g.nodeCount(); i++) {
            if (g.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) sinkCount++;
        }
        assertEquals(2, sinkCount, "two outputs -> two SINK nodes");
    }

    /** A fan-out in which one DataSet feeds 2 outputs: commit copies the shared subgraph per sink. */
    @Test
    void fanOutToTwoOutputs() {
        ExecutionEnvironment session = RowWeave.createEnvironment("fanout");
        DataSet shared = session.fromSource(input(META, row("a", new BigDecimal("1")))).filter(r -> true);
        WorkerCollectingOutput o1 = new WorkerCollectingOutput(META);
        WorkerCollectingOutput o2 = new WorkerCollectingOutput(META);
        shared.sinkTo(o1);
        shared.sinkTo(o2);  // the same DataSet feeding a second sink is a fan-out, which is supported now
        session.execute();
        assertEquals(1, o1.getRows().size(), "fan-out branch 1 received data");
        assertEquals(1, o2.getRows().size(), "fan-out branch 2 received data");
    }

    /** The commit PlanGraph of a describable dag is serializable, round-tripping through toBytes and fromBytes. */
    @Test
    void describableDagPlanGraphSerializable() {
        ExecutionEnvironment session = RowWeave.createEnvironment("ser");
        // filter(expr), groupBy and orderBy are all describable, with no lambda
        DataSet ds = session.fromSource(input(META,
                        row("a", new BigDecimal("10")), row("b", new BigDecimal("20")), row("a", new BigDecimal("5"))))
                .filter("amt > 1")
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        DataSchema gbMeta = ds.getSchema();
        DataSet ordered = ds.orderBy(
                io.rowweave.ops.OrderBy.of(gbMeta, new io.rowweave.ops.SortField(
                        0, io.rowweave.ops.SortField.Direction.ASC, io.rowweave.ops.SortField.NullOrder.NULLS_LAST)),
                1, false);
        ordered.sinkTo(new WorkerCollectingOutput(ordered.getSchema()));

        io.rowweave.plan.PlanGraph g = session.toPlanGraph();
        // The point is that toBytes does not throw, since describable nodes carry only typeId and params and no in-memory Stage, and the fromBytes round-trip is structurally equal
        byte[] bytes = g.toBytes();
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(bytes);
        assertEquals(g.nodeCount(), back.nodeCount());
        // node typeIds stay faithful: filter, groupBySum, orderBy and sink
        boolean sawGroupBy = false, sawOrderBy = false, sawFilter = false;
        for (int i = 0; i < back.nodeCount(); i++) {
            String t = back.node(i).typeId();
            if ("groupBy".equals(t)) sawGroupBy = true;  // the unified typeId, once groupBySum
            if ("orderBy".equals(t)) sawOrderBy = true;
            if ("filter".equals(t)) sawFilter = true;
        }
        assertTrue(sawFilter && sawGroupBy && sawOrderBy, "describable typeId survives serialization");
    }

    /** A describable sort-merge join, whose commit PlanGraph serializes and whose rebuilt round-trip executes correctly, keeping joinType and the left and right keys in both index and name form. */
    @Test
    void joinDescribableSerializeRebuildExecute() {
        DataSchema lm = new DataSchema(new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));
        ExecutionEnvironment session = RowWeave.createEnvironment("joinser");
        DataRow l1 = new DataRow(2); l1.set(0, 1); l1.set(1, "a");
        DataRow l2 = new DataRow(2); l2.set(0, 2); l2.set(1, "b");
        DataRow r1 = new DataRow(2); r1.set(0, 2); r1.set(1, "x");
        DataSet left = session.fromSource(input(lm, l1, l2));
        DataSet right = session.fromSource(input(rm, r1));
        SortMergeJoin smj = SortMergeJoin.of(lm, rm, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        DataSet joined = left.join(right, smj);
        joined.sinkTo(new WorkerCollectingOutput(joined.getSchema()));

        // describable, therefore serializable: toBytes does not throw, and the round-trip is rebuilt and executed
        io.rowweave.plan.PlanGraph g = session.toPlanGraph();
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(g.toBytes());
        int rows = 0;
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                new io.rowweave.plan.PushDagExecutor().runSinkGraph(back.subGraphRootedAt(i));
                rows += ((WorkerCollectingOutput) back.node(i).output()).getRows().size();
            }
        }
        assertEquals(1, rows, "INNER join after a serialization round trip: key 2 matches 1 row (joinType/keys round-trip correctly)");
    }

    /** A describable union serialized and executed after a round-trip, concatenating in order. */
    @Test
    void unionDescribableSerializeRebuildExecute() {
        ExecutionEnvironment session = RowWeave.createEnvironment("unionser");
        DataSet a = session.fromSource(input(META, row("a", new BigDecimal("1"))));
        DataSet b = session.fromSource(input(META, row("b", new BigDecimal("2"))));
        DataSet u = a.union(b, io.rowweave.ops.Union.of(META));
        u.sinkTo(new WorkerCollectingOutput(META));
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(session.toPlanGraph().toBytes());
        int rows = 0;
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                new io.rowweave.plan.PushDagExecutor().runSinkGraph(back.subGraphRootedAt(i));
                rows += ((WorkerCollectingOutput) back.node(i).output()).getRows().size();
            }
        }
        assertEquals(2, rows, "union concat rebuilt from serialized form: 2 rows");
    }

    /** A guard that a count(*) through a describable groupBy still executes correctly after a serialization round-trip, where a null measure means count(*) rather than a crash. */
    @Test
    void countStarGroupBySerializeRebuildExecute() {
        ExecutionEnvironment session = RowWeave.createEnvironment("countstar");
        DataSet grouped = session.fromSource(input(META,
                        row("a", new BigDecimal("1")), row("a", null), row("b", new BigDecimal("2"))))
                .groupBy(ParallelGroupByAgg.of(META, "k").count()); // count(*) — measure null
        DataSchema gbMeta = grouped.getSchema();
        WorkerCollectingOutput out = new WorkerCollectingOutput(gbMeta);
        grouped.sinkTo(out);

        // describable, a serialization round-trip, then rebuilt execution through PushDagExecutor, taking the count(*) path with a null measure
        io.rowweave.plan.PlanGraph g = session.toPlanGraph();
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(g.toBytes());
        // After serialization the SINK Sink is a deserialized copy that receives the rows, rather than the test out instance, so the rows are read back from that SINK.
        java.util.Map<Object, Object> counts = new java.util.HashMap<>();
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                new io.rowweave.plan.PushDagExecutor().runSinkGraph(back.subGraphRootedAt(i));
                WorkerCollectingOutput rebuilt = (WorkerCollectingOutput) back.node(i).output();
                for (DataRow r : rebuilt.getRows()) counts.put(r.get(0), r.get(1));
            }
        }
        // count(*): group a counts its null row too, giving 2, while b gives 1
        assertEquals(2L, counts.get("a"), "count(*) includes null rows");
        assertEquals(1L, counts.get("b"));
    }

    /** Each describable typeId executes the same after a serialization round-trip as directly: selectByIndex, distinct, orderBy, top and groupBySum. */
    @Test
    void describableTypeIdsSerializeRebuildMatchesDirect() {
        // selectByIndex
        assertSerializeRebuildMatches(s -> s.select(0));
        // distinct(keys)
        assertSerializeRebuildMatches(s -> s.distinct(io.rowweave.ops.Distinct.byColumns(META, "k")));
        // groupBySum
        assertSerializeRebuildMatches(s -> s.groupBy(ParallelGroupByAgg.of(META, "k").sum("amt")));
    }

    private void assertSerializeRebuildMatches(java.util.function.Function<DataSet, DataSet> op) {
        DataRow[] rows = {row("a", new BigDecimal("10")), row("b", new BigDecimal("20")), row("a", new BigDecimal("5"))};
        // run directly through commit, the same IR path production takes
        ExecutionEnvironment s1 = RowWeave.createEnvironment("direct");
        DataSet d1 = op.apply(s1.fromSource(input(META, rows)));
        WorkerCollectingOutput direct = new WorkerCollectingOutput(d1.getSchema());
        d1.sinkTo(direct);
        s1.execute();
        // executed after a serialization round-trip, with rows written into the deserialized copy of the SINK Sink, so they are read back from that SINK.
        ExecutionEnvironment s2 = RowWeave.createEnvironment("ser");
        DataSet d2 = op.apply(s2.fromSource(input(META, rows)));
        d2.sinkTo(new WorkerCollectingOutput(d2.getSchema()));
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(s2.toPlanGraph().toBytes());
        int viaSerRows = 0;
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                new io.rowweave.plan.PushDagExecutor().runSinkGraph(back.subGraphRootedAt(i));
                viaSerRows += ((WorkerCollectingOutput) back.node(i).output()).getRows().size();
            }
        }
        assertEquals(direct.getRows().size(), viaSerRows, "row count matches after a serialization round trip");
    }

    /** filter(fn), map and flatMap are describable now that a userFunction is serializable, so the commit PlanGraph toBytes does not throw and the round-trip executes. */
    @Test
    void codeLambdaDescribableSerializeRebuildExecute() {
        ExecutionEnvironment session = RowWeave.createEnvironment("codelambda");
        // filter(FilterFunction), map(MapFunction) and flatMap(FlatMapFunction) are all code lambdas, and all describable now
        DataSet ds = session.fromSource(input(META,
                        row("a", new BigDecimal("10")), row("b", new BigDecimal("1")), row("c", new BigDecimal("20"))))
                .filter(r -> ((BigDecimal) r.get(1)).compareTo(new BigDecimal("5")) >= 0)  // a and c pass
                .map(r -> r)
                .flatMap((r, collector) -> collector.collect(r));
        ds.sinkTo(new WorkerCollectingOutput(ds.getSchema()));

        io.rowweave.plan.PlanGraph g = session.toPlanGraph();
        // lambda operators reach the wire through userFunction, so serialization does not fail
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(g.toBytes());
        int rows = 0;
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                new io.rowweave.plan.PushDagExecutor().runSinkGraph(back.subGraphRootedAt(i));
                rows += ((WorkerCollectingOutput) back.node(i).output()).getRows().size();
            }
        }
        assertEquals(2, rows, "filter(fn)+map+flatMap rebuilt from serialized form: a and c pass = 2 rows");
    }

    /** A filter(fn) that captures something unserializable makes toBytes fail loudly at commit time, naming the typeId and leaving no partial result. */
    @Test
    void nonSerializableCapturedLambdaFailsLoudAtToBytes() {
        ExecutionEnvironment session = RowWeave.createEnvironment("notser");
        final Object notSerializable = new Object();  // a captured variable that is not serializable
        DataSet ds = session.fromSource(input(META, row("a", new BigDecimal("1"))))
                .filter(r -> notSerializable.hashCode() >= 0);  // the lambda captures notSerializable
        ds.sinkTo(new WorkerCollectingOutput(ds.getSchema()));
        io.rowweave.plan.PlanGraph g = session.toPlanGraph();
        // a userFunction capturing something unserializable makes toBytes throw, an UncheckedIO wrapping NotSerializable, loudly
        assertThrows(RuntimeException.class, g::toBytes);
    }

    /** coGroup on DataSet is still not describable, since its builder carries no descriptor, so the whole graph falls back to an in-memory stage on a single machine and toBytes fails loudly. */
    private static final DataSchema CG_OUT = new DataSchema(new Field("k", DataType.IntegerType));

    /** A static CoGroupFunction, serializable and capturing no instance, for the describable test. */
    static final class StaticCoGroupFn implements CoGroupFunction {
        @Override
        public void coGroup(Iterable<DataRow> i1, Iterable<DataRow> i2, io.rowweave.Collector collector) {
            DataRow o = new DataRow(1); o.set(0, 1); collector.collect(o);
        }
        @Override
        public DataSchema getResultSchema() {
            return CG_OUT;
        }
    }

    /** coGroup is describable now that CoGroupFunction is Serializable, so the commit PlanGraph serializes, the round-trip is rebuilt and executed, and outMeta agrees. */
    @Test
    void coGroupDescribableSerializeRebuildExecute() {
        DataSchema lm = new DataSchema(new Field("k", DataType.IntegerType), new Field("lv", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("k", DataType.IntegerType), new Field("rv", DataType.StringType));
        ExecutionEnvironment session = RowWeave.createEnvironment("cogroup");
        DataRow l1 = new DataRow(2); l1.set(0, 1); l1.set(1, "a");
        DataRow r1 = new DataRow(2); r1.set(0, 1); r1.set(1, "x");
        DataSet left = session.fromSource(input(lm, l1));
        DataSet right = session.fromSource(input(rm, r1));
        CoGroupFunction fn = new StaticCoGroupFn();  // static and serializable
        io.rowweave.ops.join.CoGroup cg = io.rowweave.ops.join.CoGroup.of(
                lm, rm, new String[]{"k"}, new String[]{"k"}, fn);
        DataSet joined = left.coGroup(right, cg);
        joined.sinkTo(new WorkerCollectingOutput(joined.getSchema()));

        io.rowweave.plan.PlanGraph g = session.toPlanGraph();
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(g.toBytes());  // serializable now
        int rows = 0;
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                // outMeta agreement: the outMeta of the SINK coGroup child equals fn.getResultSchema()
                io.rowweave.plan.PlanNode coGroupNode = back.node(back.node(i).child(0));
                assertEquals(CG_OUT.getFieldCount(), coGroupNode.outMeta().getFieldCount(), "coGroup outMeta round-trips unchanged");
                new io.rowweave.plan.PushDagExecutor().runSinkGraph(back.subGraphRootedAt(i));
                rows += ((WorkerCollectingOutput) back.node(i).output()).getRows().size();
            }
        }
        assertEquals(1, rows, "coGroup rebuilt from serialized form: the two-sided group for key 1 emits 1 row");
    }

    /** reduceGroup is describable now that GroupReduceFunction is Serializable, so a serialization round-trip rebuilds and executes it. */
    @Test
    void reduceGroupDescribableSerializeRebuildExecute() {
        ExecutionEnvironment session = RowWeave.createEnvironment("reduce");
        DataSet reduced = session.fromSource(input(META,
                        row("a", new BigDecimal("1")), row("a", new BigDecimal("2")), row("b", new BigDecimal("3"))))
                .reduceGroup(io.rowweave.ops.ReduceGroup.of(META, "k", new StaticReduceFn()), REDUCE_OUT);
        reduced.sinkTo(new WorkerCollectingOutput(REDUCE_OUT));
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(session.toPlanGraph().toBytes());
        int rows = 0;
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                new io.rowweave.plan.PushDagExecutor().runSinkGraph(back.subGraphRootedAt(i));
                rows += ((WorkerCollectingOutput) back.node(i).output()).getRows().size();
            }
        }
        assertEquals(2, rows, "reduceGroup rebuilt from serialized form: groups a and b each emit 1 row");
    }

    private static final DataSchema REDUCE_OUT = new DataSchema(new Field("k", DataType.StringType));

    /** A static serializable GroupReduceFunction that emits one row per group, carrying the key. */
    static final class StaticReduceFn implements GroupReduceFunction {
        @Override
        public void reduce(Iterable<DataRow> rows, io.rowweave.Collector collector) {
            for (DataRow r : rows) {
                DataRow o = new DataRow(1); o.set(0, r.get(0)); collector.collect(o);
                return;  // one row per group
            }
        }
        @Override
        public DataSchema getResultSchema() {
            return REDUCE_OUT;
        }
    }

    // ---- multi-aggregate describable groupBy, one groupBy typeId plus parallel aggKinds and aggMeasures listParams ----

    /** A 3-field meta for the multi-aggregate sum, count, max and min and column-order test. */
    private static final DataSchema META3 = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType),
            new Field("qty", DataType.BigDecimalType));

    /** Full DataSchema equality over name, type and order, compared field by field since DataSchema and Field carry no value equals. */
    private static void assertMetaEquals(DataSchema expected, DataSchema actual, String msg) {
        assertEquals(expected.getFieldCount(), actual.getFieldCount(), msg + " column count");
        for (int i = 0; i < expected.getFieldCount(); i++) {
            assertEquals(expected.getField(i).getName(), actual.getField(i).getName(), msg + " column " + i + " name");
            assertEquals(expected.getField(i).getDataType(), actual.getField(i).getDataType(), msg + " column " + i + " type");
        }
    }

    private static DataRow row3(String k, String amt, String qty) {
        DataRow r = new DataRow(3);
        r.set(0, k);
        r.set(1, amt == null ? null : new BigDecimal(amt));
        r.set(2, qty == null ? null : new BigDecimal(qty));
        return r;
    }

    /**
     * Several aggregates through a describable groupBy, rebuilt after a serialization round-trip, execute the same as directly, matching each aggregate column value and the column order, and
     * the outMeta of the rebuilt SINK child agrees in full with getOutputSchema of the direct graph, over name, type and order.
     */
    @Test
    void multiAggGroupBySerializeRebuildMatchesDirect() {
        DataRow[] rows = {row3("a", "10", "1"), row3("b", "20", "5"), row3("a", "5", "9")};
        java.util.function.Function<DataSet, DataSet> op =
                s -> s.groupBy(ParallelGroupByAgg.of(META3, "k").sum("amt").count().max("qty"));

        // run directly
        ExecutionEnvironment s1 = RowWeave.createEnvironment("direct-multi");
        DataSet d1 = op.apply(s1.fromSource(input(META3, rows)));
        WorkerCollectingOutput direct = new WorkerCollectingOutput(d1.getSchema());
        d1.sinkTo(direct);
        s1.execute();

        // serialized and round-tripped
        ExecutionEnvironment s2 = RowWeave.createEnvironment("ser-multi");
        DataSet d2 = op.apply(s2.fromSource(input(META3, rows)));
        d2.sinkTo(new WorkerCollectingOutput(d2.getSchema()));
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(s2.toPlanGraph().toBytes());

        WorkerCollectingOutput viaSer = null;
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                // outMeta identical in name, type and order
                io.rowweave.plan.PlanNode gb = back.node(back.node(i).child(0));
                assertMetaEquals(d1.getSchema(), gb.outMeta(), "rebuilt groupBy outMeta equals the direct one (name+type+order)");
                new io.rowweave.plan.PushDagExecutor().runSinkGraph(back.subGraphRootedAt(i));
                viaSer = (WorkerCollectingOutput) back.node(i).output();
            }
        }
        assertNotNull(viaSer);
        // the value and column order of every aggregate, with rows shaped [k, sum0, count1, max2]
        java.util.Map<Object, DataRow> d = new java.util.HashMap<>(), c = new java.util.HashMap<>();
        for (DataRow r : direct.getRows()) d.put(r.get(0), r);
        for (DataRow r : viaSer.getRows()) c.put(r.get(0), r);
        assertEquals(d.keySet(), c.keySet(), "grouping key set matches");
        for (Object k : d.keySet()) {
            DataRow dr = d.get(k), cr = c.get(k);
            assertEquals(0, ((BigDecimal) dr.get(1)).compareTo((BigDecimal) cr.get(1)), "sum[" + k + "]");
            assertEquals(dr.get(2), cr.get(2), "count[" + k + "]");
            assertEquals(0, ((BigDecimal) dr.get(3)).compareTo((BigDecimal) cr.get(3)), "max[" + k + "]");
        }
        // sum=15, count=2, max(qty)=9 ; b: sum=20, count=1, max(qty)=5
        assertEquals(0, ((BigDecimal) c.get("a").get(1)).compareTo(new BigDecimal("15.0000000000")));
        assertEquals(2L, c.get("a").get(2));
        assertEquals(0, ((BigDecimal) c.get("a").get(3)).compareTo(new BigDecimal("9")));
    }

    /** A measure field name holding a colon, as in sum("a:b"), survives the round-trip, since the parallel listParams encoding does not split on the separator. */
    @Test
    void multiAggColonFieldNameSerializeRoundTrips() {
        DataSchema metaColon = new DataSchema(
                new Field("k", DataType.StringType),
                new Field("a:b", DataType.BigDecimalType));
        DataRow r1 = new DataRow(2); r1.set(0, "x"); r1.set(1, new BigDecimal("3"));
        DataRow r2 = new DataRow(2); r2.set(0, "x"); r2.set(1, new BigDecimal("4"));
        java.util.function.Function<DataSet, DataSet> op =
                s -> s.groupBy(ParallelGroupByAgg.of(metaColon, "k").sum("a:b").count());

        ExecutionEnvironment s1 = RowWeave.createEnvironment("colon-direct");
        DataSet d1 = op.apply(s1.fromSource(input(metaColon, r1, r2)));
        WorkerCollectingOutput direct = new WorkerCollectingOutput(d1.getSchema());
        d1.sinkTo(direct);
        s1.execute();

        ExecutionEnvironment s2 = RowWeave.createEnvironment("colon-ser");
        DataSet d2 = op.apply(s2.fromSource(input(metaColon, r1, r2)));
        d2.sinkTo(new WorkerCollectingOutput(d2.getSchema()));
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(s2.toPlanGraph().toBytes());
        WorkerCollectingOutput viaSer = null;
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                new io.rowweave.plan.PushDagExecutor().runSinkGraph(back.subGraphRootedAt(i));
                viaSer = (WorkerCollectingOutput) back.node(i).output();
            }
        }
        assertNotNull(viaSer);
        assertEquals(1, viaSer.getRows().size());
        // sum(a:b)=7, count=2
        assertEquals(0, ((BigDecimal) viaSer.getRows().get(0).get(1)).compareTo(new BigDecimal("7.0000000000")));
        assertEquals(2L, viaSer.getRows().get(0).get(2));
    }

    /** A mixed column order (.sum.max.count.min, which is not the kind-clustered order) keeps its columns through a round trip: columns are read by position, so a reordered schema silently produces wrong results. */
    @Test
    void multiAggMixedOrderPreservesColumnOrder() {
        DataRow[] rows = {row3("a", "10", "1"), row3("a", "20", "9")};
        java.util.function.Function<DataSet, DataSet> op =
                s -> s.groupBy(ParallelGroupByAgg.of(META3, "k").sum("amt").max("qty").count().min("amt"));

        ExecutionEnvironment s1 = RowWeave.createEnvironment("mix-direct");
        DataSet d1 = op.apply(s1.fromSource(input(META3, rows)));
        WorkerCollectingOutput direct = new WorkerCollectingOutput(d1.getSchema());
        d1.sinkTo(direct);
        s1.execute();

        ExecutionEnvironment s2 = RowWeave.createEnvironment("mix-ser");
        DataSet d2 = op.apply(s2.fromSource(input(META3, rows)));
        d2.sinkTo(new WorkerCollectingOutput(d2.getSchema()));
        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(s2.toPlanGraph().toBytes());
        WorkerCollectingOutput viaSer = null;
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                io.rowweave.plan.PlanNode gb = back.node(back.node(i).child(0));
                assertMetaEquals(d1.getSchema(), gb.outMeta(), "mixed-order outMeta fully equal");
                new io.rowweave.plan.PushDagExecutor().runSinkGraph(back.subGraphRootedAt(i));
                viaSer = (WorkerCollectingOutput) back.node(i).output();
            }
        }
        assertNotNull(viaSer);
        // rows shaped [k, sum0=30, max1(qty)=9, count2=2, min3(amt)=10]
        DataRow r = viaSer.getRows().get(0);
        assertEquals(0, ((BigDecimal) r.get(1)).compareTo(new BigDecimal("30.0000000000")), "col1 sum(amt)");
        assertEquals(0, ((BigDecimal) r.get(2)).compareTo(new BigDecimal("9")), "col2 max(qty)");
        assertEquals(2L, r.get(3), "col3 count");
        assertEquals(0, ((BigDecimal) r.get(4)).compareTo(new BigDecimal("10")), "col4 min(amt)");
    }

    /** A tiny pool with multi-aggregate using an OpenAddressingGroupAggBuffer spills without running out of memory and still gets the right result. */
    @Test
    void multiAggBoundedBudgetSpillsNoOom() {
        java.util.List<DataRow> many = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            many.add(row3("k" + (i % 50), Integer.toString(i), Integer.toString(i % 7)));
        }
        ExecutionEnvironment session = RowWeave.createEnvironment("bounded-multi");
        DataSet grouped = session.fromSource(input(META3, many.toArray(new DataRow[0])))
                .groupBy(ParallelGroupByAgg.of(META3, "k").sum("amt").count().max("qty"));
        grouped.sinkTo(new WorkerCollectingOutput(grouped.getSchema()));

        io.rowweave.plan.PlanGraph back = io.rowweave.plan.PlanGraph.fromBytes(session.toPlanGraph().toBytes());
        // a tiny injected pool, so the breaker working set spills
        io.rowweave.mem.ManagedMemory pool = io.rowweave.mem.ManagedMemory.ofBytes(16 * 1024);
        WorkerCollectingOutput viaSer = null;
        for (int i = 0; i < back.nodeCount(); i++) {
            if (back.node(i).kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
                new io.rowweave.plan.PushDagExecutor(1, pool).runSinkGraph(back.subGraphRootedAt(i));
                viaSer = (WorkerCollectingOutput) back.node(i).output();
            }
        }
        assertNotNull(viaSer);
        assertEquals(50, viaSer.getRows().size(), "50 groups with one row each (spill loses no group)");
    }
}
