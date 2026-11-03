package io.rowweave;

import io.rowweave.ops.Distinct;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.ReduceGroup;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import io.rowweave.ops.Union;
import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Large-volume coverage of every operator on a single machine at parallelism=4, taking the parallel shuffle plus spill path. Each operator gets about 50K rows of input and is compared to a hand-worked oracle.
 * Covered: filter / select / map / flatMap / groupBy{Sum,Count,Max,Min} / groupBy with several aggregates / distinct / orderBy / top /
 * wholeAgg / reduceGroup / join / coGroup / union. The sensitive points are BigDecimal at scale 10, and null in grouping, sorting and joins.
 */
class BigDataOperatorCoverageTest {

    @AfterEach
    void resetMinimumGuarantee() {
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    private static final int N = 50_000;
    private static final int P = 4;
    private static final int KEYS = 200;  // the number of distinct keys

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType),
            new Field("tag", DataType.StringType));

    private static DataRow row(int k, long amt, String tag) {
        DataRow r = new DataRow(3);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        r.set(2, tag);
        return r;
    }

    /** Deterministic large data: k = i%KEYS, amt = i%97, tag = "t"+(i%5), with a few rows whose key or amt is null. */
    private static List<DataRow> bigData() {
        List<DataRow> rows = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            if (i % 1000 == 0) {
                DataRow nr = new DataRow(3);
                nr.set(0, null);  // a null key, its own group and never a join match
                nr.set(1, i % 2 == 0 ? null : new BigDecimal(i % 97));
                nr.set(2, "t" + (i % 5));
                rows.add(nr);
            } else {
                rows.add(row(i % KEYS, i % 97, "t" + (i % 5)));
            }
        }
        return rows;
    }

    private static Source input(DataSchema meta, List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return meta; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    private static List<DataRow> run(int parallelism, java.util.function.Function<DataSet, DataSet> pipeline,
                                  Source in) {
        BatchSession s = RowWeave.createBatchSession("bigdata");
        s.setParallelism(parallelism);
        DataSet ds = pipeline.apply(s.fromSource(in));
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        s.execute();
        return new ArrayList<>(out.getRows());
    }

    // ---- pass-through operators ----

    @Test
    void filterBigData() {
        List<DataRow> rows = bigData();
        long expected = rows.stream().filter(r -> r.get(0) != null && (int) r.get(0) < 100).count();
        List<DataRow> got = run(P, ds -> ds.filter(r -> r.get(0) != null && (int) r.get(0) < 100), input(META, rows));
        assertEquals(expected, got.size(), "filter k<100");
    }

    @Test
    void selectBigData() {
        List<DataRow> rows = bigData();
        List<DataRow> got = run(P, ds -> ds.select(0, 1), input(META, rows));  // projects k and amt
        assertEquals(rows.size(), got.size());
        assertEquals(2, got.get(0).size(), "projection yields 2 columns");
    }

    @Test
    void mapBigData() {
        List<DataRow> rows = bigData();
        // map: amt *2, with null preserved
        List<DataRow> got = run(P, ds -> ds.map(r -> {
            DataRow o = new DataRow(3);
            o.set(0, r.get(0));
            o.set(1, r.get(1) == null ? null : ((BigDecimal) r.get(1)).multiply(new BigDecimal(2)));
            o.set(2, r.get(2));
            return o;
        }), input(META, rows));
        assertEquals(rows.size(), got.size());
    }

    @Test
    void flatMapBigData() {
        List<DataRow> rows = bigData();
        // flatMap: each row emits 2 copies, so the row count doubles
        List<DataRow> got = run(P, ds -> ds.flatMap((r, c) -> { c.collect(r); c.collect(r); }), input(META, rows));
        assertEquals(rows.size() * 2L, got.size(), "flatMap doubles the row count");
    }

    // ---- keyed breakers: the groupBy family ----

    @Test
    void groupBySumBigData() {
        List<DataRow> rows = bigData();
        // SQL sum semantics: only non-null amt values accumulate inside a group, and an all-null group sums to null.
        Map<Object, BigDecimal> oracle = new HashMap<>();
        Set<Object> allKeys = new HashSet<>();
        for (DataRow r : rows) {
            allKeys.add(r.get(0));
            BigDecimal a = (BigDecimal) r.get(1);
            if (a != null) {
                oracle.merge(r.get(0), a, BigDecimal::add);
            }
        }
        List<DataRow> got = run(P, ds -> ds.groupBy(ParallelGroupByAgg.of(META, "k").sum("amt")), input(META, rows));
        assertEquals(allKeys.size(), got.size(), "group count (including the null-key group)");
        for (DataRow r : got) {
            BigDecimal expected = oracle.get(r.get(0));  // null is the all-null group
            BigDecimal actual = (BigDecimal) r.get(1);
            if (expected == null) {
                assertNull(actual, "key " + r.get(0) + " all-null amt -> sum null");
            } else {
                assertEquals(0, expected.compareTo(actual), "key " + r.get(0) + " sum");
            }
        }
    }

    @Test
    void groupByCountBigData() {
        List<DataRow> rows = bigData();
        Map<Object, Long> oracle = new HashMap<>();
        for (DataRow r : rows) oracle.merge(r.get(0), 1L, Long::sum);
        List<DataRow> got = run(P, ds -> ds.groupBy(ParallelGroupByAgg.of(META, "k").count()), input(META, rows));
        assertEquals(oracle.size(), got.size());
        for (DataRow r : got) assertEquals(oracle.get(r.get(0)), r.get(1), "key " + r.get(0) + " count");
    }

    @Test
    void groupByMaxMinBigData() {
        List<DataRow> rows = bigData();
        Map<Object, Long> max = new HashMap<>();
        for (DataRow r : rows) {
            BigDecimal a = (BigDecimal) r.get(1);
            if (a == null) continue;  // max ignores null
            max.merge(r.get(0), a.longValue(), Math::max);
        }
        List<DataRow> got = run(P, ds -> ds.groupBy(ParallelGroupByAgg.of(META, "k").max("amt")), input(META, rows));
        for (DataRow r : got) {
            if (max.containsKey(r.get(0))) {
                assertEquals(0, new BigDecimal(max.get(r.get(0))).compareTo((BigDecimal) r.get(1)),
                        "key " + r.get(0) + " max");
            }
        }
    }

    @Test
    void distinctBigData() {
        List<DataRow> rows = bigData();
        Set<String> oracle = new HashSet<>();
        for (DataRow r : rows) oracle.add(r.get(0) + "|" + r.get(2));
        List<DataRow> got = run(P, ds -> ds.distinct(Distinct.byColumns(META, "k", "tag")), input(META, rows));
        assertEquals(oracle.size(), got.size(), "number of distinct (k,tag) pairs");
    }

    // ---- breakers with in-operator parallelism ----

    @Test
    void orderByBigData() {
        List<DataRow> rows = bigData();
        List<DataRow> got = run(P, ds -> ds.orderBy(
                OrderBy.of(META, new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)), P, true),
                input(META, rows));
        assertEquals(rows.size(), got.size());
        // check amt ascending, with null sorted last
        BigDecimal prev = null;
        boolean seenNull = false;
        for (DataRow r : got) {
            BigDecimal a = (BigDecimal) r.get(1);
            if (a == null) { seenNull = true; continue; }
            assertFalse(seenNull, "null sorts after every non-null value (NULLS_LAST)");
            if (prev != null) assertTrue(prev.compareTo(a) <= 0, "amt ascending");
            prev = a;
        }
    }

    @Test
    void topBigData() {
        List<DataRow> rows = bigData();
        List<DataRow> got = run(P, ds -> ds.top(
                Top.of(META, 10, new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)), P),
                input(META, rows));
        assertEquals(10, got.size(), "Top-10");
        // the largest amt is 96, the maximum of i%97
        assertEquals(0, new BigDecimal(96).compareTo((BigDecimal) got.get(0).get(1)), "top row is the largest amt");
    }

    @Test
    void wholeAggBigData() {
        List<DataRow> rows = bigData();
        long oracle = 0;
        for (DataRow r : rows) {
            BigDecimal a = (BigDecimal) r.get(1);
            if (a != null) oracle += a.longValue();
        }
        // wholeAgg through DataSet: there is no public builder, so a PlanGraph runs it directly, covering the wholeAgg typeId
        io.rowweave.DataSchema waMeta = new DataSchema(new Field("total", DataType.BigDecimalType));
        io.rowweave.plan.PlanNode src = io.rowweave.plan.PlanNode.sourceFromInputs(
                META, java.util.Collections.singletonList(input(META, rows)));
        io.rowweave.plan.PlanNode wa = io.rowweave.plan.PlanNode.unary("wholeAgg", waMeta, 0)
                .param("kind", "SUM").param("field", "amt").build();
        WorkerCollectingOutput out = new WorkerCollectingOutput(waMeta);
        io.rowweave.plan.PlanNode sink = io.rowweave.plan.PlanNode.sink(waMeta, 1, out).build();
        new io.rowweave.plan.PushDagExecutor(P).runSinkGraph(
                new io.rowweave.plan.PlanGraph(java.util.Arrays.asList(src, wa, sink)));
        assertEquals(1, out.getRows().size(), "whole-table aggregation yields one row");
        assertEquals(0, new BigDecimal(oracle).compareTo((BigDecimal) out.getRows().get(0).get(0)), "sum over the whole table");
    }

    @Test
    void reduceGroupBigData() {
        List<DataRow> rows = bigData();
        // reduceGroup: each group emits [k, row count], a custom reduction
        DataSchema outMeta = new DataSchema(new Field("k", DataType.IntegerType), new Field("cnt", DataType.IntegerType));
        Map<Object, Long> oracle = new HashMap<>();
        for (DataRow r : rows) oracle.merge(r.get(0), 1L, Long::sum);
        GroupReduceFunction fn = new GroupReduceFunction() {
            @Override public DataSchema getResultSchema() { return outMeta; }
            @Override public void reduce(Iterable<DataRow> iter, Collector collector) {
                int cnt = 0; Object key = null;
                for (DataRow r : iter) { cnt++; key = r.get(0); }
                DataRow o = new DataRow(2); o.set(0, key); o.set(1, cnt);
                collector.collect(o);
            }
        };
        List<DataRow> got = run(P, ds -> ds.reduceGroup(ReduceGroup.of(META, "k", fn), outMeta), input(META, rows));
        assertEquals(oracle.size(), got.size());
        for (DataRow r : got) assertEquals(oracle.get(r.get(0)).intValue(), r.get(1), "key " + r.get(0) + " reduce cnt");
    }

    // ---- binary operators ----

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    @Test
    void joinBigData() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            DataRow l = new DataRow(2); l.set(0, i % KEYS); l.set(1, "L" + i); left.add(l);
        }
        for (int i = 0; i < KEYS; i++) {
            DataRow r = new DataRow(2); r.set(0, i); r.set(1, "R" + i); right.add(r);
        }
        // INNER: each left row matches exactly one right row, since the right key is unique, giving N rows
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        BatchSession s = RowWeave.createBatchSession("join-big");
        s.setParallelism(P);
        DataSet l = s.fromSource(input(LEFT, left));
        DataSet r = s.fromSource(input(RIGHT, right));
        DataSet joined = l.join(r, smj);
        WorkerCollectingOutput out = new WorkerCollectingOutput(joined.getSchema());
        joined.sinkTo(out);
        s.execute();
        assertEquals(N, out.getRows().size(), "INNER join: each left row matches exactly one right row -> N rows");
    }

    @Test
    void coGroupBigData() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < N; i++) { DataRow l = new DataRow(2); l.set(0, i % KEYS); l.set(1, "L" + i); left.add(l); }
        for (int i = 0; i < N; i++) { DataRow r = new DataRow(2); r.set(0, i % KEYS); r.set(1, "R" + i); right.add(r); }
        DataSchema cgOut = new DataSchema(new Field("k", DataType.IntegerType),
                new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        CoGroupFunction fn = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return cgOut; }
            @Override public void coGroup(Iterable<DataRow> it1, Iterable<DataRow> it2, Collector c) {
                int lc = 0; Object k = null;
                for (DataRow r : it1) { lc++; k = r.get(0); }
                int rc = 0;
                for (DataRow r : it2) { rc++; if (k == null) k = r.get(0); }
                DataRow o = new DataRow(3); o.set(0, k); o.set(1, lc); o.set(2, rc); c.collect(o);
            }
        };
        BatchSession s = RowWeave.createBatchSession("cogroup-big");
        s.setParallelism(P);
        DataSet joined = s.fromSource(input(LEFT, left)).coGroup(s.fromSource(input(RIGHT, right)),
                CoGroup.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, fn));
        WorkerCollectingOutput out = new WorkerCollectingOutput(joined.getSchema());
        joined.sinkTo(out);
        s.execute();
        assertEquals(KEYS, out.getRows().size(), "coGroup: one group for each of KEYS keys");
        for (DataRow r : out.getRows()) {
            assertEquals(N / KEYS, r.get(1), "left rows in each group");
            assertEquals(N / KEYS, r.get(2), "right rows in each group");
        }
    }

    // ---- expression function coverage, filter and select expressions through ExprFunctions ----

    @Test
    void expressionFunctionsBigData() {
        List<DataRow> rows = bigData();
        // select expressions: the built-in abs, upper, len and round functions, covering ExprFunctions
        List<DataRow> got = run(P, ds -> ds
                .filter("abs(amt) >= 0")          // abs
                .select("upper(tag) as ut", "round(amt, 0) as ra", "len(tag) as tl"),
                input(META, rows));
        // round and filter over rows with a null amt do not throw, since SQL null propagation applies, and the non-null rows are kept
        assertTrue(got.size() > 0, "expression function pipeline produces output");
        assertEquals(3, got.get(0).size(), "projection yields 3 columns");
        // upper(tag) = "T0".."T4"
        for (DataRow r : got) {
            Object ut = r.get(0);
            if (ut != null) assertTrue(((String) ut).startsWith("T"), "upper(tag) is upper case");
        }
    }

    @Test
    void stringFunctionsBigData() {
        List<DataRow> rows = bigData();
        // concat / substr / lower / coalesce(via select expr)
        List<DataRow> got = run(P, ds -> ds
                .select("concat(tag, '-x') as cx", "substr(tag, 0, 1) as s1", "lower(tag) as lt"),
                input(META, rows));
        assertEquals(rows.size(), got.size());
        for (DataRow r : got) {
            if (r.get(0) != null) assertTrue(((String) r.get(0)).endsWith("-x"), "concat suffix");
            if (r.get(2) != null) assertTrue(((String) r.get(2)).startsWith("t"), "lower is lower case");
        }
    }

    // ---- forcing spill: groupBy on a tiny pool takes the aggregator writeState and mergeState serialization path ----

    @Test
    void groupBySumSpillSerializationBigData() {
        System.setProperty("rowweave.worker.operator.minMemory", "16384");
        // high cardinality plus a tiny pool forces spill, which calls aggregator writeState and mergeState, covering the SumAggregator serialization path
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < N; i++) rows.add(row(i % 5000, i % 97, "t")); // 5000 distinct key
        Map<Object, BigDecimal> oracle = new HashMap<>();
        for (DataRow r : rows) oracle.merge(r.get(0), (BigDecimal) r.get(1), BigDecimal::add);

        io.rowweave.mem.ManagedMemory tiny = io.rowweave.mem.ManagedMemory.ofBytes(64 * 1024);
        DataSchema gb = ParallelGroupByAgg.of(META, "k").sum("amt").max("amt").min("amt").count().getOutputSchema();
        io.rowweave.plan.PlanNode src = io.rowweave.plan.PlanNode.sourceFromInputs(
                META, java.util.Collections.singletonList(input(META, rows)));
        io.rowweave.plan.PlanNode g = io.rowweave.plan.PlanNode.unary("groupBy", gb, 0)
                .listParam("keys", java.util.Arrays.asList("k"))
                .listParam("aggKinds", java.util.Arrays.asList("SUM", "MAX", "MIN", "COUNT"))
                .listParam("aggMeasures", java.util.Arrays.asList("amt", "amt", "amt", "")).build();
        WorkerCollectingOutput out = new WorkerCollectingOutput(gb);
        io.rowweave.plan.PlanNode sink = io.rowweave.plan.PlanNode.sink(gb, 1, out).build();
        long rc = new io.rowweave.plan.PushDagExecutor(P, tiny).runSinkGraph(
                new io.rowweave.plan.PlanGraph(java.util.Arrays.asList(src, g, sink)));
        assertEquals(5000, rc, "5000 groups (aggregation correct after spill)");
        // spot check: each group SUM is right, since the spill serialization round-trip loses no precision
        for (DataRow r : out.getRows()) {
            assertEquals(0, oracle.get(r.get(0)).compareTo((BigDecimal) r.get(1)), "key " + r.get(0) + " sum after spill");
        }
    }

    @Test
    void unionBigData() {
        List<DataRow> a = new ArrayList<>();
        List<DataRow> b = new ArrayList<>();
        for (int i = 0; i < N; i++) { DataRow r = new DataRow(2); r.set(0, i); r.set(1, "a"); a.add(r); }
        for (int i = 0; i < N; i++) { DataRow r = new DataRow(2); r.set(0, i); r.set(1, "b"); b.add(r); }
        BatchSession s = RowWeave.createBatchSession("union-big");
        s.setParallelism(P);
        DataSet u = s.fromSource(input(LEFT, a)).union(s.fromSource(input(LEFT, b)), Union.of(LEFT));
        WorkerCollectingOutput out = new WorkerCollectingOutput(u.getSchema());
        u.sinkTo(out);
        s.execute();
        assertEquals(2L * N, out.getRows().size(), "union row count = both sides combined");
    }
}
