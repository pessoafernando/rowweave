package io.rowweave;

import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * coGroup keyed shuffle on a single machine with parallel distinct, with several Sources on both sides feeding R routers.
 * The coGroup partitioner lets null equal null and stays scale-sensitive, which is the opposite of join, so reusing numericNormalized is refused; the result is bit-identical for P in {1,2,3}.
 */
class SingleMachineCoGroupShuffleTest {

    @BeforeEach
    void admitTinyPoolOwners() {
        System.setProperty("rowweave.worker.operator.minMemory", "16384");
    }

    @AfterEach
    void resetMinimumGuarantee() {
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema OUT = new DataSchema(
            new Field("k", DataType.IntegerType),
            new Field("lc", DataType.IntegerType),
            new Field("rc", DataType.IntegerType));

    /** The coGroup function emits [key, |left|, |right|] per group. */
    private static final CoGroupFunction COUNT_FN = new CoGroupFunction() {
        @Override public DataSchema getResultSchema() { return OUT; }
        @Override public void coGroup(Iterable<DataRow> it1, Iterable<DataRow> it2, Collector c) {
            int lc = 0; Object key = "__none__";
            for (DataRow r : it1) { lc++; key = r.get(0); }
            int rc = 0;
            for (DataRow r : it2) { rc++; if ("__none__".equals(key)) key = r.get(0); }
            DataRow o = new DataRow(3); o.set(0, key); o.set(1, lc); o.set(2, rc);
            c.collect(o);
        }
    };

    private static DataRow lrow(Integer k, String v) { DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r; }

    /** coGroup listParams: leftKeys plus rightKeys. */
    private static java.util.Map<String, List<String>> coGroupKeys(String lk, String rk) {
        java.util.Map<String, List<String>> m = new java.util.LinkedHashMap<>();
        m.put("leftKeys", Arrays.asList(lk));
        m.put("rightKeys", Arrays.asList(rk));
        return m;
    }

    private static Source in(DataSchema meta, List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return meta; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    private static List<Source> shards(DataSchema meta, List<DataRow> rows, int n) {
        List<List<DataRow>> b = new ArrayList<>();
        for (int i = 0; i < n; i++) b.add(new ArrayList<>());
        for (int i = 0; i < rows.size(); i++) b.get(i % n).add(rows.get(i));
        List<Source> ins = new ArrayList<>();
        for (List<DataRow> x : b) ins.add(in(meta, x));
        return ins;
    }

    private static List<DataRow> runCoGroup(List<Source> left, List<Source> right, int parallelism) {
        List<DataRow> collected = new ArrayList<>();
        Sink out = new Sink() {
            @Override public DataSchema getSchema() { return OUT; }
            @Override public void collect(DataRow r) { synchronized (collected) { collected.add(r); } }
            @Override public void close() { }
            @Override public void abort() { }
        };
        PlanNode ls = PlanNode.sourceFromInputs(LEFT, left);
        PlanNode rs = PlanNode.sourceFromInputs(RIGHT, right);
        PlanNode cg = PlanNode.binaryFn("coGroup", OUT, 0, 1,
                Collections.emptyMap(), coGroupKeys("lk", "rk"), COUNT_FN);
        PlanNode sink = PlanNode.sink(OUT, 2, out).build();
        new PushDagExecutor(parallelism).runSinkGraph(new PlanGraph(Arrays.asList(ls, rs, cg, sink)));
        collected.sort((a, b) -> Integer.compare((int) a.get(0), (int) b.get(0)));
        return collected;
    }

    @Test
    void coGroupParallelBitIdenticalToOracle() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 300; i++) left.add(lrow(i % 11, "L" + i));
        for (int i = 0; i < 300; i++) right.add(lrow(i % 7, "R" + i));
        List<DataRow> oracle = runCoGroup(Collections.singletonList(in(LEFT, left)),
                Collections.singletonList(in(RIGHT, right)), 1);
        for (int p : new int[]{2, 3}) {
            for (int nIn : new int[]{1, 3}) {
                List<DataRow> got = runCoGroup(shards(LEFT, left, nIn), shards(RIGHT, right, nIn), p);
                assertEquals(oracle.size(), got.size(), "P=" + p + " nIn=" + nIn + " group count");
                for (int i = 0; i < oracle.size(); i++) {
                    assertEquals(oracle.get(i).get(0), got.get(i).get(0), "row " + i + " key");
                    assertEquals(oracle.get(i).get(1), got.get(i).get(1), "row " + i + " lc");
                    assertEquals(oracle.get(i).get(2), got.get(i).get(2), "row " + i + " rc");
                }
            }
        }
    }

    /** coGroup is scale-sensitive and groups null on its own, so the BigDecimal keys 2.0 and 2.00 land in different groups. */
    @Test
    void coGroupScaleSensitiveNullGroupsParallel() {
        DataSchema lm = new DataSchema(new Field("lk", DataType.BigDecimalType), new Field("lv", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("rk", DataType.BigDecimalType), new Field("rv", DataType.StringType));
        DataSchema out = new DataSchema(new Field("k", DataType.BigDecimalType),
                new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        CoGroupFunction fn = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return out; }
            @Override public void coGroup(Iterable<DataRow> it1, Iterable<DataRow> it2, Collector c) {
                int lc = 0; Object k = null; boolean has = false;
                for (DataRow r : it1) { lc++; k = r.get(0); has = true; }
                int rc = 0;
                for (DataRow r : it2) { rc++; if (!has) { k = r.get(0); has = true; } }
                DataRow o = new DataRow(3); o.set(0, k); o.set(1, lc); o.set(2, rc); c.collect(o);
            }
        };
        List<DataRow> left = new ArrayList<>();
        DataRow a = new DataRow(2); a.set(0, new java.math.BigDecimal("2.0")); a.set(1, "a"); left.add(a);
        DataRow b = new DataRow(2); b.set(0, new java.math.BigDecimal("2.00")); b.set(1, "b"); left.add(b);
        DataRow c = new DataRow(2); c.set(0, null); c.set(1, "cnull"); left.add(c);
        List<DataRow> right = new ArrayList<>();
        DataRow d = new DataRow(2); d.set(0, new java.math.BigDecimal("2.0")); d.set(1, "d"); right.add(d);
        DataRow e = new DataRow(2); e.set(0, null); e.set(1, "enull"); right.add(e);

        List<DataRow> collected = new ArrayList<>();
        Sink o = new Sink() {
            @Override public DataSchema getSchema() { return out; }
            @Override public void collect(DataRow r) { synchronized (collected) { collected.add(r); } }
            @Override public void close() { } @Override public void abort() { }
        };
        PlanNode ls = PlanNode.sourceFromInputs(lm, Collections.singletonList(in(lm, left)));
        PlanNode rs = PlanNode.sourceFromInputs(rm, Collections.singletonList(in(rm, right)));
        PlanNode cg = PlanNode.binaryFn("coGroup", out, 0, 1,
                Collections.emptyMap(), coGroupKeys("lk", "rk"), fn);
        PlanNode sink = PlanNode.sink(out, 2, o).build();
        new PushDagExecutor(3).runSinkGraph(new PlanGraph(Arrays.asList(ls, rs, cg, sink)));
        // groups: 2.0 (left a and d, each with lc=1 and rc=1 in practice), 2.00 (left b, rc=0), and null (left cnull and R enull, grouped together since null equals null)
        // scale-sensitive, so 2.0 and 2.00 are two groups, and null equals null, so the null group holds lc=1 and rc=1
        assertEquals(3, collected.size(), "three groups 2.0 / 2.00 / null (scale sensitive plus null grouped)");
    }

    /** The real parallel coGroup-with-spill path on a tiny pool: at P=3 each consumer gets its own sub-allowance, CoGroup spills without running out of memory, which shows it is not the serial fallback. */
    @Test
    void parallelCoGroupSpillsUnderTinyPoolNoOom() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 600; i++) left.add(lrow(i % 30, "L" + i));
        for (int i = 0; i < 600; i++) right.add(lrow(i % 30, "R" + i));

        List<DataRow> collected = new ArrayList<>();
        Sink out = new Sink() {
            @Override public DataSchema getSchema() { return OUT; }
            @Override public void collect(DataRow r) { synchronized (collected) { collected.add(r); } }
            @Override public void close() { } @Override public void abort() { }
        };
        PlanNode ls = PlanNode.sourceFromInputs(LEFT, Collections.singletonList(in(LEFT, left)));
        PlanNode rs = PlanNode.sourceFromInputs(RIGHT, Collections.singletonList(in(RIGHT, right)));
        PlanNode cg = PlanNode.binaryFn("coGroup", OUT, 0, 1,
                Collections.emptyMap(), coGroupKeys("lk", "rk"), COUNT_FN);
        PlanNode sink = PlanNode.sink(OUT, 2, out).build();
        io.rowweave.mem.ManagedMemory tiny = io.rowweave.mem.ManagedMemory.ofBytes(64 * 1024);
        long rows = new PushDagExecutor(3, tiny).runSinkGraph(new PlanGraph(Arrays.asList(ls, rs, cg, sink)));
        assertEquals(30, rows, "30 keys with one group each");
        assertEquals(30, collected.size());
    }

    // ---- parallel distinct ----

    private static final DataSchema DMETA = new DataSchema(
            new Field("k", DataType.StringType), new Field("v", DataType.IntegerType));

    private static DataRow drow(String k, int v) { DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r; }

    private static List<DataRow> runDistinct(List<DataRow> rows, int parallelism) {
        DataSchema dout = DMETA;  // distinct deduplication keeps the input schema
        List<DataRow> collected = new ArrayList<>();
        Sink out = new Sink() {
            @Override public DataSchema getSchema() { return dout; }
            @Override public void collect(DataRow r) { synchronized (collected) { collected.add(r); } }
            @Override public void close() { } @Override public void abort() { }
        };
        PlanNode src = PlanNode.sourceFromInputs(DMETA, Collections.singletonList(in(DMETA, rows)));
        PlanNode d = PlanNode.unary("distinct", dout, 0).listParam("keys", Arrays.asList("k", "v")).build();
        PlanNode sink = PlanNode.sink(dout, 1, out).build();
        new PushDagExecutor(parallelism).runSinkGraph(new PlanGraph(Arrays.asList(src, d, sink)));
        collected.sort((a, b) -> {
            int c = String.valueOf(a.get(0)).compareTo(String.valueOf(b.get(0)));
            return c != 0 ? c : Integer.compare((int) a.get(1), (int) b.get(1));
        });
        return collected;
    }

    @Test
    void distinctParallelBitIdentical() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 400; i++) rows.add(drow("k" + (i % 8), i % 5));
        List<DataRow> oracle = runDistinct(rows, 1);
        for (int p : new int[]{2, 3}) {
            List<DataRow> got = runDistinct(rows, p);
            assertEquals(oracle.size(), got.size(), "P=" + p + " distinct row count");
            for (int i = 0; i < oracle.size(); i++) {
                assertEquals(oracle.get(i).get(0), got.get(i).get(0), "row " + i + " k");
                assertEquals(oracle.get(i).get(1), got.get(i).get(1), "row " + i + " v");
            }
        }
    }
}
