package io.rowweave.equivalence;

import io.rowweave.RowWeave;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.FlatMapFunction;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.InProcessCluster;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.RealClusterJobClient;
import io.rowweave.cluster.TenDagData;
import io.rowweave.cluster.WorkerServer;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class ComplexDagMultiInputMultiOutputEquivalenceTest {

    private static final long N = Long.getLong("rowweave.equivalence.rows", 300L);

    private Path dir;
    private String ordersA, ordersB, ordersC, supp, dim, regionDim;
    private MasterServer master;
    private WorkerServer w0, w1;

    // ===== operator lambdas, with the same semantics as ThreeWayEquivalenceTest =====
    static final MapFunction MAP_ID = in -> in;
    static final FlatMapFunction FLATMAP_GRADE = (in, c) -> {
        int n = (Integer) in.get(3) + 1;
        for (int i = 0; i < Math.min(n, 3); i++) c.collect(in);  // capped at 3 so the row count stays bounded
    };
    static final io.rowweave.CoGroupFunction CG_FN = new io.rowweave.CoGroupFunction() {
        private final DataSchema out = new DataSchema(new Field("dept", DataType.IntegerType),
                new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void coGroup(Iterable<DataRow> l, Iterable<DataRow> r, io.rowweave.Collector c) {
            int lc = 0; Object k = null;
            for (DataRow x : l) { lc++; k = x.get(1); }
            int rc = 0;
            for (DataRow x : r) { rc++; if (k == null) k = x.get(1); }
            DataRow o = new DataRow(3); o.set(0, k); o.set(1, lc); o.set(2, rc); c.collect(o);
        }
    };
    static final io.rowweave.GroupReduceFunction RG_SUM_LC = new io.rowweave.GroupReduceFunction() {
        private final DataSchema out = new DataSchema(new Field("dept", DataType.IntegerType),
                new Field("lcSum", DataType.IntegerType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void reduce(Iterable<DataRow> rows, io.rowweave.Collector c) {
            int s = 0; Object k = null;
            for (DataRow r : rows) { s += (Integer) r.get(1); k = r.get(0); }
            DataRow o = new DataRow(2); o.set(0, k); o.set(1, s); c.collect(o);
        }
    };
    static final io.rowweave.GroupCombineFunction COMBINE_SUM = (rows, c) -> {
        java.util.LinkedHashMap<Integer, DataRow> firstRow = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<Integer, java.math.BigDecimal> sum = new java.util.LinkedHashMap<>();
        for (DataRow r : rows) {
            int dept = (Integer) r.get(1);
            firstRow.putIfAbsent(dept, r);
            sum.merge(dept, (java.math.BigDecimal) r.get(4), java.math.BigDecimal::add);
        }
        for (java.util.Map.Entry<Integer, DataRow> e : firstRow.entrySet()) {
            DataRow src = e.getValue();
            DataRow o = new DataRow(6);
            o.set(0, src.get(0)); o.set(1, e.getKey()); o.set(2, src.get(2));
            o.set(3, src.get(3)); o.set(4, sum.get(e.getKey())); o.set(5, src.get(5));
            c.collect(o);
        }
    };
    static final io.rowweave.GroupCombineReduceFunction COMBINE_REDUCE_SUM = new io.rowweave.GroupCombineReduceFunction() {
        private final DataSchema out = new DataSchema(new Field("dept", DataType.IntegerType),
                new Field("total", DataType.BigDecimalType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void combine(Iterable<DataRow> rows, io.rowweave.Collector c) { COMBINE_SUM.combine(rows, c); }
        @Override public void reduce(Iterable<DataRow> rows, io.rowweave.Collector c) {
            java.util.TreeMap<Integer, java.math.BigDecimal> sum = new java.util.TreeMap<>();
            for (DataRow r : rows) sum.merge((Integer) r.get(1), (java.math.BigDecimal) r.get(4), java.math.BigDecimal::add);
            for (java.util.Map.Entry<Integer, java.math.BigDecimal> e : sum.entrySet()) {
                DataRow o = new DataRow(2); o.set(0, e.getKey());
                o.set(1, e.getValue().setScale(10, java.math.RoundingMode.HALF_UP)); c.collect(o);
            }
        }
    };
    private static SortField desc(int i) { return new SortField(i, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST); }
    private static SortField asc(int i) { return new SortField(i, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST); }
    private static Field[] f(Field... x) { return x; }

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("xdag-equivalence");
        // Multi-source dataset: one logical source split across several CSV splits (fromSource(csv,csv,...)).
        ordersA = TenDagData.orders(dir, "ordersA", N);
        ordersB = TenDagData.orders(dir, "ordersB", N);
        ordersC = TenDagData.orders(dir, "ordersC", N);
        supp = TenDagData.supplement(dir, "supp", N / 2, N / 2);
        dim = TenDagData.dim(dir);
        regionDim = TenDagData.regionDim(dir);
        master = new MasterServer(5000);
        File d0 = new File(dir.toFile(), "w0"); d0.mkdirs();
        File d1 = new File(dir.toFile(), "w1"); d1.mkdirs();
        w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), d0);
        w1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), d1);
    }

    @AfterAll
    void tearDown() {
        if (w0 != null) w0.close();
        if (w1 != null) w1.close();
        if (master != null) master.close();
        try { deleteRec(dir.toFile()); } catch (RuntimeException ignored) { }
    }

    private static void deleteRec(File file) {
        File[] ch = file.listFiles();
        if (ch != null) for (File c : ch) deleteRec(c);
        file.delete();
    }

    private io.rowweave.bench.CsvInput csv(String path, DataSchema meta) {
        return new io.rowweave.bench.CsvInput(path, meta);
    }

    /** A multi-split source: one Source cut from each CSV of the same shape, and fromSource(Source...) builds the single logical source over them. */
    private DataSet multiSrc(ExecutionEnvironment s, DataSchema meta, String... paths) {
        Source[] ins = new Source[paths.length];
        for (int i = 0; i < paths.length; i++) ins[i] = csv(paths[i], meta);
        return s.fromSource(ins);
    }

    @FunctionalInterface
    private interface Dag { void build(ExecutionEnvironment s, Io ioCols); }

    private final class Io {
        final String tag; final List<DataSchema> metas = new ArrayList<>(); final List<Path> paths = new ArrayList<>(); int used = 0;
        Io(String tag) { this.tag = tag; }
        void out(DataSet ds) {
            Path p = dir.resolve(tag + "-o" + used++ + ".tsv");
            metas.add(ds.getSchema());
            paths.add(p);
            ds.sinkTo(new FileSink(ds.getSchema(), p.toString()));
        }
    }

    private List<List<DataRow>> run(String name, int par, io.rowweave.cluster.JobClient client, Dag dag, List<DataSchema> outMetas) {
        Io ioCols = new Io(name);
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : RowWeave.createEnvironment(name);
        s.setParallelism(par);
        dag.build(s, ioCols);
        s.execute();
        List<List<DataRow>> outs = new ArrayList<>();
        outMetas.clear();
        for (int i = 0; i < ioCols.paths.size(); i++) {
            outMetas.add(ioCols.metas.get(i));
            outs.add(FileSink.read(ioCols.metas.get(i), ioCols.paths.get(i).toString()));
        }
        return outs;
    }

    /** Each output is compared as a set across four configurations (single machine P1 and P4, a real cluster P2, and LocalCluster P4), which is the data-correctness comparison against the single-machine run. */
    private void assertConsistent(String name, int nOut, Dag dag) {
        List<DataSchema> m1 = new ArrayList<>(), m4 = new ArrayList<>(), mc = new ArrayList<>(), mlc = new ArrayList<>();
        List<List<DataRow>> p1 = run(name + "-s1", 1, null, dag, m1);
        assertEquals(nOut, p1.size(), name + " output count");
        List<List<DataRow>> p4 = run(name + "-s4", 4, null, dag, m4);
        List<List<DataRow>> c2 = run(name + "-c2", 2, new RealClusterJobClient("127.0.0.1", master.port()), dag, mc);
        List<List<DataRow>> lc4;
        // The constructor argument is only a slots hint; the graph still runs at P=4, and 128 slots are reserved for the full multi-root dag.
        try (InProcessCluster lc = new InProcessCluster(8, new File(dir.toFile(), name + "-lc"))) {
            lc4 = run(name + "-lc4", 4, lc, dag, mlc);
        }
        assertEquals(p1.size(), p4.size(), name + " output count at parallelism 4 in one process");
        assertEquals(p1.size(), c2.size(), name + " output count on the cluster");
        assertEquals(p1.size(), lc4.size(), name + " output count on the local cluster");
        for (int i = 0; i < p1.size(); i++) {
            DataSchema meta = m1.get(i);
            assertNull(RowSetComparator.diff(meta, p1.get(i), p4.get(i)), name + " out" + i + " the single process P1 and P4 result sets differ");
            assertNull(RowSetComparator.diff(meta, p1.get(i), c2.get(i)), name + " out" + i + " the single process P1 and cluster P2 result sets differ");
            assertNull(RowSetComparator.diff(meta, p1.get(i), lc4.get(i)), name + " out" + i + " the single process P1 and local cluster P4 result sets differ");
        }
        System.out.printf("[xdag] %-6s %d out ✓ all 4 configurations agree (every operator, several Input and Output endpoints)%n", name, nOut);
    }

    private static final DataSchema ORD = TenDagData.ORDER;
    private static final DataSchema DIM = TenDagData.DIM;

    // ================= X01: multi-source union fanning out to 3 outputs (groupBy family + orderBy + top) =================
    @Test
    void x01_multiSrcUnionFanoutAgg() {
        assertConsistent("x01", 3, (s, ioCols) -> {
            // The A+B+C three-split source unioned with supp and then filtered, feeding the shared mid, which fans out to 3 downstreams.
            DataSet mid = multiSrc(s, ORD, ordersA, ordersB, ordersC)
                    .filter("amount >= 5")
                    .union(s.fromSource(csv(supp, ORD)))
                    .partitionByHash("dept");
            DataSchema gm = ParallelGroupByAgg.of(ORD, "dept").sum("amount").getOutputSchema();
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount").count())
                    .orderBy(OrderBy.of(mid.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount").count()).getSchema(), desc(1)), 1, false));
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "region").max("amount").min("amount")));
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount"))
                    .top(Top.of(gm, 5, desc(1)), 1));
        });
    }

    // ================= X02: multi-source join fanning out (distinct + flatMap + select) =================
    @Test
    void x02_multiSrcJoinFanout() {
        assertConsistent("x02", 3, (s, ioCols) -> {
            // The A+B split is filtered first, on the original ORDER layout so its column references stay stable, and feeds the shared mid; the single-column join output forms its own branch.
            DataSet mid = multiSrc(s, ORD, ordersA, ordersB).filter("qty >= 1");
            // Branch 1: join, then a flatMap reading get(3)=grade, a layout usable exactly as in F01, then distinct, then select.
            ioCols.out(mid.join(s.fromSource(csv(ordersC, ORD))).on("id")
                    .flatMap(FLATMAP_GRADE).distinct("id", "dept").select(0, 1, 4));
            // Branch 2: filter and groupBy on the ORDER layout, where the column names are stable.
            ioCols.out(mid.filter("amount >= 20").coGroup(s.fromSource(csv(supp, ORD)), CG_FN).on("dept")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // Branch 3: addFields, removeFields and distinct on the ORDER layout.
            ioCols.out(mid.addFields(f(new Field("one", DataType.IntegerType)), new Object[]{1})
                    .removeFields("one").map(MAP_ID).distinct("dept", "region"));
        });
    }

    // ================= X03: multi-source coGroup with combineGroup fan-out =================
    @Test
    void x03_multiSrcCoGroupCombineFanout() {
        assertConsistent("x03", 2, (s, ioCols) -> {
            ioCols.out(multiSrc(s, ORD, ordersA, ordersC)
                    .coGroup(s.fromSource(csv(supp, ORD)), CG_FN).on("dept")
                    .filter("lc >= 1")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet cg = multiSrc(s, ORD, ordersA, ordersB)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersC, ORD)).combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // ================= X04: diamond in which the multi-source shared mid is both one join input and a direct output =================
    @Test
    void x04_diamondSharedMid() {
        assertConsistent("x04", 3, (s, ioCols) -> {
            DataSet mid = multiSrc(s, ORD, ordersA, ordersB)
                    .filter("amount >= 10").select("id", "dept", "region", "grade", "amount", "qty");
            ioCols.out(mid.distinct("dept", "region"));  // branch 1: written straight to output
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount")));  // branch 2: groupBy
            ioCols.out(mid.leftJoin(s.fromSource(csv(dim, DIM))).on("dept")
                    .groupBy(ParallelGroupByAgg.of(mid.leftJoin(s.fromSource(csv(dim, DIM))).on("dept").getSchema(), "dept").count()));  // branch 3: join
        });
    }

    // ================= X05: a multi-level join chain with multi-source fan-out (left/right/full) =================
    @Test
    void x05_multiJoinTypesFanout() {
        assertConsistent("x05", 3, (s, ioCols) -> {
            DataSet base = multiSrc(s, ORD, ordersA, ordersB, ordersC).filter("qty >= 1");
            ioCols.out(base.leftJoin(s.fromSource(csv(dim, DIM))).on("dept")
                    .groupBy(ParallelGroupByAgg.of(base.leftJoin(s.fromSource(csv(dim, DIM))).on("dept").getSchema(), "dept").sum("amount")));
            ioCols.out(base.rightJoin(s.fromSource(csv(dim, DIM))).on("dept")
                    .distinct("dept"));
            ioCols.out(base.map(MAP_ID).fullJoin(s.fromSource(csv(dim, DIM))).on(new String[]{"dept"}, new String[]{"dept"})
                    .filter("dept >= 0"));
        });
    }

    // ================= X06: combineReduceGroup and reduceGroup with multi-source union fan-out =================
    @Test
    void x06_combineReduceMultiSrcFanout() {
        assertConsistent("x06", 2, (s, ioCols) -> {
            ioCols.out(multiSrc(s, ORD, ordersA, ordersB, ordersC).combineReduceGroup(COMBINE_REDUCE_SUM, 4));
            DataSet u = multiSrc(s, ORD, ordersA, ordersB).union(s.fromSource(csv(supp, ORD)))
                    .partitionByHash("region");
            ioCols.out(u.groupBy(ParallelGroupByAgg.of(ORD, "region").max("amount").min("amount").count())
                    .orderBy(OrderBy.of(u.groupBy(ParallelGroupByAgg.of(ORD, "region").max("amount").min("amount").count()).getSchema(), asc(0)), 1, false));
        });
    }

    // ================= X07: one backbone chain over every operator, fanning out to 4 outputs =================
    @Test
    void x07_allOpsBackboneFanout4() {
        assertConsistent("x07", 4, (s, ioCols) -> {
            DataSet mid = multiSrc(s, ORD, ordersA, ordersB)
                    .filter("amount >= 8")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{7})
                    .removeFields("tag")
                    .partitionByHash("dept");
            DataSchema gm = ParallelGroupByAgg.of(mid.getSchema(), "dept").sum("amount").getOutputSchema();
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(mid.getSchema(), "dept").sum("amount"))
                    .orderBy(OrderBy.of(gm, desc(1)), 1, false).top(Top.of(gm, 6, desc(1)), 1));
            ioCols.out(mid.flatMap(FLATMAP_GRADE).distinct("id", "dept").select(0, 1));
            ioCols.out(mid.coGroup(s.fromSource(csv(supp, ORD)), CG_FN).on("dept").groupBy("dept").reduceGroup(RG_SUM_LC));
            ioCols.out(mid.join(s.fromSource(csv(ordersC, ORD))).on("id").filter("grade >= 0").distinct("id"));
        });
    }

    // ================= X08: a wide fan-out to 5 outputs sharing one groupBy breaker =================
    @Test
    void x08_wideFanout5SharedBreaker() {
        assertConsistent("x08", 5, (s, ioCols) -> {
            DataSet agg = multiSrc(s, ORD, ordersA, ordersB, ordersC)
                    .filter("amount >= 5")
                    .groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount").count());  // the shared expensive breaker
            DataSchema am = agg.getSchema();
            ioCols.out(agg.filter("sum0 >= 100"));
            ioCols.out(agg.filter("sum0 < 100"));
            ioCols.out(agg.orderBy(OrderBy.of(am, desc(1)), 1, false));
            ioCols.out(agg.top(Top.of(am, 8, desc(1)), 1));
            ioCols.out(agg.distinct("dept"));
        });
    }

    // ================= X09: two levels of keyed shuffle (groupBy into distinct) with multi-source fan-out =================
    @Test
    void x09_twoLayerShuffleFanout() {
        assertConsistent("x09", 3, (s, ioCols) -> {
            DataSet layer2 = multiSrc(s, ORD, ordersA, ordersB)
                    .groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount"))
                    .distinct("dept", "sum0");
            ioCols.out(layer2.orderBy(OrderBy.of(layer2.getSchema(), asc(0)), 1, false));
            ioCols.out(layer2.filter("sum0 >= 50"));
            ioCols.out(multiSrc(s, ORD, ordersA, ordersC)
                    .union(s.fromSource(csv(supp, ORD)))
                    .groupBy(ParallelGroupByAgg.of(ORD, "region").count()));
        });
    }

    // ================= X10: the largest mix (multi-source + join + coGroup + union + every aggregate + 5 outputs) =================
    @Test
    void x10_grandMixedMultiSrcFanout5() {
        assertConsistent("x10", 5, (s, ioCols) -> {
            DataSet ab = multiSrc(s, ORD, ordersA, ordersB);
            DataSet shared = ab.filter("amount >= 3").partitionByHash("dept");
            ioCols.out(shared.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount").max("amount").min("amount").count()));
            ioCols.out(shared.flatMap(FLATMAP_GRADE).distinct("id", "dept").select(0, 1, 4));
            ioCols.out(shared.coGroup(s.fromSource(csv(supp, ORD)), CG_FN).on("dept").filter("rc >= 0").groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = shared.leftJoin(s.fromSource(csv(dim, DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersC, ORD)).rightJoin(s.fromSource(csv(dim, DIM))).on("dept");
            ioCols.out(lj.union(rj).groupBy(ParallelGroupByAgg.of(lj.getSchema(), "dept").count()));
            DataSet cg = ab.combineGroup(COMBINE_SUM, 4).groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersC, ORD)).combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }
}

