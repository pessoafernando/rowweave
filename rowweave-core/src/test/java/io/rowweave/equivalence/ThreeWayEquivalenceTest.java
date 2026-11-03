package io.rowweave.equivalence;

import io.rowweave.RowWeave;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.FlatMapFunction;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.InProcessCluster;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.ClusterJobClient;
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
class ThreeWayEquivalenceTest {

    private static final long N = Long.getLong("rowweave.equivalence.rows", 200L);

    static final MapFunction MAP_ID = in -> in;
    static final FlatMapFunction FLATMAP_GRADE = (in, c) -> {
        int n = (Integer) in.get(3) + 1;
        for (int i = 0; i < n; i++) c.collect(in);
    };
    // coGroup on dept -> [dept, lc, rc], the size of the left group and of the right group.
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
    // reduceGroup over the coGroup output [dept,lc,rc] per dept -> [dept, the sum of lc].
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
    // combineGroup sums locally per dept, with the output shaped like ORDER: the representative columns plus amount carrying the sum.
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
    // combineReduceGroup: sums globally per dept -> [dept, total].
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

    private Path dir;
    private String ordersA, ordersB, supp, dim, regionDim;
    private MasterServer master;
    private WorkerServer w0, w1;

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("equivalence");
        ordersA = TenDagData.orders(dir, "ordersA", N);
        ordersB = TenDagData.orders(dir, "ordersB", N);
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

    private static void deleteRec(File f) {
        File[] ch = f.listFiles();
        if (ch != null) for (File c : ch) deleteRec(c);
        f.delete();
    }

    private io.rowweave.bench.CsvInput csv(String path, DataSchema meta) {
        return new io.rowweave.bench.CsvInput(path, meta);
    }

    @FunctionalInterface
    private interface Dag { void build(ExecutionEnvironment s, Io ioCols); }

    /** Collects the (DataSchema, file path) of each output, then reads them back after the run to compare. */
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

    /** Runs once, single machine with a null client or a cluster with Cluster, and returns the rows read back from each output plus its meta. */
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

    private void assertSameAcrossRuntimes(String name, Dag dag) {
        List<DataSchema> m1 = new ArrayList<>(), m4 = new ArrayList<>(), mc = new ArrayList<>(), mlc = new ArrayList<>();
        List<List<DataRow>> p1 = run(name + "-s1", 1, null, dag, m1);
        List<List<DataRow>> p4 = run(name + "-s4", 4, null, dag, m4);
        List<List<DataRow>> c2 = run(name + "-c2", 2, new ClusterJobClient("127.0.0.1", master.port()), dag, mc);
        List<List<DataRow>> lc4;  // LocalCluster (InProcessCluster) P4: one worker, so the whole data plane is local
        // The constructor argument is only a slots hint and does not change P=4 inside the graph: the full multi-root dag peaks at 82 slots, and a hint of 8 provides 128.
        try (InProcessCluster lc = new InProcessCluster(8, new File(dir.toFile(), name + "-lc"))) {
            lc4 = run(name + "-lc4", 4, lc, dag, mlc);
        }
        assertEquals(p1.size(), c2.size(), name + " output counts match");
        assertEquals(p1.size(), lc4.size(), name + " local cluster output counts match");
        for (int i = 0; i < p1.size(); i++) {
            DataSchema meta = m1.get(i);
            String d14 = RowSetComparator.diff(meta, p1.get(i), p4.get(i));
            assertNull(d14, name + " out" + i + " single process P1 and P4 result sets differ:\n" + d14);
            String d1c = RowSetComparator.diff(meta, p1.get(i), c2.get(i));
            assertNull(d1c, name + " out" + i + " single process P1 and cluster P2 result sets differ:\n" + d1c);
            String d1lc = RowSetComparator.diff(meta, p1.get(i), lc4.get(i));
            assertNull(d1lc, name + " out" + i + " single process P1 and local cluster P4 result sets differ:\n" + d1lc);
        }
        // the structural dump, used to line up the column order and types with the oracle
        for (int i = 0; i < m1.size(); i++) {
            StringBuilder sb = new StringBuilder();
            DataSchema meta = m1.get(i);
            for (int j = 0; j < meta.getFieldCount(); j++) {
                if (j > 0) sb.append(", ");
                sb.append(meta.getField(j).getName()).append(':').append(meta.getField(j).getDataType());
            }
            System.out.printf("[xcheck] %s o%d rows=%d meta=[%s]%n", name, i, p1.get(i).size(), sb);
        }
        lastV2 = p1;  // kept for the oracle comparison, as a neutral baseline
        lastMetas = m1;
    }

    private List<List<DataRow>> lastV2;
    private List<DataSchema> lastMetas;

    /** Reads a CSV into a List of Object[], parsed against the meta types, as oracle input. */
    private List<Object[]> readCsv(String path, DataSchema meta) {
        List<DataRow> rows = FileSink.read(meta, path);  // FileSink.read also handles the TenDagData TSV, the same TAB-separated typed format
        List<Object[]> out = new ArrayList<>();
        for (DataRow r : rows) {
            Object[] a = new Object[meta.getFieldCount()];
            for (int i = 0; i < a.length; i++) a[i] = r.get(i);
            out.add(a);
        }
        return out;
    }

    /** Turns an oracle Object[] row into a DataRow, one field per meta column. */
    private static List<DataRow> toRows(List<Object[]> objs) {
        List<DataRow> out = new ArrayList<>();
        for (Object[] a : objs) {
            DataRow r = new DataRow(a.length);
            for (int i = 0; i < a.length; i++) r.set(i, a[i]);
            out.add(r);
        }
        return out;
    }

    /** Third-party oracle comparison: the single-machine run against the pure Java oracle, output by output, as a set. */
    private void assertVsOracle(String name, List<List<Object[]>> oracleOuts) {
        assertEquals(lastV2.size(), oracleOuts.size(), name + " output counts match the locally computed expectation");
        for (int i = 0; i < oracleOuts.size(); i++) {
            DataSchema meta = lastMetas.get(i);
            String d = RowSetComparator.diff(meta, lastV2.get(i), toRows(oracleOuts.get(i)));
            assertNull(d, name + " out" + i + " the engine and the locally computed expectation differ:\n" + d);
        }
    }

    // ================= F01: three-configuration set equivalence plus a structural dump =================
    @Test
    void dagFull01_allOpsLinearBackbone() {
        assertSameAcrossRuntimes("dagFull01", (s, ioCols) -> {
            DataSet preAgg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .filter("amount >= 10")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{1})
                    .removeFields("tag", "qty")
                    .partitionByHash("region");
            DataSchema preMeta = preAgg.getSchema();
            DataSchema o1gb = ParallelGroupByAgg.of(preMeta, "region").sum("amount").getOutputSchema();
            ioCols.out(preAgg
                    .groupBy(ParallelGroupByAgg.of(preMeta, "region").sum("amount"))
                    .orderBy(OrderBy.of(o1gb, desc(1)), 1, false)
                    .top(Top.of(o1gb, 5, desc(1)), 1));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 1")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljMeta = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .groupBy(ParallelGroupByAgg.of(ljMeta, "dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        // The third-party baseline: a pure Java oracle written from the mathematical specification, compared as a set against the engine run.
        assertVsOracle("dagFull01", OracleFull01.compute(
                readCsv(ordersA, TenDagData.ORDER), readCsv(ordersB, TenDagData.ORDER),
                readCsv(supp, TenDagData.ORDER), readCsv(dim, TenDagData.DIM)));
    }

    // ================= F02: join first, then a deep chain =================
    @Test
    void dagFull02_joinFirstDeepChain() {
        assertSameAcrossRuntimes("dagFull02", (s, ioCols) -> {
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 3)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER)).combineReduceGroup(COMBINE_REDUCE_SUM, 3);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .union(s.fromSource(csv(supp, TenDagData.ORDER)))
                    .partitionByHash("region")
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "region").max("amount").min("amount").count()));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSet rj = lj
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .filter("amount >= 5")
                    .map(MAP_ID);
            DataSet proj = rj.addFields(f(new Field("mark", DataType.IntegerType)), new Object[]{9})
                    .removeFields("mark", "qty", "bonus")
                    .select("id", "region", "amount");
            DataSchema projMeta = proj.getSchema();
            DataSchema gsum = ParallelGroupByAgg.of(projMeta, "region").sum("amount").getOutputSchema();
            ioCols.out(proj.groupBy(ParallelGroupByAgg.of(projMeta, "region").sum("amount"))
                    .orderBy(OrderBy.of(gsum, asc(0)), 1, false)
                    .top(Top.of(gsum, 6, asc(0)), 1));
        });
        assertVsOracle("dagFull02", OracleFull02.compute(
                readCsv(ordersA, TenDagData.ORDER), readCsv(ordersB, TenDagData.ORDER),
                readCsv(supp, TenDagData.ORDER), readCsv(dim, TenDagData.DIM)));
    }

    // ================= F03: fan-out to 3 outputs through a shared mid =================
    @Test
    void dagFull03_fanOutSharedMid() {
        assertSameAcrossRuntimes("dagFull03", (s, ioCols) -> {
            DataSet mid1 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .filter("qty >= 1")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{2})
                    .removeFields("tag", "grade")
                    .partitionByHash("dept");
            DataSchema m1 = mid1.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(m1, "dept").sum("amount").getOutputSchema();
            ioCols.out(mid1.groupBy(ParallelGroupByAgg.of(m1, "dept").sum("amount"))
                    .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                    .top(Top.of(g1, 7, desc(1)), 1));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagFull03", OracleFull03.compute(
                readCsv(ordersA, TenDagData.ORDER), readCsv(ordersB, TenDagData.ORDER),
                readCsv(supp, TenDagData.ORDER), readCsv(dim, TenDagData.DIM)));
    }

    // ================= F04: union first over 3 sources, then a deep tail chain =================
    @Test
    void dagFull04_unionFirstMultiSource() {
        assertSameAcrossRuntimes("dagFull04", (s, ioCols) -> {
            DataSet u3 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .union(s.fromSource(csv(ordersB, TenDagData.ORDER)))
                    .union(s.fromSource(csv(supp, TenDagData.ORDER)))
                    .filter("amount >= 0")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{4})
                    .removeFields("tag", "qty")
                    .partitionByHash("region");
            DataSchema m1 = u3.getSchema();
            ioCols.out(u3.groupBy(ParallelGroupByAgg.of(m1, "region").max("amount").min("amount").count()));
            DataSet u2 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .union(s.fromSource(csv(supp, TenDagData.ORDER)));
            DataSchema g2 = ParallelGroupByAgg.of(TenDagData.ORDER, "region").sum("amount").getOutputSchema();
            ioCols.out(u2.groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "region").sum("amount"))
                    .orderBy(OrderBy.of(g2, asc(0)), 1, false)
                    .top(Top.of(g2, 8, asc(0)), 1));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            ioCols.out(lj.union(rj)
                    .join(s.fromSource(csv(ordersA, TenDagData.ORDER))).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagFull04", OracleFull04.compute(
                readCsv(ordersA, TenDagData.ORDER), readCsv(ordersB, TenDagData.ORDER),
                readCsv(supp, TenDagData.ORDER), readCsv(dim, TenDagData.DIM)));
    }

    // ================= F05: combine first, then a reducing close and a tail projection =================
    @Test
    void dagFull05_combineFirstReduceClose() {
        assertSameAcrossRuntimes("dagFull05", (s, ioCols) -> {
            DataSet cr1 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            DataSchema crm = cr1.getSchema();
            ioCols.out(cr1.orderBy(OrderBy.of(crm, desc(1)), 1, false)
                    .top(Top.of(crm, 9, desc(1)), 1));
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr2 = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr2).on(new String[]{"dept"}, new String[]{"dept"}));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .distinct("id", "dept")
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            DataSet proj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .filter("amount >= 0")
                    .map(MAP_ID)
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{5})
                    .removeFields("tag", "grade")
                    .partitionByHash("dept");
            DataSchema pm = proj.getSchema();
            ioCols.out(proj.groupBy(ParallelGroupByAgg.of(pm, "dept").sum("amount")));
        });
        assertVsOracle("dagFull05", OracleFull05.compute(
                readCsv(ordersA, TenDagData.ORDER), readCsv(ordersB, TenDagData.ORDER),
                readCsv(supp, TenDagData.ORDER), readCsv(dim, TenDagData.DIM)));
    }

    // ================= F06: regionDim join with orderBy and top up front =================
    @Test
    void dagFull06_regionJoinOrderFirst() {
        assertSameAcrossRuntimes("dagFull06", (s, ioCols) -> {
            DataSchema om = TenDagData.ORDER;
            ioCols.out(s.fromSource(csv(ordersA, om))
                    .orderBy(OrderBy.of(om, asc(0)), 1, false)
                    .top(Top.of(om, 100, asc(0)), 1)
                    .select(0, 1, 4)
                    .map(MAP_ID));
            DataSet rjoin = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(regionDim, TenDagData.REGION_DIM))).on("region")
                    .filter("amount >= 0")
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{6})
                    .removeFields("tag", "rname");
            DataSchema rjm = rjoin.getSchema();
            DataSchema g2 = ParallelGroupByAgg.of(rjm, "dept").sum("amount").getOutputSchema();
            ioCols.out(rjoin.groupBy(ParallelGroupByAgg.of(rjm, "dept").sum("amount"))
                    .orderBy(OrderBy.of(g2, desc(1)), 1, false)
                    .top(Top.of(g2, 10, desc(1)), 1));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 1")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet fj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .fullJoin(s.fromSource(csv(dim, TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(fj)
                    .distinct("id", "dept")
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .partitionByHash("dept")
                    .flatMap(FLATMAP_GRADE)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagFull06", OracleFull06.compute(
                readCsv(ordersA, TenDagData.ORDER), readCsv(ordersB, TenDagData.ORDER),
                readCsv(supp, TenDagData.ORDER), readCsv(dim, TenDagData.DIM),
                readCsv(regionDim, TenDagData.REGION_DIM)));
    }

    // ================= F07: distinct up front, then three nested binaries =================
    @Test
    void dagFull07_distinctFirstNestedJoin() {
        assertSameAcrossRuntimes("dagFull07", (s, ioCols) -> {
            DataSet d1 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .distinct("id", "dept", "region", "grade", "amount", "qty")
                    .filter("amount >= 0")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{7})
                    .removeFields("tag", "qty")
                    .partitionByHash("region");
            DataSchema m1 = d1.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(m1, "region").sum("amount").getOutputSchema();
            ioCols.out(d1.groupBy(ParallelGroupByAgg.of(m1, "region").sum("amount"))
                    .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                    .top(Top.of(g1, 5, desc(1)), 1));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet fj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .fullJoin(s.fromSource(csv(dim, TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj).union(fj)
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagFull07", OracleFull07.compute(
                readCsv(ordersA, TenDagData.ORDER), readCsv(ordersB, TenDagData.ORDER),
                readCsv(supp, TenDagData.ORDER), readCsv(dim, TenDagData.DIM)));
    }

    // ================= F08: partitionByHash reshuffle up front, then a closing tail join =================
    @Test
    void dagFull08_partitionReshuffleFirst() {
        assertSameAcrossRuntimes("dagFull08", (s, ioCols) -> {
            DataSet p1 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .partitionByHash("dept")
                    .map(MAP_ID)
                    .flatMap(FLATMAP_GRADE)
                    .filter("amount >= 0");
            DataSchema pm1 = p1.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(pm1, "dept").sum("amount").getOutputSchema();
            ioCols.out(p1.groupBy(ParallelGroupByAgg.of(pm1, "dept").sum("amount"))
                    .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                    .top(Top.of(g1, 12, desc(1)), 1));
            DataSet p2 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .partitionByHash("region")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{8})
                    .removeFields("tag", "qty")
                    .distinct("dept", "region");
            DataSchema pm2 = p2.getSchema();
            ioCols.out(p2.groupBy(ParallelGroupByAgg.of(pm2, "region").max("amount").min("amount").count()));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSet ij = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            ioCols.out(lj.union(rj).union(ij).select(0, 1, 4));
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagFull08", OracleFull08.compute(
                readCsv(ordersA, TenDagData.ORDER), readCsv(ordersB, TenDagData.ORDER),
                readCsv(supp, TenDagData.ORDER), readCsv(dim, TenDagData.DIM)));
    }

    // ================= F09: five sources fanning out, using both dim and regionDim =================
    @Test
    void dagFull09_fiveSourceMultiOut() {
        assertSameAcrossRuntimes("dagFull09", (s, ioCols) -> {
            DataSet j1 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{9})
                    .removeFields("tag", "grade")
                    .partitionByHash("region");
            DataSchema m1 = j1.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(m1, "region").sum("amount").getOutputSchema();
            ioCols.out(j1.groupBy(ParallelGroupByAgg.of(m1, "region").sum("amount"))
                    .orderBy(OrderBy.of(g1, asc(0)), 1, false)
                    .top(Top.of(g1, 10, asc(0)), 1));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .union(s.fromSource(csv(supp, TenDagData.ORDER)))
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .filter("amount >= 0")
                    .select(0, 1, 4));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSet fj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .fullJoin(s.fromSource(csv(regionDim, TenDagData.REGION_DIM)))
                        .on(new String[]{"region"}, new String[]{"region"})
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj).union(fj)
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagFull09", OracleFull09.compute(
                readCsv(ordersA, TenDagData.ORDER), readCsv(ordersB, TenDagData.ORDER),
                readCsv(supp, TenDagData.ORDER), readCsv(dim, TenDagData.DIM),
                readCsv(regionDim, TenDagData.REGION_DIM)));
    }

    // ================= F10: a deeply mixed whole =================
    @Test
    void dagFull10_deepMixedGrandChain() {
        assertSameAcrossRuntimes("dagFull10", (s, ioCols) -> {
            DataSet chain = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .filter("amount >= 0")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .map(MAP_ID)
                    .join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .flatMap(FLATMAP_GRADE)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{10})
                    .removeFields("tag", "qty")
                    .partitionByHash("dept");
            DataSet chain2 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .filter("amount >= 0")
                    .map(MAP_ID)
                    .partitionByHash("dept")
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSchema c2m = chain2.getSchema();
            ioCols.out(chain2.orderBy(OrderBy.of(c2m, desc(1)), 1, false)
                    .top(Top.of(c2m, 15, desc(1)), 1));
            DataSchema chm = chain.getSchema();
            ioCols.out(chain.groupBy(ParallelGroupByAgg.of(chm, "region").sum("amount")));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .distinct("dept", "region", "grade")
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet fj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .fullJoin(s.fromSource(csv(dim, TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj).union(fj)
                    .distinct("id", "dept")
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
        });
        assertVsOracle("dagFull10", OracleFull10.compute(
                readCsv(ordersA, TenDagData.ORDER), readCsv(ordersB, TenDagData.ORDER),
                readCsv(supp, TenDagData.ORDER), readCsv(dim, TenDagData.DIM)));
    }
}

