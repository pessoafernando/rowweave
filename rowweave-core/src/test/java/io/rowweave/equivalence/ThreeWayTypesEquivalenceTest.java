package io.rowweave.equivalence;

import io.rowweave.RowWeave;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.FlatMapFunction;
import io.rowweave.GroupCombineFunction;
import io.rowweave.GroupCombineReduceFunction;
import io.rowweave.GroupReduceFunction;
import io.rowweave.BatchSession;
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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class ThreeWayTypesEquivalenceTest {

    private static final long N = Long.getLong("rowweave.equivalence.rows", 200L);

    static final MapFunction MAP_ID = in -> in;
    static final FlatMapFunction FLATMAP_GRADE = (in, c) -> {
        int n = (Integer) in.get(3) + 1;
        for (int i = 0; i < n; i++) c.collect(in);
    };
    static final io.rowweave.CoGroupFunction CG_FN = new io.rowweave.CoGroupFunction() {
        private final DataSchema out = new DataSchema(new Field("dept", DataType.IntegerType),
                new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void coGroup(Iterable<DataRow> l, Iterable<DataRow> r, Collector c) {
            int lc = 0; Object k = null;
            for (DataRow x : l) { lc++; k = x.get(1); }
            int rc = 0;
            for (DataRow x : r) { rc++; if (k == null) k = x.get(1); }
            DataRow o = new DataRow(3); o.set(0, k); o.set(1, lc); o.set(2, rc); c.collect(o);
        }
    };
    static final GroupReduceFunction RG_SUM_LC = new GroupReduceFunction() {
        private final DataSchema out = new DataSchema(new Field("dept", DataType.IntegerType),
                new Field("lcSum", DataType.IntegerType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void reduce(Iterable<DataRow> rows, Collector c) {
            int s = 0; Object k = null;
            for (DataRow r : rows) { s += (Integer) r.get(1); k = r.get(0); }
            DataRow o = new DataRow(2); o.set(0, k); o.set(1, s); c.collect(o);
        }
    };
    static final GroupCombineFunction COMBINE_SUM = (rows, c) -> {
        java.util.LinkedHashMap<Integer, DataRow> firstRow = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<Integer, BigDecimal> sum = new java.util.LinkedHashMap<>();
        for (DataRow r : rows) {
            int dept = (Integer) r.get(1);
            firstRow.putIfAbsent(dept, r);
            sum.merge(dept, (BigDecimal) r.get(4), BigDecimal::add);
        }
        for (java.util.Map.Entry<Integer, DataRow> e : firstRow.entrySet()) {
            DataRow src = e.getValue();
            DataRow o = new DataRow(6);
            o.set(0, src.get(0)); o.set(1, e.getKey()); o.set(2, src.get(2));
            o.set(3, src.get(3)); o.set(4, sum.get(e.getKey())); o.set(5, src.get(5));
            c.collect(o);
        }
    };
    static final GroupCombineReduceFunction COMBINE_REDUCE_SUM = new GroupCombineReduceFunction() {
        private final DataSchema out = new DataSchema(new Field("dept", DataType.IntegerType),
                new Field("total", DataType.BigDecimalType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void combine(Iterable<DataRow> rows, Collector c) { COMBINE_SUM.combine(rows, c); }
        @Override public void reduce(Iterable<DataRow> rows, Collector c) {
            java.util.TreeMap<Integer, BigDecimal> sum = new java.util.TreeMap<>();
            for (DataRow r : rows) sum.merge((Integer) r.get(1), (BigDecimal) r.get(4), BigDecimal::add);
            for (java.util.Map.Entry<Integer, BigDecimal> e : sum.entrySet()) {
                DataRow o = new DataRow(2); o.set(0, e.getKey());
                o.set(1, e.getValue().setScale(10, RoundingMode.HALF_UP)); c.collect(o);
            }
        }
    };
    private static final String[] ORDER6 = {"id", "dept", "region", "grade", "amount", "qty"};
    private static SortField asc(int i) { return new SortField(i, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST); }
    private static SortField desc(int i) { return new SortField(i, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST); }
    private static Field[] f(Field... x) { return x; }

    private Path dir;
    private String wideA, wideB, supp, dim, regionDim, labelDim;
    private MasterServer master;
    private WorkerServer w0, w1;

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("equivalence-types");
        wideA = TenDagData.wide(dir, "wideA", N);
        wideB = TenDagData.wide(dir, "wideB", N);
        supp = TenDagData.supplement(dir, "supp", N / 2, N / 2);
        dim = TenDagData.dim(dir);
        regionDim = TenDagData.regionDim(dir);
        labelDim = TenDagData.labelDim(dir);
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
    private interface Dag { void build(BatchSession s, Io ioCols); }

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
        BatchSession s = client != null ? new BatchSession(name, name, client) : RowWeave.createBatchSession(name);
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

    private List<List<DataRow>> lastV2;
    private List<DataSchema> lastMetas;

    private void assertSameAcrossRuntimes(String name, Dag dag) {
        List<DataSchema> m1 = new ArrayList<>(), m4 = new ArrayList<>(), mc = new ArrayList<>(), mlc = new ArrayList<>();
        List<List<DataRow>> p1 = run(name + "-s1", 1, null, dag, m1);
        List<List<DataRow>> p4 = run(name + "-s4", 4, null, dag, m4);
        List<List<DataRow>> c2 = run(name + "-c2", 2, new ClusterJobClient("127.0.0.1", master.port()), dag, mc);
        List<List<DataRow>> lc4;  // LocalCluster P4: one worker, so the whole data plane is local, and it agrees with the other two runs
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
        for (int i = 0; i < m1.size(); i++) {
            StringBuilder sb = new StringBuilder();
            DataSchema meta = m1.get(i);
            for (int j = 0; j < meta.getFieldCount(); j++) {
                if (j > 0) sb.append(", ");
                sb.append(meta.getField(j).getName()).append(':').append(meta.getField(j).getDataType());
            }
            System.out.printf("[xtypes] %s o%d rows=%d meta=[%s]%n", name, i, p1.get(i).size(), sb);
        }
        lastV2 = p1;
        lastMetas = m1;
    }

    private List<Object[]> readCsv(String path, DataSchema meta) {
        List<DataRow> rows = FileSink.read(meta, path);
        List<Object[]> out = new ArrayList<>();
        for (DataRow r : rows) {
            Object[] a = new Object[meta.getFieldCount()];
            for (int i = 0; i < a.length; i++) a[i] = r.get(i);
            out.add(a);
        }
        return out;
    }

    private static List<DataRow> toRows(List<Object[]> objs) {
        List<DataRow> out = new ArrayList<>();
        for (Object[] a : objs) {
            DataRow r = new DataRow(a.length);
            for (int i = 0; i < a.length; i++) r.set(i, a[i]);
            out.add(r);
        }
        return out;
    }

    private void assertVsOracle(String name, List<List<Object[]>> oracleOuts) {
        assertEquals(lastV2.size(), oracleOuts.size(), name + " output counts match the locally computed expectation");
        for (int i = 0; i < oracleOuts.size(); i++) {
            DataSchema meta = lastMetas.get(i);
            String d = RowSetComparator.diff(meta, lastV2.get(i), toRows(oracleOuts.get(i)));
            assertNull(d, name + " out" + i + " the engine and the locally computed expectation differ:\n" + d);
        }
    }

    // ================= T01: three-configuration set equivalence plus a structural dump =================
    @Test
    void dagTyped01_wideBoolGroupLabelPartition() {
        assertSameAcrossRuntimes("dagTyped01", (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .filter("amount >= 0")
                    .select("id", "dept", "active", "label", "amount", "qty")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{1})
                    .removeFields("tag", "qty")
                    .partitionByHash("label");
            DataSchema midMeta = mid.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(midMeta, "active").sum("amount").getOutputSchema();
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(midMeta, "active").sum("amount"))
                    .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                    .top(Top.of(g1, 5, desc(1)), 1));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .join(s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagTyped01", OracleTyped01.compute(w(wideA), w(wideB), o(supp), dimD()));
    }

    // Reads the wide table (10 columns), ORDER (6 columns) and DIM (2 columns) CSVs into Lists of Object[].
    private List<Object[]> w(String path) { return readCsv(path, TenDagData.WIDE); }
    private List<Object[]> o(String path) { return readCsv(path, TenDagData.ORDER); }
    private List<Object[]> dimD() { return readCsv(dim, TenDagData.DIM); }
    private List<Object[]> labelD() { return readCsv(labelDim, TenDagData.LABEL_DIM); }
    private List<Object[]> regionD() { return readCsv(regionDim, TenDagData.REGION_DIM); }

    // ================= T02:label join + Date group =================
    @Test
    void dagTyped02_labelJoinDateGroup() {
        assertSameAcrossRuntimes("dagTyped02", (s, ioCols) -> {
            DataSet j = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .join(s.fromSource(csv(labelDim, TenDagData.LABEL_DIM))).on("label")
                    .filter("amount >= 0")
                    .select("id", "dept", "created", "amount", "weight")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{2})
                    .removeFields("tag", "weight")
                    .partitionByHash("dept");
            DataSchema jm = j.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(jm, "created").sum("amount").getOutputSchema();
            ioCols.out(j.groupBy(ParallelGroupByAgg.of(jm, "created").sum("amount"))
                    .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                    .top(Top.of(g1, 8, desc(1)), 1));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE))
                    .union(s.fromSource(csv(wideB, TenDagData.WIDE)))
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "active", "created", "tstamp", "label")
                    .select(0, 1, 4));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet fj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .fullJoin(s.fromSource(csv(dim, TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj).union(fj)
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagTyped02", OracleTyped02.compute(w(wideA), w(wideB), o(supp), dimD(), labelD()));
    }

    // ================= T03: Timestamp ordering with Boolean distinct and fan-out =================
    @Test
    void dagTyped03_tsSortBoolDistinctFanout() {
        assertSameAcrossRuntimes("dagTyped03", (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .filter("amount >= 0")
                    .select("id", "dept", "active", "grade", "tstamp", "amount")
                    .map(MAP_ID)
                    .partitionByHash("dept");
            DataSchema mm = mid.getSchema();
            ioCols.out(mid.orderBy(OrderBy.of(mm, asc(4), asc(0)), 1, false)
                    .top(Top.of(mm, 150, asc(4), asc(0)), 1)
                    .flatMap(FLATMAP_GRADE)
                    .select(0, 1, 5));
            DataSet g2 = mid.groupBy(ParallelGroupByAgg.of(mm, "active").max("amount").min("amount").count());
            DataSchema g2m = g2.getSchema();
            ioCols.out(g2.distinct(g2m.getField(0).getName(), g2m.getField(1).getName())
                    .addFields(f(new Field("mk", DataType.IntegerType)), new Object[]{3})
                    .removeFields("mk"));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .join(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagTyped03", OracleTyped03.compute(w(wideA), w(wideB), o(supp), dimD()));
    }

    // ================= T04: combine first, grouping by label, then an inner-join chain =================
    @Test
    void dagTyped04_combineFirstLabelGroup() {
        assertSameAcrossRuntimes("dagTyped04", (s, ioCols) -> {
            DataSet cr1 = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            DataSchema crm = cr1.getSchema();
            ioCols.out(cr1.orderBy(OrderBy.of(crm, desc(1)), 1, false)
                    .top(Top.of(crm, 9, desc(1)), 1));
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr2 = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr2).on(new String[]{"dept"}, new String[]{"dept"}));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .distinct("id", "dept")
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            DataSet chain = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .join(s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .filter("amount >= 0")
                    .map(MAP_ID)
                    .select(0, 1, 9, 6, 4, 3)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{4})
                    .removeFields("tag", "grade")
                    .partitionByHash("active");
            DataSchema chm = chain.getSchema();
            ioCols.out(chain.groupBy(ParallelGroupByAgg.of(chm, "label").sum("amount")));
        });
        assertVsOracle("dagTyped04", OracleTyped04.compute(w(wideA), w(wideB), o(supp), dimD()));
    }

    // ================= T05: union first with Date ordering and mixed distinct =================
    @Test
    void dagTyped05_unionFirstDateSort() {
        assertSameAcrossRuntimes("dagTyped05", (s, ioCols) -> {
            DataSet u = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .union(s.fromSource(csv(wideB, TenDagData.WIDE)))
                    .filter("amount >= 0")
                    .select("id", "dept", "label", "active", "amount", "qty")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{5})
                    .removeFields("tag", "qty")
                    .partitionByHash("dept");
            DataSchema um = u.getSchema();
            ioCols.out(u.groupBy(ParallelGroupByAgg.of(um, "label").sum("amount")));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE))
                    .orderBy(OrderBy.of(TenDagData.WIDE, desc(7)), 1, false)
                    .top(Top.of(TenDagData.WIDE, 300, desc(7)), 1)
                    .flatMap(FLATMAP_GRADE)
                    .distinct("active", "label", "dept")
                    .select(0, 1, 4));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .join(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)).on("id")
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagTyped05", OracleTyped05.compute(w(wideA), w(wideB), o(supp), dimD()));
    }

    // ================= T06: fullJoin first, Timestamp grouping and distinct up front =================
    @Test
    void dagTyped06_fullJoinFirstTsGroup() {
        assertSameAcrossRuntimes("dagTyped06", (s, ioCols) -> {
            DataSet d1 = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .distinct("id", "dept", "region", "grade", "amount", "qty", "active", "created", "tstamp", "label")
                    .filter("amount >= 0")
                    .select("id", "dept", "tstamp", "amount", "grade")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{6})
                    .removeFields("tag", "grade")
                    .partitionByHash("dept");
            DataSchema d1m = d1.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(d1m, "tstamp").count().getOutputSchema();
            ioCols.out(d1.groupBy(ParallelGroupByAgg.of(d1m, "tstamp").count())
                    .orderBy(OrderBy.of(g1, asc(0)), 1, false)
                    .top(Top.of(g1, 30, asc(0)), 1));
            DataSet fj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .fullJoin(s.fromSource(csv(dim, TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema fjm = fj.getSchema();
            ioCols.out(fj.union(lj).union(rj)
                    .groupBy(ParallelGroupByAgg.of(fjm, "dept").max("amount").min("amount").count()));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .join(s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagTyped06", OracleTyped06.compute(w(wideA), w(wideB), o(supp), dimD()));
    }

    // ================= T07:Boolean/String partition reshuffle =================
    @Test
    void dagTyped07_boolPartitionReshuffle() {
        assertSameAcrossRuntimes("dagTyped07", (s, ioCols) -> {
            DataSet p1 = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .partitionByHash("active")
                    .map(MAP_ID)
                    .flatMap(FLATMAP_GRADE)
                    .filter("amount >= 0");
            DataSchema pm1 = p1.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(pm1, "active").sum("amount").getOutputSchema();
            ioCols.out(p1.groupBy(ParallelGroupByAgg.of(pm1, "active").sum("amount"))
                    .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                    .top(Top.of(g1, 5, desc(1)), 1));
            DataSet p2 = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .partitionByHash("label")
                    .select("id", "dept", "label", "created", "amount", "qty")
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{7})
                    .removeFields("tag", "qty")
                    .distinct("dept", "label", "created");
            DataSchema pm2 = p2.getSchema();
            ioCols.out(p2.groupBy(ParallelGroupByAgg.of(pm2, "label").max("amount").min("amount").count()));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept").select(ORDER6);
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept").select(ORDER6);
            DataSet ij = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .join(s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)).on("id").select(ORDER6);
            ioCols.out(lj.union(rj).union(ij).select(0, 1, 4));
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagTyped07", OracleTyped07.compute(w(wideA), w(wideB), o(supp), dimD()));
    }

    // ================= T08: label join with both Date and Timestamp ordering =================
    @Test
    void dagTyped08_labelJoinDualSort() {
        assertSameAcrossRuntimes("dagTyped08", (s, ioCols) -> {
            DataSet j = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .join(s.fromSource(csv(labelDim, TenDagData.LABEL_DIM))).on("label")
                    .filter("amount >= 0")
                    .select("id", "dept", "created", "amount", "weight")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{8})
                    .removeFields("tag", "weight")
                    .partitionByHash("dept");
            DataSchema jm = j.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(jm, "created").sum("amount").getOutputSchema();
            ioCols.out(j.groupBy(ParallelGroupByAgg.of(jm, "created").sum("amount"))
                    .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                    .top(Top.of(g1, 10, desc(1)), 1));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE))
                    .orderBy(OrderBy.of(TenDagData.WIDE, asc(8)), 1, false)
                    .top(Top.of(TenDagData.WIDE, 250, asc(8)), 1)
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "tstamp")
                    .select(0, 1, 4));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagTyped08", OracleTyped08.compute(w(wideA), w(wideB), o(supp), dimD(), labelD()));
    }

    // ================= T09: five sources across mixed types =================
    @Test
    void dagTyped09_fiveSourceMixedTypes() {
        assertSameAcrossRuntimes("dagTyped09", (s, ioCols) -> {
            DataSet j1 = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select(0, 1, 6, 4, 5)
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{9})
                    .removeFields("tag", "qty")
                    .partitionByHash("dept");
            DataSchema m1 = j1.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(m1, "active").sum("amount").getOutputSchema();
            ioCols.out(j1.groupBy(ParallelGroupByAgg.of(m1, "active").sum("amount"))
                    .orderBy(OrderBy.of(g1, asc(0)), 1, false)
                    .top(Top.of(g1, 5, asc(0)), 1));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE))
                    .union(s.fromSource(csv(wideB, TenDagData.WIDE)))
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "created", "label")
                    .filter("amount >= 0")
                    .select(0, 1, 4));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept").select(ORDER6);
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept").select(ORDER6);
            DataSet rjoin = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .join(s.fromSource(csv(regionDim, TenDagData.REGION_DIM))).on("region").select(ORDER6);
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj).union(rjoin)
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
        assertVsOracle("dagTyped09", OracleTyped09.compute(w(wideA), w(wideB), o(supp), dimD(), regionD()));
    }

    // ================= T10: a deep mix over every type =================
    @Test
    void dagTyped10_deepMixedAllTypes() {
        assertSameAcrossRuntimes("dagTyped10", (s, ioCols) -> {
            DataSet chain = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .filter("amount >= 0")
                    .select("id", "active", "created", "grade", "amount", "label")
                    .map(MAP_ID)
                    .join(s.fromSource(csv(labelDim, TenDagData.LABEL_DIM))).on("label")
                    .flatMap(FLATMAP_GRADE)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{10})
                    .removeFields("tag", "weight")
                    .partitionByHash("active");
            DataSchema chm = chain.getSchema();
            ioCols.out(chain.groupBy(ParallelGroupByAgg.of(chm, "created").sum("amount")));
            DataSet cg2 = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSchema cg2m = cg2.getSchema();
            ioCols.out(cg2.orderBy(OrderBy.of(cg2m, desc(1)), 1, false)
                    .top(Top.of(cg2m, 15, desc(1)), 1));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE))
                    .distinct("dept", "active", "created", "tstamp", "label")
                    .select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4));
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet fj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .fullJoin(s.fromSource(csv(dim, TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj).union(fj)
                    .distinct("id", "dept")
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
        });
        assertVsOracle("dagTyped10", OracleTyped10.compute(w(wideA), w(wideB), o(supp), dimD(), labelD()));
    }
}

