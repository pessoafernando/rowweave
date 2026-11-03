package io.rowweave.equivalence;

import io.rowweave.RowWeave;
import io.rowweave.Collector;
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
class ThreeWayTopoEquivalenceTest {

    private static final long N = Long.getLong("rowweave.equivalence.rows", 200L);
    private static final DataSchema ORD = TenDagData.ORDER;

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
    private static SortField asc(int i) { return new SortField(i, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST); }
    private static SortField desc(int i) { return new SortField(i, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST); }

    private Path dir;
    private String ordersA, ordersB, ordersC, supp, dim;
    private MasterServer master;
    private WorkerServer w0, w1;

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("equivalence-topo");
        ordersA = TenDagData.orders(dir, "ordersA", N);
        ordersB = TenDagData.orders(dir, "ordersB", N);
        ordersC = TenDagData.orders(dir, "ordersC", N);
        supp = TenDagData.supplement(dir, "supp", N / 2, N / 2);
        dim = TenDagData.dim(dir);
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

    private List<List<DataRow>> lastV2;
    private List<DataSchema> lastMetas;

    private void assertSameAcrossRuntimes(String name, Dag dag) {
        List<DataSchema> m1 = new ArrayList<>(), m4 = new ArrayList<>(), mc = new ArrayList<>(), mlc = new ArrayList<>();
        List<List<DataRow>> p1 = run(name + "-s1", 1, null, dag, m1);
        List<List<DataRow>> p4 = run(name + "-s4", 4, null, dag, m4);
        List<List<DataRow>> c2 = run(name + "-c2", 2, new ClusterJobClient("127.0.0.1", master.port()), dag, mc);
        List<List<DataRow>> lc4;  // LocalCluster P4: one worker, so the whole data plane is local, and it agrees with the other two runs
        // The constructor argument is only a slots hint; the graph still runs at P=4, and 128 slots are reserved for the full multi-root dag.
        try (InProcessCluster lc = new InProcessCluster(8, new File(dir.toFile(), name + "-lc"))) {
            lc4 = run(name + "-lc4", 4, lc, dag, mlc);
        }
        assertEquals(p1.size(), c2.size(), name + " output counts match");
        assertEquals(p1.size(), lc4.size(), name + " local cluster output counts match");
        for (int i = 0; i < p1.size(); i++) {
            DataSchema meta = m1.get(i);
            assertNull(RowSetComparator.diff(meta, p1.get(i), p4.get(i)), name + " out" + i + " P1 vs P4");
            assertNull(RowSetComparator.diff(meta, p1.get(i), c2.get(i)), name + " out" + i + " parallelism 1 versus cluster parallelism 2");
            assertNull(RowSetComparator.diff(meta, p1.get(i), lc4.get(i)), name + " out" + i + " P1 vs LocalCluster P4");
        }
        for (int i = 0; i < m1.size(); i++) {
            StringBuilder sb = new StringBuilder();
            DataSchema meta = m1.get(i);
            for (int j = 0; j < meta.getFieldCount(); j++) {
                if (j > 0) sb.append(", ");
                sb.append(meta.getField(j).getName()).append(':').append(meta.getField(j).getDataType());
            }
            System.out.printf("[xtopo] %s o%d rows=%d meta=[%s]%n", name, i, p1.get(i).size(), sb);
        }
        lastV2 = p1;
        lastMetas = m1;
    }

    private List<Object[]> o(String path) { return readCsv(path, ORD); }
    private List<Object[]> dimD() { return readCsv(dim, TenDagData.DIM); }

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

    @Test void topo01() { assertSameAcrossRuntimes("topo01", (s, ioCols) -> ioCols.out(s.fromSource(csv(ordersA, ORD))));
        assertVsOracle("topo01", OracleTopo.topo01(o(ordersA))); }

    @Test void topo02() { assertSameAcrossRuntimes("topo02", (s, ioCols) -> ioCols.out(s.fromSource(csv(ordersA, ORD)).filter("amount >= 100").map(MAP_ID)));
        assertVsOracle("topo02", OracleTopo.topo02(o(ordersA))); }

    @Test void topo03() { assertSameAcrossRuntimes("topo03", (s, ioCols) -> ioCols.out(s.fromSource(csv(ordersA, ORD)).groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount"))));
        assertVsOracle("topo03", OracleTopo.topo03(o(ordersA))); }

    @Test void topo04() { assertSameAcrossRuntimes("topo04", (s, ioCols) -> ioCols.out(s.fromSource(csv(ordersA, ORD)).join(s.fromSource(csv(ordersB, ORD))).on("id")));
        assertVsOracle("topo04", OracleTopo.topo04(o(ordersA), o(ordersB))); }

    @Test void topo05() { assertSameAcrossRuntimes("topo05", (s, ioCols) -> ioCols.out(s.fromSource(csv(ordersA, ORD)).union(s.fromSource(csv(ordersB, ORD))).union(s.fromSource(csv(ordersC, ORD))).union(s.fromSource(csv(supp, ORD)))));
        assertVsOracle("topo05", OracleTopo.topo05(o(ordersA), o(ordersB), o(ordersC), o(supp))); }

    @Test void topo06() { assertSameAcrossRuntimes("topo06", (s, ioCols) -> {
        DataSet j = s.fromSource(csv(ordersA, ORD)).join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        ioCols.out(j.groupBy(ParallelGroupByAgg.of(j.getSchema(), "dept").sum("amount").count())); });
        assertVsOracle("topo06", OracleTopo.topo06(o(ordersA), dimD())); }

    @Test void topo07() { assertSameAcrossRuntimes("topo07", (s, ioCols) -> {
        DataSet mid = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
        ioCols.out(mid.filter("dept >= 0").join(mid.filter("region >= 0")).on("id")); });
        assertVsOracle("topo07", OracleTopo.topo07(o(ordersA))); }

    @Test void topo08() { assertSameAcrossRuntimes("topo08", (s, ioCols) -> {
        DataSet mid = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
        ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount")));
        ioCols.out(mid.distinct("dept", "region"));
        DataSchema m = mid.getSchema();
        ioCols.out(mid.orderBy(OrderBy.of(m, asc(0)), 1, false).top(Top.of(m, 50, asc(0)), 1)); });
        assertVsOracle("topo08", OracleTopo.topo08(o(ordersA))); }

    @Test void topo09() { assertSameAcrossRuntimes("topo09", (s, ioCols) -> {
        DataSet joined = s.fromSource(csv(ordersA, ORD)).join(s.fromSource(csv(ordersB, ORD))).on("id").select("id", "dept", "region", "grade", "amount", "qty");
        DataSet unioned = s.fromSource(csv(ordersC, ORD)).union(s.fromSource(csv(supp, ORD)));
        ioCols.out(joined.union(unioned)); });
        assertVsOracle("topo09", OracleTopo.topo09(o(ordersA), o(ordersB), o(ordersC), o(supp))); }

    @Test void topo10() { assertSameAcrossRuntimes("topo10", (s, ioCols) -> ioCols.out(s.fromSource(csv(ordersA, ORD)).coGroup(s.fromSource(csv(supp, ORD)), CG_FN).on("dept")));
        assertVsOracle("topo10", OracleTopo.topo10(o(ordersA), o(supp))); }

    @Test void topo11() { assertSameAcrossRuntimes("topo11", (s, ioCols) -> {
        DataSet j1 = s.fromSource(csv(ordersA, ORD)).join(s.fromSource(csv(ordersB, ORD))).on("id").select("id", "dept", "region", "grade", "amount", "qty");
        DataSet g1 = j1.groupBy(ParallelGroupByAgg.of(j1.getSchema(), "dept").sum("amount"));
        DataSet j2 = g1.join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSchema j2m = j2.getSchema();
        ioCols.out(j2.groupBy(ParallelGroupByAgg.of(j2m, "dept").max(j2m.getField(1).getName()))); });
        assertVsOracle("topo11", OracleTopo.topo11(o(ordersA), o(ordersB), dimD())); }

    @Test void topo12() { assertSameAcrossRuntimes("topo12", (s, ioCols) -> {
        DataSet u = s.fromSource(csv(ordersA, ORD)).union(s.fromSource(csv(ordersB, ORD))).filter("amount >= 0");
        DataSet j = u.join(s.fromSource(csv(ordersC, ORD))).on("id").select("id", "dept", "region", "grade", "amount", "qty").map(MAP_ID);
        ioCols.out(j.coGroup(s.fromSource(csv(supp, ORD)), CG_FN).on("dept").filter("lc >= 0")); });
        assertVsOracle("topo12", OracleTopo.topo12(o(ordersA), o(ordersB), o(ordersC), o(supp))); }

    @Test void topo13() { assertSameAcrossRuntimes("topo13", (s, ioCols) -> {
        DataSet mid1 = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
        DataSet d1 = mid1.filter("dept >= 0").join(mid1.filter("region >= 0")).on("id").select("id", "dept", "region", "grade", "amount", "qty");
        DataSet mid2 = d1.map(MAP_ID);
        ioCols.out(mid2.filter("grade >= 0").join(mid2.filter("qty >= 0")).on("id")); });
        assertVsOracle("topo13", OracleTopo.topo13(o(ordersA))); }

    @Test void topo14() { assertSameAcrossRuntimes("topo14", (s, ioCols) -> {
        DataSet mid = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
        ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount")));
        ioCols.out(mid.join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")); });
        assertVsOracle("topo14", OracleTopo.topo14(o(ordersA), dimD())); }

    @Test void topo15() { assertSameAcrossRuntimes("topo15", (s, ioCols) -> {
        DataSet cg = s.fromSource(csv(ordersA, ORD)).coGroup(s.fromSource(csv(supp, ORD)), CG_FN).on("dept");
        ioCols.out(cg.join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")); });
        assertVsOracle("topo15", OracleTopo.topo15(o(ordersA), o(supp), dimD())); }

    @Test void topo16() { assertSameAcrossRuntimes("topo16", (s, ioCols) -> {
        DataSet mid = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
        DataSchema m = mid.getSchema();
        ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount")));
        ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "region").count()));
        ioCols.out(mid.distinct("dept", "region", "grade"));
        ioCols.out(mid.orderBy(OrderBy.of(m, desc(4)), 1, false).top(Top.of(m, 30, desc(4)), 1));
        ioCols.out(mid.filter("dept >= 25")); });
        assertVsOracle("topo16", OracleTopo.topo16(o(ordersA))); }

    @Test void topo17() { assertSameAcrossRuntimes("topo17", (s, ioCols) -> {
        DataSet mid = s.fromSource(csv(ordersA, ORD)).map(MAP_ID);
        DataSet left = mid.filter("amount >= 0").flatMap(FLATMAP_GRADE).distinct("id", "dept", "region", "grade", "amount", "qty");
        DataSet right = mid.filter("qty >= 0");
        ioCols.out(left.join(right).on("id")); });
        assertVsOracle("topo17", OracleTopo.topo17(o(ordersA))); }

    @Test void topo18() { assertSameAcrossRuntimes("topo18", (s, ioCols) -> {
        DataSet j = s.fromSource(csv(ordersA, ORD)).join(s.fromSource(csv(ordersB, ORD))).on("id").select("id", "dept", "region", "grade", "amount", "qty");
        ioCols.out(j.union(s.fromSource(csv(ordersC, ORD))).union(s.fromSource(csv(supp, ORD)))); });
        assertVsOracle("topo18", OracleTopo.topo18(o(ordersA), o(ordersB), o(ordersC), o(supp))); }

    @Test void topo19() { assertSameAcrossRuntimes("topo19", (s, ioCols) -> {
        DataSet g = s.fromSource(csv(ordersA, ORD)).groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount"));
        ioCols.out(g.join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept"));
        ioCols.out(g.map(MAP_ID).filter("dept >= 0")); });
        assertVsOracle("topo19", OracleTopo.topo19(o(ordersA), dimD())); }

    @Test void topo20() { assertSameAcrossRuntimes("topo20", (s, ioCols) -> {
        DataSet g = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount"));
        DataSet diamond = g.union(g.filter("dept >= 0"));
        DataSet joined = diamond.join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSchema jm = joined.getSchema();
        ioCols.out(joined.groupBy(ParallelGroupByAgg.of(jm, "dept").sum(jm.getField(1).getName()))); });
        assertVsOracle("topo20", OracleTopo.topo20(o(ordersA), dimD())); }

    @Test void topo21() { assertSameAcrossRuntimes("topo21", (s, ioCols) -> {
        DataSet mid = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
        ioCols.out(mid.join(mid).on("id")); });
        assertVsOracle("topo21", OracleTopo.topo21(o(ordersA))); }

    @Test void topo22() { assertSameAcrossRuntimes("topo22", (s, ioCols) -> {
        DataSet src = s.fromSource(csv(ordersA, ORD));
        ioCols.out(src);
        ioCols.out(src.filter("dept >= 25")); });
        assertVsOracle("topo22", OracleTopo.topo22(o(ordersA))); }
}

