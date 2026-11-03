package io.rowweave.cluster;

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
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.BufferedReader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TenDagTopologyTest {

    private static final long BASE = Long.getLong("rowweave.tendag.orders", 20_000L);

    private Path dir;
    private String ordersA, ordersB, ordersC, supp, dim;
    private MasterServer master2, master3;
    private WorkerServer w0, w1, w0b, w1b;    // w0/w1→master2;w0b/w1b→master3

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("tentopo");
        ordersA = TenDagData.orders(dir, "ordersA", BASE);
        ordersB = TenDagData.orders(dir, "ordersB", BASE);
        ordersC = TenDagData.orders(dir, "ordersC", BASE);
        supp = TenDagData.supplement(dir, "supp", Math.max(500, BASE / 4), BASE / 2);
        dim = TenDagData.dim(dir);
        master2 = new MasterServer(5000);
        master3 = new MasterServer(5000);
        File d0 = new File(dir.toFile(), "w0"); d0.mkdirs();
        File d1 = new File(dir.toFile(), "w1"); d1.mkdirs();
        File d0b = new File(dir.toFile(), "w0b"); d0b.mkdirs();
        File d1b = new File(dir.toFile(), "w1b"); d1b.mkdirs();
        w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master2.port(), d0);
        w1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master2.port(), d1);
        w0b = WorkerServer.newUnguarded("w0b", 32, "127.0.0.1", master3.port(), d0b);
        w1b = WorkerServer.newUnguarded("w1b", 32, "127.0.0.1", master3.port(), d1b);
    }

    @AfterAll
    void tearDown() {
        if (w0 != null) w0.close();
        if (w1 != null) w1.close();
        if (w0b != null) w0b.close();
        if (w1b != null) w1b.close();
        if (master2 != null) master2.close();
        if (master3 != null) master3.close();
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

    static final MapFunction MAP_ID = in -> in;
    static final FlatMapFunction FLATMAP_GRADE = (in, c) -> {
        int n = (Integer) in.get(3) + 1;
        for (int i = 0; i < n; i++) c.collect(in);
    };
    static final io.rowweave.CoGroupFunction CG_FN = new io.rowweave.CoGroupFunction() {
        @Override public DataSchema getResultSchema() {
            return new DataSchema(new Field("dept", DataType.IntegerType),
                    new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        }
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

    @FunctionalInterface
    private interface Dag { void build(ExecutionEnvironment s, Io ioCols); }

    private final class Io {
        final String tag; final List<Path> paths = new ArrayList<>(); int used = 0;
        Io(String tag) { this.tag = tag; }
        void out(DataSet ds) {
            Path p = dir.resolve(tag + "-o" + used++ + ".tsv");
            paths.add(p);
            ds.sinkTo(new FileSink(ds.getSchema(), p.toString()));
        }
    }

    private static long[] fingerprint(Path p) {
        long count = 0, sum = 0;
        try (BufferedReader r = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                count++;
                long h = 1125899906842597L;
                for (int i = 0; i < line.length(); i++) h = 31 * h + line.charAt(i);
                sum += h;
            }
        } catch (Exception e) { throw new RuntimeException(e); }
        return new long[]{count, sum};
    }

    private List<long[]> run(String name, int par, io.rowweave.cluster.JobClient client, int nOut, Dag dag) {
        Io ioCols = new Io(name);
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : RowWeave.createEnvironment(name);
        s.setParallelism(par);
        dag.build(s, ioCols);
        s.execute();
        List<long[]> fps = new ArrayList<>();
        for (int i = 0; i < nOut; i++) fps.add(fingerprint(ioCols.paths.get(i)));
        return fps;
    }

    private void assertTopology(String name, int nOut, Dag dag) {
        List<long[]> base = run(name + "-s1", 1, null, nOut, dag);
        assertEquals(nOut, base.size(), name + " output count");
        List<long[]> s2 = run(name + "-s2", 2, null, nOut, dag);
        List<long[]> s4 = run(name + "-s4", 4, null, nOut, dag);
        List<long[]> c2 = run(name + "-c2", 2, new RealClusterJobClient("127.0.0.1", master2.port()), nOut, dag);
        List<long[]> c3 = run(name + "-c3", 3, new RealClusterJobClient("127.0.0.1", master3.port()), nOut, dag);
        for (int i = 0; i < nOut; i++) {
            assertArrayEquals(base.get(i), s2.get(i), name + " out" + i + " single-process P2 versus P1");
            assertArrayEquals(base.get(i), s4.get(i), name + " out" + i + " single-process P4 versus P1");
            assertArrayEquals(base.get(i), c2.get(i), name + " out" + i + " cluster P2 versus single-process P1");
            assertArrayEquals(base.get(i), c3.get(i), name + " out" + i + " cluster P3 versus single-process P1");
        }
        System.out.printf("[topo] %-10s %d outputs agree at 5 parallelisms (single process P1/P2/P4 plus cluster P2/P3)%n", name, nOut);
    }

    private static final DataSchema ORD = TenDagData.ORDER;

    @Test
    void topo01_sourceToOutputDirect() {
        assertTopology("topo01", 1, (s, ioCols) ->
                ioCols.out(s.fromSource(csv(ordersA, ORD))));
    }

    @Test
    void topo02_singleUnary() {
        assertTopology("topo02", 1, (s, ioCols) ->
                ioCols.out(s.fromSource(csv(ordersA, ORD)).filter("amount >= 100").map(MAP_ID)));
    }

    @Test
    void topo03_singleBreaker() {
        assertTopology("topo03", 1, (s, ioCols) ->
                ioCols.out(s.fromSource(csv(ordersA, ORD))
                        .groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount"))));
    }

    @Test
    void topo04_singleBinaryJoin() {
        assertTopology("topo04", 1, (s, ioCols) ->
                ioCols.out(s.fromSource(csv(ordersA, ORD))
                        .join(s.fromSource(csv(ordersB, ORD))).on("id")));
    }

    @Test
    void topo05_unionChain3() {
        assertTopology("topo05", 1, (s, ioCols) ->
                ioCols.out(s.fromSource(csv(ordersA, ORD))
                        .union(s.fromSource(csv(ordersB, ORD)))
                        .union(s.fromSource(csv(ordersC, ORD)))
                        .union(s.fromSource(csv(supp, ORD)))));
    }

    @Test
    void topo06_binaryThenBreaker() {
        assertTopology("topo06", 1, (s, ioCols) -> {
            DataSet j = s.fromSource(csv(ordersA, ORD))
                    .join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema jm = j.getSchema();
            ioCols.out(j.groupBy(ParallelGroupByAgg.of(jm, "dept").sum("amount").count()));
        });
    }

    @Test
    void topo07_diamondFanOutFanIn() {
        assertTopology("topo07", 1, (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
            DataSet left = mid.filter("dept >= 0");
            DataSet right = mid.filter("region >= 0");
            ioCols.out(left.join(right).on("id"));
        });
    }

    @Test
    void topo08_wideFanOut3() {
        assertTopology("topo08", 3, (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount")));  // o1
            ioCols.out(mid.distinct("dept", "region"));                                  // o2
            DataSchema m = mid.getSchema();
            ioCols.out(mid.orderBy(OrderBy.of(m, asc(0)), 1, false).top(Top.of(m, 50, asc(0)), 1)); // o3
        });
    }

    @Test
    void topo09_binaryOfTwoBinaries() {
        assertTopology("topo09", 1, (s, ioCols) -> {
            DataSet joined = s.fromSource(csv(ordersA, ORD))
                    .join(s.fromSource(csv(ordersB, ORD))).on("id")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSet unioned = s.fromSource(csv(ordersC, ORD))
                    .union(s.fromSource(csv(supp, ORD)));
            ioCols.out(joined.union(unioned));
        });
    }

    @Test
    void topo10_coGroupFanIn() {
        assertTopology("topo10", 1, (s, ioCols) ->
                ioCols.out(s.fromSource(csv(ordersA, ORD))
                        .coGroup(s.fromSource(csv(supp, ORD)), CG_FN).on("dept")));
    }

    @Test
    void topo11_alternatingFanBinaryBreaker() {
        assertTopology("topo11", 1, (s, ioCols) -> {
            DataSet j1 = s.fromSource(csv(ordersA, ORD))
                    .join(s.fromSource(csv(ordersB, ORD))).on("id")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSchema j1m = j1.getSchema();
            DataSet g1 = j1.groupBy(ParallelGroupByAgg.of(j1m, "dept").sum("amount")); // [dept, sum]
            DataSet j2 = g1.join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema j2m = j2.getSchema();
            ioCols.out(j2.groupBy(ParallelGroupByAgg.of(j2m, "dept").max(j2m.getField(1).getName())));
        });
    }

    @Test
    void topo12_tripleBinaryChain() {
        assertTopology("topo12", 1, (s, ioCols) -> {
            DataSet u = s.fromSource(csv(ordersA, ORD))
                    .union(s.fromSource(csv(ordersB, ORD)))
                    .filter("amount >= 0");
            DataSet j = u.join(s.fromSource(csv(ordersC, ORD))).on("id")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .map(MAP_ID);
            ioCols.out(j.coGroup(s.fromSource(csv(supp, ORD)), CG_FN).on("dept").filter("lc >= 0"));
        });
    }

    @Test
    void topo13_doubleDiamond() {
        assertTopology("topo13", 1, (s, ioCols) -> {
            DataSet mid1 = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
            DataSet d1 = mid1.filter("dept >= 0")
                    .join(mid1.filter("region >= 0")).on("id")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSet mid2 = d1.map(MAP_ID);
            ioCols.out(mid2.filter("grade >= 0")
                    .join(mid2.filter("qty >= 0")).on("id"));
        });
    }

    @Test
    void topo14_midFeedsBinaryAndOutput() {
        assertTopology("topo14", 2, (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount")));
            ioCols.out(mid.join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept"));
        });
    }

    @Test
    void topo15_coGroupFeedsJoin() {
        assertTopology("topo15", 1, (s, ioCols) -> {
            DataSet cg = s.fromSource(csv(ordersA, ORD))
                    .coGroup(s.fromSource(csv(supp, ORD)), CG_FN).on("dept");  // [dept, lc, rc]
            ioCols.out(cg.join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept"));
        });
    }

    @Test
    void topo16_wideFanOut5() {
        assertTopology("topo16", 5, (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
            DataSchema m = mid.getSchema();
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount")));            // o1
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(ORD, "region").count()));                // o2
            ioCols.out(mid.distinct("dept", "region", "grade"));                                  // o3
            ioCols.out(mid.orderBy(OrderBy.of(m, desc(4)), 1, false).top(Top.of(m, 30, desc(4)), 1)); // o4
            ioCols.out(mid.filter("dept >= 25"));                                                 // o5
        });
    }

    @Test
    void topo17_asymmetricDiamond() {
        assertTopology("topo17", 1, (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(ordersA, ORD)).map(MAP_ID);
            DataSet left = mid.filter("amount >= 0").flatMap(FLATMAP_GRADE).distinct("id", "dept", "region", "grade", "amount", "qty");
            DataSet right = mid.filter("qty >= 0");
            ioCols.out(left.join(right).on("id"));
        });
    }

    @Test
    void topo18_unionMixedBinaryAndSource() {
        assertTopology("topo18", 1, (s, ioCols) -> {
            DataSet j = s.fromSource(csv(ordersA, ORD))
                    .join(s.fromSource(csv(ordersB, ORD))).on("id")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSet plain = s.fromSource(csv(ordersC, ORD));
            ioCols.out(j.union(plain).union(s.fromSource(csv(supp, ORD))));
        });
    }

    @Test
    void topo19_breakerFanOutToTwoBinaries() {
        assertTopology("topo19", 2, (s, ioCols) -> {
            DataSet g = s.fromSource(csv(ordersA, ORD))
                    .groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount"));  // [dept, sum]
            ioCols.out(g.join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept"));
            ioCols.out(g.map(MAP_ID).filter("dept >= 0"));
        });
    }

    @Test
    void topo20_deepMixedDiamondChain() {
        assertTopology("topo20", 1, (s, ioCols) -> {
            DataSet g = s.fromSource(csv(ordersA, ORD))
                    .filter("amount >= 0")
                    .groupBy(ParallelGroupByAgg.of(ORD, "dept").sum("amount"));  // [dept, sum]
            DataSet diamond = g.union(g.filter("dept >= 0"));
            DataSet joined = diamond.join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema jm = joined.getSchema();
            ioCols.out(joined.groupBy(ParallelGroupByAgg.of(jm, "dept").sum(jm.getField(1).getName())));
        });
    }

    @Test
    void topo21_trueSelfJoin() {
        assertTopology("topo21", 1, (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(ordersA, ORD)).filter("amount >= 0").map(MAP_ID);
            ioCols.out(mid.join(mid).on("id"));
        });
    }

    @Test
    void topo22_sourceToTwoSinks() {
        assertTopology("topo22", 2, (s, ioCols) -> {
            DataSet src = s.fromSource(csv(ordersA, ORD));
            ioCols.out(src);
            ioCols.out(src.filter("dept >= 25"));
        });
    }
}

