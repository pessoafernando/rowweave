package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataSet;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.bench.CsvInput;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
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
class TenComplexDagTest {

    private static final long BASE = Long.getLong("rowweave.tendag.orders", 50_000L);

    private Path dataDir;
    private String ordersBig;
    private String ordersMid;
    private String supp;
    private String dim;
    private String regionDim;

    private MasterServer master;
    private WorkerServer w0, w1;
    private final List<String[]> perf = new ArrayList<>();

    @BeforeAll
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("tendag");
        ordersBig = TenDagData.orders(dataDir, "orders_big", BASE);
        ordersMid = TenDagData.orders(dataDir, "orders_mid", Math.max(1000, BASE / 5));
        supp = TenDagData.supplement(dataDir, "supp", Math.max(500, BASE / 10), BASE / 2);
        dim = TenDagData.dim(dataDir);
        regionDim = TenDagData.regionDim(dataDir);
        master = new MasterServer(5000);
        File d0 = new File(dataDir.toFile(), "w0"); d0.mkdirs();
        File d1 = new File(dataDir.toFile(), "w1"); d1.mkdirs();
        w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), d0);
        w1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), d1);
    }

    @AfterAll
    void tearDown() {
        if (w0 != null) w0.close();
        if (w1 != null) w1.close();
        if (master != null) master.close();
        System.out.println("\n====== TENDAG performance (orders=" + BASE + ", net ms)======");
        System.out.printf("%-8s | %-9s | %-9s | %-9s%n", "DAG", "procP1", "procP4", "clusP2");
        for (String[] r : perf) {
            System.out.printf("%-8s | %7sms | %7sms | %7sms%n", r[0], r[1], r[2], r[3]);
        }
        System.out.println("=========================================");
    }

    private CsvInput csv(String path, DataSchema meta) {
        return new CsvInput(path, meta);
    }

    @FunctionalInterface
    private interface Dag {
        void build(BatchSession s, IoCollector ioCols);
    }

    private final class IoCollector {
        final String tag;
        final List<Path> paths = new ArrayList<>();
        int used = 0;
        IoCollector(String tag) { this.tag = tag; }
        void out(DataSet ds) {
            Path f = dataDir.resolve(tag + "-o" + used + ".tsv");
            paths.add(f);
            used++;
            ds.sinkTo(new FileSink(ds.getSchema(), f.toString()));
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

    private List<long[]> runQuad(String name, int parallelism, io.rowweave.cluster.JobClient client, int nOut, Dag dag, long[] elapsedMs) {
        IoCollector ioCols = new IoCollector(name);
        BatchSession s = client != null ? new BatchSession(name, name, client) : RowWeave.createBatchSession(name);
        s.setParallelism(parallelism);
        long t0 = System.nanoTime();
        dag.build(s, ioCols);
        s.execute();
        elapsedMs[0] = (System.nanoTime() - t0) / 1_000_000;
        List<long[]> fps = new ArrayList<>();
        for (int i = 0; i < nOut; i++) fps.add(fingerprint(ioCols.paths.get(i)));
        return fps;
    }

    private void assertTriEngine(String name, int nOut, Dag dag) {
        long[] tP1 = new long[1], tP4 = new long[1], tC = new long[1];
        List<long[]> p1 = runQuad(name + "-s1", 1, null, nOut, dag, tP1);
        List<long[]> p4 = runQuad(name + "-s4", 4, null, nOut, dag, tP4);
        List<long[]> c2 = runQuad(name + "-c2", 2, new ClusterJobClient("127.0.0.1", master.port()), nOut, dag, tC);
        assertEquals(nOut, p1.size(), name + " output count");
        for (int i = 0; i < nOut; i++) {
            assertArrayEquals(p1.get(i), p4.get(i), name + " out" + i + " single-process P4 versus single-process P1");
            assertArrayEquals(p1.get(i), c2.get(i), name + " out" + i + " cluster P2 versus single-process P1");
        }
        perf.add(new String[]{name, Long.toString(tP1[0]), Long.toString(tP4[0]), Long.toString(tC[0])});
        System.out.printf("[tendag] %-8s %d outputs agree across engines (P1=%dms P4=%dms C2=%dms)%n", name, nOut, tP1[0], tP4[0], tC[0]);
    }

    private static SortField desc(int i) { return new SortField(i, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST); }
    private static SortField asc(int i) { return new SortField(i, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST); }

    @Test
    void dag01_linearChain() {
        DataSchema joinMeta = io.rowweave.ops.join.SortMergeJoin.of(
                TenDagData.ORDER, TenDagData.DIM, new String[]{"dept"}, new String[]{"dept"},
                io.rowweave.ops.join.JoinType.INNER).getOutputSchema();
        DataSchema gbMeta = ParallelGroupByAgg.of(joinMeta, "region").sum("amount").getOutputSchema();
        assertTriEngine("dag01", 2, (s, ioCols) -> {
            DataSet joined = s.fromSource(csv(ordersBig, TenDagData.ORDER))
                    .filter("amount >= 100")
                    .join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet g = joined.groupBy(ParallelGroupByAgg.of(joinMeta, "region").sum("amount"));
            ioCols.out(g);
            DataSet top = g.orderBy(io.rowweave.ops.OrderBy.of(gbMeta, desc(1)), 1, false)
                    .top(io.rowweave.ops.Top.of(gbMeta, 5, desc(1)), 1);
            ioCols.out(top);
        });
    }

    // ============ dag03:fan-out 3 output 2 source ============
    // mid=orders.filter;mid.groupBy.count→o1;mid.distinct→o2;mid.top(int)→o3
    @Test
    void dag03_fanoutThree() {
        assertTriEngine("dag03", 3, (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(ordersMid, TenDagData.ORDER)).filter("qty >= 2");
            ioCols.out(mid.groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").count()));
            ioCols.out(mid.distinct("dept", "region"));
            ioCols.out(mid.orderBy("id").top(20));
        });
    }

    // src.filter→tee:(b1.filter join b2.filter, inner ON id)→addFields→o1;src2.distinct→o2
    @Test
    void dag02_diamondRejoin() {
        DataSchema joinMeta = io.rowweave.ops.join.SortMergeJoin.of(
                TenDagData.ORDER, TenDagData.ORDER, new String[]{"id"}, new String[]{"id"},
                io.rowweave.ops.join.JoinType.INNER).getOutputSchema();
        io.rowweave.Field[] addF = {new io.rowweave.Field("tag", io.rowweave.DataType.IntegerType)};
        Object[] addV = {7};
        assertTriEngine("dag02", 2, (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(ordersMid, TenDagData.ORDER)).filter("amount >= 5");
            DataSet b1 = mid.filter("dept >= 2");
            DataSet b2 = s.fromSource(csv(ordersMid, TenDagData.ORDER)).filter("amount >= 5").filter("dept <= 40");
            DataSet joined = b1.join(b2).on("id");
            ioCols.out(joined.addFields(addF, addV));
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER)).distinct("dept", "grade"));
        });
    }

    @Test
    void dag04_unionMultiSource() {
        assertTriEngine("dag04", 2, (s, ioCols) -> {
            DataSet u = s.fromSource(csv(ordersMid, TenDagData.ORDER))
                    .union(s.fromSource(csv(supp, TenDagData.ORDER)))
                    .union(s.fromSource(csv(ordersBig, TenDagData.ORDER)).filter("dept >= 45"));
            ioCols.out(u.groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "region").max("amount").min("amount")));
            DataSet u2 = s.fromSource(csv(ordersMid, TenDagData.ORDER))
                    .union(s.fromSource(csv(supp, TenDagData.ORDER)));
            ioCols.out(u2.leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept"));
        });
    }

    // ============ dag05:coGroup 2 source 2 output ============
    // orders.coGroup(supp).filter.reduceGroup→o1;orders.groupBySum→o2
    private static final DataSchema CG_OUT = new DataSchema(
            new io.rowweave.Field("dept", io.rowweave.DataType.IntegerType),
            new io.rowweave.Field("lc", io.rowweave.DataType.IntegerType),
            new io.rowweave.Field("rc", io.rowweave.DataType.IntegerType));

    private static final io.rowweave.CoGroupFunction CG_FN = new io.rowweave.CoGroupFunction() {
        @Override public DataSchema getResultSchema() { return CG_OUT; }
        @Override public void coGroup(Iterable<DataRow> l, Iterable<DataRow> r, io.rowweave.Collector c) {
            int lc = 0; Object k = null;
            for (DataRow x : l) { lc++; k = x.get(1); }
            int rc = 0;
            for (DataRow x : r) { rc++; if (k == null) k = x.get(1); }
            DataRow o = new DataRow(3); o.set(0, k); o.set(1, lc); o.set(2, rc); c.collect(o);
        }
    };

    @Test
    void dag05_coGroup() {
        assertTriEngine("dag05", 2, (s, ioCols) -> {
            DataSet cg = s.fromSource(csv(ordersMid, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept");
            ioCols.out(cg.filter("lc >= 1"));
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER)).groupBySum("amount", "region"));
        });
    }

    private static final io.rowweave.GroupCombineFunction COMBINE_SUM = (rows, c) -> {
        java.util.LinkedHashMap<Integer, DataRow> byDept = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<Integer, java.math.BigDecimal> sum = new java.util.LinkedHashMap<>();
        for (DataRow r : rows) {
            int dept = (Integer) r.get(1);
            byDept.putIfAbsent(dept, r);
            sum.merge(dept, (java.math.BigDecimal) r.get(4), java.math.BigDecimal::add);
        }
        for (java.util.Map.Entry<Integer, DataRow> e : byDept.entrySet()) {
            DataRow src = e.getValue();
            DataRow o = new DataRow(6);
            o.set(0, src.get(0)); o.set(1, e.getKey()); o.set(2, src.get(2));
            o.set(3, src.get(3)); o.set(4, sum.get(e.getKey())); o.set(5, src.get(5));
            c.collect(o);
        }
    };

    private static final DataSchema CR_OUT = new DataSchema(
            new io.rowweave.Field("dept", io.rowweave.DataType.IntegerType),
            new io.rowweave.Field("total", io.rowweave.DataType.BigDecimalType));
    private static final io.rowweave.GroupCombineReduceFunction COMBINE_REDUCE_SUM = new io.rowweave.GroupCombineReduceFunction() {
        @Override public DataSchema getResultSchema() { return CR_OUT; }
        @Override public void combine(Iterable<DataRow> rows, io.rowweave.Collector c) {
            COMBINE_SUM.combine(rows, c);
        }
        @Override public void reduce(Iterable<DataRow> rows, io.rowweave.Collector c) {
            java.util.TreeMap<Integer, java.math.BigDecimal> sum = new java.util.TreeMap<>();
            for (DataRow r : rows) sum.merge((Integer) r.get(1), (java.math.BigDecimal) r.get(4), java.math.BigDecimal::add);
            for (java.util.Map.Entry<Integer, java.math.BigDecimal> e : sum.entrySet()) {
                DataRow o = new DataRow(2); o.set(0, e.getKey());
                o.set(1, e.getValue().setScale(10, java.math.RoundingMode.HALF_UP)); c.collect(o);
            }
        }
    };

    private static final io.rowweave.FlatMapFunction EXPAND_BY_GRADE = (in, c) -> {
        int n = (Integer) in.get(3) + 1;
        for (int i = 0; i < n; i++) c.collect(in);
    };

    @Test
    void dag06_combineChain() {
        DataSchema gbMeta = ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount").getOutputSchema();
        assertTriEngine("dag06", 3, (s, ioCols) -> {
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount")));
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER)).combineReduceGroup(COMBINE_REDUCE_SUM, 4));
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER)).filter("grade >= 3").flatMap(EXPAND_BY_GRADE));
        });
    }

    // orders.leftJoin(dim)→o1;orders.rightJoin(dim)→o2;orders.fullJoin(regionDim)→o3
    @Test
    void dag07_outerJoins() {
        assertTriEngine("dag07", 3, (s, ioCols) -> {
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept"));
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept"));
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER))
                    .fullJoin(s.fromSource(csv(regionDim, TenDagData.REGION_DIM))).on("region"));
        });
    }

    // ============ dag08:partitionByHash 2 source 2 output ============
    @Test
    void dag08_partitionByHash() {
        assertTriEngine("dag08", 2, (s, ioCols) -> {
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER))
                    .partitionByHash("dept")
                    .flatMap(EXPAND_BY_GRADE)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount")));
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER)).removeFields("grade", "qty"));
        });
    }

    // orders.join(dim).groupBy.sum→o1;(orders.union(supp)).distinct→o2;regionDim.map.addFields→o3
    private static final io.rowweave.MapFunction UPPER_RNAME = in -> {
        DataRow o = new DataRow(2); o.set(0, in.get(0));
        o.set(1, ((String) in.get(1)).toUpperCase()); return o;
    };
    @Test
    void dag09_multiSourceMultiOut() {
        DataSchema joinMeta = io.rowweave.ops.join.SortMergeJoin.of(
                TenDagData.ORDER, TenDagData.DIM, new String[]{"dept"}, new String[]{"dept"},
                io.rowweave.ops.join.JoinType.INNER).getOutputSchema();
        io.rowweave.Field[] addF = {new io.rowweave.Field("flag", io.rowweave.DataType.IntegerType)};
        assertTriEngine("dag09", 3, (s, ioCols) -> {
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER))
                    .join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .groupBy(ParallelGroupByAgg.of(joinMeta, "region").sum("amount")));
            ioCols.out(s.fromSource(csv(ordersMid, TenDagData.ORDER))
                    .union(s.fromSource(csv(supp, TenDagData.ORDER)))
                    .distinct("dept", "region", "grade"));
            ioCols.out(s.fromSource(csv(regionDim, TenDagData.REGION_DIM))
                    .map(UPPER_RNAME).addFields(addF, new Object[]{1}));
        });
    }

    @Test
    void dag10_deepMix() {
        DataSchema joinMeta = io.rowweave.ops.join.SortMergeJoin.of(
                TenDagData.ORDER, TenDagData.DIM, new String[]{"dept"}, new String[]{"dept"},
                io.rowweave.ops.join.JoinType.INNER).getOutputSchema();
        DataSchema jgb = ParallelGroupByAgg.of(joinMeta, "region").sum("amount").getOutputSchema();
        assertTriEngine("dag10", 4, (s, ioCols) -> {
            DataSet g = s.fromSource(csv(ordersBig, TenDagData.ORDER))
                    .filter("amount >= 50").map(u -> u)
                    .join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .groupBy(ParallelGroupByAgg.of(joinMeta, "region").sum("amount"));
            ioCols.out(g.orderBy(io.rowweave.ops.OrderBy.of(jgb, desc(1)), 1, false)
                    .top(io.rowweave.ops.Top.of(jgb, 5, desc(1)), 1));
            // o2:coGroup
            ioCols.out(s.fromSource(csv(ordersBig, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept"));
            ioCols.out(s.fromSource(csv(ordersBig, TenDagData.ORDER)).combineReduceGroup(COMBINE_REDUCE_SUM, 4));
            // o4:distinct→removeFields
            ioCols.out(s.fromSource(csv(ordersBig, TenDagData.ORDER)).distinct("dept", "grade").removeFields("qty"));
        });
    }
}

