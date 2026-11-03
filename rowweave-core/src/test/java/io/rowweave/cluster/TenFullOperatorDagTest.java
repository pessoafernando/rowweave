package io.rowweave.cluster;

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
import io.rowweave.ExecutionEnvironment;
import io.rowweave.JoinFunction;
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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TenFullOperatorDagTest {

    private static final long BASE = Long.getLong("rowweave.tendag.orders", 20_000L);

    private Path dir;
    private String ordersA, ordersB, supp, dim, regionDim;
    private MasterServer master;
    private WorkerServer w0, w1;
    private final List<String[]> perf = new ArrayList<>();

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("tenfull");
        ordersA = TenDagData.orders(dir, "ordersA", BASE);
        ordersB = TenDagData.orders(dir, "ordersB", BASE);
        supp = TenDagData.supplement(dir, "supp", Math.max(500, BASE / 4), BASE / 2);
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
        System.out.println("\n====== full-operator DAG performance (orders=" + BASE + ", net ms)======");
        System.out.printf("%-8s | %-9s | %-9s | %-9s%n", "DAG", "procP1", "procP4", "clusP2");
        for (String[] r : perf) {
            System.out.printf("%-8s | %7sms | %7sms | %7sms%n", r[0], r[1], r[2], r[3]);
        }
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

    static final DataSchema RG_OUT = new DataSchema(
            new Field("dept", DataType.IntegerType), new Field("cnt", DataType.IntegerType));
    static final GroupReduceFunction RG_COUNT = new GroupReduceFunction() {
        @Override public DataSchema getResultSchema() { return RG_OUT; }
        @Override public void reduce(Iterable<DataRow> rows, Collector c) {
            int n = 0; Object k = null;
            for (DataRow r : rows) { n++; k = r.get(1); } // dept idx=1
            DataRow o = new DataRow(2); o.set(0, k); o.set(1, n); c.collect(o);
        }
    };

    /** reduceGroup runs over the coGroup output [dept, lc, rc]: for each dept, sum lc into [dept, lcSum]. */
    static final DataSchema RG_LC_OUT = new DataSchema(
            new Field("dept", DataType.IntegerType), new Field("lcSum", DataType.IntegerType));
    static final GroupReduceFunction RG_SUM_LC = new GroupReduceFunction() {
        @Override public DataSchema getResultSchema() { return RG_LC_OUT; }
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

    static final DataSchema CR_OUT = new DataSchema(
            new Field("dept", DataType.IntegerType), new Field("total", DataType.BigDecimalType));
    static final GroupCombineReduceFunction COMBINE_REDUCE_SUM = new GroupCombineReduceFunction() {
        @Override public DataSchema getResultSchema() { return CR_OUT; }
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

    static final DataSchema CG_OUT = new DataSchema(
            new Field("dept", DataType.IntegerType),
            new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
    static final io.rowweave.CoGroupFunction CG_FN = new io.rowweave.CoGroupFunction() {
        @Override public DataSchema getResultSchema() { return CG_OUT; }
        @Override public void coGroup(Iterable<DataRow> l, Iterable<DataRow> r, Collector c) {
            int lc = 0; Object k = null;
            for (DataRow x : l) { lc++; k = x.get(1); }
            int rc = 0;
            for (DataRow x : r) { rc++; if (k == null) k = x.get(1); }
            DataRow o = new DataRow(3); o.set(0, k); o.set(1, lc); o.set(2, rc); c.collect(o);
        }
    };

    static final DataSchema JF_OUT = new DataSchema(
            new Field("dept", DataType.IntegerType), new Field("combined", DataType.BigDecimalType));
    static final JoinFunction JOIN_FN = new JoinFunction() {
        @Override public DataSchema getResultSchema() { return JF_OUT; }
        @Override public DataRow join(DataRow left, DataRow right) {
            DataRow o = new DataRow(2);
            o.set(0, left.get(1)); // dept
            BigDecimal amt = (BigDecimal) left.get(4);
            o.set(1, amt.add(new BigDecimal((Integer) right.get(1)))); // amount + bonus
            return o;
        }
    };

    private static SortField asc(int i) { return new SortField(i, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST); }
    private static SortField desc(int i) { return new SortField(i, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST); }
    private static Field[] f(Field... x) { return x; }

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

    private List<long[]> run(String name, int par, io.rowweave.cluster.JobClient client, int nOut, Dag dag, long[] ms) {
        Io ioCols = new Io(name);
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : RowWeave.createEnvironment(name);
        s.setParallelism(par);
        long t0 = System.nanoTime();
        dag.build(s, ioCols);
        s.execute();
        ms[0] = (System.nanoTime() - t0) / 1_000_000;
        List<long[]> fps = new ArrayList<>();
        for (int i = 0; i < nOut; i++) fps.add(fingerprint(ioCols.paths.get(i)));
        return fps;
    }

    private void assertTriEngine(String name, int nOut, Dag dag) {
        long[] t1 = new long[1], t4 = new long[1], tc = new long[1];
        List<long[]> p1 = run(name + "-s1", 1, null, nOut, dag, t1);
        List<long[]> p4 = run(name + "-s4", 4, null, nOut, dag, t4);
        List<long[]> c2 = run(name + "-c2", 2, new ClusterJobClient("127.0.0.1", master.port()), nOut, dag, tc);
        assertEquals(nOut, p1.size(), name + " output count");
        for (int i = 0; i < nOut; i++) {
            assertArrayEquals(p1.get(i), p4.get(i), name + " out" + i + " single-process P4 versus single-process P1");
            assertArrayEquals(p1.get(i), c2.get(i), name + " out" + i + " cluster P2 versus single-process P1");
        }
        perf.add(new String[]{name, "" + t1[0], "" + t4[0], "" + tc[0]});
        System.out.printf("[full] %-8s %d outputs agree across engines (P1=%dms P4=%dms C2=%dms)%n", name, nOut, t1[0], t4[0], tc[0]);
    }

    // o2 join(inner on id)+ flatMap + distinct;o3 coGroup;o4 union + groupBy(max/min/count);o5 combineReduce + reduceGroup
    // source:ordersA/ordersB/supp/dim(4);output:5
    @Test
    void dagFull01_allOpsLinearBackbone() {
        assertTriEngine("dagFull01", 5, (s, ioCols) -> {
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
                    .groupBy("dept").reduceGroup(RG_SUM_LC)); // reduceGroup: sum lc per dept
            // o4: three outer join kinds nested, plus union and groupBy{max,min,count}. Covers left/right/full/union/max/min/count.
            // leftJoin(A,dim) is fullJoined with dim again (a nested binary), the other arm is rightJoin; both arms share a shape, so union then groupBy.
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");   // [ORDER..,DIM..]
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljMeta = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .groupBy(ParallelGroupByAgg.of(ljMeta, "dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount")); // [dept, sum0]
            DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);                              // [dept, total]
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));         // combineGroup+combineReduceGroup+fullJoin
        });
    }

    // o3:union→partitionByHash→groupBy{max,min,count};o4:leftJoin→flatMap→distinct→select(int);
    @Test
    void dagFull02_joinFirstDeepChain() {
        assertTriEngine("dagFull02", 5, (s, ioCols) -> {
            // o1:coGroup + reduceGroup
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o2:combineGroup + combineReduceGroup + fullJoin
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 3)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER)).combineReduceGroup(COMBINE_REDUCE_SUM, 3);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
            // o3:union → partitionByHash → groupBy{max,min,count}
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .union(s.fromSource(csv(supp, TenDagData.ORDER)))
                    .partitionByHash("region")
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "region").max("amount").min("amount").count()));
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")   // inner join
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")  // [ORDER..,DIM..]
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSet rj = lj
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .filter("amount >= 5")
                    .map(MAP_ID);
            DataSchema rjm = rj.getSchema();
            DataSet proj = rj.addFields(f(new Field("mark", DataType.IntegerType)), new Object[]{9})
                    .removeFields("mark", "qty", "bonus")
                    .select("id", "region", "amount");
            DataSchema projMeta = proj.getSchema();
            DataSchema gsum = ParallelGroupByAgg.of(projMeta, "region").sum("amount").getOutputSchema();
            ioCols.out(proj.groupBy(ParallelGroupByAgg.of(projMeta, "region").sum("amount"))
                    .orderBy(OrderBy.of(gsum, asc(0)), 1, false)
                    .top(Top.of(gsum, 6, asc(0)), 1));
        });
    }

    // o1:mid.groupBy{sum} → orderBy → top;o2:mid.flatMap → distinct → select(int);
    // o3:mid.coGroup(supp) → filter → reduceGroup;o4:leftJoin.union(rightJoin) → groupBy{max,min,count};
    @Test
    void dagFull03_fanOutSharedMid() {
        assertTriEngine("dagFull03", 5, (s, ioCols) -> {
            // o1:filter→select→map→addFields→removeFields→partitionByHash→groupBy.sum→orderBy→top
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
            // o2:inner join → flatMap → distinct → select(int)
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            // o3:coGroup → filter → reduceGroup
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:leftJoin.union(rightJoin) → groupBy{max,min,count}
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            // o5:combineGroup + combineReduceGroup + fullJoin
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // o1:union(A,B,supp) → filter → select → map → addFields → removeFields → partitionByHash → groupBy{max,min,count};
    // o2:union(A,supp).groupBy.sum → orderBy → top;o3:coGroup → filter → reduceGroup;
    @Test
    void dagFull04_unionFirstMultiSource() {
        assertTriEngine("dagFull04", 5, (s, ioCols) -> {
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
            // o2:union.groupBy.sum → orderBy → top
            DataSet u2 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .union(s.fromSource(csv(supp, TenDagData.ORDER)));
            DataSchema g2 = ParallelGroupByAgg.of(TenDagData.ORDER, "region").sum("amount").getOutputSchema();
            ioCols.out(u2.groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "region").sum("amount"))
                    .orderBy(OrderBy.of(g2, asc(0)), 1, false)
                    .top(Top.of(g2, 8, asc(0)), 1));
            // o3:coGroup → filter → reduceGroup
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            ioCols.out(lj.union(rj)
                    .join(s.fromSource(csv(ordersA, TenDagData.ORDER))).on("id")   // inner join on id
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            // o5:combineGroup + combineReduceGroup + fullJoin
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // o3:coGroup → filter → reduceGroup;o4:leftJoin.union(rightJoin) → distinct → groupBy{max,min,count};
    @Test
    void dagFull05_combineFirstReduceClose() {
        assertTriEngine("dagFull05", 5, (s, ioCols) -> {
            // o1:combineReduceGroup [dept,total] → orderBy → top
            DataSet cr1 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            DataSchema crm = cr1.getSchema();
            ioCols.out(cr1.orderBy(OrderBy.of(crm, desc(1)), 1, false)
                    .top(Top.of(crm, 9, desc(1)), 1));
            // o2:combineGroup.groupBy.sum . fullJoin(combineReduceGroup)
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr2 = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr2).on(new String[]{"dept"}, new String[]{"dept"}));
            // o3:coGroup → filter → reduceGroup
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:leftJoin.union(rightJoin) → distinct → groupBy{max,min,count}
            DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .distinct("id", "dept")
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            // o5:inner join → flatMap → filter → map → select → addFields → removeFields → partitionByHash → groupBy.sum
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
    }

    @Test
    void dagFull06_regionJoinOrderFirst() {
        assertTriEngine("dagFull06", 5, (s, ioCols) -> {
            DataSchema om = TenDagData.ORDER;
            ioCols.out(s.fromSource(csv(ordersA, om))
                    .orderBy(OrderBy.of(om, asc(0)), 1, false)
                    .top(Top.of(om, 100, asc(0)), 1)
                    .select(0, 1, 4)
                    .map(MAP_ID));
            // o2:join(regionDim on region) → filter → addFields → removeFields → groupBy.sum → orderBy → top
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
            // o3:coGroup → filter → reduceGroup
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
    }

    // o1:distinct → filter → select → map → addFields → removeFields → partitionByHash → groupBy.sum → orderBy → top;
    // o3:coGroup → filter → reduceGroup;o4:inner join → flatMap → distinct → select(int);
    @Test
    void dagFull07_distinctFirstNestedJoin() {
        assertTriEngine("dagFull07", 5, (s, ioCols) -> {
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
            // o3:coGroup → filter → reduceGroup
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:inner join → flatMap → distinct → select(int)
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            // o5:combineGroup + combineReduceGroup + fullJoin
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // o2:partitionByHash → select → addFields → removeFields → distinct → groupBy{max,min,count};
    // o3:coGroup → filter → reduceGroup;o4:leftJoin.union(rightJoin) → union(inner join) ;
    @Test
    void dagFull08_partitionReshuffleFirst() {
        assertTriEngine("dagFull08", 5, (s, ioCols) -> {
            // o1:partitionByHash(dept) → map → flatMap → filter → groupBy.sum(dept) → orderBy → top
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
            // o2:partitionByHash → select → addFields → removeFields → distinct → groupBy{max,min,count}
            DataSet p2 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .partitionByHash("region")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{8})
                    .removeFields("tag", "qty")
                    .distinct("dept", "region");
            DataSchema pm2 = p2.getSchema();
            ioCols.out(p2.groupBy(ParallelGroupByAgg.of(pm2, "region").max("amount").min("amount").count()));
            // o3:coGroup → filter → reduceGroup
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:leftJoin.union(rightJoin) → union(inner join on id) → select(int)
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
            // o5:combineGroup + combineReduceGroup + fullJoin
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // o1:orders.join(dim,inner).select.map.addFields.removeFields.partitionByHash.groupBy.sum.orderBy.top;
    // o2:orders.union(supp).flatMap.distinct.filter.select(int);o3:orders.coGroup(supp).filter.reduceGroup;
    // o4:leftJoin(dim).union(rightJoin(dim)).union(fullJoin(regionDim... via region)).groupBy{max,min,count};
    @Test
    void dagFull09_fiveSourceMultiOut() {
        assertTriEngine("dagFull09", 5, (s, ioCols) -> {
            // o1:inner join(dim) → select → map → addFields → removeFields → partitionByHash → groupBy.sum → orderBy → top
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
            // o2:union(supp) → flatMap → distinct → filter → select(int)
            ioCols.out(s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .union(s.fromSource(csv(supp, TenDagData.ORDER)))
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .filter("amount >= 0")
                    .select(0, 1, 4));
            // o3:coGroup → filter → reduceGroup
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
            // o5:combineGroup + combineReduceGroup + fullJoin
            DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER))
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    @Test
    void dagFull10_deepMixedGrandChain() {
        assertTriEngine("dagFull10", 5, (s, ioCols) -> {
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
            // o3:coGroup → filter → reduceGroup
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
    }
}

