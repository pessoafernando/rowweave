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
class TenFullOperatorDagTypesTest {

    private static final long BASE = Long.getLong("rowweave.tendag.orders", 20_000L);

    private Path dir;
    private String wideA, wideB, supp, dim, regionDim, labelDim;
    private MasterServer master;
    private WorkerServer w0, w1;
    private final List<String[]> perf = new ArrayList<>();

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("tentypes");
        wideA = TenDagData.wide(dir, "wideA", BASE);
        wideB = TenDagData.wide(dir, "wideB", BASE);
        supp = TenDagData.supplement(dir, "supp", Math.max(500, BASE / 4), BASE / 2);
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
        System.out.println("\n====== full-operator-type DAG performance (orders=" + BASE + ", net ms)======");
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

    /** reduceGroup over the coGroup output [dept, lc, rc]: for each dept, sum lc into [dept, lcSum]. */
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

    private static final String[] ORDER6 = {"id", "dept", "region", "grade", "amount", "qty"};

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
        List<long[]> c2 = run(name + "-c2", 2, new RealClusterJobClient("127.0.0.1", master.port()), nOut, dag, tc);
        assertEquals(nOut, p1.size(), name + " output count");
        for (int i = 0; i < nOut; i++) {
            assertArrayEquals(p1.get(i), p4.get(i), name + " out" + i + " single-process P4 versus single-process P1");
            assertArrayEquals(p1.get(i), c2.get(i), name + " out" + i + " cluster P2 versus single-process P1");
        }
        perf.add(new String[]{name, "" + t1[0], "" + t4[0], "" + tc[0]});
        System.out.printf("[types] %-8s %d outputs agree across engines (P1=%dms P4=%dms C2=%dms)%n", name, nOut, t1[0], t4[0], tc[0]);
    }

    // o2:inner join(id)→flatMap→distinct→select(int);o3:coGroup→filter→reduceGroup;
    @Test
    void dagTyped01_wideBoolGroupLabelPartition() {
        assertTriEngine("dagTyped01", 5, (s, ioCols) -> {
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
            // o2:inner join(id)→flatMap→distinct→select(int)
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .join(s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            // o3:coGroup→filter→reduceGroup
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:leftJoin.union(rightJoin)→groupBy{max,min,count}
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
    }

    // o1:join(labelDim on label:String)→filter→select→map→addFields→removeFields→partitionByHash→groupBy(created:Date).sum→orderBy→top;
    @Test
    void dagTyped02_labelJoinDateGroup() {
        assertTriEngine("dagTyped02", 5, (s, ioCols) -> {
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
            // o3:coGroup→filter→reduceGroup
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
            // o5:combineGroup+combineReduceGroup+fullJoin
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // mid = wideA.filter.select.map.partitionByHash;o1:mid.orderBy(tstamp:Timestamp).top.flatMap.select(int);
    // o2:mid.groupBy(active:Boolean).count.distinct;o3:coGroup.filter.reduceGroup;
    // o4:inner join(id).flatMap.distinct.select + leftJoin.union(rightJoin).groupBy{max,min,count};
    @Test
    void dagTyped03_tsSortBoolDistinctFanout() {
        assertTriEngine("dagTyped03", 5, (s, ioCols) -> {
            DataSet mid = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .filter("amount >= 0")
                    .select("id", "dept", "active", "grade", "tstamp", "amount")
                    .map(MAP_ID)
                    .partitionByHash("dept");
            // o1:orderBy(tstamp Timestamp)→top→flatMap→select(int)
            DataSchema mm = mid.getSchema();
            ioCols.out(mid.orderBy(OrderBy.of(mm, asc(4), asc(0)), 1, false)
                    .top(Top.of(mm, 150, asc(4), asc(0)), 1)
                    .flatMap(FLATMAP_GRADE)
                    .select(0, 1, 5));
            // o2:groupBy(active Boolean){max,min,count} → distinct → addFields → removeFields
            DataSet g2 = mid.groupBy(ParallelGroupByAgg.of(mm, "active").max("amount").min("amount").count());
            DataSchema g2m = g2.getSchema();
            ioCols.out(g2.distinct(g2m.getField(0).getName(), g2m.getField(1).getName())
                    .addFields(f(new Field("mk", DataType.IntegerType)), new Object[]{3})
                    .removeFields("mk"));
            // o3:coGroup→filter→reduceGroup
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:inner join(id)→flatMap→distinct→select + leftJoin.union(rightJoin)→groupBy{max,min,count}
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .join(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)).on("id")  // inner join on id
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            // o5:combineGroup+combineReduceGroup+fullJoin
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // o1:combineReduceGroup→orderBy→top;o2:combineGroup.groupBy.sum.fullJoin(cr);
    // o3:coGroup.filter.reduceGroup;o4:leftJoin.union(rightJoin).distinct.groupBy{max,min,count};
    @Test
    void dagTyped04_combineFirstLabelGroup() {
        assertTriEngine("dagTyped04", 5, (s, ioCols) -> {
            // o1:combineReduceGroup [dept,total] → orderBy → top
            DataSet cr1 = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            DataSchema crm = cr1.getSchema();
            ioCols.out(cr1.orderBy(OrderBy.of(crm, desc(1)), 1, false)
                    .top(Top.of(crm, 9, desc(1)), 1));
            // o2:combineGroup.groupBy.sum . fullJoin(combineReduceGroup)
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr2 = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr2).on(new String[]{"dept"}, new String[]{"dept"}));
            // o3:coGroup→filter→reduceGroup
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:leftJoin.union(rightJoin)→distinct→groupBy{max,min,count}
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
                    .partitionByHash("active");           // Boolean partition key
            DataSchema chm = chain.getSchema();
            ioCols.out(chain.groupBy(ParallelGroupByAgg.of(chm, "label").sum("amount")));  // String group key
        });
    }

    // o1:union(wideA,wideB,supp6)→filter→select→map→addFields→removeFields→partitionByHash→groupBy(label String).sum;
    // o2:orderBy(created Date)→top→flatMap→distinct(active,label)→select(int);o3:coGroup.filter.reduceGroup;
    @Test
    void dagTyped05_unionFirstDateSort() {
        assertTriEngine("dagTyped05", 5, (s, ioCols) -> {
            DataSet u = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .union(s.fromSource(csv(wideB, TenDagData.WIDE)))
                    .filter("amount >= 0")
                    .select("id", "dept", "label", "active", "amount", "qty")
                    .map(MAP_ID)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{5})
                    .removeFields("tag", "qty")
                    .partitionByHash("dept");
            DataSchema um = u.getSchema();
            ioCols.out(u.groupBy(ParallelGroupByAgg.of(um, "label").sum("amount")));  // String group key
            // o2:orderBy(created Date)→top→flatMap→distinct(active,label)→select(int)
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE))
                    .orderBy(OrderBy.of(TenDagData.WIDE, desc(7)), 1, false)  // created Date desc
                    .top(Top.of(TenDagData.WIDE, 300, desc(7)), 1)
                    .flatMap(FLATMAP_GRADE)
                    .distinct("active", "label", "dept")
                    .select(0, 1, 4));
            // o3:coGroup→filter→reduceGroup
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:leftJoin.union(rightJoin).union(inner join)→groupBy{max,min,count}
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .join(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)).on("id")
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            // o5:combineGroup+combineReduceGroup+fullJoin
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // o2:fullJoin(dim)→union(leftJoin)→union(rightJoin)→groupBy{max,min,count};o3:coGroup.filter.reduceGroup;
    @Test
    void dagTyped06_fullJoinFirstTsGroup() {
        assertTriEngine("dagTyped06", 5, (s, ioCols) -> {
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
            ioCols.out(d1.groupBy(ParallelGroupByAgg.of(d1m, "tstamp").count())   // Timestamp group key
                    .orderBy(OrderBy.of(g1, asc(0)), 1, false)
                    .top(Top.of(g1, 30, asc(0)), 1));
            // o2:fullJoin(dim).union(leftJoin).union(rightJoin)→groupBy{max,min,count}
            DataSet fj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .fullJoin(s.fromSource(csv(dim, TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema fjm = fj.getSchema();
            ioCols.out(fj.union(lj).union(rj)
                    .groupBy(ParallelGroupByAgg.of(fjm, "dept").max("amount").min("amount").count()));
            // o3:coGroup→filter→reduceGroup
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:inner join(id)→flatMap→distinct→select(int)
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .join(s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)).on("id")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            // o5:combineGroup+combineReduceGroup+fullJoin
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // o1:partitionByHash(active Bool)→map→flatMap→filter→groupBy(active Bool).sum→orderBy→top;
    // o2:partitionByHash(label String)→select→addFields→removeFields→distinct→groupBy{max,min,count};
    // o3:coGroup.filter.reduceGroup;o4:leftJoin.union(rightJoin).union(inner join)→select(int);
    @Test
    void dagTyped07_boolPartitionReshuffle() {
        assertTriEngine("dagTyped07", 5, (s, ioCols) -> {
            // o1:partitionByHash(active Boolean)→map→flatMap→filter→groupBy(active).sum→orderBy→top
            DataSet p1 = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .partitionByHash("active")   // Boolean partition key(reshuffle)
                    .map(MAP_ID)
                    .flatMap(FLATMAP_GRADE)
                    .filter("amount >= 0");
            DataSchema pm1 = p1.getSchema();
            DataSchema g1 = ParallelGroupByAgg.of(pm1, "active").sum("amount").getOutputSchema();
            ioCols.out(p1.groupBy(ParallelGroupByAgg.of(pm1, "active").sum("amount"))
                    .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                    .top(Top.of(g1, 5, desc(1)), 1));
            // o2:partitionByHash(label String)→select→addFields→removeFields→distinct→groupBy{max,min,count}
            DataSet p2 = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .partitionByHash("label")    // String partition key(reshuffle)
                    .select("id", "dept", "label", "created", "amount", "qty")
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{7})
                    .removeFields("tag", "qty")
                    .distinct("dept", "label", "created");
            DataSchema pm2 = p2.getSchema();
            ioCols.out(p2.groupBy(ParallelGroupByAgg.of(pm2, "label").max("amount").min("amount").count()));
            // o3:coGroup→filter→reduceGroup
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:leftJoin.union(rightJoin).union(inner join)→select(int)
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select(ORDER6);
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select(ORDER6);
            DataSet ij = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .join(s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)).on("id")
                    .select(ORDER6);
            ioCols.out(lj.union(rj).union(ij).select(0, 1, 4));
            // o5:combineGroup+combineReduceGroup+fullJoin
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // o1:join(labelDim on label)→filter→select→map→addFields→removeFields→partitionByHash→groupBy(created Date).sum→orderBy→top;
    // o2:orderBy(tstamp Timestamp)→top→flatMap→distinct→select(int);o3:coGroup.filter.reduceGroup;
    @Test
    void dagTyped08_labelJoinDualSort() {
        assertTriEngine("dagTyped08", 5, (s, ioCols) -> {
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
            // o2:orderBy(tstamp Timestamp)→top→flatMap→distinct→select(int)
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE))
                    .orderBy(OrderBy.of(TenDagData.WIDE, asc(8)), 1, false)  // tstamp idx8
                    .top(Top.of(TenDagData.WIDE, 250, asc(8)), 1)
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "tstamp")
                    .select(0, 1, 4));
            // o3:coGroup→filter→reduceGroup
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("rc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:leftJoin.union(rightJoin)→groupBy{max,min,count}
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj)
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            // o5:combineGroup+combineReduceGroup+fullJoin
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // o1:join(dim,inner).select.map.addFields.removeFields.partitionByHash.groupBy(active Bool).sum.orderBy.top;
    // o2:union(wideB).flatMap.distinct(created,label).filter.select(int);o3:coGroup.filter.reduceGroup;
    // o4:leftJoin(dim).union(rightJoin(dim)).union(join(regionDim on region)).groupBy{max,min,count};
    @Test
    void dagTyped09_fiveSourceMixedTypes() {
        assertTriEngine("dagTyped09", 5, (s, ioCols) -> {
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
            // o2:union(wideB)→flatMap→distinct(created,label)→filter→select(int)
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE))
                    .union(s.fromSource(csv(wideB, TenDagData.WIDE)))
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "created", "label")
                    .filter("amount >= 0")
                    .select(0, 1, 4));
            // o3:coGroup→filter→reduceGroup
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            // o4:leftJoin(dim).union(rightJoin(dim)).union(join(regionDim on region))→groupBy{max,min,count}
            DataSet lj = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select(ORDER6);
            DataSet rj = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                    .select(ORDER6);
            DataSet rjoin = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .join(s.fromSource(csv(regionDim, TenDagData.REGION_DIM))).on("region")
                    .select(ORDER6);
            DataSchema ljm = lj.getSchema();
            ioCols.out(lj.union(rj).union(rjoin)
                    .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()));
            // o5:combineGroup+combineReduceGroup+fullJoin
            DataSet cg = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(wideB, TenDagData.WIDE)).select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });
    }

    // o1:filter.select.map.join(labelDim String).flatMap.addFields.removeFields.partitionByHash(active Bool).groupBy(created Date).sum;
    @Test
    void dagTyped10_deepMixedAllTypes() {
        assertTriEngine("dagTyped10", 5, (s, ioCols) -> {
            DataSet chain = s.fromSource(csv(wideA, TenDagData.WIDE))
                    .filter("amount >= 0")
                    .select("id", "active", "created", "grade", "amount", "label")
                    .map(MAP_ID)
                    .join(s.fromSource(csv(labelDim, TenDagData.LABEL_DIM))).on("label")
                    .flatMap(FLATMAP_GRADE)
                    .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{10})
                    .removeFields("tag", "weight")
                    .partitionByHash("active");         // Boolean partition
            DataSchema chm = chain.getSchema();
            ioCols.out(chain.groupBy(ParallelGroupByAgg.of(chm, "created").sum("amount")));  // Date group key
            DataSet cg2 = s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
            DataSchema cg2m = cg2.getSchema();
            ioCols.out(cg2.orderBy(OrderBy.of(cg2m, desc(1)), 1, false)
                    .top(Top.of(cg2m, 15, desc(1)), 1));
            // o3:coGroup→filter→reduceGroup
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE)).select(ORDER6)
                    .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                    .filter("lc >= 0")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
            ioCols.out(s.fromSource(csv(wideA, TenDagData.WIDE))
                    .distinct("dept", "active", "created", "tstamp", "label")
                    .select(ORDER6)
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4));
            // o5:leftJoin.union(rightJoin).union(fullJoin)→distinct→groupBy{max,min,count}
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
    }
}

