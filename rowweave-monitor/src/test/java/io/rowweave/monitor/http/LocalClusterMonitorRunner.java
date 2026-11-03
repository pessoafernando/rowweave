package io.rowweave.monitor.http;

import io.rowweave.CoGroupFunction;
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
import io.rowweave.bench.CsvInput;
import io.rowweave.cluster.InProcessCluster;
import io.rowweave.cluster.TenDagData;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * A live monitor runner for LocalCluster (InProcessCluster: in-process RPC over LocalChannel).
 * It reuses the ten full-operator DAGs (F01-F10) from the equivalence tests, runs them on
 * LocalCluster and feeds the running job to the monitor HTTP server on port 18080.
 *
 * <p>{@link TenDagData} writes the data as CSV and {@code -Drowweave.rows} sets the volume, two
 * million rows per table by default. It is read back through {@link CsvInput} streaming from
 * disk, because a CollectionSource of that size does not fit in memory. Each output of each DAG
 * lands in a {@link FileSink} file under a temporary directory. The DAG shapes match
 * ThreeWayEquivalenceTest.dagFull01..dagFull10 and cover every operator: filter, select, map,
 * flatMap, join, leftJoin, rightJoin, fullJoin, coGroup, union, groupBy, reduceGroup,
 * combineGroup, combineReduceGroup, distinct, orderBy, top, addFields, removeFields,
 * partitionByHash, together with every io.out branch. {@code -Drowweave.par} sets the
 * parallelism, 4 by default. The submitter commits dag01..dag10 in order, two seconds apart,
 * then finishes and leaves the HTTP server up for browsing.
 */
public final class LocalClusterMonitorRunner {

    // ============ Local helpers: package-private statics are not reachable across packages, so they are duplicated here ============
    static final MapFunction MAP_ID = in -> in;
    static final FlatMapFunction FLATMAP_GRADE = (in, c) -> {
        int n = (Integer) in.get(3) + 1;
        for (int i = 0; i < n; i++) c.collect(in);
    };
    // coGroup on dept -> [dept, lc, rc]: rows in the left group and rows in the right group.
    static final CoGroupFunction CG_FN = new CoGroupFunction() {
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
    // reduceGroup over the coGroup output [dept, lc, rc], keyed by dept -> [dept, sum of lc].
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
    // combineGroup partial sum keyed by dept; the output has the shape of ORDER (representative column, amount = partial sum).
    static final GroupCombineFunction COMBINE_SUM = (rows, c) -> {
        LinkedHashMap<Integer, DataRow> firstRow = new LinkedHashMap<>();
        LinkedHashMap<Integer, BigDecimal> sum = new LinkedHashMap<>();
        for (DataRow r : rows) {
            int dept = (Integer) r.get(1);
            firstRow.putIfAbsent(dept, r);
            sum.merge(dept, (BigDecimal) r.get(4), BigDecimal::add);
        }
        for (Map.Entry<Integer, DataRow> e : firstRow.entrySet()) {
            DataRow src = e.getValue();
            DataRow o = new DataRow(6);
            o.set(0, src.get(0)); o.set(1, e.getKey()); o.set(2, src.get(2));
            o.set(3, src.get(3)); o.set(4, sum.get(e.getKey())); o.set(5, src.get(5));
            c.collect(o);
        }
    };
    // combineReduceGroup: global sum keyed by dept -> [dept, total].
    static final GroupCombineReduceFunction COMBINE_REDUCE_SUM = new GroupCombineReduceFunction() {
        private final DataSchema out = new DataSchema(new Field("dept", DataType.IntegerType),
                new Field("total", DataType.BigDecimalType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void combine(Iterable<DataRow> rows, Collector c) { COMBINE_SUM.combine(rows, c); }
        @Override public void reduce(Iterable<DataRow> rows, Collector c) {
            TreeMap<Integer, BigDecimal> sum = new TreeMap<>();
            for (DataRow r : rows) sum.merge((Integer) r.get(1), (BigDecimal) r.get(4), BigDecimal::add);
            for (Map.Entry<Integer, BigDecimal> e : sum.entrySet()) {
                DataRow o = new DataRow(2); o.set(0, e.getKey());
                o.set(1, e.getValue().setScale(10, RoundingMode.HALF_UP)); c.collect(o);
            }
        }
    };

    private static SortField desc(int i) { return new SortField(i, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST); }
    private static SortField asc(int i) { return new SortField(i, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST); }
    private static Field[] f(Field... x) { return x; }

    // ============ CSV paths produced by the data generator, filled in by setUp ============
    private static Path DIR;
    private static String ordersA, ordersB, supp, dim, regionDim;

    private static CsvInput csv(String path, DataSchema meta) { return new CsvInput(path, meta); }

    /** One output branch of one DAG: written to a FileSink file in a temporary directory, tagged for example "f01-o0". */
    private static void output(DataSet ds, File dir, String tag) {
        ds.sinkTo(new FileSink(ds.getSchema(), new File(dir, tag + ".tsv").toString()));
    }

    @FunctionalInterface
    private interface Dag { void build(ExecutionEnvironment s, File dir, String tag); }

    // ================ F01: the full-operator linear trunk ================
    private static void dagFull01(ExecutionEnvironment s, File dir, String tag) {
        DataSet preAgg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .filter("amount >= 10")
                .select("id", "dept", "region", "grade", "amount", "qty")
                .map(MAP_ID)
                .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{1})
                .removeFields("tag", "qty")
                .partitionByHash("region");
        DataSchema preMeta = preAgg.getSchema();
        DataSchema o1gb = ParallelGroupByAgg.of(preMeta, "region").sum("amount").getOutputSchema();
        output(preAgg
                .groupBy(ParallelGroupByAgg.of(preMeta, "region").sum("amount"))
                .orderBy(OrderBy.of(o1gb, desc(1)), 1, false)
                .top(Top.of(o1gb, 5, desc(1)), 1), dir, tag + "-o0");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                .flatMap(FLATMAP_GRADE)
                .distinct("id", "dept")
                .select(0, 1, 4), dir, tag + "-o1");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                .filter("lc >= 1")
                .groupBy("dept").reduceGroup(RG_SUM_LC), dir, tag + "-o2");
        DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSchema ljMeta = lj.getSchema();
        output(lj.union(rj)
                .groupBy(ParallelGroupByAgg.of(ljMeta, "dept").max("amount").min("amount").count()), dir, tag + "-o3");
        DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineGroup(COMBINE_SUM, 4)
                .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
        DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
        output(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}), dir, tag + "-o4");
    }

    // ================ F02: join first, then a deep chain ================
    private static void dagFull02(ExecutionEnvironment s, File dir, String tag) {
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                .filter("rc >= 0")
                .groupBy("dept").reduceGroup(RG_SUM_LC), dir, tag + "-o0");
        DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineGroup(COMBINE_SUM, 3)
                .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
        DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER)).combineReduceGroup(COMBINE_REDUCE_SUM, 3);
        output(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}), dir, tag + "-o1");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .union(s.fromSource(csv(supp, TenDagData.ORDER)))
                .partitionByHash("region")
                .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "region").max("amount").min("amount").count()), dir, tag + "-o2");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                .flatMap(FLATMAP_GRADE)
                .distinct("id", "dept")
                .select(0, 1, 4), dir, tag + "-o3");
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
        output(proj.groupBy(ParallelGroupByAgg.of(projMeta, "region").sum("amount"))
                .orderBy(OrderBy.of(gsum, asc(0)), 1, false)
                .top(Top.of(gsum, 6, asc(0)), 1), dir, tag + "-o4");
    }

    // ================ F03: fan out to three outputs through a shared mid section ================
    private static void dagFull03(ExecutionEnvironment s, File dir, String tag) {
        DataSet mid1 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .filter("qty >= 1")
                .select("id", "dept", "region", "grade", "amount", "qty")
                .map(MAP_ID)
                .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{2})
                .removeFields("tag", "grade")
                .partitionByHash("dept");
        DataSchema m1 = mid1.getSchema();
        DataSchema g1 = ParallelGroupByAgg.of(m1, "dept").sum("amount").getOutputSchema();
        output(mid1.groupBy(ParallelGroupByAgg.of(m1, "dept").sum("amount"))
                .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                .top(Top.of(g1, 7, desc(1)), 1), dir, tag + "-o0");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                .flatMap(FLATMAP_GRADE)
                .distinct("id", "dept")
                .select(0, 1, 4), dir, tag + "-o1");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                .filter("lc >= 0")
                .groupBy("dept").reduceGroup(RG_SUM_LC), dir, tag + "-o2");
        DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSchema ljm = lj.getSchema();
        output(lj.union(rj)
                .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()), dir, tag + "-o3");
        DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineGroup(COMBINE_SUM, 4)
                .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
        DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
        output(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}), dir, tag + "-o4");
    }

    // ================ F04: union first over three sources, then a deep tail chain ================
    private static void dagFull04(ExecutionEnvironment s, File dir, String tag) {
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
        output(u3.groupBy(ParallelGroupByAgg.of(m1, "region").max("amount").min("amount").count()), dir, tag + "-o0");
        DataSet u2 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .union(s.fromSource(csv(supp, TenDagData.ORDER)));
        DataSchema g2 = ParallelGroupByAgg.of(TenDagData.ORDER, "region").sum("amount").getOutputSchema();
        output(u2.groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "region").sum("amount"))
                .orderBy(OrderBy.of(g2, asc(0)), 1, false)
                .top(Top.of(g2, 8, asc(0)), 1), dir, tag + "-o1");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                .filter("rc >= 0")
                .groupBy("dept").reduceGroup(RG_SUM_LC), dir, tag + "-o2");
        DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        output(lj.union(rj)
                .join(s.fromSource(csv(ordersA, TenDagData.ORDER))).on("id")
                .flatMap(FLATMAP_GRADE)
                .distinct("id", "dept")
                .select(0, 1, 4), dir, tag + "-o3");
        DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineGroup(COMBINE_SUM, 4)
                .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
        DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
        output(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}), dir, tag + "-o4");
    }

    // ================ F05: combine first, closed out by reduce, then a tail projection ================
    private static void dagFull05(ExecutionEnvironment s, File dir, String tag) {
        DataSet cr1 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
        DataSchema crm = cr1.getSchema();
        output(cr1.orderBy(OrderBy.of(crm, desc(1)), 1, false)
                .top(Top.of(crm, 9, desc(1)), 1), dir, tag + "-o0");
        DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineGroup(COMBINE_SUM, 4)
                .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
        DataSet cr2 = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
        output(cg.fullJoin(cr2).on(new String[]{"dept"}, new String[]{"dept"}), dir, tag + "-o1");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                .filter("lc >= 0")
                .groupBy("dept").reduceGroup(RG_SUM_LC), dir, tag + "-o2");
        DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSchema ljm = lj.getSchema();
        output(lj.union(rj)
                .distinct("id", "dept")
                .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()), dir, tag + "-o3");
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
        output(proj.groupBy(ParallelGroupByAgg.of(pm, "dept").sum("amount")), dir, tag + "-o4");
    }

    // ================ F06: regionDim join with orderBy and top at the front ================
    private static void dagFull06(ExecutionEnvironment s, File dir, String tag) {
        DataSchema om = TenDagData.ORDER;
        output(s.fromSource(csv(ordersA, om))
                .orderBy(OrderBy.of(om, asc(0)), 1, false)
                .top(Top.of(om, 100, asc(0)), 1)
                .select(0, 1, 4)
                .map(MAP_ID), dir, tag + "-o0");
        DataSet rjoin = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .join(s.fromSource(csv(regionDim, TenDagData.REGION_DIM))).on("region")
                .filter("amount >= 0")
                .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{6})
                .removeFields("tag", "rname");
        DataSchema rjm = rjoin.getSchema();
        DataSchema g2 = ParallelGroupByAgg.of(rjm, "dept").sum("amount").getOutputSchema();
        output(rjoin.groupBy(ParallelGroupByAgg.of(rjm, "dept").sum("amount"))
                .orderBy(OrderBy.of(g2, desc(1)), 1, false)
                .top(Top.of(g2, 10, desc(1)), 1), dir, tag + "-o1");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                .filter("lc >= 1")
                .groupBy("dept").reduceGroup(RG_SUM_LC), dir, tag + "-o2");
        DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSet fj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .fullJoin(s.fromSource(csv(dim, TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
        DataSchema ljm = lj.getSchema();
        output(lj.union(fj)
                .distinct("id", "dept")
                .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()), dir, tag + "-o3");
        DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .partitionByHash("dept")
                .flatMap(FLATMAP_GRADE)
                .combineGroup(COMBINE_SUM, 4)
                .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
        DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                .select("id", "dept", "region", "grade", "amount", "qty")
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
        output(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}), dir, tag + "-o4");
    }

    // ================ F07: distinct first, then three nested binary operators ================
    private static void dagFull07(ExecutionEnvironment s, File dir, String tag) {
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
        output(d1.groupBy(ParallelGroupByAgg.of(m1, "region").sum("amount"))
                .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                .top(Top.of(g1, 5, desc(1)), 1), dir, tag + "-o0");
        DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSet fj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .fullJoin(s.fromSource(csv(dim, TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
        DataSchema ljm = lj.getSchema();
        output(lj.union(rj).union(fj)
                .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()), dir, tag + "-o1");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                .filter("rc >= 0")
                .groupBy("dept").reduceGroup(RG_SUM_LC), dir, tag + "-o2");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                .flatMap(FLATMAP_GRADE)
                .distinct("id", "dept")
                .select(0, 1, 4), dir, tag + "-o3");
        DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineGroup(COMBINE_SUM, 4)
                .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
        DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
        output(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}), dir, tag + "-o4");
    }

    // ================ F08: partitionByHash reshuffles up front, closed out by a join at the tail ================
    private static void dagFull08(ExecutionEnvironment s, File dir, String tag) {
        DataSet p1 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .partitionByHash("dept")
                .map(MAP_ID)
                .flatMap(FLATMAP_GRADE)
                .filter("amount >= 0");
        DataSchema pm1 = p1.getSchema();
        DataSchema g1 = ParallelGroupByAgg.of(pm1, "dept").sum("amount").getOutputSchema();
        output(p1.groupBy(ParallelGroupByAgg.of(pm1, "dept").sum("amount"))
                .orderBy(OrderBy.of(g1, desc(1)), 1, false)
                .top(Top.of(g1, 12, desc(1)), 1), dir, tag + "-o0");
        DataSet p2 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .partitionByHash("region")
                .select("id", "dept", "region", "grade", "amount", "qty")
                .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{8})
                .removeFields("tag", "qty")
                .distinct("dept", "region");
        DataSchema pm2 = p2.getSchema();
        output(p2.groupBy(ParallelGroupByAgg.of(pm2, "region").max("amount").min("amount").count()), dir, tag + "-o1");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                .filter("lc >= 0")
                .groupBy("dept").reduceGroup(RG_SUM_LC), dir, tag + "-o2");
        DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                .select("id", "dept", "region", "grade", "amount", "qty");
        DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                .select("id", "dept", "region", "grade", "amount", "qty");
        DataSet ij = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .join(s.fromSource(csv(ordersB, TenDagData.ORDER))).on("id")
                .select("id", "dept", "region", "grade", "amount", "qty");
        output(lj.union(rj).union(ij).select(0, 1, 4), dir, tag + "-o3");
        DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineGroup(COMBINE_SUM, 4)
                .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
        DataSet cr = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
        output(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}), dir, tag + "-o4");
    }

    // ================ F09: five sources and five outputs, using dim and regionDim together ================
    private static void dagFull09(ExecutionEnvironment s, File dir, String tag) {
        DataSet j1 = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .join(s.fromSource(csv(dim, TenDagData.DIM))).on("dept")
                .select("id", "dept", "region", "grade", "amount", "qty")
                .map(MAP_ID)
                .addFields(f(new Field("tag", DataType.IntegerType)), new Object[]{9})
                .removeFields("tag", "grade")
                .partitionByHash("region");
        DataSchema m1 = j1.getSchema();
        DataSchema g1 = ParallelGroupByAgg.of(m1, "region").sum("amount").getOutputSchema();
        output(j1.groupBy(ParallelGroupByAgg.of(m1, "region").sum("amount"))
                .orderBy(OrderBy.of(g1, asc(0)), 1, false)
                .top(Top.of(g1, 10, asc(0)), 1), dir, tag + "-o0");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .union(s.fromSource(csv(supp, TenDagData.ORDER)))
                .flatMap(FLATMAP_GRADE)
                .distinct("id", "dept")
                .filter("amount >= 0")
                .select(0, 1, 4), dir, tag + "-o1");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                .filter("lc >= 0")
                .groupBy("dept").reduceGroup(RG_SUM_LC), dir, tag + "-o2");
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
        output(lj.union(rj).union(fj)
                .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()), dir, tag + "-o3");
        DataSet cg = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .combineGroup(COMBINE_SUM, 4)
                .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount"));
        DataSet cr = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
        output(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}), dir, tag + "-o4");
    }

    // ================ F10: a deep mixture of everything ================
    private static void dagFull10(ExecutionEnvironment s, File dir, String tag) {
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
        output(chain2.orderBy(OrderBy.of(c2m, desc(1)), 1, false)
                .top(Top.of(c2m, 15, desc(1)), 1), dir, tag + "-o0");
        DataSchema chm = chain.getSchema();
        output(chain.groupBy(ParallelGroupByAgg.of(chm, "region").sum("amount")), dir, tag + "-o1");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .coGroup(s.fromSource(csv(supp, TenDagData.ORDER)), CG_FN).on("dept")
                .filter("lc >= 0")
                .groupBy("dept").reduceGroup(RG_SUM_LC), dir, tag + "-o2");
        output(s.fromSource(csv(ordersA, TenDagData.ORDER))
                .distinct("dept", "region", "grade")
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4), dir, tag + "-o3");
        DataSet lj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .leftJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSet rj = s.fromSource(csv(ordersB, TenDagData.ORDER))
                .rightJoin(s.fromSource(csv(dim, TenDagData.DIM))).on("dept");
        DataSet fj = s.fromSource(csv(ordersA, TenDagData.ORDER))
                .fullJoin(s.fromSource(csv(dim, TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
        DataSchema ljm = lj.getSchema();
        output(lj.union(rj).union(fj)
                .distinct("id", "dept")
                .groupBy(ParallelGroupByAgg.of(ljm, "dept").max("amount").min("amount").count()), dir, tag + "-o4");
    }

    /** The ten full-operator DAGs (name to builder), isomorphic to dagFull01..dagFull10 in the equivalence tests. */
    private static Map<String, Dag> dags() {
        Map<String, Dag> m = new LinkedHashMap<>();
        m.put("dag01", LocalClusterMonitorRunner::dagFull01);
        m.put("dag02", LocalClusterMonitorRunner::dagFull02);
        m.put("dag03", LocalClusterMonitorRunner::dagFull03);
        m.put("dag04", LocalClusterMonitorRunner::dagFull04);
        m.put("dag05", LocalClusterMonitorRunner::dagFull05);
        m.put("dag06", LocalClusterMonitorRunner::dagFull06);
        m.put("dag07", LocalClusterMonitorRunner::dagFull07);
        m.put("dag08", LocalClusterMonitorRunner::dagFull08);
        m.put("dag09", LocalClusterMonitorRunner::dagFull09);
        m.put("dag10", LocalClusterMonitorRunner::dagFull10);
        return m;
    }

    public static void main(String[] args) throws Exception {
        DIR = Files.createTempDirectory("lc-mon-tendag");
        File dir = DIR.toFile();

        // ---- setUp: generate the data as TenDagData CSV, streamed to disk so it does not exhaust memory ----
        long rows = Long.getLong("rowweave.rows", 2_000_000L);
        long t0 = System.currentTimeMillis();
        ordersA = TenDagData.orders(DIR, "ordersA", rows);
        ordersB = TenDagData.orders(DIR, "ordersB", rows);
        supp = TenDagData.supplement(DIR, "supp", rows / 2, rows / 2);
        dim = TenDagData.dim(DIR);
        regionDim = TenDagData.regionDim(DIR);
        System.out.println("data ready rows=" + rows + " elapsed " + (System.currentTimeMillis() - t0) + "ms, dir=" + dir);

        int par = Integer.getInteger("rowweave.par", 4);
        InMemoryJobMonitor mon = new InMemoryJobMonitor(100);
        InProcessCluster lc = new InProcessCluster(par, new File(dir, "cluster"), mon);

        MonitorHttpServer http = new MonitorHttpServer(mon, 18080, null);
        int hport = http.start();
        System.out.println("LIVE_MONITOR_PORT=" + hport);

        Map<String, Dag> dags = dags();
        Thread submitter = new Thread(() -> {
            for (Map.Entry<String, Dag> e : dags.entrySet()) {
                if (Thread.currentThread().isInterrupted()) return;
                String name = e.getKey();
                try {
                    ExecutionEnvironment s = new ExecutionEnvironment("lc-" + name, name, lc);
                    s.setParallelism(par);
                    e.getValue().build(s, dir, name);
                    s.execute();
                    System.out.println("submitted: " + name);
                    Thread.sleep(2000);
                } catch (Throwable t) {
                    if (t instanceof InterruptedException) { Thread.currentThread().interrupt(); return; }
                    System.out.println("FAILED " + name + ": " + t);
                }
            }
            System.out.println("=== all 10 full-operator DAGs submitted ===");
        });
        submitter.setDaemon(true);
        submitter.start();

        System.out.println("LIVE_READY (LocalCluster par=" + par + ", TenDagData CsvInput, 10 full-operator DAGs)");
        Thread.currentThread().join();
    }

    private LocalClusterMonitorRunner() { }
}
