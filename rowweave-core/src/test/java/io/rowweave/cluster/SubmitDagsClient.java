package io.rowweave.cluster;

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
import io.rowweave.bench.CsvInput;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;

public final class SubmitDagsClient {

    static final MapFunction MAP_ID = in -> {
        long ms = Long.getLong("slow.ms", 0L);
        if (ms > 0) { try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); } }
        return in;
    };
    static final FlatMapFunction FLATMAP_GRADE = (in, c) -> {
        int n = (Integer) in.get(3) + 1; for (int i = 0; i < n; i++) c.collect(in);
    };
    static final DataSchema RG_LC_OUT = new DataSchema(
            new Field("dept", DataType.IntegerType), new Field("lcSum", DataType.IntegerType));
    static final GroupReduceFunction RG_SUM_LC = new GroupReduceFunction() {
        public DataSchema getResultSchema() { return RG_LC_OUT; }
        public void reduce(Iterable<DataRow> rows, Collector c) {
            int s = 0; Object k = null; for (DataRow r : rows) { s += (Integer) r.get(1); k = r.get(0); }
            DataRow o = new DataRow(2); o.set(0, k); o.set(1, s); c.collect(o);
        }
    };
    static final GroupCombineFunction COMBINE_SUM = (rows, c) -> {
        java.util.LinkedHashMap<Integer, DataRow> first = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<Integer, BigDecimal> sum = new java.util.LinkedHashMap<>();
        for (DataRow r : rows) { int d = (Integer) r.get(1); first.putIfAbsent(d, r); sum.merge(d, (BigDecimal) r.get(4), BigDecimal::add); }
        for (java.util.Map.Entry<Integer, DataRow> e : first.entrySet()) { DataRow src = e.getValue(); DataRow o = new DataRow(6);
            o.set(0, src.get(0)); o.set(1, e.getKey()); o.set(2, src.get(2)); o.set(3, src.get(3)); o.set(4, sum.get(e.getKey())); o.set(5, src.get(5)); c.collect(o); }
    };
    static final DataSchema CR_OUT = new DataSchema(new Field("dept", DataType.IntegerType), new Field("total", DataType.BigDecimalType));
    static final GroupCombineReduceFunction COMBINE_REDUCE_SUM = new GroupCombineReduceFunction() {
        public DataSchema getResultSchema() { return CR_OUT; }
        public void combine(Iterable<DataRow> rows, Collector c) { COMBINE_SUM.combine(rows, c); }
        public void reduce(Iterable<DataRow> rows, Collector c) {
            java.util.TreeMap<Integer, BigDecimal> sum = new java.util.TreeMap<>();
            for (DataRow r : rows) sum.merge((Integer) r.get(1), (BigDecimal) r.get(4), BigDecimal::add);
            for (java.util.Map.Entry<Integer, BigDecimal> e : sum.entrySet()) { DataRow o = new DataRow(2); o.set(0, e.getKey());
                o.set(1, e.getValue().setScale(10, RoundingMode.HALF_UP)); c.collect(o); }
        }
    };
    static final DataSchema CG_OUT = new DataSchema(new Field("dept", DataType.IntegerType),
            new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
    static final io.rowweave.CoGroupFunction CG_FN = new io.rowweave.CoGroupFunction() {
        public DataSchema getResultSchema() { return CG_OUT; }
        public void coGroup(Iterable<DataRow> l, Iterable<DataRow> r, Collector c) {
            int lc = 0; Object k = null; for (DataRow x : l) { lc++; k = x.get(1); }
            int rc = 0; for (DataRow x : r) { rc++; if (k == null) k = x.get(1); }
            DataRow o = new DataRow(3); o.set(0, k); o.set(1, lc); o.set(2, rc); c.collect(o);
        }
    };
    static Field[] f(Field... x) { return x; }
    static SortField asc(int i) { return new SortField(i, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST); }
    static SortField desc(int i) { return new SortField(i, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST); }

    private static Path dir;
    private static String ordersA, ordersB, supp, dim, regionDim;
    private static int masterPort;

    private static CsvInput csv(String p, DataSchema m) { return new CsvInput(p, m); }

    interface Dag { void build(BatchSession s); }
    private static String only;
    private static void submit(String name, Dag dag) {
        if (only != null && !name.contains(only)) return;
        BatchSession s = new BatchSession(name, name, new ClusterJobClient("127.0.0.1", masterPort));
        s.setParallelism(2);
        long t0 = System.currentTimeMillis();
        dag.build(s);
        s.execute();
        System.out.println("  ✓ " + name + " (" + (System.currentTimeMillis() - t0) + "ms)");
    }
    private static int oi = 0;
    private static void out(DataSet ds) {
        ds.sinkTo(new FileSink(ds.getSchema(), dir.resolve("o" + (oi++) + ".tsv").toString()));
    }

    public static void main(String[] args) throws Exception {
        masterPort = Integer.getInteger("cluster.master.port", 19100);
        only = System.getProperty("dag");
        long BASE = Long.getLong("rowweave.tendag.orders", 20000L);
        dir = Files.createTempDirectory("submit-dags");
        ordersA = TenDagData.orders(dir, "ordersA", BASE);
        ordersB = TenDagData.orders(dir, "ordersB", BASE);
        supp = TenDagData.supplement(dir, "supp", Math.max(500, BASE / 4), BASE / 2);
        dim = TenDagData.dim(dir);
        regionDim = TenDagData.regionDim(dir);
        System.out.println("connecting to the cluster at 127.0.0.1:" + masterPort + " to submit the full-operator DAG (orders=" + BASE + ")...");

        submit("dagFull01_allOps_linear", s -> {
            DataSet pre = s.fromSource(csv(ordersA, TenDagData.ORDER)).filter("amount >= 10")
                    .select("id","dept","region","grade","amount","qty").map(MAP_ID)
                    .addFields(f(new Field("tag",DataType.IntegerType)), new Object[]{1}).removeFields("tag","qty").partitionByHash("region");
            DataSchema pm = pre.getSchema(); DataSchema g = ParallelGroupByAgg.of(pm,"region").sum("amount").getOutputSchema();
            out(pre.groupBy(ParallelGroupByAgg.of(pm,"region").sum("amount")).orderBy(OrderBy.of(g,desc(1)),1,false).top(Top.of(g,5,desc(1)),1));
            out(s.fromSource(csv(ordersA,TenDagData.ORDER)).join(s.fromSource(csv(ordersB,TenDagData.ORDER))).on("id").flatMap(FLATMAP_GRADE).distinct("id","dept").select(0,1,4));
            out(s.fromSource(csv(ordersA,TenDagData.ORDER)).coGroup(s.fromSource(csv(supp,TenDagData.ORDER)),CG_FN).on("dept").filter("lc >= 1").groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet lj = s.fromSource(csv(ordersA,TenDagData.ORDER)).leftJoin(s.fromSource(csv(dim,TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB,TenDagData.ORDER)).rightJoin(s.fromSource(csv(dim,TenDagData.DIM))).on("dept");
            out(lj.union(rj).groupBy(ParallelGroupByAgg.of(lj.getSchema(),"dept").max("amount").min("amount").count()));
            DataSet cg = s.fromSource(csv(ordersA,TenDagData.ORDER)).combineGroup(COMBINE_SUM,4).groupBy(ParallelGroupByAgg.of(TenDagData.ORDER,"dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersA,TenDagData.ORDER)).combineReduceGroup(COMBINE_REDUCE_SUM,4);
            out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        });

        submit("dagFull02_joinFirst", s -> {
            out(s.fromSource(csv(ordersA,TenDagData.ORDER)).coGroup(s.fromSource(csv(supp,TenDagData.ORDER)),CG_FN).on("dept").filter("rc >= 0").groupBy("dept").reduceGroup(RG_SUM_LC));
            DataSet cg = s.fromSource(csv(ordersA,TenDagData.ORDER)).combineGroup(COMBINE_SUM,3).groupBy(ParallelGroupByAgg.of(TenDagData.ORDER,"dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersB,TenDagData.ORDER)).combineReduceGroup(COMBINE_REDUCE_SUM,3);
            out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
            out(s.fromSource(csv(ordersA,TenDagData.ORDER)).union(s.fromSource(csv(supp,TenDagData.ORDER))).partitionByHash("region")
                    .groupBy(ParallelGroupByAgg.of(TenDagData.ORDER,"region").max("amount").min("amount").count()));
            out(s.fromSource(csv(ordersA,TenDagData.ORDER)).join(s.fromSource(csv(ordersB,TenDagData.ORDER))).on("id").flatMap(FLATMAP_GRADE).distinct("id","dept").select(0,1,4));
        });

        submit("dagFull03_fanout", s -> {
            DataSet mid = s.fromSource(csv(ordersA,TenDagData.ORDER)).filter("qty >= 1").select("id","dept","region","grade","amount","qty").map(MAP_ID)
                    .addFields(f(new Field("tag",DataType.IntegerType)), new Object[]{2}).removeFields("tag","grade").partitionByHash("dept");
            DataSchema m1 = mid.getSchema(); DataSchema g1 = ParallelGroupByAgg.of(m1,"dept").sum("amount").getOutputSchema();
            out(mid.groupBy(ParallelGroupByAgg.of(m1,"dept").sum("amount")).orderBy(OrderBy.of(g1,desc(1)),1,false).top(Top.of(g1,7,desc(1)),1));
            out(s.fromSource(csv(ordersA,TenDagData.ORDER)).join(s.fromSource(csv(ordersB,TenDagData.ORDER))).on("id").flatMap(FLATMAP_GRADE).distinct("id","dept").select(0,1,4));
            out(s.fromSource(csv(ordersA,TenDagData.ORDER)).coGroup(s.fromSource(csv(supp,TenDagData.ORDER)),CG_FN).on("dept").filter("lc >= 0").groupBy("dept").reduceGroup(RG_SUM_LC));
        });

        submit("dagFull07_distinctNestedJoin", s -> {
            DataSet d1 = s.fromSource(csv(ordersA,TenDagData.ORDER)).distinct("id","dept","region","grade","amount","qty").filter("amount >= 0")
                    .select("id","dept","region","grade","amount","qty").map(MAP_ID).addFields(f(new Field("tag",DataType.IntegerType)), new Object[]{7}).removeFields("tag","qty").partitionByHash("region");
            DataSchema m1 = d1.getSchema(); DataSchema g1 = ParallelGroupByAgg.of(m1,"region").sum("amount").getOutputSchema();
            out(d1.groupBy(ParallelGroupByAgg.of(m1,"region").sum("amount")).orderBy(OrderBy.of(g1,desc(1)),1,false).top(Top.of(g1,5,desc(1)),1));
            DataSet lj = s.fromSource(csv(ordersA,TenDagData.ORDER)).leftJoin(s.fromSource(csv(dim,TenDagData.DIM))).on("dept");
            DataSet rj = s.fromSource(csv(ordersB,TenDagData.ORDER)).rightJoin(s.fromSource(csv(dim,TenDagData.DIM))).on("dept");
            DataSet fj = s.fromSource(csv(ordersA,TenDagData.ORDER)).fullJoin(s.fromSource(csv(dim,TenDagData.DIM))).on(new String[]{"dept"}, new String[]{"dept"});
            out(lj.union(rj).union(fj).groupBy(ParallelGroupByAgg.of(lj.getSchema(),"dept").max("amount").min("amount").count()));
        });

        submit("dagFull08_partitionReshuffle", s -> {
            DataSet p1 = s.fromSource(csv(ordersA,TenDagData.ORDER)).partitionByHash("dept").map(MAP_ID).flatMap(FLATMAP_GRADE).filter("amount >= 0");
            DataSchema pm1 = p1.getSchema(); DataSchema g1 = ParallelGroupByAgg.of(pm1,"dept").sum("amount").getOutputSchema();
            out(p1.groupBy(ParallelGroupByAgg.of(pm1,"dept").sum("amount")).orderBy(OrderBy.of(g1,desc(1)),1,false).top(Top.of(g1,12,desc(1)),1));
            out(s.fromSource(csv(ordersA,TenDagData.ORDER)).coGroup(s.fromSource(csv(supp,TenDagData.ORDER)),CG_FN).on("dept").filter("lc >= 0").groupBy("dept").reduceGroup(RG_SUM_LC));
        });

        System.out.println("\n=== all DAGs finished. The cluster is still alive and its monitor at http://127.0.0.1:18090/ shows these job DAGs.===");
    }

    private SubmitDagsClient() { }
}

