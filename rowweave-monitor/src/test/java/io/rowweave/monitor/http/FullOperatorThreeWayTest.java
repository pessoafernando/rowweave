package io.rowweave.monitor.http;

import io.rowweave.RowWeave;
import io.rowweave.Collector;
import io.rowweave.CoGroupFunction;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.FlatMapFunction;
import io.rowweave.GroupCombineFunction;
import io.rowweave.GroupCombineReduceFunction;
import io.rowweave.GroupReduceFunction;
import io.rowweave.BatchSession;
import io.rowweave.JoinFunction;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.bench.CsvInput;
import io.rowweave.cluster.InProcessCluster;
import io.rowweave.ops.Distinct;
import io.rowweave.cluster.JobClient;
import io.rowweave.cluster.TenDagData;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.ReduceGroup;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import io.rowweave.ops.Union;
import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full-operator three-way comparison. Every public operator of {@link DataSet} must yield an
 * equivalent multiset of rows on a single machine (P1), the in-process pseudo cluster
 * InProcessCluster (P4) and a real process cluster (P2). Everything runs through the one entry
 * point {@link BatchSession#commit()}; input comes from {@link CsvInput} reading the CSVs
 * written by {@link TenDagData} rather than an embedded list source, which keeps the cross-process
 * case free of out-of-memory and serialization gaps. Output lands in {@link FileSink} files that are
 * compared pairwise by multiset fingerprint (rows sorted, BigDecimal normalized with
 * setScale(10, HALF_UP)). The real-cluster side additionally asserts that instances really span two
 * workers.
 *
 * <p><b>Operator coverage matrix</b>, every public operator against the DAGs that cover it:
 * <pre>
 * == transforms ==
 * filter(FilterFunction) -> dag01
 * filter(String expression) -> dag01, dag02, dag05, dag06
 * filter(String, params) -> dag03
 * select(int...) by index -> dag01, dag02
 * select(String...) by name -> dag01, dag04, dag06
 * map(MapFunction) -> dag01, dag07
 * flatMap(FlatMapFunction) -> dag02, dag07
 * addFields(Field[],Object[]) -> dag01
 * removeFields(String...) -> dag01
 * == partitioning and ordering ==
 * partitionByHash(String...) -> dag01, dag04
 * orderBy(OrderBy, par, bool) -> dag01
 * orderBy(String...) convenience overload -> dag06
 * top(Top, par) -> dag01
 * top(int) convenience overload -> dag06
 * == grouped aggregation through Grouper ==
 * Grouper.sum -> dag03
 * Grouper.max -> dag03
 * Grouper.min -> dag03
 * Grouper.count() -> dag03
 * Grouper.count(field) -> dag03
 * Grouper.reduceGroup(GroupReduceFunction) -> dag02
 * Grouper.combineGroup(fn) -> dag08
 * Grouper.combineReduceGroup(fn) -> dag08
 * == grouped aggregation called directly on DataSet ==
 * groupBy(ParallelGroupByAgg) -> dag01
 * combineGroup(fn, shards) -> dag05
 * combineReduceGroup(fn, shards) -> dag05
 * reduceGroup(ReduceGroup, outMeta) -> dag09
 * reduceGroup(GroupReduceFunction) over the whole table -> dag09
 * groupBySum(measure, keys...) -> dag04
 * == whole-table scalar aggregation (wholeAgg, a single row) ==
 * sum(field) -> dag10
 * max(field) -> dag10
 * min(field) -> dag10
 * count() -> dag10
 * count(field) -> dag10
 * == deduplication ==
 * distinct(Distinct) reached through distinct(String...) -> dag07, because distinct(String...) calls distinct(Distinct) internally
 * distinct(String...) -> dag02, dag07
 * == binary operators ==
 * join(right, SortMergeJoin) -> dag09
 * join(right).on(key) fluent INNER -> dag02
 * leftJoin(right).on -> dag04
 * rightJoin(right).on -> dag04
 * fullJoin(right).on -> dag05
 * join(right).withFunc(keys, JoinFunction) -> dag08
 * coGroup(right, CoGroupFunction).on(...) -> dag02
 * coGroup(right, CoGroup op) -> dag09
 * union(other, Union op) -> dag04
 * union(other) convenience overload -> dag05
 * </pre>
 * Note: {@code distinct(Distinct)} is the underlying implementation of {@code distinct(String...)}.
 * The latter builds {@code Distinct.byColumns} and then calls the former, so exercising
 * {@code distinct(String...)} also traverses the {@code distinct(Distinct)} path and both are covered.
 */
@Tag("multiprocess")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FullOperatorThreeWayTest {

    private static final DataSchema ORDER = TenDagData.ORDER;
    private static final DataSchema DIM = TenDagData.DIM;
    private static final DataSchema REGION_DIM = TenDagData.REGION_DIM;

    // ---- Helpers, copied from ThreeWayEquivalenceTest because package-private members are not reachable across packages ----

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

    // reduceGroup over the whole table (no key, one group) -> [cnt] as a Long; used by the whole-table reduceGroup(GroupReduceFunction) overload in dag09.
    static final GroupReduceFunction RG_WHOLE_COUNT = new GroupReduceFunction() {
        private final DataSchema out = new DataSchema(new Field("cnt", DataType.LongType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void reduce(Iterable<DataRow> rows, Collector c) {
            long n = 0; for (DataRow ignored : rows) n++;
            DataRow o = new DataRow(1); o.set(0, n); c.collect(o);
        }
    };

    // The function used by reduceGroup(ReduceGroup, outMeta): keyed by dept -> [dept, sum of amount].
    static final GroupReduceFunction RG_SUM_AMOUNT = new GroupReduceFunction() {
        private final DataSchema out = new DataSchema(new Field("dept", DataType.IntegerType),
                new Field("amt", DataType.BigDecimalType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void reduce(Iterable<DataRow> rows, Collector c) {
            BigDecimal s = BigDecimal.ZERO; Object k = null;
            for (DataRow r : rows) { s = s.add((BigDecimal) r.get(4)); k = r.get(1); }
            DataRow o = new DataRow(2); o.set(0, k); o.set(1, s.setScale(10, RoundingMode.HALF_UP)); c.collect(o);
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

    // join withFunc: (leftOrder, rightDim) -> [id, dept, bonus]; the missing side contributes null, INNER keeps rows where both sides are non-null.
    static final JoinFunction JOIN_FN = new JoinFunction() {
        private final DataSchema out = new DataSchema(new Field("id", DataType.LongType),
                new Field("dept", DataType.IntegerType), new Field("bonus", DataType.IntegerType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public DataRow join(DataRow l, DataRow r) {
            DataRow o = new DataRow(3);
            o.set(0, l == null ? null : l.get(0));
            o.set(1, l == null ? null : l.get(1));
            o.set(2, r == null ? null : r.get(1));
            return o;
        }
    };

    private static SortField desc(int i) { return new SortField(i, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST); }
    private static SortField asc(int i) { return new SortField(i, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST); }
    private static Field[] fld(Field... x) { return x; }

    private Path dir;
    private String ordersA, ordersB, supp, dim, regionDim;
    private RealProcessCluster cluster;

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("fullop-3way");
        ordersA = TenDagData.orders(dir, "ordersA", 30_000);
        ordersB = TenDagData.orders(dir, "ordersB", 30_000);
        supp = TenDagData.supplement(dir, "supp", 15_000, 15_000);
        dim = TenDagData.dim(dir);
        regionDim = TenDagData.regionDim(dir);
        cluster = new RealProcessCluster(2, 32);  // one master and two workers, as separate processes
    }

    @AfterAll
    void tearDown() {
        if (cluster != null) cluster.close();
        if (dir != null) deleteRec(dir.toFile());
    }

    private static void deleteRec(File f) {
        File[] ch = f.listFiles();
        if (ch != null) for (File c : ch) deleteRec(c);
        f.delete();
    }

    private CsvInput csv(String path, DataSchema meta) { return new CsvInput(path, meta); }

    @FunctionalInterface
    private interface Dag { void build(BatchSession s, Io ioCols); }

    /**
     * Collect the (DataSchema, file path) of every output so the files can be read back and compared after the run.
     */
    private final class Io {
        final String tag;
        final List<DataSchema> metas = new ArrayList<>();
        final List<Path> paths = new ArrayList<>();
        int used = 0;
        Io(String tag) { this.tag = tag; }
        void out(DataSet ds) {
            Path p = dir.resolve(tag + "-o" + used++ + ".tsv");
            metas.add(ds.getSchema());
            paths.add(p);
            ds.sinkTo(new FileSink(ds.getSchema(), p.toString()));
        }
    }

    /**
     * Run once (single machine with client null, pseudo cluster, or real cluster) and return the rows read back from each output with its schema.
     */
    private List<List<DataRow>> run(String name, int par, JobClient client, Dag dag, List<DataSchema> outMetas) {
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

    /**
     * Multiset fingerprint: rows sorted, one line each, BigDecimal normalized to scale 10 HALF_UP.
     */
    private List<String> fingerprint(DataSchema meta, List<DataRow> rows) {
        List<String> out = new ArrayList<>(rows.size());
        for (DataRow r : rows) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < meta.getFieldCount(); i++) {
                Object v = r.get(i);
                if (v instanceof BigDecimal) v = ((BigDecimal) v).setScale(10, RoundingMode.HALF_UP).toPlainString();
                sb.append(v).append('|');
            }
            out.add(sb.toString());
        }
        out.sort(null);
        return out;
    }

    /**
     * Compare P1, P4 and P2: assert every output fingerprint is pairwise equal and that the real cluster spans two workers.
     */
    private void assertThreeWay(String name, Dag dag) throws Exception {
        List<DataSchema> m1 = new ArrayList<>(), mlc = new ArrayList<>(), mc = new ArrayList<>();
        List<List<DataRow>> p1 = run(name + "-s1", 1, null, dag, m1);

        List<List<DataRow>> lc4;
        try (InProcessCluster lc = new InProcessCluster(4, new File(dir.toFile(), name + "-lc"))) {
            lc4 = run(name + "-lc4", 4, lc, dag, mlc);
        }

        List<List<DataRow>> c2 = run(name + "-c2", 2, cluster.newClient(), dag, mc);

        assertEquals(p1.size(), lc4.size(), name + " in-process cluster output counts agree");
        assertEquals(p1.size(), c2.size(), name + " real cluster output counts agree");
        for (int i = 0; i < p1.size(); i++) {
            DataSchema meta = m1.get(i);
            List<String> f1 = fingerprint(meta, p1.get(i));
            assertFalse(f1.isEmpty(), name + " out" + i + " single-process P1 produces output");
            assertEquals(f1, fingerprint(meta, lc4.get(i)), name + " out" + i + " single-process P1 and in-process cluster P4 differ as multisets");
            assertEquals(f1, fingerprint(meta, c2.get(i)), name + " out" + i + " single-process P1 and real cluster P2 differ as multisets");
        }

        // On the real-cluster side, assert that instances really span two workers, read through the monitor HTTP API.
        Set<String> workers = new LinkedHashSet<>();
        for (String jid : cluster.jobIds()) {
            workers.addAll(cluster.stageWorkerDistribution(jid));
        }
        assertTrue(workers.size() >= 2, name + " must really span 2 workers (cross-JVM shuffle), actual:" + workers);
    }

    // ================ dag01: the linear trunk, covering filter(fn)/select(idx)/map/addFields/removeFields/
    // partitionByHash/groupBy(ParallelGroupByAgg)/orderBy(OrderBy)/top(Top) plus select(String...) =================
    @Test
    void dag01_linearBackbone() throws Exception {
        assertThreeWay("dag01", (s, ioCols) -> {
            DataSet pre = s.fromSource(csv(ordersA, ORDER))
                    .filter(amountGe(10))
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .map(MAP_ID)
                    .addFields(fld(new Field("tag", DataType.IntegerType)), new Object[]{1})
                    .removeFields("tag", "qty")
                    .partitionByHash("region");
            DataSchema preMeta = pre.getSchema();
            DataSchema gb = ParallelGroupByAgg.of(preMeta, "region").sum("amount").getOutputSchema();
            ioCols.out(pre.groupBy(ParallelGroupByAgg.of(preMeta, "region").sum("amount"))
                    .orderBy(OrderBy.of(gb, desc(1)), 1, false)
                    .top(Top.of(gb, 5, desc(1)), 1)
                    .select(0, 1));
        });
    }

    // ================= dag02:join(fluent).on + coGroup(fn).on + Grouper.reduceGroup + flatMap + distinct(String...) + select(idx) + filter(String) =================
    @Test
    void dag02_joinCoGroupFluent() throws Exception {
        assertThreeWay("dag02", (s, ioCols) -> {
            ioCols.out(s.fromSource(csv(ordersA, ORDER))
                    .join(s.fromSource(csv(ordersB, ORDER))).on("id")
                    .filter("amount >= 0")
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept")
                    .select(0, 1, 4));
            ioCols.out(s.fromSource(csv(ordersA, ORDER))
                    .coGroup(s.fromSource(csv(supp, ORDER)), CG_FN).on("dept")
                    .filter("lc >= 1")
                    .groupBy("dept").reduceGroup(RG_SUM_LC));
        });
    }

    // ================= dag03:filter(String,params) + Grouper.sum/max/min/count()/count(field) =================
    @Test
    void dag03_grouperAggsAndFilterParams() throws Exception {
        assertThreeWay("dag03", (s, ioCols) -> {
            Map<String, Object> p = new HashMap<>();
            p.put("lo", 10);
            DataSet base = s.fromSource(csv(ordersA, ORDER)).filter("amount >= lo", p);
            ioCols.out(base.groupBy("dept").sum("amount"));
            ioCols.out(base.groupBy("dept").max("amount"));
            ioCols.out(base.groupBy("dept").min("amount"));
            ioCols.out(base.groupBy("region").count());
            ioCols.out(base.groupBy("region").count("amount"));
        });
    }

    // ================ dag04: union(other,Union) and union(other) up front, then leftJoin/rightJoin, groupBySum, partitionByHash and select(String...) =================
    @Test
    void dag04_unionJoinGroupBySum() throws Exception {
        assertThreeWay("dag04", (s, ioCols) -> {
            DataSet u = s.fromSource(csv(ordersA, ORDER))
                    .union(s.fromSource(csv(ordersB, ORDER)), Union.of(ORDER))
                    .union(s.fromSource(csv(supp, ORDER)))
                    .partitionByHash("region");
            ioCols.out(u.groupBySum("amount", "region"));
            DataSet lj = s.fromSource(csv(ordersA, ORDER))
                    .leftJoin(s.fromSource(csv(dim, DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            DataSet rj = s.fromSource(csv(ordersB, ORDER))
                    .rightJoin(s.fromSource(csv(dim, DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty");
            ioCols.out(lj.union(rj).groupBySum("amount", "dept"));
        });
    }

    // ================ dag05: combineGroup(fn,shards) and combineReduceGroup(fn,shards), fullJoin, the convenience union, and filter(String) =================
    @Test
    void dag05_combineShardsFullJoin() throws Exception {
        assertThreeWay("dag05", (s, ioCols) -> {
            DataSet cg = s.fromSource(csv(ordersA, ORDER))
                    .combineGroup(COMBINE_SUM, 4)
                    .groupBy(ParallelGroupByAgg.of(ORDER, "dept").sum("amount"));
            DataSet cr = s.fromSource(csv(ordersB, ORDER))
                    .filter("amount >= 0")
                    .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
            ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
            ioCols.out(cg.union(cr));
        });
    }

    // ================ dag06: the convenience orderBy(String...) and top(int), select(String...), filter(String) and the regionDim join =================
    @Test
    void dag06_convenienceOrderTop() throws Exception {
        assertThreeWay("dag06", (s, ioCols) -> {
            ioCols.out(s.fromSource(csv(ordersA, ORDER))
                    .filter("amount >= 0")
                    .join(s.fromSource(csv(regionDim, REGION_DIM))).on("region")
                    .select("id", "dept", "region", "amount", "rname")
                    .orderBy("id")
                    .top(50));
        });
    }

    // ================ dag07: map, flatMap, distinct(String...) which reaches distinct(Distinct), and filter(FilterFunction) =================
    @Test
    void dag07_distinctMapFlatMap() throws Exception {
        assertThreeWay("dag07", (s, ioCols) -> {
            ioCols.out(s.fromSource(csv(ordersA, ORDER))
                    .filter(amountGe(0))
                    .map(MAP_ID)
                    .flatMap(FLATMAP_GRADE)
                    .distinct("id", "dept", "region", "grade", "amount", "qty")
                    .select(0, 1, 2, 3, 4, 5));
            // An explicit distinct(Distinct op) with allColumns, covering the overload and the all-columns dedup path that distinct(String...) leaves untouched.
            ioCols.out(s.fromSource(csv(ordersA, ORDER))
                    .distinct(Distinct.allColumns(ORDER)));
        });
    }

    // ================= dag08:join(fluent).withFunc(keys,JoinFunction) + Grouper.combineGroup(fn) + Grouper.combineReduceGroup(fn) =================
    @Test
    void dag08_withFuncCombineGrouper() throws Exception {
        assertThreeWay("dag08", (s, ioCols) -> {
            ioCols.out(s.fromSource(csv(ordersA, ORDER))
                    .join(s.fromSource(csv(dim, DIM))).withFunc(new String[]{"dept"}, new String[]{"dept"}, JOIN_FN));
            ioCols.out(s.fromSource(csv(ordersA, ORDER)).groupBy("dept").combineGroup(COMBINE_SUM));
            ioCols.out(s.fromSource(csv(ordersB, ORDER)).groupBy("dept").combineReduceGroup(COMBINE_REDUCE_SUM));
        });
    }

    // ================ dag09: join(SortMergeJoin op), coGroup(CoGroup op), reduceGroup(ReduceGroup,outMeta) and whole-table reduceGroup(GroupReduceFunction) =================
    @Test
    void dag09_lowLevelOps() throws Exception {
        assertThreeWay("dag09", (s, ioCols) -> {
            SortMergeJoin smj = SortMergeJoin.of(ORDER, ORDER, new String[]{"id"}, new String[]{"id"}, JoinType.INNER);
            ioCols.out(s.fromSource(csv(ordersA, ORDER))
                    .join(s.fromSource(csv(ordersB, ORDER)), smj)
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .reduceGroup(ReduceGroup.of(ORDER, new String[]{"dept"}, RG_SUM_AMOUNT), RG_SUM_AMOUNT.getResultSchema()));
            CoGroup cg = CoGroup.of(ORDER, ORDER, new String[]{"dept"}, new String[]{"dept"}, CG_FN);
            ioCols.out(s.fromSource(csv(ordersA, ORDER))
                    .coGroup(s.fromSource(csv(supp, ORDER)), cg));
            ioCols.out(s.fromSource(csv(ordersA, ORDER))
                    .filter("amount >= 0")
                    .reduceGroup(RG_WHOLE_COUNT));
        });
    }

    // ================ dag10: whole-table scalar aggregation sum(field)/max(field)/min(field)/count()/count(field) =================
    @Test
    void dag10_wholeTableAggs() throws Exception {
        assertThreeWay("dag10", (s, ioCols) -> {
            ioCols.out(s.fromSource(csv(ordersA, ORDER)).sum("amount"));
            ioCols.out(s.fromSource(csv(ordersA, ORDER)).max("amount"));
            ioCols.out(s.fromSource(csv(ordersA, ORDER)).min("amount"));
            ioCols.out(s.fromSource(csv(ordersA, ORDER)).count());
            ioCols.out(s.fromSource(csv(ordersA, ORDER)).count("amount"));
        });
    }

    /**
     * filter(FilterFunction): amount >= threshold compared as BigDecimal. Serializable, so it ships to the cluster.
     */
    private static io.rowweave.FilterFunction amountGe(int threshold) {
        final BigDecimal t = BigDecimal.valueOf(threshold);
        return row -> ((BigDecimal) row.get(4)).compareTo(t) >= 0;
    }
}
