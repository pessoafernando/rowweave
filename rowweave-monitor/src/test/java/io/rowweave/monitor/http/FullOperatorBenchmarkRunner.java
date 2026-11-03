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
import io.rowweave.ExecutionEnvironment;
import io.rowweave.JoinFunction;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.bench.CsvInput;
import io.rowweave.cluster.InProcessCluster;
import io.rowweave.cluster.JobClient;
import io.rowweave.cluster.TenDagData;
import io.rowweave.ops.Distinct;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.ReduceGroup;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import io.rowweave.ops.Union;
import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Benchmark runner for the full operator surface on three runtimes (a main method, not JUnit).
 *
 * <p>The same ten DAGs whose correctness {@link FullOperatorThreeWayTest} establishes are timed on
 * a single machine (P1), the in-process pseudo cluster InProcessCluster (P4) and a real process
 * cluster (P2). Correctness is checked by pairing a streaming multiset fingerprint of every output
 * file, which is order-insensitive: the sum of the per-line text hashes plus the row count.
 *
 * <p>{@code -Drowweave.rows} sets the volume, 50 million rows by default. A result that large cannot
 * be collected into a list and compared, so {@link #fingerprint(Path)} hashes one line at a time
 * through a BufferedReader. The cost of forking the real cluster is paid outside the loop, so it
 * stays out of the per-DAG timings.
 *
 * <p>How to run it, outside JUnit:
 * <pre>
 * java -Drowweave.rows=200000 -cp "rowweave-monitor/target/test-classes:rowweave-monitor/target/classes:$(cat /tmp/cp.txt)" \
 * io.rowweave.monitor.http.FullOperatorBenchmarkRunner
 * </pre>
 */
public final class FullOperatorBenchmarkRunner {

    private FullOperatorBenchmarkRunner() { }

    private static final DataSchema ORDER = TenDagData.ORDER;
    private static final DataSchema DIM = TenDagData.DIM;
    private static final DataSchema REGION_DIM = TenDagData.REGION_DIM;

    // ==================== Helpers, the same code as in FullOperatorThreeWayTest ====================

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

    // reduceGroup over the whole table (no key, one group) -> [cnt] as a Long.
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

    /**
     * filter(FilterFunction): amount >= threshold compared as BigDecimal. Serializable, so it ships to the cluster.
     */
    private static io.rowweave.FilterFunction amountGe(int threshold) {
        final BigDecimal t = BigDecimal.valueOf(threshold);
        return row -> ((BigDecimal) row.get(4)).compareTo(t) >= 0;
    }

    // ==================== DAG definitions ====================

    @FunctionalInterface
    private interface Dag { void build(ExecutionEnvironment s, Io ioCols); }

    /**
     * Each output writes one FileSink file, named by tag and index; after the run those files are fingerprinted and compared.
     */
    private static final class Io {
        final Path dir;
        final String tag;
        final List<Path> paths = new ArrayList<>();
        int used = 0;
        Io(Path dir, String tag) { this.dir = dir; this.tag = tag; }
        void out(DataSet ds) {
            Path p = dir.resolve(tag + "-o" + used++ + ".tsv");
            paths.add(p);
            ds.sinkTo(new FileSink(ds.getSchema(), p.toString()));
        }
    }

    // Input CSV paths, filled in by setUp.
    private static String ordersA, ordersB, supp, dim, regionDim;

    // The ten DAG builders, named as in FullOperatorThreeWayTest (dag01..dag10).
    private static final Dag dag01 = (s, ioCols) -> {
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
    };

    private static final Dag dag02 = (s, ioCols) -> {
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
    };

    private static final Dag dag03 = (s, ioCols) -> {
        Map<String, Object> p = new HashMap<>();
        p.put("lo", 10);
        DataSet base = s.fromSource(csv(ordersA, ORDER)).filter("amount >= lo", p);
        ioCols.out(base.groupBy("dept").sum("amount"));
        ioCols.out(base.groupBy("dept").max("amount"));
        ioCols.out(base.groupBy("dept").min("amount"));
        ioCols.out(base.groupBy("region").count());
        ioCols.out(base.groupBy("region").count("amount"));
    };

    private static final Dag dag04 = (s, ioCols) -> {
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
    };

    private static final Dag dag05 = (s, ioCols) -> {
        DataSet cg = s.fromSource(csv(ordersA, ORDER))
                .combineGroup(COMBINE_SUM, 4)
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept").sum("amount"));
        DataSet cr = s.fromSource(csv(ordersB, ORDER))
                .filter("amount >= 0")
                .combineReduceGroup(COMBINE_REDUCE_SUM, 4);
        ioCols.out(cg.fullJoin(cr).on(new String[]{"dept"}, new String[]{"dept"}));
        ioCols.out(cg.union(cr));
    };

    private static final Dag dag06 = (s, ioCols) -> {
        ioCols.out(s.fromSource(csv(ordersA, ORDER))
                .filter("amount >= 0")
                .join(s.fromSource(csv(regionDim, REGION_DIM))).on("region")
                .select("id", "dept", "region", "amount", "rname")
                .orderBy("id")
                .top(50));
    };

    private static final Dag dag07 = (s, ioCols) -> {
        ioCols.out(s.fromSource(csv(ordersA, ORDER))
                .filter(amountGe(0))
                .map(MAP_ID)
                .flatMap(FLATMAP_GRADE)
                .distinct("id", "dept", "region", "grade", "amount", "qty")
                .select(0, 1, 2, 3, 4, 5));
        ioCols.out(s.fromSource(csv(ordersA, ORDER))
                .distinct(Distinct.allColumns(ORDER)));
    };

    private static final Dag dag08 = (s, ioCols) -> {
        ioCols.out(s.fromSource(csv(ordersA, ORDER))
                .join(s.fromSource(csv(dim, DIM))).withFunc(new String[]{"dept"}, new String[]{"dept"}, JOIN_FN));
        ioCols.out(s.fromSource(csv(ordersA, ORDER)).groupBy("dept").combineGroup(COMBINE_SUM));
        ioCols.out(s.fromSource(csv(ordersB, ORDER)).groupBy("dept").combineReduceGroup(COMBINE_REDUCE_SUM));
    };

    private static final Dag dag09 = (s, ioCols) -> {
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
    };

    private static final Dag dag10 = (s, ioCols) -> {
        ioCols.out(s.fromSource(csv(ordersA, ORDER)).sum("amount"));
        ioCols.out(s.fromSource(csv(ordersA, ORDER)).max("amount"));
        ioCols.out(s.fromSource(csv(ordersA, ORDER)).min("amount"));
        ioCols.out(s.fromSource(csv(ordersA, ORDER)).count());
        ioCols.out(s.fromSource(csv(ordersA, ORDER)).count("amount"));
    };

    private static final String[] NAMES = {
            "dag01", "dag02", "dag03", "dag04", "dag05",
            "dag06", "dag07", "dag08", "dag09", "dag10"};
    private static final Dag[] DAGS = {
            dag01, dag02, dag03, dag04, dag05, dag06, dag07, dag08, dag09, dag10};

    private static CsvInput csv(String path, DataSchema meta) { return new CsvInput(path, meta); }

    // ==================== Streaming fingerprint: order-insensitive, never collected into a list ====================

    /**
     * Multiset fingerprint: the sum of the per-line text hashes (independent of row order) plus the row count. Constant memory over 50 million rows.
     */
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
        } catch (Exception e) {
            throw new RuntimeException("fingerprint " + p + " failed", e);
        }
        return new long[]{count, sum};
    }

    // ==================== Execution and timing ====================

    /**
     * Run one DAG with build and commit under wall-clock timing; return the streaming fingerprint of each output and the elapsed milliseconds.
     */
    private static Result runOne(Path dir, String name, int par, JobClient client, Dag dag) {
        Io ioCols = new Io(dir, name);
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : RowWeave.createEnvironment(name);
        s.setParallelism(par);
        long t0 = System.nanoTime();
        dag.build(s, ioCols);
        s.execute();
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        List<long[]> fps = new ArrayList<>(ioCols.paths.size());
        for (Path p : ioCols.paths) {
            fps.add(fingerprint(p));
            // At 50 million rows the fingerprints are the comparison authority. Reclaim each
            // completed output before the next execution so the three modes do not retain it.
            try {
                Files.delete(p);
            } catch (IOException e) {
                throw new RuntimeException("delete benchmark output " + p + " failed", e);
            }
        }
        return new Result(ms, fps);
    }

    private static final class Result {
        final long ms;
        final List<long[]> fps;
        Result(long ms, List<long[]> fps) { this.ms = ms; this.fps = fps; }
    }

    /**
     * Whether all three fingerprints agree: the same number of outputs, and equal count and sum for each output.
     */
    private static boolean fpEquals(List<long[]> a, List<long[]> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i)[0] != b.get(i)[0] || a.get(i)[1] != b.get(i)[1]) return false;
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        long rows = Long.getLong("rowweave.rows", 50_000_000L);
        String onlyDag = System.getProperty("rowweave.benchmark.onlyDag");
        String directoryOverride = System.getProperty("rowweave.benchmark.dir");
        if (onlyDag != null && !java.util.Arrays.asList(NAMES).contains(onlyDag)) {
            throw new IllegalArgumentException("unknown benchmark DAG: " + onlyDag);
        }
        System.out.println("=== three-way performance comparison benchmark ===");
        System.out.println("rows = " + rows + "  (override with -Drowweave.rows)");

        Path dir = directoryOverride == null
                ? Files.createTempDirectory("fullop-bench") : Paths.get(directoryOverride);
        Files.createDirectories(dir);
        System.out.println("tmpDir = " + dir);

        // ---- generate the data, with the elapsed time printed ----
        long g0 = System.nanoTime();
        ordersA = TenDagData.orders(dir, "ordersA", rows);
        ordersB = TenDagData.orders(dir, "ordersB", rows);
        supp = TenDagData.supplement(dir, "supp", rows / 2, rows / 2);
        dim = TenDagData.dim(dir);
        regionDim = TenDagData.regionDim(dir);
        long genMs = (System.nanoTime() - g0) / 1_000_000L;
        System.out.println("data generated in " + genMs + " ms  (ordersA/ordersB=" + rows + ", supp=" + (rows / 2) + ")");

        // ---- Start the real cluster outside the loop, so fork cost stays out of the per-DAG timings ----
        // The worker heap defaults to 1024MB (-Drowweave.workerHeapMb): a small heap forces genuine managed-memory spill rather than hiding it under a large one. The master takes 512MB because it holds no data.
        int workerHeapMb = Integer.getInteger("rowweave.workerHeapMb", 1024);
        int masterHeapMb = Integer.getInteger("rowweave.masterHeapMb", 512);
        // dag02 alone requires seven assignments per Worker. Slots admit tasks; the managed-memory
        // budget remains independently enforced by each Worker's 1 GiB heap envelope.
        int workerSlots = Integer.getInteger("rowweave.benchmark.workerSlots", 16);
        System.out.println("starting the real process cluster (1 master + 2 workers, slots=" + workerSlots
                + ",worker -Xmx" + workerHeapMb + "m)...");
        RealProcessCluster cluster = new RealProcessCluster(2, workerSlots, workerHeapMb, masterHeapMb);

        long[] s1All = new long[DAGS.length];
        long[] lcAll = new long[DAGS.length];
        long[] c2All = new long[DAGS.length];
        boolean[] ok = new boolean[DAGS.length];

        try {
            for (int i = 0; i < DAGS.length; i++) {
                String name = NAMES[i];
                if (onlyDag != null && !onlyDag.equals(name)) {
                    continue;
                }
                Dag dag = DAGS[i];
                System.out.println("---- " + name + " ----");

                // Single machine P1
                System.out.println("MODE_START " + name + " P1");
                Result r1 = runOne(dir, name + "-s1", 1, null, dag);
                s1All[i] = r1.ms;

                // In-process pseudo cluster P4
                Result rlc;
                System.out.println("MODE_START " + name + " P4");
                try (InProcessCluster lc = new InProcessCluster(4, new File(dir.toFile(), name + "-lc"))) {
                    rlc = runOne(dir, name + "-lc4", 4, lc, dag);
                }
                lcAll[i] = rlc.ms;

                // Real process cluster P2
                System.out.println("MODE_START " + name + " P2");
                Result rc = runOne(dir, name + "-c2", 2, cluster.newClient(), dag);
                c2All[i] = rc.ms;

                boolean lcEq = fpEquals(r1.fps, rlc.fps);
                boolean c2Eq = fpEquals(r1.fps, rc.fps);
                ok[i] = lcEq && c2Eq;
                System.out.printf("   P1=%dms  localP4=%dms  realP2=%dms  equal=%s%s%n",
                        r1.ms, rlc.ms, rc.ms, ok[i] ? "OK" : "FAIL",
                        ok[i] ? "" : " (P1 vs localP4=" + lcEq + " P1 vs realP2=" + c2Eq + ")");
                if (!ok[i]) {
                    System.out.println("   !! per-output detail for the mismatch:");
                    dumpFps("     P1 ", r1.fps);
                    dumpFps("     localP4", rlc.fps);
                    dumpFps("     realP2", rc.fps);
                }
            }
        } finally {
            cluster.close();
        }

        if (onlyDag == null) {
            printTable(s1All, lcAll, c2All, ok);
        } else {
            System.out.println("selected DAG " + onlyDag + " consistent="
                    + ok[java.util.Arrays.asList(NAMES).indexOf(onlyDag)]);
        }
        if (directoryOverride == null) {
            deleteRec(dir.toFile());
        }
    }

    private static void dumpFps(String label, List<long[]> fps) {
        StringBuilder sb = new StringBuilder(label).append(": ");
        for (int i = 0; i < fps.size(); i++) {
            sb.append("out").append(i).append("=[cnt=").append(fps.get(i)[0])
              .append(",h=").append(fps.get(i)[1]).append("] ");
        }
        System.out.println(sb.toString());
    }

    private static void printTable(long[] s1, long[] lc, long[] c2, boolean[] ok) {
        System.out.println();
        System.out.println("================ performance table ================");
        System.out.printf("%-8s %10s %10s %10s %10s %10s %10s%n",
                "DAG", "single P1ms", "local P4ms", "real P2ms", "real/single", "local/single", "correctness");
        long ts1 = 0, tlc = 0, tc2 = 0; int okCount = 0;
        for (int i = 0; i < s1.length; i++) {
            String gapC = s1[i] > 0 ? String.format("%.2fx", c2[i] / (double) s1[i]) : "-";
            String gapL = s1[i] > 0 ? String.format("%.2fx", lc[i] / (double) s1[i]) : "-";
            System.out.printf("%-8s %10d %10d %10d %10s %10s %10s%n",
                    NAMES[i], s1[i], lc[i], c2[i], gapC, gapL, ok[i] ? "OK" : "FAIL");
            ts1 += s1[i]; tlc += lc[i]; tc2 += c2[i];
            if (ok[i]) okCount++;
        }
        System.out.println("-------------------------------------------");
        String tGapC = ts1 > 0 ? String.format("%.2fx", tc2 / (double) ts1) : "-";
        String tGapL = ts1 > 0 ? String.format("%.2fx", tlc / (double) ts1) : "-";
        System.out.printf("%-8s %10d %10d %10d %10s %10s %10s%n",
                "TOTAL", ts1, tlc, tc2, tGapC, tGapL, okCount + "/" + s1.length);
        System.out.println("===========================================");
        System.out.println(okCount == s1.length
                ? "correctness: all three agree OK"
                : "correctness: mismatch found FAIL(" + okCount + "/" + s1.length + ")");
    }

    private static void deleteRec(File f) {
        File[] ch = f.listFiles();
        if (ch != null) for (File c : ch) deleteRec(c);
        f.delete();
    }
}
