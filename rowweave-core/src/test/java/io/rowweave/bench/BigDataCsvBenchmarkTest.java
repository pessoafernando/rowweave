package io.rowweave.bench;

import io.rowweave.RowWeave;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.cluster.ClusterJobClient;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Large-data performance benchmark with a correctness comparison. All data comes from CSV
 * files on disk, written by CsvDataGen in a streaming fashion rather than built in memory or
 * in a database. The source is {@link CsvInput}, a line-by-line BufferedReader that holds O(1)
 * memory and materializes nothing, and the sink is {@link FileSink}, so results land on disk
 * for comparison.
 *
 * <p>CSV files suit this benchmark better than a database: the generator writes files with O(1)
 * memory, so tens of millions of rows neither exhaust the heap nor contend with connections; the
 * source is the lightest possible, holding a path, serializing to a worker, and reading through a
 * shared file system on the same machine; and there are no connections, locks or query timeouts to
 * make a run flaky.
 *
 * <p>Three modes are compared fairly, all reading the same CSV: single machine at parallelism 1,
 * single machine at parallelism 4 with intra-pipeline concurrency, and a real three-process
 * cluster of one Master and two Worker JVMs that each read by path, with RPC, socket shuffle and
 * the general DAG scheduler.
 *
 * <p>Timing excludes the CSV read: each scenario first measures a read baseline, draining and
 * discarding the source rows serially, and the reported engine time is the total minus that
 * baseline. Six scenarios cover every operator plus multi-hop DAGs, and the FileSink multiset
 * fingerprints of the three modes must match bit for bit, checked with O(1) memory so tens of
 * millions of rows do not exhaust the heap.
 *
 * <p>The scale is configurable through {@code -Drowweave.bench.orders}, which defaults to 50K so
 * the suite runs, with millions or tens of millions set by hand. Tagged bench.
 */
@Tag("bench")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BigDataCsvBenchmarkTest {

    private static final long ORDERS = Long.getLong("rowweave.bench.orders", 50_000L);
    private static final long PARTNERS = Long.getLong("rowweave.bench.partners", ORDERS);
    private static final int DEPTS = 200;
    private static final int REGIONS = 50;

    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema BONUS = new DataSchema(
            new Field("dept", DataType.IntegerType),
            new Field("bonus", DataType.IntegerType));
    private static final DataSchema PARTNER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("pamount", DataType.IntegerType));
    private static final DataSchema GB_REGION =
            ParallelGroupByAgg.of(ORDER, "region").sum("amount").getOutputSchema();
    private static final DataSchema JOIN = SortMergeJoin.of(
            ORDER, BONUS, new String[]{"dept"}, new String[]{"dept"}, JoinType.INNER).getOutputSchema();
    private static final DataSchema JOIN_GB =
            ParallelGroupByAgg.of(JOIN, "region").sum("amount").getOutputSchema();
    private static final DataSchema BIG_JOIN = SortMergeJoin.of(
            ORDER, PARTNER, new String[]{"id"}, new String[]{"id"}, JoinType.INNER).getOutputSchema();

    private Path dataDir;
    private String ordersCsv;
    private String bonusesCsv;
    private String partnersCsv;
    private final List<String[]> report = new ArrayList<>();

    private Process master;
    private Process worker0;
    private Process worker1;
    private int masterPort;

    @BeforeAll
    void setUp() throws Exception {
        // The CSV data set goes in a fixed directory, so a separate worker process can read it by
        // path and a data set already generated is reused.
        dataDir = Paths.dataDir(ORDERS);
        Files.createDirectories(dataDir);
        long t = System.nanoTime();
        ordersCsv = CsvDataGen.orders(dataDir, ORDERS, DEPTS, REGIONS).toString();
        bonusesCsv = CsvDataGen.bonuses(dataDir, DEPTS).toString();
        partnersCsv = PARTNERS > 0 ? CsvDataGen.partners(dataDir, PARTNERS).toString() : null;
        long genMs = (System.nanoTime() - t) / 1_000_000;
        System.out.println("=== CSV ready: orders=" + ORDERS + " rows, bonuses=" + DEPTS + " rows, partners="
                + PARTNERS + " rows (on-disk files, generated or reused " + genMs + "ms, directory " + dataDir + ") ===");

        // A large job runs for a long time, so the Master stage barrier timeout scales with the data
        // size, and the client read timeout matches it (ClusterJobClient defaults to 10min).
        String stageTimeout = "-Drowweave.cluster.stageTimeoutMs=" + Math.max(120_000L, ORDERS / 5);
        // Worker memory is set explicitly for a controllable benchmark: the heap -Xmx (2G by default)
        // plus the managed pool rowweave.worker.memory (1G by default, with a 1G minimum).
        String workerXmx = "-Xmx" + System.getProperty("rowweave.bench.workerXmx", "2g");
        String workerMem = "-Drowweave.worker.memory=" + Long.getLong("rowweave.bench.workerMem", 1024L * 1024 * 1024);
        // With admission control on by default, a parallelism-4 join job demands about 17 or 18 slots per
        // worker, which is the sum of stage instances, so 16 is refused and the limit is raised to 32.
        // Slots cap daemon threads and admission rather than CPU count, so 32 is comfortable, and a
        // join over tens of millions of rows needs enough semaphore parallelism anyway, where 4 would
        // stall the sort phase.
        int slots = Integer.getInteger("rowweave.bench.workerSlots", 32);
        String workerSlots = "-Drowweave.worker.slots=" + slots;
        StringBuilder mp = new StringBuilder();
        master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0", stageTimeout),
                "MASTER_PORT=", mp);
        masterPort = Integer.parseInt(mp.toString());
        worker0 = launch(Arrays.asList(workerXmx, "-Drowweave.worker.enable=true",
                "-Drowweave.worker.master.port=" + masterPort, workerSlots, workerMem, stageTimeout), "WORKER_READY=", new StringBuilder());
        worker1 = launch(Arrays.asList(workerXmx, "-Drowweave.worker.enable=true",
                "-Drowweave.worker.master.port=" + masterPort, workerSlots, workerMem, stageTimeout), "WORKER_READY=", new StringBuilder());
        System.out.println("=== real cluster ready: master@" + masterPort + " + 2 workers(each -Xmx"
                + System.getProperty("rowweave.bench.workerXmx", "2g") + ", rowweave.worker.memory="
                + (Long.getLong("rowweave.bench.workerMem", 1024L * 1024 * 1024) / 1024 / 1024) + "MB, slots=" + slots + ") ===");
    }

    @AfterAll
    void tearDown() {
        kill(worker0);
        kill(worker1);
        kill(master);
        System.out.println();
        System.out.println("====== large-volume four-engine benchmark (orders=" + ORDERS + ", streaming CSV reads, engine net time already excludes the CSV read I/O) ======");
        System.out.printf("%-30s | %9s | %9s | %10s | %10s | %10s | %11s%n",
                "scenario", "outRows", "csvReadBase", "javaOnlyNet", "procP1Net", "procP4Net", "clusterNet");
        System.out.println("---------------------------------------------------------------------------------------------------------------");
        for (String[] r : report) {
            System.out.printf("%-30s | %9s | %7sms | %8sms | %8sms | %8sms | %9sms%n",
                    r[0], r[1], r[2], r[3], r[4], r[5], r[6]);
        }
        System.out.println("================================================================================================");
        System.out.println("note: the CSV read baseline drains every source row of the scenario serially and discards it (pure file read I/O); net time = total minus that baseline");
        System.out.println("   (unrelated I/O removed, floored at 0); javaOnly is a hand-written HashMap plus sort with no engine cost, procP1 is serial, procP4 is parallel inside the pipeline, cluster is a separate master plus two worker JVMs over RPC and socket shuffle.");
        System.out.println("   all four engines agree on a bit-identical FileSink multiset fingerprint (correctness already asserted; the plain Java baseline matches scale 10 HALF_UP and null forming its own group); exact row-by-row output is covered exhaustively by the small-scale unit tests.");
    }

    // ---- source factories, which stay light by holding a path and reading in a stream ----
    private Source orders()   { return new CsvInput(ordersCsv, ORDER); }
    private Source bonuses()  { return new CsvInput(bonusesCsv, BONUS); }
    private Source partners() { return new CsvInput(partnersCsv, PARTNER); }

    /**
     * A scenario builds the graph with the public DataSet API on the given
     * {@link BatchSession} and outputs to the given {@link FileSink}. All three engines,
     * single machine at parallelism 1 and 4 and the cluster, go through this builder and then
     * BatchSession.execute(), because tests use the public entry point rather than
     * constructing internal execution objects directly.
     */
    @FunctionalInterface
    private interface SceneBuilder {
        void build(BatchSession session, FileSink out);
    }

    // ============ scenario 1: groupBySum over region ============
    @Test
    void s1_groupBySum() throws Exception {
        runScenario("groupBySum(region)", GB_REGION,
                (s, out) -> s.fromSource(orders())
                        .groupBy(ParallelGroupByAgg.of(ORDER, "region").sum("amount"))
                        .sinkTo(out),
                () -> Arrays.asList(orders()));
    }

    // ============ scenario 2: filter, groupBy, orderBy, three hops ============
    @Test
    void s2_filterGroupByOrderBy() throws Exception {
        runScenario("filter→groupBy→orderBy", GB_REGION,
                (s, out) -> {
                    DataSet g = s.fromSource(orders())
                            .filter("amount >= 100")
                            .groupBy(ParallelGroupByAgg.of(ORDER, "region").sum("amount"));
                    orderByAllFields(g).sinkTo(out);
                },
                () -> Arrays.asList(orders()));
    }

    // ============ scenario 3: distinct over dept and region ============
    @Test
    void s3_distinct() throws Exception {
        runScenario("distinct(dept,region)", ORDER,
                (s, out) -> s.fromSource(orders())
                        .distinct(io.rowweave.ops.Distinct.byColumns(ORDER, "dept", "region"))
                        .sinkTo(out),
                () -> Arrays.asList(orders()));
    }

    // ============ scenario 4: join orders with bonuses on dept, large cross small ============
    @Test
    void s4_join() throws Exception {
        runScenario("join(orders⋈bonuses)", JOIN,
                (s, out) -> {
                    DataSet l = s.fromSource(orders());
                    DataSet r = s.fromSource(bonuses());
                    l.join(r, SortMergeJoin.of(ORDER, BONUS, new String[]{"dept"}, new String[]{"dept"}, JoinType.INNER))
                            .sinkTo(out);
                },
                () -> Arrays.asList(orders(), bonuses()));
    }

    // ============ scenario 5: join then groupBy over region, two shuffle layers ============
    @Test
    void s5_joinThenGroupBy() throws Exception {
        runScenario("join→groupBy(region)", JOIN_GB,
                (s, out) -> {
                    DataSet l = s.fromSource(orders());
                    DataSet r = s.fromSource(bonuses());
                    l.join(r, SortMergeJoin.of(ORDER, BONUS, new String[]{"dept"}, new String[]{"dept"}, JoinType.INNER))
                            .groupBy(ParallelGroupByAgg.of(JOIN, "region").sum("amount"))
                            .sinkTo(out);
                },
                () -> Arrays.asList(orders(), bonuses()));
    }

    // ============ scenario 6: join orders with partners on id, both sides large and 1 to 1, so the output carries ORDERS rows ============
    @Test
    void s6_bigJoin() throws Exception {
        Assumptions.assumeTrue(PARTNERS > 0, "partners were not generated (rowweave.bench.partners=0), skipping the large x large join");
        runScenario("bigJoin(orders⋈partners on id)", BIG_JOIN,
                (s, out) -> {
                    DataSet l = s.fromSource(orders());
                    DataSet r = s.fromSource(partners());
                    l.join(r, SortMergeJoin.of(ORDER, PARTNER, new String[]{"id"}, new String[]{"id"}, JoinType.INNER))
                            .sinkTo(out);
                },
                () -> Arrays.asList(orders(), partners()));
    }

    /** orderBy ascending over every field with nulls last, through the public DataSet API rather than a
     * hand-built orderByAll PlanNode. */
    private static DataSet orderByAllFields(DataSet ds) {
        DataSchema m = ds.getSchema();
        SortField[] sf = new SortField[m.getFieldCount()];
        for (int i = 0; i < sf.length; i++) {
            sf[i] = new SortField(i, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        }
        return ds.orderBy(io.rowweave.ops.OrderBy.of(m, sf), 1, false);
    }

    /**
     * Runs one scenario in all three modes and compares the multiset fingerprints. Timing excludes
     * the CSV read: the read baseline is measured first by draining and discarding the source rows
     * serially, and the reported engine time is the total minus that baseline. The check holds O(1)
     * memory, so tens of millions of rows do not exhaust the heap.
     */
    private void runScenario(String name, DataSchema outMeta,
                             SceneBuilder scene,
                             java.util.function.Supplier<List<Source>> sources) throws Exception {
        File dir = Files.createTempDirectory("bench-out-" + System.nanoTime()).toFile();
        File fj = new File(dir, "purejava.tsv");
        File f1 = new File(dir, "p1.tsv");
        File f4 = new File(dir, "p4.tsv");
        File fc = new File(dir, "cluster.tsv");

        // The CSV read baseline drains every source row of this scenario serially and discards it, with
        // no engine and no output, which is the unrelated I/O time that gets subtracted.
        long rt = System.nanoTime();
        drainAndDiscard(sources.get());
        long readMs = (System.nanoTime() - rt) / 1_000_000;

        // A hand-written Java baseline with no engine overhead, whose output is written to FileSink.
        long t = System.nanoTime();
        long nj = pureJava(name, new FileSink(outMeta, fj.getAbsolutePath()));
        long pjms = (System.nanoTime() - t) / 1_000_000;

        // Single machine at parallelism 1, through BatchSession.execute(), which uses the default
        // LocalJobClient: the public entry point rather than constructing an executor.
        t = System.nanoTime();
        runViaSession(name + "-p1", 1, null, scene, new FileSink(outMeta, f1.getAbsolutePath()));
        long p1ms = (System.nanoTime() - t) / 1_000_000;

        // Single machine at parallelism 4, through setParallelism(4) and commit().
        t = System.nanoTime();
        runViaSession(name + "-p4", 4, null, scene, new FileSink(outMeta, f4.getAbsolutePath()));
        long p4ms = (System.nanoTime() - t) / 1_000_000;

        // The cluster path injects ClusterJobClient and commits, with a real Master and Workers and
        // socket shuffle.
        t = System.nanoTime();
        runViaSession(name + "-cluster", 4, new ClusterJobClient("127.0.0.1", masterPort),
                scene, new FileSink(outMeta, fc.getAbsolutePath()));
        long cms = (System.nanoTime() - t) / 1_000_000;

        // Correctness: the row count and the order-independent multiset fingerprint must agree, checked in
        // O(1) memory so no run exhausts the heap, while exact row-by-row behavior is covered by the
        // small exhaustive unit tests.
        long[] s1 = fileFingerprint(f1.getAbsolutePath());
        long n1 = s1[0];
        assertEquals(n1, nj, name + " P1 versus the java-only baseline row count");
        assertArrayEquals(s1, fileFingerprint(f4.getAbsolutePath()), name + " P4 versus P1 multiset fingerprint");
        assertArrayEquals(s1, fileFingerprint(fc.getAbsolutePath()), name + " cluster versus P1 multiset fingerprint");
        assertArrayEquals(s1, fileFingerprint(fj.getAbsolutePath()), name + " plain Java vs single machine P1 multiset fingerprint (semantics aligned)");

        report.add(new String[]{name, Long.toString(n1), Long.toString(readMs),
                Long.toString(Math.max(0, pjms - readMs)),
                Long.toString(Math.max(0, p1ms - readMs)),
                Long.toString(Math.max(0, p4ms - readMs)),
                Long.toString(Math.max(0, cms - readMs))});
        deleteRec(dir);
    }

    /**
     * Runs one scenario through {@link BatchSession#commit()}, because a test uses the public
     * entry point rather than constructing a PushDagExecutor or calling submitToSink. A null client
     * means the default single-machine LocalJobClient at the requested parallelism, and a non-null one
     * injects the cluster client. Canonicalization uses its default of false.
     */
    private void runViaSession(String jobName, int parallelism, io.rowweave.cluster.JobClient client,
                               SceneBuilder scene, FileSink out) {
        BatchSession s = client != null
                ? new BatchSession(jobName, jobName, client)
                : RowWeave.createBatchSession(jobName);
        s.setParallelism(parallelism);
        scene.build(s, out);
        s.execute();
    }

    // ================= plain Java baseline (a reference run with zero engine overhead; test scope may use List/HashMap) =================
    // Semantics match the engine: BigDecimal accumulates at full precision and setScale(10, HALF_UP) is applied once before output; null forms its own group.
    // The hand-written algorithm emits rows -> FileSink.collect writes them, with the same encoding as the engine, so the two outputs compare byte for byte.

    private static final java.math.BigDecimal HUNDRED = new java.math.BigDecimal("100");

    /** Runs one scenario in plain Java, collecting into a FileSink, and returns the row count. */
    private long pureJava(String scenario, io.rowweave.Sink out) {
        long n;
        switch (scenario) {
            case "groupBySum(region)":            n = pjGroupBySum(orders(), 2, out); break;  // region is column index 2
            case "filter→groupBy→orderBy":        n = pjFilterGroupByOrderBy(out); break;
            case "distinct(dept,region)":         n = pjDistinct(orders(), new int[]{1, 2}, ORDER, out); break;
            case "join(orders⋈bonuses)":          n = pjJoin(out); break;
            case "join→groupBy(region)":          n = pjJoinThenGroupBy(out); break;
            case "bigJoin(orders⋈partners on id)":n = pjBigJoin(out); break;
            default: throw new IllegalArgumentException("no pure-java impl: " + scenario);
        }
        out.close();
        return n;
    }

    /** groupBySum keyed on column keyIdx, the region, summing the amount at index 3, accumulated at full
     * precision and scaled once to 10 digits half-up before output. */
    private long pjGroupBySum(Source in, int keyIdx, io.rowweave.Sink out) {
        java.util.LinkedHashMap<Object, java.math.BigDecimal> agg = new java.util.LinkedHashMap<>();
        for (io.rowweave.DataRow r : in) {
            Object k = r.get(keyIdx);
            java.math.BigDecimal amt = (java.math.BigDecimal) r.get(3);
            agg.merge(k, amt, java.math.BigDecimal::add);
        }
        long n = 0;
        for (java.util.Map.Entry<Object, java.math.BigDecimal> e : agg.entrySet()) {
            io.rowweave.DataRow o = new io.rowweave.DataRow(2);
            o.set(0, e.getKey());
            o.set(1, e.getValue().setScale(10, java.math.RoundingMode.HALF_UP));
            out.collect(o); n++;
        }
        return n;
    }

    /** filter on amount at least 100, then groupBy over region summing the amount, then orderBy ascending
     * over every column with nulls last. */
    private long pjFilterGroupByOrderBy(io.rowweave.Sink out) {
        java.util.LinkedHashMap<Object, java.math.BigDecimal> agg = new java.util.LinkedHashMap<>();
        for (io.rowweave.DataRow r : orders()) {
            java.math.BigDecimal amt = (java.math.BigDecimal) r.get(3);
            if (amt.compareTo(HUNDRED) >= 0) {
                agg.merge(r.get(2), amt, java.math.BigDecimal::add);
            }
        }
        List<io.rowweave.DataRow> rows = new ArrayList<>();
        for (java.util.Map.Entry<Object, java.math.BigDecimal> e : agg.entrySet()) {
            io.rowweave.DataRow o = new io.rowweave.DataRow(2);
            o.set(0, e.getKey());
            o.set(1, e.getValue().setScale(10, java.math.RoundingMode.HALF_UP));
            rows.add(o);
        }
        rows.sort(BENCH_ROW_CMP);  // ascending over all columns, nulls last
        long n = 0;
        for (io.rowweave.DataRow o : rows) { out.collect(o); n++; }
        return n;
    }

    /** distinct over the given key columns, de-duplicating with a LinkedHashSet and emitting the whole row
     * as first seen. */
    private long pjDistinct(Source in, int[] keyIdxs, DataSchema meta, io.rowweave.Sink out) {
        java.util.HashSet<String> seen = new java.util.HashSet<>();
        long n = 0;
        for (io.rowweave.DataRow r : in) {
            StringBuilder key = new StringBuilder();
            for (int idx : keyIdxs) { key.append(r.get(idx)); key.append((char) 31); }
            if (seen.add(key.toString())) { out.collect(r); n++; }
        }
        return n;
    }

    /** joins orders with bonuses on dept as an inner join, building a map from dept to bonus rows from the
     * small bonuses table and probing it with orders. */
    private long pjJoin(io.rowweave.Sink out) {
        java.util.HashMap<Object, List<io.rowweave.DataRow>> build = new java.util.HashMap<>();
        for (io.rowweave.DataRow b : bonuses()) {
            build.computeIfAbsent(b.get(0), x -> new ArrayList<>()).add(b); // bonuses dept=idx0
        }
        long n = 0;
        for (io.rowweave.DataRow o : orders()) {
            List<io.rowweave.DataRow> matches = build.get(o.get(1)); // orders dept=idx1
            if (matches == null) continue;
            for (io.rowweave.DataRow b : matches) {
                out.collect(joinRow(o, b)); n++;
            }
        }
        return n;
    }

    /** joins orders with bonuses, then groups by region summing the amount. */
    private long pjJoinThenGroupBy(io.rowweave.Sink out) {
        java.util.HashMap<Object, List<io.rowweave.DataRow>> build = new java.util.HashMap<>();
        for (io.rowweave.DataRow b : bonuses()) {
            build.computeIfAbsent(b.get(0), x -> new ArrayList<>()).add(b);
        }
        java.util.LinkedHashMap<Object, java.math.BigDecimal> agg = new java.util.LinkedHashMap<>();
        for (io.rowweave.DataRow o : orders()) {
            List<io.rowweave.DataRow> matches = build.get(o.get(1));
            if (matches == null) continue;
            java.math.BigDecimal amt = (java.math.BigDecimal) o.get(3);
            for (int i = 0; i < matches.size(); i++) {
                agg.merge(o.get(2), amt, java.math.BigDecimal::add);  // after the join the region sits at index 2, since the left column order is kept
            }
        }
        long n = 0;
        for (java.util.Map.Entry<Object, java.math.BigDecimal> e : agg.entrySet()) {
            io.rowweave.DataRow o = new io.rowweave.DataRow(2);
            o.set(0, e.getKey());
            o.set(1, e.getValue().setScale(10, java.math.RoundingMode.HALF_UP));
            out.collect(o); n++;
        }
        return n;
    }

    /** the large-to-large inner join of orders with partners on id, one to one, building a map from id to
     * partner row and probing it with orders. */
    private long pjBigJoin(io.rowweave.Sink out) {
        java.util.HashMap<Object, io.rowweave.DataRow> build = new java.util.HashMap<>();
        for (io.rowweave.DataRow p : partners()) {
            build.put(p.get(0), p); // partners id=idx0
        }
        long n = 0;
        for (io.rowweave.DataRow o : orders()) {
            io.rowweave.DataRow p = build.get(o.get(0)); // orders id=idx0
            if (p != null) { out.collect(joinRow(o, p)); n++; }
        }
        return n;
    }

    /** Concatenates the left and right rows, all left columns then all right columns, matching the
     * sort-merge join output order of [left..., right...]. */
    private static io.rowweave.DataRow joinRow(io.rowweave.DataRow l, io.rowweave.DataRow r) {
        io.rowweave.DataRow o = new io.rowweave.DataRow(l.size() + r.size());
        for (int i = 0; i < l.size(); i++) o.set(i, l.get(i));
        for (int i = 0; i < r.size(); i++) o.set(l.size() + i, r.get(i));
        return o;
    }

    /** Ascending comparator over all columns with nulls last, matching orderByAll semantics. */
    private static final java.util.Comparator<io.rowweave.DataRow> BENCH_ROW_CMP = (a, b) -> {
        int w = Math.min(a.size(), b.size());
        for (int i = 0; i < w; i++) {
            int c = compareCell(a.get(i), b.get(i));
            if (c != 0) return c;
        }
        return 0;
    };

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareCell(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return 1;  // NULLS_LAST
        if (b == null) return -1;
        if (a instanceof java.math.BigDecimal && b instanceof java.math.BigDecimal) {
            return ((java.math.BigDecimal) a).compareTo((java.math.BigDecimal) b);
        }
        return ((Comparable) a).compareTo(b);
    }

    private static long drainAndDiscard(List<Source> inputs) {
        long n = 0;
        for (Source in : inputs) {
            for (io.rowweave.DataRow ignored : in) {
                n++;
            }
        }
        return n;
    }

    /** Streams a file in O(1) memory and returns the row count plus the order-independent 64-bit sum of
     * row hashes, which is the multiset fingerprint. */
    private static long[] fileFingerprint(String path) throws Exception {
        long count = 0;
        long sum = 0;
        try (BufferedReader r = Files.newBufferedReader(java.nio.file.Paths.get(path), StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                count++;
                long h = 1125899906842597L;
                for (int i = 0; i < line.length(); i++) {
                    h = 31 * h + line.charAt(i);
                }
                sum += h;
            }
        }
        return new long[]{count, sum};
    }

    // ---- process start and cleanup ----
    private static Process launch(List<String> jvmArgs, String readyPrefix, StringBuilder readyOut) throws Exception {
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String cp = System.getProperty("java.class.path");
        List<String> cmd = new ArrayList<>();
        cmd.add(javaBin);
        cmd.add("-Xms128m");
        cmd.add("-Xmx1536m");
        cmd.add("-XX:MaxDirectMemorySize=256m");
        cmd.add("-XX:MaxMetaspaceSize=256m");
        cmd.add("-XX:ReservedCodeCacheSize=128m");
        cmd.add("-Xss1m");
        cmd.add("-XX:ActiveProcessorCount=4");
        cmd.add("-Drowweave.process.memoryLimitBytes=8589934592");
        cmd.add("-cp");
        cmd.add(cp);
        io.rowweave.test.ForkedJvm.appendJvmArguments(cmd, jvmArgs);
        String muxMode = System.getProperty("rowweave.shuffle.mux");
        if (muxMode != null) {
            cmd.add("-Drowweave.shuffle.mux=" + muxMode);
        }
        cmd.add("io.rowweave.cluster.ClusterMain");
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
        long deadline = System.currentTimeMillis() + 30_000;
        final boolean[] ready = {false};
        Thread reader = new Thread(() -> {
            try {
                String line;
                while ((line = r.readLine()) != null) {
                    if (!ready[0] && line.startsWith(readyPrefix)) {
                        synchronized (readyOut) {
                            readyOut.append(line.substring(readyPrefix.length()).trim());
                            ready[0] = true;
                            readyOut.notifyAll();
                        }
                    }
                }
            } catch (Exception ignored) { }
        }, "launch-reader");
        reader.setDaemon(true);
        reader.start();
        synchronized (readyOut) {
            while (!ready[0] && System.currentTimeMillis() < deadline) {
                readyOut.wait(Math.max(1, deadline - System.currentTimeMillis()));
            }
        }
        if (!ready[0]) {
            p.destroyForcibly();
            fail("process not ready: " + readyPrefix);
        }
        return p;
    }

    private static void kill(Process p) {
        if (p != null) {
            p.destroyForcibly();
            try { p.waitFor(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
    }

    private static void deleteRec(File f) {
        File[] ch = f.listFiles();
        if (ch != null) for (File c : ch) deleteRec(c);
        f.delete();
    }

    /** The CSV data directory, one per scale, so a generated data set is reused across runs. */
    static final class Paths {
        static Path dataDir(long orders) {
            return java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"), "rowweave-bench-csv", "n" + orders);
        }
    }
}
