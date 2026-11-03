package io.rowweave.cluster;

import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.bench.CsvInput;
import io.rowweave.local.LocalJobClient;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.StageView;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

@Tag("profile")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LocalClusterVsSingleMachineBenchmarkTest {

    private static final JobClient LEGACY = new LocalJobClient();

    private Path dir;

    private String ordersA, ordersB;
    private final List<String> report = new ArrayList<>();
    private long legacyP50Total;
    private long unifiedP50Total;

    @BeforeAll
    void setUp() throws Exception {
        long rows = Long.getLong("rowweave.bench.rows", 50_000L);
        dir = Files.createTempDirectory("lcvssm");
        long t = System.nanoTime();
        ordersA = TenDagData.orders(dir, "ordersA", rows);
        ordersB = TenDagData.orders(dir, "ordersB", rows);
        System.out.println("=== CSV ready: rows=" + rows + "(generated in " + (System.nanoTime() - t) / 1_000_000 + "ms) ===");
    }

    @AfterAll
    void tearDown() {
        try { deleteRec(dir.toFile()); } catch (RuntimeException ignored) { }
        System.out.println("\n====== all-operator performance comparison: local cluster (in process, local channels) versus the single-process PushDagExecutor (through commit) ======");
        System.out.println("scenario/P | legacy cold | unified cold | legacy warm P50/P95 | unified warm P50/P95 | P95 ratio");
        System.out.println("------------------------------------------------------------------------------------------------");
        for (String line : report) System.out.println(line);
        System.out.println("======================================================================================");
        System.out.println("note: cold timings are informational only; the assertion uses warm P50/P95 on a reused Runtime, and every run still checks multiset equivalence.");
    }

    private static void deleteRec(File f) {
        File[] ch = f.listFiles();
        if (ch != null) for (File c : ch) deleteRec(c);
        f.delete();
    }

    private CsvInput csv(String path, DataSchema meta) { return new CsvInput(path, meta); }

    @FunctionalInterface
    private interface Scene { void build(ExecutionEnvironment s, FileSink out); }

    private long run(int par, JobClient client, Scene scene, DataSchema outMeta, Path outPath) {
        long t = System.nanoTime();
        ExecutionEnvironment s = new ExecutionEnvironment("b", "b", client);
        s.setParallelism(par);
        scene.build(s, new FileSink(outMeta, outPath.toString()));
        s.execute();
        return (System.nanoTime() - t) / 1_000_000;
    }

    private List<String> fingerprint(DataSchema meta, Path p) {
        List<String> rows = new ArrayList<>();
        for (DataRow r : FileSink.read(meta, p.toString())) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < meta.getFieldCount(); i++) {
                Object v = r.get(i);
                if (v instanceof BigDecimal) v = ((BigDecimal) v).setScale(10, java.math.RoundingMode.HALF_UP).stripTrailingZeros();
                sb.append(v).append('|');
            }
            rows.add(sb.toString());
        }
        rows.sort(null);
        return rows;
    }

    private void bench(String name, DataSchema outMeta, Scene scene) throws Exception {
        Timing p1 = measure(name, 1, outMeta, scene);
        Timing p4 = measure(name, 4, outMeta, scene);
        requireWarmP95(name, 1, p1);
        requireWarmP95(name, 4, p4);
        legacyP50Total += p1.legacyP50 + p4.legacyP50;
        unifiedP50Total += p1.unifiedP50 + p4.unifiedP50;
        report.add(p1.line(name, 1));
        report.add(p4.line(name, 4));
        System.out.println("[BLU-PERF] " + p1.line(name, 1));
        System.out.println("[BLU-PERF] " + p4.line(name, 4));
    }

    private Timing measure(String name, int parallelism, DataSchema outMeta, Scene scene)
            throws Exception {
        int samples = Integer.getInteger("rowweave.bench.samples", 20);
        if (samples < 20) {
            throw new IllegalArgumentException(
                    "rowweave.bench.samples must be >= 20 for a meaningful P95");
        }
        Path legacyColdPath = dir.resolve(name + "-p" + parallelism + "-legacy-cold.tsv");
        long legacyCold = run(parallelism, LEGACY, scene, outMeta, legacyColdPath);
        List<String> expected = fingerprint(outMeta, legacyColdPath);
        long[] legacy = new long[samples];
        long[] unified = new long[samples];
        File runtimeDir = new File(dir.toFile(), name + "-p" + parallelism + "-runtime");
        long coldStart = System.nanoTime();
        try (InProcessCluster runtime = new InProcessCluster(parallelism, runtimeDir)) {
            JobClient unifiedDefault = defaultLocalClient(runtime);
            Path unifiedColdPath = dir.resolve(name + "-p" + parallelism + "-unified-cold.tsv");
            run(parallelism, unifiedDefault, scene, outMeta, unifiedColdPath);
            long unifiedCold = (System.nanoTime() - coldStart) / 1_000_000;
            assertEquals(expected, fingerprint(outMeta, unifiedColdPath),
                    name + " unified cold result");
            for (int i = 0; i < samples; i++) {
                Path lp = dir.resolve(name + "-p" + parallelism + "-legacy-" + i + ".tsv");
                Path up = dir.resolve(name + "-p" + parallelism + "-unified-" + i + ".tsv");
                if ((i & 1) == 0) {
                    legacy[i] = run(parallelism, LEGACY, scene, outMeta, lp);
                    unified[i] = run(parallelism, unifiedDefault, scene, outMeta, up);
                } else {
                    unified[i] = run(parallelism, unifiedDefault, scene, outMeta, up);
                    legacy[i] = run(parallelism, LEGACY, scene, outMeta, lp);
                }
                assertEquals(expected, fingerprint(outMeta, lp), name + " legacy sample " + i);
                assertEquals(expected, fingerprint(outMeta, up), name + " unified sample " + i);
            }
            return new Timing(legacyCold, unifiedCold,
                    percentile(legacy, 0.50), percentile(legacy, 0.95),
                    percentile(unified, 0.50), percentile(unified, 0.95));
        }
    }

    private static JobClient defaultLocalClient(InProcessCluster runtime) {
        return new JobClient() {
            @Override
            public long submitToSink(PlanGraph sinkGraph) {
                return runtime.submit(LegacyPlanArtifactAdapter.wrap(
                        "blu-performance", sinkGraph), false).rowsOut();
            }

            @Override
            public ExecutionReport submit(PlanArtifact artifact) {
                return runtime.submit(artifact, false);
            }
        };
    }

    private static long percentile(long[] samples, double percentile) {
        long[] ordered = samples.clone();
        Arrays.sort(ordered);
        int index = (int) Math.ceil(percentile * ordered.length) - 1;
        return ordered[Math.max(0, Math.min(index, ordered.length - 1))];
    }

    private static void requireWarmP95(String name, int parallelism, Timing timing) {
        assertTrue(timing.unifiedP95 <= Math.ceil(timing.legacyP95 * 1.50d),
                name + " P" + parallelism + " warm P95 regression: legacy="
                        + timing.legacyP95 + "ms unified=" + timing.unifiedP95 + "ms");
    }

    private static final class Timing {
        final long legacyCold, unifiedCold, legacyP50, legacyP95, unifiedP50, unifiedP95;

        Timing(long legacyCold, long unifiedCold, long legacyP50, long legacyP95,
               long unifiedP50, long unifiedP95) {
            this.legacyCold = legacyCold;
            this.unifiedCold = unifiedCold;
            this.legacyP50 = legacyP50;
            this.legacyP95 = legacyP95;
            this.unifiedP50 = unifiedP50;
            this.unifiedP95 = unifiedP95;
        }

        String line(String name, int parallelism) {
            return String.format("%-26s P%d | %5dms | %5dms | %5d/%5dms | %5d/%5dms | %.2fx",
                    name, parallelism, legacyCold, unifiedCold, legacyP50, legacyP95,
                    unifiedP50, unifiedP95, (double) unifiedP95 / Math.max(1L, legacyP95));
        }
    }

    @Test
    void profileLcJoin() throws Exception {
        DataSchema ORDER = TenDagData.ORDER;
        DataSchema joinMeta = new DataSchema(
                new Field("id", DataType.LongType), new Field("dept", DataType.IntegerType),
                new Field("region", DataType.IntegerType), new Field("grade", DataType.IntegerType),
                new Field("amount", DataType.BigDecimalType), new Field("qty", DataType.IntegerType),
                new Field("id_r", DataType.LongType), new Field("dept_r", DataType.IntegerType),
                new Field("region_r", DataType.IntegerType), new Field("grade_r", DataType.IntegerType),
                new Field("amount_r", DataType.BigDecimalType), new Field("qty_r", DataType.IntegerType));
        DataSchema joinGb = ParallelGroupByAgg.of(joinMeta, "region").sum("amount").getOutputSchema();
        Scene scene = (s, out) -> s.fromSource(csv(ordersA, ORDER))
                .join(s.fromSource(csv(ordersB, ORDER))).on("id")
                .groupBy(ParallelGroupByAgg.of(joinMeta, "region").sum("amount"))
                .sinkTo(out);

        String engine = System.getProperty("rowweave.profile.engine", "lc");
        int loops = Integer.getInteger("rowweave.profile.loops", 6);
        int parallelism = Integer.getInteger("rowweave.profile.parallelism", 1);
        System.out.println("=== profile engine=" + engine + " loops=" + loops
                + " parallelism=" + parallelism + " ===");
        run(parallelism, LEGACY, scene, joinGb, dir.resolve("prof-warm.tsv"));
        long total = 0;
        if ("single".equals(engine)) {
            for (int i = 0; i < loops; i++) total += run(parallelism, LEGACY, scene, joinGb, dir.resolve("prof-s" + i + ".tsv"));
        } else {
            InMemoryJobMonitor monitor = new InMemoryJobMonitor(loops + 1);
            try (InProcessCluster lc = new InProcessCluster(
                    parallelism, new File(dir.toFile(), "prof-lc"), monitor)) {
                JobClient localDefault = defaultLocalClient(lc);
                for (int i = 0; i < loops; i++) total += run(parallelism, localDefault, scene, joinGb, dir.resolve("prof-lc" + i + ".tsv"));
            }
            if (Boolean.getBoolean("rowweave.profile.stageTimeline")) {
                for (JobView job : monitor.jobs()) {
                    System.out.println("[PROFILE-JOB] elapsed="
                            + (job.endTime() - job.submitTime()) + "ms state=" + job.state());
                    for (StageView stage : job.stages()) {
                        StringBuilder instances = new StringBuilder();
                        for (StageView.InstanceView instance : stage.instances()) {
                            if (instances.length() > 0) instances.append(',');
                            instances.append(instance.instanceIndex).append(':')
                                    .append(instance.endTime - instance.startTime).append("ms/")
                                    .append(instance.inputRows).append("in/")
                                    .append(instance.outputRows).append("out");
                        }
                        System.out.println("  stage=" + stage.index() + " role=" + stage.role()
                                + " op=" + stage.opChain() + " instances=[" + instances + "]");
                    }
                }
            }
        }
        System.out.println("=== profile engine=" + engine + " avg=" + (total / loops) + "ms/loop (total " + total + "ms)===");
    }

    @Test
    void scenariosAllOperators() throws Exception {
        DataSchema ORDER = TenDagData.ORDER;
        DataSchema gbRegion = ParallelGroupByAgg.of(ORDER, "region").sum("amount").getOutputSchema();
        bench("filter-select-gb-order-top", gbRegion, (s, out) -> {
            DataSet g = s.fromSource(csv(ordersA, ORDER))
                    .filter("amount >= 0")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .map(in -> in)
                    .groupBy(ParallelGroupByAgg.of(ORDER, "region").sum("amount"));
            g.orderBy(OrderBy.of(gbRegion, new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)), 1, false)
                    .top(Top.of(gbRegion, 5, new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)), 1)
                    .sinkTo(out);
        });

        DataSchema joinMeta = new DataSchema(
                new Field("id", DataType.LongType), new Field("dept", DataType.IntegerType),
                new Field("region", DataType.IntegerType), new Field("grade", DataType.IntegerType),
                new Field("amount", DataType.BigDecimalType), new Field("qty", DataType.IntegerType),
                new Field("id_r", DataType.LongType), new Field("dept_r", DataType.IntegerType),
                new Field("region_r", DataType.IntegerType), new Field("grade_r", DataType.IntegerType),
                new Field("amount_r", DataType.BigDecimalType), new Field("qty_r", DataType.IntegerType));
        DataSchema joinGb = ParallelGroupByAgg.of(joinMeta, "region").sum("amount").getOutputSchema();
        bench("join-groupBy", joinGb, (s, out) -> {
            s.fromSource(csv(ordersA, ORDER))
                    .join(s.fromSource(csv(ordersB, ORDER))).on("id")
                    .groupBy(ParallelGroupByAgg.of(joinMeta, "region").sum("amount"))
                    .sinkTo(out);
        });

        bench("distinct-union", ORDER, (s, out) -> {
            DataSet a = s.fromSource(csv(ordersA, ORDER)).distinct("dept", "region");
            DataSet b = s.fromSource(csv(ordersB, ORDER)).distinct("dept", "region");
            a.union(b).distinct("dept", "region").sinkTo(out);
        });

        double weightedThroughput = (double) legacyP50Total
                / Math.max(1L, unifiedP50Total);
        System.out.printf("[BLU-PERF] weighted warm throughput=%.3fx legacyP50Total=%dms unifiedP50Total=%dms%n",
                weightedThroughput, legacyP50Total, unifiedP50Total);
        assertTrue(weightedThroughput >= 0.95d,
                "unified weighted warm throughput must retain at least 95% of legacy: "
                        + weightedThroughput);
    }
}

