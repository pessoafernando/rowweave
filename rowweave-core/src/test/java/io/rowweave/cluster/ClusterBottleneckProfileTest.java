package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.bench.CsvInput;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

@Tag("profile")
class ClusterBottleneckProfileTest {

    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema GB_REGION =
            io.rowweave.ops.ParallelGroupByAgg.of(ORDER, "region").sum("amount").getOutputSchema();

    private static Source orders(String csv) { return new CsvInput(csv, ORDER); }

    private static PlanGraph chain(String ordersCsv, io.rowweave.Sink out) {
        PlanNode src = PlanNode.sourceFromInputs(ORDER, Collections.singletonList(orders(ordersCsv)));
        PlanNode flt = PlanNode.unary("filter", ORDER, 0).param("expr", "amount >= 100").build();
        PlanNode gb = PlanNode.unary("groupBySum", GB_REGION, 1)
                .listParam("keys", Arrays.asList("region")).param("measure", "amount").build();
        PlanNode ob = PlanNode.unary("orderBy", GB_REGION, 2)
                .listParam("sort", Collections.singletonList("1:ASC:NULLS_LAST")).build();
        PlanNode sink = PlanNode.sink(GB_REGION, 3, out).build();
        return new PlanGraph(Arrays.asList(src, flt, gb, ob, sink));
    }

    @Test
    void profileMultiStageClusterVsSingle() throws Exception {
        long orders = Long.getLong("rowweave.bench.orders", 2_000_000L);
        int iters = Integer.getInteger("rowweave.profile.iters", 20);
        File dir = Paths.get(System.getProperty("java.io.tmpdir"), "rowweave-bench-csv", "n" + orders).toFile();
        String ordersCsv = new File(dir, "orders.csv").getAbsolutePath();
        if (!new File(ordersCsv).exists()) {
            System.out.println("the CSV data set n" + orders + " does not exist, run BigDataCsvBenchmarkTest to generate it; skipping the profile");
            return;
        }

        long t0 = System.nanoTime();
        long n1 = 0;
        for (int i = 0; i < iters; i++) {
            File out = File.createTempFile("prof-p1-", ".tsv");
            n1 = new PushDagExecutor(1).runSinkGraph(chain(ordersCsv, new FileSink(GB_REGION, out.getAbsolutePath())));
            out.delete();
        }
        long p1Ms = (System.nanoTime() - t0) / 1_000_000;

        Path tmp = java.nio.file.Files.createTempDirectory("cluster-prof");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        long submitTotalMs, nc = 0;
        try (MasterServer master = new MasterServer(5000);
             WorkerServer ws0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer ws1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            int port = master.port();
            for (int i = 0; i < 3; i++) {
                File out = File.createTempFile("prof-warm-", ".tsv");
                new RealClusterJobClient("127.0.0.1", port).submitToSink(
                        chain(ordersCsv, new FileSink(GB_REGION, out.getAbsolutePath())));
                out.delete();
            }
            long t1 = System.nanoTime();
            for (int i = 0; i < iters; i++) {
                File out = File.createTempFile("prof-cl-", ".tsv");
                nc = new RealClusterJobClient("127.0.0.1", port).submitToSink(
                        chain(ordersCsv, new FileSink(GB_REGION, out.getAbsolutePath())));
                out.delete();
            }
            submitTotalMs = (System.nanoTime() - t1) / 1_000_000;
        } finally {
            deleteRec(tmp.toFile());
        }

        System.out.println("\n=== cluster bottleneck profile (orders=" + orders + ", iters=" + iters + ", multi-stage DAG)===");
        System.out.printf("single process P1: %d runs total %6d ms, avg %6.1f ms/run (out=%d)%n", iters, p1Ms, (double) p1Ms / iters, n1);
        System.out.printf("in-proc cluster:  %d runs total %6d ms, avg %6.1f ms/run (out=%d)%n", iters, submitTotalMs, (double) submitTotalMs / iters, nc);
        System.out.printf("-> each cluster run costs %.1f ms more than the single process (fixed scheduling plus data-plane overhead)%n", (double) submitTotalMs / iters - (double) p1Ms / iters);
        System.out.println("-> JFR flame graph: jfr print --events jdk.ExecutionSample /tmp/cluster.jfr aggregates the hottest stack tops");
        System.out.println("==========================================\n");
    }

    private static void deleteRec(File f) {
        File[] ch = f.listFiles();
        if (ch != null) for (File c : ch) deleteRec(c);
        f.delete();
    }
}

