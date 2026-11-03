package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.bench.CsvDataGen;
import io.rowweave.bench.CsvInput;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Tag("profile")
class ParallelismQuadrantBenchmarkTest {

    private static final int SHARDS = 4;
    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema GB_REGION =
            ParallelGroupByAgg.of(ORDER, "region").sum("amount").getOutputSchema();

    private static List<String> shardPaths;

    private void ensureData(long orders) {
        Path dir = Paths.get(System.getProperty("java.io.tmpdir"), "rowweave-bench-csv", "n" + orders + "-s" + SHARDS);
        try { Files.createDirectories(dir); } catch (Exception e) { throw new RuntimeException(e); }
        List<Path> parts = CsvDataGen.ordersSharded(dir, orders, 200, 50, SHARDS);
        shardPaths = new ArrayList<>();
        for (Path p : parts) shardPaths.add(p.toString());
    }

    private PlanGraph shardedGraph(FileSink out) {
        List<Source> inputs = new ArrayList<>();
        for (String p : shardPaths) inputs.add(new CsvInput(p, ORDER));
        PlanNode src = PlanNode.sourceFromInputs(ORDER, inputs);
        PlanNode gb = PlanNode.unary("groupBySum", GB_REGION, 0)
                .listParam("keys", Arrays.asList("region")).param("measure", "amount").build();
        PlanNode sink = PlanNode.sink(GB_REGION, 1, out).build();
        return new PlanGraph(Arrays.asList(src, gb, sink));
    }

    private long timeSingle(int parallelism, int iters) {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < iters; i++) {
            File out = tmpOut();
            long t = System.nanoTime();
            new PushDagExecutor(parallelism).runSinkGraph(shardedGraph(new FileSink(GB_REGION, out.getAbsolutePath())));
            best = Math.min(best, (System.nanoTime() - t) / 1_000_000);
            out.delete();
        }
        return best;
    }

    private long timeCluster(int parallelism, int iters, int masterPort) {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < iters; i++) {
            File out = tmpOut();
            long t = System.nanoTime();
            new RealClusterJobClient("127.0.0.1", masterPort)
                    .submitToSink(shardedGraph(new FileSink(GB_REGION, out.getAbsolutePath())));
            best = Math.min(best, (System.nanoTime() - t) / 1_000_000);
            out.delete();
        }
        return best;
    }

    private static File tmpOut() {
        try { return File.createTempFile("quad-", ".tsv"); } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Test
    void quadrantSingleVsClusterP1VsP4() throws Exception {
        long orders = Long.getLong("rowweave.bench.orders", 10_000_000L);
        int iters = Integer.getInteger("rowweave.profile.iters", 3);
        ensureData(orders);

        timeSingle(1, 1);

        long s1 = timeSingle(1, iters);
        long s4 = timeSingle(4, iters);
        System.out.printf("[single process] P1=%dms P4=%dms (P4/P1 %.2fx)%n", s1, s4, (double) s1 / s4);

        Path tmp = Files.createTempDirectory("quad-cluster");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        File w2 = new File(tmp.toFile(), "w2"); w2.mkdirs();
        File w3 = new File(tmp.toFile(), "w3"); w3.mkdirs();
        long c1 = -1, c4 = -1;
        try (MasterServer m1 = new MasterServer(5000);
             WorkerServer a = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m1.port(), w0);
             WorkerServer b = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m1.port(), w1)) {
            c1 = timeCluster(1, iters, m1.port());
        } catch (Throwable t) { System.out.println("[cluster parallelism 1 failed] "); t.printStackTrace(); }
        try (MasterServer m4 = new MasterServer(5000);
             WorkerServer a = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m4.port(), w0);
             WorkerServer b = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m4.port(), w1);
             WorkerServer c = WorkerServer.newUnguarded("w2", 32, "127.0.0.1", m4.port(), w2);
             WorkerServer d = WorkerServer.newUnguarded("w3", 32, "127.0.0.1", m4.port(), w3)) {
            c4 = timeCluster(4, iters, m4.port());
        } catch (Throwable t) { System.out.println("[cluster parallelism 4 failed] "); t.printStackTrace(); }

        System.out.println("\n=== same-JVM harness diagnostics (orders=" + orders + ", shards=" + SHARDS + ", best of " + iters + ")===");
        System.out.printf("%-14s | %-10s | %-10s%n", "", "P=1", "P=4");
        System.out.printf("%-14s | %8dms | %8dms  (P4/P1 speed-up %.2fx)%n", "single process", s1, s4, (double) s1 / s4);
        System.out.printf("%-14s | %8dms | %8dms  (P4/P1 speed-up %.2fx)%n", "same-JVM harness", c1, c4, (double) c1 / c4);
        System.out.printf("-> harness P4 vs single process P4: %dms vs %dms (harness/single %.2fx)%n", c4, s4, (double) c4 / s4);
        System.out.printf("-> harness P4 vs single process P1: %dms vs %dms (%.2fx)%n", c4, s1, (double) c4 / s1);
        System.out.println("=== this test only diagnoses split parallelism; the public API and the real master-with-two-workers result follow the performance baseline ===\n");
    }
}

