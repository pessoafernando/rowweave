package io.rowweave.bench;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.JobClient;
import io.rowweave.cluster.ClusterJobClient;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Compares end-to-end wall clock for parallelism 1 and 4, locally and on a real cluster with one
 * Master and two Workers, through the public BatchSession, over the same application split
 * across FileSource instances. Every timed run is also checked exactly against an independent
 * BigDecimal oracle, so no performance assertion on its own pollutes the CI signal.
 *
 * <p>Run by hand:
 * {@code mvn -q -pl rowweave-core -Dtest=SplitAwarePerformanceBaselineTest -Dgroups=profile
 * -Drowweave.test.excludedGroups= -Drowweave.bench.orders=2000000 -Drowweave.bench.shards=4
 * -Drowweave.profile.iters=2 test}
 */
@Tag("profile")
class SplitAwarePerformanceBaselineTest {

    private static final int DEPTS = 200;
    private static final int REGIONS = 50;
    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema RESULT =
            ParallelGroupByAgg.of(ORDER, "region").sum("amount").getOutputSchema();

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void fourQuadrantsUsePublicApiAndIndependentProcesses() throws Exception {
        long orders = Long.getLong("rowweave.bench.orders", 2_000_000L);
        int shards = Integer.getInteger("rowweave.bench.shards", 4);
        int iterations = Integer.getInteger("rowweave.profile.iters", 2);
        if (shards <= 0 || iterations <= 0) {
            throw new IllegalArgumentException("shards and iterations must be > 0");
        }

        Path work = Files.createTempDirectory("rowweave-split-perf-");
        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            List<Path> parts = CsvDataGen.ordersSharded(work, orders, DEPTS, REGIONS, shards);
            List<String> oracle = oracle(parts);

            StringBuilder masterReady = new StringBuilder();
            master = launch(Arrays.asList(
                    "-Drowweave.master.enable=true",
                    "-Drowweave.master.port=0"), "MASTER_PORT=", masterReady);
            int masterPort = Integer.parseInt(masterReady.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Xmx2g",
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=32",
                    "-Drowweave.worker.memory=268435456",
                    "-Drowweave.memory.bytes=268435456");
            worker0 = launch(workerArgs, "WORKER_READY=", new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", new StringBuilder());
            ClusterJobClient cluster = new ClusterJobClient("127.0.0.1", masterPort);

            // One warm-up per quadrant, and a warm-up result is still held to the same correctness check.
            assertEquals(oracle, runOnce(work, parts, 1, null, "warm-local-p1").rows);
            assertEquals(oracle, runOnce(work, parts, 4, null, "warm-local-p4").rows);
            assertEquals(oracle, runOnce(work, parts, 1, cluster, "warm-cluster-p1").rows);
            assertEquals(oracle, runOnce(work, parts, 4, cluster, "warm-cluster-p4").rows);

            long localP1 = bestOf(work, parts, 1, null, iterations, oracle, "local-p1");
            long localP4 = bestOf(work, parts, 4, null, iterations, oracle, "local-p4");
            long clusterP1 = bestOf(work, parts, 1, cluster, iterations, oracle, "cluster-p1");
            long clusterP4 = bestOf(work, parts, 4, cluster, iterations, oracle, "cluster-p4");

            System.out.println();
            System.out.println("=== split-aware public API / real one-master two-worker baseline ===");
            System.out.println("orders=" + orders + " shards=" + shards + " best-of=" + iterations);
            System.out.printf("local   P1=%dms P4=%dms speedup=%.2fx%n",
                    localP1, localP4, ratio(localP1, localP4));
            System.out.printf("cluster P1=%dms P4=%dms speedup=%.2fx%n",
                    clusterP1, clusterP4, ratio(clusterP1, clusterP4));
            System.out.printf("P4 cluster/local=%.2fx (>1 means cluster slower)%n",
                    ratio(clusterP4, localP4));
        } finally {
            kill(worker1);
            kill(worker0);
            kill(master);
            deleteRecursively(work.toFile());
        }
    }

    private static long bestOf(Path work, List<Path> parts, int parallelism, JobClient client,
                               int iterations, List<String> oracle, String label) {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < iterations; i++) {
            RunResult result = runOnce(work, parts, parallelism, client, label + "-" + i);
            assertEquals(oracle, result.rows, label + " iteration " + i);
            best = Math.min(best, result.elapsedMs);
        }
        return best;
    }

    private static RunResult runOnce(Path work, List<Path> parts, int parallelism,
                                     JobClient client, String label) {
        Path output = work.resolve(label + "-" + System.nanoTime() + ".tsv");
        BatchSession session = client == null
                ? RowWeave.createBatchSession(label)
                : new BatchSession(label, label, client);
        session.setParallelism(parallelism);
        Source[] inputs = new Source[parts.size()];
        for (int i = 0; i < parts.size(); i++) {
            inputs[i] = new FileSource(ORDER, parts.get(i).toString());
        }
        session.fromSource(inputs)
                .groupBy(ParallelGroupByAgg.of(ORDER, "region").sum("amount"))
                .sinkTo(new FileSink(RESULT, output.toString()));

        long started = System.nanoTime();
        session.execute();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        List<String> rows = fingerprint(output);
        try {
            Files.deleteIfExists(output);
        } catch (Exception ignored) {
            // The finally block still cleans the temporary tree, which affects neither correctness nor the timing.
        }
        return new RunResult(elapsedMs, rows);
    }

    private static List<String> oracle(List<Path> parts) {
        Map<Integer, BigDecimal> sums = new LinkedHashMap<>();
        for (Path part : parts) {
            try (FileSource.Reader rows = new FileSource(ORDER, part.toString()).openReader()) {
                while (rows.hasNext()) {
                    DataRow row = rows.next();
                    Integer region = row.getInteger(2);
                    BigDecimal amount = row.getBigDecimal(3);
                    sums.merge(region, amount, BigDecimal::add);
                }
            }
        }
        List<String> result = new ArrayList<>(sums.size());
        for (Map.Entry<Integer, BigDecimal> entry : sums.entrySet()) {
            result.add(entry.getKey() + "="
                    + entry.getValue().setScale(10, RoundingMode.HALF_UP).toPlainString());
        }
        Collections.sort(result);
        return result;
    }

    private static List<String> fingerprint(Path output) {
        List<String> rows = new ArrayList<>();
        try (FileSource.Reader reader = new FileSource(RESULT, output.toString()).openReader()) {
            while (reader.hasNext()) {
                DataRow row = reader.next();
                rows.add(row.getInteger(0) + "=" + row.getBigDecimal(1).toPlainString());
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static double ratio(long numerator, long denominator) {
        return denominator == 0 ? Double.POSITIVE_INFINITY : (double) numerator / denominator;
    }

    private static Process launch(List<String> jvmArgs, String readyPrefix,
                                  StringBuilder readyOut) throws Exception {
        List<String> command = io.rowweave.test.ForkedJvm.baseCommand();
        io.rowweave.test.ForkedJvm.appendJvmArguments(command, jvmArgs);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        String muxMode = System.getProperty("rowweave.shuffle.mux");
        if (muxMode != null) {
            command.add("-Drowweave.shuffle.mux=" + muxMode);
        }
        command.add("io.rowweave.cluster.ClusterMain");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        BufferedReader output = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        long deadline = System.currentTimeMillis() + 30_000L;
        boolean[] ready = {false};
        Thread drainer = new Thread(() -> {
            try {
                String line;
                while ((line = output.readLine()) != null) {
                    if (!ready[0] && line.startsWith(readyPrefix)) {
                        synchronized (readyOut) {
                            readyOut.append(line.substring(readyPrefix.length()).trim());
                            ready[0] = true;
                            readyOut.notifyAll();
                        }
                    }
                }
            } catch (Exception ignored) {
                // Not being ready is reported by the deadline branch, and closing a stream after the exit must not mask the test's own failure.
            }
        }, "split-perf-process-output");
        drainer.setDaemon(true);
        drainer.start();
        synchronized (readyOut) {
            while (!ready[0] && System.currentTimeMillis() < deadline) {
                readyOut.wait(Math.max(1L, deadline - System.currentTimeMillis()));
            }
        }
        if (!ready[0]) {
            process.destroyForcibly();
            fail("process did not become ready: " + readyPrefix);
        }
        return process;
    }

    private static void kill(Process process) {
        if (process == null) {
            return;
        }
        process.destroyForcibly();
        try {
            process.waitFor(5, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void deleteRecursively(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    private static final class RunResult {
        final long elapsedMs;
        final List<String> rows;

        RunResult(long elapsedMs, List<String> rows) {
            this.elapsedMs = elapsedMs;
            this.rows = rows;
        }
    }
}
