package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterConcurrentJobsTest {

    private static final int N = 20_000;
    private static final int KEYS = 100;
    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static Source input() {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    private int i = 0;
                    @Override public boolean hasNext() { return i < N; }
                    @Override public DataRow next() {
                        DataRow r = new DataRow(2);
                        r.set(0, "k" + (i % KEYS));
                        r.set(1, new BigDecimal(i % 50));
                        i++;
                        return r;
                    }
                };
            }
        };
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

    private long[] runJob(String name, int masterPort, Path dir) {
        Path f = dir.resolve(name + ".tsv");
        ExecutionEnvironment s = new ExecutionEnvironment(name, name, new RealClusterJobClient("127.0.0.1", masterPort));
        s.fromSource(input())
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                .sinkTo(new FileSink(GB, f.toString()));
        s.execute();
        return fingerprint(f);
    }

    @Test
    void concurrentJobsOnRealClusterAvailabilityAndThroughput(@TempDir Path tmp) throws Exception {
        File w0 = tmp.resolve("w0").toFile(); w0.mkdirs();
        File w1 = tmp.resolve("w1").toFile(); w1.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer ws0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer ws1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {

            int port = master.port();
            long[] oracle = runJob("oracle", port, tmp);
            assertEquals(KEYS, oracle[0], "groupBySum group count = KEYS");

            System.out.println("\n=== concurrent multi-job on a real cluster (N rows per job=" + N + ", 2 threaded workers, ExecutionEnvironment plus RealClusterJobClient)===");
            System.out.printf("%-6s %-8s %-12s %-14s %-12s %-10s%n",
                    "jobsJ", "ok", "serial ms", "wall ms", "gain", "outcome");

            for (int j : new int[]{2, 4, 8}) {
                long seqStart = System.nanoTime();
                for (int i = 0; i < j; i++) runJob("seq-" + j + "-" + i, port, tmp);
                long seqMs = (System.nanoTime() - seqStart) / 1_000_000;

                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(j);
                AtomicInteger success = new AtomicInteger();
                ConcurrentHashMap<Integer, long[]> results = new ConcurrentHashMap<>();
                List<Thread> threads = new ArrayList<>();
                for (int i = 0; i < j; i++) {
                    final int idx = i;
                    Thread t = new Thread(() -> {
                        try {
                            start.await();
                            results.put(idx, runJob("conc-" + j + "-" + idx, port, tmp));
                            success.incrementAndGet();
                        } catch (Throwable ex) {
                            System.out.println("  job " + idx + " failed: " + ex);
                        } finally {
                            done.countDown();
                        }
                    }, "cluster-conc-" + i);
                    threads.add(t);
                    t.start();
                }
                long concStart = System.nanoTime();
                start.countDown();
                assertTrue(done.await(120, TimeUnit.SECONDS), "concurrency " + j + " jobs must all complete within the timeout (no deadlock)");
                long concMs = (System.nanoTime() - concStart) / 1_000_000;
                for (Thread t : threads) t.join();

                boolean allCorrect = success.get() == j;
                for (long[] res : results.values()) {
                    if (res[0] != oracle[0] || res[1] != oracle[1]) { allCorrect = false; break; }
                }
                double speedup = concMs == 0 ? 0 : (double) seqMs / concMs;
                System.out.printf("%-6d %-8d %-12d %-14d %-12s %-10s%n",
                        j, success.get(), seqMs, concMs, String.format("%.2fx", speedup),
                        allCorrect ? "all correct" : "wrong or failed");

                assertEquals(j, success.get(), "concurrency " + j + " jobs must all succeed (availability)");
                for (long[] res : results.values()) {
                    assertArrayEquals(oracle, res, "the concurrent job result multiset fingerprint matches the serial reference");
                }
            }
            System.out.println("=== availability: every concurrency completes correctly (threaded workers avoid process explosion)===\n");
        }
    }
}

