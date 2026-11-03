package io.rowweave.bench;

import io.rowweave.RowWeave;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A benchmark of many concurrent jobs submitted through the single-machine
 * {@link BatchSession#commit()}. It measures availability, in that N jobs submitted at
 * once all complete correctly, each result multiset matching the serial oracle with no deadlock,
 * timeout or exhausted heap, and it measures throughput, in that the total wall clock of N
 * concurrent runs is reported against the accumulated wall clock of N serial runs, which shows
 * whether concurrency can amortize the fixed cost of one job and use the available cores.
 *
 * <p>Data streams from CSV in O(1) memory, and every job has its own source and sink with
 * nothing shared. Runs go through BatchSession.execute() rather than internal objects.
 * Tagged bench, so by hand: {@code mvn test -Dtest=ConcurrentJobsBenchmarkTest -Dgroups=bench
 * -Drowweave.bench.orders=500000}.
 */
@Tag("bench")
class ConcurrentJobsBenchmarkTest {

    private static final long ORDERS = Long.getLong("rowweave.bench.orders", 300_000L);
    private static final int DEPTS = 200;
    private static final int REGIONS = 50;
    private static final int[] CONCURRENCY = {1, 2, 4, 8, 16};

    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema GB_REGION =
            ParallelGroupByAgg.of(ORDER, "region").sum("amount").getOutputSchema();

    private static String ordersCsv;

    private static Source orders()  { return new CsvInput(ordersCsv, ORDER); }

    private void ensureData() {
        if (ordersCsv != null) return;
        java.nio.file.Path dir = java.nio.file.Paths.get(
                System.getProperty("java.io.tmpdir"), "rowweave-bench-csv", "n" + ORDERS);
        try { java.nio.file.Files.createDirectories(dir); } catch (Exception e) { throw new RuntimeException(e); }
        ordersCsv = CsvDataGen.orders(dir, ORDERS, DEPTS, REGIONS).toString();
    }

    /**
     * One job is a groupBy over region summing the amount, run through
     * BatchSession.execute(), with the result written to a FileSink in O(1) memory so the
     * heap is never exhausted. Returns the multiset fingerprint of that file, the row count and the
     * order-independent hash sum, which is the correctness check without materializing rows.
     */
    private long[] runJob(String name) {
        Path f;
        try { f = Files.createTempFile("conc-" + name + "-", ".tsv"); } catch (Exception e) { throw new RuntimeException(e); }
        try {
            BatchSession s = RowWeave.createBatchSession(name);
            s.setParallelism(1);  // serial inside one job, with concurrency across jobs, so this measures job-level concurrency rather than intra-job parallelism
            DataSet g = s.fromSource(orders())
                    .groupBy(ParallelGroupByAgg.of(ORDER, "region").sum("amount"));
            g.sinkTo(new FileSink(g.getSchema(), f.toString()));
            s.execute();
            return fileFingerprint(f);
        } finally {
            try { Files.deleteIfExists(f); } catch (Exception ignored) { }
        }
    }

    /** Streams the file in O(1) memory and returns the row count plus the order-independent 64-bit row hash sum, the multiset fingerprint, without materializing rows. */
    private static long[] fileFingerprint(Path path) {
        long count = 0, sum = 0;
        try (BufferedReader r = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
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

    @Test
    void concurrentJobsAvailabilityAndThroughput() throws Exception {
        ensureData();
        long[] oracle = runJob("oracle");  // the fingerprint of the serial single job is the correctness baseline

        System.out.println("\n=== concurrent multi-job benchmark (orders=" + ORDERS + "/job, each job=groupBySum(region), BatchSession.commit)===");
        System.out.printf("%-6s %-8s %-12s %-14s %-12s %-10s%n",
                "jobs", "ok", "serial ms", "wall ms", "gain", "outcome");

        for (int n : CONCURRENCY) {
            // The serial baseline runs N jobs one after another and accumulates the wall clock.
            long seqStart = System.nanoTime();
            for (int i = 0; i < n; i++) runJob("seq-" + i);
            long seqMs = (System.nanoTime() - seqStart) / 1_000_000;

            // Concurrent submits all N jobs, each on its own thread, waits for every one, and takes the wall clock.
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(n);
            AtomicInteger success = new AtomicInteger();
            ConcurrentHashMap<Integer, long[]> results = new ConcurrentHashMap<>();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                final int idx = i;
                Thread t = new Thread(() -> {
                    try {
                        start.await();
                        results.put(idx, runJob("conc-" + idx));
                        success.incrementAndGet();
                    } catch (Throwable ex) {
                        System.out.println("  job " + idx + " failed: " + ex);
                    } finally {
                        done.countDown();
                    }
                }, "conc-job-" + i);
                threads.add(t);
                t.start();
            }
            long concStart = System.nanoTime();
            start.countDown();
            done.await();
            long concMs = (System.nanoTime() - concStart) / 1_000_000;
            for (Thread t : threads) t.join();

            // Availability: every job succeeds and each result fingerprint matches the oracle.
            boolean allCorrect = success.get() == n;
            for (long[] res : results.values()) {
                if (!Arrays.equals(res, oracle)) { allCorrect = false; break; }
            }
            double speedup = concMs == 0 ? 0 : (double) seqMs / concMs;
            System.out.printf("%-6d %-8d %-12d %-14d %-12s %-10s%n",
                    n, success.get(), seqMs, concMs, String.format("%.2fx", speedup),
                    allCorrect ? "all correct" : "wrong or failed");

            assertEquals(n, success.get(), "concurrency " + n + " jobs must all succeed (no deadlock, no timeout, no OOM)");
            for (long[] res : results.values()) {
                assertArrayEquals(oracle, res, "the concurrent job result multiset fingerprint must match the serial expectation");
            }
        }
        System.out.println("=== availability: every concurrency completes correctly; gain = serial total / parallel wall (>1 means concurrency spreads the overhead across cores)===\n");
    }
}
