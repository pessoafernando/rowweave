package io.rowweave.mem;

import io.rowweave.RowWeave;
import io.rowweave.WorkerCollectingOutput;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.local.LocalJobClient;
import io.rowweave.ops.Distinct;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency capacity under load. When the full owner guarantee set for N jobs fits
 * atomically inside the 96MB envelope, every job succeeds and nothing deadlocks, and a
 * thread failure must still release the test latch rather than turn into a permanent wait.
 *
 * <p>Each job is groupBy then distinct then orderBy, three breakers, submitted through
 * {@link LocalJobClient} and sharing the process-wide fixed pool. Each job has three owners
 * with an explicit 512KB guarantee each, so N=64 is exactly 96MB. Steady rejection beyond
 * the guaranteed capacity is covered by a separate admission test.
 */
class ConcurrencyCapacityProbeTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    private static final long POOL_BYTES = 96L * 1024 * 1024;

    @BeforeEach
    void setPool() {
        System.setProperty("rowweave.worker.operator.minMemory", "524288");
        GlobalMemoryPool.configureForTest(POOL_BYTES);
    }

    @AfterEach
    void reset() {
        GlobalMemoryPool.resetForTest();
        ExecutionMemory.setAcquireListenerForTest(null);
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    private static Source input() {
        final List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            DataRow r = new DataRow(2);
            r.set(0, "k" + (i % 200));
            r.set(1, new BigDecimal(Integer.toString(i)));
            rows.add(r);
        }
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return rows.iterator(); }
        };
    }

    /** A three-breaker job: groupBy(sum) then distinct then orderBy. True when it succeeds, false when the pool is exhausted. */
    private static boolean submitJob() {
        try {
            ExecutionEnvironment s = RowWeave.createEnvironment("probe");
            DataSet ds = s.fromSource(input()).groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
            DataSchema gm = ds.getSchema();
            ds = ds.distinct(Distinct.byColumns(gm, "k"))
                    .orderBy(OrderBy.of(gm, new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                            1, false);
            ds.sinkTo(new WorkerCollectingOutput(ds.getSchema()));
            new LocalJobClient(1).submitToSink(s.toPlanGraph());
            return true;
        } catch (MemoryBudgetExceededException e) {
            return false;
        }
    }

    @Test
    void concurrencyCapacityMonotonicNoStarvation() throws Exception {
        System.out.println("\n=== concurrent capacity (pool=" + (POOL_BYTES / 1024 / 1024)
                + " MB, 3 owners per job, a 512KB floor per owner, strictly atomic admission) ===");
        System.out.printf("%-6s %-8s %-8s %-8s %-18s%n",
                "jobs", "ok", "refused", "error", "owner claimed (L1/L2)");

        for (int n : new int[]{1, 2, 4, 8, 16, 32, 64}) {
            AtomicInteger l1 = new AtomicInteger(), l2 = new AtomicInteger();
            ExecutionMemory.setAcquireListenerForTest((level, granted, waitedNs, parkRounds) ->
                    (level == 1 ? l1 : l2).incrementAndGet());

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(n);
            AtomicInteger success = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<Thread> threads = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < n; i++) {
                Thread t = new Thread(() -> {
                    try {
                        start.await();
                        if (submitJob()) success.incrementAndGet();
                        else rejected.incrementAndGet();
                    } catch (Throwable failure) {
                        unexpected.add(failure);
                    } finally {
                        done.countDown();
                    }
                });
                threads.add(t); t.start();
            }
            start.countDown();
            boolean completed = done.await(60, TimeUnit.SECONDS);
            if (!completed) {
                for (Thread t : threads) t.interrupt();
            }
            for (Thread t : threads) t.join(5000L);

            System.out.printf("%-6d %-8d %-8d %-8d %-18s%n", n, success.get(), rejected.get(),
                    unexpected.size(), l1.get() + "/" + l2.get());
            assertTrue(completed, "concurrency " + n + " job finishes within a bounded time");
            assertTrue(unexpected.isEmpty(), () -> "a concurrent job failed for a reason other than resource refusal: " + unexpected.peek());
            assertEquals(n, success.get() + rejected.get(), "every job either succeeds or is refused on resources");
            assertEquals(0, rejected.get(), "every floor in this matrix fits inside the 96MB envelope");
            ExecutionMemory.setAcquireListenerForTest(null);
        }
        System.out.println("=== atomic admission without deadlock: N more successes for N jobs, no guarantee outside the pool and no indefinite wait ===\n");
    }
}
