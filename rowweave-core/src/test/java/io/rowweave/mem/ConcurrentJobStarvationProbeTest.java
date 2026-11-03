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
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression: concurrent jobs submitted through {@link LocalJobClient} share the
 * process-wide {@link GlobalMemoryPool}. Four jobs each declare a 256K guarantee, which
 * fills the 1M hard ceiling exactly, and admission must not starve because the first job
 * borrows all the slack.
 */
class ConcurrentJobStarvationProbeTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    /** A fixed 1MB process pool, small enough that the first job fills it under static pre-allocation, which amplifies concurrent starvation. */
    private static final long POOL_BYTES = 1L * 1024 * 1024;

    @BeforeEach
    void setSmallPool() {
        System.setProperty("rowweave.worker.operator.maxMemory", Long.toString(256L * 1024));
        System.setProperty("rowweave.worker.operator.minMemory", Long.toString(256L * 1024));
        GlobalMemoryPool.configureForTest(POOL_BYTES);
    }

    @AfterEach
    void resetPool() {
        System.clearProperty("rowweave.worker.operator.maxMemory");
        System.clearProperty("rowweave.worker.operator.minMemory");
        GlobalMemoryPool.resetForTest();
    }

    /** A re-entrant in-memory Source, five rows per job, grouped by one field into a single breaker. */
    private static Source smallInput() {
        final List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            DataRow r = new DataRow(2);
            r.set(0, "k" + (i % 2));
            r.set(1, new BigDecimal(Integer.toString(i)));
            rows.add(r);
        }
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return rows.iterator(); }
        };
    }

    /** Submits a single-breaker groupBy job through LocalJobClient; true on success, false on a MemoryBudgetExceededException from an exhausted pool. */
    private static boolean submitGroupByJob() {
        try {
            ExecutionEnvironment session = RowWeave.createEnvironment("probe");
            DataSet grouped = session.fromSource(smallInput())
                    .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
            grouped.sinkTo(new WorkerCollectingOutput(grouped.getSchema()));
            new LocalJobClient(1).submitToSink(session.toPlanGraph());
            return true;
        } catch (MemoryBudgetExceededException e) {
            return false;
        }
    }

    @Test
    void concurrentJobsDoNotStarve() throws Exception {
        final int n = 4;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(n);
        AtomicInteger successCount = new AtomicInteger();
        ConcurrentLinkedQueue<Thread> threads = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < n; i++) {
            Thread t = new Thread(() -> {
                try {
                    startGate.await();  // the N threads release together to reach commit as simultaneously as possible
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (submitGroupByJob()) {
                    successCount.incrementAndGet();
                }
                done.countDown();
            });
            threads.add(t);
            t.start();
        }

        startGate.countDown();
        done.await();
        for (Thread t : threads) t.join();

        assertEquals(n, successCount.get(),
                "four floors that fit together let every concurrent groupBy job succeed; succeeded " + successCount.get());
    }
}
