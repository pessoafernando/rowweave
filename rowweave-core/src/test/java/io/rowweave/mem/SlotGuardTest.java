package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SlotGuard wraps the acquire and release of the semaphore around the second-tier CPU
 * phases, which are the sort and the external merge, so no more than the configured slots
 * run concurrently. It also reports the observed peak concurrency for test assertions.
 */
class SlotGuardTest {

    @AfterEach
    void reset() {
        System.clearProperty("rowweave.worker.slots");
        SlotSemaphore.resetForTest();
        SlotGuard.resetPeakForTest();
    }

    /** slots=2: with ten concurrent CPU phases, the peak inside a phase stays at or below two, which is the semaphore bound. */
    @Test
    void cpuPhaseConcurrencyBoundedBySlots() throws Exception {
        System.setProperty("rowweave.worker.slots", "2");
        SlotSemaphore.resetForTest();
        SlotGuard.resetPeakForTest();

        final int n = 10;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(n);
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger observedPeak = new AtomicInteger();
        Thread[] ts = new Thread[n];
        for (int i = 0; i < n; i++) {
            ts[i] = new Thread(() -> {
                try { start.await(); } catch (InterruptedException e) { return; }
                SlotGuard.cpuPhase(() -> {
                    int cur = inside.incrementAndGet();
                    observedPeak.accumulateAndGet(cur, Math::max);
                    try { Thread.sleep(20); } catch (InterruptedException ignored) { }
                    inside.decrementAndGet();
                    return null;
                });
                done.countDown();
            });
            ts[i].start();
        }
        start.countDown();
        done.await();
        for (Thread t : ts) t.join();

        assertTrue(observedPeak.get() <= 2, "at most 2 segments are in the CPU stage at once, measured peak " + observedPeak.get());
        assertEquals(2, SlotSemaphore.instance().availablePermits(), "every permit is back after all releases");
    }

    /** The cpuPhase return value passes through, and an exception still releases, so no permit leaks. */
    @Test
    void cpuPhaseReturnsValueAndReleasesOnException() {
        System.setProperty("rowweave.worker.slots", "1");
        SlotSemaphore.resetForTest();
        assertEquals("ok", SlotGuard.cpuPhase(() -> "ok"));
        try {
            SlotGuard.cpuPhase(() -> { throw new RuntimeException("boom"); });
        } catch (RuntimeException ignored) { }
        assertEquals(1, SlotSemaphore.instance().availablePermits(), "the permit is still released when the body throws");
    }

    @Test
    @Timeout(2)
    void cpuPhaseIsReentrantOnSameThread() {
        System.setProperty("rowweave.worker.slots", "1");
        SlotSemaphore.resetForTest();
        SlotGuard.resetPeakForTest();

        assertEquals("nested", SlotGuard.cpuPhase(() ->
                SlotGuard.cpuPhase(() -> "nested")));
        assertEquals(1, SlotGuard.peakConcurrency());
        assertEquals(1, SlotSemaphore.instance().availablePermits());
    }

    @Test
    void concurrentForcedSpillRespectsSingleCpuSlotAndBalancesSharedPool() throws Exception {
        System.setProperty("rowweave.worker.slots", "1");
        SlotSemaphore.resetForTest();
        SlotGuard.resetPeakForTest();
        DataSchema meta = new DataSchema(new Field("k", DataType.IntegerType));
        SortField[] fields = {new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT)};
        RowComparator comparator = new RowComparator(meta, fields);
        MemoryManager root = new MemoryManager(1024 * 1024);
        MemoryManager[] sides = root.partitionEvenly(2);
        HeapPool pool = new HeapPool();
        MemorySpillStore store = new MemorySpillStore();
        PointerSortBuffer left = new PointerSortBuffer(meta, comparator, fields, pool, sides[0], 1, store);
        PointerSortBuffer right = new PointerSortBuffer(meta, comparator, fields, pool, sides[1], 1, store);
        CountDownLatch start = new CountDownLatch(1);
        Thread lt = new Thread(() -> fill(left, start));
        Thread rt = new Thread(() -> fill(right, start));
        lt.start();
        rt.start();
        start.countDown();
        lt.join();
        rt.join();
        left.close();
        right.close();
        store.close();

        assertTrue(SlotGuard.peakConcurrency() <= 1);
        assertEquals(1, SlotSemaphore.instance().availablePermits());
        assertEquals(0, root.reservedBytes());
        assertEquals(0, pool.outstanding());
    }

    private static void fill(PointerSortBuffer buffer, CountDownLatch start) {
        try {
            start.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        for (int i = 0; i < 50; i++) {
            DataRow row = new DataRow(1);
            row.set(0, i);
            buffer.putRow(row);
        }
        buffer.sort();
    }
}
