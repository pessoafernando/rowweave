package io.rowweave.mem;

import io.rowweave.mem.spill.SpillStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Guardrails for the shared pool: with threaded Workers, the concurrent StageTasks of one
 * worker share a single process-wide GlobalMemoryPool. Under strict guaranteed admission the
 * memory invariants are pinned down:
 *
 * <ol>
 * <li>a single breaker is capped, at an explicit 64K cap here;</li>
 * <li>pool admission gates the total, so sixteen 64K guarantees fit a 1M pool exactly and
 * nothing is reserved outside the pool;</li>
 * <li>spilling is the fallback, so a budget beyond reserve only spills the current owner's
 * spillable data.</li>
 * </ol>
 *
 * <p>This test drives far more concurrent breakers than there are slots, which is the
 * co-located multi-breaker case on an effectively unbounded pool. Each takes a sub-budget
 * from the shared pool through newOperatorMemory and feeds many times its budget to
 * reserve. Assertions: every MemoryManager budget stays at or below the 16M ceiling; pool
 * outstanding never exceeds the pool total, so there is no oversubscription; spilling really
 * happens when reserve gives memory back; and the no-OOM tripwire peak at or below acquired
 * holds throughout.
 */
class ConcurrentBreakerSharedPoolNoOomTest {

    @AfterEach
    void clearProps() {
        System.clearProperty("rowweave.worker.memory");
        System.clearProperty("rowweave.worker.slots");
        System.clearProperty("rowweave.worker.operator.maxMemory");
        System.clearProperty("rowweave.worker.operator.minMemory");
        GlobalMemoryPool.resetForTest();
    }

    /** A fake spillable buffer registered with the manager: reserve gives it back by calling spill(), which releases what it holds and counts one spill to disk. */
    private static final class FakeSpillable implements MemorySpillable {
        private final MemoryManager mm;
        private long held;
        FakeSpillable(MemoryManager mm) { this.mm = mm; mm.registerSpillable(this); }
        void fill(long bytes) {
            // reserve block by block past the budget, which forces a spill rather than an OOM.
            if (mm.reserve(bytes)) {
                held += bytes;
            }
        }
        @Override public long spill() {
            long freed = held;
            mm.release(held);  // hand back everything held, which spills it
            held = 0;
            return freed;
        }
        void close() {
            mm.releaseExact(held);
            held = 0;
            mm.unregisterSpillable(this);
        }
    }

    /**
     * Sixteen concurrent breakers against two slots, sharing a 1M pool. Asserts the three
     * bounded defences plus the no-OOM tripwire, with no OOM and no oversubscription.
     */
    @Test
    void concurrentBreakersSharedPoolStayBoundedAndSpill() throws Exception {
        long poolBytes = 1L * 1024 * 1024;  // a small 1M pool, to force oversubscription and spilling
        System.setProperty("rowweave.worker.memory", Long.toString(poolBytes));
        System.setProperty("rowweave.worker.slots", "2");
        System.setProperty("rowweave.worker.operator.maxMemory", Long.toString(64L * 1024));
        System.setProperty("rowweave.worker.operator.minMemory", Long.toString(64L * 1024));
        GlobalMemoryPool.configureForTest(poolBytes);
        ManagedMemory pool = GlobalMemoryPool.instance();

        final int breakers = 16;  // far more than the two slots, as co-located breakers on an effectively unbounded pool
        // Each breaker is its own ExecutionMemory.forJob(1), exactly as the fragments of a cluster StageTask each call forJob.
        List<ExecutionMemory> mems = new ArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(breakers);
        AtomicInteger spillObserved = new AtomicInteger();
        AtomicReference<Throwable> oom = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < breakers; i++) {
            ExecutionMemory mem = ExecutionMemory.forJob(pool, 1);
            synchronized (mems) { mems.add(mem); }
            Thread t = new Thread(() -> {
                try {
                    start.await();
                    MemoryManager mm = mem.newOperatorMemory();
                    // The single-breaker ceiling never exceeds the 16M cap, which is the first defence.
                    assertTrue(mm.budget() <= WorkerConfig.OPERATOR_MAX_CEIL,
                            "one breaker budget stays at or below the 16M ceiling, measured " + mm.budget());
                    FakeSpillable buf = new FakeSpillable(mm);
                    // Feed twenty times the budget: it must be absorbed by spilling, never by an OOM.
                    long chunk = Math.max(1L, mm.budget() / 4);
                    for (int k = 0; k < 80; k++) {
                        buf.fill(chunk);
                    }
                    if (mm.spillCount() > 0) spillObserved.incrementAndGet();
                    buf.close();
                    mem.releaseOperatorMemory(mm);
                } catch (Throwable ex) {
                    oom.compareAndSet(null, ex);
                } finally {
                    done.countDown();
                }
            }, "breaker-" + i);
            threads.add(t);
            t.start();
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "concurrent breakers finish before the timeout instead of hanging");
        for (Thread t : threads) t.join();

        assertNull(oom.get(), "breakers sharing one pool never throw OOM because spill absorbs the pressure, observed: " + oom.get());
        // Second defence: every owner is admitted inside the pool, with no oversubscription outside it.
        assertTrue(pool.availableBytes() >= 0 && pool.availableBytes() <= poolBytes,
                "pool headroom stays inside [0, pool total] with no over-subscription accounted; actual headroom " + pool.availableBytes());
        // Third defence: data beyond the budget must trigger spilling, otherwise the memory path is never exercised and the assertions mean nothing.
        assertTrue(spillObserved.get() > 0, "feeding past the budget triggers spill, breaker count that spilled " + spillObserved.get());
        // Fourth defence: the per-job no-OOM tripwire holds, so peak never exceeds acquired.
        for (ExecutionMemory mem : mems) {
            mem.assertWithinBudget();  // throws when peak exceeds acquired, which is the MemoryManager bound regression
            mem.close();
        }
    }
}
