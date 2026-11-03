package io.rowweave.mem;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Slot-semaphore guard around the CPU algorithm sections of the secondary path.
 *
 * <p>Wraps the part of a breaker's secondary path that is pure CPU and never touches a downstream channel: the local {@code sort()} plus writing out a run for sort,
 * and the per-partition external merge for aggregation. The sequence is {@code acquire(1)}, run the section, {@code release(1)}, so at most {@link WorkerConfig#slots} CPU sections run at once.
 *
 * <p><b>Why only the CPU section is wrapped and emit never is</b>: nothing is emitted downstream while a permit is held, so a cycle cannot form in which one thread holds a permit while blocked on a
 * downstream channel that is itself waiting for a permit. {@code sort()} is also a closed interval, unlike a lazy merge whose emit interleaving cannot be separated.
 * The merge and emitRow happen after release and hold no permit.
 */
public final class SlotGuard {

    private static final AtomicInteger peakConcurrency = new AtomicInteger();
    private static final AtomicInteger current = new AtomicInteger();
    private static final ThreadLocal<Integer> depth = new ThreadLocal<>();

    private SlotGuard() {
    }

    /** Runs one pure-CPU algorithm section under the slot semaphore: acquire, run, then release in a finally block so no permit leaks. */
    public static <T> T cpuPhase(Supplier<T> phase) {
        Integer nestedDepth = depth.get();
        if (nestedDepth != null) {
            depth.set(nestedDepth + 1);
            try {
                return phase.get();
            } finally {
                depth.set(nestedDepth);
            }
        }
        Semaphore sem = SlotSemaphore.instance();
        sem.acquireUninterruptibly();
        depth.set(1);
        int cur = current.incrementAndGet();
        peakConcurrency.accumulateAndGet(cur, Math::max);
        try {
            return phase.get();
        } finally {
            try {
                current.decrementAndGet();
                sem.release();
            } finally {
                depth.remove();
            }
        }
    }

    /** Peak CPU-section concurrency observed, for tests to assert on. */
    public static int peakConcurrencyForTest() {
        return peakConcurrency.get();
    }

    /** Read-only observation: the all-time peak concurrency of CPU algorithm sections. */
    public static int peakConcurrency() {
        return peakConcurrency.get();
    }

    /** Resets the peak counter (test use only). */
    public static void resetPeakForTest() {
        peakConcurrency.set(0);
        current.set(0);
        depth.remove();
    }
}
