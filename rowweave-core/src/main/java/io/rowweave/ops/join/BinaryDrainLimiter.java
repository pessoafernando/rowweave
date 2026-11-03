package io.rowweave.ops.join;

import io.rowweave.mem.WorkerConfig;

import java.util.concurrent.atomic.AtomicInteger;

/** Process-wide hard cap on binary drain helpers; each acquisition takes and later returns two permits atomically. */
final class BinaryDrainLimiter {

    private static final int EXECUTOR_CAPACITY = defaultCapacity();
    private static final AtomicInteger current = new AtomicInteger();
    private static final AtomicInteger peak = new AtomicInteger();

    private BinaryDrainLimiter() {
    }

    static boolean tryAcquirePair() {
        int limit = maxThreads();
        while (true) {
            int before = current.get();
            if (before > limit - 2) {
                return false;
            }
            int after = before + 2;
            if (current.compareAndSet(before, after)) {
                peak.accumulateAndGet(after, Math::max);
                return true;
            }
        }
    }

    static void releasePair() {
        int after = current.addAndGet(-2);
        if (after < 0) {
            current.addAndGet(2);
            throw new IllegalStateException("binary drain helper permit underflow");
        }
    }

    static int currentThreads() {
        return current.get();
    }

    static int peakThreads() {
        return peak.get();
    }

    static void resetForTest() {
        current.set(0);
        peak.set(0);
    }

    static int executorCapacity() {
        return EXECUTOR_CAPACITY;
    }

    private static int maxThreads() {
        int configured = Integer.getInteger("rowweave.binary.drain.maxThreads", -1);
        if (configured >= 0) {
            return Math.min(configured, EXECUTOR_CAPACITY);
        }
        return EXECUTOR_CAPACITY;
    }

    private static int defaultCapacity() {
        int slots = WorkerConfig.slots();
        return slots > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE : Math.max(2, slots * 2);
    }
}
