package io.rowweave.mem;

import java.util.concurrent.Semaphore;

/**
 * Process-wide singleton semaphore over algorithm-section slots. The permit count is {@link WorkerConfig#slots} (128 by default).
 *
 * <p>Limits how many breakers in this process may run a secondary disk-based algorithm section at once, which is where external merge and sort aggregate spend CPU; the primary in-memory path
 * takes no permit. In-process execution means one semaphore for this JVM and a cluster gives each Worker its own, so the meaning is the same either way: bound CPU-heavy algorithm concurrency per process.
 *
 * <p>Lazily initialised; {@link #resetForTest()} lets a test rebuild it after changing the slots value.
 */
public final class SlotSemaphore {

    private static volatile Semaphore instance;

    private SlotSemaphore() {
    }

    /** Process-wide semaphore singleton; permits = {@link WorkerConfig#slots()}, the single slots value. */
    public static Semaphore instance() {
        Semaphore local = instance;
        if (local == null) {
            synchronized (SlotSemaphore.class) {
                if (instance == null) {
                    instance = new Semaphore(WorkerConfig.slots());
                }
                local = instance;
            }
        }
        return local;
    }

    /** Test hook: reset the singleton so the next {@link #instance()} sizes itself from the current slots. Test use only. */
    public static synchronized void resetForTest() {
        instance = null;
    }
}
