package io.rowweave.mem;

/**
 * Process-wide managed memory pool: a singleton whose capacity is {@link WorkerConfig#memoryBytes()} (rowweave.worker.memory, half of heap by default).
 * When jobs run concurrently, each one takes a per-job budget from this pool through {@link #acquire}, which returns an
 * independent {@link MemoryManager}, and hands it back through {@link #release} when finished. Oversubscription fails fast with an explicit
 * {@link MemoryBudgetExceededException} rather than a silent OOM, which bounds the process total. Thread-safe by delegating to the {@link ManagedMemory} lock.
 *
 * <p>Lazily initialised: the first {@link #instance()} call without a prior {@link #configure(long)} sizes the pool from the default fraction. configure is
 * idempotent: once the pool exists it is ignored, so the total cannot change underneath live per-job budgets.
 */
public final class GlobalMemoryPool {

    private static volatile ManagedMemory instance;

    private GlobalMemoryPool() {
    }

    /** Sets the global pool capacity explicitly in bytes; effective only before initialisation (idempotent), for one-time process start-up setup. */
    public static synchronized void configure(long totalBytes) {
        if (instance == null) {
            instance = ManagedMemory.ofBytes(totalBytes);
        }
    }

    /** Global pool singleton; when unconfigured, sizes itself lazily from the default fraction (maxMemory x 0.4). */
    public static ManagedMemory instance() {
        ManagedMemory local = instance;
        if (local == null) {
            synchronized (GlobalMemoryPool.class) {
                if (instance == null) {
                    instance = ManagedMemory.ofBytes(WorkerConfig.memoryBytes());
                }
                local = instance;
            }
        }
        return local;
    }

    /** Test hook: force the total capacity, replacing any existing instance. Test use only. */
    public static synchronized void configureForTest(long totalBytes) {
        instance = ManagedMemory.ofBytes(totalBytes);
    }

    /** Test hook: reset the singleton so the next {@link #instance()} sizes lazily again. Test use only, to avoid leaking state across tests. */
    public static synchronized void resetForTest() {
        instance = null;
    }
}
