package io.rowweave.mem;

/**
 * Worker-level configuration. ClusterMain freezes memory and slots through capacity admission, and the
 * compatible direct-call path rejects an invalid value just as strictly, with no silent fallback and no narrowing overflow.
 *
 * <ul>
 * <li>{@code rowweave.worker.memory} -- total process memory pool in bytes (half of heap maxMemory by default)</li>
 * <li>{@code rowweave.worker.slots} -- worker slots (128 by default): admission ceiling, memory divisor (pool/slots) and SlotSemaphore permits</li>
 * <li>{@code rowweave.worker.operator.maxMemory} -- per breaker buffer memory ceiling (16M by default)</li>
 * <li>{@code rowweave.worker.operator.minMemory} -- per breaker buffer memory floor (512K by default)</li>
 * </ul>
 *
 * <p>ClusterMain reads one immutable snapshot; the embedded compatibility path falls back to the system property when no snapshot exists.
 */
public final class WorkerConfig {

    public static final double DEFAULT_MEMORY_FRACTION = 0.5;
    public static final int DEFAULT_SLOTS = 128;
    /** 22-topology + multi-breaker/fan-out/join certified ceiling. */
    public static final int DEFAULT_MAX_STAGE_THREADS = 16;
    public static final long DEFAULT_OPERATOR_MIN_MEMORY = 512L * 1024;
    public static final long OPERATOR_MAX_FLOOR = 2L * 1024 * 1024;  // lower bound 2M on the per breaker ceiling
    public static final long OPERATOR_MAX_CEIL = 16L * 1024 * 1024;  // upper bound 16M on the per breaker ceiling, however large the pool
    public static final int DEFAULT_MERGE_FANIN = 128;              // default external-sort merge fan-in, in line with the Flink max-fan default
    private static volatile ProcessSnapshot processSnapshot;

    private WorkerConfig() {
    }

    /** Total process memory pool in bytes; half of heap maxMemory by default. */
    public static long memoryBytes() {
        ProcessSnapshot snapshot = processSnapshot;
        if (snapshot != null) return snapshot.memoryBytes;
        Long v = positiveCanonicalLong("rowweave.worker.memory");
        if (v != null) return v;
        return Runtime.getRuntime().maxMemory() / 2L;
    }

    /**
     * Worker slots: the ceiling on stage instance threads that may run concurrently. One value governs three places: cluster admission (concurrent worker
     * instances), the memory divisor (per-operator memory is pool/slots) and SlotSemaphore permits. Read from {@code rowweave.worker.slots},
     * 128 by default and never below 1. {@code rowweave.worker.slots} is the only key.
     */
    public static int slots() {
        ProcessSnapshot snapshot = processSnapshot;
        if (snapshot != null) return snapshot.slots;
        Long v = positiveCanonicalLong("rowweave.worker.slots");
        if (v == null) return DEFAULT_SLOTS;
        if (v > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "rowweave.worker.slots exceeds the signed int range");
        }
        return v.intValue();
    }

    /** Maximum threads created inside one admitted assignment, excluding owner/progress. */
    public static int maxStageThreads() {
        ProcessSnapshot snapshot = processSnapshot;
        if (snapshot != null) return snapshot.maxStageThreads;
        Long value = positiveCanonicalLong("rowweave.worker.maxStageThreads");
        if (value == null) return DEFAULT_MAX_STAGE_THREADS;
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "rowweave.worker.maxStageThreads exceeds the signed int range");
        }
        return value.intValue();
    }

    /** ClusterMain-only freeze after preflight and before any Worker resource. */
    public static synchronized void configureProcessSnapshot(long memoryBytes, int slots) {
        configureProcessSnapshot(memoryBytes, slots, DEFAULT_MAX_STAGE_THREADS);
    }

    /** ClusterMain-only frozen process snapshot after aggregate capacity preflight. */
    public static synchronized void configureProcessSnapshot(
            long memoryBytes, int slots, int maxStageThreads) {
        if (memoryBytes <= 0L || slots <= 0 || maxStageThreads <= 0) {
            throw new IllegalArgumentException(
                    "worker process memory, slots and maxStageThreads must be positive");
        }
        ProcessSnapshot current = processSnapshot;
        if (current == null) {
            processSnapshot = new ProcessSnapshot(
                    memoryBytes, slots, maxStageThreads);
        } else if (current.memoryBytes != memoryBytes || current.slots != slots
                || current.maxStageThreads != maxStageThreads) {
            throw new IllegalStateException(
                    "worker process configuration is already frozen");
        }
    }

    static synchronized void resetProcessSnapshotForTest() {
        processSnapshot = null;
    }

    /**
     * Per breaker buffer memory ceiling. The default is clamp(total memory/slots, 2M, 16M): pool/slots tracks the pool capacity, so slots buffers can each
     * take a full share without a cliff, and the clamp keeps a 2M floor so a working set stays usable and a 16M ceiling since no single breaker needs more.
     * rowweave.worker.operator.maxMemory overrides it explicitly, and an override is not clamped.
     */
    public static long operatorMaxMemory() {
        long v = longProp("rowweave.worker.operator.maxMemory", -1);
        if (v > 0) {
            return v;
        }
        long perSlot = memoryBytes() / slots();
        return Math.min(OPERATOR_MAX_CEIL, Math.max(OPERATOR_MAX_FLOOR, perSlot)); // clamp(pool/slots, 2M, 16M)
    }

    /** Per breaker buffer memory floor; 512K by default. */
    public static long operatorMinMemory() {
        long v = longProp("rowweave.worker.operator.minMemory", -1);
        return v > 0 ? v : DEFAULT_OPERATOR_MIN_MEMORY;
    }

    /** Ceiling on concurrent external-sort merge fan-in, that is spill runs open at once, with more passes beyond it. 128 by default, in line with the Flink max-fan default, and never below 2.
     * This bounds the file descriptors one operator holds open, so a large number of small runs cannot exhaust them. rowweave.worker.merge.fanIn overrides it. */
    public static int mergeFanIn() {
        long v = longProp("rowweave.worker.merge.fanIn", -1);
        return v >= 2 ? (int) v : DEFAULT_MERGE_FANIN;
    }

    /** Reads a long system property, returning the fallback when it is missing or unparsable rather than throwing. */
    private static long longProp(String key, long fallback) {
        String s = System.getProperty(key);
        if (s == null || s.isEmpty()) {
            return fallback;
        }
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static Long positiveCanonicalLong(String key) {
        String configured = System.getProperty(key);
        if (configured == null) return null;
        if (configured.isEmpty() || !configured.equals(configured.trim())
                || configured.charAt(0) == '0') {
            throw new IllegalArgumentException(
                    key + " must be a positive canonical decimal");
        }
        for (int i = 0; i < configured.length(); i++) {
            char current = configured.charAt(i);
            if (current < '0' || current > '9') {
                throw new IllegalArgumentException(
                        key + " must be a positive canonical decimal");
            }
        }
        try {
            long parsed = Long.parseLong(configured);
            if (parsed <= 0L) throw new NumberFormatException("non-positive");
            return parsed;
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(
                    key + " is outside the signed long range", invalid);
        }
    }

    private static final class ProcessSnapshot {
        private final long memoryBytes;
        private final int slots;
        private final int maxStageThreads;

        private ProcessSnapshot(long memoryBytes, int slots,
                                int maxStageThreads) {
            this.memoryBytes = memoryBytes;
            this.slots = slots;
            this.maxStageThreads = maxStageThreads;
        }
    }
}
