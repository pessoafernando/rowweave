package io.rowweave.mem;

/**
 * Memory budget policy, following Flink's MemoryManager in dividing capacity by fraction: it decides the
 * managed memory pool capacity in bytes. Three modes:
 * <ul>
 * <li>{@link #ofBytes(long)} — explicit bytes, the highest priority, overriding both fraction and fallback;</li>
 * <li>{@link #auto(double)} / {@link #autoDefault()} — derived from {@code Runtime.maxMemory() x fraction}
 * (0.4 by default), so a job still has a bounded pool when the caller forgot to configure one and would
 * otherwise run a large job into an OOM;</li>
 * <li>{@code auto(0.0)} — auto sizing disabled (fraction 0), and {@link #resolveBudget(long)} falls back to
 * the Worker-level budget, which keeps earlier configuration working.</li>
 * </ul>
 *
 * <p>An immutable value object; the fallback argument to {@link #resolveBudget(long)} is the Worker-level
 * budget used when auto sizing is disabled.
 */
public final class MemoryBudgetPolicy {

    /** Default automatic fraction, in line with the managed-memory default fraction used by Flink. */
    public static final double DEFAULT_FRACTION = 0.4;

    private final long explicitBytes; // > 0 means an explicit override
    private final double fraction;     // > 0 means automatic mode; == 0 disables it and falls back

    private MemoryBudgetPolicy(long explicitBytes, double fraction) {
        this.explicitBytes = explicitBytes;
        this.fraction = fraction;
    }

    /** Explicit byte budget, highest priority: maxMemory and the fallback are ignored. */
    public static MemoryBudgetPolicy ofBytes(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        return new MemoryBudgetPolicy(bytes, 0.0);
    }

    /** Automatic mode: budget = maxMemory times fraction; fraction=0 disables it and falls back; fraction must lie in [0,1]. */
    public static MemoryBudgetPolicy auto(double fraction) {
        if (fraction < 0 || fraction > 1) {
            throw new IllegalArgumentException("fraction must be in [0, 1]");
        }
        return new MemoryBudgetPolicy(0, fraction);
    }

    /** Default fraction for automatic mode ({@link #DEFAULT_FRACTION}). */
    public static MemoryBudgetPolicy autoDefault() {
        return auto(DEFAULT_FRACTION);
    }

    /**
     * Resolves the final budget in bytes. Explicit bytes &gt; automatic ({@code maxMemory times fraction}) &gt; fallback, used when fraction=0 disables automatic mode.
     * @param fallbackBytes the retreat when automatic mode is disabled: the Worker-level configured budget
     */
    public long resolveBudget(long fallbackBytes) {
        if (explicitBytes > 0) {
            return explicitBytes;
        }
        if (fraction > 0) {
            return (long) (Runtime.getRuntime().maxMemory() * fraction);
        }
        return fallbackBytes; // disabled -> backward-compatible fallback
    }
}
