package io.rowweave.cluster.rpc;

import java.util.Objects;
import java.util.Properties;

/** immutable, strictly parsed execution and cleanup budgets for ephemeral submits. */
public final class EphemeralSubmitTimeoutPolicy {
    public static final long DEFAULT_EXECUTION_TIMEOUT_MS = 600_000L;
    public static final long DEFAULT_CLEANUP_TIMEOUT_MS = 60_000L;
    public static final long MIN_TIMEOUT_MS = 1_000L;
    public static final long MAX_TIMEOUT_MS = 2_147_000_000L;
    public static final int PROTOCOL_SLACK_MS = 5_000;

    private static final String CLIENT_EXECUTION = "rowweave.cluster.submitTimeoutMs";
    private static final String CLIENT_CLEANUP = "rowweave.cluster.submitCleanupGraceMs";
    private static final String MASTER_EXECUTION = "rowweave.cluster.ephemeralJobTimeoutMs";
    private static final String MASTER_CLEANUP = "rowweave.cluster.ephemeralCleanupTimeoutMs";
    private static final String LEGACY_HARD_LIMIT = "rowweave.cluster.jobHardLimitMs";

    private final Budget budget;
    private final long executionHardLimitMs;

    private EphemeralSubmitTimeoutPolicy(Budget budget, long executionHardLimitMs) {
        this.budget = Objects.requireNonNull(budget, "submit timeout budget");
        if (executionHardLimitMs != 0L) {
            requireTimeout(LEGACY_HARD_LIMIT, executionHardLimitMs);
        }
        this.executionHardLimitMs = executionHardLimitMs;
    }

    public static EphemeralSubmitTimeoutPolicy clientFromProperties(Properties properties) {
        requireProperties(properties);
        return new EphemeralSubmitTimeoutPolicy(Budget.of(
                parseTimeout(properties, CLIENT_EXECUTION, DEFAULT_EXECUTION_TIMEOUT_MS),
                parseTimeout(properties, CLIENT_CLEANUP, DEFAULT_CLEANUP_TIMEOUT_MS)), 0L);
    }

    public static EphemeralSubmitTimeoutPolicy clientFromSystemProperties() {
        return clientFromProperties(System.getProperties());
    }

    public static EphemeralSubmitTimeoutPolicy masterFromProperties(Properties properties) {
        requireProperties(properties);
        Budget defaults = Budget.of(
                parseTimeout(properties, MASTER_EXECUTION, DEFAULT_EXECUTION_TIMEOUT_MS),
                parseTimeout(properties, MASTER_CLEANUP, DEFAULT_CLEANUP_TIMEOUT_MS));
        long hardLimit = parseZeroOrTimeout(properties, LEGACY_HARD_LIMIT, 0L);
        return new EphemeralSubmitTimeoutPolicy(defaults, hardLimit);
    }

    public static EphemeralSubmitTimeoutPolicy masterFromSystemProperties() {
        return masterFromProperties(System.getProperties());
    }

    /** Client-advertised budget, or the finite server default for a legacy request, capped by legacy ops limit. */
    public Budget resolve(Budget requested) {
        Budget selected = requested == null ? budget : requested;
        if (executionHardLimitMs == 0L
                || selected.executionTimeoutMs() <= executionHardLimitMs) {
            return selected;
        }
        return Budget.of(executionHardLimitMs, selected.cleanupTimeoutMs());
    }

    public Budget budget() {
        return budget;
    }

    public int clientReadTimeoutMillis() {
        return clientReadTimeoutMillis(budget);
    }

    public static int clientReadTimeoutMillis(Budget budget) {
        Objects.requireNonNull(budget, "submit timeout budget");
        long timeout = checkedClientReadTimeout(
                budget.executionTimeoutMs(), budget.cleanupTimeoutMs());
        return (int) timeout;
    }

    private static long parseTimeout(Properties properties, String key, long defaultValue) {
        String raw = properties.getProperty(key);
        long value = raw == null ? defaultValue : strictDecimal(key, raw);
        requireTimeout(key, value);
        return value;
    }

    private static long parseZeroOrTimeout(Properties properties, String key, long defaultValue) {
        String raw = properties.getProperty(key);
        long value = raw == null ? defaultValue : strictDecimal(key, raw);
        if (value != 0L) requireTimeout(key, value);
        return value;
    }

    private static long strictDecimal(String key, String raw) {
        if (raw.isEmpty()) throw invalid(key);
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c < '0' || c > '9') throw invalid(key);
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException malformed) {
            throw invalid(key, malformed);
        }
    }

    private static void requireTimeout(String key, long value) {
        if (value < MIN_TIMEOUT_MS || value > MAX_TIMEOUT_MS) {
            throw new IllegalArgumentException(key + " must be in "
                    + MIN_TIMEOUT_MS + ".." + MAX_TIMEOUT_MS + " milliseconds");
        }
    }

    private static long checkedClientReadTimeout(long execution, long cleanup) {
        final long terminal;
        try {
            terminal = Math.addExact(Math.addExact(execution, cleanup), PROTOCOL_SLACK_MS);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("ephemeral submit timeout sum overflows", overflow);
        }
        if (terminal > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "ephemeral submit timeout sum exceeds socket timeout range");
        }
        return terminal;
    }

    private static void requireProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("ephemeral submit timeout properties must not be null");
        }
    }

    private static IllegalArgumentException invalid(String key) {
        return new IllegalArgumentException(key + " must be an unsigned decimal integer");
    }

    private static IllegalArgumentException invalid(String key, Throwable cause) {
        return new IllegalArgumentException(key + " must be an unsigned decimal integer", cause);
    }

    /** Atomic wire value. Zero is deliberately unavailable so null remains the legacy-schema sentinel. */
    public static final class Budget {
        private final long executionTimeoutMs;
        private final long cleanupTimeoutMs;

        private Budget(long executionTimeoutMs, long cleanupTimeoutMs) {
            requireTimeout("executionTimeoutMs", executionTimeoutMs);
            requireTimeout("cleanupTimeoutMs", cleanupTimeoutMs);
            checkedClientReadTimeout(executionTimeoutMs, cleanupTimeoutMs);
            this.executionTimeoutMs = executionTimeoutMs;
            this.cleanupTimeoutMs = cleanupTimeoutMs;
        }

        public static Budget of(long executionTimeoutMs, long cleanupTimeoutMs) {
            return new Budget(executionTimeoutMs, cleanupTimeoutMs);
        }

        public long executionTimeoutMs() {
            return executionTimeoutMs;
        }

        public long cleanupTimeoutMs() {
            return cleanupTimeoutMs;
        }
    }
}
