package io.rowweave.run;

import java.util.Properties;

/** Production configuration and safe time arithmetic for terminal Run retention. */
public final class RunRetentionPolicy {
    public static final long DEFAULT_TERMINAL_RETENTION_MS =
            30L * 24L * 60L * 60L * 1000L;
    public static final long MIN_PRODUCTION_RETENTION_MS = 5L * 60L * 1000L;
    public static final long MAX_RETENTION_MS =
            10L * 365L * 24L * 60L * 60L * 1000L;

    private static final int MAX_ENTRIES_PER_SHARD = 4096;
    private static final int MAX_WORK_PER_PASS = 1000;
    private static final long MIN_PRODUCTION_INTERVAL_MS = 100L;
    private static final long MAX_INTERVAL_MS = 60L * 60L * 1000L;

    private final long terminalRetentionMs;
    private final int maxEntriesPerShard;
    private final int scanPageSize;
    private final int maxDeletesPerPass;
    private final long maintenanceIntervalMs;

    private RunRetentionPolicy(long terminalRetentionMs, int maxEntriesPerShard,
                               int scanPageSize, int maxDeletesPerPass,
                               long maintenanceIntervalMs, boolean production) {
        long minimumRetention = production ? MIN_PRODUCTION_RETENTION_MS : 1L;
        long minimumInterval = production ? MIN_PRODUCTION_INTERVAL_MS : 1L;
        if (terminalRetentionMs < minimumRetention
                || terminalRetentionMs > MAX_RETENTION_MS) {
            throw new IllegalArgumentException("terminal retention is out of bounds");
        }
        if (maxEntriesPerShard < 1 || maxEntriesPerShard > MAX_ENTRIES_PER_SHARD) {
            throw new IllegalArgumentException("lifecycle shard capacity is out of bounds");
        }
        if (scanPageSize < 1 || scanPageSize > MAX_WORK_PER_PASS) {
            throw new IllegalArgumentException("scan page size is out of bounds");
        }
        if (maxDeletesPerPass < 1 || maxDeletesPerPass > MAX_WORK_PER_PASS) {
            throw new IllegalArgumentException("delete limit is out of bounds");
        }
        if (maintenanceIntervalMs < minimumInterval
                || maintenanceIntervalMs > MAX_INTERVAL_MS) {
            throw new IllegalArgumentException("maintenance interval is out of bounds");
        }
        this.terminalRetentionMs = terminalRetentionMs;
        this.maxEntriesPerShard = maxEntriesPerShard;
        this.scanPageSize = scanPageSize;
        this.maxDeletesPerPass = maxDeletesPerPass;
        this.maintenanceIntervalMs = maintenanceIntervalMs;
    }

    public static RunRetentionPolicy fromProperties(Properties properties) {
        if (properties == null) throw new IllegalArgumentException("properties is required");
        return new RunRetentionPolicy(
                longProperty(properties, "rowweave.master.runRetention.terminalMs",
                        DEFAULT_TERMINAL_RETENTION_MS),
                intProperty(properties, "rowweave.master.runRetention.maxEntriesPerShard", 4096),
                intProperty(properties, "rowweave.master.runRetention.scanPageSize", 128),
                intProperty(properties, "rowweave.master.runRetention.maxDeletesPerPass", 32),
                longProperty(properties, "rowweave.master.runRetention.maintenanceIntervalMs",
                        60_000L), true);
    }

    static RunRetentionPolicy testing(long terminalRetentionMs, int maxEntriesPerShard,
                                      int scanPageSize, int maxDeletesPerPass,
                                      long maintenanceIntervalMs) {
        return new RunRetentionPolicy(terminalRetentionMs, maxEntriesPerShard,
                scanPageSize, maxDeletesPerPass, maintenanceIntervalMs, false);
    }

    public long terminalRetentionMs() { return terminalRetentionMs; }
    public int maxEntriesPerShard() { return maxEntriesPerShard; }
    public int scanPageSize() { return scanPageSize; }
    public int maxDeletesPerPass() { return maxDeletesPerPass; }
    public long maintenanceIntervalMs() { return maintenanceIntervalMs; }

    public boolean due(long nowMillis, long terminalAtMillis) {
        if (nowMillis < 0L) throw new IllegalArgumentException("nowMillis must be >= 0");
        if (terminalAtMillis <= 0L) {
            throw new IllegalArgumentException("terminalAtMillis must be > 0");
        }
        return nowMillis >= terminalAtMillis
                && nowMillis - terminalAtMillis >= terminalRetentionMs;
    }

    public static long clampAuthorityTime(long observedMillis, long previousMillis) {
        if (observedMillis < 0L || previousMillis < 0L) {
            throw new IllegalArgumentException("authority time must be >= 0");
        }
        return Math.max(observedMillis, previousMillis);
    }

    private static int intProperty(Properties properties, String key, int defaultValue) {
        String value = properties.getProperty(key);
        try {
            return value == null || value.trim().isEmpty()
                    ? defaultValue : Integer.parseInt(value.trim());
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("invalid " + key, invalid);
        }
    }

    private static long longProperty(Properties properties, String key, long defaultValue) {
        String value = properties.getProperty(key);
        try {
            return value == null || value.trim().isEmpty()
                    ? defaultValue : Long.parseLong(value.trim());
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("invalid " + key, invalid);
        }
    }
}
