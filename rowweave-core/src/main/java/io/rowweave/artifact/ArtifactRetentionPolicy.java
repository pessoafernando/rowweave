package io.rowweave.artifact;

import java.util.Properties;

/** Production hard bounds for Artifact inventory, orphan grace and one maintenance round. */
public final class ArtifactRetentionPolicy {
    public static final int DEFAULT_MAX_ENTRIES_PER_SHARD = 256;
    public static final long DEFAULT_MAX_BYTES_PER_SHARD = 1L << 30;
    public static final long DEFAULT_ORPHAN_GRACE_MS = 24L * 60L * 60L * 1000L;
    public static final long MIN_PRODUCTION_GRACE_MS = 5L * 60L * 1000L;
    public static final long MAX_GRACE_MS =
            10L * 365L * 24L * 60L * 60L * 1000L;

    private static final int MAX_ENTRIES_PER_SHARD = 4096;
    private static final long MAX_BYTES_PER_SHARD = 1L << 40;
    private static final int MAX_WORK_PER_PASS = 1000;
    private static final long MIN_PRODUCTION_INTERVAL_MS = 100L;
    private static final long MAX_INTERVAL_MS = 60L * 60L * 1000L;

    private final int maxEntriesPerShard;
    private final long maxBytesPerShard;
    private final long orphanGraceMs;
    private final int scanPageSize;
    private final int maxDeletesPerPass;
    private final long maintenanceIntervalMs;

    private ArtifactRetentionPolicy(int maxEntriesPerShard, long maxBytesPerShard,
                                    long orphanGraceMs, int scanPageSize,
                                    int maxDeletesPerPass, long maintenanceIntervalMs,
                                    boolean production) {
        long minimumGrace = production ? MIN_PRODUCTION_GRACE_MS : 1L;
        long minimumInterval = production ? MIN_PRODUCTION_INTERVAL_MS : 1L;
        if (maxEntriesPerShard < 1 || maxEntriesPerShard > MAX_ENTRIES_PER_SHARD) {
            throw new IllegalArgumentException("artifact shard entry capacity is out of bounds");
        }
        long minimumBytes = production ? PlanArtifactCodec.MAX_FRAME_BYTES : 1L;
        if (maxBytesPerShard < minimumBytes
                || maxBytesPerShard > MAX_BYTES_PER_SHARD) {
            throw new IllegalArgumentException("artifact shard byte capacity is out of bounds");
        }
        if (orphanGraceMs < minimumGrace || orphanGraceMs > MAX_GRACE_MS) {
            throw new IllegalArgumentException("artifact orphan grace is out of bounds");
        }
        if (scanPageSize < 1 || scanPageSize > MAX_WORK_PER_PASS) {
            throw new IllegalArgumentException("artifact scan page size is out of bounds");
        }
        if (maxDeletesPerPass < 1 || maxDeletesPerPass > MAX_WORK_PER_PASS) {
            throw new IllegalArgumentException("artifact delete limit is out of bounds");
        }
        if (maintenanceIntervalMs < minimumInterval
                || maintenanceIntervalMs > MAX_INTERVAL_MS) {
            throw new IllegalArgumentException("artifact maintenance interval is out of bounds");
        }
        this.maxEntriesPerShard = maxEntriesPerShard;
        this.maxBytesPerShard = maxBytesPerShard;
        this.orphanGraceMs = orphanGraceMs;
        this.scanPageSize = scanPageSize;
        this.maxDeletesPerPass = maxDeletesPerPass;
        this.maintenanceIntervalMs = maintenanceIntervalMs;
    }

    public static ArtifactRetentionPolicy fromProperties(Properties properties) {
        if (properties == null) throw new IllegalArgumentException("properties is required");
        return new ArtifactRetentionPolicy(
                intProperty(properties,
                        "rowweave.master.artifactRetention.maxEntriesPerShard",
                        DEFAULT_MAX_ENTRIES_PER_SHARD),
                longProperty(properties,
                        "rowweave.master.artifactRetention.maxBytesPerShard",
                        DEFAULT_MAX_BYTES_PER_SHARD),
                longProperty(properties,
                        "rowweave.master.artifactRetention.orphanGraceMs",
                        DEFAULT_ORPHAN_GRACE_MS),
                intProperty(properties,
                        "rowweave.master.artifactRetention.scanPageSize", 128),
                intProperty(properties,
                        "rowweave.master.artifactRetention.maxDeletesPerPass", 16),
                longProperty(properties,
                        "rowweave.master.artifactRetention.maintenanceIntervalMs", 60_000L),
                true);
    }

    static ArtifactRetentionPolicy testing(int maxEntriesPerShard,
            long maxBytesPerShard, long orphanGraceMs, int scanPageSize,
            int maxDeletesPerPass, long maintenanceIntervalMs) {
        return new ArtifactRetentionPolicy(maxEntriesPerShard, maxBytesPerShard,
                orphanGraceMs, scanPageSize, maxDeletesPerPass,
                maintenanceIntervalMs, false);
    }

    public int maxEntriesPerShard() { return maxEntriesPerShard; }
    public long maxBytesPerShard() { return maxBytesPerShard; }
    public long orphanGraceMs() { return orphanGraceMs; }
    public int scanPageSize() { return scanPageSize; }
    public int maxDeletesPerPass() { return maxDeletesPerPass; }
    public long maintenanceIntervalMs() { return maintenanceIntervalMs; }

    public boolean due(long nowMillis, long createdAtMillis) {
        if (nowMillis < 0L) throw new IllegalArgumentException("nowMillis must be >= 0");
        if (createdAtMillis <= 0L) {
            throw new IllegalArgumentException("createdAtMillis must be > 0");
        }
        return nowMillis >= createdAtMillis
                && nowMillis - createdAtMillis >= orphanGraceMs;
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
