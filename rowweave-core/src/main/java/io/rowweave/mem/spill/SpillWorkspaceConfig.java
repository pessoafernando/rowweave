package io.rowweave.mem.spill;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

/** immutable, strictly parsed spill workspace policy. */
public final class SpillWorkspaceConfig {

    private static final String PREFIX = "rowweave.worker.spill.";
    private static final long MIB = 1024L * 1024;
    private static final long GIB = 1024L * MIB;
    private static final long TIB = 1024L * GIB;

    public static final long DEFAULT_WORKER_MAX_BYTES = 64L * GIB;
    public static final long DEFAULT_JOB_MAX_BYTES = 16L * GIB;
    public static final long DEFAULT_MIN_FREE_BYTES = GIB;
    public static final long DEFAULT_ORPHAN_GRACE_MILLIS = 60L * 60 * 1000;
    public static final int DEFAULT_SCAN_MAX_ENTRIES = 4096;
    public static final int DEFAULT_DELETE_MAX_ENTRIES = 1024;
    public static final long DEFAULT_MAINTENANCE_INTERVAL_MILLIS = 60_000L;

    private static final long MAX_BYTES = 16L * TIB;
    private static final int MAX_ENTRIES = 100_000;
    private static final long MAX_ORPHAN_GRACE_MILLIS = 30L * 24 * 60 * 60 * 1000;
    private static final long MAX_MAINTENANCE_INTERVAL_MILLIS = 60L * 60 * 1000;

    private final Path baseDirectory;
    private final long workerMaxBytes;
    private final long jobMaxBytes;
    private final long minFreeBytes;
    private final long orphanGraceMillis;
    private final int scanMaxEntries;
    private final int deleteMaxEntries;
    private final long maintenanceIntervalMillis;

    private SpillWorkspaceConfig(Path baseDirectory,
                                 long workerMaxBytes, long jobMaxBytes, long minFreeBytes,
                                 long orphanGraceMillis, int scanMaxEntries, int deleteMaxEntries,
                                 long maintenanceIntervalMillis, boolean testPolicy) {
        this.baseDirectory = validateBase(baseDirectory);
        this.workerMaxBytes = inRange(PREFIX + "maxBytes", workerMaxBytes, MIB, MAX_BYTES);
        this.jobMaxBytes = inRange(PREFIX + "jobMaxBytes", jobMaxBytes, MIB, MAX_BYTES);
        if (jobMaxBytes > workerMaxBytes) {
            throw SpillDiskException.configInvalid(PREFIX + "jobMaxBytes", null);
        }
        this.minFreeBytes = inRange(PREFIX + "minFreeBytes", minFreeBytes, 0, MAX_BYTES);
        this.orphanGraceMillis = inRange(PREFIX + "orphanGraceMs", orphanGraceMillis,
                testPolicy ? 0 : 60_000L, MAX_ORPHAN_GRACE_MILLIS);
        this.scanMaxEntries = (int) inRange(PREFIX + "scanMaxEntries",
                scanMaxEntries, 4, MAX_ENTRIES);
        this.deleteMaxEntries = (int) inRange(PREFIX + "deleteMaxEntries",
                deleteMaxEntries, 3, this.scanMaxEntries);
        this.maintenanceIntervalMillis = inRange(PREFIX + "maintenanceIntervalMs",
                maintenanceIntervalMillis, testPolicy ? 0 : 1000L,
                MAX_MAINTENANCE_INTERVAL_MILLIS);
    }

    public static SpillWorkspaceConfig fromSystemProperties() {
        Path defaultBase = defaultBaseDirectory();
        Path base = parsePath(PREFIX + "dir", defaultBase);
        long worker = parseLong(PREFIX + "maxBytes", DEFAULT_WORKER_MAX_BYTES);
        long job = parseLong(PREFIX + "jobMaxBytes", DEFAULT_JOB_MAX_BYTES);
        long free = parseLong(PREFIX + "minFreeBytes", DEFAULT_MIN_FREE_BYTES);
        long grace = parseLong(PREFIX + "orphanGraceMs", DEFAULT_ORPHAN_GRACE_MILLIS);
        long scan = parseLong(PREFIX + "scanMaxEntries", DEFAULT_SCAN_MAX_ENTRIES);
        long delete = parseLong(PREFIX + "deleteMaxEntries", DEFAULT_DELETE_MAX_ENTRIES);
        long interval = parseLong(PREFIX + "maintenanceIntervalMs",
                DEFAULT_MAINTENANCE_INTERVAL_MILLIS);
        return new SpillWorkspaceConfig(base, worker, job, free, grace,
                checkedInt(PREFIX + "scanMaxEntries", scan),
                checkedInt(PREFIX + "deleteMaxEntries", delete), interval, false);
    }

    private static Path defaultBaseDirectory() {
        String temporaryDirectory = System.getProperty("java.io.tmpdir");
        if (temporaryDirectory == null || temporaryDirectory.trim().isEmpty()) {
            throw SpillDiskException.configInvalid(PREFIX + "dir", null);
        }
        try {
            // macOS commonly exposes /var as a symlink. Resolve the trusted platform temp root once,
            // then append our missing dedicated child so runtime path checks can reject all symlinks.
            return Paths.get(temporaryDirectory).toRealPath().resolve("rowweave-spill-v1").normalize();
        } catch (Exception e) {
            throw SpillDiskException.configInvalid(PREFIX + "dir", e);
        }
    }

    static SpillWorkspaceConfig forTests(
            Path baseDirectory, long workerMaxBytes, long jobMaxBytes, long minFreeBytes,
            long orphanGraceMillis, int scanMaxEntries, int deleteMaxEntries,
            long maintenanceIntervalMillis) {
        return new SpillWorkspaceConfig(baseDirectory, workerMaxBytes, jobMaxBytes,
                minFreeBytes, orphanGraceMillis, scanMaxEntries, deleteMaxEntries,
                maintenanceIntervalMillis, true);
    }

    public Path baseDirectory() {
        return baseDirectory;
    }

    public long workerMaxBytes() {
        return workerMaxBytes;
    }

    public long jobMaxBytes() {
        return jobMaxBytes;
    }

    public long minFreeBytes() {
        return minFreeBytes;
    }

    public long orphanGraceMillis() {
        return orphanGraceMillis;
    }

    public int scanMaxEntries() {
        return scanMaxEntries;
    }

    public int deleteMaxEntries() {
        return deleteMaxEntries;
    }

    public long maintenanceIntervalMillis() {
        return maintenanceIntervalMillis;
    }

    private static Path parsePath(String key, Path fallback) {
        String raw = System.getProperty(key);
        if (raw == null) {
            return fallback;
        }
        String value = raw.trim();
        if (value.isEmpty()) {
            throw SpillDiskException.configInvalid(key, null);
        }
        try {
            return validateBase(Paths.get(value));
        } catch (InvalidPathException e) {
            throw SpillDiskException.configInvalid(key, e);
        }
    }

    private static Path validateBase(Path path) {
        if (path == null || !path.isAbsolute()) {
            throw SpillDiskException.configInvalid(PREFIX + "dir", null);
        }
        return path.normalize();
    }

    private static long parseLong(String key, long fallback) {
        String raw = System.getProperty(key);
        if (raw == null) {
            return fallback;
        }
        String value = raw.trim();
        if (value.isEmpty() || !value.matches("[0-9]+")) {
            throw SpillDiskException.configInvalid(key, null);
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw SpillDiskException.configInvalid(key, e);
        }
    }

    private static int checkedInt(String key, long value) {
        if (value > Integer.MAX_VALUE) {
            throw SpillDiskException.configInvalid(key, null);
        }
        return (int) value;
    }

    private static long inRange(String key, long value, long min, long max) {
        if (value < min || value > max) {
            throw SpillDiskException.configInvalid(key, null);
        }
        return value;
    }
}
