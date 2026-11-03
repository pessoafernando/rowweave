package io.rowweave.sched.net;

import java.util.Properties;

/** Immutable resource admission configuration for the mux data port of a worker. */
public final class ShuffleIngressAdmissionConfig {

    static final String MAX_CONNECTIONS = "rowweave.shuffle.maxConnections";
    static final String MAX_HANDLERS = "rowweave.shuffle.maxIngressHandlers";
    static final String MAX_TARGETS = "rowweave.shuffle.maxTargetsPerConnection";
    static final String MAX_PENDING_FRAMES = "rowweave.shuffle.maxPendingFramesPerTarget";
    static final String RESIDENT_BYTES = "rowweave.shuffle.ingressResidentBytes";
    static final String IDLE_TIMEOUT_MS = "rowweave.shuffle.idleReadTimeoutMs";

    private static final int DEFAULT_MAX_CONNECTIONS = 256;
    private static final int DEFAULT_MAX_HANDLERS = 256;
    private static final int DEFAULT_MAX_TARGETS = 4096;
    /* The 1 MiB default stage credit divided by the smallest 21-byte wire frame stays under 65536, so a well-formed sender must never reach the frame cap before its credit runs out. */
    private static final int DEFAULT_MAX_PENDING_FRAMES = 65_536;
    private static final long DEFAULT_RESIDENT_BYTES = 256L * 1024 * 1024;
    private static final long DEFAULT_IDLE_TIMEOUT_MS = 60_000L;

    private final int maxConnections;
    private final int maxIngressHandlers;
    private final int maxTargetsPerConnection;
    private final int maxPendingFramesPerTarget;
    private final long ingressResidentBytes;
    private final long idleReadTimeoutMillis;

    private ShuffleIngressAdmissionConfig(int maxConnections, int maxIngressHandlers,
                                          int maxTargetsPerConnection,
                                          int maxPendingFramesPerTarget,
                                          long ingressResidentBytes,
                                          long idleReadTimeoutMillis,
                                          long maxHeapBytes,
                                          boolean productionBounds) {
        requireRange(MAX_CONNECTIONS, maxConnections, 1, 4096);
        requireRange(MAX_HANDLERS, maxIngressHandlers, 1, 4096);
        requireRange(MAX_TARGETS, maxTargetsPerConnection, 1, 100_000);
        requireRange(MAX_PENDING_FRAMES, maxPendingFramesPerTarget, 1, 65_536);
        long minimumResident = productionBounds
                ? ShuffleIngressResidentBudget.minimumSingleFrameBytes() : 1L;
        requireRange(RESIDENT_BYTES, ingressResidentBytes, minimumResident,
                productionBounds ? maxHeapBytes / 2L : Long.MAX_VALUE);
        requireRange(IDLE_TIMEOUT_MS, idleReadTimeoutMillis,
                productionBounds ? 1_000L : 1L, 300_000L);
        this.maxConnections = maxConnections;
        this.maxIngressHandlers = maxIngressHandlers;
        this.maxTargetsPerConnection = maxTargetsPerConnection;
        this.maxPendingFramesPerTarget = maxPendingFramesPerTarget;
        this.ingressResidentBytes = ingressResidentBytes;
        this.idleReadTimeoutMillis = idleReadTimeoutMillis;
    }

    public static ShuffleIngressAdmissionConfig fromSystemProperties() {
        return fromProperties(System.getProperties(), Runtime.getRuntime().maxMemory());
    }

    public static ShuffleIngressAdmissionConfig fromProperties(Properties properties,
                                                                long maxHeapBytes) {
        if (properties == null) {
            throw new IllegalArgumentException("shuffle ingress properties must not be null");
        }
        if (maxHeapBytes <= 0L) {
            throw new IllegalArgumentException("max heap must be positive");
        }
        return new ShuffleIngressAdmissionConfig(
                intProperty(properties, MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS),
                intProperty(properties, MAX_HANDLERS, DEFAULT_MAX_HANDLERS),
                intProperty(properties, MAX_TARGETS, DEFAULT_MAX_TARGETS),
                intProperty(properties, MAX_PENDING_FRAMES, DEFAULT_MAX_PENDING_FRAMES),
                longProperty(properties, RESIDENT_BYTES, DEFAULT_RESIDENT_BYTES),
                longProperty(properties, IDLE_TIMEOUT_MS, DEFAULT_IDLE_TIMEOUT_MS),
                maxHeapBytes, true);
    }

    /** aggregate parser: keep production minimums; unified heap ledger owns the ceiling. */
    public static ShuffleIngressAdmissionConfig fromProcessProperties(
            Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException(
                    "shuffle ingress properties must not be null");
        }
        return new ShuffleIngressAdmissionConfig(
                intProperty(properties, MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS),
                intProperty(properties, MAX_HANDLERS, DEFAULT_MAX_HANDLERS),
                intProperty(properties, MAX_TARGETS, DEFAULT_MAX_TARGETS),
                intProperty(properties, MAX_PENDING_FRAMES, DEFAULT_MAX_PENDING_FRAMES),
                longProperty(properties, RESIDENT_BYTES, DEFAULT_RESIDENT_BYTES),
                longProperty(properties, IDLE_TIMEOUT_MS, DEFAULT_IDLE_TIMEOUT_MS),
                Long.MAX_VALUE, true);
    }

    /** all established V1 connections plus one maximum reserved frame. */
    public static long minimumMtlsV1ResidentBytes(int maxConnections) {
        requireRange(MAX_CONNECTIONS, maxConnections, 1, 4096);
        long connectionScratch = Math.multiplyExact(
                (long) maxConnections,
                ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES);
        long maximumFrame = ShuffleIngressResidentBudget.accountedFrameBytes(
                FrameCodec.V1_HEADER_BYTES + (long) FrameCodec.MAX_FRAME_BYTES);
        return Math.addExact(connectionScratch, maximumFrame);
    }

    static ShuffleIngressAdmissionConfig forTesting(
            int maxConnections, int maxIngressHandlers, int maxTargetsPerConnection,
            int maxPendingFramesPerTarget, long ingressResidentBytes,
            long idleReadTimeoutMillis) {
        return new ShuffleIngressAdmissionConfig(maxConnections, maxIngressHandlers,
                maxTargetsPerConnection, maxPendingFramesPerTarget, ingressResidentBytes,
                idleReadTimeoutMillis, Long.MAX_VALUE, false);
    }

    public int maxConnections() { return maxConnections; }
    public int maxIngressHandlers() { return maxIngressHandlers; }
    public int maxTargetsPerConnection() { return maxTargetsPerConnection; }
    public int maxPendingFramesPerTarget() { return maxPendingFramesPerTarget; }
    public long ingressResidentBytes() { return ingressResidentBytes; }
    public long idleReadTimeoutMillis() { return idleReadTimeoutMillis; }

    private static int intProperty(Properties properties, String key, int defaultValue) {
        String configured = properties.getProperty(key);
        if (configured == null || configured.trim().isEmpty()) return defaultValue;
        try {
            return Integer.parseInt(configured.trim());
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " must be an integer", invalid);
        }
    }

    private static long longProperty(Properties properties, String key, long defaultValue) {
        String configured = properties.getProperty(key);
        if (configured == null || configured.trim().isEmpty()) return defaultValue;
        try {
            return Long.parseLong(configured.trim());
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " must be an integer", invalid);
        }
    }

    private static void requireRange(String key, long value, long minimum, long maximum) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(key + " must be in " + minimum + ".."
                    + maximum + ", got " + value);
        }
    }
}
