package io.rowweave.cluster.rpc;

import java.util.Properties;

/** Immutable admission configuration for RPC connections, deadlines and frames. */
public final class RpcAdmissionConfig {
    public static final int NORMAL_FRAME_BYTES = 1024 * 1024;
    public static final int LARGE_FRAME_BYTES = 80 * 1024 * 1024;

    private final int maxConnections;
    private final int listenBacklog;
    private final long frameDeadlineMillis;
    private final int normalFrameBytes;
    private final int largeFrameBytes;
    private final long inFlightFrameBytes;

    private RpcAdmissionConfig(int maxConnections, int listenBacklog, long frameDeadlineMillis,
                               int normalFrameBytes, int largeFrameBytes, long inFlightFrameBytes,
                               boolean productionBounds) {
        requireRange("maxConnections", maxConnections, 1, 4096);
        requireRange("listenBacklog", listenBacklog, 1, 4096);
        long minimumDeadline = productionBounds ? 1_000L : 1L;
        requireRange("frameDeadlineMs", frameDeadlineMillis, minimumDeadline, 300_000L);
        if (normalFrameBytes < Integer.BYTES || largeFrameBytes < normalFrameBytes) {
            throw new IllegalArgumentException("invalid RPC frame size limits");
        }
        if (inFlightFrameBytes < largeFrameBytes
                || inFlightFrameBytes > 1024L * 1024 * 1024) {
            throw new IllegalArgumentException("inFlightFrameBytes must be >= largeFrameBytes and <= 1 GiB");
        }
        this.maxConnections = maxConnections;
        this.listenBacklog = listenBacklog;
        this.frameDeadlineMillis = frameDeadlineMillis;
        this.normalFrameBytes = normalFrameBytes;
        this.largeFrameBytes = largeFrameBytes;
        this.inFlightFrameBytes = inFlightFrameBytes;
    }

    public static RpcAdmissionConfig fromProperties(Properties properties) {
        if (properties == null) throw new IllegalArgumentException("RPC admission properties must not be null");
        return new RpcAdmissionConfig(
                intProperty(properties, "rowweave.control.maxConnections", 256),
                intProperty(properties, "rowweave.control.listenBacklog", 128),
                longProperty(properties, "rowweave.control.frameDeadlineMs", 30_000L),
                NORMAL_FRAME_BYTES, LARGE_FRAME_BYTES,
                longProperty(properties, "rowweave.control.inFlightFrameBytes", 128L * 1024 * 1024), true);
    }

    public static RpcAdmissionConfig fromSystemProperties() {
        return fromProperties(System.getProperties());
    }

    static RpcAdmissionConfig forTesting(int maxConnections, int backlog, long deadlineMillis,
                                         int normalBytes, int largeBytes, long inFlightBytes) {
        return new RpcAdmissionConfig(maxConnections, backlog, deadlineMillis,
                normalBytes, largeBytes, inFlightBytes, false);
    }

    public int maxConnections() { return maxConnections; }
    public int listenBacklog() { return listenBacklog; }
    public long frameDeadlineMillis() { return frameDeadlineMillis; }
    public int normalFrameBytes() { return normalFrameBytes; }
    public int largeFrameBytes() { return largeFrameBytes; }
    public long inFlightFrameBytes() { return inFlightFrameBytes; }

    int frameLimit(RpcMessage.Type type) {
        switch (type) {
            case SUBMIT_JOB:
            case SUBMIT_PLAN_ARTIFACT:
            case SUBMIT_RUN:
            case SUBMIT_INCREMENTAL_RUN:
            case ASSIGN_STAGE:
            case RUN_SNAPSHOT:
            case EXECUTE_PROVIDER_GROUP:
            case EXECUTE_INCREMENTAL_SOURCE_ACTION:
            case INCREMENTAL_SOURCE_ACTION_RESULT:
                return largeFrameBytes;
            default:
                return normalFrameBytes;
        }
    }

    private static int intProperty(Properties p, String key, int defaultValue) {
        String value = p.getProperty(key);
        return value == null || value.trim().isEmpty() ? defaultValue : Integer.parseInt(value.trim());
    }

    private static long longProperty(Properties p, String key, long defaultValue) {
        String value = p.getProperty(key);
        return value == null || value.trim().isEmpty() ? defaultValue : Long.parseLong(value.trim());
    }

    private static void requireRange(String name, long value, long min, long max) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(name + " must be in " + min + ".." + max + ", got " + value);
        }
    }
}
