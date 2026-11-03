package io.rowweave.cluster.rpc;

import java.util.Properties;

/** Process-wide resident budget shared by control plane decoding and materialization. */
public final class ControlResidentConfig {
    static final String RESIDENT_BYTES_PROPERTY = "rowweave.control.residentBytes";
    static final long DEFAULT_RESIDENT_BYTES = 256L * 1024 * 1024;
    static final long MINIMUM_RESIDENT_BYTES = 2L * 80 * 1024 * 1024 + 64L * 1024 * 1024;

    private final long residentBytes;

    private ControlResidentConfig(long residentBytes) {
        this.residentBytes = residentBytes;
    }

    public static ControlResidentConfig fromProperties(Properties properties, long maxHeapBytes) {
        ControlResidentConfig parsed = fromProcessProperties(properties);
        if (maxHeapBytes <= 0L) {
            throw new IllegalArgumentException("max heap must be positive");
        }
        if (parsed.residentBytes > maxHeapBytes / 2L) {
            throw new IllegalArgumentException(RESIDENT_BYTES_PROPERTY
                    + " must be <= half of JVM max heap, got " + parsed.residentBytes);
        }
        return parsed;
    }

    /** aggregate-process parser; the unified heap ledger owns the upper bound. */
    public static ControlResidentConfig fromProcessProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("control resident properties must not be null");
        }
        String configured = properties.getProperty(RESIDENT_BYTES_PROPERTY);
        long residentBytes = configured == null || configured.trim().isEmpty()
                ? DEFAULT_RESIDENT_BYTES : parseBytes(configured.trim());
        if (residentBytes < MINIMUM_RESIDENT_BYTES) {
            throw new IllegalArgumentException(RESIDENT_BYTES_PROPERTY + " must be >= "
                    + MINIMUM_RESIDENT_BYTES + ", got " + residentBytes);
        }
        return new ControlResidentConfig(residentBytes);
    }

    public static ControlResidentConfig fromSystemProperties() {
        return fromProperties(System.getProperties(), Runtime.getRuntime().maxMemory());
    }

    public long residentBytes() {
        return residentBytes;
    }

    private static long parseBytes(String value) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed <= 0L) throw new NumberFormatException("non-positive");
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(RESIDENT_BYTES_PROPERTY
                    + " must be a positive byte count", e);
        }
    }
}
