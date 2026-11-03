package io.rowweave;

import java.io.Serializable;

/** The resident rows + bytes double fence of the compatibility API, which configuration cannot disable. */
public final class CompatibilityDataLimits implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_MAX_ROWS = 100_000;
    public static final long DEFAULT_MAX_BYTES = 64L * 1024L * 1024L;
    public static final int HARD_MAX_ROWS = 1_000_000;
    public static final long HARD_MAX_BYTES = 512L * 1024L * 1024L;

    private final int maxRows;
    private final long maxBytes;

    private CompatibilityDataLimits(int maxRows, long maxBytes) {
        if (maxRows < 1 || maxRows > HARD_MAX_ROWS) {
            throw new IllegalArgumentException(
                    "compatibility maxRows must be in [1," + HARD_MAX_ROWS + "]");
        }
        if (maxBytes < 1L || maxBytes > HARD_MAX_BYTES) {
            throw new IllegalArgumentException(
                    "compatibility maxBytes must be in [1," + HARD_MAX_BYTES + "]");
        }
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
    }

    public static CompatibilityDataLimits defaults() {
        return new CompatibilityDataLimits(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES);
    }

    public static CompatibilityDataLimits of(int maxRows, long maxBytes) {
        return new CompatibilityDataLimits(maxRows, maxBytes);
    }

    public int maxRows() { return maxRows; }
    public long maxBytes() { return maxBytes; }

    /** Validates and returns the resident bytes after admitting the current row; the caller must not store that row before this returns. */
    public long requireAdditional(
            int currentRows, long currentBytes, long additionalBytes,
            String owner) {
        requireOwner(owner);
        if (currentRows < 0 || currentBytes < 0L || additionalBytes < 0L) {
            throw new IllegalArgumentException(
                    "compatibility usage must be non-negative");
        }
        long nextRows = (long) currentRows + 1L;
        if (nextRows > maxRows) {
            throw new CompatibilityDataLimitException(
                    CompatibilityDataLimitException.Kind.ROWS,
                    maxRows, nextRows, owner);
        }
        long nextBytes;
        try {
            nextBytes = Math.addExact(currentBytes, additionalBytes);
        } catch (ArithmeticException overflow) {
            throw new CompatibilityDataLimitException(
                    CompatibilityDataLimitException.Kind.BYTES,
                    maxBytes, Long.MAX_VALUE, owner);
        }
        if (nextBytes > maxBytes) {
            throw new CompatibilityDataLimitException(
                    CompatibilityDataLimitException.Kind.BYTES,
                    maxBytes, nextBytes, owner);
        }
        return nextBytes;
    }

    static void requireOwner(String owner) {
        if (owner == null || owner.isEmpty() || owner.length() > 128) {
            throw new IllegalArgumentException("compatibility owner is invalid");
        }
    }
}
