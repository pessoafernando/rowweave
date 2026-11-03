package io.rowweave.incremental;

/** Closed bounded-incremental run modes. */
public enum IncrementalRunMode {
    SNAPSHOT_V1(1),
    INCREMENTAL_V1(2),
    BACKFILL_V1(3);

    private final int wireId;

    IncrementalRunMode(int wireId) {
        this.wireId = wireId;
    }

    int wireId() {
        return wireId;
    }

    static IncrementalRunMode fromWire(int wireId) {
        for (IncrementalRunMode mode : values()) if (mode.wireId == wireId) return mode;
        throw new IllegalArgumentException("unknown incremental run mode");
    }
}
