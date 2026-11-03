package io.rowweave.stream.api;

/** Checkpoint barrier handling selected for one Stream job. */
public enum StreamCheckpointMode {
    ALIGNED(1),
    UNALIGNED(2);

    private final int wireId;

    StreamCheckpointMode(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static StreamCheckpointMode fromWireId(int wireId) {
        for (StreamCheckpointMode mode : values()) {
            if (mode.wireId == wireId) return mode;
        }
        throw new IllegalArgumentException("unknown Stream checkpoint mode");
    }
}
