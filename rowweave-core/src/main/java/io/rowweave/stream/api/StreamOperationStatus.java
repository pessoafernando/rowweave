package io.rowweave.stream.api;

/** Bounded durable operation status with stable wire identifiers. */
public enum StreamOperationStatus {
    ACCEPTED(1), RUNNING(2), SUCCEEDED(3), FAILED(4);

    private final int wireId;

    StreamOperationStatus(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() { return wireId; }

    public static StreamOperationStatus fromWireId(int wireId) {
        for (StreamOperationStatus status : values()) {
            if (status.wireId == wireId) return status;
        }
        throw new IllegalArgumentException(
                "unknown Stream operation status wire ID: " + wireId);
    }
}
