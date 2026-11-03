package io.rowweave.incremental;

/** Durable state of one bounded incremental feed frontier. */
public enum IncrementalFrontierAuthorityState {
    READY(1),
    ACTIVE(2),
    CORRUPT_OR_CONFLICT(3);

    private final int wireId;

    IncrementalFrontierAuthorityState(int wireId) {
        this.wireId = wireId;
    }

    int wireId() { return wireId; }

    static IncrementalFrontierAuthorityState fromWire(int wireId) {
        for (IncrementalFrontierAuthorityState state : values()) {
            if (state.wireId == wireId) return state;
        }
        throw new IllegalArgumentException("unknown frontier authority state");
    }
}
