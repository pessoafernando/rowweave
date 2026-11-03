package io.rowweave.incremental;

/** Closed set of durable frontier authority mutations. */
public enum IncrementalAuthorityCommandKind {
    INITIALIZE(1),
    CLAIM(2),
    COMMIT(3),
    COMPLETE_BACKFILL(4),
    CLOSE_ABORTED_RUN(5),
    MARK_CONFLICT(6);

    private final int wireId;

    IncrementalAuthorityCommandKind(int wireId) {
        this.wireId = wireId;
    }

    int wireId() { return wireId; }

    static IncrementalAuthorityCommandKind fromWire(int wireId) {
        for (IncrementalAuthorityCommandKind kind : values()) {
            if (kind.wireId == wireId) return kind;
        }
        throw new IllegalArgumentException("unknown frontier authority command kind");
    }
}
