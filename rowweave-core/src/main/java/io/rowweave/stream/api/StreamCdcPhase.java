package io.rowweave.stream.api;

/** Bounded public projection of resident CDC progress. */
public enum StreamCdcPhase {
    NOT_APPLICABLE(0),
    SNAPSHOT(1),
    LOG(2);

    private final int wireId;

    StreamCdcPhase(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }
}
