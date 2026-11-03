package io.rowweave.upgrade;

/** Durable upgrade phases. Wire ids are part of the production store contract. */
public enum UpgradePhase {
    IDLE(1),
    QUIESCED(2),
    COPYING(3),
    VERIFIED(4),
    CUTOVER(5),
    CLEANUP_ELIGIBLE(6),
    FAILED_FROZEN(7);

    private final int wireId;

    UpgradePhase(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    static UpgradePhase fromWireId(int wireId) {
        for (UpgradePhase phase : values()) {
            if (phase.wireId == wireId) return phase;
        }
        throw new IllegalArgumentException("unknown upgrade phase: " + wireId);
    }
}
