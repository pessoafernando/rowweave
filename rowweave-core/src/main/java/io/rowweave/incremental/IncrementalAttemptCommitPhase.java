package io.rowweave.incremental;

/** Monotonic commit phase for the current execution attempt. */
public enum IncrementalAttemptCommitPhase {
    CLAIMED(1),
    PREPARED(2),
    COMMIT_DECIDED(3);

    private final int wireId;

    IncrementalAttemptCommitPhase(int wireId) {
        this.wireId = wireId;
    }

    int wireId() { return wireId; }

    static IncrementalAttemptCommitPhase fromWire(int wireId) {
        for (IncrementalAttemptCommitPhase phase : values()) {
            if (phase.wireId == wireId) return phase;
        }
        throw new IllegalArgumentException("unknown incremental attempt commit phase");
    }
}
