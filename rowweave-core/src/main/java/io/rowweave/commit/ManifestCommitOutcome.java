package io.rowweave.commit;

/** Manifest publication outcome queried by a stable attemptId. */
public final class ManifestCommitOutcome {

    public enum State { NOT_FOUND, PREPARING, COMMITTED, CORRUPT_OR_CONFLICT }

    private final State state;
    private final Long generation;
    private final byte[] manifestHash;
    private final String detail;

    ManifestCommitOutcome(State state, Long generation, byte[] manifestHash, String detail) {
        this.state = state;
        this.generation = generation;
        this.manifestHash = manifestHash == null ? null : manifestHash.clone();
        this.detail = detail;
    }

    public State state() { return state; }
    public Long generation() { return generation; }
    public byte[] manifestHash() { return manifestHash == null ? null : manifestHash.clone(); }
    public String detail() { return detail; }

    public static ManifestCommitOutcome of(
            State state, Long generation, byte[] manifestHash, String detail) {
        if (state == null) {
            throw new IllegalArgumentException("manifest outcome state must not be null");
        }
        return new ManifestCommitOutcome(state, generation, manifestHash, detail);
    }

    static ManifestCommitOutcome notFound() {
        return new ManifestCommitOutcome(State.NOT_FOUND, null, null, null);
    }

    static ManifestCommitOutcome preparing(long generation) {
        return new ManifestCommitOutcome(State.PREPARING, generation, null, null);
    }

    static ManifestCommitOutcome committed(long generation, byte[] manifestHash) {
        return new ManifestCommitOutcome(State.COMMITTED, generation, manifestHash, null);
    }

    static ManifestCommitOutcome conflict(long generation, String detail) {
        return new ManifestCommitOutcome(State.CORRUPT_OR_CONFLICT, generation, null, detail);
    }
}
