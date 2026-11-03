package io.rowweave.artifact;

/** One Artifact collect decision taken under a fenced publication guard. */
public final class ArtifactCollectResult {
    public enum Code {
        DELETED,
        ALREADY_DELETED,
        RETAINED_REFERENCED,
        STALE_CANDIDATE,
        STALE_OWNER
    }

    private static final ArtifactCollectResult[] VALUES;

    static {
        Code[] codes = Code.values();
        VALUES = new ArtifactCollectResult[codes.length];
        for (Code code : codes) VALUES[code.ordinal()] = new ArtifactCollectResult(code);
    }

    private final Code code;

    private ArtifactCollectResult(Code code) { this.code = code; }

    public static ArtifactCollectResult of(Code code) {
        if (code == null) throw new IllegalArgumentException("code is required");
        return VALUES[code.ordinal()];
    }

    public Code code() { return code; }
}
