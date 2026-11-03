package io.rowweave.run;

/** Closed result of publishing one immutable physical assignment manifest. */
public final class StageManifestPublication {
    public enum Code { PUBLISHED, ALREADY_PUBLISHED, CONFLICT, STALE_RUN, RECOVERY_OWNED }

    private final Code code;
    private final StageAssignmentSnapshot snapshot;

    private StageManifestPublication(Code code, StageAssignmentSnapshot snapshot) {
        if (code == null) throw new IllegalArgumentException("code is required");
        boolean hasSnapshot = code == Code.PUBLISHED || code == Code.ALREADY_PUBLISHED;
        if (hasSnapshot != (snapshot != null)) {
            throw new IllegalArgumentException("snapshot does not match publication code");
        }
        this.code = code;
        this.snapshot = snapshot;
    }

    public static StageManifestPublication published(StageAssignmentSnapshot snapshot) {
        return new StageManifestPublication(Code.PUBLISHED, snapshot);
    }

    public static StageManifestPublication replayed(StageAssignmentSnapshot snapshot) {
        return new StageManifestPublication(Code.ALREADY_PUBLISHED, snapshot);
    }

    public static StageManifestPublication rejected(Code code) {
        if (code == Code.PUBLISHED || code == Code.ALREADY_PUBLISHED) {
            throw new IllegalArgumentException("published code requires snapshot");
        }
        return new StageManifestPublication(code, null);
    }

    public Code code() { return code; }
    public StageAssignmentSnapshot snapshot() { return snapshot; }
}
