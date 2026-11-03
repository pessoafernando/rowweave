package io.rowweave.run;

/** Frozen output side-effect model for one physical assignment manifest. */
public enum AttemptOutputProfile {
    UNKNOWN_LEGACY(0),
    DISCARD(1),
    MANIFEST_ATOMIC_GROUP_V1(2);

    private final int wireId;

    AttemptOutputProfile(int wireId) {
        this.wireId = wireId;
    }

    int wireId() {
        return wireId;
    }

    static AttemptOutputProfile fromWireId(int wireId) {
        for (AttemptOutputProfile profile : values()) {
            if (profile.wireId == wireId) return profile;
        }
        throw new IllegalArgumentException(
                "unknown attempt output profile: " + wireId);
    }
}
