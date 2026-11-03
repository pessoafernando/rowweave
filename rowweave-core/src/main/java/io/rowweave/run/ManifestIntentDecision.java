package io.rowweave.run;

/** Closed result set for the irreversible built-in Manifest commit decision. */
public final class ManifestIntentDecision {
    public enum Code {
        COMMITTED,
        ALREADY_COMMITTED,
        STALE_RUN,
        ATTEMPT_NOT_COMMITTING,
        INCOMPLETE_MEMBERS,
        INTENT_CONFLICT,
        RECOVERY_OWNED,
        CORRUPT,
        UNAVAILABLE
    }

    private final Code code;
    private final ManifestCommitIntent intent;

    private ManifestIntentDecision(Code code, ManifestCommitIntent intent) {
        if (code == null) throw new IllegalArgumentException("code is required");
        boolean hasIntent = code == Code.COMMITTED || code == Code.ALREADY_COMMITTED;
        if (hasIntent != (intent != null)) {
            throw new IllegalArgumentException("intent does not match decision code");
        }
        this.code = code;
        this.intent = intent;
    }

    public static ManifestIntentDecision committed(ManifestCommitIntent intent) {
        return new ManifestIntentDecision(Code.COMMITTED, intent);
    }

    public static ManifestIntentDecision alreadyCommitted(ManifestCommitIntent intent) {
        return new ManifestIntentDecision(Code.ALREADY_COMMITTED, intent);
    }

    public static ManifestIntentDecision rejected(Code code) {
        if (code == Code.COMMITTED || code == Code.ALREADY_COMMITTED) {
            throw new IllegalArgumentException("committed code requires intent");
        }
        return new ManifestIntentDecision(code, null);
    }

    public Code code() { return code; }
    public ManifestCommitIntent intent() { return intent; }
}
