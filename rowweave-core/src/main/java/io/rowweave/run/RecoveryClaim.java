package io.rowweave.run;

/** Closed result set of one recovery ownership compare-and-swap. */
public final class RecoveryClaim {
    public enum Code { ACQUIRED, REPLAYED, STALE_RUN, LOST_RACE }

    private final Code code;
    private final RecoveryToken token;

    private RecoveryClaim(Code code, RecoveryToken token) {
        if (code == null) throw new IllegalArgumentException("code is required");
        boolean hasToken = code == Code.ACQUIRED || code == Code.REPLAYED;
        if (hasToken != (token != null)) {
            throw new IllegalArgumentException("claim token does not match code");
        }
        this.code = code;
        this.token = token;
    }

    public static RecoveryClaim acquired(RecoveryToken token) {
        return new RecoveryClaim(Code.ACQUIRED, token);
    }

    public static RecoveryClaim replayed(RecoveryToken token) {
        return new RecoveryClaim(Code.REPLAYED, token);
    }

    public static RecoveryClaim staleRun() {
        return new RecoveryClaim(Code.STALE_RUN, null);
    }

    public static RecoveryClaim lostRace() {
        return new RecoveryClaim(Code.LOST_RACE, null);
    }

    public Code code() { return code; }
    public RecoveryToken token() { return token; }
}
