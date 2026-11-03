package io.rowweave.run;

/** Closed result of one terminal retention ownership compare-and-swap. */
public final class RunRetentionClaim {
    public enum Code { ACQUIRED, REPLAYED, NOT_DUE, STALE_RUN, LOST_RACE }

    private final Code code;
    private final RetentionToken token;

    private RunRetentionClaim(Code code, RetentionToken token) {
        if (code == null) throw new IllegalArgumentException("code is required");
        boolean hasToken = code == Code.ACQUIRED || code == Code.REPLAYED;
        if (hasToken != (token != null)) {
            throw new IllegalArgumentException("claim token does not match code");
        }
        this.code = code;
        this.token = token;
    }

    public static RunRetentionClaim acquired(RetentionToken token) {
        return new RunRetentionClaim(Code.ACQUIRED, token);
    }
    public static RunRetentionClaim replayed(RetentionToken token) {
        return new RunRetentionClaim(Code.REPLAYED, token);
    }
    public static RunRetentionClaim notDue() {
        return new RunRetentionClaim(Code.NOT_DUE, null);
    }
    public static RunRetentionClaim staleRun() {
        return new RunRetentionClaim(Code.STALE_RUN, null);
    }
    public static RunRetentionClaim lostRace() {
        return new RunRetentionClaim(Code.LOST_RACE, null);
    }

    public Code code() { return code; }
    public RetentionToken token() { return token; }
}
