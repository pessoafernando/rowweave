package io.rowweave.incremental;

/** Result of acquiring the current in-memory feed authority ownership fence. */
public final class FrontierOwnershipClaim {
    public enum Code { ACQUIRED, REPLAYED, FEED_NOT_FOUND, STALE_REVISION, LOST_RACE }

    private final Code code;
    private final FrontierRecoveryToken token;

    private FrontierOwnershipClaim(Code code, FrontierRecoveryToken token) {
        if (code == null) throw new IllegalArgumentException("ownership code is required");
        boolean owns = code == Code.ACQUIRED || code == Code.REPLAYED;
        if (owns != (token != null)) {
            throw new IllegalArgumentException("ownership token does not match decision");
        }
        this.code = code;
        this.token = token;
    }

    public static FrontierOwnershipClaim acquired(FrontierRecoveryToken token) {
        return new FrontierOwnershipClaim(Code.ACQUIRED, token);
    }

    public static FrontierOwnershipClaim replayed(FrontierRecoveryToken token) {
        return new FrontierOwnershipClaim(Code.REPLAYED, token);
    }

    public static FrontierOwnershipClaim rejected(Code code) {
        return new FrontierOwnershipClaim(code, null);
    }

    public Code code() { return code; }
    public FrontierRecoveryToken token() { return token; }
}
