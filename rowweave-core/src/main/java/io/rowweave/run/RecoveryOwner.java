package io.rowweave.run;

import java.util.Objects;

/** The stable Master identity behind one recovery takeover request. */
public final class RecoveryOwner {
    private final String ownerId;
    private final long leaderTermToken;

    public RecoveryOwner(String ownerId, long leaderTermToken) {
        this.ownerId = RunText.required(ownerId, "ownerId", RunText.MAX_ID_BYTES);
        if (leaderTermToken == 0L) {
            throw new IllegalArgumentException("leaderTermToken must be non-zero");
        }
        this.leaderTermToken = leaderTermToken;
    }

    public String ownerId() { return ownerId; }
    public long leaderTermToken() { return leaderTermToken; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof RecoveryOwner)) return false;
        RecoveryOwner that = (RecoveryOwner) other;
        return leaderTermToken == that.leaderTermToken && ownerId.equals(that.ownerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ownerId, leaderTermToken);
    }
}
