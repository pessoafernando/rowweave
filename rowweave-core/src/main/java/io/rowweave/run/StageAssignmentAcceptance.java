package io.rowweave.run;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Durable proof that an exact Worker incarnation accepted an exact command blob. */
public final class StageAssignmentAcceptance {
    private final StageAssignmentToken token;
    private final byte[] commandDigest;
    private final long acceptedAtMillis;

    public StageAssignmentAcceptance(StageAssignmentToken token, byte[] commandDigest,
                                     long acceptedAtMillis) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (commandDigest == null || commandDigest.length != 32) {
            throw new IllegalArgumentException("commandDigest must be SHA-256");
        }
        if (acceptedAtMillis < 0L) {
            throw new IllegalArgumentException("acceptedAtMillis must be >= 0");
        }
        this.token = token;
        this.commandDigest = commandDigest.clone();
        this.acceptedAtMillis = acceptedAtMillis;
    }

    public StageAssignmentToken token() { return token; }
    public byte[] commandDigest() { return commandDigest.clone(); }
    public long acceptedAtMillis() { return acceptedAtMillis; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof StageAssignmentAcceptance)) return false;
        StageAssignmentAcceptance that = (StageAssignmentAcceptance) other;
        return acceptedAtMillis == that.acceptedAtMillis
                && token.equals(that.token)
                && MessageDigest.isEqual(commandDigest, that.commandDigest);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(token, acceptedAtMillis) + Arrays.hashCode(commandDigest);
    }
}
