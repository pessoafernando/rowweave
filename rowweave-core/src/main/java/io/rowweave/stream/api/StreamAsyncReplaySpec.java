package io.rowweave.stream.api;

import java.util.Objects;

/** Provider replay authority used to recover one logical async request. */
public final class StreamAsyncReplaySpec {
    public enum Mode { AT_LEAST_ONCE, IMMUTABLE_VERSION, IDENTITY_KEYED }

    private final Mode mode;
    private final String authorityId;
    private final String authorityDigest;

    private StreamAsyncReplaySpec(Mode mode, String authorityId,
            String authorityDigest) {
        boolean replayStable = mode != Mode.AT_LEAST_ONCE;
        if (mode == null || replayStable != (authorityId != null
                && !authorityId.isEmpty() && digest(authorityDigest))
                || !replayStable
                && (authorityId != null || authorityDigest != null)) {
            throw new IllegalArgumentException("invalid async replay authority");
        }
        this.mode = mode;
        this.authorityId = authorityId;
        this.authorityDigest = authorityDigest;
    }

    public static StreamAsyncReplaySpec atLeastOnce() {
        return new StreamAsyncReplaySpec(Mode.AT_LEAST_ONCE, null, null);
    }

    public static StreamAsyncReplaySpec immutableVersion(String versionId,
            String digest) {
        return new StreamAsyncReplaySpec(
                Mode.IMMUTABLE_VERSION, versionId, digest);
    }

    public static StreamAsyncReplaySpec identityKeyed(String identityCodecId,
            String digest) {
        return new StreamAsyncReplaySpec(
                Mode.IDENTITY_KEYED, identityCodecId, digest);
    }

    private static boolean digest(String value) {
        if (value == null || value.length() != 64) return false;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current < '0' || current > '9'
                    && (current < 'a' || current > 'f')) return false;
        }
        return true;
    }

    public Mode mode() { return mode; }
    public String authorityId() { return authorityId; }
    public String authorityDigest() { return authorityDigest; }
    public boolean replayStable() { return mode != Mode.AT_LEAST_ONCE; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamAsyncReplaySpec)) return false;
        StreamAsyncReplaySpec that = (StreamAsyncReplaySpec) other;
        return mode == that.mode && Objects.equals(authorityId, that.authorityId)
                && Objects.equals(authorityDigest, that.authorityDigest);
    }

    @Override public int hashCode() {
        return Objects.hash(mode, authorityId, authorityDigest);
    }
}
