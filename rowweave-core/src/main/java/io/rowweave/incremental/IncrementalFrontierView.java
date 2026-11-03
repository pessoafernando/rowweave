package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Bounded public projection that never exposes frontier payload or source identity. */
public final class IncrementalFrontierView {
    private static final int DIGEST_PREFIX_HEX_LENGTH = 16;

    private final String feedId;
    private final long feedIncarnation;
    private final long revision;
    private final IncrementalFrontier.Kind frontierKind;
    private final String definitionDigestPrefix;
    private final String committedFrontierDigestPrefix;
    private final String activeRunId;
    private final String state;

    public IncrementalFrontierView(
            String feedId, long feedIncarnation, long revision,
            IncrementalFrontier.Kind frontierKind,
            String definitionDigestPrefix,
            String committedFrontierDigestPrefix,
            String activeRunId, String state) {
        this.feedId = requireFeedId(feedId);
        if (feedIncarnation <= 0L || revision < 0L || frontierKind == null
                || frontierKind == IncrementalFrontier.Kind.BEGIN_V1) {
            throw new IllegalArgumentException("frontier view identity is invalid");
        }
        this.feedIncarnation = feedIncarnation;
        this.revision = revision;
        this.frontierKind = frontierKind;
        this.definitionDigestPrefix = requireDigestPrefix(
                definitionDigestPrefix, "definition digest prefix");
        this.committedFrontierDigestPrefix = requireDigestPrefix(
                committedFrontierDigestPrefix, "committed frontier digest prefix");
        this.activeRunId = activeRunId == null ? null
                : new RunAttemptFence(activeRunId, "frontier-view", 1L).runId();
        try {
            this.state = IncrementalFrontierAuthorityState.valueOf(state).name();
        } catch (RuntimeException invalid) {
            throw new IllegalArgumentException("frontier view state is invalid", invalid);
        }
        if ((this.activeRunId != null)
                != (IncrementalFrontierAuthorityState.ACTIVE.name().equals(this.state))) {
            throw new IllegalArgumentException("frontier view active Run does not match state");
        }
    }

    public static IncrementalFrontierView from(IncrementalAuthorityRecord record) {
        if (record == null) throw new IllegalArgumentException("frontier record is required");
        String activeRunId = record.activeInterval() == null ? null
                : record.activeInterval().fence().runId();
        return new IncrementalFrontierView(
                record.feedId(), record.feedIncarnation(), record.revision(),
                record.frontierKind(), hexPrefix(record.definitionDigest()),
                hexPrefix(sha256(IncrementalFrontierCodec.encode(
                        record.committedFrontier()))),
                activeRunId, record.state().name());
    }

    public String feedId() { return feedId; }
    public long feedIncarnation() { return feedIncarnation; }
    public long revision() { return revision; }
    public IncrementalFrontier.Kind frontierKind() { return frontierKind; }
    public String definitionDigestPrefix() { return definitionDigestPrefix; }
    public String committedFrontierDigestPrefix() { return committedFrontierDigestPrefix; }
    public String activeRunId() { return activeRunId; }
    public String state() { return state; }

    static String requireFeedId(String value) {
        if (value == null || value.isEmpty()
                || value.getBytes(StandardCharsets.UTF_8).length
                > IncrementalAuthorityValue.MAX_ID_BYTES) {
            throw new IllegalArgumentException("frontier view feed id is invalid");
        }
        return value;
    }

    private static String requireDigestPrefix(String value, String field) {
        if (value == null || value.length() != DIGEST_PREFIX_HEX_LENGTH) {
            throw new IllegalArgumentException(field + " is invalid");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw new IllegalArgumentException(field + " is invalid");
            }
        }
        return value;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String hexPrefix(byte[] digest) {
        if (digest == null || digest.length < DIGEST_PREFIX_HEX_LENGTH / 2) {
            throw new IllegalArgumentException("digest is too short for public prefix");
        }
        char[] digits = "0123456789abcdef".toCharArray();
        char[] prefix = new char[DIGEST_PREFIX_HEX_LENGTH];
        for (int i = 0; i < prefix.length / 2; i++) {
            int next = digest[i] & 0xff;
            prefix[i * 2] = digits[next >>> 4];
            prefix[i * 2 + 1] = digits[next & 0x0f];
        }
        return new String(prefix);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalFrontierView)) return false;
        IncrementalFrontierView that = (IncrementalFrontierView) other;
        return feedIncarnation == that.feedIncarnation && revision == that.revision
                && feedId.equals(that.feedId) && frontierKind == that.frontierKind
                && definitionDigestPrefix.equals(that.definitionDigestPrefix)
                && committedFrontierDigestPrefix.equals(
                that.committedFrontierDigestPrefix)
                && Objects.equals(activeRunId, that.activeRunId)
                && state.equals(that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(feedId, feedIncarnation, revision, frontierKind,
                definitionDigestPrefix, committedFrontierDigestPrefix,
                activeRunId, state);
    }
}
