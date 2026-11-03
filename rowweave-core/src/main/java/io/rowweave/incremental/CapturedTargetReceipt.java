package io.rowweave.incremental;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;

/** Immutable exact result of one bounded target capture. */
public final class CapturedTargetReceipt {
    public static final int DIGEST_BYTES = 32;
    public static final int MAX_REQUEST_CODE_UNITS = 256;
    public static final int MAX_REQUEST_BYTES = 256;
    public static final int MAX_SNAPSHOT_IDENTITY_BYTES = 16 * 1024;

    private final long feedIncarnation;
    private final byte[] definitionDigest;
    private final byte[] sourceContractDigest;
    private final long expectedSequence;
    private final IncrementalFrontier from;
    private final String requestIdentity;
    private final byte[] requestIdentityBytes;
    private final IncrementalRunMode mode;
    private final IncrementalFrontier target;
    private final byte[] snapshotIdentity;

    private CapturedTargetReceipt(
            long feedIncarnation, byte[] definitionDigest, byte[] sourceContractDigest,
            long expectedSequence, IncrementalFrontier from, String requestIdentity,
            IncrementalRunMode mode, IncrementalFrontier target, byte[] snapshotIdentity) {
        if (feedIncarnation <= 0L) throw new IllegalArgumentException("feed incarnation must be positive");
        if (expectedSequence < 0L) throw new IllegalArgumentException("expected sequence must be non-negative");
        this.feedIncarnation = feedIncarnation;
        this.definitionDigest = requireDigest(definitionDigest, "definition digest");
        this.sourceContractDigest = requireDigest(sourceContractDigest, "source contract digest");
        if (from == null || target == null || mode == null) {
            throw new IllegalArgumentException("capture frontiers and mode are required");
        }
        this.expectedSequence = expectedSequence;
        this.from = from;
        this.requestIdentityBytes = canonicalText(requestIdentity);
        this.requestIdentity = requestIdentity;
        this.mode = mode;
        this.target = target;
        if (snapshotIdentity == null || snapshotIdentity.length == 0
                || snapshotIdentity.length > MAX_SNAPSHOT_IDENTITY_BYTES) {
            throw new IllegalArgumentException("snapshot identity length is invalid");
        }
        this.snapshotIdentity = snapshotIdentity.clone();
        validateTransition();
    }

    public static CapturedTargetReceipt normal(
            long feedIncarnation, byte[] definitionDigest, byte[] sourceContractDigest,
            long expectedCommittedRevision, IncrementalFrontier committedFrontier,
            String triggerId, IncrementalRunMode mode, IncrementalFrontier target,
            byte[] snapshotIdentity) {
        if (mode != IncrementalRunMode.SNAPSHOT_V1
                && mode != IncrementalRunMode.INCREMENTAL_V1) {
            throw new IllegalArgumentException("normal receipt requires snapshot or incremental mode");
        }
        return new CapturedTargetReceipt(
                feedIncarnation, definitionDigest, sourceContractDigest,
                expectedCommittedRevision, committedFrontier, triggerId,
                mode, target, snapshotIdentity);
    }

    public static CapturedTargetReceipt backfill(
            long feedIncarnation, byte[] definitionDigest, byte[] sourceContractDigest,
            long expectedBackfillOrdinal, IncrementalFrontier from,
            IncrementalFrontier target, String requestKey, byte[] snapshotIdentity) {
        if (expectedBackfillOrdinal <= 0L) {
            throw new IllegalArgumentException("backfill ordinal must be positive");
        }
        return new CapturedTargetReceipt(
                feedIncarnation, definitionDigest, sourceContractDigest,
                expectedBackfillOrdinal, from, requestKey,
                IncrementalRunMode.BACKFILL_V1, target, snapshotIdentity);
    }

    public long feedIncarnation() {
        return feedIncarnation;
    }

    public byte[] definitionDigest() {
        return definitionDigest.clone();
    }

    public byte[] sourceContractDigest() {
        return sourceContractDigest.clone();
    }

    public long expectedSequence() {
        return expectedSequence;
    }

    public IncrementalFrontier from() {
        return from;
    }

    public String requestIdentity() {
        return requestIdentity;
    }

    byte[] requestIdentityBytes() {
        return requestIdentityBytes.clone();
    }

    public IncrementalRunMode mode() {
        return mode;
    }

    public IncrementalFrontier target() {
        return target;
    }

    public byte[] snapshotIdentity() {
        return snapshotIdentity.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof CapturedTargetReceipt)) return false;
        CapturedTargetReceipt that = (CapturedTargetReceipt) other;
        return feedIncarnation == that.feedIncarnation
                && expectedSequence == that.expectedSequence
                && mode == that.mode
                && requestIdentity.equals(that.requestIdentity)
                && from.equals(that.from)
                && target.equals(that.target)
                && Arrays.equals(definitionDigest, that.definitionDigest)
                && Arrays.equals(sourceContractDigest, that.sourceContractDigest)
                && Arrays.equals(snapshotIdentity, that.snapshotIdentity);
    }

    @Override
    public int hashCode() {
        int result = (int) (feedIncarnation ^ (feedIncarnation >>> 32));
        result = 31 * result + Arrays.hashCode(definitionDigest);
        result = 31 * result + Arrays.hashCode(sourceContractDigest);
        result = 31 * result + (int) (expectedSequence ^ (expectedSequence >>> 32));
        result = 31 * result + from.hashCode();
        result = 31 * result + requestIdentity.hashCode();
        result = 31 * result + mode.hashCode();
        result = 31 * result + target.hashCode();
        result = 31 * result + Arrays.hashCode(snapshotIdentity);
        return result;
    }

    private void validateTransition() {
        if (target.kind() == IncrementalFrontier.Kind.BEGIN_V1) {
            throw new IllegalArgumentException("captured target cannot be BEGIN");
        }
        if (mode == IncrementalRunMode.SNAPSHOT_V1) {
            IncrementalFrontierOrder.isSnapshotTransition(from, target, target.kind());
            return;
        }
        if (from.kind() == IncrementalFrontier.Kind.BEGIN_V1 || from.kind() != target.kind()) {
            throw new IllegalArgumentException("concrete capture frontier kinds differ");
        }
        int compared = IncrementalFrontierOrder.compareConcrete(from, target);
        if (mode == IncrementalRunMode.INCREMENTAL_V1 && compared > 0) {
            throw new IllegalArgumentException("incremental target precedes committed frontier");
        }
        if (mode == IncrementalRunMode.BACKFILL_V1 && compared >= 0) {
            throw new IllegalArgumentException("backfill interval must have positive width");
        }
    }

    private static byte[] requireDigest(byte[] digest, String field) {
        if (digest == null || digest.length != DIGEST_BYTES) {
            throw new IllegalArgumentException(field + " must be SHA-256");
        }
        return digest.clone();
    }

    private static byte[] canonicalText(String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("capture request identity is required");
        }
        if (value.length() > MAX_REQUEST_CODE_UNITS || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("capture request identity exceeds text limit");
        }
        if (!Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException("capture request identity must be NFC");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > MAX_REQUEST_BYTES) {
                throw new IllegalArgumentException("capture request identity exceeds byte limit");
            }
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException("capture request identity is invalid UTF-16", invalid);
        }
    }
}
