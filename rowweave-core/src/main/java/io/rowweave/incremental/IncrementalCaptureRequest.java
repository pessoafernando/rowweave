package io.rowweave.incremental;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;

/** Core-owned bounded request for a read-only provider target capture. */
public final class IncrementalCaptureRequest {
    private final long feedIncarnation;
    private final byte[] definitionDigest;
    private final long expectedSequence;
    private final IncrementalFrontier from;
    private final String requestIdentity;
    private final byte[] requestIdentityBytes;
    private final IncrementalRunMode mode;
    private final IncrementalFrontier requestedTarget;

    private IncrementalCaptureRequest(
            long feedIncarnation, byte[] definitionDigest,
            long expectedSequence, IncrementalFrontier from,
            String requestIdentity, IncrementalRunMode mode,
            IncrementalFrontier requestedTarget) {
        if (feedIncarnation <= 0L || definitionDigest == null
                || definitionDigest.length != CapturedTargetReceipt.DIGEST_BYTES
                || expectedSequence < 0L || from == null || mode == null) {
            throw new IllegalArgumentException(
                    "incremental capture request fields are invalid");
        }
        this.requestIdentityBytes = canonicalText(requestIdentity);
        if (mode == IncrementalRunMode.SNAPSHOT_V1) {
            if (from.kind() != IncrementalFrontier.Kind.BEGIN_V1
                    || requestedTarget != null) {
                throw new IllegalArgumentException(
                        "snapshot capture must start at BEGIN without a requested target");
            }
        } else if (mode == IncrementalRunMode.INCREMENTAL_V1) {
            if (from.kind() == IncrementalFrontier.Kind.BEGIN_V1
                    || requestedTarget != null) {
                throw new IllegalArgumentException(
                        "incremental capture requires a concrete committed frontier");
            }
        } else {
            if (expectedSequence <= 0L || requestedTarget == null
                    || from.kind() == IncrementalFrontier.Kind.BEGIN_V1
                    || requestedTarget.kind() != from.kind()
                    || IncrementalFrontierOrder.compareConcrete(
                    from, requestedTarget) >= 0) {
                throw new IllegalArgumentException(
                        "backfill capture requires an exact positive-width target");
            }
        }
        this.feedIncarnation = feedIncarnation;
        this.definitionDigest = definitionDigest.clone();
        this.expectedSequence = expectedSequence;
        this.from = from;
        this.requestIdentity = requestIdentity;
        this.mode = mode;
        this.requestedTarget = requestedTarget;
    }

    public static IncrementalCaptureRequest normal(
            long feedIncarnation, byte[] definitionDigest,
            long expectedCommittedRevision, IncrementalFrontier committed,
            String triggerId, IncrementalRunMode mode) {
        if (mode != IncrementalRunMode.SNAPSHOT_V1
                && mode != IncrementalRunMode.INCREMENTAL_V1) {
            throw new IllegalArgumentException(
                    "normal capture requires snapshot or incremental mode");
        }
        return new IncrementalCaptureRequest(
                feedIncarnation, definitionDigest, expectedCommittedRevision,
                committed, triggerId, mode, null);
    }

    public static IncrementalCaptureRequest backfill(
            long feedIncarnation, byte[] definitionDigest,
            long expectedBackfillOrdinal, IncrementalFrontier from,
            IncrementalFrontier target, String requestKey) {
        return new IncrementalCaptureRequest(
                feedIncarnation, definitionDigest, expectedBackfillOrdinal,
                from, requestKey, IncrementalRunMode.BACKFILL_V1, target);
    }

    public CapturedTargetReceipt bind(
            byte[] sourceContractDigest,
            IncrementalCaptureResponse response) {
        if (sourceContractDigest == null
                || sourceContractDigest.length
                != CapturedTargetReceipt.DIGEST_BYTES
                || response == null) {
            throw new IllegalArgumentException(
                    "capture binding fields are invalid");
        }
        if (mode == IncrementalRunMode.BACKFILL_V1) {
            if (!requestedTarget.equals(response.target())) {
                throw new IllegalArgumentException(
                        "backfill provider target differs from the requested target");
            }
            return CapturedTargetReceipt.backfill(
                    feedIncarnation, definitionDigest, sourceContractDigest,
                    expectedSequence, from, requestedTarget, requestIdentity,
                    response.snapshotIdentity());
        }
        return CapturedTargetReceipt.normal(
                feedIncarnation, definitionDigest, sourceContractDigest,
                expectedSequence, from, requestIdentity, mode,
                response.target(), response.snapshotIdentity());
    }

    public long feedIncarnation() { return feedIncarnation; }
    public byte[] definitionDigest() { return definitionDigest.clone(); }
    public long expectedSequence() { return expectedSequence; }
    public IncrementalFrontier from() { return from; }
    public String requestIdentity() { return requestIdentity; }
    byte[] requestIdentityBytes() { return requestIdentityBytes.clone(); }
    public IncrementalRunMode mode() { return mode; }
    public IncrementalFrontier requestedTarget() { return requestedTarget; }

    private static byte[] canonicalText(String value) {
        if (value == null || value.isEmpty()
                || value.length()
                > CapturedTargetReceipt.MAX_REQUEST_CODE_UNITS
                || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException(
                    "capture request identity is not canonical bounded text");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > CapturedTargetReceipt.MAX_REQUEST_BYTES) {
                throw new IllegalArgumentException(
                        "capture request identity exceeds UTF-8 bound");
            }
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(
                    "capture request identity is invalid UTF-16", invalid);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalCaptureRequest)) return false;
        IncrementalCaptureRequest that = (IncrementalCaptureRequest) other;
        return feedIncarnation == that.feedIncarnation
                && expectedSequence == that.expectedSequence
                && Arrays.equals(definitionDigest, that.definitionDigest)
                && from.equals(that.from)
                && requestIdentity.equals(that.requestIdentity)
                && mode == that.mode
                && Objects.equals(requestedTarget, that.requestedTarget);
    }

    @Override
    public int hashCode() {
        return Objects.hash(feedIncarnation, Arrays.hashCode(definitionDigest),
                expectedSequence, from, requestIdentity, mode,
                requestedTarget);
    }
}
