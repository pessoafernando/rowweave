package io.rowweave;

import io.rowweave.sched.stream.SourceRecordPosition;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;
import java.util.regex.Pattern;

/** Bounded, payload-free identity of one rejected quality record. */
public final class QualityExemplar implements Comparable<QualityExemplar> {
    private static final Pattern SPLIT = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern CODE = Pattern.compile("[A-Z0-9_]{1,64}");

    private final String sourceOperatorUid;
    private final String splitIdentitySha256;
    private final long splitLocalOrdinal;
    private final String qualityErrorCode;
    private final byte[] sourceOperatorUidBytes;
    private final byte[] splitIdentityBytes;
    private final byte[] qualityErrorCodeBytes;

    private QualityExemplar(String sourceOperatorUid, String splitIdentitySha256,
                            long splitLocalOrdinal, String qualityErrorCode) {
        this.sourceOperatorUidBytes = canonicalUidBytes(sourceOperatorUid);
        if (splitIdentitySha256 == null
                || !SPLIT.matcher(splitIdentitySha256).matches()) {
            throw new IllegalArgumentException(
                    "quality exemplar split identity must be lowercase SHA-256");
        }
        if (splitLocalOrdinal < 0L) {
            throw new IllegalArgumentException(
                    "quality exemplar split-local ordinal is negative");
        }
        if (qualityErrorCode == null || !CODE.matcher(qualityErrorCode).matches()) {
            throw new IllegalArgumentException(
                    "quality exemplar error code is outside the closed grammar");
        }
        this.sourceOperatorUid = sourceOperatorUid;
        this.splitIdentitySha256 = splitIdentitySha256;
        this.splitLocalOrdinal = splitLocalOrdinal;
        this.qualityErrorCode = qualityErrorCode;
        this.splitIdentityBytes = decodeHex(splitIdentitySha256);
        this.qualityErrorCodeBytes = qualityErrorCode.getBytes(StandardCharsets.US_ASCII);
    }

    public static QualityExemplar of(String sourceOperatorUid,
                                     String splitIdentitySha256,
                                     long splitLocalOrdinal,
                                     String qualityErrorCode) {
        return new QualityExemplar(sourceOperatorUid, splitIdentitySha256,
                splitLocalOrdinal, qualityErrorCode);
    }

    public String sourceOperatorUid() { return sourceOperatorUid; }
    public String splitIdentitySha256() { return splitIdentitySha256; }
    public long splitLocalOrdinal() { return splitLocalOrdinal; }
    public String qualityErrorCode() { return qualityErrorCode; }

    @Override
    public int compareTo(QualityExemplar other) {
        int compared = compareUnsigned(sourceOperatorUidBytes,
                other.sourceOperatorUidBytes);
        if (compared != 0) return compared;
        compared = compareUnsigned(splitIdentityBytes, other.splitIdentityBytes);
        if (compared != 0) return compared;
        compared = Long.compare(splitLocalOrdinal, other.splitLocalOrdinal);
        if (compared != 0) return compared;
        return compareUnsigned(qualityErrorCodeBytes, other.qualityErrorCodeBytes);
    }

    private static byte[] canonicalUidBytes(String uid) {
        if (uid == null || uid.isEmpty() || uid.indexOf('\0') >= 0
                || !Normalizer.isNormalized(uid, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException(
                    "quality exemplar source UID is noncanonical");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(uid));
            if (encoded.remaining() > SourceRecordPosition.MAX_SOURCE_UID_BYTES) {
                throw new IllegalArgumentException(
                        "quality exemplar source UID exceeds its UTF-8 bound");
            }
            byte[] result = new byte[encoded.remaining()];
            encoded.get(result);
            return result;
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException(
                    "quality exemplar source UID is not strict UTF-8", malformed);
        }
    }

    private static byte[] decodeHex(String value) {
        byte[] result = new byte[value.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) ((Character.digit(value.charAt(i * 2), 16) << 4)
                    | Character.digit(value.charAt(i * 2 + 1), 16));
        }
        return result;
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        int common = Math.min(left.length, right.length);
        for (int i = 0; i < common; i++) {
            int compared = Integer.compare(left[i] & 0xff, right[i] & 0xff);
            if (compared != 0) return compared;
        }
        return Integer.compare(left.length, right.length);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof QualityExemplar)) return false;
        QualityExemplar that = (QualityExemplar) other;
        return splitLocalOrdinal == that.splitLocalOrdinal
                && sourceOperatorUid.equals(that.sourceOperatorUid)
                && splitIdentitySha256.equals(that.splitIdentitySha256)
                && qualityErrorCode.equals(that.qualityErrorCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceOperatorUid, splitIdentitySha256,
                splitLocalOrdinal, qualityErrorCode);
    }
}
