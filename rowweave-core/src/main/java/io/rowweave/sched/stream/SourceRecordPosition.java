package io.rowweave.sched.stream;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;

/**
 * one decoded row's deterministic position inside an exact bounded source split.
 * The raw split/snapshot locator is deliberately not retained.
 */
public final class SourceRecordPosition {

    public static final int MAX_SOURCE_UID_BYTES = 256;

    private final String sourceOperatorUid;
    private final String splitIdentitySha256;
    private final long splitLocalOrdinal;

    private SourceRecordPosition(
            String sourceOperatorUid, String splitIdentitySha256, long splitLocalOrdinal) {
        this.sourceOperatorUid = sourceOperatorUid;
        this.splitIdentitySha256 = splitIdentitySha256;
        this.splitLocalOrdinal = splitLocalOrdinal;
    }

    public static SourceRecordPosition of(
            String sourceOperatorUid, BatchSourceSplitDescriptor descriptor,
            long splitLocalOrdinal) {
        return forSplit(sourceOperatorUid, descriptor).at(splitLocalOrdinal);
    }

    /** Precomputes the bounded split digest once; one factory is reused for all rows in a split. */
    public static SplitFactory forSplit(
            String sourceOperatorUid, BatchSourceSplitDescriptor descriptor) {
        String validatedUid = validateSourceUid(sourceOperatorUid);
        if (descriptor == null) {
            throw invalid("split descriptor is required", null);
        }
        if (descriptor.replayGuarantee()
                != BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START) {
            throw invalid("source split must provide exact ordered replay", null);
        }
        final byte[] digest;
        try {
            digest = BatchSourceSplitDescriptorCodec.digest(descriptor);
        } catch (RuntimeException failure) {
            throw invalid("source split identity cannot be encoded", failure);
        }
        return new SplitFactory(validatedUid, lowercaseHex(digest));
    }

    public String sourceOperatorUid() {
        return sourceOperatorUid;
    }

    public String splitIdentitySha256() {
        return splitIdentitySha256;
    }

    public long splitLocalOrdinal() {
        return splitLocalOrdinal;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof SourceRecordPosition)) return false;
        SourceRecordPosition that = (SourceRecordPosition) other;
        return splitLocalOrdinal == that.splitLocalOrdinal
                && sourceOperatorUid.equals(that.sourceOperatorUid)
                && splitIdentitySha256.equals(that.splitIdentitySha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceOperatorUid, splitIdentitySha256, splitLocalOrdinal);
    }

    @Override
    public String toString() {
        return "SourceRecordPosition{source=" + sourceOperatorUid
                + ", split=" + splitIdentitySha256
                + ", ordinal=" + splitLocalOrdinal + '}';
    }

    private static String validateSourceUid(String value) {
        if (value == null || value.isEmpty()) {
            throw invalid("source operator uid must not be empty", null);
        }
        if (value.indexOf('\0') >= 0) {
            throw invalid("source operator uid must not contain NUL", null);
        }
        if (!Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw invalid("source operator uid must use NFC normalization", null);
        }
        final int bytes;
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            bytes = encoded.remaining();
        } catch (CharacterCodingException malformed) {
            throw invalid("source operator uid is not valid UTF-8", malformed);
        }
        if (bytes > MAX_SOURCE_UID_BYTES) {
            throw invalid("source operator uid exceeds UTF-8 byte limit "
                    + MAX_SOURCE_UID_BYTES, null);
        }
        return value;
    }

    private static String lowercaseHex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }

    private static IllegalArgumentException invalid(String detail, Throwable cause) {
        return new IllegalArgumentException("SOURCE_POSITION_INVALID: " + detail, cause);
    }

    /** Immutable split-scoped constructor that avoids digesting descriptors per row. */
    public static final class SplitFactory {
        private final String sourceOperatorUid;
        private final String splitIdentitySha256;

        private SplitFactory(String sourceOperatorUid, String splitIdentitySha256) {
            this.sourceOperatorUid = sourceOperatorUid;
            this.splitIdentitySha256 = splitIdentitySha256;
        }

        public SourceRecordPosition at(long splitLocalOrdinal) {
            if (splitLocalOrdinal < 0L) {
                throw invalid("split-local ordinal must not be negative", null);
            }
            return new SourceRecordPosition(
                    sourceOperatorUid, splitIdentitySha256, splitLocalOrdinal);
        }
    }
}
