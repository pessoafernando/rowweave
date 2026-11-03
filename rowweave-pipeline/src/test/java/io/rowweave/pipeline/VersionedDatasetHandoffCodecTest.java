package io.rowweave.pipeline;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VersionedDatasetHandoffCodecTest {
    private static final String DIGEST = repeat('a', 64);
    private static final String SCHEMA = repeat('b', 64);
    private static final String CONTENT = repeat('c', 64);

    @Test
    void everyPublicRecordHasCanonicalRoundTripAndStableIdentity() {
        VersionedDatasetHandoffKey key = key();
        VersionedDatasetProducerAttempt attempt = new VersionedDatasetProducerAttempt(
                key, "producer-run", "attempt-2", 2L, 19L);
        VersionedDatasetCommitEvidence evidence = new VersionedDatasetCommitEvidence(
                attempt, "s3-compatible", 1, "version-7",
                "rowweave.s3.reader-binding", 1, new byte[] {4, 5, 6},
                CONTENT, 23L, 4096L, 1700000000000L);
        VersionedDatasetVisibleWinner winner = new VersionedDatasetVisibleWinner(
                evidence, 1700000001000L);
        VersionedDatasetConsumerRequest request = new VersionedDatasetConsumerRequest(
                key, winner.winnerDigest(), "consumer-step", "consumer-source",
                "consumer-artifact", "consumer-run", "consumer-attempt");
        VersionedDatasetConsumerBinding binding = new VersionedDatasetConsumerBinding(
                request, winner, 1700000002000L);

        assertEquals(key, VersionedDatasetHandoffCodec.decodeKey(
                VersionedDatasetHandoffCodec.encodeKey(key)));
        assertEquals(attempt, VersionedDatasetHandoffCodec.decodeAttempt(
                VersionedDatasetHandoffCodec.encodeAttempt(attempt)));
        assertEquals(evidence, VersionedDatasetHandoffCodec.decodeEvidence(
                VersionedDatasetHandoffCodec.encodeEvidence(evidence)));
        assertEquals(winner, VersionedDatasetHandoffCodec.decodeWinner(
                VersionedDatasetHandoffCodec.encodeWinner(winner)));
        assertEquals(request, VersionedDatasetHandoffCodec.decodeConsumerRequest(
                VersionedDatasetHandoffCodec.encodeConsumerRequest(request)));
        assertEquals(binding, VersionedDatasetHandoffCodec.decodeConsumerBinding(
                VersionedDatasetHandoffCodec.encodeConsumerBinding(binding)));

        assertEquals(64, key.keyDigest().length());
        assertEquals(64, evidence.evidenceDigest().length());
        assertEquals("9bda97e9143425942980a779110c952696cc75a5b811a2d94856e7b3c289ebb1",
                key.keyDigest());
        assertEquals("00b0b7cd05b13a507e31cff410c66b643e880cb16dd652d6a1479c3c2a72bde0",
                evidence.evidenceDigest());
        assertEquals(evidence.evidenceDigest(), winner.winnerDigest());
        assertEquals(64, binding.bindingId().length());
        assertArrayEquals(VersionedDatasetHandoffCodec.encodeEvidence(evidence),
                VersionedDatasetHandoffCodec.encodeEvidence(
                        VersionedDatasetHandoffCodec.decodeEvidence(
                                VersionedDatasetHandoffCodec.encodeEvidence(evidence))));
    }

    @Test
    void framesFailClosedBeforeReturningPartialRecords() {
        byte[] encoded = VersionedDatasetHandoffCodec.encodeEvidence(evidence(3));

        byte[] badMagic = encoded.clone();
        badMagic[0] ^= 1;
        assertDecodeFailure("INVALID_MAGIC", badMagic);

        byte[] badVersion = encoded.clone();
        writeInt(badVersion, 8, 2);
        assertDecodeFailure("UNSUPPORTED_ENVELOPE_VERSION", badVersion);

        byte[] wrongType = VersionedDatasetHandoffCodec.encodeKey(key());
        PipelineValidationException typeFailure = assertThrows(
                PipelineValidationException.class,
                () -> VersionedDatasetHandoffCodec.decodeEvidence(wrongType));
        assertEquals("UNEXPECTED_RECORD_TYPE", typeFailure.code());

        byte[] badLength = encoded.clone();
        writeInt(badLength, 16, Integer.MAX_VALUE);
        assertDecodeFailure("LIMIT_EXCEEDED", badLength);

        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertDecodeFailure("CHECKSUM_MISMATCH", corrupt);

        assertDecodeFailure("FRAME_LENGTH_MISMATCH",
                Arrays.copyOf(encoded, encoded.length - 1));
        assertDecodeFailure("FRAME_LENGTH_MISMATCH",
                Arrays.copyOf(encoded, encoded.length + 1));
    }

    @Test
    void readerBindingAndAllDigestsAreDefensivelyOwned() {
        byte[] reader = new byte[] {1, 2, 3};
        VersionedDatasetCommitEvidence evidence = new VersionedDatasetCommitEvidence(
                attempt(), "provider", 1, "version",
                "reader", 1, reader, CONTENT, 1L, 2L, 3L);
        byte[] original = VersionedDatasetHandoffCodec.encodeEvidence(evidence);
        reader[0] = 99;
        byte[] returned = evidence.readerBinding();
        returned[1] = 88;

        assertArrayEquals(original, VersionedDatasetHandoffCodec.encodeEvidence(evidence));
        assertArrayEquals(new byte[] {1, 2, 3}, evidence.readerBinding());
    }

    @Test
    void resourceLimitsAndIdentityDriftAreRejected() {
        byte[] atLimit = new byte[VersionedDatasetHandoffLimits.MAX_READER_BINDING_BYTES];
        VersionedDatasetCommitEvidence accepted = new VersionedDatasetCommitEvidence(
                attempt(), "provider", 1, "version", "reader", 1,
                atLimit, CONTENT, 0L, 0L, 1L);
        VersionedDatasetHandoffCodec.encodeEvidence(accepted);

        assertFailure("LIMIT_EXCEEDED", () -> new VersionedDatasetCommitEvidence(
                attempt(), "provider", 1, "version", "reader", 1,
                new byte[VersionedDatasetHandoffLimits.MAX_READER_BINDING_BYTES + 1],
                CONTENT, 0L, 0L, 1L));
        assertFailure("INVALID_DIGEST", () -> new VersionedDatasetCommitEvidence(
                attempt(), "provider", 1, "version", "reader", 1,
                new byte[0], "not-a-digest", 0L, 0L, 1L));
        assertFailure("INVALID_COUNT", () -> new VersionedDatasetCommitEvidence(
                attempt(), "provider", 1, "version", "reader", 1,
                new byte[0], CONTENT, -1L, 0L, 1L));
        assertFailure("INVALID_ATTEMPT_FENCE", () ->
                new VersionedDatasetProducerAttempt(key(), "run", "attempt", 0L, 1L));
        assertFailure("INVALID_UTF8", () -> new VersionedDatasetHandoffKey(
                "bad-\uD800", "r", DIGEST, "edge", "dataset", SCHEMA,
                "producer", "sink", "artifact", "logical"));

        VersionedDatasetVisibleWinner first = new VersionedDatasetVisibleWinner(
                evidence(1), 10L);
        VersionedDatasetVisibleWinner second = new VersionedDatasetVisibleWinner(
                evidence(2), 10L);
        assertNotEquals(first.winnerDigest(), second.winnerDigest());
    }

    private static VersionedDatasetCommitEvidence evidence(int marker) {
        return new VersionedDatasetCommitEvidence(attempt(), "provider", 1,
                "version-" + marker, "reader", 1, new byte[] {(byte) marker},
                CONTENT, marker, marker * 10L, 100L + marker);
    }

    private static VersionedDatasetProducerAttempt attempt() {
        return new VersionedDatasetProducerAttempt(
                key(), "producer-run", "attempt", 1L, 11L);
    }

    private static VersionedDatasetHandoffKey key() {
        return new VersionedDatasetHandoffKey(
                "pipeline", "revision", DIGEST, "edge", "dataset", SCHEMA,
                "producer-step", "producer-sink", "producer-artifact", "logical-run");
    }

    private static void assertDecodeFailure(String code, byte[] encoded) {
        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class,
                () -> VersionedDatasetHandoffCodec.decodeEvidence(encoded));
        assertEquals(code, failure.code());
    }

    private static void assertFailure(String code, Runnable action) {
        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
}
