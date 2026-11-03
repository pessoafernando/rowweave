package io.rowweave.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the refusal codes that belong to the durable hand-off substrate: the
 * frame codecs and the authority's own outcomes. Together with
 * {@link PipelineRefusalContractTest} this covers every code the module can
 * return; the pairing table is
 * {@code docs/product/reference/pipeline-boundary.md} sections 3.1 and 3.2.
 */
class VersionedDatasetHandoffFaultContractTest {
    private static final String SCHEMA = repeat('a', 64);
    private static final String DIGEST = repeat('b', 64);
    private static final long EPOCH_MILLIS = 1_700_000_000_000L;

    @TempDir
    Path temporary;

    @Test
    void publicEncodersRefuseAMissingValue() {
        assertFailure("INVALID_RECORD", () -> VersionedDatasetHandoffCodec.encodeKey(null));
        assertFailure("INVALID_RECORD", () -> VersionedDatasetHandoffCodec.encodeAttempt(null));
        assertFailure("INVALID_RECORD", () -> VersionedDatasetHandoffCodec.encodeEvidence(null));
        assertFailure("INVALID_RECORD", () -> VersionedDatasetHandoffCodec.encodeWinner(null));
        assertFailure("INVALID_RECORD",
                () -> VersionedDatasetHandoffCodec.encodeConsumerRequest(null));
        assertFailure("INVALID_RECORD",
                () -> VersionedDatasetHandoffCodec.encodeConsumerBinding(null));
    }

    @Test
    void frameWithExtraBytesIsRefusedAsNonCanonical() throws IOException {
        byte[] frame = VersionedDatasetHandoffCodec.encodeEvidence(evidence());
        byte[] extended = reEnvelope(frame, 9);

        assertFailure("NON_CANONICAL_FRAME",
                () -> VersionedDatasetHandoffCodec.decodeEvidence(extended));
    }

    @Test
    void authorityRecordWithAWinnerFromAnotherKeyIsRefused() {
        VersionedDatasetProducerAttempt claim = attempt("edge-one", "dataset-one");
        VersionedDatasetVisibleWinner other = new VersionedDatasetVisibleWinner(
                evidence(attempt("edge-two", "dataset-two"), "version-two"), EPOCH_MILLIS);
        byte[] frame = VersionedDatasetAuthorityRecordCodec.encode(
                new VersionedDatasetAuthorityRecordCodec.Slot(claim, other));

        assertFailure("AUTHORITY_RECORD_MISMATCH",
                () -> VersionedDatasetAuthorityRecordCodec.decode(frame));
    }

    @Test
    void exhaustedCompareAndSetBudgetIsReportedAsContention() {
        VersionedDatasetAuthorityRecordStore alwaysLoses =
                new VersionedDatasetAuthorityRecordStore() {
                    @Override public Optional<VersionedDatasetAuthorityRecord> read(
                            RecordKind kind, String recordId) {
                        return Optional.empty();
                    }
                    @Override public boolean compareAndSet(
                            RecordKind kind, String recordId,
                            String expectedVersionToken, byte[] replacement) {
                        return false;
                    }
                };
        StoreBackedVersionedDatasetHandoffAuthority authority =
                new StoreBackedVersionedDatasetHandoffAuthority(
                        alwaysLoses, () -> EPOCH_MILLIS);

        assertAuthorityFailure("AUTHORITY_CONTENTION",
                () -> authority.claim(attempt("edge-one", "dataset-one")));
    }

    @Test
    void aRecordOwnedByAnotherKeyIsReportedAsDigestConflict() {
        final VersionedDatasetProducerAttempt occupant = attempt("edge-two", "dataset-two");
        byte[] record = VersionedDatasetAuthorityRecordCodec.encode(
                new VersionedDatasetAuthorityRecordCodec.Slot(occupant, null));
        VersionedDatasetAuthorityRecordStore mislabelled =
                new VersionedDatasetAuthorityRecordStore() {
                    @Override public Optional<VersionedDatasetAuthorityRecord> read(
                            RecordKind kind, String recordId) {
                        return Optional.of(new VersionedDatasetAuthorityRecord(record));
                    }
                    @Override public boolean compareAndSet(
                            RecordKind kind, String recordId,
                            String expectedVersionToken, byte[] replacement) {
                        return false;
                    }
                };
        StoreBackedVersionedDatasetHandoffAuthority authority =
                new StoreBackedVersionedDatasetHandoffAuthority(
                        mislabelled, () -> EPOCH_MILLIS);

        assertAuthorityFailure("KEY_DIGEST_CONFLICT",
                () -> authority.claim(attempt("edge-one", "dataset-one")));
    }

    @Test
    void aNonPositiveAuthorityClockStopsThePublish() {
        AtomicInteger reads = new AtomicInteger();
        FileVersionedDatasetHandoffAuthority authority =
                new FileVersionedDatasetHandoffAuthority(
                        temporary, () -> EPOCH_MILLIS + reads.incrementAndGet(),
                        () -> { });
        VersionedDatasetProducerAttempt claimed = authority.claim(
                attempt("edge-one", "dataset-one"));
        final VersionedDatasetCommitEvidence committed = evidence(claimed, "version-one");

        FileVersionedDatasetHandoffAuthority frozenClock =
                new FileVersionedDatasetHandoffAuthority(temporary, () -> 0L, () -> { });
        assertAuthorityFailure("INVALID_AUTHORITY_TIME",
                () -> frozenClock.publish(committed));
    }

    private static VersionedDatasetHandoffKey key(String edgeId, String materializationId) {
        return new VersionedDatasetHandoffKey("p", "r", DIGEST, edgeId, materializationId,
                SCHEMA, "a", "a-sink", "artifact-a", "producer-run");
    }

    private static VersionedDatasetProducerAttempt attempt(
            String edgeId, String materializationId) {
        return new VersionedDatasetProducerAttempt(key(edgeId, materializationId),
                "producer-run", "producer-attempt", 1L, 7L);
    }

    private static VersionedDatasetCommitEvidence evidence() {
        return evidence(attempt("edge-one", "dataset-one"), "version-one");
    }

    private static VersionedDatasetCommitEvidence evidence(
            VersionedDatasetProducerAttempt attempt, String versionId) {
        return new VersionedDatasetCommitEvidence(attempt, "file", 1, versionId, "raw", 1,
                new byte[] {1, 2, 3}, DIGEST, 10L, 1024L, EPOCH_MILLIS);
    }

    /**
     * Keeps the encoded body of a valid frame and adds {@code extraBytes} of
     * trailing payload, then re-signs the frame so the envelope checksum passes
     * and only the canonical-shape guard can reject it.
     */
    private static byte[] reEnvelope(byte[] frame, int extraBytes) throws IOException {
        int headerBytes = 8 + 4 + 4 + 4;
        int bodyLength = ((frame[16] & 0xff) << 24) | ((frame[17] & 0xff) << 16)
                | ((frame[18] & 0xff) << 8) | (frame[19] & 0xff);
        byte[] magic = Arrays.copyOfRange(frame, 0, 8);
        int version = ((frame[8] & 0xff) << 24) | ((frame[9] & 0xff) << 16)
                | ((frame[10] & 0xff) << 8) | (frame[11] & 0xff);
        int type = ((frame[12] & 0xff) << 24) | ((frame[13] & 0xff) << 16)
                | ((frame[14] & 0xff) << 8) | (frame[15] & 0xff);
        byte[] body = Arrays.copyOfRange(frame, headerBytes, headerBytes + bodyLength);
        byte[] grown = Arrays.copyOf(body, body.length + extraBytes);
        byte[] checksum = fromHex(VersionedDatasetHandoffCodec.sha256Hex(grown));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream frame1 = new DataOutputStream(out)) {
            frame1.write(magic);
            frame1.writeInt(version);
            frame1.writeInt(type);
            frame1.writeInt(grown.length);
            frame1.write(grown);
            frame1.write(checksum);
        }
        return out.toByteArray();
    }

    private static byte[] fromHex(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    private static void assertFailure(String code, Runnable action) {
        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class, action::run);
        assertEquals(code, failure.code());
        assertTrue(failure.getMessage().contains(code));
    }

    private static void assertAuthorityFailure(String code, Runnable action) {
        VersionedDatasetHandoffException failure = assertThrows(
                VersionedDatasetHandoffException.class, action::run);
        assertEquals(code, failure.code());
        assertTrue(failure.getMessage().contains(code));
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
}
