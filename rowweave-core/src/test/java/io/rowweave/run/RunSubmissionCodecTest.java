package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunSubmissionCodecTest {

    @Test
    void immutableSubmissionAndEnvelopeRoundTripDeterministically() {
        RunSubmission submission = new RunSubmission(
                "run-Ω-001", "attempt-Ψ-001", "publication-Ψ-001",
                3, 10_000L, 1_000L);
        byte[] artifact = {0, 1, 2, 3, (byte) 0xff};

        byte[] encoded = RunSubmissionCodec.encode(submission, artifact);
        RunSubmissionCodec.Decoded decoded = RunSubmissionCodec.decode(encoded);

        assertEquals(submission.runId(), decoded.submission().runId());
        assertEquals(submission.attemptId(), decoded.submission().attemptId());
        assertEquals(submission.publicationId(), decoded.submission().publicationId());
        assertEquals(3, decoded.submission().maxAttempts());
        assertEquals(10_000L, decoded.submission().deadlineEpochMillis());
        assertEquals(1_000L, decoded.submission().submittedAtMillis());
        assertArrayEquals(artifact, decoded.artifactBytes());
        assertArrayEquals(encoded, RunSubmissionCodec.encode(
                decoded.submission(), decoded.artifactBytes()));

        byte[] first = decoded.artifactBytes();
        byte[] second = decoded.artifactBytes();
        assertNotSame(first, second);
        first[0] = 99;
        assertArrayEquals(artifact, decoded.artifactBytes());
    }

    @Test
    void identityAndScalarBoundsFailBeforeEncoding() {
        String nfd = Normalizer.normalize("é", Normalizer.Form.NFD);
        assertThrows(IllegalArgumentException.class,
                () -> new RunSubmission(null, "attempt", "publication", 1, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunSubmission("run\0bad", "attempt", "publication", 1, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunSubmission(nfd, "attempt", "publication", 1, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunSubmission(repeat('r', 257), "attempt", "publication", 1, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunSubmission("run", "attempt", "publication", 0, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunSubmission("run", "attempt", "publication", 1001, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunSubmission("run", "attempt", "publication", 1, -1L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunSubmission("run", "attempt", "publication", 1, 9L, 10L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunSubmission("run", "attempt", "publication", 1, 0L, -1L));

        RunSubmission boundary = new RunSubmission(
                repeat('r', 256), repeat('a', 256), repeat('p', 256), 1000,
                Long.MAX_VALUE, Long.MAX_VALUE);
        RunSubmission decoded = RunSubmissionCodec.decode(
                RunSubmissionCodec.encode(boundary, new byte[0])).submission();
        assertEquals(boundary.runId(), decoded.runId());
        assertEquals(1000, decoded.maxAttempts());
    }

    @Test
    void everyTruncatedPrefixTrailingByteAndChecksumDamageFailClosed() {
        byte[] encoded = RunSubmissionCodec.encode(
                new RunSubmission("run", "attempt", "publication", 2, 0L, 1L),
                new byte[]{4, 5, 6});
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class,
                    () -> RunSubmissionCodec.decode(truncated), "length=" + length);
        }

        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        assertThrows(IllegalArgumentException.class, () -> RunSubmissionCodec.decode(trailing));

        byte[] corrupt = encoded.clone();
        corrupt[20] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RunSubmissionCodec.decode(corrupt));
    }

    @Test
    void unknownHeaderAndImpossibleArtifactLengthFailClosedEvenWithValidChecksum() throws Exception {
        byte[] encoded = RunSubmissionCodec.encode(
                new RunSubmission("run", "attempt", "publication", 2, 0L, 1L),
                new byte[]{4, 5, 6});

        byte[] unknownVersion = encoded.clone();
        unknownVersion[5] = 2;
        rewriteChecksum(unknownVersion);
        assertThrows(IllegalArgumentException.class,
                () -> RunSubmissionCodec.decode(unknownVersion));

        byte[] nonZeroReserved = encoded.clone();
        nonZeroReserved[6] = 1;
        rewriteChecksum(nonZeroReserved);
        assertThrows(IllegalArgumentException.class,
                () -> RunSubmissionCodec.decode(nonZeroReserved));

        byte[] impossibleBody = encoded.clone();
        impossibleBody[8] = 0x7f;
        impossibleBody[9] = (byte) 0xff;
        impossibleBody[10] = (byte) 0xff;
        impossibleBody[11] = (byte) 0xff;
        rewriteChecksum(impossibleBody);
        assertThrows(IllegalArgumentException.class,
                () -> RunSubmissionCodec.decode(impossibleBody));
    }

    @Test
    void residentCallbackCoversEnvelopeCopiesStringsArtifactAndGetterClone() {
        byte[] artifact = new byte[257];
        byte[] encoded = RunSubmissionCodec.encode(
                new RunSubmission("run", "attempt", "publication", 2, 0L, 1L), artifact);
        AtomicLong charged = new AtomicLong();

        RunSubmissionCodec.Decoded decoded = RunSubmissionCodec.decode(encoded, charged::addAndGet);
        long beforeGetter = charged.get();
        decoded.artifactBytes();

        assertEquals(artifact.length, charged.get() - beforeGetter);
        org.junit.jupiter.api.Assertions.assertTrue(beforeGetter > encoded.length,
                "body/checksum/hash/text/artifact copies must all be charged");
    }

    private static void rewriteChecksum(byte[] frame) throws Exception {
        int checksumLength = 32;
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(Arrays.copyOf(frame, frame.length - checksumLength));
        System.arraycopy(digest, 0, frame, frame.length - checksumLength, checksumLength);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
