package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.text.Normalizer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunRetentionModelCodecTest {

    @Test
    void ownerTokenAndLifecycleRefsRoundTripDeterministically() {
        RetentionOwner owner = new RetentionOwner("master-Ω", -9L);
        RetentionToken token = RetentionToken.create(
                "run-Φ", owner, 3L, 17L, 1_000L);
        byte[] encodedToken = RunRetentionCodec.encodeToken(token);
        assertArrayEquals(encodedToken, RunRetentionCodec.encodeToken(token));
        RetentionToken decodedToken = RunRetentionCodec.decodeToken(encodedToken);
        assertEquals(token, decodedToken);
        byte[] digest = decodedToken.tokenDigest();
        digest[0] ^= 1;
        assertFalse(Arrays.equals(digest, decodedToken.tokenDigest()));

        RunLifecycleRef active = RunLifecycleRef.active(
                "run-active", 4L, RunStatus.RUNNING, "artifact-a");
        RunLifecycleRef terminal = RunLifecycleRef.terminal(
                "run-terminal", 9L, RunStatus.SUCCEEDED, "artifact-b", 900L);
        RunLifecycleRef deleting = RunLifecycleRef.deleting(
                "run-Φ", 17L, RunStatus.FAILED, "artifact-c", 1_000L, token);
        assertRefRoundTrip(active);
        assertRefRoundTrip(terminal);
        assertRefRoundTrip(deleting);
        assertNull(active.retentionToken());
        assertEquals(token, deleting.retentionToken());
    }

    @Test
    void modelRejectsInvalidStateCombinationsAndText() {
        assertThrows(IllegalArgumentException.class,
                () -> new RetentionOwner("", 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new RetentionOwner("master", 0L));
        assertThrows(IllegalArgumentException.class, () -> new RetentionOwner(
                Normalizer.normalize("e\u0301", Normalizer.Form.NFD), 1L));
        RetentionOwner owner = new RetentionOwner("master", 1L);
        assertThrows(IllegalArgumentException.class,
                () -> RetentionToken.create("run", owner, 0L, 1L, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> RetentionToken.create("run", owner, 1L, 0L, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> RetentionToken.create("run", owner, 1L, 1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> RunLifecycleRef.active(
                "run", 1L, RunStatus.SUCCEEDED, "artifact"));
        assertThrows(IllegalArgumentException.class, () -> RunLifecycleRef.terminal(
                "run", 1L, RunStatus.RUNNING, "artifact", 1L));
        assertThrows(IllegalArgumentException.class, () -> RunLifecycleRef.deleting(
                "other-run", 1L, RunStatus.FAILED, "artifact", 1L,
                RetentionToken.create("run", owner, 1L, 1L, 1L)));
    }

    @Test
    void codecRejectsCorruptionTruncationAndTrailingBytes() {
        RetentionToken token = RetentionToken.create(
                "run", new RetentionOwner("master", 1L), 1L, 3L, 5L);
        byte[] encodedToken = RunRetentionCodec.encodeToken(token);
        assertMalformedToken(mutate(encodedToken, 8));
        assertMalformedToken(Arrays.copyOf(encodedToken, encodedToken.length - 1));
        assertMalformedToken(Arrays.copyOf(encodedToken, encodedToken.length + 1));

        byte[] encodedRef = RunRetentionCodec.encodeRef(RunLifecycleRef.deleting(
                "run", 3L, RunStatus.CANCELED, "artifact", 5L, token));
        assertThrows(IllegalArgumentException.class,
                () -> RunRetentionCodec.decodeRef(mutate(encodedRef, 12)));
        assertThrows(IllegalArgumentException.class,
                () -> RunRetentionCodec.decodeRef(
                        Arrays.copyOf(encodedRef, encodedRef.length - 1)));
        assertThrows(IllegalArgumentException.class,
                () -> RunRetentionCodec.decodeRef(
                        Arrays.copyOf(encodedRef, encodedRef.length + 1)));

        byte[] unknownState = encodedRef.clone();
        unknownState[5] = (byte) 127;
        refreshChecksum(unknownState);
        assertThrows(IllegalArgumentException.class,
                () -> RunRetentionCodec.decodeRef(unknownState));

        byte[] malformedUtf8 = RunRetentionCodec.encodeToken(RetentionToken.create(
                "rr", new RetentionOwner("master", 1L), 1L, 3L, 5L));
        malformedUtf8[7] = (byte) 0xc3;
        malformedUtf8[8] = (byte) 0x28;
        refreshChecksum(malformedUtf8);
        assertThrows(IllegalArgumentException.class,
                () -> RunRetentionCodec.decodeToken(malformedUtf8));
    }

    @Test
    void claimAndProgressExposeOnlyValidClosedResults() {
        RetentionToken token = RetentionToken.create(
                "run", new RetentionOwner("master", 1L), 1L, 2L, 3L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED,
                RunRetentionClaim.acquired(token).code());
        assertEquals(token, RunRetentionClaim.replayed(token).token());
        assertNull(RunRetentionClaim.notDue().token());
        assertNull(RunRetentionClaim.staleRun().token());
        assertNull(RunRetentionClaim.lostRace().token());

        assertEquals(7, RunPurgeProgress.inProgress(7).deletedRecords());
        assertEquals(0, RunPurgeProgress.purged().deletedRecords());
        assertEquals(RunPurgeProgress.Code.STALE_OWNER,
                RunPurgeProgress.staleOwner().code());
        assertEquals(RunPurgeProgress.Code.CORRUPT,
                RunPurgeProgress.corrupt().code());
        assertThrows(IllegalArgumentException.class,
                () -> RunPurgeProgress.inProgress(-1));
        assertThrows(IllegalArgumentException.class,
                () -> RunPurgeProgress.inProgress(1001));
    }

    private static void assertRefRoundTrip(RunLifecycleRef ref) {
        byte[] encoded = RunRetentionCodec.encodeRef(ref);
        assertArrayEquals(encoded, RunRetentionCodec.encodeRef(ref));
        RunLifecycleRef decoded = RunRetentionCodec.decodeRef(encoded);
        assertEquals(ref.runId(), decoded.runId());
        assertEquals(ref.revision(), decoded.revision());
        assertEquals(ref.status(), decoded.status());
        assertEquals(ref.artifactId(), decoded.artifactId());
        assertEquals(ref.state(), decoded.state());
        assertEquals(ref.terminalAtMillis(), decoded.terminalAtMillis());
        assertEquals(ref.retentionToken(), decoded.retentionToken());
    }

    private static byte[] mutate(byte[] encoded, int index) {
        byte[] corrupt = encoded.clone();
        corrupt[index] ^= 1;
        return corrupt;
    }

    private static void assertMalformedToken(byte[] encoded) {
        assertThrows(IllegalArgumentException.class,
                () -> RunRetentionCodec.decodeToken(encoded));
    }

    private static void refreshChecksum(byte[] encoded) {
        int bodyLength = encoded.length - 32;
        try {
            byte[] checksum = MessageDigest.getInstance("SHA-256")
                    .digest(Arrays.copyOf(encoded, bodyLength));
            System.arraycopy(checksum, 0, encoded, bodyLength, checksum.length);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
