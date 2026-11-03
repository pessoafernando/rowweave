package io.rowweave.upgrade;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StoreUpgradeContractCodecTest {

    @Test
    void headerAndRichAuthorityRecordHaveCanonicalChecksummedFrames() {
        StoreFormatHeader active = new StoreFormatHeader(
                1, "primary-store", 1, 0, repeat('a', 64));
        StoreFormatHeader target = new StoreFormatHeader(
                1, "primary-store", 1, 2, repeat('b', 64));
        UpgradeAuthorityRecord record = new UpgradeAuthorityRecord(
                1, active, target, 9L, UpgradePhase.VERIFIED,
                "namespace-legacy", repeat('c', 64),
                "namespace-shadow-9", repeat('d', 64),
                new UpgradeOwner(17L, "master-a", "incarnation-a", 23L),
                41L, "namespace-legacy", 41L, 7L,
                120L, 16, repeat('e', 64), repeat('e', 64),
                2_000_000L, false, false,
                Collections.singletonList(new UpgradeCommandEvidence(
                        "verify-9", repeat('f', 64))));

        byte[] headerFrame = StoreFormatHeaderCodec.encode(active);
        byte[] authorityFrame = UpgradeAuthorityRecordCodec.encode(record);

        assertEquals(active, StoreFormatHeaderCodec.decode(headerFrame));
        assertEquals(record, UpgradeAuthorityRecordCodec.decode(authorityFrame));
        assertArrayEquals(headerFrame, StoreFormatHeaderCodec.encode(active));
        assertArrayEquals(authorityFrame,
                UpgradeAuthorityRecordCodec.encode(record));
        assertArrayEquals("RWXXSTH1".getBytes(StandardCharsets.US_ASCII),
                Arrays.copyOf(headerFrame, 8));
        assertArrayEquals("RWXXUPA1".getBytes(StandardCharsets.US_ASCII),
                Arrays.copyOf(authorityFrame, 8));
        assertFalse(containsJavaSerialization(headerFrame));
        assertFalse(containsJavaSerialization(authorityFrame));
    }

    @Test
    void everyUpgradePhaseHasAnExplicitStableWireId() {
        assertEquals(1, UpgradePhase.IDLE.wireId());
        assertEquals(2, UpgradePhase.QUIESCED.wireId());
        assertEquals(3, UpgradePhase.COPYING.wireId());
        assertEquals(4, UpgradePhase.VERIFIED.wireId());
        assertEquals(5, UpgradePhase.CUTOVER.wireId());
        assertEquals(6, UpgradePhase.CLEANUP_ELIGIBLE.wireId());
        assertEquals(7, UpgradePhase.FAILED_FROZEN.wireId());
    }

    @Test
    void corruptTruncatedOversizedAndUnknownFramesFailClosed() throws Exception {
        StoreFormatHeader header = new StoreFormatHeader(
                1, "primary-store", 1, 0, repeat('a', 64));
        byte[] encoded = StoreFormatHeaderCodec.encode(header);
        byte[] corrupt = encoded.clone();
        corrupt[20] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> StoreFormatHeaderCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> StoreFormatHeaderCodec.decode(
                        Arrays.copyOf(encoded, encoded.length - 1)));
        assertThrows(IllegalArgumentException.class,
                () -> StoreFormatHeaderCodec.decode(
                        new byte[StoreFormatHeaderCodec.MAX_FRAME_BYTES + 1]));

        byte[] unknown = encoded.clone();
        ByteBuffer.wrap(unknown).putInt(8, 2);
        resign(unknown);
        assertThrows(IllegalArgumentException.class,
                () -> StoreFormatHeaderCodec.decode(unknown));
    }

    @Test
    void impossiblePhaseAndSemanticVerificationShapesAreRejected() {
        StoreFormatHeader source = new StoreFormatHeader(
                1, "store", 1, 0, repeat('a', 64));
        StoreFormatHeader target = new StoreFormatHeader(
                1, "store", 1, 2, repeat('b', 64));
        UpgradeOwner owner = new UpgradeOwner(1L, "master", "inc", 1L);
        assertThrows(IllegalArgumentException.class, () ->
                new UpgradeAuthorityRecord(1, source, target, 1L,
                        UpgradePhase.VERIFIED, "legacy", repeat('c', 64),
                        "shadow", repeat('d', 64), owner, 0L, "legacy",
                        0L, 1L, 1L, 1, repeat('e', 64), repeat('e', 64),
                        2_000L, true, false, Collections.emptyList()));
        assertThrows(IllegalArgumentException.class, () ->
                new UpgradeAuthorityRecord(1, source, target, 1L,
                        UpgradePhase.VERIFIED, "legacy", repeat('c', 64),
                        "shadow", repeat('d', 64), owner, 0L, "legacy",
                        0L, 1L, 1L, 1, repeat('e', 64), repeat('f', 64),
                        2_000L, false, false, Collections.emptyList()));
    }

    private static void resign(byte[] frame) throws Exception {
        int bodyLength = ByteBuffer.wrap(frame).getInt(12);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(frame, 16, bodyLength);
        byte[] checksum = digest.digest();
        System.arraycopy(checksum, 0, frame, 16 + bodyLength,
                checksum.length);
    }

    private static boolean containsJavaSerialization(byte[] bytes) {
        for (int index = 0; index < bytes.length - 3; index++) {
            if (bytes[index] == (byte) 0xac && bytes[index + 1] == (byte) 0xed
                    && bytes[index + 2] == 0 && bytes[index + 3] == 5) {
                return true;
            }
        }
        return false;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
