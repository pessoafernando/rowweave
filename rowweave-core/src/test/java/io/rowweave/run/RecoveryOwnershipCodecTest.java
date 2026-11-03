package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.text.Normalizer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecoveryOwnershipCodecTest {

    @Test
    void ownerAndTokenRoundTripDeterministicallyWithDefensiveDigest() {
        RecoveryOwner owner = new RecoveryOwner("master-Ω", -7L);
        byte[] encodedOwner = RecoveryOwnershipCodec.encodeOwner(owner);
        assertArrayEquals(encodedOwner, RecoveryOwnershipCodec.encodeOwner(owner));
        RecoveryOwner decodedOwner = RecoveryOwnershipCodec.decodeOwner(encodedOwner);
        assertEquals(owner.ownerId(), decodedOwner.ownerId());
        assertEquals(owner.leaderTermToken(), decodedOwner.leaderTermToken());

        RecoveryToken token = RecoveryToken.create("run-Φ", owner, 3L, 11L);
        byte[] encoded = RecoveryOwnershipCodec.encodeToken(token);
        assertArrayEquals(encoded, RecoveryOwnershipCodec.encodeToken(token));
        RecoveryToken decoded = RecoveryOwnershipCodec.decodeToken(encoded);
        assertEquals(token.runId(), decoded.runId());
        assertEquals(token.owner(), decoded.owner());
        assertEquals(3L, decoded.ownershipEpoch());
        assertEquals(11L, decoded.claimedRevision());
        assertArrayEquals(token.tokenDigest(), decoded.tokenDigest());
        byte[] digest = decoded.tokenDigest();
        digest[0] ^= 1;
        assertFalse(Arrays.equals(digest, decoded.tokenDigest()));
    }

    @Test
    void rejectsInvalidTextScalarsCorruptionAndTrailingBytes() {
        assertThrows(IllegalArgumentException.class, () -> new RecoveryOwner("", 1L));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryOwner("master", 0L));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryOwner(
                Normalizer.normalize("e\u0301", Normalizer.Form.NFD), 1L));
        RecoveryOwner owner = new RecoveryOwner("master", 1L);
        assertThrows(IllegalArgumentException.class,
                () -> RecoveryToken.create("run", owner, 0L, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> RecoveryToken.create("run", owner, 1L, 0L));

        byte[] ownerBytes = RecoveryOwnershipCodec.encodeOwner(owner);
        ownerBytes[8] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> RecoveryOwnershipCodec.decodeOwner(ownerBytes));

        byte[] token = RecoveryOwnershipCodec.encodeToken(
                RecoveryToken.create("run", owner, 1L, 1L));
        token[12] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> RecoveryOwnershipCodec.decodeToken(token));
        byte[] trailing = Arrays.copyOf(token, token.length + 1);
        assertThrows(IllegalArgumentException.class,
                () -> RecoveryOwnershipCodec.decodeToken(trailing));
    }
}
