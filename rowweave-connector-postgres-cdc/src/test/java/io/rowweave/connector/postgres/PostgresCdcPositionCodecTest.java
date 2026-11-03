package io.rowweave.connector.postgres;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresCdcPositionCodecTest {
    @Test
    void positionBindsClusterDatabaseBoundaryAndUnsignedLsnCanonically() {
        PostgresCdcPosition handoff = PostgresCdcPosition.snapshotHandoff(
                "18446744073709551615", 4_294_967_295L,
                "FFFFFFFF/FFFFFFFF");
        byte[] handoffFrame = PostgresCdcPositionCodec.encode(handoff);
        PostgresCdcPosition restored =
                PostgresCdcPositionCodec.decode(handoffFrame);

        assertEquals(PostgresCdcPosition.Boundary.SNAPSHOT_HANDOFF,
                restored.boundary());
        assertEquals("18446744073709551615",
                restored.databaseSystemIdentifier());
        assertEquals(4_294_967_295L, restored.databaseOid());
        assertEquals("FFFFFFFF/FFFFFFFF", restored.endLsn());
        assertArrayEquals(handoffFrame,
                PostgresCdcPositionCodec.encode(restored));
        assertThrows(IllegalStateException.class, restored::transactionId);

        PostgresCdcPosition commit = PostgresCdcPosition.transactionCommit(
                "18446744073709551615", 4_294_967_295L,
                "0/16B6D20", 4_294_967_295L);
        byte[] commitFrame = PostgresCdcPositionCodec.encode(commit);
        assertEquals("5257585047504f310000001d0000000101"
                        + "ffffffffffffffffffffffff00000000016b6d20ffffffff"
                        + "9c67331a5e5154f0d307c182bdd002ba2dd17602e0c5b552"
                        + "41c0d7ab870e2400",
                hex(commitFrame));
        PostgresCdcPosition restoredCommit =
                PostgresCdcPositionCodec.decode(commitFrame);
        assertEquals(PostgresCdcPosition.Boundary.TRANSACTION_COMMIT,
                restoredCommit.boundary());
        assertEquals("0/16B6D20", restoredCommit.endLsn());
        assertEquals(4_294_967_295L, restoredCommit.transactionId());
        assertNotEquals(Arrays.toString(handoffFrame),
                Arrays.toString(commitFrame));

        assertInvalid(() -> PostgresCdcPosition.snapshotHandoff(
                "01", 1L, "0/1"));
        assertInvalid(() -> PostgresCdcPosition.snapshotHandoff(
                "1", 1L, "00/1"));
        assertInvalid(() -> PostgresCdcPosition.transactionCommit(
                "1", 1L, "0/1", 0L));
        assertCorrupt(commitFrame);
    }

    private static void assertCorrupt(byte[] frame) {
        byte[] corrupt = frame.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertInvalid(() -> PostgresCdcPositionCodec.decode(corrupt));
        assertInvalid(() -> PostgresCdcPositionCodec.decode(
                Arrays.copyOf(frame, frame.length - 1)));
        byte[] trailing = Arrays.copyOf(frame, frame.length + 1);
        assertInvalid(() -> PostgresCdcPositionCodec.decode(trailing));
    }

    private static void assertInvalid(Runnable action) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class, action::run);
        assertTrue(failure.getMessage().startsWith(
                "POSTGRES_CDC_POSITION_"), failure.getMessage());
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte item : value) {
            result.append(String.format("%02x", item));
        }
        return result.toString();
    }
}
