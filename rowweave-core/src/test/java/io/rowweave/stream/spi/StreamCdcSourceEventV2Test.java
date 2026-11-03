package io.rowweave.stream.spi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamCdcSourceEventV2Test {

    @Test
    void atomicSnapshotHandoffCarriesExactExternalPosition() {
        byte[] position = bytes("0/10");
        StreamCdcSourceEvent event = StreamCdcSourceEvent
                .snapshotHandoffComplete("snapshot-all", position);

        position[0] = 'X';
        assertEquals(StreamCdcSourceEvent.Kind.SNAPSHOT_HANDOFF_COMPLETE,
                event.kind());
        assertEquals("snapshot-all", event.splitId());
        assertTrue(event.hasCompletedExternalPosition());
        assertArrayEquals(bytes("0/10"),
                event.completedExternalPositionFrame());
    }

    @Test
    void logEventDistinguishesIntermediateAndCompletedTransactionRows() {
        StreamCdcSourceEvent intermediate = StreamCdcSourceEvent.logRecord(
                1L, bytes("upsert-1"), null);
        StreamCdcSourceEvent completed = StreamCdcSourceEvent.logRecord(
                2L, bytes("upsert-2"), bytes("0/20"));

        assertFalse(intermediate.hasCompletedExternalPosition());
        assertThrows(IllegalStateException.class,
                intermediate::completedExternalPositionFrame);
        assertTrue(completed.hasCompletedExternalPosition());
        assertArrayEquals(bytes("0/20"),
                completed.completedExternalPositionFrame());

        StreamCdcSourceEvent position = StreamCdcSourceEvent
                .logPosition(bytes("0/30"));
        assertEquals(StreamCdcSourceEvent.Kind.LOG_POSITION,
                position.kind());
        assertTrue(position.hasCompletedExternalPosition());
        assertArrayEquals(bytes("0/30"),
                position.completedExternalPositionFrame());
        assertEquals(null, position.upsertFrame());
    }

    @Test
    void externalPositionRejectsEmptyAndLimitPlusOne() {
        assertThrows(IllegalArgumentException.class,
                () -> StreamCdcSourceEvent.snapshotHandoffComplete(
                        "snapshot-all", new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> StreamCdcSourceEvent.snapshotHandoffComplete(
                        "snapshot-all", new byte[64 * 1024 + 1]));
        assertThrows(IllegalArgumentException.class,
                () -> StreamCdcSourceEvent.logRecord(
                        1L, bytes("upsert"), new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> StreamCdcSourceEvent.logPosition(new byte[0]));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
