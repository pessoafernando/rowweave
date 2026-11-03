package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runtime-generation launch metadata stays outside the signed Artifact. */
class StreamTaskLaunchPayloadTest {

    @Test
    void carriesBoundedParallelismAndRestoreCutWithLegacyCompatibility() {
        CheckpointIdentity checkpoint = new CheckpointIdentity(
                repeat('a'), repeat('b'), "run", "attempt-1",
                3L, 2L, "master", 7L, repeat('c'), repeat('d'),
                1, 1);
        byte[] checkpointFrame =
                StreamCheckpointControlCodec.encode(checkpoint);

        StreamTaskLaunchPayload start = StreamTaskLaunchPayload.decode(
                StreamTaskLaunchPayload.start(9L, 4,
                        Collections.singletonMap("source", 3)),
                StreamControlMessage.Kind.START);
        assertEquals(9L, start.channelEpoch());
        assertEquals(4, start.parallelismOverride());
        assertEquals(Collections.singletonMap("source", 3),
                start.nodeMinimumParallelism());
        assertFalse(start.restoring());

        StreamTaskLaunchPayload restore = StreamTaskLaunchPayload.decode(
                StreamTaskLaunchPayload.restore(10L, 2, checkpointFrame),
                StreamControlMessage.Kind.RESTORE);
        assertEquals(10L, restore.channelEpoch());
        assertEquals(2, restore.parallelismOverride());
        assertTrue(restore.restoring());
        assertArrayEquals(checkpointFrame, restore.checkpointFrame());

        StreamTaskLaunchPayload legacyStart = StreamTaskLaunchPayload.decode(
                ByteBuffer.allocate(8).putLong(5L).array(),
                StreamControlMessage.Kind.START);
        assertEquals(0, legacyStart.parallelismOverride());
        assertFalse(legacyStart.restoring());

        StreamTaskLaunchPayload versionOneStart =
                StreamTaskLaunchPayload.decode(
                        ByteBuffer.allocate(24).putLong(8L)
                                .putInt(0x52575232).putInt(1)
                                .putInt(3).putInt(0).array(),
                        StreamControlMessage.Kind.START);
        assertEquals(3, versionOneStart.parallelismOverride());
        assertTrue(versionOneStart.nodeMinimumParallelism().isEmpty());

        ByteBuffer legacyRestore = ByteBuffer.allocate(
                8 + checkpointFrame.length).putLong(6L)
                .put(checkpointFrame);
        assertArrayEquals(checkpointFrame,
                StreamTaskLaunchPayload.decode(legacyRestore.array(),
                        StreamControlMessage.Kind.RESTORE).checkpointFrame());
        assertThrows(IllegalArgumentException.class, () ->
                StreamTaskLaunchPayload.decode(
                        StreamTaskLaunchPayload.start(1L, 4),
                        StreamControlMessage.Kind.RESTORE));
    }

    private static String repeat(char value) {
        char[] text = new char[64];
        java.util.Arrays.fill(text, value);
        return new String(text);
    }
}
