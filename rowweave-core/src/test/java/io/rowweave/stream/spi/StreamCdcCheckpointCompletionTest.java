package io.rowweave.stream.spi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamCdcCheckpointCompletionTest {

    @Test
    void completionIsBoundedImmutableAndIdentifiesExactDurableCut() {
        byte[] position = bytes("0/20");
        StreamCdcCheckpointCompletion completion =
                new StreamCdcCheckpointCompletion(7L, 11L, position);

        position[0] = 'X';
        assertEquals(7L, completion.checkpointSequence());
        assertEquals(11L, completion.lastLogSequence());
        assertArrayEquals(bytes("0/20"),
                completion.externalPositionFrame());
        byte[] returned = completion.externalPositionFrame();
        returned[0] = 'Y';
        assertArrayEquals(bytes("0/20"),
                completion.externalPositionFrame());
    }

    @Test
    void completionRejectsInvalidIdentityAndPositionBounds() {
        assertThrows(IllegalArgumentException.class,
                () -> new StreamCdcCheckpointCompletion(
                        0L, 1L, bytes("0/20")));
        assertThrows(IllegalArgumentException.class,
                () -> new StreamCdcCheckpointCompletion(
                        1L, -1L, bytes("0/20")));
        assertThrows(IllegalArgumentException.class,
                () -> new StreamCdcCheckpointCompletion(
                        1L, 1L, new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> new StreamCdcCheckpointCompletion(
                        1L, 1L, new byte[64 * 1024 + 1]));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
