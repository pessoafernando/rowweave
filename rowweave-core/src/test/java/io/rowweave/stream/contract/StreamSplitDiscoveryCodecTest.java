package io.rowweave.stream.contract;

import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Dynamic discovery crosses the Worker control plane as a bounded frame. */
class StreamSplitDiscoveryCodecTest {

    @Test
    void canonicalFrameRoundTripsAndRejectsCorruption() {
        StreamSplitDiscovery discovery = new StreamSplitDiscovery(9L,
                StreamDiscoveryCursor.of(new byte[] {1, 2}), Arrays.asList(
                StreamSplitChange.remove(repeat('b'), 3L),
                StreamSplitChange.add(new StreamSplitSpec(
                        repeat('a'), "position-v1", new byte[] {4}), 2L)));

        byte[] frame = StreamSplitDiscoveryCodec.encode(discovery,
                8, 16, 16);
        assertEquals(discovery, StreamSplitDiscoveryCodec.decode(frame,
                8, 16, 16));

        frame[frame.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> StreamSplitDiscoveryCodec.decode(frame, 8, 16, 16));
        assertThrows(IllegalArgumentException.class,
                () -> StreamSplitDiscoveryCodec.encode(discovery,
                        1, 16, 16));
    }

    private static String repeat(char value) {
        char[] result = new char[64];
        Arrays.fill(result, value);
        return new String(result);
    }
}
