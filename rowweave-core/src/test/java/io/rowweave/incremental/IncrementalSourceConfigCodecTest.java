package io.rowweave.incremental;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** source credentials remain Worker-only canonical bytes on RPC. */
class IncrementalSourceConfigCodecTest {

    @Test
    void roundTripIsCanonicalAndRetainsExactProviderBinding()
            throws Exception {
        IncrementalSourceConfig config = new IncrementalSourceConfig(
                new IncrementalSourceProviderKey("embedded.cdc", 3),
                digest("contract"), bytes("opaque-configuration"));

        byte[] frame = IncrementalSourceConfigCodec.encode(config);
        IncrementalSourceConfig decoded =
                IncrementalSourceConfigCodec.decode(frame);

        assertEquals(config, decoded);
        assertArrayEquals(frame, IncrementalSourceConfigCodec.encode(decoded));
        assertArrayEquals(digest(frame),
                IncrementalSourceConfigCodec.digest(config));
    }

    @Test
    void checksumLengthAndTrailingMutationsFailClosed() throws Exception {
        IncrementalSourceConfig config = new IncrementalSourceConfig(
                new IncrementalSourceProviderKey("embedded.cdc", 1),
                digest("contract"), bytes("config"));
        byte[] canonical = IncrementalSourceConfigCodec.encode(config);
        byte[] tampered = canonical.clone();
        tampered[tampered.length - 1] ^= 1;
        byte[] trailing = java.util.Arrays.copyOf(
                canonical, canonical.length + 1);

        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceConfigCodec.decode(tampered));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceConfigCodec.decode(trailing));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceConfigCodec.decode(new byte[0]));
    }

    private static byte[] digest(String value) throws Exception {
        return digest(bytes(value));
    }

    private static byte[] digest(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
