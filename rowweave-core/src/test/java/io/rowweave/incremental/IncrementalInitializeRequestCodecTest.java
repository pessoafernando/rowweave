package io.rowweave.incremental;

import org.junit.jupiter.api.Test;

import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** feed bootstrap has one canonical bounded wire. */
class IncrementalInitializeRequestCodecTest {

    @Test
    void requestRoundTripsCanonicallyWithinTheNormalControlLane()
            throws Exception {
        IncrementalInitializeRequest request = request();

        byte[] encoded = IncrementalInitializeRequestCodec.encode(request);
        IncrementalInitializeRequest decoded =
                IncrementalInitializeRequestCodec.decode(encoded);

        assertTrue(encoded.length
                <= IncrementalInitializeRequestCodec.MAX_FRAME_BYTES);
        assertEquals(request.feedId(), decoded.feedId());
        assertEquals(request.feedIncarnation(), decoded.feedIncarnation());
        assertArrayEquals(request.definitionDigest(), decoded.definitionDigest());
        assertArrayEquals(request.sourceContractDigest(),
                decoded.sourceContractDigest());
        assertArrayEquals(request.frontierContractDigest(),
                decoded.frontierContractDigest());
        assertEquals(request.frontierKind(), decoded.frontierKind());
        assertArrayEquals(encoded,
                IncrementalInitializeRequestCodec.encode(decoded));
    }

    @Test
    void corruptTruncatedAndOversizeFramesFailClosed() throws Exception {
        byte[] encoded = IncrementalInitializeRequestCodec.encode(request());
        encoded[encoded.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalInitializeRequestCodec.decode(encoded));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalInitializeRequestCodec.decode(new byte[1]));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalInitializeRequestCodec.decode(new byte[
                        IncrementalInitializeRequestCodec.MAX_FRAME_BYTES + 1]));
    }

    private static IncrementalInitializeRequest request() throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return new IncrementalInitializeRequest(
                "feed-init", 7L,
                digest.digest("definition".getBytes("UTF-8")),
                digest.digest("source".getBytes("UTF-8")),
                digest.digest("frontier".getBytes("UTF-8")),
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
    }
}
