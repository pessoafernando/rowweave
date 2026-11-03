package io.rowweave.mem.spill;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpillWorkspaceMarkerCodecTest {

    private static final String INSTANCE = "0123456789abcdef0123456789abcdef";
    private static final String JOB = "fedcba9876543210fedcba9876543210";

    @Test
    void baseInstanceAndJobMarkersRoundTripCanonically() {
        assertCanonicalRoundTrip(SpillWorkspaceMarker.base());
        assertCanonicalRoundTrip(SpillWorkspaceMarker.instance(INSTANCE, 1_725_404_400_123L));
        assertCanonicalRoundTrip(SpillWorkspaceMarker.job(JOB, INSTANCE));
    }

    @Test
    void markerEncodingIsBoundedAsciiAndContainsNoBusinessIdentity() {
        byte[] encoded = SpillWorkspaceMarkerCodec.encode(
                SpillWorkspaceMarker.job(JOB, INSTANCE));

        assertTrue(encoded.length <= SpillWorkspaceMarkerCodec.MAX_ENCODED_BYTES);
        for (byte b : encoded) {
            assertTrue((b & 0xff) >= 0x20 && (b & 0xff) <= 0x7e,
                    "marker must use bounded printable ASCII");
        }
        String text = new String(encoded, StandardCharsets.US_ASCII);
        assertTrue(!text.contains("runId") && !text.contains("worker") && !text.contains("/"));
    }

    @Test
    void rejectsNonCanonicalIdentityNegativeTimeAndWrongRelationships() {
        assertThrows(IllegalArgumentException.class,
                () -> SpillWorkspaceMarker.instance(INSTANCE.toUpperCase(), 1));
        assertThrows(IllegalArgumentException.class,
                () -> SpillWorkspaceMarker.instance("abc", 1));
        assertThrows(IllegalArgumentException.class,
                () -> SpillWorkspaceMarker.instance(INSTANCE, -1));
        assertThrows(IllegalArgumentException.class,
                () -> SpillWorkspaceMarker.job(JOB, JOB));
    }

    @Test
    void rejectsCorruptionTruncationTrailingBytesAndOversizeBeforeAcceptance() {
        byte[] canonical = SpillWorkspaceMarkerCodec.encode(
                SpillWorkspaceMarker.instance(INSTANCE, 123));

        byte[] corrupt = canonical.clone();
        corrupt[10] ^= 1;
        assertThrows(SpillDiskException.class, () -> SpillWorkspaceMarkerCodec.decode(corrupt));
        assertThrows(SpillDiskException.class,
                () -> SpillWorkspaceMarkerCodec.decode(Arrays.copyOf(canonical, canonical.length - 1)));

        byte[] trailing = Arrays.copyOf(canonical, canonical.length + 1);
        trailing[trailing.length - 1] = 'x';
        assertThrows(SpillDiskException.class, () -> SpillWorkspaceMarkerCodec.decode(trailing));

        byte[] oversized = new byte[SpillWorkspaceMarkerCodec.MAX_ENCODED_BYTES + 1];
        Arrays.fill(oversized, (byte) 'a');
        assertThrows(SpillDiskException.class, () -> SpillWorkspaceMarkerCodec.decode(oversized));
    }

    @Test
    void decodeFailuresUseFixedCategoryAndDoNotEchoMarkerPayload() {
        byte[] hostile = "secret=/private/customer-ledger".getBytes(StandardCharsets.US_ASCII);

        SpillDiskException failure = assertThrows(
                SpillDiskException.class, () -> SpillWorkspaceMarkerCodec.decode(hostile));

        assertEquals(SpillFailureCode.SPILL_WORKSPACE_UNAVAILABLE, failure.code());
        assertTrue(!failure.getMessage().contains("secret"));
        assertTrue(!failure.getMessage().contains("customer-ledger"));
        assertTrue(failure.getMessage().length() <= 256);
    }

    private static void assertCanonicalRoundTrip(SpillWorkspaceMarker marker) {
        byte[] first = SpillWorkspaceMarkerCodec.encode(marker);
        SpillWorkspaceMarker decoded = SpillWorkspaceMarkerCodec.decode(first);
        assertEquals(marker, decoded);
        assertArrayEquals(first, SpillWorkspaceMarkerCodec.encode(decoded));
    }
}
