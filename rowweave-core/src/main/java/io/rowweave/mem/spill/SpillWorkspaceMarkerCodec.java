package io.rowweave.mem.spill;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** bounded canonical ASCII codec for spill ownership markers. */
final class SpillWorkspaceMarkerCodec {

    static final int MAX_ENCODED_BYTES = 512;
    private static final String MAGIC = "ROWWEAVE_SPILL_MARKER_V1";

    private SpillWorkspaceMarkerCodec() {
    }

    static byte[] encode(SpillWorkspaceMarker marker) {
        String instance = marker.instanceId() == null ? "-" : marker.instanceId();
        String job = marker.jobId() == null ? "-" : marker.jobId();
        String created = marker.createdAtMillis() < 0
                ? "-" : Long.toString(marker.createdAtMillis());
        String body = MAGIC + "|" + marker.kind().name()
                + "|" + instance + "|" + job + "|" + created;
        byte[] encoded = (body + "|" + sha256Hex(body.getBytes(StandardCharsets.US_ASCII)))
                .getBytes(StandardCharsets.US_ASCII);
        if (encoded.length > MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("spill marker exceeds encoded limit");
        }
        return encoded;
    }

    static SpillWorkspaceMarker decode(byte[] encoded) {
        try {
            if (encoded == null || encoded.length == 0 || encoded.length > MAX_ENCODED_BYTES) {
                throw new IllegalArgumentException("invalid marker length");
            }
            for (byte value : encoded) {
                int unsigned = value & 0xff;
                if (unsigned < 0x20 || unsigned > 0x7e) {
                    throw new IllegalArgumentException("marker is not printable ASCII");
                }
            }
            String text = new String(encoded, StandardCharsets.US_ASCII);
            String[] fields = text.split("\\|", -1);
            if (fields.length != 6 || !MAGIC.equals(fields[0])) {
                throw new IllegalArgumentException("invalid marker envelope");
            }
            String body = fields[0] + "|" + fields[1] + "|" + fields[2]
                    + "|" + fields[3] + "|" + fields[4];
            String expectedDigest = sha256Hex(body.getBytes(StandardCharsets.US_ASCII));
            if (!constantTimeAsciiEquals(expectedDigest, fields[5])) {
                throw new IllegalArgumentException("marker checksum mismatch");
            }
            SpillWorkspaceMarker marker = decodeFields(fields);
            if (!Arrays.equals(encoded, encode(marker))) {
                throw new IllegalArgumentException("marker is not canonical");
            }
            return marker;
        } catch (RuntimeException invalid) {
            if (invalid instanceof SpillDiskException) {
                throw invalid;
            }
            throw SpillDiskException.workspaceUnavailable(invalid);
        }
    }

    private static SpillWorkspaceMarker decodeFields(String[] fields) {
        SpillWorkspaceMarker.Kind kind = SpillWorkspaceMarker.Kind.valueOf(fields[1]);
        switch (kind) {
            case BASE:
                requireDash(fields[2]);
                requireDash(fields[3]);
                requireDash(fields[4]);
                return SpillWorkspaceMarker.base();
            case INSTANCE:
                requireDash(fields[3]);
                return SpillWorkspaceMarker.instance(fields[2], parseCanonicalMillis(fields[4]));
            case JOB:
                requireDash(fields[4]);
                return SpillWorkspaceMarker.job(fields[3], fields[2]);
            default:
                throw new IllegalArgumentException("unknown marker kind");
        }
    }

    private static long parseCanonicalMillis(String value) {
        if (value.isEmpty() || !value.matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException("invalid marker time");
        }
        return Long.parseLong(value);
    }

    private static void requireDash(String value) {
        if (!"-".equals(value)) {
            throw new IllegalArgumentException("unexpected marker field");
        }
    }

    private static boolean constantTimeAsciiEquals(String expected, String actual) {
        return MessageDigest.isEqual(expected.getBytes(StandardCharsets.US_ASCII),
                actual.getBytes(StandardCharsets.US_ASCII));
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            char[] chars = new char[digest.length * 2];
            final char[] hex = "0123456789abcdef".toCharArray();
            for (int i = 0; i < digest.length; i++) {
                int value = digest[i] & 0xff;
                chars[i * 2] = hex[value >>> 4];
                chars[i * 2 + 1] = hex[value & 0x0f];
            }
            return new String(chars);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
