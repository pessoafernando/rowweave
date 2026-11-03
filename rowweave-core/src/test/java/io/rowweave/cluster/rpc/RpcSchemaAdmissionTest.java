package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcSchemaAdmissionTest {

    private static final String SET_A = repeat('a', 64);
    private static final String SET_B = repeat('b', 64);

    @Test
    void acceptsEveryCompleteHistoricalLevel() throws Exception {
        for (int level = 0; level <= 8; level++) {
            RpcMessage decoded = decode(legacyFrame(level));
            assertEquals(RpcMessage.Type.ACK, decoded.type(), "L" + level);
        }
    }

    @Test
    void rejectsManifestAndFenceGroupsCutAtFieldBoundaries() throws Exception {
        IOException partialManifest = assertThrows(IOException.class,
                () -> decode(frame(payloadWithPartialManifestGroup())));
        assertTrue(partialManifest.getMessage().contains("manifest"), partialManifest.getMessage());

        IOException partialFence = assertThrows(IOException.class,
                () -> decode(frame(payloadWithPartialRunFenceGroup())));
        assertTrue(partialFence.getMessage().contains("run-fence"), partialFence.getMessage());
    }

    @Test
    void rejectsUnknownTypeMalformedUtf8AndUnknownTrailingBytes() throws Exception {
        byte[] unknownType = legacyFrame(0);
        ByteBuffer.wrap(unknownType, Integer.BYTES, Integer.BYTES).putInt(Integer.MAX_VALUE);
        assertThrows(IOException.class, () -> decode(unknownType));

        byte[] malformed = legacyFrame(0);
        // frame(4) + type(4) + workerId length(4), followed by one malformed UTF-8 byte.
        ByteBuffer.wrap(malformed, 8, 4).putInt(1);
        malformed[12] = (byte) 0x80;
        assertThrows(IOException.class, () -> decode(malformed));

        byte[] current = legacyFrame(8);
        byte[] trailing = Arrays.copyOf(current, current.length + 1);
        ByteBuffer.wrap(trailing).putInt(ByteBuffer.wrap(current).getInt() + 1);
        trailing[trailing.length - 1] = 7;
        assertThrows(IOException.class, () -> decode(trailing));
    }

    @Test
    void completeSecureSuffixRoundTripsAndRejectsVersionIdentityOrTrailingMutation() throws Exception {
        RpcSecurityMetadata metadata = RpcSecurityMetadata.of(SET_A, SET_B);
        RpcMessage secured = RpcMessage.ack().withSecurityMetadata(metadata);
        byte[] encoded = RpcCodec.encode(secured);

        RpcMessage decoded = decode(encoded);
        assertNotNull(decoded.securityMetadata());
        assertEquals(SET_A, decoded.securityMetadata().deploymentSetId());
        assertEquals(SET_B, decoded.securityMetadata().authzSetId());

        int suffixStart = encoded.length - RpcCodec.SECURE_SUFFIX_BYTES;
        byte[] unknownVersion = encoded.clone();
        ByteBuffer.wrap(unknownVersion, suffixStart + RpcCodec.SECURE_MAGIC_BYTES, Integer.BYTES)
                .putInt(99);
        assertThrows(IOException.class, () -> decode(unknownVersion));

        byte[] invalidIdentity = encoded.clone();
        invalidIdentity[suffixStart + RpcCodec.SECURE_MAGIC_BYTES + Integer.BYTES] = 'A';
        assertThrows(IOException.class, () -> decode(invalidIdentity));

        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        ByteBuffer.wrap(trailing).putInt(ByteBuffer.wrap(encoded).getInt() + 1);
        assertThrows(IOException.class, () -> decode(trailing));
    }

    @Test
    void normalControlTypeCannotBorrowLargeUnionBlobAllowance() {
        byte[] unrelatedPlan = new byte[2 * 1024 * 1024];
        RpcMessage malformedAck = new RpcMessage(
                RpcMessage.Type.ACK, null, null, 0, 0, unrelatedPlan,
                0L, null, true, 0L, null);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> RpcCodec.encode(malformedAck));
        assertTrue(failure.getMessage().contains("ACK"), failure.getMessage());
    }

    private static RpcMessage decode(byte[] frame) throws IOException {
        return RpcCodec.decode(new DataInputStream(new ByteArrayInputStream(frame)));
    }

    private static byte[] legacyFrame(int level) throws IOException {
        ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(payloadBytes)) {
            writeBase(out);
            writeTail(out, level);
        }
        return frame(payloadBytes.toByteArray());
    }

    private static byte[] payloadWithPartialManifestGroup() throws IOException {
        ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(payloadBytes)) {
            writeBase(out);
            writeTail(out, 5);
            writeString(out, "attempt-only");
        }
        return payloadBytes.toByteArray();
    }

    private static byte[] payloadWithPartialRunFenceGroup() throws IOException {
        ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(payloadBytes)) {
            writeBase(out);
            writeTail(out, 6);
            writeString(out, null);
        }
        return payloadBytes.toByteArray();
    }

    private static void writeBase(DataOutputStream out) throws IOException {
        out.writeInt(RpcMessage.Type.ACK.ordinal());
        writeString(out, null);
        writeString(out, null);
        out.writeInt(0);
        out.writeInt(0);
        writeBytes(out, null);
        out.writeLong(0L);
        writeBytes(out, null);
        out.writeBoolean(true);
        out.writeLong(0L);
        writeString(out, null);
    }

    private static void writeTail(DataOutputStream out, int level) throws IOException {
        if (level >= 1) out.writeLong(0L);
        if (level >= 2) out.writeLong(-1L);
        if (level >= 3) out.writeInt(-1);
        if (level >= 4) writeString(out, null);
        if (level >= 5) writeString(out, null);
        if (level >= 6) {
            writeString(out, null);
            out.writeLong(0L);
            writeBytes(out, null);
            writeString(out, null);
            writeString(out, null);
            writeString(out, null);
        }
        if (level >= 7) {
            writeString(out, null);
            writeString(out, null);
            out.writeLong(0L);
        }
        if (level >= 8) writeString(out, null);
    }

    private static byte[] frame(byte[] payload) throws IOException {
        ByteArrayOutputStream framed = new ByteArrayOutputStream(payload.length + Integer.BYTES);
        try (DataOutputStream out = new DataOutputStream(framed)) {
            out.writeInt(payload.length);
            out.write(payload);
        }
        return framed.toByteArray();
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(value.length);
        out.write(value);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}

