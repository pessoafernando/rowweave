package io.rowweave;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical bounded codec for {@link BatchSinkDescriptor}; never uses Java serialization. */
public final class BatchSinkDescriptorCodec {

    public static final int MAX_FRAME_BYTES = 4 * 1024;

    private static final int MAGIC = 0x5257534b; // RWSK
    private static final int CODEC_VERSION = 2;
    private static final int DIGEST_BYTES = 32;
    private static final int MIN_FRAME_BYTES = 4 + 4 + 4 + 4 + 1 + 4
            + 4 + 64 + 4 + 1 + 4 + 1 + 1 + 4 + 1 + DIGEST_BYTES;

    private BatchSinkDescriptorCodec() {
    }

    public static byte[] encode(BatchSinkDescriptor descriptor) {
        if (descriptor == null) {
            throw new IllegalArgumentException("batch sink descriptor is required");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(512);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(CODEC_VERSION);
            out.writeInt(descriptor.descriptorVersion());
            writeText(out, descriptor.connectorType());
            out.writeInt(descriptor.connectorVersion());
            writeText(out, descriptor.schemaFingerprint());
            writeText(out, descriptor.destinationId());
            writeText(out, descriptor.commitDomainId());
            out.writeByte(descriptor.capabilityProfile().wireId());
            writeText(out, descriptor.bindingIdentity());
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        byte[] body = bytes.toByteArray();
        if (body.length > MAX_FRAME_BYTES - DIGEST_BYTES) {
            throw new IllegalArgumentException("batch sink descriptor frame length exceeds limit");
        }
        byte[] checksum = sha256(body, 0, body.length);
        byte[] frame = Arrays.copyOf(body, body.length + checksum.length);
        System.arraycopy(checksum, 0, frame, body.length, checksum.length);
        return frame;
    }

    public static BatchSinkDescriptor decode(byte[] frame) {
        if (frame == null || frame.length < MIN_FRAME_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("batch sink descriptor frame length out of bounds");
        }
        int bodyLength = frame.length - DIGEST_BYTES;
        byte[] expected = sha256(frame, 0, bodyLength);
        byte[] actual = Arrays.copyOfRange(frame, bodyLength, frame.length);
        if (!MessageDigest.isEqual(expected, actual)) {
            throw new IllegalArgumentException(
                    "batch sink descriptor frame length or checksum mismatch");
        }
        try (DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(frame, 0, bodyLength))) {
            if (in.readInt() != MAGIC) {
                throw new IllegalArgumentException("batch sink descriptor magic mismatch");
            }
            int codecVersion = in.readInt();
            if (codecVersion != CODEC_VERSION) {
                throw new IllegalArgumentException(
                        "batch sink descriptor codec version is unsupported: " + codecVersion);
            }
            BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                    in.readInt(),
                    readText(in, "connectorType",
                            BatchSinkDescriptor.MAX_CONNECTOR_TYPE_BYTES),
                    in.readInt(),
                    readText(in, "schemaFingerprint", 64),
                    readText(in, "destinationId",
                            BatchSinkDescriptor.MAX_DESTINATION_ID_BYTES),
                    readText(in, "commitDomainId",
                            BatchSinkDescriptor.MAX_COMMIT_DOMAIN_ID_BYTES),
                    BatchSinkDescriptor.CapabilityProfile.fromWire(
                            in.readUnsignedByte()),
                    readText(in, "bindingIdentity",
                            BatchSinkDescriptor.MAX_BINDING_IDENTITY_BYTES));
            if (in.available() != 0) {
                throw new IllegalArgumentException(
                        "batch sink descriptor has trailing bytes");
            }
            return descriptor;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "batch sink descriptor field length is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "batch sink descriptor is malformed", malformed);
        }
    }

    public static byte[] digest(BatchSinkDescriptor descriptor) {
        byte[] frame = encode(descriptor);
        return sha256(frame, 0, frame.length);
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static String readText(DataInputStream in, String field, int maxBytes)
            throws IOException {
        int length = in.readInt();
        if (length < 1 || length > maxBytes || length > in.available()) {
            throw new IllegalArgumentException(
                    "batch sink " + field + " length out of bounds");
        }
        byte[] encoded = new byte[length];
        in.readFully(encoded);
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded));
            return decoded.toString();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(
                    "batch sink " + field + " is invalid UTF-8", invalid);
        }
    }

    private static byte[] sha256(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
