package io.rowweave.cluster.rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Canonical bounded frame for {@link ProtocolHelloV1}. */
public final class ProtocolHelloV1Codec {
    public static final int MAX_FRAME_BYTES = 256 * 1024;
    private static final byte[] MAGIC = "RWXXCTH1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int HEADER_BYTES = MAGIC.length + 2 * Integer.BYTES;

    private ProtocolHelloV1Codec() { }

    public static byte[] encode(ProtocolHelloV1 hello) {
        if (hello == null) throw new IllegalArgumentException("protocol hello is required");
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.write(fromHex(hello.manifestDigest()));
            body.writeInt(hello.protocolEpoch());
            writeRange(body, hello.readerRevisions());
            writeRange(body, hello.writerRevisions());
            writeInts(body, hello.messageIds());
            writeInts(body, hello.artifactFormats());
            writeInts(body, hello.stageCommandVersions());
            writeInts(body, hello.dataProtocolVersions());
            writeInts(body, hello.deploymentAbis());
            writeTexts(body, hello.runtimeCapabilities());
            writeInts(body, hello.requiredExtensionIds());
            Map<Integer, byte[]> extensions = hello.extensionValues();
            body.writeInt(extensions.size());
            for (Map.Entry<Integer, byte[]> entry : extensions.entrySet()) {
                body.writeInt(entry.getKey());
                body.writeInt(entry.getValue().length);
                body.write(entry.getValue());
            }
            body.writeInt(hello.maximumFrameBytes());
            body.writeLong(hello.maximumResidentBytes());
            body.close();
            byte[] payload = bodyBytes.toByteArray();
            long frameLength = HEADER_BYTES + (long) payload.length + DIGEST_BYTES;
            if (frameLength > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("protocol hello exceeds frame limit");
            }
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream((int) frameLength);
            DataOutputStream frame = new DataOutputStream(frameBytes);
            frame.write(MAGIC);
            frame.writeInt(VERSION);
            frame.writeInt(payload.length);
            frame.write(payload);
            frame.write(sha256(payload));
            frame.close();
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory protocol hello encode failed", impossible);
        }
    }

    public static void encodeTo(ProtocolHelloV1 hello, OutputStream target)
            throws IOException {
        if (target == null) throw new IllegalArgumentException("hello target is required");
        target.write(encode(hello));
    }

    public static ProtocolHelloV1 decodeFrom(DataInputStream input) throws IOException {
        if (input == null) throw new IllegalArgumentException("hello input is required");
        byte[] header = new byte[HEADER_BYTES];
        input.readFully(header);
        DataInputStream headerInput = new DataInputStream(
                new ByteArrayInputStream(header));
        byte[] magic = new byte[MAGIC.length];
        headerInput.readFully(magic);
        if (!MessageDigest.isEqual(MAGIC, magic)) {
            throw new IOException("protocol hello magic is invalid");
        }
        if (headerInput.readInt() != VERSION) {
            throw new IOException("protocol hello version is unsupported");
        }
        int bodyLength = headerInput.readInt();
        long frameLength = HEADER_BYTES + (long) bodyLength + DIGEST_BYTES;
        if (bodyLength < 0 || frameLength > MAX_FRAME_BYTES) {
            throw new IOException("protocol hello body length is invalid");
        }
        byte[] frame = new byte[(int) frameLength];
        System.arraycopy(header, 0, frame, 0, header.length);
        input.readFully(frame, header.length, frame.length - header.length);
        try {
            return decode(frame);
        } catch (IllegalArgumentException malformed) {
            throw new IOException("protocol hello frame is invalid", malformed);
        }
    }

    public static ProtocolHelloV1 decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("protocol hello frame length is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(frame));
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!MessageDigest.isEqual(MAGIC, magic)) fail("protocol hello magic is invalid");
            if (input.readInt() != VERSION) fail("protocol hello version is unsupported");
            int bodyLength = input.readInt();
            if (bodyLength < 0
                    || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES != frame.length) {
                fail("protocol hello frame length does not match header");
            }
            byte[] bodyBytes = new byte[bodyLength];
            input.readFully(bodyBytes);
            byte[] expectedDigest = new byte[DIGEST_BYTES];
            input.readFully(expectedDigest);
            if (!MessageDigest.isEqual(sha256(bodyBytes), expectedDigest)) {
                fail("protocol hello checksum mismatch");
            }
            DataInputStream body = new DataInputStream(new ByteArrayInputStream(bodyBytes));
            byte[] manifestDigest = new byte[DIGEST_BYTES];
            body.readFully(manifestDigest);
            int epoch = body.readInt();
            ProtocolRevisionRange readers = readRange(body);
            ProtocolRevisionRange writers = readRange(body);
            List<Integer> messageIds = readInts(body, true);
            List<Integer> artifactFormats = readInts(body, true);
            List<Integer> stageVersions = readInts(body, true);
            List<Integer> dataVersions = readInts(body, true);
            List<Integer> deploymentAbis = readInts(body, true);
            List<String> capabilities = readTexts(body);
            List<Integer> requiredExtensions = readInts(body, false);
            int extensionCount = readCount(body, false);
            Map<Integer, byte[]> extensions = new LinkedHashMap<>();
            int previous = 0;
            for (int index = 0; index < extensionCount; index++) {
                int id = body.readInt();
                int length = body.readInt();
                if (id <= previous || length < 0
                        || length > ProtocolHelloV1.MAX_EXTENSION_BYTES
                        || length > body.available()) fail("protocol extension is invalid");
                byte[] value = new byte[length];
                body.readFully(value);
                extensions.put(id, value);
                previous = id;
            }
            int maximumFrameBytes = body.readInt();
            long maximumResidentBytes = body.readLong();
            if (body.available() != 0) fail("protocol hello has trailing bytes");
            return new ProtocolHelloV1(hex(manifestDigest), epoch, readers, writers,
                    messageIds, artifactFormats, stageVersions, dataVersions,
                    deploymentAbis, capabilities, requiredExtensions, extensions,
                    maximumFrameBytes, maximumResidentBytes);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException("protocol hello is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException("protocol hello is malformed", malformed);
        }
    }

    private static void writeRange(DataOutputStream out, ProtocolRevisionRange range)
            throws IOException {
        out.writeInt(range.minimum());
        out.writeInt(range.maximum());
    }

    private static ProtocolRevisionRange readRange(DataInputStream in) throws IOException {
        return new ProtocolRevisionRange(in.readInt(), in.readInt());
    }

    private static void writeInts(DataOutputStream out, List<Integer> values)
            throws IOException {
        out.writeInt(values.size());
        for (Integer value : values) out.writeInt(value);
    }

    private static List<Integer> readInts(DataInputStream in, boolean nonEmpty)
            throws IOException {
        int count = readCount(in, nonEmpty);
        List<Integer> values = new ArrayList<>(count);
        int previous = 0;
        for (int index = 0; index < count; index++) {
            int value = in.readInt();
            if (value <= previous) fail("protocol ID set is not canonical");
            values.add(value);
            previous = value;
        }
        return values;
    }

    private static void writeTexts(DataOutputStream out, List<String> values)
            throws IOException {
        out.writeInt(values.size());
        for (String value : values) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    private static List<String> readTexts(DataInputStream in) throws IOException {
        int count = readCount(in, true);
        List<String> values = new ArrayList<>(count);
        String previous = null;
        for (int index = 0; index < count; index++) {
            int length = in.readInt();
            if (length <= 0 || length > ProtocolHelloV1.MAX_CAPABILITY_BYTES
                    || length > in.available()) fail("runtime capability length is invalid");
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes)).toString();
            if (previous != null && previous.compareTo(value) >= 0) {
                fail("runtime capabilities are not canonical");
            }
            values.add(value);
            previous = value;
        }
        return values;
    }

    private static int readCount(DataInputStream in, boolean nonEmpty) throws IOException {
        int count = in.readInt();
        if (count < (nonEmpty ? 1 : 0) || count > ProtocolHelloV1.MAX_ITEMS) {
            fail("protocol collection count is invalid");
        }
        return count;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static byte[] fromHex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return bytes;
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte current : value) {
            result.append(Character.forDigit((current >>> 4) & 0xf, 16));
            result.append(Character.forDigit(current & 0xf, 16));
        }
        return result.toString();
    }

    private static void fail(String message) throws IOException {
        throw new IOException(message);
    }
}
