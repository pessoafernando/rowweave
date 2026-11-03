package io.rowweave.upgrade;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

final class UpgradeCodecSupport {
    static final int DIGEST_BYTES = 32;
    static final int ENVELOPE_BYTES = 8 + 4 + 4 + DIGEST_BYTES;
    static final int MAX_TEXT_BYTES = 1024;

    private UpgradeCodecSupport() {
    }

    static byte[] frame(byte[] magic, int codecVersion, byte[] body,
                        int maxFrameBytes) {
        if (magic.length != 8 || body == null) {
            throw new IllegalArgumentException("invalid upgrade frame input");
        }
        long frameLength = 16L + body.length + DIGEST_BYTES;
        if (frameLength > maxFrameBytes) {
            throw new IllegalArgumentException("upgrade frame exceeds limit");
        }
        ByteBuffer frame = ByteBuffer.allocate((int) frameLength);
        frame.put(magic);
        frame.putInt(codecVersion);
        frame.putInt(body.length);
        frame.put(body);
        frame.put(sha256(body));
        return frame.array();
    }

    static byte[] body(byte[] frame, byte[] magic, int codecVersion,
                       int maxFrameBytes) {
        if (frame == null || frame.length < ENVELOPE_BYTES
                || frame.length > maxFrameBytes) {
            throw new IllegalArgumentException("upgrade frame length is invalid");
        }
        ByteBuffer input = ByteBuffer.wrap(frame);
        byte[] actualMagic = new byte[8];
        input.get(actualMagic);
        if (!Arrays.equals(magic, actualMagic)) {
            throw new IllegalArgumentException("upgrade frame magic is invalid");
        }
        if (input.getInt() != codecVersion) {
            throw new IllegalArgumentException("unknown upgrade codec version");
        }
        int bodyLength = input.getInt();
        if (bodyLength < 0 || bodyLength != frame.length - ENVELOPE_BYTES) {
            throw new IllegalArgumentException("upgrade body length is invalid");
        }
        byte[] body = new byte[bodyLength];
        input.get(body);
        byte[] expected = new byte[DIGEST_BYTES];
        input.get(expected);
        if (input.hasRemaining() || !MessageDigest.isEqual(expected, sha256(body))) {
            throw new IllegalArgumentException("upgrade frame digest mismatch");
        }
        return body;
    }

    static void writeString(DataOutputStream out, String value)
            throws IOException {
        requireText(value, "text");
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    static String readString(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException("encoded text length is invalid");
        }
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded)).toString();
            requireText(value, "text");
            return value;
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException("encoded text is not UTF-8", invalid);
        }
    }

    static void writeDigest(DataOutputStream out, String value)
            throws IOException {
        requireDigest(value, "digest");
        out.write(value.getBytes(StandardCharsets.US_ASCII));
    }

    static String readDigest(DataInputStream input) throws IOException {
        byte[] digest = new byte[64];
        input.readFully(digest);
        String value = new String(digest, StandardCharsets.US_ASCII);
        requireDigest(value, "digest");
        return value;
    }

    static byte[] encodeHeaderBody(StoreFormatHeader header) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(header.formatVersion());
        writeString(out, header.storeId());
        out.writeInt(header.protocolEpoch());
        out.writeInt(header.activeRevision());
        writeDigest(out, header.manifestDigest());
        out.flush();
        return bytes.toByteArray();
    }

    static StoreFormatHeader decodeHeaderBody(DataInputStream input)
            throws IOException {
        return new StoreFormatHeader(input.readInt(), readString(input),
                input.readInt(), input.readInt(), readDigest(input));
    }

    static void requireConsumed(DataInputStream input) throws IOException {
        if (input.read() != -1) {
            throw new IllegalArgumentException("upgrade body has trailing bytes");
        }
    }

    static IllegalArgumentException malformed(String label, IOException cause) {
        if (cause instanceof EOFException) {
            return new IllegalArgumentException(label + " is truncated", cause);
        }
        return new IllegalArgumentException(label + " is malformed", cause);
    }

    static void requireText(String value, String field) {
        if (value == null || value.isEmpty()
                || value.getBytes(StandardCharsets.UTF_8).length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(field + " is invalid");
        }
    }

    static void requireDigest(String value, String field) {
        if (value == null || value.length() != 64) {
            throw new IllegalArgumentException(field + " is invalid");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= '0' && character <= '9')
                    || (character >= 'a' && character <= 'f'))) {
                throw new IllegalArgumentException(field + " is invalid");
            }
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    static String sha256Hex(byte[] value) {
        byte[] digest = sha256(value);
        char[] encoded = new char[digest.length * 2];
        char[] alphabet = "0123456789abcdef".toCharArray();
        for (int index = 0; index < digest.length; index++) {
            int current = digest[index] & 0xff;
            encoded[index * 2] = alphabet[current >>> 4];
            encoded[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(encoded);
    }
}
