package compat.v1_1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Frozen v1.1 implementation. It deliberately has no RowWeave production dependency. */
public final class ReleaseContractV11Runner {
    private static final byte[] MAGIC = "RWXXRLS1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;

    private ReleaseContractV11Runner() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && "emit".equals(args[0])) {
            System.out.println(hex(emit()));
            return;
        }
        if (args.length == 2 && "project".equals(args[0])) {
            System.out.println(project(fromHex(args[1])));
            return;
        }
        throw new IllegalArgumentException("usage: emit | project HEX");
    }

    private static byte[] emit() throws Exception {
        ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bodyBytes);
        writeText(body, "rowweave-1.1.0");
        body.writeInt(1);
        body.writeInt(1);
        body.writeInt(0);
        body.writeInt(0);
        body.writeInt(1);
        writeText(body, "rowweave.plan-artifact-envelope");
        body.writeInt(1);
        body.writeInt(0);
        body.writeInt(1);
        body.writeInt(0);
        body.writeInt(1);
        body.writeInt(1);
        writeText(body, repeat('1', 64));
        body.writeBoolean(true);
        body.writeInt(2);
        writeText(body, "rowweave.required.n-only.v1");
        writeText(body, "rowweave.runtime.batch.v1");
        writeText(body, repeat('a', 64));
        writeText(body, "rowweave-decimal-semantics-v1");
        body.close();

        byte[] payload = bodyBytes.toByteArray();
        ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
        DataOutputStream frame = new DataOutputStream(frameBytes);
        frame.write(MAGIC);
        frame.writeInt(VERSION);
        frame.writeInt(payload.length);
        frame.write(payload);
        frame.write(sha256(payload));
        frame.close();
        return frameBytes.toByteArray();
    }

    private static String project(byte[] frame) throws Exception {
        if (frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            return "FRAME_LENGTH_MISMATCH";
        }
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(frame));
        byte[] magic = new byte[MAGIC.length];
        input.readFully(magic);
        if (!MessageDigest.isEqual(MAGIC, magic)) return "INVALID_MAGIC";
        if (input.readInt() != VERSION) return "UNSUPPORTED_VERSION";
        int bodyLength = input.readInt();
        if (bodyLength < 0
                || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES != frame.length) {
            return "FRAME_LENGTH_MISMATCH";
        }
        byte[] actual = sha256(frame, HEADER_BYTES, bodyLength);
        byte[] expected = Arrays.copyOfRange(
                frame, HEADER_BYTES + bodyLength, frame.length);
        if (!MessageDigest.isEqual(actual, expected)) return "CHECKSUM_MISMATCH";

        DataInputStream body = new DataInputStream(
                new ByteArrayInputStream(frame, HEADER_BYTES, bodyLength));
        String releaseId = readText(body);
        int epoch = body.readInt();
        int revision = body.readInt();
        body.readInt();
        body.readInt();
        if (epoch != 1 || revision > 1) return "READER_TOO_OLD";
        int componentCount = count(body.readInt(), 1, 256);
        List<String> componentIds = new ArrayList<>();
        for (int index = 0; index < componentCount; index++) {
            componentIds.add(readText(body));
            body.readInt();
            body.readInt();
            body.readInt();
            body.readInt();
            body.readInt();
            body.readInt();
            readText(body);
            body.readBoolean();
        }
        int capabilityCount = count(body.readInt(), 1, 256);
        List<String> capabilities = new ArrayList<>();
        for (int index = 0; index < capabilityCount; index++) {
            capabilities.add(readText(body));
        }
        readText(body);
        String decimal = readText(body);
        if (body.available() != 0) return "FRAME_LENGTH_MISMATCH";
        return releaseId + "|" + epoch + "|" + revision + "|"
                + join(componentIds) + "|" + join(capabilities) + "|" + decimal;
    }

    private static void writeText(DataOutputStream output, String value)
            throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws Exception {
        int length = count(input.readInt(), 1, 512);
        if (length > input.available()) throw new IllegalArgumentException("truncated text");
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int count(int value, int minimum, int maximum) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException("count out of bounds");
        }
        return value;
    }

    private static String join(List<String> values) {
        StringBuilder result = new StringBuilder();
        for (int index = 0; index < values.size(); index++) {
            if (index > 0) result.append(',');
            result.append(values.get(index));
        }
        return result.toString();
    }

    private static byte[] sha256(byte[] bytes) throws Exception {
        return sha256(bytes, 0, bytes.length);
    }

    private static byte[] sha256(byte[] bytes, int offset, int length)
            throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(bytes, offset, length);
        return digest.digest();
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte current : bytes) {
            value.append(Character.forDigit((current >>> 4) & 0xf, 16));
            value.append(Character.forDigit(current & 0xf, 16));
        }
        return value.toString();
    }

    private static byte[] fromHex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return bytes;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
