package compat.v1_3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

/** Frozen v1.3 store wire implementation with no RowWeave production dependency. */
final class StoreContractsV13Wire {
    static final String HEADER = "header";
    static final String AUTHORITY = "authority";

    private static final byte[] HEADER_MAGIC = ascii("RWXXSTH1");
    private static final byte[] AUTHORITY_MAGIC = ascii("RWXXUPA1");
    private static final int ENVELOPE_BYTES = 8 + 4 + 4 + 32;

    private StoreContractsV13Wire() { }

    static void run(String component, String[] args) throws Exception {
        if (args.length == 1 && "emit".equals(args[0])) {
            System.out.println(hex(HEADER.equals(component)
                    ? emitHeader() : emitAuthority()));
            return;
        }
        if (args.length == 2 && "project".equals(args[0])) {
            byte[] frame = fromHex(args[1]);
            System.out.println(HEADER.equals(component)
                    ? projectHeader(frame) : projectAuthority(frame));
            return;
        }
        throw new IllegalArgumentException("usage: emit | project HEX");
    }

    private static byte[] emitHeader() throws Exception {
        return envelope(HEADER_MAGIC, headerBody(0, 'a'));
    }

    private static byte[] emitAuthority() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bytes);
        body.writeInt(1);
        writeFrameBody(body, headerBody(0, 'a'));
        writeFrameBody(body, headerBody(0, 'a'));
        writeFrameBody(body, headerBody(2, 'b'));
        body.writeLong(9L);
        body.writeInt(4);
        writeText(body, "legacy-v0");
        writeDigest(body, 'c');
        writeText(body, "shadow-9");
        writeDigest(body, 'd');
        body.writeLong(17L);
        writeText(body, "master-a");
        writeText(body, "incarnation-a");
        body.writeLong(23L);
        body.writeLong(41L);
        writeText(body, "legacy-v0");
        body.writeLong(41L);
        body.writeLong(7L);
        body.writeLong(120L);
        body.writeInt(16);
        writeDigest(body, 'e');
        writeDigest(body, 'e');
        body.writeLong(2_000_000L);
        body.writeBoolean(false);
        body.writeBoolean(false);
        body.writeInt(1);
        writeText(body, "verify-9");
        writeDigest(body, 'f');
        body.close();
        return envelope(AUTHORITY_MAGIC, bytes.toByteArray());
    }

    private static byte[] headerBody(int revision, char digest) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bytes);
        body.writeInt(1);
        writeText(body, "golden-store");
        body.writeInt(1);
        body.writeInt(revision);
        writeDigest(body, digest);
        body.close();
        return bytes.toByteArray();
    }

    private static String projectHeader(byte[] frame) throws Exception {
        DataInputStream body = body(frame, HEADER_MAGIC, 4096);
        Header header = readHeader(body);
        requireEnd(body);
        return header.version + "|" + header.storeId + "|" + header.epoch
                + "|" + header.revision + "|" + header.digest;
    }

    private static String projectAuthority(byte[] frame) throws Exception {
        DataInputStream body = body(frame, AUTHORITY_MAGIC, 64 * 1024);
        int version = body.readInt();
        Header active = readNestedHeader(body);
        readNestedHeader(body);
        readNestedHeader(body);
        long generation = body.readLong();
        int phase = body.readInt();
        String source = readText(body);
        readDigest(body);
        String shadow = readText(body);
        readDigest(body);
        long term = body.readLong();
        readText(body);
        readText(body);
        body.readLong();
        body.readLong();
        readText(body);
        long activePointer = body.readLong();
        long writerEpoch = body.readLong();
        long objects = body.readLong();
        int shards = body.readInt();
        readDigest(body);
        readDigest(body);
        body.readLong();
        boolean writes = body.readBoolean();
        boolean newSemantics = body.readBoolean();
        int evidence = body.readInt();
        if (evidence < 0 || evidence > 16) throw new IllegalArgumentException("evidence");
        for (int index = 0; index < evidence; index++) {
            readText(body);
            readDigest(body);
        }
        requireEnd(body);
        return version + "|" + active.storeId + "|" + generation + "|"
                + phase + "|" + source + "|" + shadow + "|" + term + "|"
                + activePointer + "|" + writerEpoch + "|" + objects + "|"
                + shards + "|" + writes + "|" + newSemantics;
    }

    private static byte[] envelope(byte[] magic, byte[] body) throws Exception {
        ByteBuffer frame = ByteBuffer.allocate(16 + body.length + 32);
        frame.put(magic);
        frame.putInt(1);
        frame.putInt(body.length);
        frame.put(body);
        frame.put(MessageDigest.getInstance("SHA-256").digest(body));
        return frame.array();
    }

    private static DataInputStream body(
            byte[] frame, byte[] magic, int maximum) throws Exception {
        if (frame.length < ENVELOPE_BYTES || frame.length > maximum) {
            throw new IllegalArgumentException("frame length");
        }
        ByteBuffer input = ByteBuffer.wrap(frame);
        byte[] actualMagic = new byte[8];
        input.get(actualMagic);
        if (!Arrays.equals(magic, actualMagic) || input.getInt() != 1) {
            throw new IllegalArgumentException("frame identity");
        }
        int length = input.getInt();
        if (length < 0 || length != frame.length - ENVELOPE_BYTES) {
            throw new IllegalArgumentException("body length");
        }
               byte[] bytes = new byte[length];
        input.get(bytes);
        byte[] expected = new byte[32];
        input.get(expected);
        byte[] actual = MessageDigest.getInstance("SHA-256").digest(bytes);
        if (!MessageDigest.isEqual(expected, actual)) {
            throw new IllegalArgumentException("frame digest");
        }
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    private static Header readNestedHeader(DataInputStream input) throws Exception {
        int length = input.readInt();
        if (length <= 0 || length > 4096) throw new IllegalArgumentException("header length");
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        DataInputStream nested = new DataInputStream(new ByteArrayInputStream(bytes));
        Header header = readHeader(nested);
        requireEnd(nested);
        return header;
    }

    private static Header readHeader(DataInputStream input) throws Exception {
        int version = input.readInt();
        String store = readText(input);
        int epoch = input.readInt();
        int revision = input.readInt();
        String digest = readDigest(input);
        if (version != 1 || epoch < 1 || revision < 0) {
            throw new IllegalArgumentException("header values");
        }
        return new Header(version, store, epoch, revision, digest);
    }

    private static void writeFrameBody(DataOutputStream output, byte[] body)
            throws Exception {
        output.writeInt(body.length);
        output.write(body);
    }

    private static void writeText(DataOutputStream output, String value)
            throws Exception {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readText(DataInputStream input) throws Exception {
        int length = input.readInt();
        if (length <= 0 || length > 1024) throw new IllegalArgumentException("text length");
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        return new String(encoded, StandardCharsets.UTF_8);
    }

    private static void writeDigest(DataOutputStream output, char value)
            throws Exception {
        output.write(repeat(value, 64).getBytes(StandardCharsets.US_ASCII));
    }

    private static String readDigest(DataInputStream input) throws Exception {
        byte[] digest = new byte[64];
        input.readFully(digest);
        String value = new String(digest, StandardCharsets.US_ASCII);
        if (!value.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("digest");
        return value;
    }

    private static void requireEnd(DataInputStream input) throws Exception {
        if (input.read() != -1) throw new IllegalArgumentException("trailing bytes");
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
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
        if ((value.length() & 1) != 0) throw new IllegalArgumentException("hex");
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return bytes;
    }

    private static final class Header {
        private final int version;
        private final String storeId;
        private final int epoch;
        private final int revision;
        private final String digest;

        private Header(int version, String storeId, int epoch,
                       int revision, String digest) {
            this.version = version;
            this.storeId = storeId;
            this.epoch = epoch;
            this.revision = revision;
            this.digest = digest;
        }
    }
}
