package compat.stream_v1;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

/** Frozen standalone Stream golden writer/reader; intentionally has no RowWeave dependency. */
public final class PortableStreamPlanV1Runner {
    private static final byte[] MAGIC = "RWXXPSP1".getBytes(StandardCharsets.US_ASCII);
    private static final String PROJECTION =
            "1|EXACTLY_ONCE|1|128|APPEND|EVENT_TIME|"
                    + "rowweave.runtime.stream.v1|sink|source:SOURCE,sink:SINK";

    private PortableStreamPlanV1Runner() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length == 1 && "emit".equals(arguments[0])) {
            System.out.println(hex(frame()));
            return;
        }
        if (arguments.length == 2 && "project".equals(arguments[0])) {
            byte[] candidate = fromHex(arguments[1]);
            verifyEnvelope(candidate);
            if (!MessageDigest.isEqual(candidate, frame())) {
                System.out.println("UNSUPPORTED_STREAM_V1_FRAME");
            } else {
                System.out.println(PROJECTION);
            }
            return;
        }
        throw new IllegalArgumentException("usage: emit | project <hex>");
    }

    private static byte[] frame() throws Exception {
        ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bodyBytes)) {
            out.writeInt(1); // plan version
            out.writeInt(1); // EXACTLY_ONCE
            out.writeInt(1); // parallelism
            out.writeInt(128); // maximum parallelism
            out.writeInt(1); // APPEND
            out.writeInt(2); // EVENT_TIME
            writeTexts(out, new String[] {"rowweave.runtime.stream.v1"});
            writeTexts(out, new String[] {"sink"});

            out.writeInt(1); // edges
            writeText(out, "source");
            writeText(out, "sink");
            out.writeInt(0);
            writeText(out, "lane");
            out.writeInt(1); // FORWARD
            out.writeInt(1024);
            out.writeInt(4096);

            out.writeInt(2); // nodes
            writeNodePrefix(out, "source", 1, new String[0],
                    new String[0]);
            out.writeInt(1); // source binding
            writeText(out, "src");
            writeText(out, repeat('a'));
            writeText(out,
                    "4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a");
            writeFrame(out, new byte[] {1});
            out.writeInt(1); // split count
            writeText(out, repeat('c'));
            writeText(out, "pos");
            writeFrame(out, new byte[] {2});
            out.writeInt(1); // EXACT_ORDERED

            writeNodePrefix(out, "sink", 8, new String[] {"source"},
                    new String[] {
                            "9a74a09b02f4bf8e68303467d0d7ecb55167fe78298cc851d0f3a963c2501e13"
                    });
            out.writeInt(2); // sink binding
            writeText(out, "sink");
            writeText(out, repeat('b'));
            writeText(out,
                    "084fed08b978af4d7d196a7446a86b58009e636b611db16211b65a9aadff29c5");
            writeFrame(out, new byte[] {3});
            out.writeInt(1); // CHECKPOINT_TRANSACTIONAL
        }
        byte[] body = bodyBytes.toByteArray();
        ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(frameBytes)) {
            out.write(MAGIC);
            out.writeInt(1);
            out.writeInt(body.length);
            out.write(body);
            out.write(MessageDigest.getInstance("SHA-256").digest(body));
        }
        return frameBytes.toByteArray();
    }

    private static void writeNodePrefix(
            DataOutputStream out, String uid, int opcode,
            String[] inputs, String[] inputSchemas) throws Exception {
        writeText(out, uid);
        out.writeInt(opcode);
        writeTexts(out, inputs);
        writeTexts(out, inputSchemas);
        out.writeInt(1); // schema field count
        writeText(out, "id");
        writeText(out, "id");
        writeText(out, "id");
        out.writeInt(4); // LONG
        out.writeBoolean(false);
        out.writeInt(0); // stable keys
        out.writeInt(0); // states
        out.writeInt(0); // scalar parameters
    }

    private static void writeTexts(DataOutputStream out, String[] values)
            throws Exception {
        out.writeInt(values.length);
        for (String value : values) writeText(out, value);
    }

    private static void writeText(DataOutputStream out, String value)
            throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeFrame(DataOutputStream out, byte[] value)
            throws Exception {
        out.writeInt(value.length);
        out.write(value);
    }

    private static void verifyEnvelope(byte[] frame) throws Exception {
        if (frame.length < 48
                || !MessageDigest.isEqual(MAGIC, Arrays.copyOf(frame, 8))) {
            throw new IllegalArgumentException("INVALID_STREAM_V1_FRAME");
        }
        int version = readInt(frame, 8);
        int bodyLength = readInt(frame, 12);
        if (version != 1 || bodyLength < 0 || frame.length != bodyLength + 48) {
            throw new IllegalArgumentException("INVALID_STREAM_V1_FRAME");
        }
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(frame, 16, bodyLength);
        if (!MessageDigest.isEqual(digest.digest(),
                Arrays.copyOfRange(frame, 16 + bodyLength, frame.length))) {
            throw new IllegalArgumentException("CHECKSUM_MISMATCH");
        }
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24)
                | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8)
                | (bytes[offset + 3] & 0xff);
    }

    private static String repeat(char value) {
        char[] values = new char[64];
        Arrays.fill(values, value);
        return new String(values);
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
        if ((value.length() & 1) != 0) {
            throw new IllegalArgumentException("INVALID_HEX");
        }
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return bytes;
    }
}
