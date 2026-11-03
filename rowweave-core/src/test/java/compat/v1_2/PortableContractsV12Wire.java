package compat.v1_2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Frozen v1.2 wire implementation with no RowWeave production dependency. */
final class PortableContractsV12Wire {
    static final String FUNCTION = "function";
    static final String PLAN = "plan";
    static final String CONNECTOR = "connector";

    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final byte[] FUNCTION_MAGIC = ascii("RWXXFPC1");
    private static final byte[] PLAN_MAGIC = ascii("RWXXPBP1");
    private static final byte[] CONNECTOR_MAGIC = ascii("RWXXCPR1");
    private static final byte[] SCHEMA_MAGIC = ascii("RWXXSC01");

    private PortableContractsV12Wire() { }

    static void run(String component, String[] args) throws Exception {
        if (args.length == 1 && "emit".equals(args[0])) {
            System.out.println(hex(emit(component)));
            return;
        }
        if (args.length == 2 && "project".equals(args[0])) {
            System.out.println(project(component, fromHex(args[1])));
            return;
        }
        throw new IllegalArgumentException("usage: emit | project HEX");
    }

    private static byte[] emit(String component) throws Exception {
        if (FUNCTION.equals(component)) return emitFunctionContract();
        if (PLAN.equals(component)) return emitPlan();
        if (CONNECTOR.equals(component)) return emitConnectorContract();
        throw new IllegalArgumentException("unknown component");
    }

    private static String project(String component, byte[] frame)
            throws Exception {
        if (FUNCTION.equals(component)) return projectFunctionContract(frame);
        if (PLAN.equals(component)) return projectPlan(frame);
        if (CONNECTOR.equals(component)) return projectConnectorContract(frame);
        throw new IllegalArgumentException("unknown component");
    }

    private static byte[] emitFunctionContract() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bytes);
        body.writeInt(1);
        writeText(body, "com.example.rowweave.GoldenMapFunction");
        body.writeInt(2);
        writeText(body, "json-v1");
        body.writeInt(4096);
        body.writeLong(64L * 1024 * 1024);
        body.writeLong(8L * 1024 * 1024);
        body.writeInt(4);
        body.close();
        return envelope(FUNCTION_MAGIC, bytes.toByteArray());
    }

    private static String projectFunctionContract(byte[] frame)
            throws Exception {
        DataInputStream body = body(frame, FUNCTION_MAGIC, 4096);
        int version = body.readInt();
        String className = readText(body, false);
        int abi = body.readInt();
        String codec = readText(body, false);
        int configurationBytes = body.readInt();
        long heapBytes = body.readLong();
        long directBytes = body.readLong();
        int rows = body.readInt();
        requireEnd(body);
        return version + "|" + className + "|" + abi + "|" + codec + "|"
                + configurationBytes + "|" + heapBytes + "|" + directBytes
                + "|" + rows;
    }

    private static byte[] emitConnectorContract() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bytes);
        body.writeInt(1);
        body.writeInt(1);
        writeText(body, "golden-source-v1");
        writeText(body, "json-v1");
        body.writeInt(4096);
        body.writeInt(64 * 1024);
        body.writeInt(8);
        body.close();
        return envelope(CONNECTOR_MAGIC, bytes.toByteArray());
    }

    private static String projectConnectorContract(byte[] frame)
            throws Exception {
        DataInputStream body = body(frame, CONNECTOR_MAGIC, 2048);
        int version = body.readInt();
        int kind = body.readInt();
        String provider = readText(body, false);
        String codec = readText(body, false);
        int configurationBytes = body.readInt();
        int payloadBytes = body.readInt();
        int instances = body.readInt();
        requireEnd(body);
        return version + "|" + kind + "|" + provider + "|" + codec + "|"
                + configurationBytes + "|" + payloadBytes + "|" + instances;
    }

    private static byte[] emitPlan() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bytes);
        body.writeInt(1);
        body.writeInt(2);
        writeTexts(body, Arrays.asList(
                "rowweave.runtime.batch.v1", "rowweave.source.jdbc.v1"));
        writeTexts(body, Arrays.asList("sink"));
        body.writeInt(2);

        writeText(body, "source");
        body.writeInt(1);
        writeTexts(body, new ArrayList<String>());
        writeTexts(body, new ArrayList<String>());
        writeGoldenSchema(body);
        body.writeInt(2);
        writeText(body, "alpha");
        writeText(body, "first");
        writeText(body, "zeta");
        writeText(body, "last");
        body.writeInt(2);
        writeText(body, "alpha");
        writeTexts(body, Arrays.asList("id"));
        writeText(body, "zeta");
        writeTexts(body, Arrays.asList("amount"));
        body.writeInt(1);
        writeText(body, "jdbc-v1");
        writeText(body, repeat('a', 64));
        writeFrame(body, new byte[]{1, 2});
        body.writeInt(2);
        writeFrame(body, new byte[]{3});
        writeFrame(body, new byte[]{4});

        writeText(body, "sink");
        body.writeInt(2);
        writeTexts(body, Arrays.asList("source"));
        writeTexts(body, Arrays.asList(goldenSchemaFingerprint()));
        writeGoldenSchema(body);
        body.writeInt(0);
        body.writeInt(0);
        body.writeInt(3);
        writeText(body, "manifest-file-v1");
        writeText(body, repeat('b', 64));
        writeFrame(body, new byte[]{5});
        writeFrame(body, new byte[]{6});
        body.close();
        return envelope(PLAN_MAGIC, bytes.toByteArray());
    }

    private static String projectPlan(byte[] frame) throws Exception {
        DataInputStream body = body(frame, PLAN_MAGIC,
                HEADER_BYTES + 32 * 1024 * 1024 + DIGEST_BYTES);
        int version = body.readInt();
        int parallelism = body.readInt();
        List<String> capabilities = readTexts(body);
        List<String> roots = readTexts(body);
        int nodeCount = readCount(body, 1, 4096);
        List<String> nodes = new ArrayList<>();
        for (int index = 0; index < nodeCount; index++) {
            String id = readText(body, false);
            int opcode = body.readInt();
            List<String> inputs = readTexts(body);
            readTexts(body);
            String fields = readSchemaProjection(body);
            String scalar = readScalarProjection(body);
            String lists = readListProjection(body);
            String binding = readBindingProjection(body);
            nodes.add(id + "@" + opcode + "(inputs=" + join(inputs)
                    + ";fields=" + fields + ";binding=" + binding
                    + ";scalar=" + scalar + ";lists=" + lists + ")");
        }
        requireEnd(body);
        return version + "|" + parallelism + "|" + join(capabilities)
                + "|" + join(roots) + "|" + join(nodes, ";");
    }

    private static String readSchemaProjection(DataInputStream input)
            throws Exception {
        int count = readCount(input, 1, 1024);
        List<String> fields = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            String id = readText(input, false);
            readText(input, false);
            readText(input, false);
            int type = input.readInt();
            boolean nullable = input.readBoolean();
            fields.add(id + ":" + type + ":" + nullable);
        }
        return join(fields);
    }

    private static String readScalarProjection(DataInputStream input)
            throws Exception {
        int count = readCount(input, 0, 64);
        List<String> values = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            values.add(readText(input, false) + ":" + readText(input, true));
        }
        return join(values);
    }

    private static String readListProjection(DataInputStream input)
            throws Exception {
        int count = readCount(input, 0, 64);
        List<String> values = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            values.add(readText(input, false) + ":" + join(readTexts(input)));
        }
        return join(values);
    }

    private static String readBindingProjection(DataInputStream input)
            throws Exception {
        int kind = input.readInt();
        if (kind == 0) return "none";
        if (kind == 1) {
            String provider = readText(input, false);
            readText(input, false);
            readFrame(input);
            int splitCount = readCount(input, 1, 4096);
            for (int index = 0; index < splitCount; index++) readFrame(input);
            return "source:" + provider;
        }
        if (kind == 2) {
            readText(input, false);
            readText(input, false);
            readText(input, false);
            String className = readText(input, false);
            input.readInt();
            readText(input, false);
            readText(input, false);
            readFrame(input);
            input.readLong();
            input.readLong();
            input.readInt();
            return "function:" + className;
        }
        if (kind == 3) {
            String provider = readText(input, false);
            readText(input, false);
            readFrame(input);
            readFrame(input);
            return "sink:" + provider;
        }
        throw new IllegalArgumentException("unknown binding kind");
    }

    private static void writeGoldenSchema(DataOutputStream output)
            throws Exception {
        output.writeInt(2);
        writeField(output, "id", 4, false);
        writeField(output, "amount", 5, true);
    }

    private static String goldenSchemaFingerprint() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream schema = new DataOutputStream(bytes);
        schema.write(SCHEMA_MAGIC);
        schema.writeInt(1);
        writeGoldenSchema(schema);
        schema.close();
        return hex(sha256(bytes.toByteArray()));
    }

    private static void writeField(
            DataOutputStream output, String name, int type, boolean nullable)
            throws Exception {
        writeText(output, name);
        writeText(output, name);
        writeText(output, name);
        output.writeInt(type);
        output.writeBoolean(nullable);
    }

    private static byte[] envelope(byte[] magic, byte[] body) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream frame = new DataOutputStream(bytes);
        frame.write(magic);
        frame.writeInt(1);
        frame.writeInt(body.length);
        frame.write(body);
        frame.write(sha256(body));
        frame.close();
        return bytes.toByteArray();
    }

    private static DataInputStream body(
            byte[] frame, byte[] magic, int maximum) throws Exception {
        if (frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > maximum) {
            throw new IllegalArgumentException("frame length mismatch");
        }
        DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame));
        byte[] actualMagic = new byte[magic.length];
        input.readFully(actualMagic);
        if (!MessageDigest.isEqual(magic, actualMagic)) {
            throw new IllegalArgumentException("magic mismatch");
        }
        if (input.readInt() != 1) {
            throw new IllegalArgumentException("unsupported version");
        }
        int bodyLength = input.readInt();
        if (bodyLength <= 0
                || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES
                != frame.length) {
            throw new IllegalArgumentException("frame length mismatch");
        }
        byte[] body = new byte[bodyLength];
        input.readFully(body);
        byte[] expected = new byte[DIGEST_BYTES];
        input.readFully(expected);
        if (!MessageDigest.isEqual(expected, sha256(body))) {
            throw new IllegalArgumentException("checksum mismatch");
        }
        return new DataInputStream(new ByteArrayInputStream(body));
    }

    private static void writeTexts(
            DataOutputStream output, List<String> values) throws Exception {
        output.writeInt(values.size());
        for (String value : values) writeText(output, value);
    }

    private static List<String> readTexts(DataInputStream input)
            throws Exception {
        int count = readCount(input, 0, 4096);
        List<String> values = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            values.add(readText(input, true));
        }
        return values;
    }

    private static void writeText(DataOutputStream output, String value)
            throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input, boolean allowEmpty)
            throws Exception {
        int length = readCount(input, allowEmpty ? 0 : 1, 4096);
        if (length > input.available()) {
            throw new IllegalArgumentException("truncated text");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeFrame(DataOutputStream output, byte[] value)
            throws Exception {
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readFrame(DataInputStream input) throws Exception {
        int length = readCount(input, 1, 1024 * 1024);
        if (length > input.available()) {
            throw new IllegalArgumentException("truncated frame");
        }
        byte[] value = new byte[length];
        input.readFully(value);
        return value;
    }

    private static int readCount(
            DataInputStream input, int minimum, int maximum) throws Exception {
        int value = input.readInt();
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException("count out of bounds");
        }
        return value;
    }

    private static void requireEnd(DataInputStream input) throws Exception {
        if (input.available() != 0) {
            throw new IllegalArgumentException("trailing bytes");
        }
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static String join(List<String> values) {
        return join(values, ",");
    }

    private static String join(List<String> values, String separator) {
        StringBuilder result = new StringBuilder();
        for (int index = 0; index < values.size(); index++) {
            if (index > 0) result.append(separator);
            result.append(values.get(index));
        }
        return result.toString();
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
        if (value.length() % 2 != 0) {
            throw new IllegalArgumentException("invalid hex");
        }
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
