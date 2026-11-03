package compat.v1_4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Frozen standalone v1.4 hello reader/writer with no RowWeave production dependency. */
public final class ControlProtocolV14Runner {
    private static final byte[] MAGIC =
            "RWXXCTH1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = MAGIC.length + 8;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_FRAME_BYTES = 256 * 1024;
    private static final int REGISTER_WORKER = 0x1001;
    private static final int HEARTBEAT = 0x1002;
    private static final int JOB_RESULT = 0x1006;
    private static final int ACK = 0x1007;
    private static final int DEREGISTER_WORKER = 0x100b;
    private static final int QUERY_RUN = 0x1016;
    private static final int RUN_SNAPSHOT = 0x1017;
    private static final int MAX_RPC_FRAME_BYTES = 80 * 1024 * 1024;

    private ControlProtocolV14Runner() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && "emit".equals(args[0])) {
            System.out.println(hex(emit()));
            return;
        }
        if (args.length == 2 && "project".equals(args[0])) {
            System.out.println(project(fromHex(args[1])));
            return;
        }
        if (args.length == 1 && "serve".equals(args[0])) {
            serve();
            return;
        }
        if (args.length == 4 && "worker".equals(args[0])) {
            worker(args[1], Integer.parseInt(args[2]), args[3]);
            return;
        }
        if (args.length == 3 && "client".equals(args[0])) {
            client(args[1], Integer.parseInt(args[2]));
            return;
        }
        throw new IllegalArgumentException(
                "usage: emit | project HEX | serve | worker HOST PORT ID"
                        + " | client HOST PORT");
    }

    private static void client(String host, int port) throws Exception {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(10_000);
            OutputStream output = socket.getOutputStream();
            DataInputStream input = new DataInputStream(socket.getInputStream());
            output.write(emit());
            output.flush();
            requireCompatibleHello(readHelloFrame(input));
            writeRpc(output, QUERY_RUN, null, null,
                    0, 0, -1, null, false, null, "missing");
            int response = readRpcType(input);
            if (response != RUN_SNAPSHOT) {
                throw new IllegalStateException(
                        "query response was " + messageName(response));
            }
            System.out.println("CLIENT|RUN_SNAPSHOT|writer=0");
        }
    }

    private static void worker(String host, int port, String workerId)
            throws Exception {
        String incarnation = "frozen-v1.0-" + workerId;
        BufferedReader control = new BufferedReader(new InputStreamReader(
                System.in, StandardCharsets.UTF_8));
        boolean moved = false;
        int activePort = port;
        while (true) {
            try (Socket socket = new Socket(host, activePort)) {
                socket.setSoTimeout(10_000);
                OutputStream output = socket.getOutputStream();
                DataInputStream input = new DataInputStream(
                        socket.getInputStream());
                output.write(emit());
                output.flush();
                requireCompatibleHello(readHelloFrame(input));
                writeRpc(output, REGISTER_WORKER, workerId, "127.0.0.1",
                        19_000, 1, -1, incarnation, false, null);
                int response = readRpcType(input);
                if (response != ACK) {
                    throw new IllegalStateException(
                            "registration response was " + messageName(response));
                }
                System.out.println((moved ? "MOVED" : "REGISTERED")
                        + "|ACK|writer=0");
                System.out.flush();
                String command = control.readLine();
                if ("STOP".equals(command)) {
                    writeRpc(output, DEREGISTER_WORKER, workerId, "127.0.0.1",
                            19_000, 1, -1, incarnation, false, null);
                    return;
                }
                if (command == null || !command.startsWith("MOVE|")) {
                    throw new IllegalArgumentException(
                            "worker expected STOP or MOVE|PORT");
                }
                activePort = Integer.parseInt(command.substring(5));
                moved = true;
            }
        }
    }

    private static void serve() throws Exception {
        final ServerSocket server = new ServerSocket(
                0, 16, InetAddress.getByName("127.0.0.1"));
        server.setSoTimeout(15_000);
        final AtomicBoolean stopping = new AtomicBoolean();
        final List<String> events = Collections.synchronizedList(
                new ArrayList<String>());
        ExecutorService connections = Executors.newCachedThreadPool();
        System.out.println("READY|" + server.getLocalPort());
        System.out.flush();
        try {
            while (!stopping.get()) {
                try {
                    final Socket socket = server.accept();
                    connections.execute(new Runnable() {
                        @Override
                        public void run() {
                            serveConnection(socket, server, stopping, events);
                        }
                    });
                } catch (SocketException closed) {
                    if (!stopping.get()) throw closed;
                }
            }
        } finally {
            try { server.close(); } catch (Exception ignored) { }
            connections.shutdown();
            if (!connections.awaitTermination(10, TimeUnit.SECONDS)) {
                connections.shutdownNow();
                throw new IllegalStateException("connections did not stop");
            }
        }
        System.out.println("EVENTS|" + join(events));
    }

    private static void serveConnection(
            Socket socket, ServerSocket server, AtomicBoolean stopping,
            List<String> events) {
        try (Socket connected = socket) {
            connected.setSoTimeout(10_000);
            DataInputStream input = new DataInputStream(
                    connected.getInputStream());
            OutputStream output = connected.getOutputStream();
            requireCompatibleHello(readHelloFrame(input));
            output.write(emit());
            output.flush();
            while (!stopping.get()) {
                int type = readRpcType(input);
                if (type < 0) return;
                events.add(messageName(type));
                if (type == REGISTER_WORKER) {
                    writeRpc(output, ACK, null, null,
                            0, 0, -1, null, true, null);
                } else if (type == QUERY_RUN) {
                    writeRpc(output, RUN_SNAPSHOT, null, null,
                            0, 0, -1, null, false, null);
                } else if (type == DEREGISTER_WORKER) {
                    stopping.set(true);
                    server.close();
                    return;
                }
            }
        } catch (EOFException disconnected) {
            // One peer may close while another connection completes the matrix.
        } catch (Exception failure) {
            failure.printStackTrace(System.out);
            stopping.set(true);
            try { server.close(); } catch (Exception ignored) { }
        }
    }

    private static void requireCompatibleHello(byte[] frame) throws Exception {
        String projection = project(frame);
        String[] fields = projection.split("\\|", -1);
        boolean compatible = fields.length == 7
                && "1".equals(fields[0])
                && overlaps(fields[1], 0, 1)
                && overlaps(fields[2], 0, 0)
                && Integer.parseInt(fields[3]) > 0
                && contains(fields[4], "rowweave.runtime.batch.v1")
                && Integer.parseInt(fields[5]) > 0
                && Long.parseLong(fields[6]) > 0L
                && supportsRequiredExtensions(frame)
                && MessageDigest.isEqual(
                        Arrays.copyOfRange(frame, HEADER_BYTES,
                                HEADER_BYTES + DIGEST_BYTES),
                        repeatBytes((byte) 0xaa, DIGEST_BYTES));
        if (!compatible) {
            throw new IllegalArgumentException(
                    "incompatible peer hello: " + projection);
        }
    }

    private static boolean supportsRequiredExtensions(byte[] frame)
            throws Exception {
        DataInputStream framed = new DataInputStream(
                new ByteArrayInputStream(frame));
        read(framed, MAGIC.length);
        framed.readInt();
        int bodyLength = framed.readInt();
        DataInputStream body = new DataInputStream(
                new ByteArrayInputStream(read(framed, bodyLength)));
        read(body, DIGEST_BYTES);
        body.readInt();
        body.readInt();
        body.readInt();
        body.readInt();
        body.readInt();
        readIncreasingInts(body, true);
        readIncreasingInts(body, true);
        readIncreasingInts(body, true);
        readIncreasingInts(body, true);
        readIncreasingInts(body, true);
        int capabilityCount = count(body.readInt(), 1, 512);
        for (int index = 0; index < capabilityCount; index++) {
            readText(body);
        }
        for (Integer required : readIncreasingInts(body, false)) {
            if (required.intValue() != 200) return false;
        }
        return true;
    }

    private static boolean overlaps(
            String encoded, int localMinimum, int localMaximum) {
        int separator = encoded.indexOf('-');
        if (separator <= 0) return false;
        int minimum = Integer.parseInt(encoded.substring(0, separator));
        int maximum = Integer.parseInt(encoded.substring(separator + 1));
        return minimum <= localMaximum && maximum >= localMinimum;
    }

    private static boolean contains(String encoded, String expected) {
        return Arrays.asList(encoded.split(",", -1)).contains(expected);
    }

    private static byte[] readHelloFrame(DataInputStream input)
            throws Exception {
        byte[] header = read(input, HEADER_BYTES);
        DataInputStream decoded = new DataInputStream(
                new ByteArrayInputStream(header));
        read(decoded, MAGIC.length);
        decoded.readInt();
        int bodyLength = decoded.readInt();
        if (bodyLength < 0 || bodyLength > MAX_FRAME_BYTES
                - HEADER_BYTES - DIGEST_BYTES) {
            throw new IllegalArgumentException("hello body length out of bounds");
        }
        byte[] frame = new byte[HEADER_BYTES + bodyLength + DIGEST_BYTES];
        System.arraycopy(header, 0, frame, 0, header.length);
        input.readFully(frame, header.length, frame.length - header.length);
        return frame;
    }

    private static int readRpcType(DataInputStream input) throws Exception {
        int length;
        try {
            length = input.readInt();
        } catch (EOFException end) {
            return -1;
        }
        if (length < Integer.BYTES || length > MAX_RPC_FRAME_BYTES) {
            throw new IllegalArgumentException("RPC frame length out of bounds");
        }
        int type = input.readInt();
        int remaining = length - Integer.BYTES;
        byte[] scratch = new byte[Math.min(8192, remaining)];
        while (remaining > 0) {
            int count = input.read(scratch, 0, Math.min(scratch.length, remaining));
            if (count < 0) throw new EOFException("truncated RPC frame");
            remaining -= count;
        }
        return type;
    }

    private static void writeRpc(
            OutputStream target, int type, String workerId, String host,
            int port, int slots, int dataPort, String incarnation,
            boolean success, String error) throws Exception {
        writeRpc(target, type, workerId, host, port, slots, dataPort,
                incarnation, success, error, null);
    }

    private static void writeRpc(
            OutputStream target, int type, String workerId, String host,
            int port, int slots, int dataPort, String incarnation,
            boolean success, String error, String queryRunId) throws Exception {
        ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bodyBytes);
        body.writeInt(type);
        writeNullableText(body, workerId);
        writeNullableText(body, host);
        body.writeInt(port);
        body.writeInt(slots);
        writeNullableBytes(body, null);
        body.writeLong(0L);
        writeNullableBytes(body, null);
        body.writeBoolean(success);
        body.writeLong(0L);
        writeNullableText(body, error);
        body.writeLong(0L);
        body.writeLong(-1L);
        body.writeInt(dataPort);
        writeNullableText(body, incarnation);
        writeNullableText(body, null);
        writeNullableText(body, null);
        body.writeLong(0L);
        writeNullableBytes(body, null);
        writeNullableText(body, null);
        writeNullableText(body, null);
        writeNullableText(body, null);
        writeNullableText(body, null);
        writeNullableText(body, null);
        body.writeLong(0L);
        writeNullableText(body, queryRunId);
        body.flush();
        byte[] payload = bodyBytes.toByteArray();
        DataOutputStream output = new DataOutputStream(target);
        output.writeInt(payload.length);
        output.write(payload);
        output.flush();
    }

    private static void writeNullableText(
            DataOutputStream output, String value) throws Exception {
        if (value == null) {
            output.writeInt(-1);
        } else {
            writeText(output, value);
        }
    }

    private static void writeNullableBytes(
            DataOutputStream output, byte[] value) throws Exception {
        if (value == null) {
            output.writeInt(-1);
        } else {
            output.writeInt(value.length);
            output.write(value);
        }
    }

    private static String messageName(int type) {
        if (type == REGISTER_WORKER) return "REGISTER_WORKER";
        if (type == HEARTBEAT) return "HEARTBEAT";
        if (type == JOB_RESULT) return "JOB_RESULT";
        if (type == ACK) return "ACK";
        if (type == DEREGISTER_WORKER) return "DEREGISTER_WORKER";
        if (type == QUERY_RUN) return "QUERY_RUN";
        if (type == RUN_SNAPSHOT) return "RUN_SNAPSHOT";
        return "UNKNOWN_" + type;
    }

    private static byte[] emit() throws Exception {
        ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bodyBytes);
        body.write(repeatBytes((byte) 0xaa, 32));
        body.writeInt(1);
        writeRange(body, 0, 1);
        writeRange(body, 0, 0);
        body.writeInt(46);
        for (int id = 0x1001; id <= 0x102e; id++) body.writeInt(id);
        writeInts(body, 1);
        writeInts(body, 1, 2, 3, 4, 5, 6, 7);
        writeInts(body, 1, 2);
        writeInts(body, 1, 2, 3, 4, 5, 6, 7);
        body.writeInt(1);
        writeText(body, "rowweave.runtime.batch.v1");
        body.writeInt(0);
        body.writeInt(1);
        body.writeInt(200);
        body.writeInt(1);
        body.writeByte(7);
        body.writeInt(80 * 1024 * 1024);
        body.writeLong(128L * 1024 * 1024);
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
                || frame.length > MAX_FRAME_BYTES) return "FRAME_LENGTH_MISMATCH";
        DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame));
        byte[] magic = read(input, MAGIC.length);
        if (!MessageDigest.isEqual(MAGIC, magic)) return "INVALID_MAGIC";
        if (input.readInt() != VERSION) return "UNSUPPORTED_VERSION";
        int bodyLength = input.readInt();
        if (bodyLength < 0
                || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES
                != frame.length) return "FRAME_LENGTH_MISMATCH";
        byte[] payload = read(input, bodyLength);
        if (!MessageDigest.isEqual(sha256(payload), read(input, DIGEST_BYTES))) {
            return "CHECKSUM_MISMATCH";
        }

        DataInputStream body = new DataInputStream(
                new ByteArrayInputStream(payload));
        read(body, 32);
        int epoch = body.readInt();
        int readerMinimum = body.readInt();
        int readerMaximum = body.readInt();
        int writerMinimum = body.readInt();
        int writerMaximum = body.readInt();
        int messageCount = readIncreasingInts(body, true).size();
        readIncreasingInts(body, true);
        readIncreasingInts(body, true);
        readIncreasingInts(body, true);
        readIncreasingInts(body, true);
        int capabilityCount = count(body.readInt(), 1, 512);
        List<String> capabilities = new ArrayList<String>();
        String previous = null;
        for (int index = 0; index < capabilityCount; index++) {
            String capability = readText(body);
            if (previous != null && previous.compareTo(capability) >= 0) {
                throw new IllegalArgumentException("non-canonical capability set");
            }
            capabilities.add(capability);
            previous = capability;
        }
        readIncreasingInts(body, false);
        int extensionCount = count(body.readInt(), 0, 512);
        int previousExtension = 0;
        for (int index = 0; index < extensionCount; index++) {
            int id = body.readInt();
            int length = count(body.readInt(), 0, 4096);
            if (id <= previousExtension) {
                throw new IllegalArgumentException("non-canonical extension set");
            }
            read(body, length);
            previousExtension = id;
        }
        int maximumFrame = body.readInt();
        long maximumResident = body.readLong();
        if (body.available() != 0 || epoch <= 0 || readerMinimum < 0
                || readerMaximum < readerMinimum || writerMinimum < 0
                || writerMaximum < writerMinimum || maximumFrame <= 0
                || maximumResident <= 0L) {
            return "INVALID_HELLO";
        }
        return epoch + "|" + readerMinimum + "-" + readerMaximum + "|"
                + writerMinimum + "-" + writerMaximum + "|"
                + messageCount + "|" + join(capabilities) + "|"
                + maximumFrame + "|" + maximumResident;
    }

    private static void writeRange(DataOutputStream output, int minimum,
                                   int maximum) throws Exception {
        output.writeInt(minimum);
        output.writeInt(maximum);
    }

    private static void writeInts(DataOutputStream output, int... values)
            throws Exception {
        output.writeInt(values.length);
        for (int value : values) output.writeInt(value);
    }

    private static List<Integer> readIncreasingInts(
            DataInputStream input, boolean nonEmpty) throws Exception {
        int total = count(input.readInt(), nonEmpty ? 1 : 0, 512);
        List<Integer> values = new ArrayList<Integer>(total);
        int previous = 0;
        for (int index = 0; index < total; index++) {
            int value = input.readInt();
            if (value <= previous) {
                throw new IllegalArgumentException("non-canonical id set");
            }
            values.add(value);
            previous = value;
        }
        return values;
    }

    private static void writeText(DataOutputStream output, String value)
            throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws Exception {
        int length = count(input.readInt(), 1, 256);
        return new String(read(input, length), StandardCharsets.UTF_8);
    }

    private static byte[] read(DataInputStream input, int length)
            throws Exception {
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }

    private static int count(int value, int minimum, int maximum) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException("count is outside bounds");
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

    private static byte[] repeatBytes(byte value, int count) {
        byte[] bytes = new byte[count];
        Arrays.fill(bytes, value);
        return bytes;
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }

    private static byte[] fromHex(String value) {
        if ((value.length() & 1) != 0) {
            throw new IllegalArgumentException("odd hex length");
        }
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return bytes;
    }
}
