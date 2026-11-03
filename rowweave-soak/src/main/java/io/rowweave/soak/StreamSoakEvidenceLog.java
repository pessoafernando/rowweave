package io.rowweave.soak;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Append-only bounded-frequency sample log with a checksum on every record. */
public final class StreamSoakEvidenceLog implements AutoCloseable {
    private final FileChannel channel;

    public StreamSoakEvidenceLog(Path path) throws IOException {
        if (path == null) throw new IllegalArgumentException("path required");
        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent != null) Files.createDirectories(parent);
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
    }

    public synchronized void append(Map<String, String> fields)
            throws IOException {
        byte[] payload = canonical(fields).getBytes(StandardCharsets.US_ASCII);
        String line = new String(payload, StandardCharsets.US_ASCII)
                + '#' + hex(sha256(payload)) + '\n';
        ByteBuffer buffer = ByteBuffer.wrap(
                line.getBytes(StandardCharsets.US_ASCII));
        while (buffer.hasRemaining()) channel.write(buffer);
        channel.force(false);
    }

    public static List<Map<String, String>> readVerified(Path path)
            throws IOException {
        List<Map<String, String>> result = new ArrayList<>();
        for (String line : Files.readAllLines(path,
                StandardCharsets.US_ASCII)) {
            int separator = line.lastIndexOf('#');
            if (separator < 1 || separator + 65 != line.length()) {
                throw new IllegalStateException(
                        "STREAM_SOAK_SAMPLE_FRAME_INVALID");
            }
            String payload = line.substring(0, separator);
            String expected = line.substring(separator + 1);
            String actual = hex(sha256(
                    payload.getBytes(StandardCharsets.US_ASCII)));
            if (!MessageDigest.isEqual(
                    expected.getBytes(StandardCharsets.US_ASCII),
                    actual.getBytes(StandardCharsets.US_ASCII))) {
                throw new IllegalStateException(
                        "STREAM_SOAK_SAMPLE_CHECKSUM_MISMATCH");
            }
            result.add(parse(payload));
        }
        return Collections.unmodifiableList(result);
    }

    public static String fileSha256(Path path) throws IOException {
        return hex(sha256(Files.readAllBytes(path)));
    }

    private static String canonical(Map<String, String> fields) {
        if (fields == null || fields.isEmpty() || fields.size() > 128) {
            throw new IllegalArgumentException("sample fields invalid");
        }
        TreeMap<String, String> sorted = new TreeMap<>(fields);
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, String> field : sorted.entrySet()) {
            if (!field.getKey().matches("[a-z][a-z0-9_.-]{0,63}")
                    || field.getValue() == null
                    || field.getValue().length() > 4096) {
                throw new IllegalArgumentException("sample field invalid");
            }
            if (result.length() > 0) result.append('&');
            result.append(field.getKey()).append('=').append(
                    Base64.getUrlEncoder().withoutPadding().encodeToString(
                            field.getValue().getBytes(
                                    StandardCharsets.UTF_8)));
        }
        if (result.length() > 64 * 1024) {
            throw new IllegalArgumentException("sample too large");
        }
        return result.toString();
    }

    private static Map<String, String> parse(String payload) {
        Map<String, String> result = new LinkedHashMap<>();
        for (String item : payload.split("&")) {
            int separator = item.indexOf('=');
            if (separator < 1) {
                throw new IllegalStateException("STREAM_SOAK_SAMPLE_FRAME_INVALID");
            }
            String key = item.substring(0, separator);
            String value;
            try {
                value = new String(Base64.getUrlDecoder().decode(
                                item.substring(separator + 1)),
                        StandardCharsets.UTF_8);
            } catch (IllegalArgumentException malformed) {
                throw new IllegalStateException(
                        "STREAM_SOAK_SAMPLE_FRAME_INVALID", malformed);
            }
            if (result.put(key, value) != null) {
                throw new IllegalStateException("STREAM_SOAK_SAMPLE_DUPLICATE_FIELD");
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }

    @Override public synchronized void close() throws IOException {
        channel.close();
    }
}
