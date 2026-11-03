package io.rowweave.stream.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** Canonical, credential-free configuration for a signed Stream connector. */
public final class StreamConnectorConfiguration {
    public static final String CODEC_ID =
            "rowweave.stream.connector-config.v1";
    public static final int MAX_FRAME_BYTES = 1024 * 1024;
    private static final byte[] MAGIC =
            "ALSCCFG1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_OPTIONS = 128;
    private static final int MAX_REFERENCES = 64;
    private static final int MAX_OPTION_BYTES = 4096;
    private static final int MAX_REFERENCE_BYTES = 512;
    private static final Pattern NAME = Pattern.compile(
            "[A-Za-z][A-Za-z0-9._-]{0,127}");

    private final Map<String, String> options;
    private final Map<String, String> secretReferences;

    private StreamConnectorConfiguration(Map<String, String> options,
            Map<String, String> secretReferences) {
        this.options = Collections.unmodifiableMap(options);
        this.secretReferences = Collections.unmodifiableMap(secretReferences);
    }

    public static StreamConnectorConfiguration of(
            Map<String, String> options,
            Map<String, String> secretReferences) {
        if (options == null || secretReferences == null
                || options.size() > MAX_OPTIONS
                || secretReferences.size() > MAX_REFERENCES) {
            throw invalid();
        }
        Map<String, String> canonicalOptions = new TreeMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            requireName(entry.getKey());
            requireText(entry.getValue(), MAX_OPTION_BYTES, true);
            canonicalOptions.put(entry.getKey(), entry.getValue());
        }
        Map<String, String> canonicalReferences = new TreeMap<>();
        for (Map.Entry<String, String> entry : secretReferences.entrySet()) {
            requireName(entry.getKey());
            requireText(entry.getValue(), MAX_REFERENCE_BYTES, false);
            if (canonicalOptions.containsKey(entry.getKey())) {
                throw invalid();
            }
            canonicalReferences.put(entry.getKey(), entry.getValue());
        }
        return new StreamConnectorConfiguration(
                new LinkedHashMap<>(canonicalOptions),
                new LinkedHashMap<>(canonicalReferences));
    }

    public Map<String, String> options() { return options; }
    public Map<String, String> secretReferences() {
        return secretReferences;
    }

    public byte[] encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.write(MAGIC);
            output.writeShort(options.size());
            for (Map.Entry<String, String> entry : options.entrySet()) {
                writeText(output, entry.getKey());
                writeText(output, entry.getValue());
            }
            output.writeShort(secretReferences.size());
            for (Map.Entry<String, String> entry
                    : secretReferences.entrySet()) {
                writeText(output, entry.getKey());
                writeText(output, entry.getValue());
            }
            output.flush();
            byte[] frame = bytes.toByteArray();
            if (frame.length > MAX_FRAME_BYTES) throw invalid();
            return frame;
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    public static StreamConnectorConfiguration decode(byte[] frame) {
        if (frame == null || frame.length < MAGIC.length + 4
                || frame.length > MAX_FRAME_BYTES) {
            throw invalid();
        }
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame));
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(MAGIC, magic)) throw invalid();
            int optionCount = input.readUnsignedShort();
            if (optionCount > MAX_OPTIONS) throw invalid();
            Map<String, String> options = new LinkedHashMap<>();
            for (int index = 0; index < optionCount; index++) {
                String key = readText(input, 128);
                String value = readText(input, MAX_OPTION_BYTES);
                if (options.put(key, value) != null) throw invalid();
            }
            int referenceCount = input.readUnsignedShort();
            if (referenceCount > MAX_REFERENCES) throw invalid();
            Map<String, String> references = new LinkedHashMap<>();
            for (int index = 0; index < referenceCount; index++) {
                String key = readText(input, 128);
                String value = readText(input, MAX_REFERENCE_BYTES);
                if (references.put(key, value) != null) throw invalid();
            }
            if (input.available() != 0) throw invalid();
            StreamConnectorConfiguration decoded = of(options, references);
            if (!Arrays.equals(frame, decoded.encode())) throw invalid();
            return decoded;
        } catch (IOException malformed) {
            throw invalid();
        }
    }

    private static String readText(DataInputStream input, int maximum)
            throws IOException {
        int length = input.readUnsignedShort();
        if (length > maximum) throw invalid();
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw invalid();
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = utf8(value);
        output.writeShort(bytes.length);
        output.write(bytes);
    }

    private static void requireName(String value) {
        if (value == null || !NAME.matcher(value).matches()) throw invalid();
    }

    private static void requireText(String value, int maximum,
            boolean allowEmpty) {
        if (value == null || (!allowEmpty && value.isEmpty())) {
            throw invalid();
        }
        for (int index = 0; index < value.length(); index++) {
            if (Character.isISOControl(value.charAt(index))) {
                throw invalid();
            }
        }
        if (utf8(value).length > maximum) throw invalid();
    }

    private static byte[] utf8(String value) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException malformed) {
            throw invalid();
        }
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException(
                "STREAM_CONNECTOR_CONFIGURATION_INVALID");
    }
}
