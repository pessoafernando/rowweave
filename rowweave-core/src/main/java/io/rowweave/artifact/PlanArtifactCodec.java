package io.rowweave.artifact;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/** Deterministic, bounded codec for the version 1 Plan Artifact envelope. */
public final class PlanArtifactCodec {

    private static final byte[] WIRE_MAGIC = "RWXXPA01".getBytes(StandardCharsets.US_ASCII);
    public static final int ENVELOPE_VERSION = 1;
    private static final int HEADER_BYTES = WIRE_MAGIC.length + 4 + 4;
    private static final int MIN_FRAME_BYTES = HEADER_BYTES + PlanArtifactLimits.SHA256_BYTES;
    public static final int MAX_FRAME_BYTES =
            HEADER_BYTES + PlanArtifactLimits.MAX_BODY_BYTES + PlanArtifactLimits.SHA256_BYTES;

    private PlanArtifactCodec() {
    }

    public static byte[] magic() {
        return WIRE_MAGIC.clone();
    }

    public static byte[] encode(PlanArtifact artifact) {
        return encode(artifact, value -> { });
    }

    /** Exact encoded frame size without materializing the body or payload bytes. */
    public static int encodedFrameSize(PlanArtifact artifact) {
        if (artifact == null) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "plan", "artifact is required");
        }
        long bodyBytes = encodedBodySize(artifact);
        if (bodyBytes > PlanArtifactLimits.MAX_BODY_BYTES) {
            throw ArtifactText.failure("LIMIT_EXCEEDED", "envelope.bodyLength",
                    "body exceeds limit");
        }
        long frameBytes = addExact(HEADER_BYTES,
                addExact(bodyBytes, PlanArtifactLimits.SHA256_BYTES));
        if (frameBytes > Integer.MAX_VALUE) {
            throw ArtifactText.failure("LIMIT_EXCEEDED", "envelope",
                    "artifact frame exceeds integer capacity");
        }
        return (int) frameBytes;
    }

    /** The caller holds the reservation until the RPC or repository has finished consuming the encoded bytes. */
    public static byte[] encode(PlanArtifact artifact,
                                java.util.function.LongConsumer reservation) {
        if (artifact == null) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "plan", "artifact is required");
        }
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        // Conservative coexistence bound across body-buffer growth, the toByteArray copy and per-field UTF-8 scratch.
        reservation.accept(multiplyExact(4L, encodeBodyUpperBound(artifact)));
        byte[] body = encodeBody(artifact);
        if (body.length > PlanArtifactLimits.MAX_BODY_BYTES) {
            throw ArtifactText.failure("LIMIT_EXCEEDED", "envelope.bodyLength", "body exceeds limit");
        }
        long frameBytes = HEADER_BYTES + (long) body.length + PlanArtifactLimits.SHA256_BYTES;
        reservation.accept(addExact(multiplyExact(2L, frameBytes),
                PlanArtifactLimits.SHA256_BYTES));
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    HEADER_BYTES + body.length + PlanArtifactLimits.SHA256_BYTES);
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.write(WIRE_MAGIC);
                out.writeInt(ENVELOPE_VERSION);
                out.writeInt(body.length);
                out.write(body);
                out.write(sha256(body));
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory artifact encoding failed", impossible);
        }
    }

    private static long encodeBodyUpperBound(PlanArtifact artifact) {
        long bytes = 64L + artifact.payloadUnsafe().length;
        bytes = addExact(bytes, textUpperBound(artifact.definitionId()));
        for (String value : artifact.requiredFeatures()) bytes = addExact(bytes, textUpperBound(value));
        for (String value : artifact.optionalFeatures()) bytes = addExact(bytes, textUpperBound(value));
        for (OperatorDescriptor operator : artifact.operators()) {
            bytes = addExact(bytes, 32L);
            bytes = addExact(bytes, textUpperBound(operator.operatorId()));
            bytes = addExact(bytes, textUpperBound(operator.typeId()));
            bytes = addExact(bytes, textUpperBound(operator.schemaFingerprint()));
            for (String input : operator.inputOperatorIds()) {
                bytes = addExact(bytes, textUpperBound(input));
            }
        }
        return bytes;
    }

    private static long encodedBodySize(PlanArtifact artifact) {
        long bytes = 8L;
        bytes = addExact(bytes, encodedTextSize(artifact.definitionId()));
        bytes = addExact(bytes, 16L);
        bytes = addExact(bytes, encodedStringsSize(artifact.requiredFeatures()));
        bytes = addExact(bytes, encodedStringsSize(artifact.optionalFeatures()));
        bytes = addExact(bytes, 4L);
        for (OperatorDescriptor operator : artifact.operators()) {
            bytes = addExact(bytes, encodedTextSize(operator.operatorId()));
            bytes = addExact(bytes, 4L);
            bytes = addExact(bytes, encodedTextSize(operator.typeId()));
            bytes = addExact(bytes, encodedTextSize(operator.schemaFingerprint()));
            bytes = addExact(bytes, encodedStringsSize(operator.inputOperatorIds()));
        }
        bytes = addExact(bytes, 8L);
        return addExact(bytes, artifact.payloadSize());
    }

    private static long encodedStringsSize(List<String> values) {
        long bytes = 4L;
        for (String value : values) {
            bytes = addExact(bytes, encodedTextSize(value));
        }
        return bytes;
    }

    private static long encodedTextSize(String value) {
        return addExact(4L, utf8Length(value));
    }

    private static int utf8Length(String value) {
        int bytes = 0;
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current <= 0x7f) {
                bytes++;
            } else if (current <= 0x7ff) {
                bytes += 2;
            } else if (Character.isHighSurrogate(current)
                    && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                bytes += 4;
                i++;
            } else {
                bytes += 3;
            }
        }
        return bytes;
    }

    private static long textUpperBound(String value) {
        return 4L + (value == null ? 0L : multiplyExact(3L, value.length()));
    }

    private static long addExact(long left, long right) {
        if (right < 0L || left > Long.MAX_VALUE - right) {
            throw new IllegalArgumentException("artifact resident estimate overflow");
        }
        return left + right;
    }

    private static long multiplyExact(long left, long right) {
        if (left < 0L || right < 0L || (left != 0L && right > Long.MAX_VALUE / left)) {
            throw new IllegalArgumentException("artifact resident estimate overflow");
        }
        return left * right;
    }

    public static PlanArtifact decode(byte[] encoded) {
        return decode(encoded, value -> { });
    }

    /** The reservation is called before each decode buffer or object is allocated. */
    public static PlanArtifact decode(byte[] encoded, java.util.function.LongConsumer reservation) {
        if (encoded == null || encoded.length < MIN_FRAME_BYTES) {
            throw ArtifactText.failure("FRAME_LENGTH_MISMATCH", "envelope",
                    "artifact frame is shorter than minimum header/checksum");
        }
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
            reservation.accept(WIRE_MAGIC.length);
            byte[] magic = new byte[WIRE_MAGIC.length];
            in.readFully(magic);
            if (!MessageDigest.isEqual(WIRE_MAGIC, magic)) {
                throw ArtifactText.failure("INVALID_MAGIC", "envelope.magic", "invalid artifact magic");
            }
            int envelopeVersion = in.readInt();
            if (envelopeVersion != ENVELOPE_VERSION) {
                throw ArtifactText.failure("UNSUPPORTED_ENVELOPE_VERSION", "envelope.version",
                        "unsupported envelope version: " + envelopeVersion);
            }
            int bodyLength = in.readInt();
            if (bodyLength < 0 || bodyLength > PlanArtifactLimits.MAX_BODY_BYTES) {
                throw ArtifactText.failure("LIMIT_EXCEEDED", "envelope.bodyLength",
                        "invalid body length: " + bodyLength);
            }
            long expectedLength = (long) HEADER_BYTES + bodyLength + PlanArtifactLimits.SHA256_BYTES;
            if (expectedLength != encoded.length) {
                throw ArtifactText.failure("FRAME_LENGTH_MISMATCH", "envelope.bodyLength",
                        "frame length does not match body length");
            }
            reservation.accept(PlanArtifactLimits.SHA256_BYTES);
            byte[] expectedDigest = new byte[PlanArtifactLimits.SHA256_BYTES];
            System.arraycopy(encoded, HEADER_BYTES + bodyLength,
                    expectedDigest, 0, expectedDigest.length);
            reservation.accept(PlanArtifactLimits.SHA256_BYTES);
            byte[] actualDigest = sha256(encoded, HEADER_BYTES, bodyLength);
            if (!MessageDigest.isEqual(expectedDigest, actualDigest)) {
                throw ArtifactText.failure("CHECKSUM_MISMATCH", "envelope.checksum",
                        "artifact body checksum mismatch");
            }
            reservation.accept(512L); // artifact root + immutable collection wrappers
            PlanArtifact artifact = decodeBody(
                    new DataInputStream(new ByteArrayInputStream(encoded, HEADER_BYTES, bodyLength)),
                    hex(actualDigest), reservation);
            PlanArtifactValidator.validateReadable(artifact);
            return artifact;
        } catch (PlanArtifactValidationException validation) {
            throw validation;
        } catch (EOFException truncated) {
            throw new PlanArtifactValidationException("FRAME_LENGTH_MISMATCH", "envelope",
                    "truncated artifact frame", truncated);
        } catch (IOException impossible) {
            throw new PlanArtifactValidationException("FRAME_LENGTH_MISMATCH", "envelope",
                    "failed to decode artifact frame", impossible);
        }
    }

    public static String artifactId(byte[] encoded) {
        return decode(encoded).artifactId();
    }

    static byte[] encodeBody(PlanArtifact artifact) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(artifact.planSchemaVersion());
                out.writeInt(artifact.minimumReaderVersion());
                writeText(out, artifact.definitionId(), "plan.definitionId", PlanArtifactLimits.MAX_TEXT_BYTES);
                out.writeInt(artifact.runtimeTarget().wireId());
                out.writeInt(artifact.boundedness().wireId());
                out.writeInt(artifact.changelogMode().wireId());
                out.writeInt(artifact.timeMode().wireId());
                writeStrings(out, artifact.requiredFeatures(), "plan.requiredFeatures",
                        PlanArtifactLimits.MAX_FEATURES, PlanArtifactLimits.MAX_TEXT_BYTES);
                writeStrings(out, artifact.optionalFeatures(), "plan.optionalFeatures",
                        PlanArtifactLimits.MAX_FEATURES, PlanArtifactLimits.MAX_TEXT_BYTES);
                out.writeInt(artifact.operators().size());
                for (int i = 0; i < artifact.operators().size(); i++) {
                    OperatorDescriptor operator = artifact.operators().get(i);
                    String path = "operators[" + i + "]";
                    writeText(out, operator.operatorId(), path + ".operatorId", PlanArtifactLimits.MAX_TEXT_BYTES);
                    out.writeInt(operator.kind().wireId());
                    writeText(out, operator.typeId(), path + ".typeId", PlanArtifactLimits.MAX_TEXT_BYTES);
                    writeText(out, operator.schemaFingerprint(), path + ".schemaFingerprint",
                            PlanArtifactLimits.MAX_SCHEMA_FINGERPRINT_BYTES);
                    writeStrings(out, operator.inputOperatorIds(), path + ".inputOperatorIds",
                            PlanArtifactLimits.MAX_OPERATOR_INPUTS, PlanArtifactLimits.MAX_TEXT_BYTES);
                }
                out.writeInt(artifact.payloadFormat().wireId());
                out.writeInt(artifact.payloadUnsafe().length);
                out.write(artifact.payloadUnsafe());
            }
            byte[] body = bytes.toByteArray();
            if (body.length > PlanArtifactLimits.MAX_BODY_BYTES) {
                throw ArtifactText.failure("LIMIT_EXCEEDED", "envelope.bodyLength", "body exceeds limit");
            }
            return body;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory artifact body encoding failed", impossible);
        }
    }

    private static PlanArtifact decodeBody(DataInputStream in, String verifiedArtifactId,
                                           java.util.function.LongConsumer reservation) throws IOException {
        try (DataInputStream bodyInput = in) {
            int planSchemaVersion = bodyInput.readInt();
            int minimumReaderVersion = bodyInput.readInt();
            String definitionId = readText(bodyInput, "plan.definitionId",
                    PlanArtifactLimits.MAX_TEXT_BYTES, reservation);
            PlanArtifact.RuntimeTarget runtime = PlanArtifact.RuntimeTarget.fromWire(
                    bodyInput.readInt(), "plan.runtimeTarget");
            PlanArtifact.Boundedness boundedness = PlanArtifact.Boundedness.fromWire(
                    bodyInput.readInt(), "plan.boundedness");
            PlanArtifact.ChangelogMode changelog = PlanArtifact.ChangelogMode.fromWire(
                    bodyInput.readInt(), "plan.changelogMode");
            PlanArtifact.TimeMode time = PlanArtifact.TimeMode.fromWire(
                    bodyInput.readInt(), "plan.timeMode");
            List<String> required = readCanonicalStrings(bodyInput, "plan.requiredFeatures",
                    PlanArtifactLimits.MAX_FEATURES, PlanArtifactLimits.MAX_TEXT_BYTES, reservation);
            List<String> optional = readCanonicalStrings(bodyInput, "plan.optionalFeatures",
                    PlanArtifactLimits.MAX_FEATURES, PlanArtifactLimits.MAX_TEXT_BYTES, reservation);
            int operatorCount = readCount(bodyInput, "operators", PlanArtifactLimits.MAX_OPERATORS);
            if (operatorCount == 0) {
                throw ArtifactText.failure("INVALID_SEMANTICS", "operators", "artifact requires operators");
            }
            reservation.accept(128L * operatorCount);
            List<OperatorDescriptor> operators = new ArrayList<>(operatorCount);
            for (int i = 0; i < operatorCount; i++) {
                String path = "operators[" + i + "]";
                String operatorId = readText(bodyInput, path + ".operatorId",
                        PlanArtifactLimits.MAX_TEXT_BYTES, reservation);
                OperatorDescriptor.Kind kind = OperatorDescriptor.Kind.fromWire(
                        bodyInput.readInt(), path + ".kind");
                String typeId = readText(bodyInput, path + ".typeId",
                        PlanArtifactLimits.MAX_TEXT_BYTES, reservation);
                String schemaFingerprint = readText(bodyInput, path + ".schemaFingerprint",
                        PlanArtifactLimits.MAX_SCHEMA_FINGERPRINT_BYTES, reservation);
                List<String> inputs = readStrings(bodyInput, path + ".inputOperatorIds",
                        PlanArtifactLimits.MAX_OPERATOR_INPUTS, PlanArtifactLimits.MAX_TEXT_BYTES,
                        reservation);
                reservation.accept(512L);
                operators.add(new OperatorDescriptor(operatorId, kind, typeId, schemaFingerprint, inputs));
            }
            PlanArtifact.PayloadFormat payloadFormat = PlanArtifact.PayloadFormat.fromWire(
                    bodyInput.readInt(), "payload.format");
            int payloadLength = readCount(bodyInput, "payload.bytes", PlanArtifactLimits.MAX_PAYLOAD_BYTES);
            if (payloadLength > bodyInput.available()) {
                throw ArtifactText.failure("FRAME_LENGTH_MISMATCH", "payload.bytes", "truncated payload");
            }
            reservation.accept(payloadLength);
            byte[] payload = new byte[payloadLength];
            bodyInput.readFully(payload);
            if (bodyInput.available() != 0) {
                throw ArtifactText.failure("FRAME_LENGTH_MISMATCH", "envelope.body",
                        "artifact body has trailing bytes");
            }
            return PlanArtifact.decoded(definitionId, planSchemaVersion, minimumReaderVersion,
                    runtime, boundedness, changelog, time, required, optional,
                    operators, payloadFormat, payload, verifiedArtifactId);
        } catch (EOFException truncated) {
            throw new PlanArtifactValidationException("FRAME_LENGTH_MISMATCH", "envelope.body",
                    "truncated artifact body", truncated);
        }
    }

    private static void writeStrings(
            DataOutputStream out, List<String> values, String path, int maxCount, int maxBytes) throws IOException {
        if (values.size() > maxCount) {
            throw ArtifactText.failure("LIMIT_EXCEEDED", path, "string count exceeds limit");
        }
        out.writeInt(values.size());
        for (int i = 0; i < values.size(); i++) {
            writeText(out, values.get(i), path + "[" + i + "]", maxBytes);
        }
    }

    private static List<String> readCanonicalStrings(
            DataInputStream in, String path, int maxCount, int maxBytes,
            java.util.function.LongConsumer reservation) throws IOException {
        List<String> values = readStrings(in, path, maxCount, maxBytes, reservation);
        String previous = null;
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            if (previous != null && previous.compareTo(value) >= 0) {
                String code = previous.equals(value) ? "DUPLICATE_FEATURE" : "NON_CANONICAL_TEXT";
                throw ArtifactText.failure(code, path + "[" + i + "]",
                        "feature set must be strictly sorted without duplicates");
            }
            previous = value;
        }
        return values;
    }

    private static List<String> readStrings(
            DataInputStream in, String path, int maxCount, int maxBytes,
            java.util.function.LongConsumer reservation) throws IOException {
        int count = readCount(in, path, maxCount);
        reservation.accept(128L * count);
        List<String> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(readText(in, path + "[" + i + "]", maxBytes, reservation));
        }
        return values;
    }

    private static int readCount(DataInputStream in, String path, int max) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > max) {
            throw ArtifactText.failure("LIMIT_EXCEEDED", path, "invalid count/length: " + count);
        }
        return count;
    }

    private static void writeText(DataOutputStream out, String value, String path, int maxBytes) throws IOException {
        byte[] encoded = ArtifactText.encode(value, path, maxBytes);
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static String readText(DataInputStream in, String path, int maxBytes,
                                   java.util.function.LongConsumer reservation) throws IOException {
        int length = readCount(in, path, maxBytes);
        if (length > in.available()) {
            throw ArtifactText.failure("FRAME_LENGTH_MISMATCH", path, "truncated UTF-8 text");
        }
        reservation.accept(3L * length);
        byte[] encoded = new byte[length];
        in.readFully(encoded);
        return ArtifactText.decode(encoded, path, maxBytes);
    }

    static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
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

    static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            result[i * 2] = digits[(bytes[i] >>> 4) & 0x0f];
            result[i * 2 + 1] = digits[bytes[i] & 0x0f];
        }
        return new String(result);
    }
}
