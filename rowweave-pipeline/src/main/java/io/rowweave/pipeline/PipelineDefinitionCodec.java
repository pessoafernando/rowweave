package io.rowweave.pipeline;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactValidationException;

import java.io.ByteArrayInputStream;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.LongConsumer;

/** Deterministic, bounded wire codec for PipelineDefinition v1. */
public final class PipelineDefinitionCodec {
    private static final byte[] WIRE_MAGIC =
            "RWXXPIP1".getBytes(StandardCharsets.US_ASCII);
    public static final int ENVELOPE_VERSION = 1;
    private static final int HEADER_BYTES = WIRE_MAGIC.length + 4 + 4;
    private static final int MIN_FRAME_BYTES = HEADER_BYTES + PipelineLimits.SHA256_BYTES;
    public static final int MAX_FRAME_BYTES =
            HEADER_BYTES + PipelineLimits.MAX_BODY_BYTES + PipelineLimits.SHA256_BYTES;

    private PipelineDefinitionCodec() { }

    public static byte[] magic() { return WIRE_MAGIC.clone(); }

    public static byte[] encode(PipelineDefinition definition) {
        return encode(definition, ignored -> { });
    }

    /** The caller retains the reservation until the returned frame is consumed. */
    public static byte[] encode(
            PipelineDefinition definition, LongConsumer reservation) {
        if (definition == null) {
            throw PipelineText.failure("INVALID_DEFINITION", "pipeline",
                    "definition is required");
        }
        if (reservation == null) {
            throw new IllegalArgumentException("reservation is required");
        }
        int expectedBodyLength = encodedBodySize(definition);
        reservation.accept(multiplyExact(expectedBodyLength, 2L));
        byte[] body = encodeBody(definition, expectedBodyLength, reservation);
        long frameLength = addExact(HEADER_BYTES,
                addExact(body.length, PipelineLimits.SHA256_BYTES));
        reservation.accept(addExact(multiplyExact(frameLength, 2L),
                PipelineLimits.SHA256_BYTES));
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream((int) frameLength);
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.write(WIRE_MAGIC);
                out.writeInt(ENVELOPE_VERSION);
                out.writeInt(body.length);
                out.write(body);
                out.write(sha256(body));
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory pipeline encoding failed", impossible);
        }
    }

    public static PipelineDefinition decode(byte[] encoded) {
        return decode(encoded, ignored -> { });
    }

    /** Reservation is invoked before codec-owned arrays and decoded objects are allocated. */
    public static PipelineDefinition decode(
            byte[] encoded, LongConsumer reservation) {
        if (encoded == null || encoded.length < MIN_FRAME_BYTES) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "envelope",
                    "pipeline frame is shorter than header/checksum");
        }
        if (encoded.length > MAX_FRAME_BYTES) {
            throw PipelineText.failure("LIMIT_EXCEEDED", "envelope",
                    "pipeline frame exceeds limit");
        }
        if (reservation == null) {
            throw new IllegalArgumentException("reservation is required");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
            reservation.accept(WIRE_MAGIC.length);
            byte[] magic = new byte[WIRE_MAGIC.length];
            in.readFully(magic);
            if (!MessageDigest.isEqual(WIRE_MAGIC, magic)) {
                throw PipelineText.failure("INVALID_MAGIC", "envelope.magic",
                        "invalid pipeline magic");
            }
            int envelopeVersion = in.readInt();
            if (envelopeVersion != ENVELOPE_VERSION) {
                throw PipelineText.failure("UNSUPPORTED_ENVELOPE_VERSION",
                        "envelope.version", "unsupported envelope version: "
                                + envelopeVersion);
            }
            int bodyLength = in.readInt();
            if (bodyLength < 0 || bodyLength > PipelineLimits.MAX_BODY_BYTES) {
                throw PipelineText.failure("LIMIT_EXCEEDED", "envelope.bodyLength",
                        "invalid body length: " + bodyLength);
            }
            long expectedLength = addExact(HEADER_BYTES,
                    addExact(bodyLength, PipelineLimits.SHA256_BYTES));
            if (expectedLength != encoded.length) {
                throw PipelineText.failure("FRAME_LENGTH_MISMATCH",
                        "envelope.bodyLength",
                        "frame length does not match body length");
            }
            reservation.accept(PipelineLimits.SHA256_BYTES * 2L);
            byte[] expectedDigest = new byte[PipelineLimits.SHA256_BYTES];
            System.arraycopy(encoded, HEADER_BYTES + bodyLength,
                    expectedDigest, 0, expectedDigest.length);
            byte[] actualDigest = sha256(encoded, HEADER_BYTES, bodyLength);
            if (!MessageDigest.isEqual(expectedDigest, actualDigest)) {
                throw PipelineText.failure("CHECKSUM_MISMATCH", "envelope.checksum",
                        "pipeline body checksum mismatch");
            }
            reservation.accept(512L);
            return decodeBody(new DataInputStream(new ByteArrayInputStream(
                    encoded, HEADER_BYTES, bodyLength)), reservation);
        } catch (PipelineValidationException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "envelope",
                    "truncated pipeline frame");
        } catch (IOException impossible) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "envelope",
                    "failed to decode pipeline frame");
        }
    }

    static String definitionDigest(PipelineDefinition definition) {
        if (definition == null) {
            throw PipelineText.failure("INVALID_DEFINITION", "pipeline",
                    "definition is required");
        }
        return hex(sha256(encodeDigestBody(definition)));
    }

    private static byte[] encodeDigestBody(PipelineDefinition definition) {
        List<PipelineStep> steps = sorted(definition.steps(),
                Comparator.comparing(PipelineStep::stepId));
        List<PipelineEdge> edges = sorted(definition.edges(),
                Comparator.comparing(PipelineEdge::edgeId));
        List<PipelineOutput> outputs = sorted(definition.outputs(),
                Comparator.comparing(PipelineOutput::outputId));
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writeText(out, "rowweave.pipeline.definition-digest.v1",
                        "digest.domain", 64);
                out.writeInt(definition.formatVersion());
                out.writeInt(definition.minimumReaderVersion());
                writeText(out, definition.pipelineId(), "pipeline.pipelineId",
                        PipelineLimits.MAX_ID_BYTES);
                writeText(out, definition.revisionId(), "pipeline.revisionId",
                        PipelineLimits.MAX_ID_BYTES);
                writeStrings(out, definition.requiredFeatures(),
                        "pipeline.requiredFeatures");
                writeStrings(out, definition.optionalFeatures(),
                        "pipeline.optionalFeatures");
                out.writeInt(steps.size());
                for (int i = 0; i < steps.size(); i++) {
                    PipelineStep step = steps.get(i);
                    writeText(out, step.stepId(), "pipeline.steps[" + i + "].stepId",
                            PipelineLimits.MAX_ID_BYTES);
                    writeText(out, step.artifact().artifactId(),
                            "pipeline.steps[" + i + "].artifactId", 64);
                }
                writeEdges(out, edges);
                writeOutputs(out, outputs);
            }
            byte[] body = bytes.toByteArray();
            requireBodyLimit(body.length);
            return body;
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory pipeline digest encoding failed", impossible);
        }
    }

    private static byte[] encodeBody(
            PipelineDefinition definition, int expectedBodyLength,
            LongConsumer reservation) {
        List<PipelineStep> steps = sorted(definition.steps(),
                Comparator.comparing(PipelineStep::stepId));
        List<PipelineEdge> edges = sorted(definition.edges(),
                Comparator.comparing(PipelineEdge::edgeId));
        List<PipelineOutput> outputs = sorted(definition.outputs(),
                Comparator.comparing(PipelineOutput::outputId));
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(expectedBodyLength);
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(definition.formatVersion());
                out.writeInt(definition.minimumReaderVersion());
                writeText(out, definition.pipelineId(), "pipeline.pipelineId",
                        PipelineLimits.MAX_ID_BYTES);
                writeText(out, definition.revisionId(), "pipeline.revisionId",
                        PipelineLimits.MAX_ID_BYTES);
                writeStrings(out, definition.requiredFeatures(),
                        "pipeline.requiredFeatures");
                writeStrings(out, definition.optionalFeatures(),
                        "pipeline.optionalFeatures");
                out.writeInt(steps.size());
                for (int i = 0; i < steps.size(); i++) {
                    PipelineStep step = steps.get(i);
                    writeText(out, step.stepId(), "pipeline.steps[" + i + "].stepId",
                            PipelineLimits.MAX_ID_BYTES);
                    byte[] artifact = PlanArtifactCodec.encode(
                            step.artifact(), reservation);
                    out.writeInt(artifact.length);
                    out.write(artifact);
                    requireBodyLimit(bytes.size());
                }
                writeEdges(out, edges);
                writeOutputs(out, outputs);
            }
            byte[] body = bytes.toByteArray();
            if (body.length != expectedBodyLength) {
                throw new IllegalStateException(
                        "pipeline encoded size preflight mismatch: expected="
                                + expectedBodyLength + " actual=" + body.length);
            }
            return body;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory pipeline body encoding failed", impossible);
        }
    }

    private static void writeEdges(
            DataOutputStream out, List<PipelineEdge> edges) throws IOException {
        out.writeInt(edges.size());
        for (int i = 0; i < edges.size(); i++) {
            PipelineEdge edge = edges.get(i);
            String path = "pipeline.edges[" + i + "]";
            writeText(out, edge.edgeId(), path + ".edgeId", PipelineLimits.MAX_ID_BYTES);
            writeText(out, edge.producerStepId(), path + ".producerStepId", PipelineLimits.MAX_ID_BYTES);
            writeText(out, edge.producerSinkOperatorId(), path + ".producerSinkOperatorId", PipelineLimits.MAX_ID_BYTES);
            writeText(out, edge.consumerStepId(), path + ".consumerStepId", PipelineLimits.MAX_ID_BYTES);
            writeText(out, edge.consumerSourceOperatorId(), path + ".consumerSourceOperatorId", PipelineLimits.MAX_ID_BYTES);
            writeText(out, edge.materializationId(), path + ".materializationId", PipelineLimits.MAX_ID_BYTES);
            writeText(out, edge.schemaFingerprint(), path + ".schemaFingerprint", 64);
            out.writeInt(boundaryWireId(edge.boundaryKind()));
        }
    }

    private static void writeOutputs(
            DataOutputStream out, List<PipelineOutput> outputs) throws IOException {
        out.writeInt(outputs.size());
        for (int i = 0; i < outputs.size(); i++) {
            PipelineOutput output = outputs.get(i);
            String path = "pipeline.outputs[" + i + "]";
            writeText(out, output.outputId(), path + ".outputId", PipelineLimits.MAX_ID_BYTES);
            writeText(out, output.stepId(), path + ".stepId", PipelineLimits.MAX_ID_BYTES);
            writeText(out, output.sinkOperatorId(), path + ".sinkOperatorId", PipelineLimits.MAX_ID_BYTES);
        }
    }

    private static PipelineDefinition decodeBody(
            DataInputStream in, LongConsumer reservation) throws IOException {
        try (DataInputStream body = in) {
            int formatVersion = body.readInt();
            int minimumReaderVersion = body.readInt();
            String pipelineId = readText(body, "pipeline.pipelineId",
                    PipelineLimits.MAX_ID_BYTES, reservation);
            String revisionId = readText(body, "pipeline.revisionId",
                    PipelineLimits.MAX_ID_BYTES, reservation);
            List<String> required = readStrings(body, "pipeline.requiredFeatures",
                    PipelineLimits.MAX_FEATURES, PipelineLimits.MAX_FEATURE_BYTES,
                    reservation);
            List<String> optional = readStrings(body, "pipeline.optionalFeatures",
                    PipelineLimits.MAX_FEATURES, PipelineLimits.MAX_FEATURE_BYTES,
                    reservation);
            int stepCount = readCount(body, "pipeline.steps", PipelineLimits.MAX_STEPS);
            if (stepCount == 0) {
                throw PipelineText.failure("LIMIT_EXCEEDED", "pipeline.steps",
                        "Pipeline requires at least one step");
            }
            reservation.accept(multiplyExact(stepCount, 64L));
            List<PipelineStep> steps = new ArrayList<>(stepCount);
            for (int i = 0; i < stepCount; i++) {
                String path = "pipeline.steps[" + i + "]";
                String stepId = readText(body, path + ".stepId",
                        PipelineLimits.MAX_ID_BYTES, reservation);
                int artifactLength = readCount(body, path + ".artifact",
                        PlanArtifactCodec.MAX_FRAME_BYTES);
                if (artifactLength > body.available()) {
                    throw PipelineText.failure("FRAME_LENGTH_MISMATCH",
                            path + ".artifact", "truncated artifact frame");
                }
                reservation.accept(artifactLength);
                byte[] artifactBytes = new byte[artifactLength];
                body.readFully(artifactBytes);
                final PlanArtifact artifact;
                try {
                    artifact = PlanArtifactCodec.decode(artifactBytes, reservation);
                } catch (PlanArtifactValidationException invalid) {
                    throw PipelineText.failure("INVALID_ARTIFACT", path + ".artifact",
                            invalid.code() + " at " + invalid.path());
                }
                reservation.accept(96L);
                steps.add(new PipelineStep(stepId, artifact));
            }
            int edgeCount = readCount(body, "pipeline.edges", PipelineLimits.MAX_EDGES);
            reservation.accept(multiplyExact(edgeCount, 192L));
            List<PipelineEdge> edges = new ArrayList<>(edgeCount);
            for (int i = 0; i < edgeCount; i++) {
                String path = "pipeline.edges[" + i + "]";
                edges.add(new PipelineEdge(
                        readText(body, path + ".edgeId", PipelineLimits.MAX_ID_BYTES, reservation),
                        readText(body, path + ".producerStepId", PipelineLimits.MAX_ID_BYTES, reservation),
                        readText(body, path + ".producerSinkOperatorId", PipelineLimits.MAX_ID_BYTES, reservation),
                        readText(body, path + ".consumerStepId", PipelineLimits.MAX_ID_BYTES, reservation),
                        readText(body, path + ".consumerSourceOperatorId", PipelineLimits.MAX_ID_BYTES, reservation),
                        readText(body, path + ".materializationId", PipelineLimits.MAX_ID_BYTES, reservation),
                        readText(body, path + ".schemaFingerprint", 64, reservation),
                        boundaryFromWire(body.readInt(), path + ".boundaryKind")));
            }
            int outputCount = readCount(body, "pipeline.outputs", PipelineLimits.MAX_OUTPUTS);
            if (outputCount == 0) {
                throw PipelineText.failure("LIMIT_EXCEEDED", "pipeline.outputs",
                        "Pipeline requires at least one output");
            }
            reservation.accept(multiplyExact(outputCount, 96L));
            List<PipelineOutput> outputs = new ArrayList<>(outputCount);
            for (int i = 0; i < outputCount; i++) {
                String path = "pipeline.outputs[" + i + "]";
                outputs.add(new PipelineOutput(
                        readText(body, path + ".outputId", PipelineLimits.MAX_ID_BYTES, reservation),
                        readText(body, path + ".stepId", PipelineLimits.MAX_ID_BYTES, reservation),
                        readText(body, path + ".sinkOperatorId", PipelineLimits.MAX_ID_BYTES, reservation)));
            }
            if (body.available() != 0) {
                throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "envelope.body",
                        "pipeline body has trailing bytes");
            }
            return new PipelineDefinition(formatVersion, minimumReaderVersion,
                    pipelineId, revisionId, required, optional, steps, edges, outputs);
        } catch (EOFException truncated) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "envelope.body",
                    "truncated pipeline body");
        }
    }

    private static void writeStrings(
            DataOutputStream out, List<String> values, String path) throws IOException {
        out.writeInt(values.size());
        for (int i = 0; i < values.size(); i++) {
            writeText(out, values.get(i), path + "[" + i + "]",
                    PipelineLimits.MAX_FEATURE_BYTES);
        }
    }

    private static List<String> readStrings(
            DataInputStream in, String path, int maxCount, int maxTextBytes,
            LongConsumer reservation) throws IOException {
        int count = readCount(in, path, maxCount);
        reservation.accept(multiplyExact(count, 32L));
        List<String> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(readText(in, path + "[" + i + "]", maxTextBytes, reservation));
        }
        return values;
    }

    private static void writeText(
            DataOutputStream out, String value, String path, int maxBytes)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > maxBytes) {
            throw PipelineText.failure("LIMIT_EXCEEDED", path,
                    "UTF-8 value exceeds byte limit");
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readText(
            DataInputStream in, String path, int maxBytes, LongConsumer reservation)
            throws IOException {
        int length = readCount(in, path, maxBytes);
        if (length == 0) {
            throw PipelineText.failure("INVALID_ID", path, "value is required");
        }
        if (length > in.available()) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", path,
                    "truncated UTF-8 value");
        }
        reservation.accept(length);
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        final String value;
        try {
            value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException invalid) {
            throw PipelineText.failure("INVALID_UTF8", path,
                    "value is not canonical UTF-8");
        }
        return PipelineText.required(value, path, maxBytes);
    }

    private static int readCount(DataInputStream in, String path, int maximum)
            throws IOException {
        int count = in.readInt();
        if (count < 0 || count > maximum) {
            throw PipelineText.failure("LIMIT_EXCEEDED", path,
                    "count or length exceeds limit");
        }
        return count;
    }

    private static int boundaryWireId(PipelineEdge.BoundaryKind kind) {
        if (kind == PipelineEdge.BoundaryKind.VERSIONED_DATASET) return 1;
        throw PipelineText.failure("INVALID_BOUNDARY", "edge.boundaryKind",
                "unsupported boundary kind");
    }

    private static PipelineEdge.BoundaryKind boundaryFromWire(int wireId, String path) {
        if (wireId == 1) return PipelineEdge.BoundaryKind.VERSIONED_DATASET;
        throw PipelineText.failure("INVALID_BOUNDARY", path,
                "unsupported boundary wire ID: " + wireId);
    }

    private static <T> List<T> sorted(List<T> values, Comparator<T> comparator) {
        List<T> copy = new ArrayList<>(values);
        copy.sort(comparator);
        return copy;
    }

    private static void requireBodyLimit(int length) {
        if (length > PipelineLimits.MAX_BODY_BYTES) {
            throw PipelineText.failure("LIMIT_EXCEEDED", "envelope.bodyLength",
                    "pipeline body exceeds limit");
        }
    }

    private static int encodedBodySize(PipelineDefinition definition) {
        long bytes = 8L;
        bytes = addExact(bytes, encodedTextSize(definition.pipelineId()));
        bytes = addExact(bytes, encodedTextSize(definition.revisionId()));
        bytes = addExact(bytes, 4L);
        for (String feature : definition.requiredFeatures()) {
            bytes = addExact(bytes, encodedTextSize(feature));
        }
        bytes = addExact(bytes, 4L);
        for (String feature : definition.optionalFeatures()) {
            bytes = addExact(bytes, encodedTextSize(feature));
        }
        bytes = addExact(bytes, 4L);
        for (PipelineStep step : definition.steps()) {
            bytes = addExact(bytes, encodedTextSize(step.stepId()));
            bytes = addExact(bytes,
                    addExact(4L, PlanArtifactCodec.encodedFrameSize(step.artifact())));
        }
        bytes = addExact(bytes, 4L);
        for (PipelineEdge edge : definition.edges()) {
            bytes = addExact(bytes, encodedTextSize(edge.edgeId()));
            bytes = addExact(bytes, encodedTextSize(edge.producerStepId()));
            bytes = addExact(bytes, encodedTextSize(edge.producerSinkOperatorId()));
            bytes = addExact(bytes, encodedTextSize(edge.consumerStepId()));
            bytes = addExact(bytes, encodedTextSize(edge.consumerSourceOperatorId()));
            bytes = addExact(bytes, encodedTextSize(edge.materializationId()));
            bytes = addExact(bytes, encodedTextSize(edge.schemaFingerprint()));
            bytes = addExact(bytes, 4L);
        }
        bytes = addExact(bytes, 4L);
        for (PipelineOutput output : definition.outputs()) {
            bytes = addExact(bytes, encodedTextSize(output.outputId()));
            bytes = addExact(bytes, encodedTextSize(output.stepId()));
            bytes = addExact(bytes, encodedTextSize(output.sinkOperatorId()));
        }
        if (bytes > PipelineLimits.MAX_BODY_BYTES) {
            throw PipelineText.failure("LIMIT_EXCEEDED", "envelope.bodyLength",
                    "pipeline body exceeds limit");
        }
        return (int) bytes;
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

    private static long addExact(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            throw new IllegalArgumentException("pipeline resident estimate overflow");
        }
        return left + right;
    }

    private static long multiplyExact(long left, long right) {
        if (left < 0L || right < 0L || (left != 0L && right > Long.MAX_VALUE / left)) {
            throw new IllegalArgumentException("pipeline resident estimate overflow");
        }
        return left * right;
    }

    private static byte[] sha256(byte[] bytes) {
        return sha256(bytes, 0, bytes.length);
    }

    private static byte[] sha256(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required", impossible);
        }
    }

    private static String hex(byte[] bytes) {
        char[] output = new char[bytes.length * 2];
        char[] alphabet = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            output[i * 2] = alphabet[(bytes[i] >>> 4) & 0xf];
            output[i * 2 + 1] = alphabet[bytes[i] & 0xf];
        }
        return new String(output);
    }
}
