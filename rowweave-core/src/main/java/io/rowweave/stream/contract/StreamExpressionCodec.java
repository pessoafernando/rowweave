package io.rowweave.stream.contract;

import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamExpressionException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/** Canonical checksummed codec for one bounded Stream expression tree. */
public final class StreamExpressionCodec {
    private static final byte[] MAGIC = "RWXXEXP1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = MAGIC.length + 8;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_FRAME_BYTES = 1024 * 1024;

    private StreamExpressionCodec() { }

    public static byte[] encode(StreamExpression expression) {
        if (expression == null) throw fail("INVALID_EXPRESSION", "expression is required");
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                writeExpression(body, expression);
            }
            byte[] payload = bodyBytes.toByteArray();
            if (payload.length > MAX_FRAME_BYTES - HEADER_BYTES - DIGEST_BYTES) {
                throw fail("EXPRESSION_LIMIT_EXCEEDED", "expression frame exceeds limit");
            }
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(
                    HEADER_BYTES + payload.length + DIGEST_BYTES);
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.write(MAGIC);
                frame.writeInt(VERSION);
                frame.writeInt(payload.length);
                frame.write(payload);
                frame.write(sha256(payload));
            }
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static StreamExpression decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw fail("FRAME_LENGTH_MISMATCH", "expression frame length is invalid");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!MessageDigest.isEqual(magic, MAGIC)) {
                throw fail("INVALID_MAGIC", "expression frame magic is invalid");
            }
            if (input.readInt() != VERSION) {
                throw fail("UNSUPPORTED_EXPRESSION_VERSION",
                        "expression version is unsupported");
            }
            int length = input.readInt();
            if (length < 0 || HEADER_BYTES + (long) length + DIGEST_BYTES
                    != frame.length) {
                throw fail("FRAME_LENGTH_MISMATCH", "expression body length is invalid");
            }
            byte[] payload = new byte[length];
            input.readFully(payload);
            byte[] expected = new byte[DIGEST_BYTES];
            input.readFully(expected);
            if (!MessageDigest.isEqual(expected, sha256(payload))) {
                throw fail("CHECKSUM_MISMATCH", "expression checksum differs");
            }
            try (DataInputStream body = new DataInputStream(
                    new ByteArrayInputStream(payload))) {
                Counter counter = new Counter();
                StreamExpression expression = readExpression(body, 1, counter);
                if (body.available() != 0) {
                    throw fail("FRAME_LENGTH_MISMATCH",
                            "expression body has trailing bytes");
                }
                return expression;
            }
        } catch (StreamContractException failure) {
            throw failure;
        } catch (StreamExpressionException failure) {
            throw fail(failure.code(), failure.getMessage());
        } catch (EOFException truncated) {
            throw fail("FRAME_LENGTH_MISMATCH", "expression frame is truncated");
        } catch (IOException malformed) {
            throw fail("FRAME_LENGTH_MISMATCH", "expression frame is malformed");
        }
    }

    private static void writeExpression(DataOutputStream output,
            StreamExpression expression) throws IOException {
        output.writeInt(expression.kind().wireId());
        switch (expression.kind()) {
            case INPUT:
                output.writeInt(expression.dataType().wireId());
                output.writeBoolean(expression.nullable());
                writeText(output, expression.inputFieldId());
                return;
            case LITERAL:
                output.writeInt(expression.dataType().wireId());
                output.writeBoolean(expression.nullable());
                if (!expression.nullable()) writeText(output,
                        expression.literalValue());
                return;
            case CALL:
                output.writeInt(expression.function().wireId());
                output.writeInt(expression.arguments().size());
                for (StreamExpression argument : expression.arguments()) {
                    writeExpression(output, argument);
                }
                return;
            default:
                throw fail("UNKNOWN_EXPRESSION_KIND", "unknown expression kind");
        }
    }

    private static StreamExpression readExpression(DataInputStream input,
            int depth, Counter counter) throws IOException {
        counter.nodes++;
        if (depth > StreamExpression.MAX_DEPTH
                || counter.nodes > StreamExpression.MAX_NODES) {
            throw fail("EXPRESSION_LIMIT_EXCEEDED",
                    "expression depth or node count exceeds limit");
        }
        StreamExpression.Kind kind = StreamExpression.Kind.fromWireId(
                input.readInt());
        switch (kind) {
            case INPUT:
                return input(readType(input), input.readBoolean(),
                        readText(input));
            case LITERAL: {
                PortableDataType type = readType(input);
                boolean nullable = input.readBoolean();
                return decodedLiteral(type,
                        nullable ? null : readTextAllowEmpty(input), nullable);
            }
            case CALL: {
                StreamExpression.Function function =
                        StreamExpression.Function.fromWireId(input.readInt());
                int count = input.readInt();
                if (count <= 0 || count > StreamExpression.MAX_ARGUMENTS
                        || count > (input.available() / 4) + 1) {
                    throw fail("EXPRESSION_ARITY_MISMATCH",
                            "expression argument count exceeds limit");
                }
                List<StreamExpression> arguments = new ArrayList<>(count);
                for (int index = 0; index < count; index++) {
                    arguments.add(readExpression(input, depth + 1, counter));
                }
                return StreamExpression.call(function,
                        arguments.toArray(new StreamExpression[arguments.size()]));
            }
            default:
                throw fail("UNKNOWN_EXPRESSION_KIND", "unknown expression kind");
        }
    }

    private static StreamExpression input(PortableDataType type,
            boolean nullable, String fieldId) {
        return StreamExpression.input(fieldId, type, nullable);
    }

    private static StreamExpression decodedLiteral(PortableDataType type,
            String value, boolean nullable) {
        if (nullable) return StreamExpression.nullLiteral(type);
        try {
            StreamExpression decoded;
            switch (type) {
                case BOOLEAN:
                    if (!"true".equals(value) && !"false".equals(value)) {
                        throw fail("INVALID_EXPRESSION_LITERAL",
                                "invalid BOOLEAN literal");
                    }
                    decoded = StreamExpression.booleanLiteral(
                            Boolean.parseBoolean(value));
                    break;
                case STRING:
                    decoded = StreamExpression.stringLiteral(value);
                    break;
                case INTEGER:
                    decoded = StreamExpression.integerLiteral(
                            Integer.parseInt(value));
                    break;
                case LONG:
                    decoded = StreamExpression.longLiteral(Long.parseLong(value));
                    break;
                case BIG_DECIMAL:
                    decoded = StreamExpression.decimalLiteral(
                            new BigDecimal(value));
                    break;
                case DATE:
                    decoded = StreamExpression.dateLiteral(Long.parseLong(value));
                    break;
                case TIMESTAMP:
                    decoded = StreamExpression.timestampLiteral(
                            Long.parseLong(value));
                    break;
                default:
                    throw fail("INVALID_EXPRESSION_LITERAL",
                            "unsupported expression literal type");
            }
            if (!decoded.literalValue().equals(value)) {
                throw fail("INVALID_EXPRESSION_LITERAL",
                        "expression literal is not canonical");
            }
            return decoded;
        } catch (NumberFormatException invalid) {
            throw fail("INVALID_EXPRESSION_LITERAL",
                    "expression literal does not match its type");
        }
    }

    private static PortableDataType readType(DataInputStream input)
            throws IOException {
        int id = input.readInt();
        try {
            return PortableDataType.fromWireId(id);
        } catch (IllegalArgumentException unknown) {
            throw fail("UNKNOWN_EXPRESSION_TYPE", "unknown expression data type");
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        String value = readTextAllowEmpty(input);
        if (value.isEmpty()) throw fail("NON_CANONICAL_TEXT", "text is empty");
        return value;
    }

    private static String readTextAllowEmpty(DataInputStream input)
            throws IOException {
        int length = input.readInt();
        if (length < 0 || length > StreamExpression.MAX_LITERAL_BYTES
                || length > input.available()) {
            throw fail("EXPRESSION_LIMIT_EXCEEDED", "expression text exceeds limit");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw fail("MALFORMED_UTF8", "expression text is malformed");
        }
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static StreamContractException fail(String code, String message) {
        return StreamContractSupport.failure(code, "expression", message);
    }

    private static final class Counter { int nodes; }
}
