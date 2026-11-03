package io.rowweave.stream.api;

import io.rowweave.portable.PortableDataType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable, typed and bounded portable expression used by Stream CALC. */
public final class StreamExpression {
    public static final int MAX_DEPTH = 32;
    public static final int MAX_NODES = 256;
    public static final int MAX_LITERAL_BYTES = 4096;
    public static final int MAX_ARGUMENTS = 64;

    public enum Kind { INPUT(1), LITERAL(2), CALL(3);
        private final int wireId;
        Kind(int wireId) { this.wireId = wireId; }
        public int wireId() { return wireId; }
        public static Kind fromWireId(int id) {
            for (Kind value : values()) if (value.wireId == id) return value;
            throw failure("UNKNOWN_EXPRESSION_KIND", "unknown expression node kind");
        }
    }

    /** Additive function registry. IDs are permanent and never use enum ordinal. */
    public enum Function {
        AND(1), OR(2), NOT(3), EQUAL(4), NOT_EQUAL(5), LESS_THAN(6),
        LESS_OR_EQUAL(7), GREATER_THAN(8), GREATER_OR_EQUAL(9),
        MULTIPLY(10), ADD(11), SUBTRACT(12), DIVIDE(13), NEGATE(14),
        IS_NULL(15), IS_NOT_NULL(16), COALESCE(17), CONCAT(18), LOWER(19),
        CASE_WHEN(20), UPPER(21), TRIM(22), SUBSTRING(23), LENGTH(24),
        CAST_BOOLEAN(25), CAST_STRING(26), CAST_INTEGER(27),
        DATE_ADD_DAYS(28), CAST_TIMESTAMP(29), DATE_DIFF_DAYS(30),
        CAST_LONG(31), CAST_DECIMAL(32), CAST_DATE(33),
        EPOCH_MILLIS_TO_TIMESTAMP(34), TIMESTAMP_TO_EPOCH_MILLIS(35),
        YEAR(36), MONTH(37), DAY(38);

        private final int wireId;
        Function(int wireId) { this.wireId = wireId; }
        public int wireId() { return wireId; }
        public static Function fromWireId(int id) {
            for (Function value : values()) if (value.wireId == id) return value;
            throw failure("UNKNOWN_EXPRESSION_FUNCTION",
                    "unknown expression function ID");
        }
    }

    private final Kind kind;
    private final PortableDataType dataType;
    private final boolean nullable;
    private final String inputFieldId;
    private final String literalValue;
    private final Function function;
    private final List<StreamExpression> arguments;
    private final int depth;
    private final int nodeCount;

    private StreamExpression(Kind kind, PortableDataType dataType,
            boolean nullable, String inputFieldId, String literalValue,
            Function function, List<StreamExpression> arguments) {
        this.kind = kind;
        this.dataType = dataType;
        this.nullable = nullable;
        this.inputFieldId = inputFieldId;
        this.literalValue = literalValue;
        this.function = function;
        this.arguments = Collections.unmodifiableList(
                new ArrayList<>(arguments));
        int maximumDepth = 0;
        int nodes = 1;
        for (StreamExpression argument : arguments) {
            maximumDepth = Math.max(maximumDepth, argument.depth);
            nodes = Math.addExact(nodes, argument.nodeCount);
        }
        this.depth = 1 + maximumDepth;
        this.nodeCount = nodes;
        if (depth > MAX_DEPTH || nodeCount > MAX_NODES) {
            throw failure("EXPRESSION_LIMIT_EXCEEDED",
                    "expression depth or node count exceeds its limit");
        }
    }

    public static StreamExpression input(String stableFieldId,
            PortableDataType dataType, boolean nullable) {
        if (dataType == null || dataType == PortableDataType.NULL) typeFailure();
        String field = canonicalFieldId(stableFieldId);
        return new StreamExpression(Kind.INPUT, dataType, nullable, field,
                null, null, Collections.<StreamExpression>emptyList());
    }

    public static StreamExpression nullLiteral(PortableDataType dataType) {
        if (dataType == null || dataType == PortableDataType.NULL) typeFailure();
        return literal(dataType, null, true);
    }

    public static StreamExpression booleanLiteral(boolean value) {
        return literal(PortableDataType.BOOLEAN, Boolean.toString(value), false);
    }

    public static StreamExpression stringLiteral(String value) {
        return literal(PortableDataType.STRING,
                canonicalLiteral(value, "expression.literal"), false);
    }

    public static StreamExpression integerLiteral(int value) {
        return literal(PortableDataType.INTEGER, Integer.toString(value), false);
    }

    public static StreamExpression longLiteral(long value) {
        return literal(PortableDataType.LONG, Long.toString(value), false);
    }

    public static StreamExpression decimalLiteral(BigDecimal value) {
        if (value == null) throw failure("INVALID_EXPRESSION_LITERAL",
                "decimal literal is required");
        BigDecimal normalized = value.signum() == 0
                ? BigDecimal.ZERO : value.stripTrailingZeros();
        return literal(PortableDataType.BIG_DECIMAL,
                normalized.toPlainString(), false);
    }

    /** Canonical DATE literal as an epoch-day, independent of timezone. */
    public static StreamExpression dateLiteral(long epochDay) {
        return literal(PortableDataType.DATE, Long.toString(epochDay), false);
    }

    /** Canonical TIMESTAMP literal as epoch milliseconds. */
    public static StreamExpression timestampLiteral(long epochMillis) {
        return literal(PortableDataType.TIMESTAMP,
                Long.toString(epochMillis), false);
    }

    static StreamExpression decodedLiteral(PortableDataType type,
            String value, boolean nullable) {
        if (type == null || type == PortableDataType.NULL
                || (nullable && value != null) || (!nullable && value == null)) {
            throw failure("INVALID_EXPRESSION_LITERAL",
                    "literal payload is inconsistent");
        }
        if (value != null) canonicalLiteral(value, "expression.literal");
        validateLiteral(type, value);
        return literal(type, value, nullable);
    }

    private static StreamExpression literal(PortableDataType type,
            String value, boolean nullable) {
        return new StreamExpression(Kind.LITERAL, type, nullable, null, value,
                null, Collections.<StreamExpression>emptyList());
    }

    public static StreamExpression call(Function function,
            StreamExpression... arguments) {
        if (arguments == null) {
            throw failure("EXPRESSION_ARITY_MISMATCH",
                    "expression arguments are required");
        }
        return call(function, Arrays.asList(arguments));
    }

    static StreamExpression call(Function function,
            List<StreamExpression> arguments) {
        if (function == null || arguments == null || arguments.isEmpty()
                || arguments.size() > MAX_ARGUMENTS) arityFailure();
        List<StreamExpression> copy = new ArrayList<>(arguments.size());
        for (StreamExpression argument : arguments) {
            if (argument == null) typeFailure();
            copy.add(argument);
        }
        Type result = infer(function, copy);
        return new StreamExpression(Kind.CALL, result.type, result.nullable,
                null, null, function, copy);
    }

    private static Type infer(Function function, List<StreamExpression> args) {
        switch (function) {
            case AND: case OR:
                exact(args, 2); requireAll(args, PortableDataType.BOOLEAN);
                return type(PortableDataType.BOOLEAN, anyNullable(args));
            case NOT:
                exact(args, 1); requireAll(args, PortableDataType.BOOLEAN);
                return type(PortableDataType.BOOLEAN, args.get(0).nullable);
            case EQUAL: case NOT_EQUAL: case LESS_THAN: case LESS_OR_EQUAL:
            case GREATER_THAN: case GREATER_OR_EQUAL:
                exact(args, 2); requireSame(args);
                if (args.get(0).dataType == PortableDataType.NULL) typeFailure();
                return type(PortableDataType.BOOLEAN, anyNullable(args));
            case MULTIPLY: case ADD: case SUBTRACT:
                exact(args, 2); requireNumeric(args);
                return type(promoteNumeric(args), anyNullable(args));
            case DIVIDE:
                exact(args, 2); requireNumeric(args);
                return type(PortableDataType.BIG_DECIMAL, anyNullable(args));
            case NEGATE:
                exact(args, 1); requireNumeric(args);
                return type(args.get(0).dataType, args.get(0).nullable);
            case IS_NULL: case IS_NOT_NULL:
                exact(args, 1);
                return type(PortableDataType.BOOLEAN, false);
            case COALESCE:
                range(args, 2, MAX_ARGUMENTS); requireSame(args);
                return type(args.get(0).dataType, allNullable(args));
            case CONCAT:
                range(args, 2, MAX_ARGUMENTS); requireAll(args,
                        PortableDataType.STRING);
                return type(PortableDataType.STRING, anyNullable(args));
            case LOWER: case UPPER: case TRIM:
                exact(args, 1); requireAll(args, PortableDataType.STRING);
                return type(PortableDataType.STRING, args.get(0).nullable);
            case SUBSTRING:
                range(args, 2, 3);
                require(args.get(0), PortableDataType.STRING);
                requireInteger(args.get(1));
                if (args.size() == 3) requireInteger(args.get(2));
                return type(PortableDataType.STRING, anyNullable(args));
            case LENGTH:
                exact(args, 1); requireAll(args, PortableDataType.STRING);
                return type(PortableDataType.INTEGER, args.get(0).nullable);
            case CASE_WHEN:
                exact(args, 3); require(args.get(0), PortableDataType.BOOLEAN);
                if (args.get(1).dataType != args.get(2).dataType) typeFailure();
                return type(args.get(1).dataType,
                        args.get(1).nullable || args.get(2).nullable);
            case CAST_BOOLEAN:
                return cast(args, PortableDataType.BOOLEAN);
            case CAST_STRING:
                return cast(args, PortableDataType.STRING);
            case CAST_INTEGER:
                return cast(args, PortableDataType.INTEGER);
            case CAST_LONG:
                return cast(args, PortableDataType.LONG);
            case CAST_DECIMAL:
                return cast(args, PortableDataType.BIG_DECIMAL);
            case CAST_DATE:
                return cast(args, PortableDataType.DATE);
            case CAST_TIMESTAMP:
                return cast(args, PortableDataType.TIMESTAMP);
            case DATE_ADD_DAYS:
                exact(args, 2); require(args.get(0), PortableDataType.DATE);
                requireInteger(args.get(1));
                return type(PortableDataType.DATE, anyNullable(args));
            case DATE_DIFF_DAYS:
                exact(args, 2); requireAll(args, PortableDataType.DATE);
                return type(PortableDataType.LONG, anyNullable(args));
            case EPOCH_MILLIS_TO_TIMESTAMP:
                exact(args, 1); requireInteger(args.get(0));
                return type(PortableDataType.TIMESTAMP, args.get(0).nullable);
            case TIMESTAMP_TO_EPOCH_MILLIS:
                exact(args, 1); require(args.get(0), PortableDataType.TIMESTAMP);
                return type(PortableDataType.LONG, args.get(0).nullable);
            case YEAR: case MONTH: case DAY:
                exact(args, 1);
                PortableDataType input = args.get(0).dataType;
                if (input != PortableDataType.DATE
                        && input != PortableDataType.TIMESTAMP) typeFailure();
                return type(PortableDataType.INTEGER, args.get(0).nullable);
            default:
                throw failure("UNKNOWN_EXPRESSION_FUNCTION",
                        "expression function is unsupported");
        }
    }

    private static Type cast(List<StreamExpression> args,
            PortableDataType output) {
        exact(args, 1);
        return type(output, args.get(0).nullable);
    }

    private static void exact(List<StreamExpression> args, int size) {
        if (args.size() != size) arityFailure();
    }

    private static void range(List<StreamExpression> args, int min, int max) {
        if (args.size() < min || args.size() > max) arityFailure();
    }

    private static void requireAll(List<StreamExpression> args,
            PortableDataType type) {
        for (StreamExpression argument : args) require(argument, type);
    }

    private static void requireSame(List<StreamExpression> args) {
        PortableDataType type = args.get(0).dataType;
        for (StreamExpression argument : args) if (argument.dataType != type) {
            typeFailure();
        }
    }

    private static void requireNumeric(List<StreamExpression> args) {
        for (StreamExpression argument : args) if (!numeric(argument.dataType)) {
            typeFailure();
        }
    }

    private static void requireInteger(StreamExpression expression) {
        if (expression.dataType != PortableDataType.INTEGER
                && expression.dataType != PortableDataType.LONG) typeFailure();
    }

    private static void require(StreamExpression expression,
            PortableDataType expected) {
        if (expression.dataType != expected) typeFailure();
    }

    private static boolean numeric(PortableDataType type) {
        return type == PortableDataType.INTEGER || type == PortableDataType.LONG
                || type == PortableDataType.BIG_DECIMAL;
    }

    private static PortableDataType promoteNumeric(List<StreamExpression> args) {
        PortableDataType result = PortableDataType.INTEGER;
        for (StreamExpression argument : args) {
            if (argument.dataType == PortableDataType.BIG_DECIMAL) {
                return PortableDataType.BIG_DECIMAL;
            }
            if (argument.dataType == PortableDataType.LONG) {
                result = PortableDataType.LONG;
            }
        }
        return result;
    }

    private static boolean anyNullable(List<StreamExpression> args) {
        for (StreamExpression argument : args) if (argument.nullable) return true;
        return false;
    }

    private static boolean allNullable(List<StreamExpression> args) {
        for (StreamExpression argument : args) if (!argument.nullable) return false;
        return true;
    }

    private static void validateLiteral(PortableDataType type, String value) {
        if (value == null) return;
        try {
            switch (type) {
                case BOOLEAN:
                    if (!"true".equals(value) && !"false".equals(value)) {
                        throw new IllegalArgumentException();
                    }
                    break;
                case INTEGER: Integer.parseInt(value); break;
                case LONG: case DATE: case TIMESTAMP: Long.parseLong(value); break;
                case BIG_DECIMAL:
                    if (!new BigDecimal(value).stripTrailingZeros().toPlainString()
                            .equals(value) && !("0".equals(value)
                            && new BigDecimal(value).signum() == 0)) {
                        throw new IllegalArgumentException();
                    }
                    break;
                case STRING: break;
                default: throw new IllegalArgumentException();
            }
        } catch (RuntimeException invalid) {
            throw failure("INVALID_EXPRESSION_LITERAL",
                    "literal does not match its canonical type");
        }
    }

    private static String canonicalLiteral(String value, String path) {
        if (value == null || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw failure("NON_CANONICAL_TEXT", "canonical literal is required");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > MAX_LITERAL_BYTES) {
            throw failure("EXPRESSION_LIMIT_EXCEEDED", path + " exceeds limit");
        }
        return value;
    }

    private static void arityFailure() {
        throw failure("EXPRESSION_ARITY_MISMATCH",
                "expression function arity is invalid");
    }

    private static void typeFailure() {
        throw failure("EXPRESSION_TYPE_MISMATCH",
                "expression operand types are invalid");
    }

    private static StreamExpressionException failure(String code,
            String message) {
        return new StreamExpressionException(code, message);
    }

    private static String canonicalFieldId(String value) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)
                || value.getBytes(StandardCharsets.UTF_8).length > 256) {
            throw failure("NON_CANONICAL_TEXT",
                    "canonical input field ID is required");
        }
        return value;
    }

    private static Type type(PortableDataType type, boolean nullable) {
        return new Type(type, nullable);
    }

    private static final class Type {
        final PortableDataType type;
        final boolean nullable;
        Type(PortableDataType type, boolean nullable) {
            this.type = type;
            this.nullable = nullable;
        }
    }

    public Kind kind() { return kind; }
    public PortableDataType dataType() { return dataType; }
    public boolean nullable() { return nullable; }
    public String inputFieldId() { return inputFieldId; }
    public String literalValue() { return literalValue; }
    public Function function() { return function; }
    public List<StreamExpression> arguments() { return arguments; }
    public int depth() { return depth; }
    public int nodeCount() { return nodeCount; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamExpression)) return false;
        StreamExpression that = (StreamExpression) other;
        return kind == that.kind && dataType == that.dataType
                && nullable == that.nullable
                && Objects.equals(inputFieldId, that.inputFieldId)
                && Objects.equals(literalValue, that.literalValue)
                && function == that.function && arguments.equals(that.arguments);
    }

    @Override public int hashCode() {
        return Objects.hash(kind, dataType, nullable, inputFieldId,
                literalValue, function, arguments);
    }
}
