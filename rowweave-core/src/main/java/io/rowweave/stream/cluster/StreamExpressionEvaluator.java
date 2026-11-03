package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamExpression;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Worker-side evaluator for the closed, typed Stream expression registry. */
final class StreamExpressionEvaluator {
    private static final long MILLIS_PER_DAY = 86_400_000L;
    private final Map<String, Integer> fields = new HashMap<>();

    StreamExpressionEvaluator(PortableRowSchema inputSchema) {
        for (int index = 0; index < inputSchema.fieldCount(); index++) {
            fields.put(inputSchema.field(index).fieldId(), index);
        }
    }

    Object evaluate(StreamExpression expression, DataRow row) {
        switch (expression.kind()) {
            case INPUT:
                Integer index = fields.get(expression.inputFieldId());
                if (index == null) {
                    throw new IllegalArgumentException(
                            "Stream expression input field is unavailable");
                }
                return row.get(index);
            case LITERAL:
                return literal(expression.dataType(), expression.literalValue());
            case CALL:
                return call(expression.function(), expression.arguments(), row);
            default:
                throw new UnsupportedOperationException(
                        "Stream expression kind is unsupported");
        }
    }

    private Object call(StreamExpression.Function function,
            List<StreamExpression> arguments, DataRow row) {
        Object first = evaluate(arguments.get(0), row);
        switch (function) {
            case AND: return and(first, evaluate(arguments.get(1), row));
            case OR: return or(first, evaluate(arguments.get(1), row));
            case NOT: return first == null ? null : !((Boolean) first);
            case EQUAL: case NOT_EQUAL: case LESS_THAN: case LESS_OR_EQUAL:
            case GREATER_THAN: case GREATER_OR_EQUAL:
                return compare(function, first,
                        evaluate(arguments.get(1), row));
            case MULTIPLY: case ADD: case SUBTRACT: case DIVIDE:
                return arithmetic(function, first,
                        evaluate(arguments.get(1), row));
            case NEGATE: return negate(first);
            case IS_NULL: return first == null;
            case IS_NOT_NULL: return first != null;
            case COALESCE:
                if (first != null) return first;
                for (int index = 1; index < arguments.size(); index++) {
                    Object value = evaluate(arguments.get(index), row);
                    if (value != null) return value;
                }
                return null;
            case CONCAT:
                if (first == null) return null;
                StringBuilder joined = new StringBuilder((String) first);
                for (int index = 1; index < arguments.size(); index++) {
                    Object value = evaluate(arguments.get(index), row);
                    if (value == null) return null;
                    joined.append((String) value);
                }
                return joined.toString();
            case LOWER: return first == null ? null
                    : ((String) first).toLowerCase(java.util.Locale.ROOT);
            case UPPER: return first == null ? null
                    : ((String) first).toUpperCase(java.util.Locale.ROOT);
            case TRIM: return first == null ? null : ((String) first).trim();
            case SUBSTRING: return substring(first, arguments, row);
            case LENGTH: return first == null ? null
                    : ((String) first).length();
            case CASE_WHEN:
                return Boolean.TRUE.equals(first)
                        ? evaluate(arguments.get(1), row)
                        : evaluate(arguments.get(2), row);
            case CAST_BOOLEAN: return castBoolean(first);
            case CAST_STRING: return castString(first);
            case CAST_INTEGER: return castInteger(first);
            case CAST_LONG: return castLong(first);
            case CAST_DECIMAL: return castDecimal(first);
            case CAST_DATE: return castDate(first);
            case CAST_TIMESTAMP: return castTimestamp(first);
            case DATE_ADD_DAYS:
                return dateAdd(first, evaluate(arguments.get(1), row));
            case DATE_DIFF_DAYS:
                return dateDiff(first, evaluate(arguments.get(1), row));
            case EPOCH_MILLIS_TO_TIMESTAMP:
                return first == null ? null : new Timestamp(number(first).longValue());
            case TIMESTAMP_TO_EPOCH_MILLIS:
                return first == null ? null : ((Timestamp) first).getTime();
            case YEAR: case MONTH: case DAY:
                return datePart(function, first);
            default:
                throw new UnsupportedOperationException(
                        "Stream expression function is unsupported");
        }
    }

    private static Object literal(PortableDataType type, String value) {
        if (value == null) return null;
        switch (type) {
            case BOOLEAN: return Boolean.valueOf(value);
            case STRING: return value;
            case INTEGER: return Integer.valueOf(value);
            case LONG: return Long.valueOf(value);
            case BIG_DECIMAL: return new BigDecimal(value);
            case DATE: return new java.sql.Date(
                    Math.multiplyExact(Long.parseLong(value), MILLIS_PER_DAY));
            case TIMESTAMP: return new Timestamp(Long.parseLong(value));
            default: throw new UnsupportedOperationException(
                    "Stream expression literal type is unsupported");
        }
    }

    private static Object and(Object left, Object right) {
        if (Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right)) {
            return false;
        }
        return left == null || right == null ? null : true;
    }

    private static Object or(Object left, Object right) {
        if (Boolean.TRUE.equals(left) || Boolean.TRUE.equals(right)) return true;
        return left == null || right == null ? null : false;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object compare(StreamExpression.Function function,
            Object left, Object right) {
        if (left == null || right == null) return null;
        int compared = left instanceof BigDecimal
                ? ((BigDecimal) left).compareTo((BigDecimal) right)
                : ((Comparable) left).compareTo(right);
        switch (function) {
            case EQUAL: return compared == 0;
            case NOT_EQUAL: return compared != 0;
            case LESS_THAN: return compared < 0;
            case LESS_OR_EQUAL: return compared <= 0;
            case GREATER_THAN: return compared > 0;
            case GREATER_OR_EQUAL: return compared >= 0;
            default: throw new AssertionError(function);
        }
    }

    private static Object arithmetic(StreamExpression.Function function,
            Object left, Object right) {
        if (left == null || right == null) return null;
        if (left instanceof BigDecimal || right instanceof BigDecimal
                || function == StreamExpression.Function.DIVIDE) {
            BigDecimal a = decimal(left);
            BigDecimal b = decimal(right);
            switch (function) {
                case ADD: return a.add(b);
                case SUBTRACT: return a.subtract(b);
                case MULTIPLY: return a.multiply(b);
                case DIVIDE: return a.divide(b, MathContext.DECIMAL128);
                default: throw new AssertionError(function);
            }
        }
        if (left instanceof Long || right instanceof Long) {
            long a = number(left).longValue();
            long b = number(right).longValue();
            if (function == StreamExpression.Function.ADD) {
                return Math.addExact(a, b);
            }
            if (function == StreamExpression.Function.SUBTRACT) {
                return Math.subtractExact(a, b);
            }
            return Math.multiplyExact(a, b);
        }
        int a = number(left).intValue();
        int b = number(right).intValue();
        if (function == StreamExpression.Function.ADD) return Math.addExact(a, b);
        if (function == StreamExpression.Function.SUBTRACT) {
            return Math.subtractExact(a, b);
        }
        return Math.multiplyExact(a, b);
    }

    private static Object negate(Object value) {
        if (value == null) return null;
        if (value instanceof BigDecimal) return ((BigDecimal) value).negate();
        if (value instanceof Long) return Math.negateExact((Long) value);
        return Math.negateExact((Integer) value);
    }

    private Object substring(Object value, List<StreamExpression> arguments,
            DataRow row) {
        if (value == null) return null;
        Object startValue = evaluate(arguments.get(1), row);
        Object lengthValue = arguments.size() == 3
                ? evaluate(arguments.get(2), row) : null;
        if (startValue == null || (arguments.size() == 3
                && lengthValue == null)) return null;
        String text = (String) value;
        int start = Math.max(0, number(startValue).intValue() - 1);
        if (start >= text.length()) return "";
        int end = arguments.size() == 2 ? text.length()
                : Math.min(text.length(), Math.addExact(start,
                Math.max(0, number(lengthValue).intValue())));
        return text.substring(start, end);
    }

    private static Object castBoolean(Object value) {
        if (value == null || value instanceof Boolean) return value;
        if (value instanceof Number) {
            return decimal(value).compareTo(BigDecimal.ZERO) != 0;
        }
        String text = ((String) value).trim();
        if ("true".equalsIgnoreCase(text)) return true;
        if ("false".equalsIgnoreCase(text)) return false;
        throw new IllegalArgumentException("invalid BOOLEAN cast");
    }

    private static Object castString(Object value) {
        if (value == null) return null;
        if (value instanceof java.util.Date) {
            return Long.toString(((java.util.Date) value).getTime());
        }
        return value instanceof BigDecimal
                ? ((BigDecimal) value).toPlainString() : value.toString();
    }

    private static Object castInteger(Object value) {
        if (value == null || value instanceof Integer) return value;
        if (value instanceof BigDecimal) return ((BigDecimal) value).intValueExact();
        if (value instanceof Number) return Math.toIntExact(((Number) value).longValue());
        return Integer.valueOf(((String) value).trim());
    }

    private static Object castLong(Object value) {
        if (value == null || value instanceof Long) return value;
        if (value instanceof BigDecimal) return ((BigDecimal) value).longValueExact();
        if (value instanceof Number) return ((Number) value).longValue();
        return Long.valueOf(((String) value).trim());
    }

    private static Object castDecimal(Object value) {
        if (value == null || value instanceof BigDecimal) return value;
        return value instanceof Number
                ? new BigDecimal(value.toString())
                : new BigDecimal(((String) value).trim());
    }

    private static Object castDate(Object value) {
        if (value == null || value instanceof java.sql.Date) return value;
        if (value instanceof java.util.Date) {
            return new java.sql.Date(((java.util.Date) value).getTime());
        }
        return new java.sql.Date(number(value).longValue());
    }

    private static Object castTimestamp(Object value) {
        if (value == null || value instanceof Timestamp) return value;
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        return new Timestamp(number(value).longValue());
    }

    private static Object dateAdd(Object date, Object days) {
        if (date == null || days == null) return null;
        long delta = Math.multiplyExact(number(days).longValue(), MILLIS_PER_DAY);
        return new java.sql.Date(Math.addExact(
                ((java.util.Date) date).getTime(), delta));
    }

    private static Object dateDiff(Object left, Object right) {
        if (left == null || right == null) return null;
        return Math.subtractExact(((java.util.Date) left).getTime(),
                ((java.util.Date) right).getTime()) / MILLIS_PER_DAY;
    }

    private static Object datePart(StreamExpression.Function function,
            Object value) {
        if (value == null) return null;
        ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(
                ((java.util.Date) value).getTime()), ZoneOffset.UTC);
        if (function == StreamExpression.Function.YEAR) return utc.getYear();
        if (function == StreamExpression.Function.MONTH) {
            return utc.getMonthValue();
        }
        return utc.getDayOfMonth();
    }

    private static Number number(Object value) {
        return (Number) value;
    }

    private static BigDecimal decimal(Object value) {
        return value instanceof BigDecimal ? (BigDecimal) value
                : new BigDecimal(value.toString());
    }
}
