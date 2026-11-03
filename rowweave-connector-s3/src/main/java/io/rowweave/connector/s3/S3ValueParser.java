package io.rowweave.connector.s3;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.rowweave.DataType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

final class S3ValueParser {
    private S3ValueParser() { }

    static Object json(JsonParser parser, DataType type, boolean nullable) {
        JsonToken token = parser.currentToken();
        if (token == JsonToken.VALUE_NULL) return nullable(type, nullable);
        try {
            switch (type) {
                case BooleanType:
                    if (token == JsonToken.VALUE_TRUE) return Boolean.TRUE;
                    if (token == JsonToken.VALUE_FALSE) return Boolean.FALSE;
                    break;
                case StringType:
                    if (token == JsonToken.VALUE_STRING) return parser.getText();
                    break;
                case IntegerType:
                    if (token == JsonToken.VALUE_NUMBER_INT) {
                        return parser.getIntValue();
                    }
                    break;
                case LongType:
                    if (token == JsonToken.VALUE_NUMBER_INT) {
                        return parser.getLongValue();
                    }
                    break;
                case BigDecimalType:
                    if (token == JsonToken.VALUE_NUMBER_INT
                            || token == JsonToken.VALUE_NUMBER_FLOAT) {
                        return parser.getDecimalValue();
                    }
                    break;
                case DateType:
                    if (token == JsonToken.VALUE_STRING) {
                        return Date.valueOf(LocalDate.parse(parser.getText()));
                    }
                    break;
                case TimestampType:
                    if (token == JsonToken.VALUE_STRING) {
                        return timestamp(parser.getText());
                    }
                    break;
                case NullType:
                    break;
                default:
                    break;
            }
        } catch (RuntimeException | java.io.IOException invalid) {
            throw invalid();
        }
        throw invalid();
    }

    static Object text(String value, DataType type, boolean nullable,
            String nullLiteral) {
        if (value.equals(nullLiteral)) return nullable(type, nullable);
        try {
            switch (type) {
                case BooleanType:
                    if ("true".equals(value)) return Boolean.TRUE;
                    if ("false".equals(value)) return Boolean.FALSE;
                    break;
                case StringType:
                    return value;
                case IntegerType:
                    return Integer.valueOf(value);
                case LongType:
                    return Long.valueOf(value);
                case BigDecimalType:
                    return new BigDecimal(value);
                case DateType:
                    return Date.valueOf(LocalDate.parse(value));
                case TimestampType:
                    return timestamp(value);
                case NullType:
                    break;
                default:
                    break;
            }
        } catch (RuntimeException invalid) {
            throw invalid();
        }
        throw invalid();
    }

    private static Object nullable(DataType type, boolean nullable) {
        if (nullable || type == DataType.NullType) return null;
        throw invalid();
    }

    private static Timestamp timestamp(String value) {
        if (value.endsWith("Z") || hasOffset(value)) {
            return Timestamp.from(OffsetDateTime.parse(value).toInstant());
        }
        return Timestamp.valueOf(LocalDateTime.parse(value));
    }

    private static boolean hasOffset(String value) {
        int time = value.indexOf('T');
        return time >= 0 && (value.indexOf('+', time) >= 0
                || value.indexOf('-', time) >= 0);
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("S3_ROW_INVALID");
    }
}
