package io.rowweave.connector.postgres;

import io.rowweave.DataType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.OffsetDateTime;

final class PostgresTypeMapping {
    private static final String TYPE_CODE = "POSTGRES_CDC_TYPE_UNSUPPORTED";
    private static final String VALUE_CODE = "POSTGRES_CDC_VALUE_INVALID";

    private PostgresTypeMapping() { }

    static DataType dataType(long oid) {
        switch ((int) oid) {
            case 16: return DataType.BooleanType;       // bool
            case 21:                                   // int2
            case 23: return DataType.IntegerType;       // int4
            case 20: return DataType.LongType;          // int8
            case 1700: return DataType.BigDecimalType;  // numeric
            case 25:                                    // text
            case 1042:                                  // bpchar
            case 1043:                                  // varchar
            case 114:                                   // json
            case 3802:                                  // jsonb
            case 2950: return DataType.StringType;      // uuid
            case 1082: return DataType.DateType;
            case 1114:                                  // timestamp
            case 1184: return DataType.TimestampType;   // timestamptz
            default: throw failure(TYPE_CODE);
        }
    }

    static Object decode(byte[] bytes, PostgresColumnBinding column) {
        String value = utf8(bytes);
        try {
            switch (dataType(column.typeOid())) {
                case BooleanType:
                    if ("t".equals(value)) return Boolean.TRUE;
                    if ("f".equals(value)) return Boolean.FALSE;
                    break;
                case IntegerType:
                    return Integer.valueOf(value);
                case LongType:
                    return Long.valueOf(value);
                case BigDecimalType:
                    return new BigDecimal(value);
                case StringType:
                    return value;
                case DateType:
                    return Date.valueOf(LocalDate.parse(value));
                case TimestampType:
                    if (column.typeOid() == 1184L) {
                        return Timestamp.from(OffsetDateTime.parse(
                                value.replace(' ', 'T')).toInstant());
                    }
                    return Timestamp.valueOf(value.replace('T', ' '));
                default:
                    break;
            }
        } catch (RuntimeException malformed) {
            throw failure(VALUE_CODE);
        }
        throw failure(VALUE_CODE);
    }

    private static String utf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw failure(VALUE_CODE);
        }
    }

    private static IllegalArgumentException failure(String code) {
        return new IllegalArgumentException(code);
    }
}
