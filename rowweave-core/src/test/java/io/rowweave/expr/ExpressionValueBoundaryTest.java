package io.rowweave.expr;

import io.rowweave.DataRow;
import io.rowweave.DataSchema;
import io.rowweave.DataType;
import io.rowweave.Field;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ExpressionValueBoundaryTest {
    private static final DataSchema SCHEMA = new DataSchema(
            new Field("value", DataType.BigDecimalType),
            new Field("text", DataType.StringType),
            new Field("day", DataType.DateType),
            new Field("instant", DataType.TimestampType));

    @ParameterizedTest
    @EnumSource(ExprEngineFactory.EngineType.class)
    void integralCastsKeepArbitraryPrecision(ExprEngineFactory.EngineType type) {
        ExprEngine engine = ExprEngineFactory.create(type);
        for (String input : new String[]{"9223372036854775808.9",
                "-9223372036854775809.9", "18446744073709551616.1"}) {
            DataRow row = new DataRow(4);
            row.set(0, new BigDecimal(input));
            row.set(1, input);
            BigDecimal expected = new BigDecimal(input).setScale(0,
                    java.math.RoundingMode.DOWN);
            for (String target : new String[]{"int", "integer", "long"}) {
                assertEquals(expected, engine.compile(SCHEMA,
                        "cast(value as " + target + ")").eval(row));
                assertEquals(expected, engine.compile(SCHEMA,
                        "cast(text as " + target + ")").eval(row));
            }
            assertEquals(expected, engine.compile(SCHEMA, "long(value)").eval(row));
        }
        assertNull(engine.compile(SCHEMA, "cast(value as long)").eval(new DataRow(4)));
    }

    @ParameterizedTest
    @EnumSource(ExprEngineFactory.EngineType.class)
    void temporalCastsUseTheSameValueRules(ExprEngineFactory.EngineType type) {
        ExprEngine engine = ExprEngineFactory.create(type);
        DataRow row = new DataRow(4);
        row.set(2, Date.valueOf("2026-11-02"));
        row.set(3, Timestamp.valueOf("2026-11-02 12:34:56.123456789"));
        assertEquals(LocalDateTime.of(2026, 11, 2, 0, 0),
                engine.compile(SCHEMA, "cast(day as timestamp)").eval(row));
        assertEquals(LocalDate.of(2026, 11, 2),
                engine.compile(SCHEMA, "cast(instant as date)").eval(row));
        assertEquals(LocalDateTime.of(2026, 11, 2, 12, 34, 56, 123456789),
                engine.compile(SCHEMA, "cast(instant as timestamp)").eval(row));
    }

    @ParameterizedTest
    @EnumSource(ExprEngineFactory.EngineType.class)
    void likeWildcardsIncludeLineTerminators(ExprEngineFactory.EngineType type) {
        ExprEngine engine = ExprEngineFactory.create(type);
        for (String text : new String[]{"a\nb", "a\rb", "a\r\nb", "a\u2028b"}) {
            DataRow row = new DataRow(4);
            row.set(1, text);
            assertEquals(Boolean.TRUE, engine.compile(SCHEMA, "text like '%'").eval(row));
            assertEquals(Boolean.TRUE, engine.compile(SCHEMA, "text like 'a%b'").eval(row));
        }
        DataRow row = new DataRow(4);
        row.set(1, "a\nb");
        assertEquals(Boolean.TRUE, engine.compile(SCHEMA, "text like 'a_b'").eval(row));
    }

    @ParameterizedTest
    @EnumSource(ExprEngineFactory.EngineType.class)
    @ResourceLock("default-locale")
    void languageTokensAreIndependentOfHostLocale(ExprEngineFactory.EngineType type) {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            ExprEngine engine = ExprEngineFactory.create(type);
            DataRow row = new DataRow(4);
            row.set(0, new BigDecimal("3.7"));
            assertEquals(Boolean.TRUE,
                    engine.compile(SCHEMA, "value IS NOT NULL").eval(row));
            assertEquals(new BigDecimal("3"),
                    engine.compile(SCHEMA, "INT(value)").eval(row));
            assertEquals(new BigDecimal("3"),
                    engine.compile(SCHEMA, "CAST(value AS INTEGER)").eval(row));
        } finally {
            Locale.setDefault(previous);
        }
    }
}
