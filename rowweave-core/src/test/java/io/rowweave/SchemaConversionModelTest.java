package io.rowweave;

import io.rowweave.ops.SchemaConversion;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded, explicit and immutable schema conversion model. */
class SchemaConversionModelTest {

    @Test
    void freezesOrderedRulesAndBuildsExactTargetMeta() {
        DataSchema source = new DataSchema(
                new Field("id", DataType.IntegerType),
                new Field("memo", "display_memo", DataType.StringType));

        SchemaConversion conversion = SchemaConversion.builder(source)
                .required("id", new Field("adaptor_id", DataType.LongType),
                        SchemaConversion.Cast.INTEGER_TO_LONG)
                .nullable(1, new Field("description", "label", DataType.StringType),
                        SchemaConversion.Cast.IDENTITY)
                .defaulted(new Field("opening_readout", DataType.BigDecimalType),
                        new BigDecimal("0.00"))
                .build();

        assertMeta(conversion.getOutputSchema(),
                new String[]{"adaptor_id", "description", "opening_readout"},
                new String[]{"adaptor_id", "label", "opening_readout"},
                new DataType[]{DataType.LongType, DataType.StringType, DataType.BigDecimalType});
        assertEquals(Arrays.asList("0", "1", "-1"), conversion.sourceIndices());
        assertEquals(Arrays.asList("INTEGER_TO_LONG", "IDENTITY", "IDENTITY"), conversion.casts());
        assertEquals(Arrays.asList("REQUIRED", "NULLABLE", "REQUIRED"), conversion.nullPolicies());
        assertEquals(Arrays.asList("NONE", "NONE", "VALUE"), conversion.defaultKinds());
        assertEquals(Arrays.asList("", "", "0.00"), conversion.defaultValues());
    }

    @Test
    void nameLookupRequiresOneExactPrimaryNameAndIndexAllowsIntentionalDuplicates() {
        DataSchema duplicate = new DataSchema(
                new Field("id", "left_id", DataType.IntegerType),
                new Field("id", "right_id", DataType.IntegerType));

        IllegalArgumentException ambiguous = assertThrows(IllegalArgumentException.class,
                () -> SchemaConversion.builder(duplicate)
                        .required("id", new Field("out", DataType.IntegerType),
                                SchemaConversion.Cast.IDENTITY));
        assertTrue(ambiguous.getMessage().contains("SCHEMA_MAPPING_INVALID"));

        IllegalArgumentException alias = assertThrows(IllegalArgumentException.class,
                () -> SchemaConversion.builder(duplicate)
                        .required("left_id", new Field("out", DataType.IntegerType),
                                SchemaConversion.Cast.IDENTITY));
        assertTrue(alias.getMessage().contains("SCHEMA_MAPPING_INVALID"));

        SchemaConversion indexed = SchemaConversion.builder(duplicate)
                .required(1, new Field("right", DataType.IntegerType),
                        SchemaConversion.Cast.IDENTITY)
                .build();
        assertEquals(Arrays.asList("1"), indexed.sourceIndices());
    }

    @Test
    void acceptsOnlyTheClosedLosslessCastMatrix() {
        DataSchema source = new DataSchema(
                new Field("i", DataType.IntegerType),
                new Field("l", DataType.LongType),
                new Field("d", DataType.BigDecimalType),
                new Field("s", DataType.StringType));

        SchemaConversion valid = SchemaConversion.builder(source)
                .required(0, new Field("i", DataType.IntegerType), SchemaConversion.Cast.IDENTITY)
                .required(0, new Field("il", DataType.LongType), SchemaConversion.Cast.INTEGER_TO_LONG)
                .required(0, new Field("id", DataType.BigDecimalType), SchemaConversion.Cast.INTEGER_TO_DECIMAL)
                .required(1, new Field("ld", DataType.BigDecimalType), SchemaConversion.Cast.LONG_TO_DECIMAL)
                .nullable(2, new Field("d", DataType.BigDecimalType), SchemaConversion.Cast.IDENTITY)
                .nullable(3, new Field("s", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                .build();
        assertEquals(6, valid.getOutputSchema().getFieldCount());

        assertInvalid(() -> SchemaConversion.builder(source)
                .required(1, new Field("narrow", DataType.IntegerType), SchemaConversion.Cast.IDENTITY));
        assertInvalid(() -> SchemaConversion.builder(source)
                .required(2, new Field("rounded", DataType.LongType), SchemaConversion.Cast.LONG_TO_DECIMAL));
        assertInvalid(() -> SchemaConversion.builder(source)
                .required(3, new Field("parsed", DataType.LongType), SchemaConversion.Cast.INTEGER_TO_LONG));
        assertInvalid(() -> SchemaConversion.builder(source)
                .required(0, new Field("unknown", DataType.NullType), SchemaConversion.Cast.IDENTITY));
        assertInvalid(() -> SchemaConversion.builder(
                        new DataSchema(new Field("unknown", DataType.NullType)))
                .required(0, new Field("out", DataType.NullType), SchemaConversion.Cast.IDENTITY));
    }

    @Test
    void validatesAndFreezesTypedDefaultsIncludingMutableTimeValues() {
        Date date = new Date(1_700_000_000_123L);
        Timestamp timestamp = new Timestamp(1_700_000_000_456L);
        timestamp.setNanos(456_789_123);
        DataSchema empty = new DataSchema();

        SchemaConversion conversion = SchemaConversion.builder(empty)
                .defaulted(new Field("date", DataType.DateType), date)
                .defaulted(new Field("timestamp", DataType.TimestampType), timestamp)
                .defaulted(new Field("escaped", DataType.StringType), "\\N")
                .defaulted(new Field("missing", DataType.StringType), null)
                .build();
        java.util.List<String> frozen = new ArrayList<>(conversion.defaultValues());

        date.setTime(1L);
        timestamp.setTime(2L);
        timestamp.setNanos(3);

        assertEquals(frozen, conversion.defaultValues());
        assertEquals(Arrays.asList("VALUE", "VALUE", "VALUE", "NULL"), conversion.defaultKinds());
        assertNotEquals(conversion.defaultValues().get(2), conversion.defaultValues().get(3));
        assertEquals("NULLABLE", conversion.nullPolicies().get(3));

        assertInvalid(() -> SchemaConversion.builder(empty)
                .defaulted(new Field("bad", DataType.LongType), Integer.valueOf(1)));
        assertInvalid(() -> SchemaConversion.builder(empty)
                .defaulted(new Field("bad", DataType.DateType), timestamp));
    }

    @Test
    void rejectsUnboundedOrAmbiguousMappingsAndBindsDatasetInputFingerprint() {
        DataSchema source = new DataSchema(new Field("id", DataType.IntegerType));

        assertInvalid(() -> SchemaConversion.builder(source).build());
        assertInvalid(() -> SchemaConversion.builder(source)
                .required(0, new Field("same", DataType.IntegerType), SchemaConversion.Cast.IDENTITY)
                .nullable(0, new Field("same", DataType.IntegerType), SchemaConversion.Cast.IDENTITY));
        assertInvalid(() -> SchemaConversion.builder(source)
                .required(-1, new Field("bad", DataType.IntegerType), SchemaConversion.Cast.IDENTITY));

        SchemaConversion.Builder tooWide = SchemaConversion.builder(new DataSchema());
        for (int i = 0; i < 4097; i++) {
            tooWide.defaulted(new Field("f" + i, DataType.IntegerType), i);
        }
        assertInvalid(tooWide::build);

        SchemaConversion mapping = SchemaConversion.builder(source)
                .required(0, new Field("id64", DataType.LongType), SchemaConversion.Cast.INTEGER_TO_LONG)
                .build();
        ExecutionEnvironment session = new ExecutionEnvironment("schema-model", "schema model");
        DataRow row = new DataRow(1);
        row.set(0, 1);
        DataSet dataSet = session.fromSource(new CollectionSource(source, Arrays.asList(row)));
        assertEquals(DataType.LongType,
                dataSet.convertSchema(mapping).getSchema().getField(0).getDataType());

        DataSchema drifted = new DataSchema(new Field("id", "changed-alias", DataType.IntegerType));
        ExecutionEnvironment other = new ExecutionEnvironment("schema-drift", "schema drift");
        DataSet driftedData = other.fromSource(new CollectionSource(drifted, Arrays.asList(row)));
        IllegalArgumentException mismatch = assertThrows(IllegalArgumentException.class,
                () -> driftedData.convertSchema(mapping));
        assertTrue(mismatch.getMessage().contains("SCHEMA_MAPPING_INPUT_MISMATCH"));
    }

    private static void assertInvalid(org.junit.jupiter.api.function.Executable executable) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(failure.getMessage().contains("SCHEMA_MAPPING_INVALID"), failure::getMessage);
    }

    private static void assertMeta(DataSchema actual, String[] names, String[] aliases, DataType[] types) {
        assertEquals(names.length, actual.getFieldCount());
        for (int i = 0; i < names.length; i++) {
            assertEquals(names[i], actual.getField(i).getName());
            assertEquals(aliases[i], actual.getField(i).getAlias());
            assertEquals(types[i], actual.getField(i).getDataType());
        }
    }
}
