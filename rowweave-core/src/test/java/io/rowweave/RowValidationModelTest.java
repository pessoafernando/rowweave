package io.rowweave;

import io.rowweave.ops.RowValidation;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded typed-row validation model and canonical descriptor. */
class RowValidationModelTest {

    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType),
            new Field("sequence", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));

    @Test
    void freezesOrderedClosedRulesAndCanonicalDescriptors() {
        RowValidation validation = RowValidation.builder(META)
                .notNull("adaptor.required", "adaptor")
                .stringUtf8BytesRange("adaptor.length", 0, 1, 128)
                .integerRange("period.range", "period", 1, 12)
                .longRange("sequence.range", 2, 0L, Long.MAX_VALUE)
                .decimalRange("amount.range", "amount",
                        new BigDecimal("-999999.990"), new BigDecimal("999999.990"))
                .build();

        assertEquals(Arrays.asList("adaptor.required", "adaptor.length", "period.range",
                "sequence.range", "amount.range"), validation.ruleIds());
        assertEquals(Arrays.asList("NOT_NULL", "STRING_UTF8_BYTES_RANGE_CLOSED",
                "INTEGER_RANGE_CLOSED", "LONG_RANGE_CLOSED", "DECIMAL_RANGE_CLOSED"),
                validation.ruleKinds());
        assertEquals(Arrays.asList("0", "0", "1", "2", "3"),
                validation.fieldIndices());
        assertEquals(Arrays.asList("", "1", "1", "0", "-999999.990"),
                validation.lowerBounds());
        assertEquals(Arrays.asList("", "128", "12", Long.toString(Long.MAX_VALUE),
                "999999.990"), validation.upperBounds());
        assertThrows(UnsupportedOperationException.class,
                () -> validation.ruleIds().add("mutated"));
    }

    @Test
    void fieldNamesAreExactUniqueAndRuleIdsAreStableUnique() {
        DataSchema duplicate = new DataSchema(
                new Field("id", "left", DataType.LongType),
                new Field("id", "right", DataType.LongType));

        assertInvalid(() -> RowValidation.builder(duplicate)
                .notNull("id.required", "id"));
        assertInvalid(() -> RowValidation.builder(duplicate)
                .notNull("alias.required", "left"));

        RowValidation indexed = RowValidation.builder(duplicate)
                .notNull("right.required", 1)
                .build();
        assertEquals(Collections.singletonList("1"), indexed.fieldIndices());

        assertInvalid(() -> RowValidation.builder(META)
                .notNull("bad id", 0));
        assertInvalid(() -> RowValidation.builder(META)
                .notNull("duplicate", 0)
                .notNull("duplicate", 1));
        assertInvalid(() -> RowValidation.builder(META)
                .notNull("1starts-with-digit", 0));
    }

    @Test
    void rejectsTypeMismatchesInvalidRangesAndUnboundedModels() {
        assertInvalid(() -> RowValidation.builder(META).build());
        assertInvalid(() -> RowValidation.builder(META)
                .integerRange("wrong.type", 0, 1, 2));
        assertInvalid(() -> RowValidation.builder(META)
                .longRange("reverse", 2, 2L, 1L));
        assertInvalid(() -> RowValidation.builder(META)
                .decimalRange("reverse.decimal", 3, BigDecimal.ONE, BigDecimal.ZERO));
        assertInvalid(() -> RowValidation.builder(META)
                .stringUtf8BytesRange("too.large", 0, 0, 16 * 1024 * 1024 + 1));
        assertInvalid(() -> RowValidation.builder(
                        new DataSchema(new Field("unknown", DataType.NullType)))
                .notNull("unknown.required", 0));

        RowValidation.Builder tooMany = RowValidation.builder(META);
        for (int i = 0; i < RowValidation.MAX_RULES; i++) {
            tooMany.notNull("r" + i, 0);
        }
        assertInvalid(() -> tooMany.notNull("overflow", 0));
    }

    @Test
    void canonicalRebuildRejectsForgedListsBoundsAndOutputSchemaDrift() {
        RowValidation original = RowValidation.builder(META)
                .notNull("adaptor.required", 0)
                .decimalRange("amount.range", 3,
                        new BigDecimal("-10.00"), new BigDecimal("10.00"))
                .build();
        RowValidation rebuilt = RowValidation.fromDescribable(
                META, META, original.ruleIds(), original.ruleKinds(), original.fieldIndices(),
                original.lowerBounds(), original.upperBounds());
        assertEquals(original.ruleIds(), rebuilt.ruleIds());
        assertEquals(original.upperBounds(), rebuilt.upperBounds());

        assertInvalid(() -> RowValidation.fromDescribable(
                META, META, original.ruleIds(), original.ruleKinds(),
                Collections.singletonList("0"), original.lowerBounds(), original.upperBounds()));

        List<String> nonCanonicalIndex = new ArrayList<>(original.fieldIndices());
        nonCanonicalIndex.set(0, "00");
        assertInvalid(() -> RowValidation.fromDescribable(
                META, META, original.ruleIds(), original.ruleKinds(), nonCanonicalIndex,
                original.lowerBounds(), original.upperBounds()));

        List<String> scientific = new ArrayList<>(original.lowerBounds());
        scientific.set(1, "-1E+1");
        assertInvalid(() -> RowValidation.fromDescribable(
                META, META, original.ruleIds(), original.ruleKinds(), original.fieldIndices(),
                scientific, original.upperBounds()));

        DataSchema drifted = new DataSchema(
                new Field("adaptor", "changed", DataType.StringType),
                new Field("period", DataType.IntegerType),
                new Field("sequence", DataType.LongType),
                new Field("amount", DataType.BigDecimalType));
        assertInvalid(() -> RowValidation.fromDescribable(
                META, drifted, original.ruleIds(), original.ruleKinds(), original.fieldIndices(),
                original.lowerBounds(), original.upperBounds()));
    }

    @Test
    void datasetBindingRejectsSchemaDriftAndPreservesExactOutputMeta() {
        RowValidation validation = RowValidation.builder(META)
                .notNull("adaptor.required", 0)
                .build();
        DataRow row = new DataRow(4);
        row.setValues(new Object[]{"1001", 1, 1L, BigDecimal.ONE});
        ExecutionEnvironment session = new ExecutionEnvironment("quality-model", "quality model");
        DataSet dataSet = session.fromSource(new CollectionSource(META, Arrays.asList(row)));

        assertSame(META, dataSet.validateRows(validation).getSchema());

        DataSchema drifted = new DataSchema(
                new Field("adaptor", "changed", DataType.StringType),
                new Field("period", DataType.IntegerType),
                new Field("sequence", DataType.LongType),
                new Field("amount", DataType.BigDecimalType));
        ExecutionEnvironment other = new ExecutionEnvironment("quality-drift", "quality drift");
        DataSet driftedData = other.fromSource(new CollectionSource(drifted, Arrays.asList(row)));
        IllegalArgumentException mismatch = assertThrows(IllegalArgumentException.class,
                () -> driftedData.validateRows(validation));
        assertTrue(mismatch.getMessage().contains("QUALITY_VALIDATION_INPUT_MISMATCH"),
                mismatch::getMessage);
    }

    private static void assertInvalid(org.junit.jupiter.api.function.Executable executable) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(failure.getMessage().startsWith("QUALITY_VALIDATION_INVALID:"),
                failure::getMessage);
    }
}
