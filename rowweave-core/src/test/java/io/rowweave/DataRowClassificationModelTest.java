package io.rowweave;

import io.rowweave.ops.RowValidation;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public classification pair and its bounded schemas. */
class DataRowClassificationModelTest {

    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));

    @Test
    void exposesOnlyValidAndFixedRedactedRejectedDataSets() {
        DataRowClassification classification = exactSource(META, "ledger.source")
                .classifyRowsWithSourcePosition(validation(META));

        assertSame(META, classification.validRows().getSchema());
        assertNotSame(classification.validRows(), classification.rejectedRows());
        DataSchema rejected = classification.rejectedRows().getSchema();
        assertEquals(7, rejected.getFieldCount());
        assertField(rejected, 0, "source_operator_uid", DataType.StringType);
        assertField(rejected, 1, "split_identity_sha256", DataType.StringType);
        assertField(rejected, 2, "split_local_ordinal", DataType.LongType);
        assertField(rejected, 3, "quality_error_code", DataType.StringType);
        assertField(rejected, 4, "quality_rule_id", DataType.StringType);
        assertField(rejected, 5, "quality_field_index", DataType.IntegerType);
        assertField(rejected, 6, "quality_detail", DataType.StringType);

        long publicMethods = Arrays.stream(DataRowClassification.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .map(Method::getName)
                .filter(name -> name.equals("validRows") || name.equals("rejectedRows")
                        || name.equals("writeToManifest"))
                .count();
        assertEquals(3L, publicMethods);
        for (Constructor<?> constructor : DataRowClassification.class.getDeclaredConstructors()) {
            assertFalse(Modifier.isPublic(constructor.getModifiers()),
                    "callers must not forge a classification pair");
        }
    }

    @Test
    void rejectsNullSchemaDriftNonSourceMissingUidAndLegacySource() {
        DataSet exact = exactSource(META, "ledger.source");
        assertInvalid(() -> exact.classifyRowsWithSourcePosition(null));
        assertInvalid(() -> new BatchSession("missing-uid", "missing uid")
                .fromSource(new CollectionSource(META, "split-a", Collections.emptyList()))
                .classifyRowsWithSourcePosition(validation(META)));
        assertInvalid(() -> new BatchSession("legacy", "legacy")
                .fromSource(new CollectionSource(META, Collections.emptyList()))
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(validation(META)));
        assertInvalid(() -> exact.validateRows(validation(META))
                .classifyRowsWithSourcePosition(validation(META)));

        DataSchema drifted = new DataSchema(
                new Field("adaptor", "changed", DataType.StringType),
                new Field("period", DataType.IntegerType));
        assertInvalid(() -> exact.classifyRowsWithSourcePosition(validation(drifted)));
    }

    @Test
    void rejectsCarrierWhoseInputPlusEightFieldsExceedsPlanLimit() {
        Field[] fields = new Field[RowValidation.MAX_FIELDS - 7];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new Field("f" + i, DataType.IntegerType);
        }
        DataSchema wide = new DataSchema(fields);
        RowValidation validation = RowValidation.builder(wide)
                .notNull("first.required", 0)
                .build();

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> exactSource(wide, "wide.source")
                        .classifyRowsWithSourcePosition(validation));
        assertTrue(failure.getMessage().startsWith("QUALITY_CLASSIFICATION_INVALID:"),
                failure::getMessage);
        assertTrue(failure.getMessage().contains("field"), failure::getMessage);
    }

    private static DataSet exactSource(DataSchema meta, String uid) {
        return new BatchSession("classification-model", "classification model")
                .fromSource(new CollectionSource(meta, "split-a", Collections.emptyList()))
                .uid(uid);
    }

    private static RowValidation validation(DataSchema meta) {
        return RowValidation.builder(meta)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .build();
    }

    private static void assertField(DataSchema meta, int index, String name, DataType type) {
        assertEquals(name, meta.getField(index).getName());
        assertEquals(type, meta.getField(index).getDataType());
    }

    private static void assertInvalid(org.junit.jupiter.api.function.Executable executable) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(failure.getMessage().startsWith("QUALITY_CLASSIFICATION_INVALID:"),
                failure::getMessage);
    }
}
