package io.rowweave;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataSchemaTest {

    @Test
    void fieldAccessAndCount() {
        DataSchema meta = new DataSchema(
                new Field("id", DataType.LongType),
                new Field("name", DataType.StringType));
        assertEquals(2, meta.getFieldCount());
        assertEquals("id", meta.getField(0).getName());
        assertEquals(DataType.StringType, meta.getField(1).getDataType());
    }

    @Test
    void getFieldIndexByName() {
        DataSchema meta = new DataSchema(
                new Field("id", DataType.LongType),
                new Field("amount", DataType.BigDecimalType));
        assertEquals(0, meta.getFieldIndex("id"));
        assertEquals(1, meta.getFieldIndex("amount"));
        assertEquals(-1, meta.getFieldIndex("missing"));
    }

    @Test
    void getFieldIndexFallsBackToAlias() {
        DataSchema meta = new DataSchema(
                new Field("col0", "total", DataType.BigDecimalType));
        assertEquals(0, meta.getFieldIndex("col0"));
        assertEquals(0, meta.getFieldIndex("total"));
    }

    @Test
    void nameTakesPrecedenceOverAlias() {
        // field A: name="x"; field B: name="b", alias="x" → lookup "x" hits name match first (index 0)
        DataSchema meta = new DataSchema(
                new Field("x", "ax", DataType.IntegerType),
                new Field("b", "x", DataType.IntegerType));
        assertEquals(0, meta.getFieldIndex("x"));
    }

    @Test
    void aliasDefaultsToName() {
        Field f = new Field("col", DataType.StringType);
        assertEquals("col", f.getName());
        assertEquals("col", f.getAlias());

        Field nullAlias = new Field("col", null, DataType.StringType);
        assertEquals("col", nullAlias.getAlias());
    }
}
