package io.rowweave;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class RowTest {

    @Test
    void getSet() {
        DataRow row = new DataRow(3);
        row.set(0, "hello");
        row.set(1, 42);
        row.set(2, null);
        assertEquals("hello", row.get(0));
        assertEquals(42, row.get(1));
        assertNull(row.get(2));
        assertEquals(3, row.size());
    }

    @Test
    void typedGettersReturnNullForNull() {
        DataRow row = new DataRow(6);
        assertNull(row.getString(0));
        assertNull(row.getInteger(1));
        assertNull(row.getLong(2));
        assertNull(row.getBigDecimal(3));
        assertNull(row.getBoolean(4));
        assertNull(row.getDouble(5));
    }

    @Test
    void getBigDecimalConvertsIntegralTypes() {
        DataRow row = new DataRow(3);
        row.set(0, new BigDecimal("1.5000000000"));
        row.set(1, 7);
        row.set(2, 9L);
        assertEquals(new BigDecimal("1.5000000000"), row.getBigDecimal(0));
        assertEquals(BigDecimal.valueOf(7L), row.getBigDecimal(1));
        assertEquals(BigDecimal.valueOf(9L), row.getBigDecimal(2));
    }

    @Test
    void copyIsIndependent() {
        DataRow row = new DataRow(2);
        row.set(0, "a");
        row.set(1, "b");
        DataRow copy = row.copy();
        copy.set(0, "z");
        assertEquals("a", row.get(0));
        assertEquals("z", copy.get(0));
    }

    @Test
    void setValuesRejectsLengthMismatch() {
        DataRow row = new DataRow(2);
        assertThrows(IllegalArgumentException.class, () -> row.setValues(new Object[]{1, 2, 3}));
        assertThrows(IllegalArgumentException.class, () -> row.setValues(null));
    }
}
