package io.rowweave.equivalence;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RowSetComparatorTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType),
            new Field("n", DataType.IntegerType));

    private static DataRow row(String k, String amt, Integer n) {
        DataRow r = new DataRow(3);
        r.set(0, k);
        r.set(1, amt == null ? null : new BigDecimal(amt));
        r.set(2, n);
        return r;
    }

    /** A different row order but the same set still counts as equal, since the rows agree once sorted. */
    @Test
    void sameSetDifferentOrderIsEqual() {
        List<DataRow> a = Arrays.asList(row("x", "1.0", 1), row("y", "2.0", 2));
        List<DataRow> b = Arrays.asList(row("y", "2.0", 2), row("x", "1.0", 1));
        assertTrue(RowSetComparator.equal(META, a, b), "the same multiset in a different order is equal");
    }

    /** Two BigDecimal values with different scales but the same number are equal, decided by setScale(10,HALF_UP) and compareTo rather than equals. */
    @Test
    void bigDecimalScaleInsensitive() {
        List<DataRow> a = Arrays.asList(row("x", "1.5", 1));
        List<DataRow> b = Arrays.asList(row("x", "1.5000000000", 1));
        assertTrue(RowSetComparator.equal(META, a, b), "1.5 and 1.5000000000 are numerically equal");
    }

    /** A multiset: the count of repeated rows has to match, which is what the flatMap amplification produces. */
    @Test
    void multisetCountMatters() {
        List<DataRow> a = Arrays.asList(row("x", "1.0", 1), row("x", "1.0", 1));
        List<DataRow> b = Arrays.asList(row("x", "1.0", 1));
        assertFalse(RowSetComparator.equal(META, a, b), "a different count of duplicate rows is not equal");
    }

    /** Null values take part in the comparison: two nulls in the same position are equal, and null differs from any value. */
    @Test
    void nullHandling() {
        List<DataRow> a = Arrays.asList(row("x", null, 1));
        List<DataRow> b = Arrays.asList(row("x", null, 1));
        assertTrue(RowSetComparator.equal(META, a, b), "two null values are equal");
        List<DataRow> c = Arrays.asList(row("x", "0", 1));
        assertFalse(RowSetComparator.equal(META, a, c), "null and 0 are not equal");
    }

    /** Where a difference lies: when the two sides differ, the first unequal row is reported for diagnosis. */
    @Test
    void diffReportsMismatch() {
        List<DataRow> a = Arrays.asList(row("x", "1.0", 1));
        List<DataRow> b = Arrays.asList(row("x", "2.0", 1));
        String diff = RowSetComparator.diff(META, a, b);
        assertNotNull(diff, "a difference returns a description");
        assertFalse(diff.isEmpty(), "the difference description is not empty");
    }

    /** A different row count is unequal right away. */
    @Test
    void differentSizeUnequal() {
        List<DataRow> a = Arrays.asList(row("x", "1.0", 1), row("y", "2.0", 2));
        List<DataRow> b = Arrays.asList(row("x", "1.0", 1));
        assertFalse(RowSetComparator.equal(META, a, b), "different row counts are not equal");
    }
}

