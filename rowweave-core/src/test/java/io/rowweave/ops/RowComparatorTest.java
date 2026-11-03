package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class RowComparatorTest {

    private static final DataSchema NUM = new DataSchema(new Field("n", DataType.IntegerType));
    private static final DataSchema STR = new DataSchema(new Field("s", DataType.StringType));

    private static DataRow numRow(Integer v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    private static DataRow strRow(String v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    @Test
    void ascNumericOrder() {
        RowComparator c = new RowComparator(NUM,
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT));
        assertTrue(c.compare(numRow(1), numRow(2)) < 0);
        assertTrue(c.compare(numRow(2), numRow(1)) > 0);
        assertEquals(0, c.compare(numRow(5), numRow(5)));
    }

    @Test
    void descNumericOrder() {
        RowComparator c = new RowComparator(NUM,
                new SortField(0, SortField.Direction.DESC, SortField.NullOrder.DEFAULT));
        assertTrue(c.compare(numRow(1), numRow(2)) > 0);
    }

    @Test
    void defaultNumericNullIsZero() {
        RowComparator c = new RowComparator(NUM,
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT));
        // null valued as 0: null < 1, null > -1, null == 0
        assertTrue(c.compare(numRow(null), numRow(1)) < 0);
        assertTrue(c.compare(numRow(null), numRow(-1)) > 0);
        assertEquals(0, c.compare(numRow(null), numRow(0)));
    }

    @Test
    void defaultStringNullIsSmallest() {
        RowComparator c = new RowComparator(STR,
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT));
        assertTrue(c.compare(strRow(null), strRow("a")) < 0);
        assertEquals(0, c.compare(strRow(null), strRow(null)));
    }

    @Test
    void nullsFirstIgnoresDirection() {
        RowComparator asc = new RowComparator(NUM,
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_FIRST));
        RowComparator desc = new RowComparator(NUM,
                new SortField(0, SortField.Direction.DESC, SortField.NullOrder.NULLS_FIRST));
        assertTrue(asc.compare(numRow(null), numRow(-999)) < 0);
        assertTrue(desc.compare(numRow(null), numRow(999)) < 0); // null still first despite DESC
    }

    @Test
    void nullsLastIgnoresDirection() {
        RowComparator desc = new RowComparator(NUM,
                new SortField(0, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST));
        assertTrue(desc.compare(numRow(null), numRow(999)) > 0); // null last
    }

    @Test
    void bigDecimalScalePreservedInComparison() {
        DataSchema meta = new DataSchema(new Field("d", DataType.BigDecimalType));
        RowComparator c = new RowComparator(meta,
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT));
        DataRow a = new DataRow(1);
        a.set(0, new BigDecimal("1.5000000000"));
        DataRow b = new DataRow(1);
        b.set(0, new BigDecimal("1.5000000001"));
        assertTrue(c.compare(a, b) < 0);
    }

    @Test
    void multiFieldTieBreak() {
        DataSchema meta = new DataSchema(
                new Field("a", DataType.IntegerType),
                new Field("b", DataType.IntegerType));
        RowComparator c = new RowComparator(meta,
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT),
                new SortField(1, SortField.Direction.ASC, SortField.NullOrder.DEFAULT));
        DataRow r1 = new DataRow(2); r1.set(0, 1); r1.set(1, 2);
        DataRow r2 = new DataRow(2); r2.set(0, 1); r2.set(1, 3);
        assertTrue(c.compare(r1, r2) < 0); // tie on a, b decides
    }

    @Test
    void requiresAtLeastOneSortField() {
        assertThrows(IllegalArgumentException.class, () -> new RowComparator(NUM));
    }
}
