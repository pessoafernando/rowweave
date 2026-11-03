package io.rowweave.examples.scenariolab;

import io.rowweave.DataRow;

import java.util.Arrays;

/**
 * The exact comparison key for a row: encodes every cell of a DataRow as lossless text carrying its runtime
 * type, so results can be compared as multisets.
 *
 * <p>Integer, Long and BigDecimal do not fold into each other, and BigDecimal scale is not lost. That way
 * every scenario can surface a type drift or a scale drift on its own, instead of relying on a handful of
 * dedicated batches. null takes part in comparisons and equals no non-null value.
 */
public final class RowKey {

    private final String[] cells;
    private final int hash;

    private RowKey(String[] cells) {
        this.cells = cells;
        this.hash = Arrays.hashCode(cells);
    }

    public static RowKey of(DataRow row) {
        Object[] vals = row.values();
        String[] cells = new String[vals.length];
        for (int i = 0; i < vals.length; i++) {
            cells[i] = encode(vals[i]);
        }
        return new RowKey(cells);
    }

    public static String encode(Object v) {
        if (v == null) {
            return "NUL";
        }
        if (v instanceof java.math.BigDecimal) {
            java.math.BigDecimal decimal = (java.math.BigDecimal) v;
            return "BigDecimal(scale=" + decimal.scale() + "):" + decimal.toPlainString();
        }
        if (v instanceof java.sql.Timestamp) {
            java.sql.Timestamp timestamp = (java.sql.Timestamp) v;
            return "Timestamp:" + timestamp.getTime() + ":" + timestamp.getNanos();
        }
        if (v instanceof java.util.Date) {
            return v.getClass().getName() + ":" + ((java.util.Date) v).getTime();
        }
        return v.getClass().getName() + ":" + String.valueOf(v);
    }

    public String[] cells() {
        return cells.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowKey)) {
            return false;
        }
        return Arrays.equals(cells, ((RowKey) o).cells);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "(" + String.join(", ", cells) + ")";
    }
}
