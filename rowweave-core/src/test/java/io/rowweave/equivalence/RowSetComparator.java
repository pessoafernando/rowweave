package io.rowweave.equivalence;

import io.rowweave.DataType;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * Set-equivalence comparator shared by the three-way equivalence tests (**test-only**). It decides that two groups of
 * rows are set equivalent, meaning their multisets are equal without depending on row order: each field
 * is normalized (BigDecimal setScale(10, HALF_UP); null sorted by the engine's null semantics), the rows
 * are sorted by every field, and then rows are compared field by field.
 *
 * <p>Correctness points:
 * <ul>
 * <li>BigDecimal is compared with {@code compareTo}==0 after {@code setScale(10,HALF_UP)}, never with
 * equals (equals is scale sensitive).</li>
 * <li>The comparator covers **every field**; otherwise rows that are equivalent keys shuffle internally,
 * the two sides merge in different orders, and a row-by-row compare reports a false mismatch.</li>
 * <li>null has a fixed position: a null number sorts smallest (first) and a null string sorts smallest,
 * with **the same** comparator on both sides rather than relying on engine defaults.</li>
 * <li>Multiset: after sorting on all fields equal rows become adjacent, so comparing row by row already
 * requires the counts to match.</li>
 * </ul>
 */
public final class RowSetComparator {

    private RowSetComparator() { }

    /** Set equivalence: true when the two multisets are equal. */
    public static boolean equal(DataSchema meta, List<DataRow> a, List<DataRow> b) {
        return diff(meta, a, b) == null;
    }

    /** Returns a description of the first difference, or null when the two sides are equal. */
    public static String diff(DataSchema meta, List<DataRow> a, List<DataRow> b) {
        if (a.size() != b.size()) {
            return "the row counts differ: a=" + a.size() + " b=" + b.size();
        }
        List<DataRow> sa = sorted(meta, a);
        List<DataRow> sb = sorted(meta, b);
        Comparator<DataRow> cmp = rowComparator(meta);
        for (int i = 0; i < sa.size(); i++) {
            if (cmp.compare(sa.get(i), sb.get(i)) != 0) {
                return "call " + i + " row (after sorting) differs:\n  a=" + fmt(meta, sa.get(i)) + "\n  b=" + fmt(meta, sb.get(i));
            }
        }
        return null;
    }

    private static List<DataRow> sorted(DataSchema meta, List<DataRow> rows) {
        List<DataRow> copy = new ArrayList<>(rows);
        copy.sort(rowComparator(meta));
        return copy;
    }

    /** A stable comparator over every field, walking the columns in schema order, with null the smallest. */
    static Comparator<DataRow> rowComparator(DataSchema meta) {
        return (x, y) -> {
            int n = meta.getFieldCount();
            for (int i = 0; i < n; i++) {
                int c = compareCell(meta.getField(i).getDataType(), x.get(i), y.get(i));
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        };
    }

    /** One cell: null is the smallest; a BigDecimal is compared after setScale(10) with compareTo; anything else follows its natural order. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static int compareCell(DataType type, Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return -1;  // null is the smallest
        }
        if (b == null) {
            return 1;
        }
        switch (type) {
            case BigDecimalType:
                return norm((BigDecimal) a).compareTo(norm((BigDecimal) b));
            case TimestampType:
                return Long.compare(tsKey((Timestamp) a), tsKey((Timestamp) b));
            case DateType:
                return Long.compare(((Date) a).getTime(), ((Date) b).getTime());
            case BooleanType:
                return Boolean.compare((Boolean) a, (Boolean) b);
            default:
                return ((Comparable) a).compareTo(b);
        }
    }

    private static BigDecimal norm(BigDecimal v) {
        return v.setScale(10, RoundingMode.HALF_UP);
    }

    private static long tsKey(Timestamp ts) {
        return ts.getTime() * 1_000_000L + (ts.getNanos() % 1_000_000L);
    }

    private static String fmt(DataSchema meta, DataRow r) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < meta.getFieldCount(); i++) {
            if (i > 0) sb.append(", ");
            Object v = r.get(i);
            if (v instanceof BigDecimal) v = norm((BigDecimal) v).toPlainString();
            sb.append(v);
        }
        return sb.append(']').toString();
    }
}
