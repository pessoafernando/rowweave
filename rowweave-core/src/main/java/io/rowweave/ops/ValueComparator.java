package io.rowweave.ops;

import java.math.BigDecimal;

/**
 * A single-value comparison helper used by MAX/MIN aggregation: numeric values across Integer/Long/BigDecimal compare uniformly as BigDecimal numbers
 * (10 equals 10.0), and any other comparable type follows its natural order. The non-null comparison semantics match {@link RowComparator}.
 *
 * <p>Only non-null values are compared; null is left to the caller, which handles it with the SQL semantics of ignoring null.
 */
final class ValueComparator {

    private ValueComparator() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static int compare(Object a, Object b) {
        if (isNumeric(a) && isNumeric(b)) {
            // both BigDecimal, same scale, precision<=18 -> compare unscaled longs (~3ns) instead of
            // BigDecimal.compareTo. Scale must match (50.0/unscaled500 vs 50.00/unscaled5000 are equal as
            // decimals — comparing raw unscaled across scales would misjudge). precision<=18 fits a long.
            if (a instanceof BigDecimal && b instanceof BigDecimal) {
                BigDecimal x = (BigDecimal) a;
                BigDecimal y = (BigDecimal) b;
                if (x.scale() == y.scale() && x.precision() <= 18 && y.precision() <= 18) {
                    return Long.compare(x.unscaledValue().longValue(), y.unscaledValue().longValue());
                }
            }
            return toBigDecimal(a).compareTo(toBigDecimal(b));
        }
        if (a instanceof Comparable) {
            return ((Comparable) a).compareTo(b);
        }
        throw new IllegalArgumentException("Cannot compare value of type " + a.getClass().getName());
    }

    private static boolean isNumeric(Object v) {
        return v instanceof Integer || v instanceof Long || v instanceof BigDecimal;
    }

    private static BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        return BigDecimal.valueOf(((Number) v).longValue());
    }
}
