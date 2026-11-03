package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Comparator;

/**
 * A multi-column row comparator, which pins the null sort semantics the engine contract fixes.
 *
 * <p>Each {@link SortField} configures its own direction (ASC/DESC) and null policy:
 * <ul>
 * <li>DEFAULT (value-based): a numeric null counts as 0 and moves with the direction, while a null in any non-numeric type counts as the smallest value;</li>
 * <li>NULLS_FIRST / NULLS_LAST: an absolute placement that ignores direction and forces null to the front or the back.</li>
 * </ul>
 * When a comparison returns 0, a stable sort keeps the input order, which is what makes orderBy stable.
 */
public final class RowComparator implements Comparator<DataRow>, Serializable {

    private static final long serialVersionUID = 1L;

    private final DataSchema dataSchema;
    private final SortField[] sortFields;

    public RowComparator(DataSchema dataSchema, SortField... sortFields) {
        if (sortFields.length == 0) {
            throw new IllegalArgumentException("RowComparator requires at least one SortField");
        }
        this.dataSchema = dataSchema;
        this.sortFields = sortFields.clone();
    }

    public DataSchema getSchema() {
        return dataSchema;
    }

    public SortField[] getSortFields() {
        return sortFields.clone();
    }

    @Override
    public int compare(DataRow a, DataRow b) {
        for (SortField sf : sortFields) {
            int c = compareField(a, b, sf);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private int compareField(DataRow a, DataRow b, SortField sf) {
        int idx = sf.getFieldIndex();
        Object av = a.get(idx);
        Object bv = b.get(idx);
        DataType type = dataSchema.getField(idx).getDataType();
        boolean desc = sf.getDirection() == SortField.Direction.DESC;

        switch (sf.getNullOrder()) {
            case NULLS_FIRST:
                return absoluteNull(av, bv, true, desc, type);
            case NULLS_LAST:
                return absoluteNull(av, bv, false, desc, type);
            case DEFAULT:
            default:
                return valueBasedNull(av, bv, desc, type);
        }
    }

    /** NULLS_FIRST/LAST: null is placed absolutely, ignoring direction; only non-null values compare by value with the direction applied. */
    private int absoluteNull(Object av, Object bv, boolean nullsFirst, boolean desc, DataType type) {
        if (av == null && bv == null) {
            return 0;
        }
        if (av == null) {
            return nullsFirst ? -1 : 1;
        }
        if (bv == null) {
            return nullsFirst ? 1 : -1;
        }
        int cmp = compareNonNull(av, bv, type);
        return desc ? -cmp : cmp;
    }

    /** DEFAULT value-based: a numeric null becomes 0, and any non-numeric null counts as the smallest value. Both move with the direction. */
    private int valueBasedNull(Object av, Object bv, boolean desc, DataType type) {
        int cmp;
        if (isNumeric(type)) {
            BigDecimal an = (av == null) ? BigDecimal.ZERO : toBigDecimal(av);
            BigDecimal bn = (bv == null) ? BigDecimal.ZERO : toBigDecimal(bv);
            cmp = an.compareTo(bn);
        } else {
            if (av == null && bv == null) {
                cmp = 0;
            } else if (av == null) {
                cmp = -1; // null is smallest
            } else if (bv == null) {
                cmp = 1;
            } else {
                cmp = compareNonNull(av, bv, type);
            }
        }
        return desc ? -cmp : cmp;
    }

    private boolean isNumeric(DataType type) {
        return type == DataType.IntegerType
                || type == DataType.LongType
                || type == DataType.BigDecimalType;
    }

    private BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        if (v instanceof Integer || v instanceof Long) {
            return BigDecimal.valueOf(((Number) v).longValue());
        }
        return new BigDecimal(v.toString());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareNonNull(Object av, Object bv, DataType type) {
        if (isNumeric(type)) {
            return toBigDecimal(av).compareTo(toBigDecimal(bv));
        }
        return ((Comparable) av).compareTo(bv);
    }
}
