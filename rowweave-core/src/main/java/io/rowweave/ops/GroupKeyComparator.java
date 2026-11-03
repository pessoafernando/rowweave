package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Comparator;

/**
 * The authoritative total order for a grouping key: null sorts last, and numbers that compare equal are ordered further by BigDecimal scale.
 * Sorting, same-side block boundaries and the CoGroup merge across sides must all reuse this, so that compare==0 and exact key equality mean the same thing.
 */
public final class GroupKeyComparator implements Comparator<DataRow>, Serializable {

    private static final long serialVersionUID = 1L;

    private final DataSchema dataSchema;
    private final int[] keyIndices;

    public GroupKeyComparator(DataSchema dataSchema, int[] keyIndices) {
        if (dataSchema == null) {
            throw new IllegalArgumentException("dataSchema must not be null");
        }
        if (keyIndices == null || keyIndices.length == 0) {
            throw new IllegalArgumentException("keyIndices must not be empty");
        }
        this.dataSchema = dataSchema;
        this.keyIndices = keyIndices.clone();
    }

    public DataSchema getSchema() {
        return dataSchema;
    }

    public int[] getKeyIndices() {
        return keyIndices.clone();
    }

    @Override
    public int compare(DataRow left, DataRow right) {
        return compareRows(left, dataSchema, keyIndices, right, dataSchema, keyIndices);
    }

    public boolean sameKey(DataRow left, DataRow right) {
        return compare(left, right) == 0;
    }

    /** A CoGroup may have different left and right schemas; the field count must be validated as equal at construction. */
    public static int compareRows(DataRow left, DataSchema leftMeta, int[] leftIndices,
                                  DataRow right, DataSchema rightMeta, int[] rightIndices) {
        if (leftIndices.length != rightIndices.length) {
            throw new IllegalArgumentException("group key field counts must match");
        }
        for (int i = 0; i < leftIndices.length; i++) {
            int leftIndex = leftIndices[i];
            int rightIndex = rightIndices[i];
            int compared = compareValue(left.get(leftIndex), leftMeta.getField(leftIndex).getDataType(),
                    right.get(rightIndex), rightMeta.getField(rightIndex).getDataType());
            if (compared != 0) {
                return compared;
            }
        }
        return 0;
    }

    private static int compareValue(Object left, DataType leftType, Object right, DataType rightType) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        if (isNumeric(leftType) && isNumeric(rightType)) {
            BigDecimal a = toBigDecimal(left);
            BigDecimal b = toBigDecimal(right);
            int value = a.compareTo(b);
            return value != 0 ? value : Integer.compare(a.scale(), b.scale());
        }
        @SuppressWarnings("unchecked")
        Comparable<Object> comparable = (Comparable<Object>) left;
        return comparable.compareTo(right);
    }

    private static boolean isNumeric(DataType type) {
        return type == DataType.IntegerType
                || type == DataType.LongType
                || type == DataType.BigDecimalType;
    }

    private static BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        return new BigDecimal(value.toString());
    }
}
