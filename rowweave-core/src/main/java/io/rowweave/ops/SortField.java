package io.rowweave.ops;

import java.io.Serializable;

/**
 * A sort field specification: field index, direction and null sort policy.
 *
 * <p>NullOrder.DEFAULT takes the value-based semantics the engine contract fixes: a numeric null counts as 0 (moving with the direction),
 * and a null in any non-numeric type such as a string counts as the smallest value. NULLS_FIRST / NULLS_LAST are absolute placements that
 * ignore direction and force null to the front or the back.
 */
public final class SortField implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Direction { ASC, DESC }

    public enum NullOrder { DEFAULT, NULLS_FIRST, NULLS_LAST }

    private final int fieldIndex;
    private final Direction direction;
    private final NullOrder nullOrder;

    public SortField(int fieldIndex, Direction direction, NullOrder nullOrder) {
        this.fieldIndex = fieldIndex;
        this.direction = direction;
        this.nullOrder = nullOrder;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    public Direction getDirection() {
        return direction;
    }

    public NullOrder getNullOrder() {
        return nullOrder;
    }
}
