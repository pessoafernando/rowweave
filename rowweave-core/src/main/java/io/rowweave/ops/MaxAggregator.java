package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.Accumulator;
import io.rowweave.mem.RowSerializer;

/**
 * The MAX aggregator, with SQL semantics: null is ignored, and an empty set (nothing non-null ever joined) returns null.
 * Comparison goes through {@link ValueComparator} (numeric BigDecimal comparison, natural order for Comparable), consistent with the type handling in sorting and partitioning.
 *
 * <p>Implements {@link Accumulator}: spilling encodes the current extreme as a self-describing cell (reusing the
 * {@link RowSerializer} NullType tagging scheme), and a merge keeps the larger. Taking the maximum is associative and order-independent, so the bytes stay identical across a spill.
 */
public final class MaxAggregator implements Accumulator {

    // single untyped cell: self-describes runtime type (BigDecimal/String/Long/Integer/Boolean)
    private static final RowSerializer CELL =
            new RowSerializer(new DataSchema(new Field("v", DataType.NullType)));

    private Object value;

    public void add(Object v) {
        if (v == null) {
            return;
        }
        if (value == null || ValueComparator.compare(v, value) > 0) {
            value = v;
        }
    }

    public void merge(MaxAggregator other) {
        if (other != null) {
            add(other.value);
        }
    }

    public Object result() {
        return value;
    }

    @Override
    public byte[] writeState() {
        if (value == null) {
            return new byte[0];
        }
        DataRow cell = new DataRow(1);
        cell.set(0, value);
        return CELL.serialize(cell);
    }

    @Override
    public void mergeState(byte[] state) {
        if (state == null || state.length == 0) {
            return;
        }
        add(CELL.deserialize(state).get(0));
    }
}
