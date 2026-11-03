package io.rowweave.internal;

import io.rowweave.DataRow;

import java.sql.Timestamp;
import java.util.Date;

/**
 * Isolated snapshot of a typed DataRow, used inside the engine.
 *
 * <p>The DataRow container is always copied. The mutable Date and Timestamp values that DataType admits are copied too; every other legal scalar is immutable and may be shared.
 * This is not a general deep copier for an arbitrary Java object graph.
 */
public final class RowSnapshot {

    private RowSnapshot() {
    }

    public static DataRow copyOf(DataRow source) {
        if (source == null) {
            throw new IllegalArgumentException("source row must not be null");
        }
        DataRow copy = new DataRow(source.size());
        for (int i = 0; i < source.size(); i++) {
            copy.set(i, copyValue(source.get(i)));
        }
        return copy;
    }

    /** Copy one typed scalar without allocating an intermediate DataRow. */
    public static Object copyValue(Object value) {
        if (value instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value;
            Timestamp copy = new Timestamp(timestamp.getTime());
            copy.setNanos(timestamp.getNanos());
            return copy;
        }
        if (value instanceof Date) {
            return ((Date) value).clone();
        }
        return value;
    }
}
