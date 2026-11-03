package io.rowweave;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

/**
 * The RowWeave row object: field values stored by index.
 * Typed getters return null for a null value; getBigDecimal performs a numeric conversion for integral types.
 */
public final class DataRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Object[] values;

    public DataRow(int fieldCount) {
        this.values = new Object[fieldCount];
    }

    public int size() {
        return values.length;
    }

    public void set(int index, Object value) {
        values[index] = value;
    }

    public Object get(int index) {
        return values[index];
    }

    /** The backing array holding the whole row. */
    public Object[] values() {
        return values;
    }

    /** Replaces the whole row; the length must match the field count. */
    public void setValues(Object[] vals) {
        if (vals == null || vals.length != values.length) {
            throw new IllegalArgumentException("setValues length mismatch: expected " + values.length
                    + " got " + (vals == null ? "null" : vals.length));
        }
        System.arraycopy(vals, 0, values, 0, values.length);
    }

    /** Copies this row into an independent DataRow (value references are shallow-copied, the array is fresh). */
    public DataRow copy() {
        DataRow c = new DataRow(values.length);
        System.arraycopy(values, 0, c.values, 0, values.length);
        return c;
    }

    /** Converts the row into a map keyed by DataSchema field names (reuse may be reused, null creates a new map). */
    public Map<String, Object> toMap(DataSchema dataSchema, Map<String, Object> reuse) {
        Map<String, Object> out = (reuse != null) ? reuse : new java.util.HashMap<>();
        for (int i = 0; i < values.length && i < dataSchema.getFieldCount(); i++) {
            out.put(dataSchema.getField(i).getName(), values[i]);
        }
        return out;
    }

    public String getString(int index) {
        Object v = values[index];
        return (v == null) ? null : (String) v;
    }

    public Integer getInteger(int index) {
        Object v = values[index];
        return (v == null) ? null : (Integer) v;
    }

    public Long getLong(int index) {
        Object v = values[index];
        return (v == null) ? null : (Long) v;
    }

    public BigDecimal getBigDecimal(int index) {
        Object v = values[index];
        if (v == null) {
            return null;
        }
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        if (v instanceof Integer || v instanceof Long) {
            return BigDecimal.valueOf(((Number) v).longValue());
        }
        if (v instanceof Number) {
            return new BigDecimal(v.toString());
        }
        throw new ClassCastException("Cannot convert " + v.getClass().getName() + " to BigDecimal at index " + index);
    }

    public Boolean getBoolean(int index) {
        Object v = values[index];
        return (v == null) ? null : (Boolean) v;
    }

    public Double getDouble(int index) {
        Object v = values[index];
        if (v == null) {
            return null;
        }
        if (v instanceof Double) {
            return (Double) v;
        }
        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }
        throw new ClassCastException("Cannot convert " + v.getClass().getName() + " to Double at index " + index);
    }

    public Date getDate(int index) {
        Object v = values[index];
        return (v == null) ? null : (Date) v;
    }

    public Timestamp getTimestamp(int index) {
        Object v = values[index];
        return (v == null) ? null : (Timestamp) v;
    }
}
