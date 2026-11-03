package io.rowweave.stream.api;

import io.rowweave.DataType;
import java.util.Objects;

/** Immutable Stream field with identity and nullability preserved across restore. */
public final class StreamField {
    private final String fieldId;
    private final String name;
    private final String alias;
    private final DataType dataType;
    private final boolean nullable;

    public StreamField(String fieldId, String name, String alias,
                       DataType dataType, boolean nullable) {
        this.fieldId = required(fieldId, "fieldId");
        this.name = required(name, "name");
        this.alias = alias == null ? this.name : required(alias, "alias");
        if (dataType == null) throw new IllegalArgumentException("dataType is required");
        this.dataType = dataType;
        this.nullable = nullable;
    }

    private static String required(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    public String fieldId() { return fieldId; }
    public String name() { return name; }
    public String alias() { return alias; }
    public DataType dataType() { return dataType; }
    public boolean nullable() { return nullable; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamField)) return false;
        StreamField that = (StreamField) other;
        return nullable == that.nullable && fieldId.equals(that.fieldId)
                && name.equals(that.name) && alias.equals(that.alias)
                && dataType == that.dataType;
    }

    @Override public int hashCode() {
        return Objects.hash(fieldId, name, alias, dataType, nullable);
    }
}
