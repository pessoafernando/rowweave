package io.rowweave.portable;

import java.util.Objects;

/** Immutable v1 field identity; nullability is explicit and fingerprinted. */
public final class PortableField {
    public static final int MAX_FIELD_ID_BYTES = 256;
    public static final int MAX_NAME_BYTES = 256;

    private final String fieldId;
    private final String name;
    private final String alias;
    private final PortableDataType dataType;
    private final boolean nullable;

    public PortableField(
            String fieldId, String name, String alias,
            PortableDataType dataType, boolean nullable) {
        this.fieldId = PortablePlanText.require(
                fieldId, "schema.fieldId", MAX_FIELD_ID_BYTES);
        this.name = PortablePlanText.require(
                name, "schema.name", MAX_NAME_BYTES);
        this.alias = PortablePlanText.require(
                alias, "schema.alias", MAX_NAME_BYTES);
        if (dataType == null) {
            throw PortablePlanText.failure("INVALID_FIELD", "schema.type",
                    "data type is required");
        }
        this.dataType = dataType;
        this.nullable = nullable;
    }

    public String fieldId() { return fieldId; }
    public String name() { return name; }
    public String alias() { return alias; }
    public PortableDataType dataType() { return dataType; }
    public boolean nullable() { return nullable; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PortableField)) return false;
        PortableField that = (PortableField) other;
        return nullable == that.nullable
                && fieldId.equals(that.fieldId)
                && name.equals(that.name)
                && alias.equals(that.alias)
                && dataType == that.dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldId, name, alias, dataType, nullable);
    }
}
