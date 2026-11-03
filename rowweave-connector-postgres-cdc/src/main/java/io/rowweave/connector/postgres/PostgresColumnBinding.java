package io.rowweave.connector.postgres;

/** Immutable binding from a PostgreSQL catalog column to a stable field id. */
public final class PostgresColumnBinding {
    private static final String CODE = "POSTGRES_CDC_BINDING_INVALID";
    private final String fieldId;
    private final String columnName;
    private final int attributeNumber;
    private final long typeOid;
    private final int typeModifier;
    private final boolean nullable;

    public PostgresColumnBinding(String fieldId, String columnName,
            int attributeNumber, long typeOid, int typeModifier,
            boolean nullable) {
        PostgresCodecSupport.utf8(fieldId, 256, false, CODE);
        PostgresCodecSupport.utf8(columnName, 63, false, CODE);
        if (attributeNumber < 1 || attributeNumber > 32_767
                || typeModifier < -1) {
            throw invalid();
        }
        PostgresCodecSupport.requireUint32(typeOid, false, CODE);
        this.fieldId = fieldId;
        this.columnName = columnName;
        this.attributeNumber = attributeNumber;
        this.typeOid = typeOid;
        this.typeModifier = typeModifier;
        this.nullable = nullable;
    }

    public String fieldId() { return fieldId; }
    public String columnName() { return columnName; }
    public int attributeNumber() { return attributeNumber; }
    public long typeOid() { return typeOid; }
    public int typeModifier() { return typeModifier; }
    public boolean nullable() { return nullable; }

    private static IllegalArgumentException invalid() {
        return PostgresCodecSupport.invalid(CODE);
    }
}
