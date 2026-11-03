package io.rowweave.portable;

/** Explicit v1 data type IDs; declaration order is never a wire contract. */
public enum PortableDataType {
    BOOLEAN(1),
    STRING(2),
    INTEGER(3),
    LONG(4),
    BIG_DECIMAL(5),
    DATE(6),
    TIMESTAMP(7),
    NULL(8);

    private final int wireId;

    PortableDataType(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() { return wireId; }

    public static PortableDataType fromWireId(int wireId) {
        for (PortableDataType value : values()) {
            if (value.wireId == wireId) return value;
        }
        throw PortablePlanText.failure("UNKNOWN_DATA_TYPE", "schema.type",
                "portable data type ID is not supported");
    }
}
