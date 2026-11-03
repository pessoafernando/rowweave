package io.rowweave;

/**
 * The enumeration of RowWeave field data types.
 * The constant set is closed: a Field carries exactly one of the types below.
 */
public enum DataType {
    BooleanType,
    StringType,
    IntegerType,
    LongType,
    BigDecimalType,
    DateType,
    TimestampType,
    NullType
}
