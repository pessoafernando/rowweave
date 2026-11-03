package io.rowweave;

/**
 * Metadata accessor: an operator whose output DataSchema comes from a user Function
 * (reduceGroup / coGroup and the like) reads the result header through this interface, so the caller never passes outMeta explicitly.
 */
public interface ResultSchemaAware {

    /** Read the (output) metadata. */
    DataSchema getResultSchema();
}
