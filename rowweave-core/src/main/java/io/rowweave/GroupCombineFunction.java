package io.rowweave;

import java.io.Serializable;

/**
 * combine callback: a map-side pre-aggregation that combines a batch of rows locally
 * and emits the result through a Collector. <b>The input and output structures must match</b> (after combine the DataSchema is unchanged, so downstream aggregation can continue).
 * Implements {@link ResultSchemaAware} (output DataSchema, same structure as the input) plus Serializable.
 *
 * <p>The default {@link #getResultSchema()} returns null - the caller (combineGroup) then uses the input DataSchema
 * as the output (the matching-structure constraint); a user may still override it and return an explicit meta.
 */
public interface GroupCombineFunction extends ResultSchemaAware, Serializable {

    void combine(Iterable<DataRow> iter, Collector collector);

    /** The combine output structure matches the input, so the default returns null (the caller uses the input DataSchema). */
    @Override
    default DataSchema getResultSchema() {
        return null;
    }
}
