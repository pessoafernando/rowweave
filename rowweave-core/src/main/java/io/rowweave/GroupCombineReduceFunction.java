package io.rowweave;

import java.io.Serializable;

/**
 * combine + reduce callback: a map-side pre-aggregation combine
 * (which shrinks the shuffle volume and keeps the input/output structure identical) plus the final reduce (which reduces every combine result into the final output).
 * Implements {@link ResultSchemaAware} (the reduce output DataSchema comes from {@link #getResultSchema()}) plus Serializable.
 */
public interface GroupCombineReduceFunction extends ResultSchemaAware, Serializable {

    /** Map-side pre-aggregation: combine a batch of rows locally; the output structure must match the input (so it can be combined or reduced again). */
    void combine(Iterable<DataRow> iter, Collector collector);

    /** Final reduce: reduce every combine result into the final output rows (the structure is defined by {@link #getResultSchema()}). */
    void reduce(Iterable<DataRow> iter, Collector collector);
}
