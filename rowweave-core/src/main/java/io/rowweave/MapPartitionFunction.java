package io.rowweave;

import java.io.Serializable;

/** Streaming transformation invoked once for each physical Batch partition. */
public interface MapPartitionFunction extends ResultSchemaAware, Serializable {
    void mapPartition(PartitionDataRowCursor rows, Collector collector);
}
