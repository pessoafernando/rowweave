package io.rowweave.mem;

import io.rowweave.DataRow;

/**
 * Row storage abstraction for the aggregation and buffering operators.
 * Exposes only putRow (serialize into the buffer), size, and repeatable iteration; no underlying Java object collection is reachable.
 * The heap implementation holds byte[] values; a segmented implementation changes only the storage underneath, so the operators over this interface do not change.
 */
public interface AggBuffer extends Iterable<DataRow> {

    void putRow(DataRow row);

    long size();
}
