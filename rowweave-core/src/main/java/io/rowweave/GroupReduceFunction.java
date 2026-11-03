package io.rowweave;

import java.io.Serializable;

/**
 * Group reduce function: reduces all the rows of one group,
 * emitting 0..N rows through a Collector. Implements {@link ResultSchemaAware} (the output DataSchema comes from the function) plus Serializable.
 *
 * <p><b>No key parameter</b>: the grouping key fields sit inside every row, so a user function reads them from the rows as needed.
 * The output header comes from {@link #getResultSchema()}, so the caller (Grouper/DataSet) no longer passes outMeta explicitly.
 */
public interface GroupReduceFunction extends ResultSchemaAware, Serializable {

    void reduce(Iterable<DataRow> iter, Collector collector);
}
