package io.rowweave;

import java.io.Serializable;

/**
 * coGroup function: reduces the left and right group of rows that share one key,
 * emitting 0..N rows through a Collector. Implements {@link ResultSchemaAware} (the output DataSchema comes from the function) plus Serializable.
 *
 * <p>When one side has no rows for a key, an empty Iterable is passed and the call still happens. <b>No key parameter</b>:
 * the grouping key fields sit inside the rows of iter1/iter2 (both sides use the same key), so a user function reads them from the rows as needed.
 * The output header comes from {@link #getResultSchema()}, so the caller no longer passes outMeta explicitly.
 * null participates in grouping - it forms its own group and is never dropped, unlike join, where null never equals null.
 */
public interface CoGroupFunction extends ResultSchemaAware, Serializable {

    void coGroup(Iterable<DataRow> iter1, Iterable<DataRow> iter2, Collector collector);
}
