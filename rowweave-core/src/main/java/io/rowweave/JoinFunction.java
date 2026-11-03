package io.rowweave;

import java.io.Serializable;

/**
 * Function for custom join result columns: produces an output row for each matched pair of left and right rows.
 * Implements {@link ResultSchemaAware} (the output DataSchema comes from the function) plus Serializable.
 *
 * <p>Used by join's withFunc; withFunc takes precedence over select (when withFunc is given, select is ignored).
 * In an outer join, the row of the missing side is passed as {@code null} (the null-padding semantics of left/right join).
 */
public interface JoinFunction extends ResultSchemaAware, Serializable {

    /** Handle one pair of join result rows: row1 = the left row (possibly null), row2 = the right row (possibly null). */
    DataRow join(DataRow row1, DataRow row2);
}
