package io.rowweave;

import java.io.Serializable;

/**
 * Portable incremental aggregate for one keyed window namespace.
 *
 * <p>Accumulator byte arrays are canonical values owned by the function. The
 * runtime treats arguments as immutable and stores each returned array as the
 * complete replacement accumulator. {@link #merge(byte[], byte[])} is used by
 * session windows when adjacent namespaces are coalesced.
 */
public interface StreamWindowAggregateFunction extends Serializable {

    byte[] createAccumulator();

    byte[] add(DataRow value, byte[] currentAccumulator);

    byte[] merge(byte[] leftAccumulator, byte[] rightAccumulator);

    /** Projects one trigger firing to its UPSERT or APPEND result row. */
    DataRow getResult(byte[] accumulator, StreamWindowContext context);
}
