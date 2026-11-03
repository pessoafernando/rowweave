package io.rowweave;

import java.io.Serializable;

/**
 * Stream keyed aggregate with an explicit, incrementally updated accumulator.
 *
 * <p>The accumulator bytes belong to the user function's declared state codec.
 * Implementations must treat the supplied byte array as immutable and return a
 * complete replacement accumulator from {@link #add(DataRow, byte[])}.
 */
public interface StreamAggregateFunction extends Serializable {

    /** Creates the canonical accumulator for a key with no prior state. */
    byte[] createAccumulator();

    /** Incorporates one input row and returns the canonical replacement state. */
    byte[] add(DataRow value, byte[] currentAccumulator);

    /** Projects the current accumulator to the UPSERT after-image. */
    DataRow getResult(byte[] accumulator);
}
