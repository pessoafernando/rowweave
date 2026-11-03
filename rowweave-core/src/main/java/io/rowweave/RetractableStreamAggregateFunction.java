package io.rowweave;

/** Reversible keyed aggregate used by the Stream RETRACT contract. */
public interface RetractableStreamAggregateFunction
        extends StreamAggregateFunction {

    /** Removes one previously incorporated row from the accumulator. */
    byte[] retract(DataRow value, byte[] currentAccumulator);

    /** Returns whether the replacement accumulator has no remaining rows. */
    boolean isEmpty(byte[] accumulator);
}
