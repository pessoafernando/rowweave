package io.rowweave;

/** Reversible aggregate for bounded Stream RETRACT windows. */
public interface RetractableStreamWindowAggregateFunction
        extends StreamWindowAggregateFunction {

    /** Removes one previously incorporated row from the accumulator. */
    byte[] retract(DataRow value, byte[] currentAccumulator);

    /** Returns whether the replacement accumulator has no remaining rows. */
    boolean isEmpty(byte[] accumulator);
}
