package io.rowweave.mem;

/**
 * Contract for a spillable aggregation accumulator: serialize the current, unrounded state and merge in the
 * serialized state of a peer. Merging is associative and order-insensitive, since BigDecimal values accumulate at full precision, so output is byte-identical before and after a spill and across repeated runs.
 */
public interface Accumulator {

    /** Serialize the current intermediate state, left unrounded so that precision is preserved. */
    byte[] writeState();

    /** Merge one serialized peer state into this accumulator: associative and order-insensitive. */
    void mergeState(byte[] state);
}
