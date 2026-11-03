package io.rowweave;

/** Optional descriptor capability surface for a bounded batch {@link Sink}. */
public interface BatchSink extends Sink {

    /** Stable, side-effect-free descriptor. It is frozen into the Plan before execution. */
    BatchSinkDescriptor batchSinkDescriptor();
}
