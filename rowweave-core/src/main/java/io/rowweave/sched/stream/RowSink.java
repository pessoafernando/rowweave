package io.rowweave.sched.stream;

import io.rowweave.DataRow;

/**
 * The push output side of an operator, under the pure-push contract: an operator emits {@link StreamEvent}s downstream as it computes,
 * and never returns a List, which is what keeps full materialisation from reaching OOM. Pairs with the pull input side {@link RowStream}:
 * an operator pulls input through RowStream and pushes output through RowSink, which fits a push engine (bounded channel plus credit backpressure between stages).
 *
 * <p>A functional interface (single method {@link #emit}), so it is portable; {@code emitRow} and {@code endOfStream} are convenience default methods.
 */
@FunctionalInterface
public interface RowSink {

    /** Pushes one event downstream (Record, Barrier, EndOfStream, Error or Cancel). */
    void emit(StreamEvent event);

    /** Convenience: push one data row (wrapped as RECORD). */
    default void emitRow(DataRow row) {
        emit(StreamEvent.record(row));
    }

    /** Convenience: push the end-of-stream signal. */
    default void endOfStream() {
        emit(StreamEvent.endOfStream());
    }
}
