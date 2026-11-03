package io.rowweave.sched.stream;

import io.rowweave.DataRow;

import java.util.Iterator;
import java.util.List;

/**
 * Contract for the internal row stream: the lazy flow between operators and inside the execution layer. Each {@link #next()} pulls one
 * {@link StreamEvent}, and the true end of the stream returns {@code null}.
 *
 * <p><b>Boundary contract</b>: {@code java.util.Iterator} and {@code Iterable} are used only at the outward API, that is where a user can see them;
 * internally RowStream is used everywhere. Adaptation happens once, at the API entry and exit: {@link #fromIterable} in, {@link #toIterator} out.
 *
 * <p>A functional interface, so a lambda can express a lazy source such as a spill merge or a network pull.
 */
@FunctionalInterface
public interface RowStream {

    /** Pulls the next event; returns null at the true end of the stream. Lazy: nothing upstream may be consumed ahead of the call. */
    StreamEvent next();

    /** Engine-internal capability: cancel reliably unblocks next() and the upstream send(). A user-supplied stream declares nothing by default. */
    default boolean supportsCancellation() {
        return false;
    }

    /** Poisons the underlying transport and wakes a blocked reader; called only when supportsCancellation is true. */
    default boolean cancel(Throwable cause) {
        return false;
    }

    /** API boundary adapter: wraps an outward Iterable&lt;DataRow&gt; into an internal RowStream (one RECORD per row, null once exhausted). */
    static RowStream fromIterable(Iterable<DataRow> rows) {
        Iterator<DataRow> it = rows.iterator();
        return () -> it.hasNext() ? StreamEvent.record(it.next()) : null;
    }

    /** Uniform cancellable adapter over an engine Channel; ERROR is emitted once only, and cancel poisons the channel and releases both ends from park. */
    static RowStream fromChannel(io.rowweave.sched.Channel channel) {
        return fromChannel(channel, null);
    }

    /** Channel adapter with a RECORD observation callback (used by the cluster monitor for counting). */
    static RowStream fromChannel(io.rowweave.sched.Channel channel, Runnable onRecord) {
        return new RowStream() {
            private boolean terminal;

            @Override
            public StreamEvent next() {
                if (terminal) {
                    return null;
                }
                try {
                    StreamEvent event = channel.poll();
                    if (event != null && event.isRecord() && onRecord != null) {
                        onRecord.run();
                    }
                    if (event != null && (event.isError() || event.isCancel())) {
                        terminal = true;
                    }
                    return event;
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted reading channel", interrupted);
                }
            }

            @Override
            public boolean supportsCancellation() {
                return true;
            }

            @Override
            public boolean cancel(Throwable cause) {
                Throwable effective = cause;
                if (effective == null) {
                    effective = new java.util.concurrent.CancellationException("channel stream cancelled");
                }
                terminal = true;
                channel.fail(effective);
                return true;
            }
        };
    }

    /**
     * A closeable RowStream wrapping the execution-side row flow of an {@link io.rowweave.SourceExecutor}. Closes automatically when the stream is
     * drained, or explicitly through try-with-resources or {@link #close()} on cancellation (idempotent, the backstop that releases executor resources).
     */
    interface CloseableRowStream extends RowStream, AutoCloseable {
        @Override
        void close();
    }

    /**
     * Builds an execution-side row flow from a <b>single</b> {@link io.rowweave.Source} description, through
     * {@link io.rowweave.SourceExecutorFactory}: open the executor, then next() lazily, producing a fresh DataRow per row.
     * Closes immediately once the executor reports hasNext false; the caller may also use try-with-resources or close early (idempotent).
     */
    static CloseableRowStream fromSource(io.rowweave.Source input) {
        return fromInputs(java.util.Collections.singletonList(input));
    }

    /**
     * Builds an execution-side row flow by chaining several {@link io.rowweave.Source} descriptions (source splits, ConcatSource semantics). Each Source is
     * built, opened, drained and closed in turn before the next. An exception anywhere, or an early {@link CloseableRowStream#close()}, closes the current
     * executor and every one already opened, so no resource leaks; idempotent.
     */
    static CloseableRowStream fromInputs(List<io.rowweave.Source> inputs) {
        return new CloseableRowStream() {
            private final io.rowweave.internal.SequentialInputExecutorCursor cursor =
                    new io.rowweave.internal.SequentialInputExecutorCursor(inputs);
            private final java.util.concurrent.atomic.AtomicBoolean closed =
                    new java.util.concurrent.atomic.AtomicBoolean();

            {
                cursor.open();
            }

            @Override
            public StreamEvent next() {
                if (closed.get()) {
                    return null;
                }
                try {
                    if (!cursor.hasNext()) {
                        close(); // All sources drained - close as the backstop.
                        return null;
                    }
                    return StreamEvent.record(cursor.next());
                } catch (RuntimeException e) {
                    close(); // Read failed - release the resources, then rethrow.
                    throw e;
                }
            }

            @Override
            public void close() {
                if (closed.compareAndSet(false, true)) {
                    cursor.close();
                }
            }
        };
    }

    /** exact bounded split stream with deterministic source-local row positions. */
    static CloseableRowStream fromSourceBindings(
            String sourceOperatorUid,
            List<io.rowweave.BatchSourceSplitBinding> sourceBindings) {
        return new io.rowweave.internal.PositionedSourceRowStream(
                sourceOperatorUid, sourceBindings);
    }

    /** API boundary adapter: exposes an internal RowStream as an outward Iterator&lt;DataRow&gt; (RECORD rows only; EOS ends it; Error is thrown). */
    default Iterator<DataRow> toIterator() {
        RowStream self = this;
        return new Iterator<DataRow>() {
            private DataRow head = pull();

            private DataRow pull() {
                StreamEvent e = self.next();
                while (e != null) {
                    if (e.isRecord()) {
                        return e.row();
                    }
                    if (e.isError()) {
                        throw new RuntimeException("stream error", e.cause());
                    }
                    if (e.isEndOfStream() || e.isCancel()) {
                        return null;
                    }
                    // BARRIER and other control events are transparent at the API boundary
                    e = self.next();
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return head != null;
            }

            @Override
            public DataRow next() {
                DataRow cur = head;
                head = pull();
                return cur;
            }
        };
    }
}
