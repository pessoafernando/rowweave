package io.rowweave.ops;

import io.rowweave.DataSchema;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

/**
 * limit/topN without sorting: it takes <b>the first n rows in source order</b>. This differs from the sorting {@link Top}, which sees a full sort - topN needs no sorting and
 * is a <b>streaming pass-through</b>: the first n rows are emitted while being read, and from row n+1 it only drains without emitting. Draining the whole upstream avoids a push deadlock, and the heap stays O(1).
 *
 * <p>Because it streams (it is not a breaker), topN <b>can take part in operator chaining</b>, and the downstream or sink sees the first n rows before the upstream finishes draining (early output).
 */
public final class TopN {

    private final DataSchema dataSchema;
    private final int n;

    private TopN(DataSchema dataSchema, int n) {
        if (n < 0) {
            throw new IllegalArgumentException("n must be >= 0");
        }
        this.dataSchema = dataSchema;
        this.n = n;
    }

    public static TopN of(DataSchema dataSchema, int n) {
        return new TopN(dataSchema, n);
    }

    public DataSchema getOutputSchema() {
        return dataSchema;
    }

    /**
     * Streaming (the pure Push contract): it reads {@link RowStream}, and the first n RECORDs are pushed downstream immediately through {@link RowSink#emitRow}; later RECORDs are read but not emitted
     * (the upstream is drained so a push cannot deadlock). Control events (Error/Cancel) pass through, and endOfStream closes the stream.
     */
    public void applyStream(RowStream in, RowSink out) {
        int emitted = 0;
        StreamEvent e;
        while ((e = in.next()) != null) {
            if (e.isError() || e.isCancel()) {
                out.emit(e);
                return;
            }
            if (!e.isRecord()) {
                continue; // EOS/Barrier: digested inside this operator (endOfStream is emitted once at the end)
            }
            if (emitted < n) {
                out.emitRow(e.row());
                emitted++;
            }
            // From row n+1 on: only drain (the read already happened) and emit nothing - the heap does not grow, so upstream never deadlocks
        }
        out.endOfStream();
    }
}
