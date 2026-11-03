package io.rowweave.ops;

import io.rowweave.Collector;
import io.rowweave.FlatMapFunction;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

/**
 * The 1-to-N mapping operator (N may be 0; pass-through, purely streaming).
 *
 * <p>The pure Push contract: pull one row at a time from {@link RowStream} and push to {@link RowSink}, emitting as it evaluates, and never return or materialize a List.
 */
public final class FlatMap {

    private final FlatMapFunction fn;

    private FlatMap(FlatMapFunction fn) {
        this.fn = fn;
    }

    public static FlatMap of(FlatMapFunction fn) {
        return new FlatMap(fn);
    }

    /**
     * Streaming flatMap: each RECORD is expanded into 0..N rows pushed through a Collector into the RowSink; control events pass through,
     * and processing stops after Error/Cancel; once upstream is exhausted, endOfStream is emitted.
     */
    public void apply(RowStream in, RowSink out) {
        Collector adapter = out::emitRow; // turns the per-row collect of a user flatMap into RowSink pushes
        StreamEvent e;
        while ((e = in.next()) != null) {
            if (!e.isRecord()) {
                out.emit(e); // pass Barrier/Error/Cancel through
                if (e.isError() || e.isCancel()) {
                    return;
                }
                continue;
            }
            fn.flatMap(e.row(), adapter);
        }
        out.endOfStream();
    }
}
