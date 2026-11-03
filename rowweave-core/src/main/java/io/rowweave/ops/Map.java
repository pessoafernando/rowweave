package io.rowweave.ops;

import io.rowweave.MapFunction;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

/**
 * The 1-to-1 mapping operator (pass-through, purely streaming).
 *
 * <p>The pure Push contract: pull one row at a time from {@link RowStream} and push to {@link RowSink}, emitting as it evaluates, and never return or materialize a List.
 */
public final class Map {

    private final MapFunction fn;

    private Map(MapFunction fn) {
        this.fn = fn;
    }

    public static Map of(MapFunction fn) {
        return new Map(fn);
    }

    /**
     * Streaming map: each RECORD is mapped and then emitted; control events (Barrier/Error/Cancel) pass through downstream,
     * and processing stops after Error/Cancel; once upstream is exhausted, endOfStream is emitted.
     */
    public void apply(RowStream in, RowSink out) {
        StreamEvent e;
        while ((e = in.next()) != null) {
            if (!e.isRecord()) {
                out.emit(e); // pass Barrier/Error/Cancel through
                if (e.isError() || e.isCancel()) {
                    return;
                }
                continue;
            }
            out.emitRow(fn.map(e.row()));
        }
        out.endOfStream();
    }
}
