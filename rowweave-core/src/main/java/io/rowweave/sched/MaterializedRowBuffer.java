package io.rowweave.sched;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.Iterator;

/**
 * The materialization buffer behind a fan-out: the shared upstream is <b>materialized once</b> into a
 * SpillStore FIFO with spill as backstop, and then <b>N independent replays</b> are opened for the several
 * downstream sides. This is what breaks the deadlock cycle of a fan-out that re-joins into an
 * order-consuming operator, such as SortMergeJoin, CoGroup, HashJoin or Union, and it plays the role of the
 * ResultPartition of a Flink BLOCKING edge.
 *
 * <p><b>Where it lives</b>: materialization is a property of the fan-out <b>edge</b>, that is of the channel,
 * and not a node of the logical graph. The logical {@code PlanGraph} stays exactly the DAG the user wrote, so
 * the monitor graph corresponds one to one with the program, as in Flink. The execution assembly inserts this
 * component on the fork edge, and it never appears in the logical graph.
 *
 * <p><b>Why the cycle is broken</b>: the producer, being the shared upstream, is drained to the end once
 * inside {@link #fill} and written to the SpillStore, then exits, <b>with no downstream able to backpressure
 * it</b>. Each downstream reads through its own cursor from {@link #replay}, reusing the contract that {@link
 * SpillRun#read()} can be opened repeatedly from the beginning. So a binary that drains one side fully before
 * the other can never pin the broadcast edge it is not reading.
 *
 * <p><b>Extending bounded materialization</b>: this materializes the raw rows of a pass-through, which is the
 * first member of the breaker whitelist whose working set is exactly the data passed through, so <b>spill
 * backstop is required</b> through the SpillStore, and collecting into a bare {@code List<DataRow>} is
 * forbidden. The null and BigDecimal semantics survive {@link RowSerializer} unchanged, since it encodes a
 * bitmap over all fields plus the original values, keeping both precision and nulls.
 *
 * <p><b>Immutability discipline</b>: {@link #fill} is called once, during the writing phase, and afterwards
 * {@link #replay} may be called N times, which is read-only.
 */
public final class MaterializedRowBuffer {

    private final RowSerializer serializer;
    private final SpillStore spillStore;
    private SpillRun run;  // The run during writing, and the materialized run once fill has written; replay opens N cursors over it
    private boolean filled;

    public MaterializedRowBuffer(DataSchema dataSchema, SpillStore spillStore) {
        this.serializer = new RowSerializer(dataSchema);
        this.spillStore = spillStore;
    }

    /**
     * The materializing phase: drain the upstream to the end, serialize each row and append it to the
     * SpillRun, then finishWriting to turn the run over to read mode. The producer returns as soon as it has
     * written, <b>with no downstream backpressure on it</b>, which is the key to breaking the cycle. Should be
     * called once.
     */
    public void fill(RowStream upstream) {
        StreamEvent e;
        while ((e = upstream.next()) != null) {
            if (e.isRecord()) {
                append(e.row());
            }
            // EOS, barrier and cancel: materialization cares only about records, so control events are not written. The end is expressed by cursor exhaustion on replay.
            // Error: the fail-fast of the upstream stage handles it, and it is not swallowed here, since the materialization simply produces nothing.
        }
        finish();
    }

    /**
     * The row-at-a-time writing phase, for a breaker that is itself the producer, such as a BINARY join,
     * which writes its operator output straight into a BLOCKING partition. This path uses the same spill
     * backend as {@link #fill(RowStream)}, and replay is not allowed until {@link #finish()} has been called.
     */
    public void append(DataRow row) {
        if (filled) {
            throw new IllegalStateException("MaterializedRowBuffer already finished");
        }
        if (run == null) {
            run = spillStore.createRun();
        }
        run.append(serializer.serialize(row));
    }

    /** Ends the row-at-a-time writing and switches to a read-only run that can be replayed by several readers; empty output still produces a valid empty run. */
    public void finish() {
        if (filled) {
            throw new IllegalStateException("MaterializedRowBuffer already finished");
        }
        if (run == null) {
            run = spillStore.createRun();
        }
        run.finishWriting();
        filled = true;
    }

    /**
     * The reading phase: opens one <b>independent cursor</b> as a RowStream, reading the materialized rows
     * from the beginning. May be called N times, with the cursors undisturbed by each other, because each is
     * its own {@link Iterator} and {@link SpillRun#read()} starts from the beginning every time.
     */
    public RowStream replay() {
        if (!filled) {
            throw new IllegalStateException("MaterializedRowBuffer not filled yet");
        }
        Iterator<byte[]> it = run.read();
        return () -> it.hasNext() ? StreamEvent.record(serializer.deserialize(it.next())) : null;
    }
}
