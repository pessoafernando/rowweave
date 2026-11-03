package io.rowweave.ops;

import io.rowweave.DataSchema;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.List;

/**
 * The multi-input concat operator (pass-through, purely streaming): it joins all rows in input order without deduplication (SQL UNION ALL semantics, order preserved).
 * Every input must match the target DataSchema (field count plus each column's DataType); that consistency check belongs to the graph layer (DataSet).
 *
 * <p>The pure Push contract: push one row at a time to {@link RowSink} and materialize no List. Note that {@code applyStream} takes a {@code List<RowStream>}
 * (a list of streams, not a {@code List<DataRow>} data carrier), which does not violate that contract.
 */
public final class Union {

    private final DataSchema dataSchema;

    private Union(DataSchema dataSchema) {
        this.dataSchema = dataSchema;
    }

    public static Union of(DataSchema dataSchema) {
        return new Union(dataSchema);
    }

    public DataSchema getSchema() {
        return dataSchema;
    }

    /**
     * Streaming concat: inputs are concatenated one by one in input order, rows are emitted as they arrive with no materialized List, and once every input is exhausted a <b>single</b> endOfStream is emitted.
     * Control events: an input's own EOS or Barrier is <b>not forwarded</b> (one input stream ending is not the union ending); an input's
     * Error or Cancel passes through and terminates at once (later inputs are no longer consumed). UNION ALL semantics, order preserved.
     */
    public void applyStream(List<RowStream> inputs, RowSink out) {
        for (RowStream in : inputs) {
            StreamEvent e;
            while ((e = in.next()) != null) {
                if (e.isRecord()) {
                    out.emitRow(e.row());
                } else if (e.isError() || e.isCancel()) {
                    out.emit(e);
                    return;
                }
                // An input's own EOS/Barrier is absorbed: it is not a logical boundary of the union
            }
        }
        out.endOfStream();
    }
}
