package io.rowweave;

import io.rowweave.internal.SequentialInputExecutorCursor;

/** Close-aware execution-side reader for one assigned SOURCE binding bucket. */
public final class AssignedSourceInputBatchExecutor
        extends SourceExecutor<AssignedSourceBatch> {

    private final SequentialInputExecutorCursor cursor;

    public AssignedSourceInputBatchExecutor(AssignedSourceBatch input) {
        super(input, input.getSchema());
        this.cursor = new SequentialInputExecutorCursor(input.sources());
    }

    @Override public void open() { cursor.open(); }
    @Override public boolean hasNext() { return cursor.hasNext(); }
    @Override public DataRow next() { return cursor.next(); }
    @Override public void close() { cursor.close(); }
}
