package io.rowweave;

import io.rowweave.internal.SequentialInputExecutorCursor;

/** Compatibility executor that gives ConcatSource the same close-aware child lifecycle as source batches. */
public final class ConcatInputExecutor extends SourceExecutor<ConcatSource> {

    private final SequentialInputExecutorCursor cursor;

    public ConcatInputExecutor(ConcatSource input) {
        super(input, input.getSchema());
        this.cursor = new SequentialInputExecutorCursor(input.parts());
    }

    @Override public void open() { cursor.open(); }
    @Override public boolean hasNext() { return cursor.hasNext(); }
    @Override public DataRow next() { return cursor.next(); }
    @Override public void close() { cursor.close(); }
}
