package io.rowweave;

import java.util.Iterator;

/**
 * The bridge: the default executor for a {@link Source} that has no dedicated registered {@link SourceExecutor}
 * (it still implements {@code Iterable<DataRow>}) - it reads by delegating to {@code input.iterator()}. Byte-identical to {@code RowStream.fromIterable}
 * (open does nothing, next delegates to the iterator, close is idempotent).
 *
 * <p><b>Row reference contract</b>: this executor neither copies nor overwrites rows - it returns the row references produced by the underlying
 * iterator directly, so a Source implementation must produce a new object every time. The built-in CollectionSource/IterableSource each build an independent snapshot at their own iterator boundary, and the bridge does not copy again.
 *
 * <p>When a Source implements {@link AutoCloseable}, {@link #close()} calls back into its close as a resource-release floor; otherwise it is a no-op.
 */
public final class IterableInputExecutor extends SourceExecutor<Source> {

    private Iterator<DataRow> it;
    private boolean closed;

    public IterableInputExecutor(Source input) {
        super(input, input.getSchema());
    }

    @Override
    public void open() {
        if (it == null) {
            it = input.iterator();
        }
    }

    @Override
    public boolean hasNext() {
        return it != null && it.hasNext();
    }

    @Override
    public DataRow next() {
        return it.next();
    }

    @Override
    public void close() {
        if (closed) {
            return; // idempotent
        }
        closed = true;
        if (input instanceof AutoCloseable) {
            try {
                ((AutoCloseable) input).close();
            } catch (Exception e) {
                throw new RuntimeException("close input failed", e);
            }
        }
    }
}
