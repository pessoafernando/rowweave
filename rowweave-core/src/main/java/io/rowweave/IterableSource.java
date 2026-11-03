package io.rowweave;

import io.rowweave.internal.RowSnapshot;

import java.util.Iterator;

/**
 * Adapts any {@code Iterable<DataRow>} + DataSchema into a {@link Source} (every source shape converges on Source).
 * Iterates lazily and collects nothing up front - it delegates to the underlying Iterable's iterator, but takes an independent row snapshot on each next,
 * so row references are never reused (re-iteration still depends on the underlying Iterable).
 *
 * <p>A production source (DB cursor / file / Redis...) only has to implement Source; an in-memory collection can use {@link CollectionSource}
 * or wrap an existing Iterable with this adapter. The engine only knows about Source.
 */
public final class IterableSource implements Source {

    private final DataSchema dataSchema;
    private final Iterable<DataRow> rows;

    private IterableSource(DataSchema dataSchema, Iterable<DataRow> rows) {
        this.dataSchema = dataSchema;
        this.rows = rows;
    }

    public static IterableSource of(DataSchema dataSchema, Iterable<DataRow> rows) {
        return new IterableSource(dataSchema, rows);
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    @Override
    public Iterator<DataRow> iterator() {
        final Iterator<DataRow> delegate = rows.iterator();
        return new Iterator<DataRow>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public DataRow next() {
                return RowSnapshot.copyOf(delegate.next());
            }
        };
    }
}
