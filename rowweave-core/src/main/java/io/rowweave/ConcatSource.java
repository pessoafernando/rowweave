package io.rowweave;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Collections;

/**
 * Concatenates multiple {@link Source} splits **lazily** into one logical Source (no prefetch, no
 * materialization). Used by the cluster source stage when a single instance must read every split —
 * reading only {@code inputs[0]} would silently drop data.
 *
 * <p>All splits must be homogeneous (identical DataSchema, taken from the first). Serializable by
 * delegating to each Source's own Serializable contract. Each {@link #iterator()} replays every split
 * from the beginning, depending on the underlying Source being replayable.
 */
public final class ConcatSource implements Source {

    private static final long serialVersionUID = 1L;

    private final List<Source> parts;

    public ConcatSource(List<Source> parts) {
        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("ConcatSource requires at least one Source");
        }
        this.parts = new ArrayList<>(parts);
    }

    @Override
    public DataSchema getSchema() {
        return parts.get(0).getSchema();
    }

    List<Source> parts() {
        return Collections.unmodifiableList(parts);
    }

    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {
            private int idx = 0;
            private Iterator<DataRow> cur = parts.get(0).iterator();

            @Override
            public boolean hasNext() {
                while (!cur.hasNext()) {
                    if (++idx >= parts.size()) {
                        return false;
                    }
                    cur = parts.get(idx).iterator();
                }
                return true;
            }

            @Override
            public DataRow next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return cur.next();
            }
        };
    }
}
