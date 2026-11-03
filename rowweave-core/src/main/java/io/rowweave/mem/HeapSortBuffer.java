package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * The on-heap SortBuffer: each row is serialized to a byte[] held in a list, and sort() decodes, sorts
 * stably and re-encodes.
 * No spill — it serves only direct calls that bypass PlanGraph, plus tests, while the production path uses
 * PointerSortBuffer, which spills and merges externally.
 *
 * <p>{@link List#sort} (TimSort) is a stable sort, so when RowComparator returns 0 the putRow order is kept,
 * which is what orderBy stability requires.
 *
 * <p>Safety valve: once the row count passes {@code maxRows} it fails loudly with an
 * {@link IllegalStateException} naming the spill implementations to inject, rather than letting the heap
 * drift into an OOM. Failing fast is preferred to running out of memory.
 */
public final class HeapSortBuffer implements SortBuffer {

    /** Default hard row cap when no explicit limit is set (guards against runaway misuse; the production path does not use this implementation). */
    private static final int DEFAULT_MAX_ROWS = 5_000_000;

    private final RowSerializer serializer;
    private final Comparator<DataRow> comparator;
    private final int maxRows;
    private final List<byte[]> rows = new ArrayList<>();

    public HeapSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator) {
        this(dataSchema, comparator, DEFAULT_MAX_ROWS);
    }

    public HeapSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator, int maxRows) {
        this.serializer = new RowSerializer(dataSchema);
        this.comparator = comparator;
        this.maxRows = maxRows;
    }

    @Override
    public void putRow(DataRow row) {
        if (rows.size() >= maxRows) {
            throw new IllegalStateException(
                    "HeapSortBuffer exceeded the row limit " + maxRows + "; "
                            + " use PlanGraph, which injects spill-capable PointerSortBuffer");
        }
        rows.add(serializer.serialize(row));
    }

    @Override
    public void sort() {
        List<DataRow> decoded = new ArrayList<>(rows.size());
        for (byte[] b : rows) {
            decoded.add(serializer.deserialize(b));
        }
        decoded.sort(comparator); // stable
        rows.clear();
        for (DataRow r : decoded) {
            rows.add(serializer.serialize(r));
        }
    }

    @Override
    public long size() {
        return rows.size();
    }

    @Override
    public Iterator<DataRow> iterator() {
        Iterator<byte[]> it = rows.iterator();
        return new Iterator<DataRow>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public DataRow next() {
                return serializer.deserialize(it.next());
            }
        };
    }
}
