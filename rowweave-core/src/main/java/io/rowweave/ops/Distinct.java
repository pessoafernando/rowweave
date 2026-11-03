package io.rowweave.ops;

import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.GroupAggBuffer;
import io.rowweave.mem.HeapGroupAggBuffer;
import io.rowweave.mem.ResourceCloser;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The dedup operator (a breaker, pure push). It deduplicates by all columns or by the columns given, built on {@link GroupAggBuffer} (a binary key), and
 * a segmented or spilling buffer implementation can be injected through {@link #withBufferFactory} with no change to the operator.
 *
 * <p>Semantics: equal keys are duplicates, and null is one more value that can be equal - this follows grouping, and differs from join, where null never equals null.
 * BigDecimal scale is part of the key (1.0 and 1.00 are distinct). Each key always keeps its first-seen representative row, and the output order across groups
 * is first-seen order on the heap/no-spill path, while the spill path only guarantees determinism, so anything that depends on order must state an explicit {@code orderBy}.
 * The representative row lives in a {@link RowHolderAccumulator} (spillable), so once a disk-spill buffer is injected, a high-cardinality dedup does not OOM.
 *
 * <p><b>The pure Push contract</b>: the only outlet is {@code dedup(RowStream, RowSink)}, and one GroupAggBuffer deduplicates directly
 * (no parallel partitioning). There is no apply or applyShuffled outlet returning a List.
 */
public final class Distinct {

    private final DataSchema dataSchema;
    private final int[] keyIndices;
    private final DataSchema keyMeta;
    private final boolean allColumns; // distinguishes allColumns from byColumns (for the describable round trip)
    private Function<Supplier<Object[]>, GroupAggBuffer> bufferFactory = HeapGroupAggBuffer::new;

    private Distinct(DataSchema dataSchema, int[] keyIndices, boolean allColumns) {
        this.dataSchema = dataSchema;
        this.keyIndices = keyIndices;
        this.allColumns = allColumns;
        Field[] keyFieldDefs = new Field[keyIndices.length];
        for (int i = 0; i < keyIndices.length; i++) {
            keyFieldDefs[i] = dataSchema.getField(keyIndices[i]);
        }
        this.keyMeta = new DataSchema(keyFieldDefs);
    }

    /** Injects the GroupAggBuffer factory (on-heap by default; a segmented plus disk-spill implementation keeps a high-cardinality dedup from OOMing). */
    public Distinct withBufferFactory(Function<Supplier<Object[]>, GroupAggBuffer> bufferFactory) {
        this.bufferFactory = bufferFactory;
        return this;
    }

    public static Distinct allColumns(DataSchema dataSchema) {
        int[] idx = new int[dataSchema.getFieldCount()];
        for (int i = 0; i < idx.length; i++) {
            idx[i] = i;
        }
        return new Distinct(dataSchema, idx, true);
    }

    /** Whether all columns form the dedup key (describable: allColumns means empty keys, byColumns means the keys given). */
    public boolean isAllColumns() {
        return allColumns;
    }

    /** The dedup key field names (the columns given to byColumns, or every column name under allColumns); the index-to-name round trip is consistent. */
    public List<String> keyFieldNames() {
        List<String> names = new ArrayList<>(keyIndices.length);
        for (int idx : keyIndices) {
            names.add(dataSchema.getField(idx).getName());
        }
        return names;
    }

    public static Distinct byColumns(DataSchema dataSchema, String... keyFields) {
        if (keyFields.length == 0) {
            throw new IllegalArgumentException("Distinct requires at least one key column");
        }
        int[] idx = new int[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            int found = dataSchema.getFieldIndex(keyFields[i]);
            if (found < 0) {
                throw new IllegalArgumentException("Unknown key field: " + keyFields[i]);
            }
            idx[i] = found;
        }
        return new Distinct(dataSchema, idx, false);
    }

    /**
     * The only outlet (the pure Push contract): the breaker pulls upstream until null (finishing it), deduplicates by key into one
     * GroupAggBuffer (the representative row lives in a RowHolderAccumulator, and the buffer handles spill), then emits each group's first-seen row with emitRow and closes with endOfStream.
     * Control events: Error and Cancel pass through and then terminate; a Barrier is absorbed.
     */
    public void dedup(RowStream upstream, RowSink out) {
        RowSerializer keySerializer = new RowSerializer(keyMeta);
        boolean directProjectedKeys = KeyProjectionMode.enabled();
        Supplier<Object[]> accFactory = () -> new Object[]{new RowHolderAccumulator(dataSchema)};
        GroupAggBuffer buf = bufferFactory.apply(accFactory);
        Throwable primary = null;
        try {
            StreamEvent e;
            while ((e = upstream.next()) != null) {
                if (!e.isRecord()) {
                    if (e.isError() || e.isCancel()) {
                        out.emit(e);
                        return;
                    }
                    continue; // Barrier: absorbed
                }
                DataRow r = e.row();
                byte[] keyBytes = directProjectedKeys
                        ? keySerializer.serializeProjected(r, keyIndices)
                        : keySerializer.serialize(keyRowOf(r));
                ((RowHolderAccumulator) buf.getOrCreate(keyBytes)[0]).setIfEmpty(r); // first-seen wins
            }

            for (GroupAggBuffer.Group g : buf) {
                out.emitRow(((RowHolderAccumulator) g.accumulators()[0]).get());
            }
            out.endOfStream();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            ResourceCloser.close(primary, buf);
        }
    }

    private DataRow keyRowOf(DataRow row) {
        DataRow keyRow = new DataRow(keyIndices.length);
        for (int i = 0; i < keyIndices.length; i++) {
            keyRow.set(i, row.get(keyIndices[i]));
        }
        return keyRow;
    }
}
