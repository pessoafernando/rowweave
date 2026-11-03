package io.rowweave.ops;

import io.rowweave.Collector;
import io.rowweave.GroupReduceFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.ExternalSortBuffers;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.ResourceCloser;
import io.rowweave.mem.SortBuffer;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.EqualKeyBlock;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.Iterator;

/**
 * Group reduce operator (a breaker, pure push). Rows are grouped by key (null participates in grouping and forms its own group), and the row set of each group is
 * handed to the user's {@link GroupReduceFunction}. The reduce may emit 0..N rows.
 *
 * <p><b>The pure Push contract</b>: the only outlet is {@code reduce(RowStream, RowSink)}, and the operator <b>always takes the sorted path</b>,
 * which removes the full on-heap materialization {@code Map<GroupKey,List<DataRow>>}.
 *
 * <p><b>Group order = key sort order</b> (NULLS_LAST), and within a group the order is the input order (a PointerSortBuffer stable sort on the global seq).
 * First-seen order is not promised: avoiding OOM outranks matching the arrival order, so no non-spillable index is introduced to preserve it. A downstream that needs a particular order states an explicit orderBy.
 *
 * <p>Memory: sorting uses the shared Pointer/SortedRunStore core (an off-heap segment pool plus disk as the floor), and one group is fed to the function through a re-readable {@link EqualKeyBlock}
 * that spills as soon as either the group's row count or its byte budget is met. Memory holds only the sort work set plus the current group block, so a large group or high cardinality does not OOM. A whole-table reduce (no key) is the single-group path.
 */
public final class ReduceGroup {

    private static final int GROUP_BLOCK_THRESHOLD = 1024; // A group spilling to disk beyond this many rows, which keeps a large group from OOMing
    private static final int SORT_RUN_THRESHOLD = 1024;
    private static final long DEFAULT_BUDGET = 64L * 1024 * 1024; // the default managed budget of 64MB

    private final DataSchema dataSchema;
    private final int[] keyIndices;
    private final GroupReduceFunction fn;
    private MemoryManager memoryManager = new MemoryManager(DEFAULT_BUDGET);
    private SpillStore spillStore = new MemorySpillStore();
    private MemorySegmentPool segmentPool = new HeapPool();
    private boolean invoked;

    private ReduceGroup(DataSchema dataSchema, int[] keyIndices, GroupReduceFunction fn) {
        this.dataSchema = dataSchema;
        this.keyIndices = keyIndices;
        this.fn = fn;
    }

    public static ReduceGroup of(DataSchema dataSchema, String keyField, GroupReduceFunction fn) {
        return of(dataSchema, new String[]{keyField}, fn);
    }

    public static ReduceGroup of(DataSchema dataSchema, String[] keyFields, GroupReduceFunction fn) {
        int[] idx = new int[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            int found = dataSchema.getFieldIndex(keyFields[i]);
            if (found < 0) {
                throw new IllegalArgumentException("Unknown key field: " + keyFields[i]);
            }
            idx[i] = found;
        }
        return new ReduceGroup(dataSchema, idx, fn);
    }

    /** The grouping key field names (index to name, round-tripping with the constructor arguments), used by DataSet to extract describable params. */
    public java.util.List<String> keyFieldNames() {
        java.util.List<String> names = new java.util.ArrayList<>(keyIndices.length);
        for (int idx : keyIndices) {
            names.add(dataSchema.getField(idx).getName());
        }
        return names;
    }

    /** The GroupReduceFunction (Serializable, carried as the describable userFunction). */
    public GroupReduceFunction function() {
        return fn;
    }

    /** Injects the spill floor (overriding the default): sorting lands in the off-heap segment pool plus disk, and each group's EqualKeyBlock stays re-readable when fed to the function. */
    public ReduceGroup withSpill(MemoryManager mm, SpillStore store, MemorySegmentPool pool) {
        this.memoryManager = mm;
        this.spillStore = store;
        this.segmentPool = pool;
        return this;
    }

    /**
     * The only outlet (the pure Push contract): the breaker pulls upstream until null (finishing it), sorts by key (ASC NULLS_LAST per field),
     * then scans the equal-key blocks in order and feeds each to the user's function, emitting rows one by one and closing with endOfStream. Group order = key order, and within a group the input order (a stable sort).
     * Control events: Error and Cancel pass through and then terminate; a Barrier is absorbed.
     */
    public void reduce(RowStream upstream, RowSink out) {
        ensureSingleInvocation();
        // Convenience overload with no key (a whole-table reduce): every row is one group, and nothing is sorted (there is no sort key; the source order is kept and the function is called once).
        if (keyIndices.length == 0) {
            reduceWholeTable(upstream, out);
            return;
        }
        // The sort key is every key field (ASC NULLS_LAST per field): equal keys become contiguous, and null keys land at the end and form one group.
        SortField[] sf = new SortField[keyIndices.length];
        for (int i = 0; i < keyIndices.length; i++) {
            sf[i] = new SortField(keyIndices[i], SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        }
        GroupKeyComparator cmp = new GroupKeyComparator(dataSchema, keyIndices);
        SortBuffer sortBuf = ExternalSortBuffers.pointerForGroupedReplay(dataSchema, cmp, sf, segmentPool,
                memoryManager, SORT_RUN_THRESHOLD, spillStore);
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
                sortBuf.putRow(e.row());
            }
            io.rowweave.mem.SlotGuard.cpuPhase(() -> { sortBuf.sort(); return null; });

            Collector collector = out::emitRow;
            Iterator<DataRow> it = sortBuf.iterator();
            DataRow pending = it.hasNext() ? it.next() : null;
            while (pending != null) {
                DataRow first = pending;
                EqualKeyBlock block = new EqualKeyBlock(
                        dataSchema, spillStore, GROUP_BLOCK_THRESHOLD, memoryManager);
                Throwable blockPrimary = null;
                try {
                    block.add(pending);
                    pending = it.hasNext() ? it.next() : null;
                    while (pending != null && cmp.sameKey(first, pending)) {
                        block.add(pending);
                        pending = it.hasNext() ? it.next() : null;
                    }
                    block.finish();
                    fn.reduce(block::iterator, collector);
                } catch (RuntimeException | Error failure) {
                    blockPrimary = failure;
                    throw failure;
                } finally {
                    ResourceCloser.close(blockPrimary, block);
                }
            }
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            ResourceCloser.close(primary, sortBuf, spillStore);
        }
        out.endOfStream();
    }

    /** Whole-table reduce (no key): every row goes into one spill-backed block in source order and is handed to the function once (an empty table calls the function once with an empty iterable). */
    private void reduceWholeTable(RowStream upstream, RowSink out) {
        EqualKeyBlock block = new EqualKeyBlock(
                dataSchema, spillStore, GROUP_BLOCK_THRESHOLD, memoryManager);
        Throwable primary = null;
        try {
            StreamEvent e;
            while ((e = upstream.next()) != null) {
                if (!e.isRecord()) {
                    if (e.isError() || e.isCancel()) {
                        out.emit(e);
                        return;
                    }
                    continue;
                }
                block.add(e.row());
            }
            block.finish();
            fn.reduce(block::iterator, out::emitRow);
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            ResourceCloser.close(primary, block, spillStore);
        }
        out.endOfStream();
    }

    private synchronized void ensureSingleInvocation() {
        if (invoked) {
            throw new IllegalStateException("ReduceGroup instances are single-use");
        }
        invoked = true;
    }

}
