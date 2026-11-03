package io.rowweave.ops.join;

import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.ExternalSortBuffers;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.SortBuffer;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.EqualKeyBlock;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.ops.GroupKeyComparator;
import io.rowweave.ops.SortField;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.Iterator;

/**
 * The coGroup operator (binary breaker, pure Push): each input is grouped by key, and for every key in
 * the union of both key sets the left group and the right group are handed to the user
 * {@link CoGroupFunction}. When one side has no entry for a key, an empty group is passed and the
 * function still fires, which is what gives coGroup its full outer behaviour.
 *
 * <p><b>null participates in grouping</b>: a null key on both sides forms one group. This is the
 * opposite of join, where null never matches null. Grouping is <b>scale sensitive</b>, so 5 and 5.0 fall
 * in different groups, matching the byte key encoding the engine uses for BigDecimal. Group order
 * follows key order with NULLS_LAST.
 *
 * <p>Both sides drain into a spillable SortBuffer, sort, and are then merged by two cursors that walk
 * equal-key blocks; a block spills to disk once either its row limit or the managed-memory limit of its
 * side is reached, and stays re-readable. Each block goes to fn.coGroup and every produced row is
 * emitted. The merge is always sort-merge, so the working set is the two SortBuffers plus the current
 * equal-key block, and a large input cannot exhaust the heap.
 *
 * <p><b>Invariant</b>: the RowComparator used by SortBuffer and the merge comparison
 * {@link #compareCrossKey} share one definition, both NULLS_LAST and with the same scale ordering. If
 * they diverge, sorted key order and merge key order disagree and null and scale alignment breaks.
 */
public final class CoGroup {

    private static final int GROUP_BLOCK_THRESHOLD = 1 << 20;
    private static final int SORT_RUN_THRESHOLD = 1024;
    private static final long DEFAULT_BUDGET = 64L * 1024 * 1024;

    private final DataSchema leftMeta;
    private final DataSchema rightMeta;
    private final int[] leftKeyIndices;
    private final int[] rightKeyIndices;
    private final CoGroupFunction fn;

    private MemoryManager memoryManager = new MemoryManager(DEFAULT_BUDGET);
    private SpillStore spillStore = new MemorySpillStore();
    private MemorySegmentPool segmentPool = new HeapPool();
    private int equalKeyBlockThreshold = GROUP_BLOCK_THRESHOLD;
    private boolean invoked;

    private CoGroup(DataSchema leftMeta, DataSchema rightMeta,
                    int[] leftKeyIndices, int[] rightKeyIndices, CoGroupFunction fn) {
        this.leftMeta = leftMeta;
        this.rightMeta = rightMeta;
        this.leftKeyIndices = leftKeyIndices;
        this.rightKeyIndices = rightKeyIndices;
        this.fn = fn;
    }

    public static CoGroup of(DataSchema leftMeta, DataSchema rightMeta,
                             String[] leftKeyFields, String[] rightKeyFields, CoGroupFunction fn) {
        if (leftKeyFields.length != rightKeyFields.length || leftKeyFields.length == 0) {
            throw new IllegalArgumentException("coGroup key field counts must match and be non-empty");
        }
        return new CoGroup(leftMeta, rightMeta,
                resolve(leftMeta, leftKeyFields), resolve(rightMeta, rightKeyFields), fn);
    }

    /**
     * Overrides the default spill wiring: both external sorts write to the segment pool and to disk, and
     * an equal-key block spills when either its row limit or the managed-memory limit of its side is
     * reached. Only the three-argument form exists, so the segment pool always comes from one place.
     */
    public CoGroup withSpill(MemoryManager mm, SpillStore store, MemorySegmentPool pool) {
        this.memoryManager = mm;
        this.spillStore = store;
        this.segmentPool = pool;
        return this;
    }

    public CoGroup withEqualKeyBlockThreshold(int rows) {
        if (rows <= 0) {
            throw new IllegalArgumentException("equalKeyBlockThreshold must be > 0");
        }
        this.equalKeyBlockThreshold = rows;
        return this;
    }

    /** Left and right join key field names (index to name, round-trip stable), read by DataSet to build describable parameters. */
    public java.util.List<String> leftKeyFieldNames() {
        return keyNames(leftMeta, leftKeyIndices);
    }

    public java.util.List<String> rightKeyFieldNames() {
        return keyNames(rightMeta, rightKeyIndices);
    }

    private static java.util.List<String> keyNames(DataSchema meta, int[] idx) {
        java.util.List<String> names = new java.util.ArrayList<>(idx.length);
        for (int i : idx) {
            names.add(meta.getField(i).getName());
        }
        return names;
    }

    /** The user function; Serializable so that describable output can carry it as userFunction. */
    public CoGroupFunction function() {
        return fn;
    }

    /** Output DataSchema for DataSet integration: delegates to the user function's getResultSchema(). */
    public DataSchema getResultSchema() {
        return fn.getResultSchema();
    }

    /**
     * The only outlet (the pure Push contract): drain both sides into a SortBuffer that is NULLS_LAST and
     * spillable, sort, merge the two sorted streams by equal key, call
     * fn.coGroup(leftBlock, rightBlock, collector) and emit every row it collects, then close with
     * endOfStream. Control events: an Error or Cancel on either side passes through and ends the operator.
     */
    public void coGroup(RowStream left, RowStream right, RowSink out) {
        ensureSingleInvocation();
        MemoryManager[] sides = memoryManager.partitionEvenly(2);
        SortBuffer leftSorted = newSortBuffer(leftMeta, leftKeyIndices, sides[0]);
        SortBuffer rightSorted = newSortBuffer(rightMeta, rightKeyIndices, sides[1]);
        Throwable primary = null;
        try {
            StreamEvent terminal = ConcurrentBinaryDrain.drain(left, leftSorted, right, rightSorted);
            if (terminal != null) {
                BinaryTerminalEmitter.emit(out, terminal);
                return;
            }
            // CPU phase: the slot semaphore covers both sorts and never touches downstream; the merge emits outside it and holds no permit.
            io.rowweave.mem.SlotGuard.cpuPhase(() -> {
                leftSorted.sort();
                rightSorted.sort();
                return null;
            });

            Collector collector = out::emitRow;
            PeekCursor li = new PeekCursor(leftSorted.iterator());
            PeekCursor ri = new PeekCursor(rightSorted.iterator());

            while (li.hasNext() || ri.hasNext()) {
                int cmp;
                if (!li.hasNext()) {
                    cmp = 1; // only right remains
                } else if (!ri.hasNext()) {
                    cmp = -1; // only left remains
                } else {
                    cmp = compareCrossKey(li.peek(), ri.peek());
                }

                EqualKeyBlock leftBlock = new EqualKeyBlock(
                        leftMeta, spillStore, equalKeyBlockThreshold, sides[0]);
                EqualKeyBlock rightBlock = new EqualKeyBlock(
                        rightMeta, spillStore, equalKeyBlockThreshold, sides[1]);
                Throwable blockPrimary = null;
                try {
                    if (cmp < 0) {
                        DataRow firstLeft = li.peek();
                        do {
                            leftBlock.add(li.next());
                        } while (li.hasNext()
                                && sameKey(firstLeft, leftKeyIndices, li.peek(), leftKeyIndices, leftMeta));
                    } else if (cmp > 0) {
                        DataRow firstRight = ri.peek();
                        do {
                            rightBlock.add(ri.next());
                        } while (ri.hasNext()
                                && sameKey(firstRight, rightKeyIndices, ri.peek(), rightKeyIndices, rightMeta));
                    } else {
                        DataRow firstLeft = li.peek();
                        DataRow firstRight = ri.peek();
                        do {
                            leftBlock.add(li.next());
                        } while (li.hasNext()
                                && sameKey(firstLeft, leftKeyIndices, li.peek(), leftKeyIndices, leftMeta));
                        do {
                            rightBlock.add(ri.next());
                        } while (ri.hasNext()
                                && sameKey(firstRight, rightKeyIndices, ri.peek(), rightKeyIndices, rightMeta));
                    }
                    leftBlock.finish();
                    rightBlock.finish();
                    fn.coGroup(leftBlock::iterator, rightBlock::iterator, collector);
                } catch (RuntimeException | Error failure) {
                    blockPrimary = failure;
                    throw failure;
                } finally {
                    io.rowweave.mem.ResourceCloser.close(blockPrimary, leftBlock, rightBlock);
                }
            }
            out.endOfStream();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            BinaryOperatorResources.close(primary, leftSorted, rightSorted, spillStore);
        }
    }

    /** Sort comparator: one definition with {@link #compareCrossKey}, per key field ASC NULLS_LAST plus the scale ordering. */
    private SortBuffer newSortBuffer(DataSchema dataSchema, int[] keyIndices, MemoryManager sideMemory) {
        SortField[] sf = new SortField[keyIndices.length];
        for (int i = 0; i < keyIndices.length; i++) {
            sf[i] = new SortField(keyIndices[i], SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        }
        GroupKeyComparator cmp = new GroupKeyComparator(dataSchema, keyIndices);
        return ExternalSortBuffers.pointer(dataSchema, cmp, sf, segmentPool, sideMemory,
                SORT_RUN_THRESHOLD, spillStore);
    }

    private synchronized void ensureSingleInvocation() {
        if (invoked) {
            throw new IllegalStateException("CoGroup instances are single-use");
        }
        invoked = true;
    }

    /** Compare a key across the two sides: null is the largest value, which agrees with NULLS_LAST, null against null returns 0 because coGroup groups on null, and numbers compare scale-sensitively. */
    private int compareCrossKey(DataRow leftRow, DataRow rightRow) {
        return GroupKeyComparator.compareRows(leftRow, leftMeta, leftKeyIndices,
                rightRow, rightMeta, rightKeyIndices);
    }

    /**
     * Whether two keys taken from the same side are equal, null against null included; the ordering
     * matches compareKeyValue, so the result stays scale sensitive. {@code meta} is the DataType of the
     * side being read, which keeps the comparison scale sensitive when the two key columns differ in type.
     */
    private boolean sameKey(DataRow a, int[] aIdx, DataRow b, int[] bIdx, DataSchema meta) {
        return GroupKeyComparator.compareRows(a, meta, aIdx, b, meta, bIdx) == 0;
    }

    private static int[] resolve(DataSchema meta, String[] fields) {
        int[] idx = new int[fields.length];
        for (int i = 0; i < fields.length; i++) {
            int found = meta.getFieldIndex(fields[i]);
            if (found < 0) {
                throw new IllegalArgumentException("Unknown coGroup key field: " + fields[i]);
            }
            idx[i] = found;
        }
        return idx;
    }

    /** One-step lookahead cursor over SortBuffer.iterator. */
    private static final class PeekCursor {
        private final Iterator<DataRow> it;
        private DataRow head;
        private boolean has;

        PeekCursor(Iterator<DataRow> it) {
            this.it = it;
            advance();
        }

        private void advance() {
            if (it.hasNext()) {
                head = it.next();
                has = true;
            } else {
                head = null;
                has = false;
            }
        }

        boolean hasNext() {
            return has;
        }

        DataRow peek() {
            return head;
        }

        DataRow next() {
            DataRow cur = head;
            advance();
            return cur;
        }
    }
}
