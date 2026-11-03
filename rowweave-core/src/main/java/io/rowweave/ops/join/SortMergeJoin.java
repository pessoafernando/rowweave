package io.rowweave.ops.join;

import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.HeapSortBuffer;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.spill.EqualKeyBlock;
import io.rowweave.mem.SortBuffer;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.Iterator;

/**
 * Streaming sort-merge join (binary breaker, the pure Push contract): both inputs are pulled from a
 * {@link RowStream}, each side is sorted externally on the join key with spilling, and the two sorted
 * streams are merged by lockstep cursors that push each result row to the {@link RowSink} as it appears.
 *
 * <p>Memory holds only the working set the algorithm itself needs: the sort buffers, which spill, and
 * the current equal-key block, which is the set of matching rows for one key and also spills. There is
 * no hash table for an entire build side and no list of output rows, so a large-to-large join does not
 * exhaust the heap.
 *
 * <p>Semantics: a null key never matches, so null does not equal null; unmatched rows are padded
 * according to the join type; an equal-key block yields a cartesian product; the output DataSchema is the
 * left fields followed by the right fields. When the join is done, the sink receives {@code endOfStream}.
 */
public final class SortMergeJoin {

    private final DataSchema leftMeta;
    private final DataSchema rightMeta;
    private final int[] leftKeyIndices;
    private final int[] rightKeyIndices;
    private final DataSchema keyMeta;
    private final JoinType joinType;
    private final DataSchema outputMeta;

    private MemoryManager memoryManager;
    private SpillStore spillStore;
    private io.rowweave.mem.seg.MemorySegmentPool segmentPool;  // when set, sort segments live off-heap first and fall back to disk
    private MemoryManager[] prePartitionedSides;
    private boolean borrowedSpillScope;
    // Row limit for an equal-key block; on the production path the byte budget of the right side's
    // managed memory applies as well. The default is 8192, while PlanGraph injects 1024 explicitly
    // through PushDagExecutor.EQ_BLOCK_THRESHOLD, so this default only guards direct use that bypasses
    // PlanGraph.
    private int equalKeyBlockThreshold = 8192;
    private boolean invoked;

    private SortMergeJoin(DataSchema leftMeta, DataSchema rightMeta, int[] leftKeyIndices, int[] rightKeyIndices,
                          DataSchema keyMeta, JoinType joinType) {
        this.leftMeta = leftMeta;
        this.rightMeta = rightMeta;
        this.leftKeyIndices = leftKeyIndices;
        this.rightKeyIndices = rightKeyIndices;
        this.keyMeta = keyMeta;
        this.joinType = joinType;
        this.outputMeta = buildOutputMeta(leftMeta, rightMeta);
    }

    public static SortMergeJoin of(DataSchema leftMeta, DataSchema rightMeta,
                                   String[] leftKeyFields, String[] rightKeyFields, JoinType joinType) {
        if (leftKeyFields.length != rightKeyFields.length || leftKeyFields.length == 0) {
            throw new IllegalArgumentException("join key field counts must match and be non-empty");
        }
        int[] lIdx = resolve(leftMeta, leftKeyFields);
        int[] rIdx = resolve(rightMeta, rightKeyFields);
        KeySchemaValidator.validate("join", leftMeta, rightMeta, lIdx, rIdx);
        Field[] keyFields = new Field[lIdx.length];
        for (int i = 0; i < lIdx.length; i++) {
            keyFields[i] = new Field("__k" + i, leftMeta.getField(lIdx[i]).getDataType());
        }
        return new SortMergeJoin(leftMeta, rightMeta, lIdx, rIdx, new DataSchema(keyFields), joinType);
    }

    /** The join type, extracted into describable form. */
    public JoinType joinType() {
        return joinType;
    }

    /** Left join key field names (index to name, round-trip stable with the constructor arguments). */
    public java.util.List<String> leftKeyFieldNames() {
        return keyNames(leftMeta, leftKeyIndices);
    }

    /** Right join key field names. */
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

    public DataSchema getOutputSchema() {
        return outputMeta;
    }

    /**
     * Injects the off-heap segment pool and disk spill: the sorting working set uses the segment pool
     * first and falls back to disk once the budget is exceeded. Only the three-argument form exists, so
     * the segment pool always comes from the engine and never silently degrades to the heap.
     */
    public SortMergeJoin withSpill(MemoryManager mm, SpillStore store,
                                   io.rowweave.mem.seg.MemorySegmentPool pool) {
        this.memoryManager = mm;
        this.spillStore = store;
        this.segmentPool = pool;
        return this;
    }

    /** After a HASH/AUTO handoff, reuse the children that are already split and leave the single SpillStore owner held by the caller open. */
    SortMergeJoin withBorrowedSpill(MemoryManager leftMemory, MemoryManager rightMemory,
                                    SpillStore store,
                                    io.rowweave.mem.seg.MemorySegmentPool pool) {
        this.memoryManager = null;
        this.prePartitionedSides = new MemoryManager[]{
                java.util.Objects.requireNonNull(leftMemory, "leftMemory"),
                java.util.Objects.requireNonNull(rightMemory, "rightMemory")};
        this.spillStore = java.util.Objects.requireNonNull(store, "spillStore");
        this.segmentPool = java.util.Objects.requireNonNull(pool, "segmentPool");
        this.borrowedSpillScope = true;
        return this;
    }

    /**
     * Row limit for an equal-key block. During the merge, if the right block exceeds this many rows or no
     * longer fits the managed memory of its side, it spills to disk as a re-readable run and the left
     * block rescans it row by row. Heap residency then drops from the left block plus the right block to
     * the right block within the limit plus one left row. Only takes effect together with
     * {@link #withSpill}.
     */
    public SortMergeJoin withEqualKeyBlockThreshold(int rows) {
        if (rows <= 0) {
            throw new IllegalArgumentException("equalKeyBlockThreshold must be > 0");
        }
        this.equalKeyBlockThreshold = rows;
        return this;
    }

    /**
     * Executes as a stream: pull both inputs, sort externally, merge by lockstep cursors and push each
     * result to {@code out} while working, then send end-of-stream. Memory holds only the sort buffers,
     * which spill, and the current equal-key block.
     */
    public void join(RowStream left, RowStream right, RowSink out) {
        ensureSingleInvocation();
        boolean emitLeftOuter = joinType == JoinType.LEFT || joinType == JoinType.FULL;
        boolean emitRightOuter = joinType == JoinType.RIGHT || joinType == JoinType.FULL;

        // Every row, including rows whose key is null, goes into the ordered buffer: the comparator sorts
        // NULLS_LAST, so null keys gather at the tail, as a database merge join does.
        // The never-match rule for null keys lives in the null-key guard inside mergeJoin rather than in a
        // filter applied while draining: an inner join drops those rows and an outer join pads them.
        MemoryManager[] sides = prePartitionedSides != null
                ? prePartitionedSides
                : (memoryManager == null ? null : memoryManager.partitionEvenly(2));
        SortBuffer leftSorted = newSortBuffer(leftMeta, leftKeyIndices, sides == null ? null : sides[0]);
        SortBuffer rightSorted = newSortBuffer(rightMeta, rightKeyIndices, sides == null ? null : sides[1]);
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

            // lockstep merge: only the current equal-key block lives in memory; see mergeJoin for the null-key guard.
            mergeJoin(leftSorted, rightSorted, out, emitLeftOuter, emitRightOuter,
                    sides == null ? null : sides[1]);
            out.endOfStream();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            if (borrowedSpillScope) {
                io.rowweave.mem.ResourceCloser.close(primary, leftSorted, rightSorted);
            } else {
                BinaryOperatorResources.close(primary, leftSorted, rightSorted, spillStore);
            }
        }
    }

    private void mergeJoin(SortBuffer leftSorted, SortBuffer rightSorted, RowSink out,
                           boolean emitLeftOuter, boolean emitRightOuter,
                           MemoryManager rightBlockMemory) {
        PeekIterator li = new PeekIterator(leftSorted.iterator());
        PeekIterator ri = new PeekIterator(rightSorted.iterator());

        while (li.hasNext() && ri.hasNext()) {
            // Null-key guard: a row whose key is null never matches, even though compareKeys returns 0 for
            // two nulls, so such rows never enter an equal-key block.
            // Under NULLS_LAST the null keys sit at the tail, so once one side peeks a null key every later
            // row on that side is null: each one is handled alone according to the join type, dropped for an
            // inner join and padded for an outer join, and never compared with the other side. Handling null
            // keys before calling compareKeys keeps null out of the cartesian product entirely.
            if (hasNullKey(li.peek(), leftKeyIndices)) {
                if (emitLeftOuter) {
                    out.emitRow(joinRow(li.peek(), null));
                }
                li.next();
                continue;
            }
            if (hasNullKey(ri.peek(), rightKeyIndices)) {
                if (emitRightOuter) {
                    out.emitRow(joinRow(null, ri.peek()));
                }
                ri.next();
                continue;
            }
            int cmp = compareKeys(li.peek(), leftKeyIndices, ri.peek(), rightKeyIndices);
            if (cmp < 0) {
                if (emitLeftOuter) {
                    out.emitRow(joinRow(li.peek(), null));
                }
                li.next();
            } else if (cmp > 0) {
                if (emitRightOuter) {
                    out.emitRow(joinRow(null, ri.peek()));
                }
                ri.next();
            } else {
                // equal-key block on both sides -> full cross product. To bound memory under skew,
                // materialize only the RIGHT block into a re-readable (spillable) container, then stream the
                // LEFT equal-key rows one at a time, re-scanning the right block per left row. Emit order is
                // (left-outer, right-inner), identical to nested-loop -> bit-level equivalent.
                DataRow firstRight = ri.peek();
                EqualKeyBlock rightBlock = new EqualKeyBlock(
                        rightMeta, spillStore, equalKeyBlockThreshold, rightBlockMemory);
                Throwable blockPrimary = null;
                try {
                    rightBlock.add(ri.next());
                    // Growing an equal-key block must skip rows whose key is null: null does not equal null,
                    // and because NULLS_LAST gathers them at the tail, a block ends at the first null anyway.
                    // Testing the key first also matters: compareKeys on a null key would reach compareValue,
                    // which cannot compare null and would throw.
                    while (ri.hasNext() && !hasNullKey(ri.peek(), rightKeyIndices)
                            && compareKeys(firstRight, rightKeyIndices, ri.peek(), rightKeyIndices) == 0) {
                        rightBlock.add(ri.next());
                    }
                    rightBlock.finish();
                    // stream left equal-key rows; for each, re-scan the (possibly spilled) right block
                    DataRow firstLeft = li.peek();
                    do {
                        DataRow l = li.next();
                        Iterator<DataRow> rit = rightBlock.iterator();
                        while (rit.hasNext()) {
                            out.emitRow(joinRow(l, rit.next()));
                        }
                    } while (li.hasNext() && !hasNullKey(li.peek(), leftKeyIndices)
                            && compareKeys(firstLeft, leftKeyIndices, li.peek(), leftKeyIndices) == 0);
                } catch (RuntimeException | Error failure) {
                    blockPrimary = failure;
                    throw failure;
                } finally {
                    io.rowweave.mem.ResourceCloser.close(blockPrimary, rightBlock);
                }
            }
        }
        while (li.hasNext()) {
            if (emitLeftOuter) {
                out.emitRow(joinRow(li.peek(), null));
            }
            li.next();
        }
        while (ri.hasNext()) {
            if (emitRightOuter) {
                out.emitRow(joinRow(null, ri.peek()));
            }
            ri.next();
        }
    }

    private SortBuffer newSortBuffer(DataSchema dataSchema, int[] keyIndices, MemoryManager sideMemory) {
        RowComparator cmp = keyComparator(dataSchema, keyIndices);
        if (sideMemory != null && spillStore != null) {
            // Retained serialization-free resident fast path; once the budget is exceeded, the external path goes through Pointer/SortedRunStore like everything else.
            final DataSchema rm = dataSchema;
            final io.rowweave.ops.RowComparator fc = cmp;
            return new io.rowweave.mem.TwoTierSortBuffer(rm, fc, sideMemory,
                    fc.getSortFields(), 1024, 2,
                    () -> io.rowweave.mem.ExternalSortBuffers.pointer(
                            rm, fc, fc.getSortFields(), segmentPool, sideMemory, 1024, spillStore));
        }
        return new HeapSortBuffer(dataSchema, cmp);
    }

    private synchronized void ensureSingleInvocation() {
        if (invoked) {
            throw new IllegalStateException("SortMergeJoin instances are single-use");
        }
        invoked = true;
    }

    private RowComparator keyComparator(DataSchema dataSchema, int[] keyIndices) {
        SortField[] sf = new SortField[keyIndices.length];
        for (int i = 0; i < keyIndices.length; i++) {
            sf[i] = new SortField(keyIndices[i], SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        }
        return new RowComparator(dataSchema, sf);
    }

    /** Compare join keys across the two sides. Null keys are routed elsewhere, so only non-null keys reach this. */
    private int compareKeys(DataRow a, int[] aIdx, DataRow b, int[] bIdx) {
        for (int i = 0; i < aIdx.length; i++) {
            int c = compareValue(a.get(aIdx[i]), b.get(bIdx[i]), keyMeta.getField(i).getDataType());
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareValue(Object av, Object bv, io.rowweave.DataType type) {
        if (isNumeric(type)) {
            return toBigDecimal(av).compareTo(toBigDecimal(bv));
        }
        return ((Comparable) av).compareTo(bv);
    }

    private boolean isNumeric(io.rowweave.DataType type) {
        return type == io.rowweave.DataType.IntegerType
                || type == io.rowweave.DataType.LongType
                || type == io.rowweave.DataType.BigDecimalType;
    }

    private java.math.BigDecimal toBigDecimal(Object v) {
        if (v instanceof java.math.BigDecimal) {
            return (java.math.BigDecimal) v;
        }
        if (v instanceof Integer || v instanceof Long) {
            return java.math.BigDecimal.valueOf(((Number) v).longValue());
        }
        return new java.math.BigDecimal(v.toString());
    }

    private boolean hasNullKey(DataRow row, int[] keyIndices) {
        for (int idx : keyIndices) {
            if (row.get(idx) == null) {
                return true;
            }
        }
        return false;
    }

    private DataRow joinRow(DataRow lrow, DataRow rrow) {
        int ln = leftMeta.getFieldCount();
        int rn = rightMeta.getFieldCount();
        DataRow out = new DataRow(ln + rn);
        if (lrow != null) {
            for (int i = 0; i < ln; i++) {
                out.set(i, lrow.get(i));
            }
        }
        if (rrow != null) {
            for (int i = 0; i < rn; i++) {
                out.set(ln + i, rrow.get(i));
            }
        }
        return out;
    }

    private static int[] resolve(DataSchema meta, String[] fields) {
        int[] idx = new int[fields.length];
        for (int i = 0; i < fields.length; i++) {
            int found = meta.getFieldIndex(fields[i]);
            if (found < 0) {
                throw new IllegalArgumentException("Unknown join key field: " + fields[i]);
            }
            idx[i] = found;
        }
        return idx;
    }

    private static DataSchema buildOutputMeta(DataSchema leftMeta, DataSchema rightMeta) {
        Field[] fields = new Field[leftMeta.getFieldCount() + rightMeta.getFieldCount()];
        int c = 0;
        for (int i = 0; i < leftMeta.getFieldCount(); i++) {
            fields[c++] = leftMeta.getField(i);
        }
        for (int i = 0; i < rightMeta.getFieldCount(); i++) {
            fields[c++] = rightMeta.getField(i);
        }
        return new DataSchema(fields);
    }

    /** One-step lookahead iterator used internally over SortBuffer.iterator; not exposed. */
    private static final class PeekIterator {
        private final Iterator<DataRow> it;
        private DataRow head;
        private boolean has;

        PeekIterator(Iterator<DataRow> it) {
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
