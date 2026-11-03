package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.EscrowedExternalSortBuffer;
import io.rowweave.mem.ExternalSortEscrow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.ResourceCloser;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CancellationException;
import java.util.function.BiFunction;

/**
 * The Top-N operator (a sorting breaker, pure push): it takes the first N rows in {@link RowComparator} order.
 *
 * <p>The production path uses one bounded managed heap. When the budget cannot hold it, the current Top-N prefix is migrated exactly into an external sort buffer, later input may spill,
 * and only the first N rows are read at the end. A direct call with no managed memory injected still takes the single-heap compatibility path.
 *
 * <p>A global __seq ASC tie-breaker is appended to the comparator, so equal keys stay stable and independent of the parallelism passed in. The Top stage itself is
 * single-threaded today, and parallelism is kept only for API and plan compatibility. Null sort semantics are pinned by {@link RowComparator}.
 */
public final class Top {

    private final DataSchema dataSchema;
    private final int n;
    private final SortField[] sortFields;
    private MemoryManager memoryManager;
    private BiFunction<DataSchema, RowComparator, ? extends EscrowedExternalSortBuffer> fallbackFactory;

    private Top(DataSchema dataSchema, int n, SortField[] sortFields) {
        this.dataSchema = dataSchema;
        this.n = n;
        this.sortFields = sortFields;
    }

    public static Top of(DataSchema dataSchema, int n, SortField... sortFields) {
        if (n < 0) {
            throw new IllegalArgumentException("n must be >= 0");
        }
        if (sortFields.length == 0) {
            throw new IllegalArgumentException("Top requires at least one SortField");
        }
        return new Top(dataSchema, n, sortFields.clone());
    }

    /** How many rows to take (for describable extraction). */
    public int n() {
        return n;
    }

    /** The sort fields (round-tripping with the constructor arguments). */
    public SortField[] sortFields() {
        return sortFields.clone();
    }

    /** For production execution, injects the exclusive operator budget and the lazy external sort factory. */
    public Top withSpill(MemoryManager memoryManager,
                         BiFunction<DataSchema, RowComparator, ? extends EscrowedExternalSortBuffer> fallbackFactory) {
        if (memoryManager == null || fallbackFactory == null) {
            throw new IllegalArgumentException("memoryManager and fallbackFactory are required");
        }
        this.memoryManager = memoryManager;
        this.fallbackFactory = fallbackFactory;
        return this;
    }

    /**
     * The breaker finishes upstream first: each row is augmented with __seq and fed to one bounded heap, and the external sort takes over only when the managed budget runs out. Rows are
     * emitted in ascending order of the augmented total order, one by one with emitRow, and EOS closes the stream. Error and Cancel pass through and terminate; a Barrier is absorbed.
     */
    public void applyStream(RowStream upstream, int parallelism, RowSink out) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0");
        }
        int seqIndex = dataSchema.getFieldCount();
        DataSchema augMeta = augmentedMeta(seqIndex);
        RowComparator augCmp = augmentedComparator(seqIndex);
        CandidateHeap candidates = new CandidateHeap(n, augMeta, augCmp, memoryManager);
        EscrowedExternalSortBuffer fallback = null;
        Throwable primary = null;
        try {
            long seq = 0;
            StreamEvent event;
            while ((event = upstream.next()) != null) {
                if (!event.isRecord()) {
                    if (event.isError() || event.isCancel()) {
                        if (fallback != null) {
                            fallback.cancel(event.isError() ? event.cause()
                                    : new CancellationException("Top cancelled"));
                        }
                        out.emit(event);
                        return;
                    }
                    continue;
                }
                if (n == 0) {
                    seq++;
                    continue;
                }
                DataRow augmented = augment(event.row(), seqIndex, seq);
                if (fallback != null) {
                    fallback.putRow(augmented);
                } else if (!candidates.offer(augmented)) {
                    fallback = switchToFallback(candidates, augMeta, augCmp, augmented);
                }
                seq++;
            }

            if (fallback == null) {
                for (Candidate candidate : candidates.sortedSnapshot()) {
                    out.emitRow(strip(candidate.row, seqIndex));
                }
            } else {
                fallback.sort();
                Iterator<DataRow> sorted = fallback.iterator();
                int emitted = 0;
                while (emitted < n && sorted.hasNext()) {
                    out.emitRow(strip(sorted.next(), seqIndex));
                    emitted++;
                }
            }
            out.endOfStream();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            if (fallback != null) {
                fallback.cancel(failure);
            }
            throw failure;
        } finally {
            ResourceCloser.close(primary, candidates, fallback);
        }
    }

    private EscrowedExternalSortBuffer switchToFallback(CandidateHeap candidates, DataSchema augMeta,
                                                        RowComparator augCmp, DataRow trigger) {
        if (fallbackFactory == null || memoryManager == null) {
            throw new IllegalStateException("Top exceeded unmanaged compatibility heap; inject spill support");
        }
        EscrowedExternalSortBuffer buffer = fallbackFactory.apply(augMeta, augCmp);
        try {
            candidates.transferTo(buffer);
            buffer.putRow(trigger);
            memoryManager.recordSelfSpill();
            return buffer;
        } catch (RuntimeException | Error failure) {
            ResourceCloser.close(failure, buffer);
            throw failure;
        }
    }

    private RowComparator augmentedComparator(int seqIndex) {
        SortField[] augmented = new SortField[sortFields.length + 1];
        System.arraycopy(sortFields, 0, augmented, 0, sortFields.length);
        augmented[sortFields.length] = new SortField(
                seqIndex, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        return new RowComparator(augmentedMeta(seqIndex), augmented);
    }

    private DataSchema augmentedMeta(int seqIndex) {
        Field[] fields = new Field[seqIndex + 1];
        for (int i = 0; i < seqIndex; i++) {
            fields[i] = dataSchema.getField(i);
        }
        fields[seqIndex] = new Field("__seq", DataType.LongType);
        return new DataSchema(fields);
    }

    private DataRow augment(DataRow row, int seqIndex, long seq) {
        if (row.size() != seqIndex) {
            throw new IllegalStateException("Top input row width " + row.size()
                    + " != DataSchema field count " + seqIndex);
        }
        DataRow augmented = new DataRow(seqIndex + 1);
        for (int i = 0; i < seqIndex; i++) {
            augmented.set(i, row.get(i));
        }
        augmented.set(seqIndex, seq);
        return augmented;
    }

    private DataRow strip(DataRow augmented, int seqIndex) {
        DataRow row = new DataRow(seqIndex);
        for (int i = 0; i < seqIndex; i++) {
            row.set(i, augmented.get(i));
        }
        return row;
    }

    /** A single max-heap. Its credit shares the candidate's lifetime and conservatively covers both the candidate wrapper and the snapshot reference. */
    private static final class CandidateHeap implements AutoCloseable {
        private static final long CONTAINER_CREDIT = 128L;

        private final int limit;
        private final Comparator<DataRow> comparator;
        private final MemoryManager memoryManager;
        private final ExternalSortEscrow escrow;
        private final PriorityQueue<Candidate> heap;
        private long reserved;
        private long comparatorEscrow;
        private long finalHeadEscrow;
        private boolean batchCreditReserved;
        private boolean closed;

        CandidateHeap(int limit, DataSchema dataSchema, Comparator<DataRow> comparator, MemoryManager memoryManager) {
            this.limit = limit;
            this.comparator = comparator;
            this.memoryManager = memoryManager;
            this.escrow = memoryManager == null ? null : new ExternalSortEscrow(dataSchema);
            this.heap = new PriorityQueue<>((left, right) -> comparator.compare(right.row, left.row));
        }

        /** true means already handled (kept or evicted); false means the external sort must take over and the current row has not yet been claimed. */
        boolean offer(DataRow row) {
            ensureOpen();
            if (limit == 0) {
                return true;
            }
            if (heap.size() >= limit && comparator.compare(row, heap.peek().row) >= 0) {
                return true;
            }
            ExternalSortEscrow.Credit escrowCredit = memoryManager == null ? null : escrow.credit(row);
            long rowCredit = escrowCredit == null ? 0L : escrowCredit.rowCredit();
            long retainedCredit = Math.addExact(rowCredit, memoryManager == null ? 0L : CONTAINER_CREDIT);
            Candidate worst = heap.size() < limit ? null : heap.peek();
            long rowDelta = worst == null ? retainedCredit
                    : Math.max(0L, retainedCredit - worst.retainedCredit());
            long comparatorDelta = escrowCredit == null ? 0L
                    : Math.max(0L, escrowCredit.comparatorTarget() - comparatorEscrow);
            long headDelta = escrowCredit == null ? 0L
                    : Math.max(0L, escrowCredit.finalHeadTarget() - finalHeadEscrow);
            long batchDelta = memoryManager != null && !batchCreditReserved
                    ? ExternalSortEscrow.batchFixedCredit() : 0L;
            long additional = Math.addExact(rowDelta,
                    Math.addExact(batchDelta, Math.addExact(comparatorDelta, headDelta)));
            if (memoryManager != null && !memoryManager.tryReserve(additional)) {
                return false;
            }
            boolean published = false;
            try {
                if (worst != null) {
                    heap.poll();
                }
                heap.add(new Candidate(row, escrowCredit));
                if (worst == null) {
                    reserved = Math.addExact(reserved, retainedCredit);
                } else {
                    reserved = Math.addExact(Math.subtractExact(reserved, worst.retainedCredit()), retainedCredit);
                    if (memoryManager != null && worst.retainedCredit() > retainedCredit) {
                        memoryManager.releaseExact(worst.retainedCredit() - retainedCredit);
                    }
                }
                if (memoryManager != null) {
                    reserved = Math.addExact(reserved,
                            Math.addExact(batchDelta, Math.addExact(comparatorDelta, headDelta)));
                    batchCreditReserved = true;
                    comparatorEscrow = Math.addExact(comparatorEscrow, comparatorDelta);
                    finalHeadEscrow = Math.addExact(finalHeadEscrow, headDelta);
                }
                published = true;
                return true;
            } finally {
                if (!published && memoryManager != null) {
                    memoryManager.releaseExact(additional);
                }
            }
        }

        List<Candidate> sortedSnapshot() {
            ensureOpen();
            List<Candidate> sorted = new ArrayList<>(heap);
            sorted.sort((left, right) -> comparator.compare(left.row, right.row));
            heap.clear();
            return sorted;
        }

        void transferTo(EscrowedExternalSortBuffer target) {
            ensureOpen();
            List<Candidate> transfer = new ArrayList<>(heap);
            heap.clear();
            target.adoptAuxEscrow(comparatorEscrow, finalHeadEscrow);
            reserved -= comparatorEscrow + finalHeadEscrow;
            comparatorEscrow = 0;
            finalHeadEscrow = 0;
            for (int i = 0; i < transfer.size(); i++) {
                Candidate candidate = transfer.get(i);
                boolean first = i == 0;
                boolean last = i + 1 == transfer.size();
                if (memoryManager != null) {
                    memoryManager.releaseExact(CONTAINER_CREDIT);
                    reserved -= CONTAINER_CREDIT;
                }
                long transferred = Math.addExact(candidate.escrowCredit.rowCredit(),
                        first ? ExternalSortEscrow.batchFixedCredit() : 0L);
                reserved -= transferred; // as soon as the import starts, logical ownership moves to the external buffer
                if (first) {
                    batchCreditReserved = false;
                }
                target.importEscrowedRow(candidate.row,
                        candidate.escrowCredit.serializedSize(), candidate.escrowCredit.rowEstimate(),
                        candidate.escrowCredit.scratch(), transferred, first, last);
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            heap.clear();
            if (memoryManager != null) {
                memoryManager.releaseExact(reserved);
            }
            reserved = 0;
            comparatorEscrow = 0;
            finalHeadEscrow = 0;
            batchCreditReserved = false;
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("Top candidate heap is closed");
            }
        }
    }

    private static final class Candidate {
        final DataRow row;
        final ExternalSortEscrow.Credit escrowCredit;

        Candidate(DataRow row, ExternalSortEscrow.Credit escrowCredit) {
            this.row = row;
            this.escrowCredit = escrowCredit;
        }

        long retainedCredit() {
            return escrowCredit == null ? 0L
                    : Math.addExact(escrowCredit.rowCredit(), CandidateHeap.CONTAINER_CREDIT);
        }
    }
}
