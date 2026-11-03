package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * Two-tier SortBuffer decorator, transparent to the operator (it implements SortBuffer, so injection points
 * need no change).
 *
 * <ul>
 * <li><b>Tier one, in-memory fast path</b>: while the data fits the budget it holds {@link DataRow} objects
 * directly in a List, {@code sort()} goes through {@link List#sort} (TimSort, stable) and the output emits
 * DataRow objects as they are, so nothing is serialized at all and performance matches plain Java.</li>
 * <li><b>Tier two, serialized spill</b>: each row calls {@link MemoryManager#tryReserve} for its estimated
 * size, and on failure (over budget) it walks the rows already held and {@code putRow}s each into a lazily
 * built {@link PointerSortBuffer}, sending subsequent rows there too, which ends in an external merge.</li>
 * </ul>
 *
 * <p><b>Switch correctness</b>: the switch uses the secondary {@code putRow(DataRow)} with the object reused,
 * avoiding a serialize-then-putBytes double conversion; tie order relies on the comparator being a total order
 * (OrderBy injects __seq, and for operators without a total order an equal-key tie is pinned by the A/B
 * equivalence check on recorded outputs). Charges go through tryReserve, matching the child-quota isolation, so
 * several buffers sharing one MemoryManager in parallel cannot exceed the budget.
 */
public final class TwoTierSortBuffer implements SortBuffer, CoordinatedExternalSortBuffer {

    private final io.rowweave.sched.Task owningPipelineTask =
            io.rowweave.sched.Task.currentPipelineTask();
    private final Comparator<DataRow> comparator;
    private final MemoryManager memoryManager;
    private final Supplier<SortBuffer> secondaryFactory;
    private final RowSerializer serializer;
    private final boolean externalEscrow;
    private final int escrowRunThreshold;
    private final int escrowBufferCount;

    private final List<ResidentEntry> resident = new ArrayList<>(); // the primary breaker working set
    private long residentReserved;                        // estimate already reserved at the primary tier, returned when the secondary is taken
    private long comparatorEscrow;
    private long finalHeadEscrow;
    private long residentBatchBytes;
    private int residentBatchRows;
    private SortBuffer secondary;                          // secondary tier, built lazily once the budget is exceeded
    private boolean sorted;
    private boolean closed;
    private Throwable terminalFailure;

    public TwoTierSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator,
                             MemoryManager memoryManager, Supplier<SortBuffer> secondaryFactory) {
        this.comparator = comparator;
        this.memoryManager = memoryManager;
        this.secondaryFactory = secondaryFactory;
        this.serializer = new RowSerializer(dataSchema);
        this.externalEscrow = false;
        this.escrowRunThreshold = 1;
        this.escrowBufferCount = 2;
    }

    /** Production constructor: the primary tier holds, from the first row, an external escrow that a Pipelined buffer can take over without a second reservation. */
    public TwoTierSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator,
                             MemoryManager memoryManager, io.rowweave.ops.SortField[] sortFields,
                             Supplier<SortBuffer> secondaryFactory) {
        this(dataSchema, comparator, memoryManager, sortFields, 1024, 3, secondaryFactory);
    }

    /** The batch policy must match the Pipelined buffer it hands over to, so fixed credit transfers per real logical batch. */
    public TwoTierSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator,
                             MemoryManager memoryManager, io.rowweave.ops.SortField[] sortFields,
                             int runThreshold, int bufferCount, Supplier<SortBuffer> secondaryFactory) {
        this.comparator = comparator;
        this.memoryManager = memoryManager;
        this.secondaryFactory = secondaryFactory;
        this.serializer = new RowSerializer(dataSchema);
        this.externalEscrow = true;
        this.escrowRunThreshold = Math.max(1, runThreshold);
        this.escrowBufferCount = Math.max(2, bufferCount);
    }

    @Override
    public void putRow(DataRow row) {
        ensureWritable();
        if (secondary != null) {
            secondary.putRow(row); // already on the secondary tier: pass straight through
            return;
        }
        EscrowReservation escrow = externalEscrow ? externalReservation(row) : null;
        long est = externalEscrow ? escrow.total : RowSizeEstimator.estimateRow(row);
        if (memoryManager.tryReserve(est)) {
            try {
                ResidentEntry entry = externalEscrow
                        ? new ResidentEntry(row, escrow.batchCredit, escrow.batchStart,
                                escrow.serializedSize, escrow.rowEstimate, escrow.scratch)
                        : new ResidentEntry(row, est, false, 0, 0, 0);
                resident.add(entry); // single publish point, and a failed ArrayList.add leaves the size unchanged
                residentReserved = Math.addExact(residentReserved, est);
                if (externalEscrow) {
                    comparatorEscrow = Math.addExact(comparatorEscrow, escrow.comparatorDelta);
                    finalHeadEscrow = Math.addExact(finalHeadEscrow, escrow.headDelta);
                    residentBatchBytes = Math.addExact(residentBatchBytes, escrow.serializedSize);
                    residentBatchRows++;
                    if (residentBatchBytes >= escrowBatchTargetBytes()
                            || residentBatchRows >= escrowRunThreshold) {
                        residentBatchBytes = 0;
                        residentBatchRows = 0;
                    }
                }
            } catch (RuntimeException | Error failure) {
                memoryManager.releaseExact(est);
                throw failure;
            }
        } else {
            switchToSecondary(); // budget exceeded, switch to the secondary tier
            secondary.putRow(row);
        }
    }

    private SortBuffer createSecondary() {
        return owningPipelineTask == null ? secondaryFactory.get()
                : owningPipelineTask.withPipelineContext(secondaryFactory);
    }

    /** Over budget: build the secondary lazily, drain each resident DataRow into it with putRow (reusing the object), and return the tier-one estimated charge, since the secondary re-charges real bytes and must not double-count. */
    private void switchToSecondary() {
        if (externalEscrow) {
            switchEscrowToSecondary();
            return;
        }
        SortBuffer candidate = createSecondary();
        try {
            for (int i = 0; i < resident.size(); i++) {
                ResidentEntry entry = resident.set(i, null); // remove the primary tier's retained owner first
                long bytes = entry.credit;
                memoryManager.releaseExact(bytes);
                residentReserved -= bytes;
                candidate.putRow(entry.row);
            }
            resident.clear();
            secondary = candidate;
        } catch (RuntimeException | Error failure) {
            candidate.close();
            resident.clear();
            memoryManager.releaseExact(residentReserved);
            residentReserved = 0;
            throw failure;
        }
    }

    private EscrowReservation externalReservation(DataRow row) {
        int serializedSize = serializer.serializedSize(row);
        long rowEstimate = RowSizeEstimator.estimateRow(row);
        long scratch = serializer.serializerScratchCredit(row);
        long rowCredit = PipelinedExternalSortBuffer.rowCredit(serializedSize, rowEstimate, scratch);
        long comparatorTarget = Math.multiplyExact(2L, rowEstimate);
        long headTarget = PipelinedExternalSortBuffer.finalHeadCredit(serializedSize, rowEstimate);
        long comparatorDelta = Math.max(0, comparatorTarget - comparatorEscrow);
        long headDelta = Math.max(0, headTarget - finalHeadEscrow);
        boolean batchStart = residentBatchRows == 0;
        long batchCredit = Math.addExact(rowCredit,
                batchStart ? PipelinedExternalSortBuffer.BATCH_FIXED_CREDIT : 0);
        return new EscrowReservation(batchCredit, comparatorDelta, headDelta,
                batchStart, serializedSize, rowEstimate, scratch);
    }

    private long escrowBatchTargetBytes() {
        return Math.max(1L, memoryManager.budget() / (escrowBufferCount + 1L));
    }

    private void switchEscrowToSecondary() {
        SortBuffer candidate = createSecondary();
        if (!(candidate instanceof EscrowedExternalSortBuffer)) {
            try {
                candidate.close();
            } catch (RuntimeException | Error ignored) {
                // configuration failure below remains primary
            }
            throw new IllegalStateException("external-escrow TwoTier requires escrow-capable secondary");
        }
        EscrowedExternalSortBuffer external = (EscrowedExternalSortBuffer) candidate;
        try {
            long aux = Math.addExact(comparatorEscrow, finalHeadEscrow);
            long transferredComparator = comparatorEscrow;
            long transferredHead = finalHeadEscrow;
            external.adoptAuxEscrow(transferredComparator, transferredHead);
            residentReserved -= aux;
            comparatorEscrow = 0;
            finalHeadEscrow = 0;

            for (int i = 0; i < resident.size(); i++) {
                ResidentEntry entry = resident.set(i, null);
                boolean batchEnd = i + 1 == resident.size() || resident.get(i + 1).batchStart;
                residentReserved -= entry.credit;
                external.importEscrowedRow(entry.row, entry.serializedSize, entry.rowEstimate,
                        entry.scratch, entry.credit, entry.batchStart, batchEnd);
            }
            resident.clear();
            residentBatchBytes = 0;
            residentBatchRows = 0;
            secondary = candidate;
        } catch (RuntimeException | Error failure) {
            try {
                candidate.close();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            resident.clear();
            residentBatchBytes = 0;
            residentBatchRows = 0;
            if (residentReserved > 0) {
                try {
                    memoryManager.releaseExact(residentReserved);
                } catch (RuntimeException | Error cleanup) {
                    failure.addSuppressed(cleanup);
                }
                residentReserved = 0;
            }
            throw failure;
        }
    }

    @Override
    public void sort() {
        ensureWritable();
        if (secondary != null) {
            secondary.sort();
        } else {
            resident.sort((left, right) -> comparator.compare(left.row, right.row));
        }
        sorted = true;
    }

    @Override
    public Iterator<DataRow> iterator() {
        ensureOpen();
        if (!sorted) {
            throw new IllegalStateException("iterate before sort()");
        }
        if (secondary != null) {
            return secondary.iterator();
        }
        Iterator<ResidentEntry> entries = resident.iterator();
        return new Iterator<DataRow>() {
            @Override public boolean hasNext() { return entries.hasNext(); }
            @Override public DataRow next() {
                if (!entries.hasNext()) throw new NoSuchElementException();
                return entries.next().row;
            }
        };
    }

    @Override
    public long size() {
        ensureOpen();
        return secondary != null ? secondary.size() : resident.size();
    }

    /** Test probe: whether the secondary tier, the spill path, has been taken. */
    public boolean spilledForTest() {
        return secondary != null;
    }

    @Override
    public boolean externalized() {
        return secondary != null;
    }

    @Override
    public void externalizeFromEscrow() {
        ensureWritable();
        if (!externalEscrow) {
            throw new IllegalStateException("TwoTier was not constructed with external escrow");
        }
        if (secondary == null) {
            switchEscrowToSecondary();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (secondary != null) {
            secondary.close();
        }
        resident.clear();
        residentBatchBytes = 0;
        residentBatchRows = 0;
        memoryManager.releaseExact(residentReserved);
        residentReserved = 0;
    }

    @Override
    public void cancel(Throwable cause) {
        if (terminalFailure == null) {
            terminalFailure = cause == null ? new java.util.concurrent.CancellationException("sort buffer cancelled") : cause;
        }
        if (secondary != null) {
            secondary.cancel(terminalFailure);
        }
    }

    private void ensureWritable() {
        ensureOpen();
        if (sorted) {
            throw new IllegalStateException("sort buffer already sorted");
        }
    }

    private void ensureOpen() {
        if (terminalFailure != null) {
            if (terminalFailure instanceof RuntimeException) {
                throw (RuntimeException) terminalFailure;
            }
            if (terminalFailure instanceof Error) {
                throw (Error) terminalFailure;
            }
            throw new RuntimeException(terminalFailure);
        }
        if (closed) {
            throw new IllegalStateException("sort buffer is closed");
        }
    }

    private static final class EscrowReservation {
        final long batchCredit;
        final long comparatorDelta;
        final long headDelta;
        final boolean batchStart;
        final int serializedSize;
        final long rowEstimate;
        final long scratch;
        final long total;

        EscrowReservation(long batchCredit, long comparatorDelta, long headDelta,
                          boolean batchStart, int serializedSize, long rowEstimate, long scratch) {
            this.batchCredit = batchCredit;
            this.comparatorDelta = comparatorDelta;
            this.headDelta = headDelta;
            this.batchStart = batchStart;
            this.serializedSize = serializedSize;
            this.rowEstimate = rowEstimate;
            this.scratch = scratch;
            this.total = Math.addExact(batchCredit, Math.addExact(comparatorDelta, headDelta));
        }
    }

    private static final class ResidentEntry {
        final DataRow row;
        final long credit;
        final boolean batchStart;
        final int serializedSize;
        final long rowEstimate;
        final long scratch;

        private ResidentEntry(DataRow row, long credit, boolean batchStart,
                              int serializedSize, long rowEstimate, long scratch) {
            this.row = row;
            this.credit = credit;
            this.batchStart = batchStart;
            this.serializedSize = serializedSize;
            this.rowEstimate = rowEstimate;
            this.scratch = scratch;
        }
    }
}
