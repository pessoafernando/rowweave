package io.rowweave.mem.spill;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryBudgetExceededException;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.RowSerializer;
import io.rowweave.mem.RowSizeEstimator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Rereadable row block with a double spill backstop: a row-count threshold and a managed-byte budget.
 *
 * <p>The compatibility constructor without a {@link MemoryManager} still uses only the threshold. Once a production owner injects the manager, every retained
 * DataRow and the largest single-row serialization scratch are reserved first. Whichever limit is reached first diverts to an exclusive SpillRun, leaving the order and the reread semantics unchanged.
 */
public final class EqualKeyBlock implements AutoCloseable {

    private static final long CONTAINER_CREDIT = 128L;

    private final RowSerializer serializer;
    private final SpillStore spillStore;
    private final int threshold;
    private final MemoryManager memoryManager;
    private final long borrowedSerializationEscrow;
    private final List<ResidentRow> heapRows = new ArrayList<>();
    private SpillRun run;
    private long reserved;
    private long serializationEscrow;
    private boolean spilling;
    private boolean closed;

    public EqualKeyBlock(DataSchema dataSchema, SpillStore spillStore, int threshold) {
        this(dataSchema, spillStore, threshold, null, 0L);
    }

    /** Production constructor: spills when either the row-count threshold or the manager byte budget is reached first. */
    public EqualKeyBlock(DataSchema dataSchema, SpillStore spillStore, int threshold,
                         MemoryManager memoryManager) {
        this(dataSchema, spillStore, threshold, memoryManager,
                memoryManager == null ? 0L : memoryManager.reusableReplayScratchBytes());
    }

    private EqualKeyBlock(DataSchema dataSchema, SpillStore spillStore, int threshold,
                          MemoryManager memoryManager, long borrowedSerializationEscrow) {
        if (threshold <= 0) {
            throw new IllegalArgumentException("threshold must be > 0");
        }
        if (borrowedSerializationEscrow < 0) {
            throw new IllegalArgumentException("borrowedSerializationEscrow must be >= 0");
        }
        this.serializer = new RowSerializer(dataSchema);
        this.spillStore = spillStore;
        this.threshold = threshold;
        this.memoryManager = memoryManager;
        this.borrowedSerializationEscrow = borrowedSerializationEscrow;
    }

    public void add(DataRow row) {
        ensureOpen();
        if (spilling) {
            appendSpilled(row);
            return;
        }
        if (spillStore != null && heapRows.size() >= threshold) {
            long scratch = memoryManager == null ? 0L : serializationCredit(row);
            switchToSpill(scratch);
            appendSpilled(row, scratch);
            return;
        }
        if (memoryManager == null) {
            heapRows.add(new ResidentRow(row, 0L));
            return;
        }

        RowCredit credit = credit(row);
        long escrowDelta = Math.max(0L,
                credit.serializationCredit - totalSerializationEscrow());
        long additional = Math.addExact(credit.retainedCredit, escrowDelta);
        if (memoryManager.tryReserve(additional)) {
            boolean published = false;
            try {
                heapRows.add(new ResidentRow(row, credit.retainedCredit));
                reserved = Math.addExact(reserved, additional);
                serializationEscrow = Math.addExact(serializationEscrow, escrowDelta);
                published = true;
                return;
            } finally {
                if (!published) {
                    memoryManager.releaseExact(additional);
                }
            }
        }
        if (spillStore == null) {
            throw new MemoryBudgetExceededException(
                    "EqualKeyBlock cannot retain row within budget=" + memoryManager.budget()
                            + " and no SpillStore is configured");
        }
        switchToSpill(credit.serializationCredit);
        appendSpilled(row, credit.serializationCredit);
    }

    public void finish() {
        ensureOpen();
        if (spilling) {
            try {
                run.finishWriting();
            } catch (RuntimeException | Error failure) {
                failStop(failure);
            }
        }
    }

    /** May be called repeatedly: insertion order is preserved across both the spill run and the heap. */
    public Iterator<DataRow> iterator() {
        ensureOpen();
        if (spilling) {
            final Iterator<byte[]> frames = run.read();
            return new Iterator<DataRow>() {
                @Override public boolean hasNext() { return frames.hasNext(); }
                @Override public DataRow next() { return serializer.deserialize(frames.next()); }
            };
        }
        final Iterator<ResidentRow> residents = heapRows.iterator();
        return new Iterator<DataRow>() {
            @Override public boolean hasNext() { return residents.hasNext(); }
            @Override public DataRow next() {
                if (!residents.hasNext()) throw new NoSuchElementException();
                return residents.next().row;
            }
        };
    }

    private void switchToSpill(long requiredScratch) {
        SpillRun candidate = null;
        try {
            candidate = spillStore.createRun();
            run = candidate;
            long targetScratch = memoryManager == null
                    ? 0L : Math.max(serializationEscrow, requiredScratch);
            long targetOwnedScratch = Math.max(0L, targetScratch - borrowedSerializationEscrow);
            if (memoryManager != null && targetOwnedScratch > reserved) {
                long additional = targetOwnedScratch - reserved;
                long nextReserved = Math.addExact(reserved, additional);
                if (!memoryManager.tryReserve(additional)) {
                    throw new MemoryBudgetExceededException(
                            "EqualKeyBlock spill scratch " + targetScratch
                                    + " cannot fit: reserved=" + reserved
                                    + " budget=" + memoryManager.budget());
                }
                reserved = nextReserved;
            }
            for (int i = 0; i < heapRows.size(); i++) {
                ResidentRow resident = heapRows.get(i);
                run.append(serializer.serialize(resident.row));
                heapRows.set(i, null);
            }
            heapRows.clear();
            if (memoryManager != null) {
                long excess = reserved - targetOwnedScratch;
                if (excess > 0) memoryManager.releaseExact(excess);
                reserved = targetOwnedScratch;
                serializationEscrow = targetOwnedScratch;
                memoryManager.recordSelfSpill();
            }
            spilling = true;
        } catch (RuntimeException | Error failure) {
            if (run == null) {
                run = candidate;
            }
            failStop(failure);
        }
    }

    private void appendSpilled(DataRow row) {
        appendSpilled(row, serializationCredit(row));
    }

    private void appendSpilled(DataRow row, long temporaryCredit) {
        if (memoryManager == null) {
            try {
                run.append(serializer.serialize(row));
                return;
            } catch (RuntimeException | Error failure) {
                failStop(failure);
                return;
            }
        }
        long additional = Math.max(0L, temporaryCredit - totalSerializationEscrow());
        if (additional > 0 && !memoryManager.tryReserve(additional)) {
            failStop(new MemoryBudgetExceededException(
                    "EqualKeyBlock serialization scratch " + additional
                            + " cannot fit: reserved=" + memoryManager.reservedBytes()
                            + " budget=" + memoryManager.budget()));
            return;
        }
        if (additional > 0) {
            reserved = Math.addExact(reserved, additional);
            serializationEscrow = Math.addExact(serializationEscrow, additional);
        }
        try {
            run.append(serializer.serialize(row));
        } catch (RuntimeException | Error failure) {
            failStop(failure);
        }
    }

    private RowCredit credit(DataRow row) {
        long retained = Math.addExact(RowSizeEstimator.estimateRow(row), CONTAINER_CREDIT);
        return new RowCredit(retained, serializationCredit(row));
    }

    private long serializationCredit(DataRow row) {
        int serializedSize = serializer.serializedSize(row);
        return Math.addExact(RowSizeEstimator.estimateByteArray(serializedSize),
                serializer.serializerScratchCredit(row));
    }

    private long totalSerializationEscrow() {
        return Math.addExact(borrowedSerializationEscrow, serializationEscrow);
    }

    /** After a conversion or write failure, release all ownership at once and keep the original failure as the cause. Always rethrows. */
    private void failStop(Throwable primary) {
        closed = true;
        SpillRun ownedRun = run;
        run = null;
        heapRows.clear();
        long ownedCredit = reserved;
        reserved = 0;
        serializationEscrow = 0;
        spilling = false;
        if (memoryManager != null && ownedCredit > 0) {
            try {
                memoryManager.releaseExact(ownedCredit);
            } catch (RuntimeException | Error cleanup) {
                addSuppressed(primary, cleanup);
            }
        }
        if (ownedRun != null) {
            try {
                ownedRun.dispose();
            } catch (RuntimeException | Error cleanup) {
                addSuppressed(primary, cleanup);
            }
        }
        rethrow(primary);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        SpillRun ownedRun = run;
        run = null;
        heapRows.clear();
        long ownedCredit = reserved;
        reserved = 0;
        serializationEscrow = 0;
        Throwable cleanup = null;
        if (memoryManager != null && ownedCredit > 0) {
            try {
                memoryManager.releaseExact(ownedCredit);
            } catch (RuntimeException | Error failure) {
                cleanup = failure;
            }
        }
        if (ownedRun != null) {
            try {
                ownedRun.dispose();
            } catch (RuntimeException | Error failure) {
                if (cleanup == null) cleanup = failure;
                else addSuppressed(cleanup, failure);
            }
        }
        if (cleanup != null) rethrow(cleanup);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("EqualKeyBlock is closed");
        }
    }

    private static void addSuppressed(Throwable primary, Throwable cleanup) {
        if (primary != cleanup) primary.addSuppressed(cleanup);
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        throw new RuntimeException(failure);
    }

    private static final class ResidentRow {
        final DataRow row;
        final long credit;
        ResidentRow(DataRow row, long credit) {
            this.row = row;
            this.credit = credit;
        }
    }

    private static final class RowCredit {
        final long retainedCredit;
        final long serializationCredit;
        RowCredit(long retainedCredit, long serializationCredit) {
            this.retainedCredit = retainedCredit;
            this.serializationCredit = serializationCredit;
        }
    }
}
