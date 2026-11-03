package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillRunWriter;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.ops.SortField;
import io.rowweave.sched.Task;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Synchronous pointer-based external sort. The resident batch, the primitive pointer sort and the merge credit
 * come from {@link EncodedSortBatch} and {@link ExternalSortMergePlanner}; every allocation happens only after the full
 * worst-case credit has been reserved. Execution stays a single-threaded synchronous drain-sort-spill inside the calling stage.
 */
public final class PointerSortBuffer implements EscrowedExternalSortBuffer {

    private final DataSchema dataSchema;
    private final RowSerializer serializer;
    private final NormalizedKeyComputer normKey;
    private final MemoryManager memoryManager;
    private final int runThreshold;
    private final SpillStore spillStore;
    private final boolean retainReplayScratch;
    private final EncodedSortBatch batch;
    private final SortedRunStore result;
    private final Task owningPipelineTask;

    private long seqCounter;
    private long batchReserved;
    private long comparatorReserved;
    private long finalHeadReserved;
    private long replayScratchReserved;
    private long auxReserved;
    private long totalRows;
    private int maxFrameSize;
    private boolean sorted;
    private boolean closed;
    private boolean iteratorOpened;
    private boolean auxReleased;
    private Throwable terminalFailure;

    public PointerSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator, SortField[] sortFields,
                             MemorySegmentPool pool, MemoryManager memoryManager, int runThreshold) {
        this(dataSchema, comparator, sortFields, pool, memoryManager, runThreshold, new MemorySpillStore());
    }

    public PointerSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator, SortField[] sortFields,
                             MemorySegmentPool pool, MemoryManager memoryManager, int runThreshold,
                             SpillStore spillStore) {
        this(dataSchema, comparator, NormalizedKeyComputer.of(dataSchema, sortFields), pool,
                memoryManager, runThreshold, spillStore, sortFields, false);
    }

    PointerSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator, SortField[] sortFields,
                      MemorySegmentPool pool, MemoryManager memoryManager, int runThreshold,
                      SpillStore spillStore, boolean groupedReplayConsumer) {
        this(dataSchema, comparator, NormalizedKeyComputer.of(dataSchema, sortFields), pool,
                memoryManager, runThreshold, spillStore, sortFields, groupedReplayConsumer);
    }

    private PointerSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator, NormalizedKeyComputer normKey,
                              MemorySegmentPool pool, MemoryManager memoryManager, int runThreshold,
                              SpillStore spillStore, SortField[] sortFields,
                              boolean groupedReplayConsumer) {
        this.serializer = new RowSerializer(dataSchema);
        this.dataSchema = dataSchema;
        this.normKey = normKey;
        this.memoryManager = memoryManager;
        this.runThreshold = Math.max(1, runThreshold);
        this.spillStore = spillStore;
        this.retainReplayScratch = groupedReplayConsumer
                || memoryManager.requiresGroupedReplayScratch();
        EncodedSortKeyComparator encodedComparator = EncodedSortKeyComparator.create(
                dataSchema, comparator, sortFields);
        this.batch = new EncodedSortBatch(serializer, comparator, normKey, encodedComparator, pool);
        this.result = new SortedRunStore(
                dataSchema, serializer, comparator, normKey, encodedComparator, spillStore);
        this.owningPipelineTask = Task.currentPipelineTask();
    }

    static PointerSortBuffer comparatorOnly(DataSchema dataSchema, Comparator<DataRow> comparator,
                                            MemorySegmentPool pool, MemoryManager memoryManager,
                                            int runThreshold, SpillStore spillStore) {
        return new PointerSortBuffer(dataSchema, comparator, NormalizedKeyComputer.comparatorFallback(dataSchema),
                pool, memoryManager, runThreshold, spillStore, null, false);
    }

    @Override
    public void putRow(DataRow row) {
        requireWritable();
        int serializedSize = serializer.serializedSize(row);
        long rowEstimate = RowSizeEstimator.estimateRow(row);
        long scratch = serializer.serializerScratchCredit(row);
        long replayScratch = Math.addExact(RowSizeEstimator.estimateByteArray(serializedSize), scratch);
        append(serializedSize, rowEstimate, scratch, replayScratch,
                seq -> batch.addDirect(row, serializedSize, normKey.computePrefix(row), seq));
    }

    /** The byte-native entry point does not re-serialize: the decode upper bound is reserved from the encoded length first, then the prefix is computed. */
    @Override
    public void putBytes(byte[] rowBytes) {
        requireWritable();
        long rowEstimate = RowSizeEstimator.estimateEncodedUpperBound(
                dataSchema.getFieldCount(), rowBytes.length);
        long replayScratch = Math.addExact(RowSizeEstimator.estimateByteArray(rowBytes.length), rowEstimate);
        append(rowBytes.length, rowEstimate, 0, replayScratch,
                seq -> batch.addBytes(rowBytes,
                        normKey.computePrefixFromBytes(rowBytes, dataSchema), seq));
    }

    private void append(int serializedSize, long rowEstimate, long scratch, long replayScratch,
                        BatchAppender appender) {
        long rowCredit = ExternalSortMemory.rowCredit(serializedSize, rowEstimate, scratch);
        long comparatorTarget = Math.multiplyExact(2L, rowEstimate);
        long headTarget = ExternalSortMemory.finalHeadCredit(serializedSize, rowEstimate);
        while (true) {
            requireWritable();
            boolean newBatch = batch.isEmpty();
            long comparatorDelta = Math.max(0, comparatorTarget - comparatorReserved);
            long headDelta = Math.max(0, headTarget - finalHeadReserved);
            long replayDelta = retainReplayScratch
                    ? Math.max(0L, replayScratch - replayScratchReserved) : 0L;
            long batchDelta = Math.addExact(rowCredit,
                    newBatch ? ExternalSortMemory.BATCH_FIXED_CREDIT : 0);
            long reserveDelta = Math.addExact(batchDelta,
                    Math.addExact(comparatorDelta, Math.addExact(headDelta, replayDelta)));
            if (!memoryManager.tryReserve(reserveDelta)) {
                if (!batch.isEmpty()) {
                    spillCurrentBatch();
                    continue;
                }
                throw new MemoryBudgetExceededException("sort row credit " + reserveDelta
                        + " cannot fit: reserved=" + memoryManager.reservedBytes()
                        + " budget=" + memoryManager.budget());
            }

            boolean published = false;
            try {
                appender.append(seqCounter);
                batchReserved = Math.addExact(batchReserved, batchDelta);
                comparatorReserved = Math.addExact(comparatorReserved, comparatorDelta);
                finalHeadReserved = Math.addExact(finalHeadReserved, headDelta);
                replayScratchReserved = Math.addExact(replayScratchReserved, replayDelta);
                auxReserved = Math.addExact(auxReserved,
                        Math.addExact(comparatorDelta, Math.addExact(headDelta, replayDelta)));
                maxFrameSize = Math.max(maxFrameSize,
                        Math.addExact(ExternalSortMemory.FRAME_HEADER_BYTES, serializedSize));
                seqCounter = Math.incrementExact(seqCounter);
                totalRows = Math.incrementExact(totalRows);
                published = true;
            } catch (RuntimeException | Error failure) {
                fail(failure);
                throw failure;
            } finally {
                if (!published) memoryManager.releaseExact(reserveDelta);
            }
            if (replayDelta > 0) {
                memoryManager.publishReplayScratch(this, replayScratchReserved);
            }
            if (batch.currentBytes() >= batchTargetBytes() || batch.size() >= runThreshold) {
                spillCurrentBatch();
            }
            return;
        }
    }

    private long batchTargetBytes() {
        return Math.max(1L, memoryManager.budget() / 2L);
    }

    @Override
    public void adoptAuxEscrow(long comparatorCredit, long headCredit) {
        requireWritable();
        if (auxReserved != 0 || comparatorReserved != 0 || finalHeadReserved != 0 || totalRows != 0) {
            throw new IllegalStateException("aux escrow can only be adopted by a fresh buffer");
        }
        comparatorReserved = comparatorCredit;
        finalHeadReserved = headCredit;
        auxReserved = Math.addExact(comparatorCredit, headCredit);
    }

    @Override
    public void importEscrowedRow(DataRow row, int serializedSize, long rowEstimate, long scratch,
                                  long credit, boolean batchStart, boolean batchEnd) {
        requireWritable();
        long expected = Math.addExact(
                ExternalSortMemory.rowCredit(serializedSize, rowEstimate, scratch),
                batchStart ? ExternalSortMemory.BATCH_FIXED_CREDIT : 0);
        boolean currentEmpty = batch.isEmpty();
        if (credit != expected || batchStart != currentEmpty) {
            throw new IllegalArgumentException("invalid transferred batch credit: " + credit
                    + " expected=" + expected + " batchStart=" + batchStart
                    + " currentEmpty=" + currentEmpty);
        }
        batchReserved = Math.addExact(batchReserved, credit);
        try {
            long prefix = normKey.computePrefix(row);
            batch.addDirect(row, serializedSize, prefix, seqCounter);
            maxFrameSize = Math.max(maxFrameSize,
                    Math.addExact(ExternalSortMemory.FRAME_HEADER_BYTES, serializedSize));
            seqCounter = Math.incrementExact(seqCounter);
            totalRows = Math.incrementExact(totalRows);
            if (batchEnd) spillCurrentBatch();
        } catch (RuntimeException | Error failure) {
            fail(failure);
            throw failure;
        }
    }

    private void spillCurrentBatch() {
        if (batch.isEmpty()) return;
        if (owningPipelineTask != null) {
            owningPipelineTask.beginDelegatedWork();
        }
        try {
            SlotGuard.cpuPhase(() -> {
                int[] order = batch.sortPointers();
                SpillRun run = SpillRunWriter.write(spillStore,
                        target -> batch.appendSortedFrames(target, order));
                result.acceptRun(run);
                batch.releaseAllAndTrim();
                long released = batchReserved;
                memoryManager.releaseExact(released);
                batchReserved = 0;
                return null;
            });
        } catch (RuntimeException | Error failure) {
            fail(failure);
            throw failure;
        } finally {
            if (owningPipelineTask != null) {
                owningPipelineTask.endDelegatedWork();
            }
        }
    }

    @Override
    public void sort() {
        requireWritable();
        spillCurrentBatch();
        try {
            finalHeadReserved = ExternalSortMergePlanner.prepareFinalRuns(
                    result, memoryManager, maxFrameSize, finalHeadReserved);
            auxReserved = Math.addExact(replayScratchReserved,
                    Math.addExact(comparatorReserved, finalHeadReserved));
            if (result.acceptedRunCount() == 0) releaseComparatorCredit();
            sorted = true;
        } catch (RuntimeException | Error failure) {
            fail(failure);
            throw failure;
        }
    }

    private void releaseComparatorCredit() {
        if (comparatorReserved == 0) return;
        long released = comparatorReserved;
        memoryManager.releaseExact(released);
        comparatorReserved = 0;
        auxReserved -= released;
    }

    @Override
    public long size() {
        return totalRows;
    }

    @Override
    public synchronized long retainedReplayScratchBytes() {
        return replayScratchReserved;
    }

    @Override
    public synchronized Iterator<DataRow> iterator() {
        requireReadable();
        if (iteratorOpened) throw new IllegalStateException("sort buffer iterator already opened");
        iteratorOpened = true;
        Iterator<DataRow> delegate;
        try {
            delegate = result.iterateSorted();
        } catch (RuntimeException | Error failure) {
            fail(failure);
            releaseAuxAfter(failure);
            throw failure;
        }
        return new Iterator<DataRow>() {
            @Override
            public boolean hasNext() {
                requireReadable();
                try {
                    boolean has = delegate.hasNext();
                    if (!has) releaseAuxOnce();
                    return has;
                } catch (RuntimeException | Error failure) {
                    fail(failure);
                    releaseAuxAfter(failure);
                    throw failure;
                }
            }

            @Override
            public DataRow next() {
                requireReadable();
                try {
                    if (!delegate.hasNext()) {
                        releaseAuxOnce();
                        throw new NoSuchElementException();
                    }
                    DataRow row = delegate.next();
                    if (!delegate.hasNext()) releaseAuxOnce();
                    return row;
                } catch (RuntimeException | Error failure) {
                    if (!(failure instanceof NoSuchElementException)) fail(failure);
                    releaseAuxAfter(failure);
                    throw failure;
                }
            }
        };
    }

    private void requireWritable() {
        checkFailure();
        if (closed || sorted) throw new IllegalStateException("sort buffer is not writable");
    }

    private void requireReadable() {
        checkFailure();
        if (closed) throw new IllegalStateException("sort buffer is closed");
        if (!sorted) throw new IllegalStateException("iterate before sort()");
    }

    private void fail(Throwable failure) {
        if (terminalFailure == null) terminalFailure = failure;
    }

    private void checkFailure() {
        if (terminalFailure instanceof RuntimeException) throw (RuntimeException) terminalFailure;
        if (terminalFailure instanceof Error) throw (Error) terminalFailure;
    }

    private synchronized void releaseAuxOnce() {
        if (auxReleased) return;
        long released = auxReserved - replayScratchReserved;
        if (released != 0) memoryManager.releaseExact(released);
        auxReserved = replayScratchReserved;
        comparatorReserved = 0;
        finalHeadReserved = 0;
        auxReleased = true;
    }

    private synchronized void releaseReplayScratchOnce() {
        if (replayScratchReserved == 0) return;
        long released = replayScratchReserved;
        memoryManager.publishReplayScratch(this, 0L);
        memoryManager.releaseExact(released);
        replayScratchReserved = 0;
        auxReserved -= released;
    }

    private void releaseAuxAfter(Throwable primary) {
        try {
            releaseAuxOnce();
        } catch (RuntimeException | Error cleanup) {
            if (primary != cleanup) primary.addSuppressed(cleanup);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        Throwable cleanupFailure = null;
        try {
            batch.discardAndTrim();
        } catch (RuntimeException | Error cleanup) {
            cleanupFailure = appendFailure(cleanupFailure, cleanup);
        }
        try {
            result.disposeAll();
        } catch (RuntimeException | Error cleanup) {
            cleanupFailure = appendFailure(cleanupFailure, cleanup);
        }
        try {
            if (batchReserved != 0) memoryManager.releaseExact(batchReserved);
            batchReserved = 0;
        } catch (RuntimeException | Error cleanup) {
            cleanupFailure = appendFailure(cleanupFailure, cleanup);
        }
        try {
            releaseAuxOnce();
        } catch (RuntimeException | Error cleanup) {
            cleanupFailure = appendFailure(cleanupFailure, cleanup);
        }
        try {
            releaseReplayScratchOnce();
        } catch (RuntimeException | Error cleanup) {
            cleanupFailure = appendFailure(cleanupFailure, cleanup);
        }
        if (cleanupFailure != null && terminalFailure == null) throwFailure(cleanupFailure);
        if (terminalFailure != null && cleanupFailure != null && terminalFailure != cleanupFailure) {
            terminalFailure.addSuppressed(cleanupFailure);
        }
    }

    private static Throwable appendFailure(Throwable primary, Throwable cleanup) {
        if (primary == null) return cleanup;
        if (primary != cleanup) primary.addSuppressed(cleanup);
        return primary;
    }

    private static void throwFailure(Throwable failure) {
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
    }

    @FunctionalInterface
    private interface BatchAppender {
        void append(long seq);
    }
}
