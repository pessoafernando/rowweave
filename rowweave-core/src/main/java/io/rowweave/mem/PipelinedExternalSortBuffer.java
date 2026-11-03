package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillRunWriter;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.ops.SortField;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Three-threaded read-sort-spill external sort. Each batch in flight holds its operator MemoryManager credit through a distinct token,
 * so a worker failure stops the pipeline and close reclaims each segment, pool reservation, run and token exactly once.
 */
public final class PipelinedExternalSortBuffer implements EscrowedExternalSortBuffer {

    static final long BATCH_FIXED_CREDIT = ExternalSortMemory.BATCH_FIXED_CREDIT;
    private static final int FRAME_HEADER_BYTES = ExternalSortMemory.FRAME_HEADER_BYTES;

    private final io.rowweave.sched.Task owningPipelineTask =
            io.rowweave.sched.Task.currentPipelineTask();
    private final DataSchema dataSchema;
    private final java.util.Comparator<DataRow> comparator;
    private final SortField[] sortFields;
    private final RowSerializer serializer;
    private final NormalizedKeyComputer normKey;
    private final MemoryManager memoryManager;
    private final int runThreshold;
    private final int nBuffers;
    private final SpillStore spillStore;
    private final java.util.function.Supplier<MemorySegmentPool> poolFactory;
    private final AtomicLong globalSeq = new AtomicLong();
    private final AtomicReference<Throwable> terminalFailure = new AtomicReference<>();
    private final Set<BatchCreditToken> activeTokens = ConcurrentHashMap.newKeySet();
    private final java.util.List<EncodedSortBatch> allStores = new java.util.ArrayList<>();
    private final Object generationMonitor = new Object();

    private final ArrayBlockingQueue<EncodedSortBatch> emptyQ;
    private final ArrayBlockingQueue<Batch> sortQ;
    private final ArrayBlockingQueue<Batch> spillQ;
    private final SortedRunStore result;
    private final Thread sortThread;
    private final Thread spillThread;

    private volatile Lifecycle lifecycle = Lifecycle.RUNNING;
    private volatile boolean eof;
    private volatile boolean sortDone;
    private volatile boolean spillDone;
    private volatile boolean sorted;
    private boolean closed;
    private boolean iteratorOpened;
    private boolean auxReleased;
    private long totalRows;
    private long comparatorReserved;
    private long finalHeadReserved;
    private long auxReserved;
    private int maxFrameSize;
    private Batch current;

    public PipelinedExternalSortBuffer(DataSchema dataSchema, java.util.Comparator<DataRow> comparator,
                                       SortField[] sortFields, MemoryManager memoryManager,
                                       int runThreshold, int nBuffers) {
        this(dataSchema, comparator, sortFields, memoryManager, runThreshold, nBuffers,
                new MemorySpillStore(), HeapPool::new);
    }

    public PipelinedExternalSortBuffer(DataSchema dataSchema, java.util.Comparator<DataRow> comparator,
                                       SortField[] sortFields, MemoryManager memoryManager,
                                       int runThreshold, int nBuffers, SpillStore spillStore,
                                       java.util.function.Supplier<MemorySegmentPool> poolFactory) {
        this.dataSchema = dataSchema;
        this.comparator = comparator;
        this.sortFields = sortFields.clone();
        this.serializer = new RowSerializer(dataSchema);
        this.normKey = NormalizedKeyComputer.of(dataSchema, sortFields);
        this.memoryManager = memoryManager;
        this.runThreshold = Math.max(1, runThreshold);
        this.nBuffers = Math.max(2, nBuffers);
        this.spillStore = spillStore;
        this.poolFactory = poolFactory;
        this.emptyQ = new ArrayBlockingQueue<>(this.nBuffers);
        this.sortQ = new ArrayBlockingQueue<>(this.nBuffers);
        this.spillQ = new ArrayBlockingQueue<>(this.nBuffers);

        SortedRunStore resultCandidate = null;
        try {
            EncodedSortKeyComparator encodedComparator = EncodedSortKeyComparator.create(
                    dataSchema, comparator, sortFields);
            resultCandidate = new SortedRunStore(dataSchema, serializer, comparator, normKey,
                    encodedComparator, spillStore);
            for (int i = 0; i < this.nBuffers; i++) {
                EncodedSortBatch store = new EncodedSortBatch(
                        serializer, comparator, normKey, encodedComparator, poolFactory.get());
                allStores.add(store);
                emptyQ.add(store);
            }
            current = new Batch(takeEmpty());
        } catch (RuntimeException | Error failure) {
            cleanupConstructionFailure(failure);
            throw failure;
        }
        this.result = resultCandidate;

        String suffix = Integer.toHexString(System.identityHashCode(this));
        this.sortThread = new Thread(() -> runOwnedWorker(this::runSortWorker), "rowweave-sort-" + suffix);
        this.spillThread = new Thread(() -> runOwnedWorker(this::runSpillWorker), "rowweave-spill-" + suffix);
        this.sortThread.setDaemon(true);
        this.spillThread.setDaemon(true);
        this.sortThread.start();
        this.spillThread.start();
    }

    @Override
    public void putRow(DataRow row) {
        requireWritable();
        final int serializedSize = serializer.serializedSize(row);
        final long rowEstimate = RowSizeEstimator.estimateRow(row);
        final long scratch = serializer.serializerScratchCredit(row);
        final long rowCredit = rowCredit(serializedSize, rowEstimate, scratch);
        final long comparatorTarget = Math.multiplyExact(2L, rowEstimate);
        final long headTarget = finalHeadCredit(serializedSize, rowEstimate);

        while (true) {
            requireWritable();
            boolean newBatch = current.store.isEmpty();
            long comparatorDelta = Math.max(0, comparatorTarget - comparatorReserved);
            long headDelta = Math.max(0, headTarget - finalHeadReserved);
            long batchDelta = Math.addExact(rowCredit, newBatch ? BATCH_FIXED_CREDIT : 0);
            long reserveDelta = Math.addExact(batchDelta, Math.addExact(comparatorDelta, headDelta));
            boolean reserved = memoryManager.tryReserve(reserveDelta);
            if (!reserved) {
                if (!current.store.isEmpty()) {
                    handOffCurrent(true);
                    continue;
                }
                if (activeTokens.isEmpty()) {
                    // A spill may have released the last token between failed tryReserve and this check.
                    reserved = memoryManager.tryReserve(reserveDelta);
                    if (!reserved) {
                        throw new MemoryBudgetExceededException("sort row credit " + reserveDelta
                                + " cannot fit: reserved=" + memoryManager.reservedBytes()
                                + " budget=" + memoryManager.budget());
                    }
                } else {
                    awaitCreditGeneration();
                    continue;
                }
            }

            boolean published = false;
            try {
                long prefix = normKey.computePrefix(row);
                current.store.addDirect(row, serializedSize, prefix, globalSeq.get());
                if (newBatch) {
                    activeTokens.add(current.token);
                }
                current.token.addBytes(batchDelta);
                comparatorReserved = Math.addExact(comparatorReserved, comparatorDelta);
                finalHeadReserved = Math.addExact(finalHeadReserved, headDelta);
                auxReserved = Math.addExact(auxReserved, Math.addExact(comparatorDelta, headDelta));
                maxFrameSize = Math.max(maxFrameSize, Math.addExact(FRAME_HEADER_BYTES, serializedSize));
                globalSeq.incrementAndGet();
                totalRows++;
                published = true;
            } catch (RuntimeException | Error failure) {
                fail(failure);
                throw failure;
            } finally {
                if (!published) {
                    memoryManager.releaseExact(reserveDelta);
                }
            }
            if (current.store.currentBytes() >= batchTargetBytes()
                    || current.store.size() >= runThreshold) {
                handOffCurrent(true);
            }
            return;
        }
    }

    private long batchTargetBytes() {
        return Math.max(1L, memoryManager.budget() / (nBuffers + 1L));
    }

    static long rowCredit(int serializedSize, long rowEstimate, long scratch) {
        return ExternalSortMemory.rowCredit(serializedSize, rowEstimate, scratch);
    }

    static long finalHeadCredit(int serializedSize, long rowEstimate) {
        return ExternalSortMemory.finalHeadCredit(serializedSize, rowEstimate);
    }

    /** Hands the auxiliary ownership already reserved by the same MemoryManager from the TwoTier escrow to the new Pipelined buffer atomically. */
    @Override
    public void adoptAuxEscrow(long comparatorCredit, long headCredit) {
        requireWritable();
        if (auxReserved != 0 || comparatorReserved != 0 || finalHeadReserved != 0 || totalRows != 0) {
            throw new IllegalStateException("aux escrow can only be adopted by a fresh buffer");
        }
        long total = Math.addExact(comparatorCredit, headCredit);
        comparatorReserved = comparatorCredit;
        finalHeadReserved = headCredit;
        auxReserved = total;
    }

    /** TwoTier has already transferred logical ownership, so the handoff happens only at batch end; on failure the current token is still reclaimed by close. */
    @Override
    public void importEscrowedRow(DataRow row, int serializedSize, long rowEstimate, long scratch,
                                  long credit, boolean batchStart, boolean batchEnd) {
        requireWritable();
        long expected = Math.addExact(rowCredit(serializedSize, rowEstimate, scratch),
                batchStart ? BATCH_FIXED_CREDIT : 0);
        boolean currentEmpty = current.store.isEmpty();
        if (credit != expected || batchStart != currentEmpty) {
            throw new IllegalArgumentException("invalid transferred batch credit: " + credit
                    + " expected=" + expected + " batchStart=" + batchStart
                    + " currentEmpty=" + currentEmpty);
        }
        current.token.addBytes(credit);
        if (batchStart) {
            activeTokens.add(current.token);
        }
        try {
            long prefix = normKey.computePrefix(row);
            current.store.addDirect(row, serializedSize, prefix, globalSeq.getAndIncrement());
            totalRows++;
            maxFrameSize = Math.max(maxFrameSize, Math.addExact(FRAME_HEADER_BYTES, serializedSize));
            if (batchEnd) {
                handOffCurrent(true);
            }
        } catch (RuntimeException | Error failure) {
            fail(failure);
            throw failure;
        }
    }

    private void handOffCurrent(boolean acquireNext) {
        if (current.store.isEmpty()) {
            return;
        }
        Batch batch = current;
        while (true) {
            requireWritable();
            if (!batch.token.state.compareAndSet(TokenState.HELD_CURRENT, TokenState.HELD_SORT_Q)) {
                checkFailure();
                throw new IllegalStateException("batch token lost current ownership: " + batch.token.state.get());
            }
            if (sortQ.offer(batch)) {
                current = acquireNext ? new Batch(takeEmpty()) : null;
                signalGeneration();
                return;
            }
            if (!batch.token.state.compareAndSet(TokenState.HELD_SORT_Q, TokenState.HELD_CURRENT)) {
                checkFailure();
                throw new IllegalStateException("batch token claimed during handoff");
            }
            awaitCreditGeneration();
        }
    }

    private EncodedSortBatch takeEmpty() {
        while (true) {
            checkFailure();
            if (lifecycle == Lifecycle.CLOSING || lifecycle == Lifecycle.CLOSED) {
                throw new IllegalStateException("sort buffer is closed");
            }
            try {
                EncodedSortBatch store = emptyQ.poll(50, TimeUnit.MILLISECONDS);
                if (store != null) {
                    return store;
                }
            } catch (InterruptedException interrupted) {
                RuntimeException root = new RuntimeException("interrupted waiting for sort buffer", interrupted);
                fail(root);
                Thread.currentThread().interrupt();
                throw root;
            }
        }
    }

    private void awaitCreditGeneration() {
        checkFailure();
        try {
            synchronized (generationMonitor) {
                generationMonitor.wait(50L);
            }
        } catch (InterruptedException interrupted) {
            RuntimeException root = new RuntimeException("interrupted waiting for sort credit", interrupted);
            fail(root);
            Thread.currentThread().interrupt();
            throw root;
        }
        checkFailure();
    }

    private void runOwnedWorker(Runnable work) {
        if (owningPipelineTask == null) {
            work.run();
        } else {
            owningPipelineTask.withPipelineContext(() -> {
                work.run();
                return null;
            });
        }
    }

    private void runSortWorker() {
        try {
            while (true) {
                if (terminalFailure.get() != null) {
                    return;
                }
                Batch batch = sortQ.poll(50, TimeUnit.MILLISECONDS);
                if (batch == null) {
                    if (eof && sortQ.isEmpty()) {
                        return;
                    }
                    continue;
                }
                batch.order = batch.store.sortPointers();
                if (!batch.token.state.compareAndSet(TokenState.HELD_SORT_Q, TokenState.HELD_SPILL_Q)) {
                    throw new IllegalStateException("sort token ownership changed unexpectedly");
                }
                while (!spillQ.offer(batch, 50, TimeUnit.MILLISECONDS)) {
                    checkFailure();
                }
                signalGeneration();
            }
        } catch (InterruptedException interrupted) {
            if (terminalFailure.get() == null && lifecycle == Lifecycle.RUNNING) {
                fail(new RuntimeException("sort worker interrupted", interrupted));
            }
            Thread.currentThread().interrupt();
        } catch (Throwable failure) {
            fail(failure);
        } finally {
            sortDone = true;
            signalGeneration();
        }
    }

    private void runSpillWorker() {
        try {
            while (true) {
                if (terminalFailure.get() != null) {
                    return;
                }
                Batch batch = spillQ.poll(50, TimeUnit.MILLISECONDS);
                if (batch == null) {
                    if (sortDone && spillQ.isEmpty()) {
                        return;
                    }
                    continue;
                }
                SpillRun run = SpillRunWriter.write(spillStore,
                        target -> batch.store.appendSortedFrames(target, batch.order));
                synchronized (result) {
                    result.acceptRun(run);
                }
                batch.store.releaseAllAndTrim();
                releaseToken(batch.token, TokenState.HELD_SPILL_Q, false);
                batch.order = null;
                emptyQ.offer(batch.store);
                signalGeneration();
            }
        } catch (InterruptedException interrupted) {
            if (terminalFailure.get() == null && lifecycle == Lifecycle.RUNNING) {
                fail(new RuntimeException("spill worker interrupted", interrupted));
            }
            Thread.currentThread().interrupt();
        } catch (Throwable failure) {
            fail(failure);
        } finally {
            spillDone = true;
            signalGeneration();
        }
    }

    @Override
    public void sort() {
        requireWritable();
        handOffCurrent(false);
        eof = true;
        signalGeneration();
        joinWorkersUninterruptibly();
        checkFailure();
        try {
            mergeAcceptedRunsWithCredit();
        } catch (Throwable failure) {
            fail(failure);
            throwFailure(failure);
        }
        sorted = true;
        lifecycle = Lifecycle.EOF;
    }

    private void mergeAcceptedRunsWithCredit() {
        finalHeadReserved = ExternalSortMergePlanner.prepareFinalRuns(
                result, memoryManager, maxFrameSize, finalHeadReserved);
        auxReserved = Math.addExact(comparatorReserved, finalHeadReserved);
        if (result.acceptedRunCount() == 0) {
            releaseComparatorCredit();
        }
    }

    private synchronized void releaseComparatorCredit() {
        if (comparatorReserved == 0) {
            return;
        }
        long bytes = comparatorReserved;
        memoryManager.releaseExact(bytes);
        comparatorReserved = 0;
        auxReserved -= bytes;
        signalGeneration();
    }

    @Override
    public long size() {
        return totalRows;
    }

    @Override
    public synchronized Iterator<DataRow> iterator() {
        checkFailure();
        if (!sorted) {
            throw new IllegalStateException("iterate before sort()");
        }
        if (iteratorOpened) {
            throw new IllegalStateException("sort buffer iterator already opened");
        }
        iteratorOpened = true;
        Iterator<DataRow> delegate;
        try {
            delegate = result.iterateSorted();
        } catch (RuntimeException | Error failure) {
            fail(failure);
            try {
                releaseAuxOnce();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        return new Iterator<DataRow>() {
            @Override
            public boolean hasNext() {
                requireReadable();
                boolean has = delegate.hasNext();
                if (!has) {
                    releaseAuxOnce();
                }
                return has;
            }

            @Override
            public DataRow next() {
                requireReadable();
                if (!delegate.hasNext()) {
                    releaseAuxOnce();
                    throw new NoSuchElementException();
                }
                DataRow row = delegate.next();
                if (!delegate.hasNext()) {
                    releaseAuxOnce();
                }
                return row;
            }
        };
    }

    private void requireReadable() {
        checkFailure();
        if (lifecycle == Lifecycle.CLOSING || lifecycle == Lifecycle.CLOSED) {
            throw new IllegalStateException("sort buffer is closed");
        }
    }

    private void requireWritable() {
        checkFailure();
        if (lifecycle != Lifecycle.RUNNING || eof) {
            throw new IllegalStateException("sort buffer is not writable: " + lifecycle);
        }
    }

    private void fail(Throwable failure) {
        if (failure != null && terminalFailure.compareAndSet(null, failure)) {
            Thread self = Thread.currentThread();
            if (sortThread != null && sortThread != self) {
                sortThread.interrupt();
            }
            if (spillThread != null && spillThread != self) {
                spillThread.interrupt();
            }
            signalGeneration();
        }
    }

    @Override
    public void cancel(Throwable cause) {
        fail(cause == null ? new CancellationException("sort buffer cancelled") : cause);
    }

    private void checkFailure() {
        Throwable failure = terminalFailure.get();
        if (failure != null) {
            throwFailure(failure);
        }
    }

    private static void throwFailure(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    private void joinWorkersUninterruptibly() {
        boolean interrupted = joinUninterruptibly(sortThread) | joinUninterruptibly(spillThread);
        if (interrupted) {
            RuntimeException root = new RuntimeException("interrupted awaiting sort pipeline");
            fail(root);
            Thread.currentThread().interrupt();
            checkFailure();
        }
    }

    private static boolean joinUninterruptibly(Thread thread) {
        boolean interrupted = false;
        while (thread != null && thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        return interrupted;
    }

    private void releaseToken(BatchCreditToken token, TokenState expected, boolean byClose) {
        TokenState claimed = byClose ? TokenState.CLOSE_CLAIMED : TokenState.RELEASE_CLAIMED;
        if (!token.state.compareAndSet(expected, claimed)) {
            return;
        }
        try {
            memoryManager.releaseExact(token.bytes);
            token.state.set(byClose ? TokenState.RELEASED_BY_CLOSE : TokenState.RELEASED);
            activeTokens.remove(token);
        } catch (RuntimeException | Error releaseFailure) {
            token.state.set(TokenState.RELEASE_FAILED);
            fail(releaseFailure);
        } finally {
            signalGeneration();
        }
    }

    private synchronized void releaseAuxOnce() {
        if (auxReleased || auxReserved == 0) {
            auxReleased = true;
            return;
        }
        long bytes = auxReserved;
        memoryManager.releaseExact(bytes);
        auxReserved = 0;
        comparatorReserved = 0;
        finalHeadReserved = 0;
        auxReleased = true;
        signalGeneration();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        Lifecycle before = lifecycle;
        lifecycle = Lifecycle.CLOSING;
        eof = true;
        if (before == Lifecycle.RUNNING && !sorted) {
            terminalFailure.compareAndSet(null, new CancellationException("sort buffer closed"));
        }
        sortThread.interrupt();
        spillThread.interrupt();
        signalGeneration();
        joinUninterruptibly(sortThread);
        joinUninterruptibly(spillThread);

        Throwable cleanupFailure = null;
        for (EncodedSortBatch store : allStores) {
            try {
                store.discardAndTrim();
            } catch (RuntimeException | Error cleanup) {
                cleanupFailure = appendFailure(cleanupFailure, cleanup);
            }
        }
        try {
            result.disposeAll();
        } catch (RuntimeException | Error cleanup) {
            cleanupFailure = appendFailure(cleanupFailure, cleanup);
        }
        try {
            spillStore.close();
        } catch (RuntimeException | Error cleanup) {
            cleanupFailure = appendFailure(cleanupFailure, cleanup);
        }
        for (BatchCreditToken token : activeTokens.toArray(new BatchCreditToken[0])) {
            TokenState state = token.state.get();
            if (state == TokenState.HELD_CURRENT || state == TokenState.HELD_SORT_Q
                    || state == TokenState.HELD_SPILL_Q) {
                releaseToken(token, state, true);
            }
        }
        try {
            releaseAuxOnce();
        } catch (RuntimeException | Error cleanup) {
            cleanupFailure = appendFailure(cleanupFailure, cleanup);
        }
        sortQ.clear();
        spillQ.clear();
        emptyQ.clear();
        lifecycle = Lifecycle.CLOSED;
        if (cleanupFailure != null && terminalFailure.get() == null) {
            throwFailure(cleanupFailure);
        }
        Throwable root = terminalFailure.get();
        if (root != null && cleanupFailure != null && root != cleanupFailure) {
            root.addSuppressed(cleanupFailure);
        }
    }

    private static Throwable appendFailure(Throwable primary, Throwable cleanup) {
        if (primary == null) {
            return cleanup;
        }
        if (primary != cleanup) {
            primary.addSuppressed(cleanup);
        }
        return primary;
    }

    private void cleanupConstructionFailure(Throwable primary) {
        for (EncodedSortBatch store : allStores) {
            try {
                store.discardAndTrim();
            } catch (RuntimeException | Error cleanup) {
                primary.addSuppressed(cleanup);
            }
        }
        try {
            spillStore.close();
        } catch (RuntimeException | Error cleanup) {
            primary.addSuppressed(cleanup);
        }
    }

    private void signalGeneration() {
        synchronized (generationMonitor) {
            generationMonitor.notifyAll();
        }
    }

    boolean workerThreadsAliveForTest() {
        return sortThread.isAlive() || spillThread.isAlive();
    }

    int activeTokenCountForTest() {
        return activeTokens.size();
    }

    private enum Lifecycle { RUNNING, EOF, CLOSING, CLOSED }

    private enum TokenState {
        HELD_CURRENT, HELD_SORT_Q, HELD_SPILL_Q,
        RELEASE_CLAIMED, CLOSE_CLAIMED, RELEASED, RELEASED_BY_CLOSE, RELEASE_FAILED
    }

    private static final class BatchCreditToken {
        private final AtomicReference<TokenState> state = new AtomicReference<>(TokenState.HELD_CURRENT);
        private long bytes;

        void addBytes(long delta) {
            bytes = Math.addExact(bytes, delta);
        }
    }

    private static final class Batch {
        final EncodedSortBatch store;
        final BatchCreditToken token = new BatchCreditToken();
        int[] order;

        Batch(EncodedSortBatch store) {
            this.store = store;
        }
    }

}
