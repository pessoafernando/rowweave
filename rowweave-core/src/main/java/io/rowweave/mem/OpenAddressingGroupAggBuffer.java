package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.mem.seg.MemorySegment;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Byte-key open-addressing group aggregation buffer, replacing the
 * {@code LinkedHashMap<KeyWrapper,Entry>} of {@link SpillingSegmentGroupAggBuffer}, which cost three heap
 * objects per group (KeyWrapper, Entry, Node). The build-phase map becomes an **open-addressing hash table**:
 * a slot array (an on-heap {@code int[]} holding entryIndex, with linear probing) plus parallel arrays per
 * entry, where the key lands in an off-heap segment and the accumulator stays in an on-heap {@code Object[]}.
 * That removes the per-group heap objects and the chained pointer hops, which keeps the access cache friendly.
 *
 * <p>The no-spill path keeps first-seen group order by increasing entry index; the spill path emits stably by
 * hash partition and DFS sub-partition, so repeated full iterations yield the same order and values, but no
 * global first-seen order is promised. {@link SpillingSegmentGroupAggBuffer} stays as a compatibility adapter
 * delegating to this implementation. Accumulators remain on the heap, so BigDecimal keeps full precision
 * without being pushed down, and scale 10 HALF_UP is applied once, only at the final output.
 *
 * <p>Memory-bound guarantee: when the slots fill, the contents drop into 16 disk partitions, each
 * (sub)partition is merged under budget, and going over budget re-partitions with a new salt. If the safe
 * depth is reached repeatedly and it still does not fit, {@link MemoryBudgetExceededException} is thrown
 * explicitly, and the merge never degrades into an unbudgeted one.
 *
 * <p><b>Threading:</b> one stage thread owns this buffer exclusively; concurrent calls are unsupported, as is
 * interleaving iteration with mutation. Production PlanOps inject a dedicated opMM per instance, and the
 * spill callback inside {@link MemoryManager#reserve(long)} happens synchronously on the same thread.
 */
public final class OpenAddressingGroupAggBuffer implements GroupAggBuffer, MemorySpillable {

    private static final int SPILL_PARTITIONS = 16;
    private static final long PER_GROUP_RESERVE = 64;
    private static final int DEFAULT_MAX_REPARTITION_DEPTH = 12;
    private static final int INITIAL_CAPACITY = 16; // power of two
    private static final float LOAD_FACTOR = 0.7f;

    private int recursiveRepartitions;

    private final Supplier<Object[]> accumulatorFactory;
    private final MemoryManager memoryManager;
    private final MemorySegmentPool pool;
    private final SpillStore spillStore;
    private final int maxRepartitionDepth;

    // open-addressing slot table: slots[i] = entryIndex+1 (0 = empty); linear probing on hash
    private int[] slots = new int[INITIAL_CAPACITY];
    private int mask = INITIAL_CAPACITY - 1;
    // per-entry parallel arrays (entry index = first-seen order)
    private MemorySegment[] keySegs = new MemorySegment[INITIAL_CAPACITY];
    private int[] keyLens = new int[INITIAL_CAPACITY];
    private int[] keyHashes = new int[INITIAL_CAPACITY];
    private Object[][] accs = new Object[INITIAL_CAPACITY][];
    private int entryCount;
    private long memReserved;

    private final SpillRun[] runs = new SpillRun[SPILL_PARTITIONS];
    private final Set<StreamingMergeIterator> activeIterators =
            Collections.newSetFromMap(new IdentityHashMap<StreamingMergeIterator, Boolean>());
    private boolean anySpilled;
    private Throwable terminalFailure;
    private boolean spillStoreClosed;
    private boolean closed;

    public OpenAddressingGroupAggBuffer(DataSchema dataSchema, String[] keyFields,
                                        Supplier<Object[]> accumulatorFactory, MemoryManager memoryManager,
                                        MemorySegmentPool pool) {
        this(dataSchema, keyFields, accumulatorFactory, memoryManager, pool, new MemorySpillStore(),
                DEFAULT_MAX_REPARTITION_DEPTH);
    }

    public OpenAddressingGroupAggBuffer(DataSchema dataSchema, String[] keyFields,
                                        Supplier<Object[]> accumulatorFactory, MemoryManager memoryManager,
                                        MemorySegmentPool pool, SpillStore spillStore) {
        this(dataSchema, keyFields, accumulatorFactory, memoryManager, pool, spillStore,
                DEFAULT_MAX_REPARTITION_DEPTH);
    }

    /** Seam for same-package tests to shrink the repartition depth; the public production constructor always uses 12. */
    OpenAddressingGroupAggBuffer(DataSchema dataSchema, String[] keyFields,
                                 Supplier<Object[]> accumulatorFactory, MemoryManager memoryManager,
                                 MemorySegmentPool pool, SpillStore spillStore, int maxRepartitionDepth) {
        if (maxRepartitionDepth < 0) {
            throw new IllegalArgumentException("maxRepartitionDepth must be >= 0");
        }
        this.accumulatorFactory = accumulatorFactory;
        this.memoryManager = memoryManager;
        this.pool = pool;
        this.spillStore = spillStore;
        this.maxRepartitionDepth = maxRepartitionDepth;
        memoryManager.registerSpillable(this);
    }

    @Override
    public Object[] getOrCreate(byte[] keyBytes) {
        ensureUsable();
        int hash = Arrays.hashCode(keyBytes); // same hash as KeyWrapper -> identical spill partition routing
        int slot = hash & mask;
        while (slots[slot] != 0) {
            int entry = slots[slot] - 1;
            if (keyHashes[entry] == hash && keyEquals(entry, keyBytes)) {
                return accs[entry];
            }
            slot = (slot + 1) & mask;
        }
        // not found -> reserve, prepare, publish . MemoryManager.reserve synchronously invokes
        // this.spill() when needed, so it is the single spill arbitration + metric path.
        if (!memoryManager.tryReserve(PER_GROUP_RESERVE)) {
            memoryManager.reserve(PER_GROUP_RESERVE);
        }
        MemorySegment candidateSegment = null;
        try {
            int entry = entryCount;
            ensureEntryCapacity(entry + 1);
            // resize/rehash can allocate and throw; finish it before the candidate entry becomes visible.
            if (entry + 1 > (int) (slots.length * LOAD_FACTOR)) {
                resize();
            }
            // reserve() may have synchronously spilled and reset the table; always recompute against current mask.
            slot = hash & mask;
            while (slots[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            candidateSegment = pool.allocate(Math.max(1, keyBytes.length));
            candidateSegment.putBytes(0, keyBytes);
            Object[] candidateAccumulators = accumulatorFactory.get();
            long nextReserved = Math.addExact(memReserved, PER_GROUP_RESERVE);

            // Publish section: field/array assignments only; no allocation, callbacks, resize or accounting calls.
            keySegs[entry] = candidateSegment;
            keyLens[entry] = keyBytes.length;
            keyHashes[entry] = hash;
            accs[entry] = candidateAccumulators;
            entryCount = entry + 1;
            slots[slot] = entry + 1;
            memReserved = nextReserved;
            return candidateAccumulators;
        } catch (RuntimeException | Error failure) {
            boolean cleanupFailed = false;
            if (candidateSegment != null) {
                MemorySegment toFree = candidateSegment;
                candidateSegment = null;
                try {
                    pool.free(toFree);
                } catch (RuntimeException | Error cleanupFailure) {
                    addSuppressed(failure, cleanupFailure);
                    cleanupFailed = true;
                }
            }
            try {
                memoryManager.releaseExact(PER_GROUP_RESERVE);
            } catch (RuntimeException | Error cleanupFailure) {
                addSuppressed(failure, cleanupFailure);
                cleanupFailed = true;
            }
            if (cleanupFailed) {
                failStop(failure);
            }
            throw failure;
        }
    }

    private boolean keyEquals(int entry, byte[] keyBytes) {
        int len = keyLens[entry];
        if (len != keyBytes.length) {
            return false;
        }
        // Byte-by-byte comparison with no allocation: getBytes allocated a fresh byte[] on every probe, which showed up as Young GC pressure on high-cardinality grouping.
        MemorySegment seg = keySegs[entry];
        for (int i = 0; i < len; i++) {
            if (seg.getByte(i) != keyBytes[i]) {
                return false;
            }
        }
        return true;
    }

    private void ensureEntryCapacity(int n) {
        if (n > keySegs.length) {
            int cap = keySegs.length * 2;
            MemorySegment[] enlargedKeySegs = Arrays.copyOf(keySegs, cap);
            int[] enlargedKeyLens = Arrays.copyOf(keyLens, cap);
            int[] enlargedKeyHashes = Arrays.copyOf(keyHashes, cap);
            Object[][] enlargedAccs = Arrays.copyOf(accs, cap);
            // Publish all parallel arrays only after every allocation/copy succeeded.
            keySegs = enlargedKeySegs;
            keyLens = enlargedKeyLens;
            keyHashes = enlargedKeyHashes;
            accs = enlargedAccs;
        }
    }

    /** Doubles the slot array and rehashes: only the int[] slot index is rearranged, key segments and accumulators stay where they are. */
    private void resize() {
        int newCap = slots.length * 2;
        int[] newSlots = new int[newCap];
        int newMask = newCap - 1;
        for (int e = 0; e < entryCount; e++) {
            int slot = keyHashes[e] & newMask;
            while (newSlots[slot] != 0) {
                slot = (slot + 1) & newMask;
            }
            newSlots[slot] = e + 1;
        }
        slots = newSlots;
        mask = newMask;
    }

    /** Spill the in-memory table: append every group to a hash-partitioned SpillRun (key plus accumulator states), then release the segments and the budget and clear the table. */
    @Override
    public long spill() {
        ensureUsable();
        if (entryCount == 0) {
            return 0;
        }
        try {
            // Phase 1: append the complete batch. No build segment is freed before every frame is durable in its run.
            for (int e = 0; e < entryCount; e++) {
                byte[] keyBytes = keySegs[e].getBytes(0, keyLens[e]);
                int p = Math.floorMod(keyHashes[e], SPILL_PARTITIONS);
                if (runs[p] == null) {
                    runs[p] = spillStore.createRun();
                }
                runs[p].append(encodeFrame(keyBytes, serializeStates(accs[e])));
            }

            // Phase 2: the batch is complete; detach each owner before its one direct free attempt.
            for (int e = 0; e < entryCount; e++) {
                MemorySegment segment = keySegs[e];
                keySegs[e] = null;
                pool.free(segment);
            }
            long freed = memReserved;
            memReserved = 0;
            memoryManager.releaseExact(freed);
            resetTable();
            anySpilled = true;
            return freed;
        } catch (RuntimeException | Error failure) {
            failStop(failure);
            return 0; // unreachable: failStop always rethrows the terminal root
        }
    }

    private void resetTable() {
        int[] emptySlots = new int[INITIAL_CAPACITY];
        MemorySegment[] emptyKeySegs = new MemorySegment[INITIAL_CAPACITY];
        int[] emptyKeyLens = new int[INITIAL_CAPACITY];
        int[] emptyKeyHashes = new int[INITIAL_CAPACITY];
        Object[][] emptyAccs = new Object[INITIAL_CAPACITY][];
        slots = emptySlots;
        keySegs = emptyKeySegs;
        keyLens = emptyKeyLens;
        keyHashes = emptyKeyHashes;
        accs = emptyAccs;
        mask = INITIAL_CAPACITY - 1;
        entryCount = 0;
    }

    @Override
    public long groupCount() {
        ensureUsable();
        // Counting: without a spill this is entryCount directly; with a spill the streaming iterator counts, so nothing is materialized into a List.
        if (!anySpilled) {
            return entryCount;
        }
        long n = 0;
        for (Iterator<Group> it = iterator(); it.hasNext(); it.next()) {
            n++;
        }
        return n;
    }

    @Override
    public Iterator<Group> iterator() {
        ensureUsable();
        // Streaming Iterator rather than a materialized List, which removes the O(total groups) heap residency at high cardinality.
        // Without a spill the table slots are walked directly, and the walk is re-entrant. With a spill an explicit stack reproduces the DFS pre-order of mergeRun over partitions 0..15.
        if (!anySpilled) {
            return new Iterator<Group>() {
                private int e = 0;
                @Override public boolean hasNext() {
                    ensureUsable();
                    return e < entryCount;
                }
                @Override public Group next() {
                    ensureUsable();
                    if (e >= entryCount) {
                        throw new java.util.NoSuchElementException();
                    }
                    // A fresh SpillGroup per key with keyBytes copied, because the table segment is freed once iteration ends and no reference may outlive it.
                    Group g = new SpillGroup(keySegs[e].getBytes(0, keyLens[e]), accs[e]);
                    e++;
                    return g;
                }
            };
        }
        spill(); // flushes the rest of the table into hash partitions; all data now lives in runs[]
        return new StreamingMergeIterator();
    }

    /**
     * Streaming merge iterator: DFS pre-order matches the recursive pre-order of a full merge, so each group yields byte-identical output.
     * An explicit stack reproduces that pre-order: partitions 0..15 are pushed in reverse so 0 pops first, and each frame merges lazily into one LinkedHashMap
     * bounded by the budget, yielding group by group. A frame that overflows is re-partitioned and its subs pushed in reverse to keep the 0..15 pre-order. Only one (sub)partition is resident at a time, so high cardinality does not OOM.
     */
    private final class StreamingMergeIterator implements Iterator<Group> {
        private final java.util.ArrayDeque<Frame2> stack = new java.util.ArrayDeque<>();
        private Iterator<Map.Entry<KeyWrapper, Object[]>> curGroups; // group iterator merged out of the frame being emitted
        private long curReserved;                                    // budget reserved for the current frame, released once it is fully emitted
        private Group nextGroup;
        private RuntimeException iteratorFailure;

        StreamingMergeIterator() {
            // Push partitions 0..15 in reverse so they pop in that order, matching the for p=0..15 loop of the materialized path.
            // runs[] is not nulled, because SpillRun.read is re-entrant: iterator() can be called repeatedly and re-merges each time, which matches a full materialization.
            activeIterators.add(this);
            try {
                for (int p = SPILL_PARTITIONS - 1; p >= 0; p--) {
                    if (runs[p] != null) {
                        runs[p].finishWriting();
                        stack.push(new Frame2(runs[p], 0, 0));
                    }
                }
            } catch (RuntimeException | Error failure) {
                failStop(failure);
            }
            advance();
        }

        /** Advance to the next group that can be emitted: when the current frame is exhausted, the next frame is merged or re-partitioned onto the stack. */
        private void advance() {
            ensureIteratorUsable();
            try {
                advanceInternal();
            } catch (GroupMergeDepthExceededException depthFailure) {
                MemoryBudgetExceededException exposed = new MemoryBudgetExceededException(
                        depthFailure.getMessage());
                exposed.initCause(depthFailure);
                iteratorFailure = exposed;
                throw exposed;
            } catch (RuntimeException | Error failure) {
                releaseCurrentMergeReservation(failure);
                curGroups = null;
                nextGroup = null;
                stack.clear();
                failStop(failure);
            }
        }

        private void advanceInternal() {
            while (true) {
                if (curGroups != null && curGroups.hasNext()) {
                    Map.Entry<KeyWrapper, Object[]> e = curGroups.next();
                    nextGroup = new SpillGroup(e.getKey().bytes, e.getValue());
                    return;
                }
                // The current frame is fully emitted, so release its budget.
                if (curReserved > 0) {
                    memoryManager.releaseExact(curReserved);
                    curReserved = 0;
                }
                curGroups = null;
                if (stack.isEmpty()) {
                    nextGroup = null;
                    activeIterators.remove(this);
                    return;
                }
                mergeFrame(stack.pop());
            }
        }

        /** Merge one frame: a frame that fits points curGroups at its map and yields group by group; one that overflows is released, re-partitioned, and the subs pushed in reverse so advance continues in order. */
        private void mergeFrame(Frame2 frame) {
            Map<KeyWrapper, Object[]> map = new LinkedHashMap<>();
            long reserved = 0;
            boolean reservationTransferred = false;
            Throwable mergeFailure = null;
            try {
                boolean overflow = false;
                Iterator<byte[]> reader = frame.run.read();
                Throwable readFailure = null;
                try {
                    while (reader.hasNext()) {
                        Frame f = decodeFrame(reader.next());
                        KeyWrapper kw = new KeyWrapper(f.keyBytes);
                        Object[] target = map.get(kw);
                        if (target == null) {
                            if (!memoryManager.tryReserve(PER_GROUP_RESERVE)) {
                                overflow = true;
                                break;
                            }
                            reserved += PER_GROUP_RESERVE;
                            target = accumulatorFactory.get();
                            map.put(kw, target);
                        }
                        mergeStatesInto(target, f.states);
                    }
                } catch (RuntimeException | Error failure) {
                    readFailure = failure;
                    throw failure;
                } finally {
                    closeIterator(reader, readFailure);
                }

                if (!overflow) {
                    disposeConsumedScratch(frame);
                    curGroups = map.entrySet().iterator();
                    curReserved = reserved;
                    reservationTransferred = true;
                    return;
                }

                memoryManager.releaseExact(reserved);
                reserved = 0;
                if (frame.depth >= maxRepartitionDepth) {
                    throw new GroupMergeDepthExceededException(
                            "group aggregation spill merge exceeded max repartition depth "
                                    + maxRepartitionDepth + " with budget=" + memoryManager.budget()
                                    + "; refusing unbounded merge");
                }

                recursiveRepartitions++;
                int nextSalt = frame.salt + 1;
                SpillRun[] subs = new SpillRun[SPILL_PARTITIONS];
                try {
                    Iterator<byte[]> repartitionReader = frame.run.read();
                    Throwable readFailure2 = null;
                    try {
                        while (repartitionReader.hasNext()) {
                            byte[] frameBytes = repartitionReader.next();
                            Frame f = decodeFrame(frameBytes);
                            int sp = Math.floorMod(saltedHash(f.keyBytes, nextSalt), SPILL_PARTITIONS);
                            if (subs[sp] == null) {
                                subs[sp] = spillStore.createRun();
                            }
                            subs[sp].append(frameBytes);
                        }
                    } catch (RuntimeException | Error failure) {
                        readFailure2 = failure;
                        throw failure;
                    } finally {
                        closeIterator(repartitionReader, readFailure2);
                    }
                    for (SpillRun sub : subs) {
                        if (sub != null) {
                            sub.finishWriting();
                        }
                    }
                    disposeConsumedScratch(frame);
                } catch (RuntimeException | Error failure) {
                    abortRuns(subs, failure);
                    throw failure;
                }
                for (int i = SPILL_PARTITIONS - 1; i >= 0; i--) {
                    if (subs[i] != null) {
                        stack.push(new Frame2(subs[i], frame.depth + 1, nextSalt));
                    }
                }
                curGroups = null;
            } catch (RuntimeException | Error failure) {
                mergeFailure = failure;
                throw failure;
            } finally {
                if (!reservationTransferred && reserved > 0) {
                    try {
                        memoryManager.releaseExact(reserved);
                    } catch (RuntimeException | Error cleanupFailure) {
                        if (mergeFailure != null) {
                            addSuppressed(mergeFailure, cleanupFailure);
                        } else {
                            throw cleanupFailure;
                        }
                    }
                }
            }
        }

        /** Root runs remain replayable; descendants are one-iterator scratch after consumption. */
        private void disposeConsumedScratch(Frame2 frame) {
            if (frame.depth > 0) {
                frame.run.dispose();
            }
        }

        private void releaseCurrentMergeReservation(Throwable primary) {
            long reserved = curReserved;
            curReserved = 0;
            if (reserved == 0) {
                return;
            }
            try {
                memoryManager.releaseExact(reserved);
            } catch (RuntimeException | Error cleanupFailure) {
                addSuppressed(primary, cleanupFailure);
            }
        }

        private void ensureIteratorUsable() {
            ensureUsable();
            if (iteratorFailure != null) {
                throw iteratorFailure;
            }
        }

        @Override public boolean hasNext() {
            ensureIteratorUsable();
            return nextGroup != null;
        }

        @Override public Group next() {
            ensureIteratorUsable();
            if (nextGroup == null) {
                throw new java.util.NoSuchElementException();
            }
            Group g = nextGroup;
            advance();
            return g;
        }

        /** Abandons frames that were never produced once the owner closes or turns terminal, and returns the current merge reservation. */
        private void abandon() {
            long reserved = curReserved;
            curReserved = 0;
            curGroups = null;
            nextGroup = null;
            stack.clear();
            activeIterators.remove(this);
            if (reserved > 0) {
                memoryManager.releaseExact(reserved);
            }
        }
    }

    /** Only an explicit depth exhaustion may surface as a budget failure on a non-terminal path. */
    private static final class GroupMergeDepthExceededException extends RuntimeException {
        GroupMergeDepthExceededException(String message) {
            super(message);
        }
    }

    /** One frame of the streaming merge stack: a (sub)partition run awaiting merge plus its recursion depth and salt. */
    private static final class Frame2 {
        final SpillRun run;
        final int depth;
        final int salt;
        Frame2(SpillRun run, int depth, int salt) {
            this.run = run;
            this.depth = depth;
            this.salt = salt;
        }
    }

    public int recursiveRepartitions() {
        return recursiveRepartitions;
    }

    private void ensureUsable() {
        if (terminalFailure != null) {
            rethrow(terminalFailure);
        }
        if (closed) {
            throw new IllegalStateException("GroupAggBuffer is closed");
        }
    }

    /** Success, cancellation and downstream failure all reach here from the operator's finally block; the cleanup covers everything and is idempotent. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        memoryManager.unregisterSpillable(this);

        Throwable cleanupFailure = null;
        while (!activeIterators.isEmpty()) {
            StreamingMergeIterator iterator = activeIterators.iterator().next();
            try {
                iterator.abandon();
            } catch (RuntimeException | Error failure) {
                cleanupFailure = appendFailure(cleanupFailure, failure);
            }
        }

        if (!spillStoreClosed) {
            spillStoreClosed = true; // throwing close has unknown ownership; never retry it
            try {
                spillStore.close();
            } catch (RuntimeException | Error failure) {
                cleanupFailure = appendFailure(cleanupFailure, failure);
            }
        }
        Arrays.fill(runs, null);

        cleanupFailure = releaseResidentSegments(cleanupFailure);
        cleanupFailure = releaseResidentReservation(cleanupFailure);
        clearTableWithoutAllocation();
        anySpilled = false;

        if (cleanupFailure != null) {
            rethrow(cleanupFailure);
        }
    }

    /** The working set can no longer be retried safely, so the first cause is kept and every exclusively owned resource is cleaned up on a best-effort basis. */
    private void failStop(Throwable primary) {
        if (terminalFailure != null) {
            rethrow(terminalFailure);
        }
        terminalFailure = primary;

        while (!activeIterators.isEmpty()) {
            StreamingMergeIterator iterator = activeIterators.iterator().next();
            try {
                iterator.abandon();
            } catch (RuntimeException | Error cleanupFailure) {
                addSuppressed(primary, cleanupFailure);
            }
        }

        for (int i = 0; i < runs.length; i++) {
            SpillRun run = runs[i];
            runs[i] = null;
            if (run == null) {
                continue;
            }
            try {
                run.abort();
            } catch (RuntimeException | Error cleanupFailure) {
                addSuppressed(primary, cleanupFailure);
            }
        }
        if (!spillStoreClosed) {
            spillStoreClosed = true;
            try {
                spillStore.close();
            } catch (RuntimeException | Error cleanupFailure) {
                addSuppressed(primary, cleanupFailure);
            }
        }

        releaseResidentSegments(primary);
        releaseResidentReservation(primary);
        clearTableWithoutAllocation();
        anySpilled = false;
        rethrow(primary);
    }

    /** Detach before free: once free throws, pool ownership is undefined, so the same segment cannot be attempted a second time. */
    private Throwable releaseResidentSegments(Throwable primary) {
        Throwable failure = primary;
        for (int i = 0; i < keySegs.length; i++) {
            MemorySegment segment = keySegs[i];
            keySegs[i] = null;
            if (segment == null) {
                continue;
            }
            try {
                pool.free(segment);
            } catch (RuntimeException | Error cleanupFailure) {
                failure = appendFailure(failure, cleanupFailure);
            }
        }
        return failure;
    }

    private Throwable releaseResidentReservation(Throwable primary) {
        Throwable failure = primary;
        long reservedToRelease = memReserved;
        memReserved = 0;
        if (reservedToRelease > 0) {
            try {
                memoryManager.releaseExact(reservedToRelease);
            } catch (RuntimeException | Error cleanupFailure) {
                failure = appendFailure(failure, cleanupFailure);
            }
        }
        return failure;
    }

    private void clearTableWithoutAllocation() {
        Arrays.fill(slots, 0);
        Arrays.fill(keyLens, 0);
        Arrays.fill(keyHashes, 0);
        Arrays.fill(accs, null);
        entryCount = 0;
    }

    private static Throwable appendFailure(Throwable primary, Throwable cleanupFailure) {
        if (primary == null) {
            return cleanupFailure;
        }
        addSuppressed(primary, cleanupFailure);
        return primary;
    }

    private static void addSuppressed(Throwable primary, Throwable cleanupFailure) {
        if (cleanupFailure == primary) {
            return;
        }
        for (Throwable existing : primary.getSuppressed()) {
            if (existing == cleanupFailure) {
                return;
            }
        }
        primary.addSuppressed(cleanupFailure);
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IllegalStateException("unexpected checked terminal failure", failure);
    }

    private static void closeIterator(Iterator<?> iterator, Throwable primary) {
        if (!(iterator instanceof AutoCloseable)) {
            return;
        }
        try {
            ((AutoCloseable) iterator).close();
        } catch (Throwable closeFailure) {
            if (primary != null) {
                primary.addSuppressed(closeFailure);
            } else {
                throw new RuntimeException("failed to close group spill reader", closeFailure);
            }
        }
    }

    private static void abortRuns(SpillRun[] runs, Throwable primary) {
        for (SpillRun run : runs) {
            if (run == null) {
                continue;
            }
            try {
                run.abort();
            } catch (Throwable abortFailure) {
                primary.addSuppressed(abortFailure);
            }
        }
    }

    private static int saltedHash(byte[] key, int salt) {
        int h = 0x811C9DC5 ^ (salt * 0x9E3779B1);
        for (byte b : key) {
            h = (h ^ (b & 0xFF)) * 0x01000193;
        }
        h ^= (h >>> 15);
        h *= 0x85EBCA77;
        h ^= (h >>> 13);
        return h;
    }

    // frame layout: [keyLen int][keyBytes][stateCount int]( [stateLen int][stateBytes] )*
    private byte[] encodeFrame(byte[] keyBytes, byte[][] states) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(keyBytes.length);
            out.write(keyBytes);
            out.writeInt(states.length);
            for (byte[] s : states) {
                out.writeInt(s.length);
                out.write(s);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    private Frame decodeFrame(byte[] frame) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame))) {
            int keyLen = in.readInt();
            byte[] keyBytes = new byte[keyLen];
            in.readFully(keyBytes);
            int n = in.readInt();
            byte[][] states = new byte[n][];
            for (int i = 0; i < n; i++) {
                int len = in.readInt();
                byte[] s = new byte[len];
                in.readFully(s);
                states[i] = s;
            }
            return new Frame(keyBytes, states);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private byte[][] serializeStates(Object[] accumulators) {
        byte[][] states = new byte[accumulators.length][];
        for (int i = 0; i < accumulators.length; i++) {
            states[i] = ((Accumulator) accumulators[i]).writeState();
        }
        return states;
    }

    private void mergeStatesInto(Object[] target, byte[][] states) {
        for (int i = 0; i < target.length; i++) {
            ((Accumulator) target[i]).mergeState(states[i]);
        }
    }

    private static final class Frame {
        final byte[] keyBytes;
        final byte[][] states;

        Frame(byte[] keyBytes, byte[][] states) {
            this.keyBytes = keyBytes;
            this.states = states;
        }
    }

    private static final class SpillGroup implements Group {
        private final byte[] keyBytes;
        private final Object[] accumulators;

        SpillGroup(byte[] keyBytes, Object[] accumulators) {
            this.keyBytes = keyBytes;
            this.accumulators = accumulators;
        }

        @Override
        public byte[] keyBytes() {
            return keyBytes;
        }

        @Override
        public Object[] accumulators() {
            return accumulators;
        }
    }

    private static final class KeyWrapper {
        final byte[] bytes;
        private final int hash;

        KeyWrapper(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof KeyWrapper)) {
                return false;
            }
            return Arrays.equals(bytes, ((KeyWrapper) o).bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
