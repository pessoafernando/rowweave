package io.rowweave.ops.join;

import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryBudgetExceededException;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.RowSerializer;
import io.rowweave.mem.RowSizeEstimator;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegment;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.BoundedReplayRun;
import io.rowweave.mem.spill.BoundedReplayStore;
import io.rowweave.mem.spill.BudgetedFrameReader;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;

/**
  * Managed HashJoin. Build payload stays in segments owned by the pool and the index is primitive
  * arrays; probe walks the chain for one row and emits the result directly, holding no output
  * collection. An explicit HASH builds the right side by default, while AUTO may build the left.
 */
public final class HashJoin {

    public enum BuildSide { LEFT, RIGHT }

    private static final int INITIAL_NODE_CAPACITY = 8;
    private static final int INITIAL_BUCKET_CAPACITY = 16;
    private static final int NOT_INDEXED = -2;
    private static final long REPLAY_WRITER_IO_CREDIT =
            512L + RowSizeEstimator.estimateByteArray(1024);

    private final DataSchema leftMeta;
    private final DataSchema rightMeta;
    private final int[] leftKeyIndices;
    private final int[] rightKeyIndices;
    private final JoinType joinType;
    private final DataSchema outputMeta;
    private final RowSerializer leftSerializer;
    private final RowSerializer rightSerializer;

    private MemoryManager memoryManager;
    private SpillStore spillStore;
    private MemorySegmentPool segmentPool;
    private boolean ownsPool;
    private BuildSide buildSide = BuildSide.RIGHT;
    private boolean fellBackToSortMerge;
    private boolean invoked;

    private HashJoin(DataSchema leftMeta, DataSchema rightMeta,
                     int[] leftKeyIndices, int[] rightKeyIndices, JoinType joinType) {
        this.leftMeta = leftMeta;
        this.rightMeta = rightMeta;
        this.leftKeyIndices = leftKeyIndices;
        this.rightKeyIndices = rightKeyIndices;
        this.joinType = Objects.requireNonNull(joinType, "joinType");
        this.outputMeta = buildOutputMeta(leftMeta, rightMeta);
        this.leftSerializer = new RowSerializer(leftMeta);
        this.rightSerializer = new RowSerializer(rightMeta);
    }

    public static HashJoin of(DataSchema leftMeta, DataSchema rightMeta,
                              String[] leftKeyFields, String[] rightKeyFields, JoinType joinType) {
        if (leftKeyFields.length != rightKeyFields.length || leftKeyFields.length == 0) {
            throw new IllegalArgumentException("join key field counts must match and be non-empty");
        }
        int[] leftIndices = resolve(leftMeta, leftKeyFields);
        int[] rightIndices = resolve(rightMeta, rightKeyFields);
        KeySchemaValidator.validate("join", leftMeta, rightMeta, leftIndices, rightIndices);
        return new HashJoin(leftMeta, rightMeta, leftIndices, rightIndices, joinType);
    }

    /** Compatibility injection. Production runtime uses the three-argument overload and owns the shared pool. */
    public HashJoin withSpill(MemoryManager memory, SpillStore store) {
        return configure(memory, store, new HeapPool(), true);
    }

    public HashJoin withSpill(MemoryManager memory, SpillStore store, MemorySegmentPool pool) {
        return configure(memory, store, pool, false);
    }

    private HashJoin configure(MemoryManager memory, SpillStore store,
                               MemorySegmentPool pool, boolean ownPool) {
        if (invoked) throw new IllegalStateException("HashJoin already invoked");
        this.memoryManager = Objects.requireNonNull(memory, "memoryManager");
        this.spillStore = Objects.requireNonNull(store, "spillStore");
        this.segmentPool = Objects.requireNonNull(pool, "segmentPool");
        this.ownsPool = ownPool;
        return this;
    }

    public HashJoin withBuildSide(BuildSide side) {
        if (invoked) throw new IllegalStateException("HashJoin already invoked");
        this.buildSide = Objects.requireNonNull(side, "buildSide");
        return this;
    }

    public DataSchema getOutputSchema() {
        return outputMeta;
    }

    public void join(RowStream left, RowStream right, RowSink out) {
        validateBeforeInput();
        invoked = true;
        fellBackToSortMerge = false;

        boolean buildLeft = buildSide == BuildSide.LEFT;
        RowStream build = buildLeft ? left : right;
        RowStream probe = buildLeft ? right : left;
        DataSchema buildMeta = buildLeft ? leftMeta : rightMeta;
        int[] buildKeys = buildLeft ? leftKeyIndices : rightKeyIndices;
        int[] probeKeys = buildLeft ? rightKeyIndices : leftKeyIndices;
        RowSerializer buildSerializer = buildLeft ? leftSerializer : rightSerializer;
        boolean emitBuildOuter = buildLeft
                ? joinType == JoinType.LEFT || joinType == JoinType.FULL
                : joinType == JoinType.RIGHT || joinType == JoinType.FULL;
        boolean emitProbeOuter = buildLeft
                ? joinType == JoinType.RIGHT || joinType == JoinType.FULL
                : joinType == JoinType.LEFT || joinType == JoinType.FULL;

        ManagedBuildTable table = null;
        Throwable primary = null;
        try {
            table = new ManagedBuildTable(buildMeta, buildKeys, buildSerializer);
            BuildDrainResult buildResult = drainBuild(build, table, emitBuildOuter);
            if (buildResult.terminal != null) {
                out.emit(buildResult.terminal);
                return;
            }
            if (buildResult.overflowRow != null) {
                fallbackToSortMerge(left, right, build, table, buildResult.overflowRow,
                        buildSerializer, buildMeta, buildLeft, out);
                return;
            }
            StreamEvent terminal = drainProbe(probe, probeKeys, table, buildLeft, emitProbeOuter, out);
            if (terminal != null) {
                out.emit(terminal);
                return;
            }
            if (emitBuildOuter) emitUnmatchedBuild(table, buildLeft, out);
            out.endOfStream();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (RuntimeException | Error cleanup) {
                    if (primary != null) primary.addSuppressed(cleanup);
                    else throw cleanup;
                }
            }
            if (ownsPool) segmentPool.trim();
        }
    }

    private void validateBeforeInput() {
        if (invoked) throw new IllegalStateException("HashJoin is single-use");
        if (memoryManager == null || spillStore == null || segmentPool == null) {
            throw new IllegalStateException(
                    "HashJoin requires managed memory, spill store and segment pool before reading input");
        }
        if (!(spillStore instanceof BoundedReplayStore)) {
            throw new IllegalStateException("HashJoin requires a bounded disk replay store");
        }
    }

    private BuildDrainResult drainBuild(RowStream build, ManagedBuildTable table, boolean retainNullOuter) {
        StreamEvent event;
        while ((event = build.next()) != null) {
            if (!event.isRecord()) {
                if (event.isError() || event.isCancel()) return BuildDrainResult.terminal(event);
                continue;
            }
            DataRow row = event.row();
            boolean nullKey = hasNullKey(row, table.keyIndices);
            if (!nullKey || retainNullOuter) {
                try {
                    table.add(row, !nullKey);
                } catch (MemoryBudgetExceededException overflow) {
                    return BuildDrainResult.overflow(row);
                }
            }
        }
        return BuildDrainResult.complete();
    }

    private void fallbackToSortMerge(RowStream originalLeft, RowStream originalRight,
                                     RowStream buildTail, ManagedBuildTable table, DataRow overflowRow,
                                     RowSerializer buildSerializer, DataSchema buildMeta,
                                     boolean buildLeft, RowSink out) {
        fellBackToSortMerge = true;
        BoundedReplayRun buildRun = null;
        ReplayRowStream replay = null;
        MemoryManager[] sides = null;
        Throwable primary = null;
        long writerCredit = 0;
        int maximumFrameLength = 0;
        try {
            table.releaseReplayEscrow();
            reserveExact(REPLAY_WRITER_IO_CREDIT, "replay writer I/O");
            writerCredit = REPLAY_WRITER_IO_CREDIT;
            buildRun = ((BoundedReplayStore) spillStore).createRun();
            memoryManager.recordSelfSpill();
            maximumFrameLength = table.appendAll(buildRun);
            table.close();

            maximumFrameLength = Math.max(maximumFrameLength,
                    appendRow(buildRun, buildSerializer, overflowRow));
            StreamEvent event;
            while ((event = buildTail.next()) != null) {
                if (!event.isRecord()) {
                    if (event.isError() || event.isCancel()) {
                        out.emit(event);
                        buildRun.abort();
                        return;
                    }
                    continue;
                }
                maximumFrameLength = Math.max(maximumFrameLength,
                        appendRow(buildRun, buildSerializer, event.row()));
            }
            buildRun.finishWriting();
            memoryManager.releaseExact(writerCredit);
            writerCredit = 0;

            if (memoryManager.reservedBytes() != 0) {
                throw new IllegalStateException("HashJoin handoff root is not clean: reserved="
                        + memoryManager.reservedBytes());
            }
            sides = memoryManager.partitionEvenly(2);
            rebindReplay(originalLeft, sides[0]);
            rebindReplay(originalRight, sides[1]);
            MemoryManager replayMemory = buildLeft ? sides[0] : sides[1];
            replay = new ReplayRowStream(buildRun, buildSerializer, buildMeta.getFieldCount(),
                    maximumFrameLength, replayMemory);
            RowStream left = buildLeft ? replay : originalLeft;
            RowStream right = buildLeft ? originalRight : replay;

            SortMergeJoin.of(leftMeta, rightMeta,
                            keyNames(leftMeta, leftKeyIndices), keyNames(rightMeta, rightKeyIndices), joinType)
                    .withBorrowedSpill(sides[0], sides[1], spillStore, segmentPool)
                    .join(left, right, out);
            assertChildReleased(sides[0], "left");
            assertChildReleased(sides[1], "right");
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            Throwable cleanupFailure = null;
            if (writerCredit != 0) {
                try {
                    memoryManager.releaseExact(writerCredit);
                } catch (RuntimeException | Error cleanup) {
                    cleanupFailure = appendCleanup(cleanupFailure, cleanup);
                }
            }
            if (replay != null) {
                try {
                    replay.close();
                } catch (RuntimeException | Error cleanup) {
                    cleanupFailure = appendCleanup(cleanupFailure, cleanup);
                }
            }
            if (buildRun != null) {
                try {
                    buildRun.dispose();
                } catch (RuntimeException | Error cleanup) {
                    cleanupFailure = appendCleanup(cleanupFailure, cleanup);
                }
            }
            if (sides != null) {
                for (int i = 0; i < sides.length; i++) {
                    if (sides[i].reservedBytes() != 0) {
                        cleanupFailure = appendCleanup(cleanupFailure,
                                new IllegalStateException("HashJoin fallback leaked child " + i
                                        + " memory: " + sides[i].reservedBytes()));
                    }
                }
            }
            if (cleanupFailure != null) {
                if (primary != null) primary.addSuppressed(cleanupFailure);
                else if (cleanupFailure instanceof RuntimeException) throw (RuntimeException) cleanupFailure;
                else throw (Error) cleanupFailure;
            }
        }
    }

    boolean fellBackToSortMerge() {
        return fellBackToSortMerge;
    }

    private static Throwable appendCleanup(Throwable first, Throwable next) {
        if (first == null) return next;
        if (first != next) first.addSuppressed(next);
        return first;
    }

    private int appendRow(BoundedReplayRun run, RowSerializer serializer, DataRow row) {
        int length = serializer.serializedSize(row);
        long credit = Math.addExact(RowSizeEstimator.estimateByteArray(length),
                serializer.serializerScratchCredit(row));
        reserveExact(credit, "replay frame serialization");
        try {
            run.append(serializer.serialize(row));
            return length;
        } finally {
            memoryManager.releaseExact(credit);
        }
    }

    private static void assertChildReleased(MemoryManager child, String side) {
        if (child.reservedBytes() != 0) {
            throw new IllegalStateException("HashJoin fallback leaked " + side
                    + " child memory: " + child.reservedBytes());
        }
    }

    private static void rebindReplay(RowStream stream, MemoryManager child) {
        if (stream instanceof MemoryRebindableRowStream) {
            ((MemoryRebindableRowStream) stream).rebindMemory(child);
        }
    }

    private StreamEvent drainProbe(RowStream probe, int[] probeKeys, ManagedBuildTable table,
                                   boolean buildLeft, boolean emitProbeOuter, RowSink out) {
        StreamEvent event;
        while ((event = probe.next()) != null) {
            if (!event.isRecord()) {
                if (event.isError() || event.isCancel()) return event;
                continue;
            }
            DataRow probeRow = event.row();
            if (hasNullKey(probeRow, probeKeys)) {
                if (emitProbeOuter) emitJoined(buildLeft ? null : probeRow, buildLeft ? probeRow : null, out);
                continue;
            }

            long keyCredit = keyScratchCredit(probeRow, probeKeys);
            reserveExact(keyCredit, "probe key scratch");
            boolean found = false;
            try {
                int hash = hashKey(probeRow, probeKeys);
                for (int node = table.bucketHead(hash); node >= 0; node = table.next[node]) {
                    if (table.hashes[node] != hash) continue;
                    long decodeCredit = RowSizeEstimator.estimateEncodedUpperBound(
                            table.meta.getFieldCount(), table.lengths[node]);
                    reserveExact(decodeCredit, "build row decode");
                    try {
                        DataRow buildRow = table.decode(node);
                        if (!keysEqual(probeRow, probeKeys, buildRow, table.keyIndices)) continue;
                        found = true;
                        table.matched[node] = true;
                        emitJoined(buildLeft ? buildRow : probeRow,
                                buildLeft ? probeRow : buildRow, out);
                    } finally {
                        memoryManager.releaseExact(decodeCredit);
                    }
                }
            } finally {
                memoryManager.releaseExact(keyCredit);
            }
            if (!found && emitProbeOuter) {
                emitJoined(buildLeft ? null : probeRow, buildLeft ? probeRow : null, out);
            }
        }
        return null;
    }

    private void emitUnmatchedBuild(ManagedBuildTable table, boolean buildLeft, RowSink out) {
        for (int node = 0; node < table.size; node++) {
            if (table.matched[node]) continue;
            long decodeCredit = RowSizeEstimator.estimateEncodedUpperBound(
                    table.meta.getFieldCount(), table.lengths[node]);
            reserveExact(decodeCredit, "outer build decode");
            try {
                DataRow buildRow = table.decode(node);
                emitJoined(buildLeft ? buildRow : null, buildLeft ? null : buildRow, out);
            } finally {
                memoryManager.releaseExact(decodeCredit);
            }
        }
    }

    private void emitJoined(DataRow left, DataRow right, RowSink out) {
        long credit = RowSizeEstimator.estimateRowContainer(outputMeta.getFieldCount());
        reserveExact(credit, "join output container");
        try {
            int leftFields = leftMeta.getFieldCount();
            int rightFields = rightMeta.getFieldCount();
            DataRow joined = new DataRow(leftFields + rightFields);
            if (left != null) for (int i = 0; i < leftFields; i++) joined.set(i, left.get(i));
            if (right != null) for (int i = 0; i < rightFields; i++) joined.set(leftFields + i, right.get(i));
            out.emitRow(joined);
        } finally {
            memoryManager.releaseExact(credit);
        }
    }

    private void reserveExact(long bytes, String owner) {
        if (!memoryManager.tryReserve(bytes)) {
            throw new MemoryBudgetExceededException("HashJoin cannot reserve " + bytes
                    + " bytes for " + owner + ": reserved=" + memoryManager.reservedBytes()
                    + " budget=" + memoryManager.budget());
        }
    }

    private final class ManagedBuildTable implements AutoCloseable {
        private final DataSchema meta;
        private final int[] keyIndices;
        private final RowSerializer serializer;
        private MemorySegment[] segments;
        private int[] lengths;
        private int[] next;
        private int[] hashes;
        private boolean[] matched;
        private int[] buckets;
        private long arraysCredit;
        private long replayEscrowCredit;
        private int size;
        private boolean closed;

        ManagedBuildTable(DataSchema meta, int[] keyIndices, RowSerializer serializer) {
            this.meta = meta;
            this.keyIndices = keyIndices;
            this.serializer = serializer;
            allocateArrays(INITIAL_NODE_CAPACITY, INITIAL_BUCKET_CAPACITY);
            try {
                reserveExact(REPLAY_WRITER_IO_CREDIT, "hash fallback escrow");
                replayEscrowCredit = REPLAY_WRITER_IO_CREDIT;
            } catch (RuntimeException | Error failure) {
                close();
                throw failure;
            }
        }

        void add(DataRow row, boolean indexed) {
            int encodedLength = serializer.serializedSize(row);
            ensureReplayEscrow(encodedLength);
            ensureCapacity();
            long segmentCredit = RowSizeEstimator.estimateMemorySegment(encodedLength);
            reserveExact(segmentCredit, "encoded build payload");
            MemorySegment segment = null;
            boolean published = false;
            try {
                segment = segmentPool.allocate(encodedLength);
                long scratchCredit = serializer.serializerScratchCredit(row);
                reserveExact(scratchCredit, "build serialization scratch");
                try {
                    serializer.serializeInto(row, segment, 0);
                } finally {
                    memoryManager.releaseExact(scratchCredit);
                }

                int node = size;
                segments[node] = segment;
                lengths[node] = encodedLength;
                matched[node] = false;
                if (indexed) {
                    long keyCredit = keyScratchCredit(row, keyIndices);
                    reserveExact(keyCredit, "build key scratch");
                    try {
                        int hash = hashKey(row, keyIndices);
                        int bucket = bucketIndex(hash, buckets.length);
                        hashes[node] = hash;
                        next[node] = buckets[bucket];
                        buckets[bucket] = node;
                    } finally {
                        memoryManager.releaseExact(keyCredit);
                    }
                } else {
                    next[node] = NOT_INDEXED;
                }
                size++;
                published = true;
            } finally {
                if (!published) {
                    segments[size] = null;
                    if (segment != null) segmentPool.free(segment);
                    memoryManager.releaseExact(segmentCredit);
                }
            }
        }

        int bucketHead(int hash) {
            return buckets[bucketIndex(hash, buckets.length)];
        }

        DataRow decode(int node) {
            byte[] encoded = segments[node].getBytes(0, lengths[node]);
            return serializer.deserialize(encoded);
        }

        int appendAll(BoundedReplayRun run) {
            int maximum = 0;
            for (int node = 0; node < size; node++) {
                MemorySegment segment = segments[node];
                if (segment == null) continue;
                int length = lengths[node];
                long credit = RowSizeEstimator.estimateByteArray(length);
                reserveExact(credit, "resident replay frame");
                try {
                    run.append(segment.getBytes(0, length));
                    maximum = Math.max(maximum, length);
                } finally {
                    memoryManager.releaseExact(credit);
                }
                releaseNode(node);
            }
            return maximum;
        }

        void releaseReplayEscrow() {
            if (replayEscrowCredit != 0) {
                long credit = replayEscrowCredit;
                replayEscrowCredit = 0;
                memoryManager.releaseExact(credit);
            }
        }

        private void ensureReplayEscrow(int encodedLength) {
            long target = Math.addExact(REPLAY_WRITER_IO_CREDIT,
                    RowSizeEstimator.estimateByteArray(encodedLength));
            if (target <= replayEscrowCredit) return;
            long delta = target - replayEscrowCredit;
            reserveExact(delta, "hash fallback frame escrow");
            replayEscrowCredit = target;
        }

        private void ensureCapacity() {
            if (size < segments.length) return;
            int newNodeCapacity = Math.multiplyExact(segments.length, 2);
            int newBucketCapacity = Math.multiplyExact(buckets.length, 2);
            long newCredit = arraysCredit(newNodeCapacity, newBucketCapacity);
            reserveExact(newCredit, "hash index growth overlap");

            MemorySegment[] newSegments;
            int[] newLengths;
            int[] newNext;
            int[] newHashes;
            boolean[] newMatched;
            int[] newBuckets;
            try {
                newSegments = Arrays.copyOf(segments, newNodeCapacity);
                newLengths = Arrays.copyOf(lengths, newNodeCapacity);
                newNext = Arrays.copyOf(next, newNodeCapacity);
                newHashes = Arrays.copyOf(hashes, newNodeCapacity);
                newMatched = Arrays.copyOf(matched, newNodeCapacity);
                newBuckets = new int[newBucketCapacity];
                Arrays.fill(newBuckets, -1);
                for (int node = 0; node < size; node++) {
                    if (newNext[node] == NOT_INDEXED) continue;
                    int bucket = bucketIndex(newHashes[node], newBucketCapacity);
                    newNext[node] = newBuckets[bucket];
                    newBuckets[bucket] = node;
                }
            } catch (RuntimeException | Error failure) {
                memoryManager.releaseExact(newCredit);
                throw failure;
            }

            long oldCredit = arraysCredit;
            segments = newSegments;
            lengths = newLengths;
            next = newNext;
            hashes = newHashes;
            matched = newMatched;
            buckets = newBuckets;
            arraysCredit = newCredit;
            memoryManager.releaseExact(oldCredit);
        }

        private void allocateArrays(int nodeCapacity, int bucketCapacity) {
            long credit = arraysCredit(nodeCapacity, bucketCapacity);
            reserveExact(credit, "initial hash index");
            try {
                segments = new MemorySegment[nodeCapacity];
                lengths = new int[nodeCapacity];
                next = new int[nodeCapacity];
                hashes = new int[nodeCapacity];
                matched = new boolean[nodeCapacity];
                buckets = new int[bucketCapacity];
                Arrays.fill(buckets, -1);
                arraysCredit = credit;
            } catch (RuntimeException | Error failure) {
                memoryManager.releaseExact(credit);
                throw failure;
            }
        }

        private long arraysCredit(int nodeCapacity, int bucketCapacity) {
            long total = RowSizeEstimator.estimateReferenceArray(nodeCapacity);
            total = Math.addExact(total, RowSizeEstimator.estimateIntArray(nodeCapacity));
            total = Math.addExact(total, RowSizeEstimator.estimateIntArray(nodeCapacity));
            total = Math.addExact(total, RowSizeEstimator.estimateIntArray(nodeCapacity));
            total = Math.addExact(total, RowSizeEstimator.estimateBooleanArray(nodeCapacity));
            return Math.addExact(total, RowSizeEstimator.estimateIntArray(bucketCapacity));
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            Throwable failure = null;
            for (int node = 0; node < size; node++) {
                try {
                    releaseNode(node);
                } catch (RuntimeException | Error cleanup) {
                    if (failure == null) failure = cleanup;
                    else failure.addSuppressed(cleanup);
                }
            }
            segments = null;
            lengths = null;
            next = null;
            hashes = null;
            matched = null;
            buckets = null;
            try {
                memoryManager.releaseExact(arraysCredit);
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) failure = cleanup;
                else failure.addSuppressed(cleanup);
            }
            arraysCredit = 0;
            try {
                releaseReplayEscrow();
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) failure = cleanup;
                else failure.addSuppressed(cleanup);
            }
            if (failure instanceof RuntimeException) throw (RuntimeException) failure;
            if (failure instanceof Error) throw (Error) failure;
        }

        private void releaseNode(int node) {
            MemorySegment segment = segments[node];
            segments[node] = null;
            if (segment == null) return;
            long credit = RowSizeEstimator.estimateMemorySegment(lengths[node]);
            segmentPool.free(segment);
            memoryManager.releaseExact(credit);
        }
    }

    private static final class BuildDrainResult {
        private final StreamEvent terminal;
        private final DataRow overflowRow;

        private BuildDrainResult(StreamEvent terminal, DataRow overflowRow) {
            this.terminal = terminal;
            this.overflowRow = overflowRow;
        }

        static BuildDrainResult complete() { return new BuildDrainResult(null, null); }
        static BuildDrainResult terminal(StreamEvent terminal) { return new BuildDrainResult(terminal, null); }
        static BuildDrainResult overflow(DataRow row) { return new BuildDrainResult(null, row); }
    }

    private static final class ReplayRowStream implements RowStream.CloseableRowStream {
        private final BoundedReplayRun run;
        private final RowSerializer serializer;
        private final int fieldCount;
        private final int maximumFrameLength;
        private final MemoryManager memoryManager;
        private BudgetedFrameReader reader;
        private boolean closed;

        ReplayRowStream(BoundedReplayRun run, RowSerializer serializer, int fieldCount,
                        int maximumFrameLength, MemoryManager memoryManager) {
            this.run = run;
            this.serializer = serializer;
            this.fieldCount = fieldCount;
            this.maximumFrameLength = maximumFrameLength;
            this.memoryManager = memoryManager;
        }

        @Override
        public StreamEvent next() {
            if (closed) return null;
            if (reader == null) {
                reader = run.openBudgetedReader(memoryManager, fieldCount, maximumFrameLength);
            }
            try {
                byte[] frame = reader.nextFrame();
                if (frame == null) {
                    close();
                    return null;
                }
                return StreamEvent.record(serializer.deserialize(frame));
            } catch (RuntimeException | Error failure) {
                try {
                    close();
                } catch (RuntimeException | Error cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            if (reader != null) reader.close();
        }
    }

    private long keyScratchCredit(DataRow row, int[] indices) {
        long credit = RowSizeEstimator.estimateReferenceArray(indices.length);
        for (int index : indices) {
            Object value = row.get(index);
            if (value instanceof BigDecimal) {
                credit = Math.addExact(credit, 128L + 8L * ((BigDecimal) value).precision());
            }
        }
        return credit;
    }

    private static int hashKey(DataRow row, int[] indices) {
        int hash = 1;
        for (int index : indices) hash = 31 * hash + normalize(row.get(index)).hashCode();
        return hash;
    }

    private static boolean keysEqual(DataRow first, int[] firstIndices, DataRow second, int[] secondIndices) {
        for (int i = 0; i < firstIndices.length; i++) {
            if (!normalize(first.get(firstIndices[i])).equals(normalize(second.get(secondIndices[i])))) return false;
        }
        return true;
    }

    private static Object normalize(Object value) {
        if (value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        if (value instanceof BigDecimal) return ((BigDecimal) value).stripTrailingZeros();
        return value;
    }

    private static int bucketIndex(int hash, int bucketCount) {
        return hash & (bucketCount - 1);
    }

    private static boolean hasNullKey(DataRow row, int[] indices) {
        for (int index : indices) if (row.get(index) == null) return true;
        return false;
    }

    private static int[] resolve(DataSchema meta, String[] fields) {
        int[] indices = new int[fields.length];
        for (int i = 0; i < fields.length; i++) {
            int index = meta.getFieldIndex(fields[i]);
            if (index < 0) throw new IllegalArgumentException("Unknown join key field: " + fields[i]);
            indices[i] = index;
        }
        return indices;
    }

    private static String[] keyNames(DataSchema meta, int[] indices) {
        String[] names = new String[indices.length];
        for (int i = 0; i < indices.length; i++) names[i] = meta.getField(indices[i]).getName();
        return names;
    }

    private static DataSchema buildOutputMeta(DataSchema left, DataSchema right) {
        Field[] fields = new Field[left.getFieldCount() + right.getFieldCount()];
        int position = 0;
        for (int i = 0; i < left.getFieldCount(); i++) fields[position++] = left.getField(i);
        for (int i = 0; i < right.getFieldCount(); i++) fields[position++] = right.getField(i);
        return new DataSchema(fields);
    }
}
