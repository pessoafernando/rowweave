package io.rowweave.ops.join;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryBudgetExceededException;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.RowSerializer;
import io.rowweave.mem.RowSizeEstimator;
import io.rowweave.mem.seg.MemorySegment;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.BoundedReplayRun;
import io.rowweave.mem.spill.BoundedReplayStore;
import io.rowweave.mem.spill.BudgetedFrameReader;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.Arrays;
import java.util.Objects;

/** Round-robin AUTO probe over byte-managed candidates, which picks HASH_LEFT, HASH_RIGHT or SMJ. */
public final class AutoJoinSelector {

    private static final int MAX_PROBE_ROWS = 1_000_000;
    private static final int INITIAL_CAPACITY = 8;
    private static final long WRITER_IO_CREDIT = 512L + RowSizeEstimator.estimateByteArray(1024);

    private final DataSchema leftMeta;
    private final DataSchema rightMeta;
    private final String[] leftKeys;
    private final String[] rightKeys;
    private final JoinType joinType;
    private final int probeRows;
    private final MemoryManager memoryManager;
    private final BoundedReplayStore spillStore;
    private final MemorySegmentPool segmentPool;

    private boolean choseHash;
    private HashJoin.BuildSide selectedBuildSide;
    private boolean hashFellBack;
    private boolean strategySelected;
    private boolean invoked;

    public AutoJoinSelector(DataSchema leftMeta, DataSchema rightMeta, String[] leftKeys, String[] rightKeys,
                            JoinType joinType, int probeRows, MemoryManager memoryManager,
                            BoundedReplayStore spillStore, MemorySegmentPool segmentPool) {
        this.leftMeta = Objects.requireNonNull(leftMeta, "leftMeta");
        this.rightMeta = Objects.requireNonNull(rightMeta, "rightMeta");
        this.leftKeys = leftKeys.clone();
        this.rightKeys = rightKeys.clone();
        this.joinType = Objects.requireNonNull(joinType, "joinType");
        this.probeRows = validateProbeRows(probeRows);
        this.memoryManager = Objects.requireNonNull(memoryManager, "memoryManager");
        this.spillStore = Objects.requireNonNull(spillStore, "spillStore");
        this.segmentPool = Objects.requireNonNull(segmentPool, "segmentPool");
    }

    public void join(RowStream left, RowStream right, RowSink out) {
        if (invoked) throw new IllegalStateException("AutoJoinSelector is single-use");
        invoked = true;
        ProbeBuffer leftBuffer = null;
        ProbeBuffer rightBuffer = null;
        ReplayStream leftReplay = null;
        ReplayStream rightReplay = null;
        Throwable primary = null;
        try {
            leftBuffer = new ProbeBuffer(leftMeta);
            rightBuffer = new ProbeBuffer(rightMeta);
            ProbeResult result = probeRoundRobin(left, right, leftBuffer, rightBuffer);
            if (result.terminal != null) {
                cancelOther(left, right, result.terminal.cause());
                out.emit(result.terminal);
                return;
            }
            leftBuffer.freeze();
            rightBuffer.freeze();

            strategySelected = true;
            selectedBuildSide = chooseBuildSide(result, leftBuffer.records, rightBuffer.records);
            if (selectedBuildSide != null) {
                choseHash = true;
                leftReplay = leftBuffer.openReplay(memoryManager, left);
                rightReplay = rightBuffer.openReplay(memoryManager, right);
                HashJoin hash = HashJoin.of(leftMeta, rightMeta, leftKeys, rightKeys, joinType)
                        .withSpill(memoryManager, spillStore, segmentPool)
                        .withBuildSide(selectedBuildSide);
                hash.join(leftReplay, rightReplay, out);
                hashFellBack = hash.fellBackToSortMerge();
            } else {
                choseHash = false;
                MemoryManager[] sides = memoryManager.partitionEvenly(2);
                leftReplay = leftBuffer.openReplay(sides[0], left);
                rightReplay = rightBuffer.openReplay(sides[1], right);
                SortMergeJoin.of(leftMeta, rightMeta, leftKeys, rightKeys, joinType)
                        .withBorrowedSpill(sides[0], sides[1], spillStore, segmentPool)
                        .join(leftReplay, rightReplay, out);
            }
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            io.rowweave.mem.ResourceCloser.close(primary, leftReplay, rightReplay, leftBuffer, rightBuffer);
        }
    }

    private ProbeResult probeRoundRobin(RowStream left, RowStream right,
                                        ProbeBuffer leftBuffer, ProbeBuffer rightBuffer) {
        boolean leftEof = false;
        boolean rightEof = false;
        while (true) {
            if (!leftEof) {
                StreamEvent event = left.next();
                if (event == null || event.isEndOfStream()) leftEof = true;
                else if (event.isError() || event.isCancel()) return ProbeResult.terminal(event);
                else if (event.isRecord()) addCoordinated(leftBuffer, rightBuffer, event.row());
            }
            if (!rightEof) {
                StreamEvent event = right.next();
                if (event == null || event.isEndOfStream()) rightEof = true;
                else if (event.isError() || event.isCancel()) return ProbeResult.terminal(event);
                else if (event.isRecord()) addCoordinated(rightBuffer, leftBuffer, event.row());
            }
            if (leftEof || rightEof
                    || leftBuffer.records >= probeRows || rightBuffer.records >= probeRows) {
                return new ProbeResult(leftEof, rightEof, null);
            }
        }
    }

    private void addCoordinated(ProbeBuffer target, ProbeBuffer other, DataRow row) {
        try {
            target.add(row);
        } catch (MemoryBudgetExceededException pressure) {
            other.forceSpill();
            target.add(row);
        }
    }

    private HashJoin.BuildSide chooseBuildSide(ProbeResult result, int leftCount, int rightCount) {
        if (result.leftEof && result.rightEof) {
            return leftCount <= rightCount
                    ? HashJoin.BuildSide.LEFT : HashJoin.BuildSide.RIGHT;
        }
        if (result.leftEof) return HashJoin.BuildSide.LEFT;
        if (result.rightEof) return HashJoin.BuildSide.RIGHT;
        return null;
    }

    private void cancelOther(RowStream left, RowStream right, Throwable cause) {
        if (left.supportsCancellation()) left.cancel(cause);
        if (right.supportsCancellation()) right.cancel(cause);
    }

    public boolean choseHashForTest() {
        return choseHash;
    }

    public HashJoin.BuildSide selectedBuildSideForTest() {
        return selectedBuildSide;
    }

    String runtimeDecision() {
        if (!strategySelected) return null;
        if (!choseHash) return "SORT_MERGE";
        if (hashFellBack) return "HASH_OVERFLOW_TO_SMJ";
        return selectedBuildSide == HashJoin.BuildSide.LEFT ? "HASH_LEFT" : "HASH_RIGHT";
    }

    private static int validateProbeRows(int value) {
        if (value <= 0 || value > MAX_PROBE_ROWS) {
            throw new IllegalArgumentException("probeRows must be in [1," + MAX_PROBE_ROWS + "]: " + value);
        }
        return value;
    }

    private final class ProbeBuffer implements AutoCloseable {
        private final DataSchema meta;
        private final RowSerializer serializer;
        private BoundedReplayRun run;
        private MemorySegment[] segments;
        private int[] lengths;
        private int records;
        private int resident;
        private int spillCursor;
        private int maximumFrameLength;
        private long arraysCredit;
        private long scratchEscrow;
        private long writerCredit;
        private boolean spilled;
        private boolean spillRecorded;
        private boolean frozen;
        private boolean closed;

        ProbeBuffer(DataSchema meta) {
            this.meta = meta;
            this.serializer = new RowSerializer(meta);
            reserve(WRITER_IO_CREDIT, "AUTO replay writer");
            writerCredit = WRITER_IO_CREDIT;
            try {
                run = spillStore.createRun();
                allocateArrays(Math.min(INITIAL_CAPACITY, probeRows));
            } catch (RuntimeException | Error failure) {
                close();
                throw failure;
            }
        }

        void add(DataRow row) {
            if (spilled) {
                appendRow(row);
                records++;
                return;
            }
            int length = serializer.serializedSize(row);
            try {
                ensureScratchEscrow(length);
                ensureCapacity();
                addResident(row, length);
                records++;
            } catch (MemoryBudgetExceededException pressure) {
                forceSpill();
                appendRow(row);
                records++;
            }
        }

        private void addResident(DataRow row, int length) {
            long segmentCredit = RowSizeEstimator.estimateMemorySegment(length);
            reserve(segmentCredit, "AUTO resident frame");
            MemorySegment segment = null;
            boolean published = false;
            try {
                segment = segmentPool.allocate(length);
                long serializerScratch = serializer.serializerScratchCredit(row);
                reserve(serializerScratch, "AUTO serializer scratch");
                try {
                    serializer.serializeInto(row, segment, 0);
                } finally {
                    memoryManager.releaseExact(serializerScratch);
                }
                segments[resident] = segment;
                lengths[resident] = length;
                resident++;
                maximumFrameLength = Math.max(maximumFrameLength, length);
                published = true;
            } finally {
                if (!published) {
                    if (segment != null) segmentPool.free(segment);
                    memoryManager.releaseExact(segmentCredit);
                }
            }
        }

        void forceSpill() {
            if (spilled) return;
            releaseScratchEscrow();
            while (spillCursor < resident) {
                MemorySegment segment = segments[spillCursor];
                int length = lengths[spillCursor];
                if (segment == null) {
                    spillCursor++;
                    continue;
                }
                long credit = RowSizeEstimator.estimateByteArray(length);
                reserve(credit, "AUTO resident replay copy");
                try {
                    recordDiskUse();
                    run.append(segment.getBytes(0, length));
                } finally {
                    memoryManager.releaseExact(credit);
                }
                segments[spillCursor] = null;
                segmentPool.free(segment);
                memoryManager.releaseExact(RowSizeEstimator.estimateMemorySegment(length));
                spillCursor++;
            }
            resident = 0;
            spillCursor = 0;
            releaseArrays();
            spilled = true;
        }

        void freeze() {
            if (frozen) return;
            forceSpill();
            run.finishWriting();
            frozen = true;
            if (writerCredit != 0) {
                long credit = writerCredit;
                writerCredit = 0;
                memoryManager.releaseExact(credit);
            }
        }

        ReplayStream openReplay(MemoryManager replayMemory, RowStream tail) {
            if (!frozen) throw new IllegalStateException("AUTO replay opened before freeze");
            return new ReplayStream(run, serializer, meta.getFieldCount(), maximumFrameLength,
                    replayMemory, tail);
        }

        private void appendRow(DataRow row) {
            int length = serializer.serializedSize(row);
            long credit = Math.addExact(RowSizeEstimator.estimateByteArray(length),
                    serializer.serializerScratchCredit(row));
            reserve(credit, "AUTO replay serialization");
            try {
                recordDiskUse();
                run.append(serializer.serialize(row));
                maximumFrameLength = Math.max(maximumFrameLength, length);
            } finally {
                memoryManager.releaseExact(credit);
            }
        }

        private void recordDiskUse() {
            if (!spillRecorded) {
                spillRecorded = true;
                memoryManager.recordSelfSpill();
            }
        }

        private void ensureScratchEscrow(int length) {
            long target = RowSizeEstimator.estimateByteArray(length);
            if (target <= scratchEscrow) return;
            reserve(target - scratchEscrow, "AUTO fallback scratch escrow");
            scratchEscrow = target;
        }

        private void ensureCapacity() {
            if (resident < segments.length) return;
            int nextCapacity = Math.min(probeRows, Math.multiplyExact(segments.length, 2));
            if (nextCapacity <= segments.length) throw new IllegalStateException("AUTO probe capacity exhausted");
            long newCredit = arraysCredit(nextCapacity);
            reserve(newCredit, "AUTO probe array growth overlap");
            MemorySegment[] newSegments;
            int[] newLengths;
            try {
                newSegments = Arrays.copyOf(segments, nextCapacity);
                newLengths = Arrays.copyOf(lengths, nextCapacity);
            } catch (RuntimeException | Error failure) {
                memoryManager.releaseExact(newCredit);
                throw failure;
            }
            long oldCredit = arraysCredit;
            segments = newSegments;
            lengths = newLengths;
            arraysCredit = newCredit;
            memoryManager.releaseExact(oldCredit);
        }

        private void allocateArrays(int capacity) {
            long credit = arraysCredit(capacity);
            if (!memoryManager.tryReserve(credit)) {
                spilled = true;
                return;
            }
            try {
                segments = new MemorySegment[capacity];
                lengths = new int[capacity];
                arraysCredit = credit;
            } catch (RuntimeException | Error failure) {
                memoryManager.releaseExact(credit);
                throw failure;
            }
        }

        private long arraysCredit(int capacity) {
            return Math.addExact(RowSizeEstimator.estimateReferenceArray(capacity),
                    RowSizeEstimator.estimateIntArray(capacity));
        }

        private void releaseArrays() {
            segments = null;
            lengths = null;
            if (arraysCredit != 0) {
                long credit = arraysCredit;
                arraysCredit = 0;
                memoryManager.releaseExact(credit);
            }
        }

        private void releaseScratchEscrow() {
            if (scratchEscrow != 0) {
                long credit = scratchEscrow;
                scratchEscrow = 0;
                memoryManager.releaseExact(credit);
            }
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            Throwable failure = null;
            if (segments != null) {
                for (int i = 0; i < resident; i++) {
                    MemorySegment segment = segments[i];
                    segments[i] = null;
                    if (segment == null) continue;
                    try {
                        segmentPool.free(segment);
                        memoryManager.releaseExact(RowSizeEstimator.estimateMemorySegment(lengths[i]));
                    } catch (RuntimeException | Error cleanup) {
                        failure = append(failure, cleanup);
                    }
                }
            }
            try { releaseArrays(); } catch (RuntimeException | Error cleanup) { failure = append(failure, cleanup); }
            try { releaseScratchEscrow(); } catch (RuntimeException | Error cleanup) { failure = append(failure, cleanup); }
            if (writerCredit != 0) {
                try {
                    long credit = writerCredit;
                    writerCredit = 0;
                    memoryManager.releaseExact(credit);
                } catch (RuntimeException | Error cleanup) {
                    failure = append(failure, cleanup);
                }
            }
            if (run != null) {
                try { run.dispose(); } catch (RuntimeException | Error cleanup) { failure = append(failure, cleanup); }
            }
            if (failure instanceof RuntimeException) throw (RuntimeException) failure;
            if (failure instanceof Error) throw (Error) failure;
        }
    }

    private final class ReplayStream implements RowStream.CloseableRowStream, MemoryRebindableRowStream {
        private final BoundedReplayRun run;
        private final RowSerializer serializer;
        private final int fieldCount;
        private final int maximumFrameLength;
        private MemoryManager replayMemory;
        private final RowStream tail;
        private BudgetedFrameReader reader;
        private boolean prefixDone;
        private boolean tailDone;
        private boolean closed;

        ReplayStream(BoundedReplayRun run, RowSerializer serializer, int fieldCount,
                     int maximumFrameLength, MemoryManager replayMemory, RowStream tail) {
            this.run = run;
            this.serializer = serializer;
            this.fieldCount = fieldCount;
            this.maximumFrameLength = maximumFrameLength;
            this.replayMemory = replayMemory;
            this.tail = tail;
        }

        @Override
        public StreamEvent next() {
            if (closed) return null;
            if (!prefixDone) {
                if (reader == null) {
                    reader = run.openBudgetedReader(replayMemory, fieldCount, maximumFrameLength);
                }
                byte[] frame = reader.nextFrame();
                if (frame != null) return StreamEvent.record(serializer.deserialize(frame));
                prefixDone = true;
                reader = null;
            }
            StreamEvent event = tail.next();
            if (event == null) tailDone = true;
            return event;
        }

        @Override
        public boolean supportsCancellation() {
            return tail.supportsCancellation();
        }

        @Override
        public void rebindMemory(MemoryManager memoryManager) {
            if (reader != null) {
                throw new IllegalStateException("cannot rebind AUTO replay with an active frame reader");
            }
            this.replayMemory = Objects.requireNonNull(memoryManager, "memoryManager");
        }

        @Override
        public boolean cancel(Throwable cause) {
            closeReader();
            tailDone = true;
            return tail.supportsCancellation() && tail.cancel(cause);
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            closeReader();
            if (!tailDone && tail.supportsCancellation()) tail.cancel(new RuntimeException("AUTO replay closed"));
            if (tail instanceof RowStream.CloseableRowStream) {
                ((RowStream.CloseableRowStream) tail).close();
            }
        }

        private void closeReader() {
            if (reader != null) {
                reader.close();
                reader = null;
            }
            prefixDone = true;
        }
    }

    private static final class ProbeResult {
        private final boolean leftEof;
        private final boolean rightEof;
        private final StreamEvent terminal;

        ProbeResult(boolean leftEof, boolean rightEof, StreamEvent terminal) {
            this.leftEof = leftEof;
            this.rightEof = rightEof;
            this.terminal = terminal;
        }

        static ProbeResult terminal(StreamEvent terminal) {
            return new ProbeResult(false, false, terminal);
        }
    }

    private void reserve(long bytes, String owner) {
        if (!memoryManager.tryReserve(bytes)) {
            throw new MemoryBudgetExceededException("AUTO cannot reserve " + bytes + " for " + owner
                    + ": reserved=" + memoryManager.reservedBytes()
                    + " budget=" + memoryManager.budget());
        }
    }

    private static Throwable append(Throwable first, Throwable next) {
        if (first == null) return next;
        if (first != next) first.addSuppressed(next);
        return first;
    }
}
