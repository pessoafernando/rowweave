package io.rowweave.mem;

import io.rowweave.DataRow;
import io.rowweave.mem.seg.MemorySegment;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.SpillRun;

import java.util.Comparator;

/** Shared direct-encoded batch with primitive pointer sort and comparison over encoded built-in keys. */
final class EncodedSortBatch {

    private final RowSerializer serializer;
    private final Comparator<DataRow> comparator;
    private final NormalizedKeyComputer normKey;
    private final EncodedSortKeyComparator encodedComparator;
    private final MemorySegmentPool pool;
    private MemorySegment[] segments = new MemorySegment[16];
    private int[] lengths = new int[16];
    private long[] normKeys = new long[16];
    private long[] seqs = new long[16];
    private int count;
    private long currentBytes;

    EncodedSortBatch(RowSerializer serializer, Comparator<DataRow> comparator,
                     NormalizedKeyComputer normKey, EncodedSortKeyComparator encodedComparator,
                     MemorySegmentPool pool) {
        this.serializer = serializer;
        this.comparator = comparator;
        this.normKey = normKey;
        this.encodedComparator = encodedComparator;
        this.pool = pool;
    }

    boolean isEmpty() { return count == 0; }

    int size() { return count; }

    long currentBytes() { return currentBytes; }

    void addDirect(DataRow row, int serializedSize, long prefix, long seq) {
        MemorySegment candidate = null;
        try {
            candidate = pool.allocate(Math.max(1, serializedSize));
            serializer.serializeInto(row, candidate, 0);
            ensureCapacity(count + 1);
            segments[count] = candidate;
            lengths[count] = serializedSize;
            normKeys[count] = prefix;
            seqs[count] = seq;
            count++;
            currentBytes = Math.addExact(currentBytes, serializedSize);
            candidate = null;
        } finally {
            if (candidate != null) {
                pool.free(candidate);
                pool.trim();
            }
        }
    }

    void addBytes(byte[] rowBytes, long prefix, long seq) {
        MemorySegment candidate = null;
        try {
            candidate = pool.allocate(Math.max(1, rowBytes.length));
            candidate.putBytes(0, rowBytes);
            ensureCapacity(count + 1);
            segments[count] = candidate;
            lengths[count] = rowBytes.length;
            normKeys[count] = prefix;
            seqs[count] = seq;
            count++;
            currentBytes = Math.addExact(currentBytes, rowBytes.length);
            candidate = null;
        } finally {
            if (candidate != null) {
                pool.free(candidate);
                pool.trim();
            }
        }
    }

    private void ensureCapacity(int required) {
        if (required <= segments.length) {
            return;
        }
        int capacity = Math.max(required, Math.multiplyExact(segments.length, 2));
        MemorySegment[] newSegments = new MemorySegment[capacity];
        int[] newLengths = new int[capacity];
        long[] newNormKeys = new long[capacity];
        long[] newSeqs = new long[capacity];
        System.arraycopy(segments, 0, newSegments, 0, count);
        System.arraycopy(lengths, 0, newLengths, 0, count);
        System.arraycopy(normKeys, 0, newNormKeys, 0, count);
        System.arraycopy(seqs, 0, newSeqs, 0, count);
        segments = newSegments;
        lengths = newLengths;
        normKeys = newNormKeys;
        seqs = newSeqs;
    }

    int[] sortPointers() {
        int[] order = new int[count];
        for (int i = 0; i < count; i++) {
            order[i] = i;
        }
        for (int start = count / 2 - 1; start >= 0; start--) {
            siftDown(order, start, count);
        }
        for (int end = count - 1; end > 0; end--) {
            int tmp = order[0];
            order[0] = order[end];
            order[end] = tmp;
            siftDown(order, 0, end);
        }
        return order;
    }

    private void siftDown(int[] order, int root, int size) {
        while (true) {
            int child = root * 2 + 1;
            if (child >= size) {
                return;
            }
            if (child + 1 < size && comparePointers(order[child], order[child + 1]) < 0) {
                child++;
            }
            if (comparePointers(order[root], order[child]) >= 0) {
                return;
            }
            int tmp = order[root];
            order[root] = order[child];
            order[child] = tmp;
            root = child;
        }
    }

    private int comparePointers(int left, int right) {
        int compared = Long.compareUnsigned(normKeys[left], normKeys[right]);
        if (compared != 0) {
            return compared;
        }
        if (!normKey.isFullyDetermined()) {
            compared = encodedComparator.compare(
                    segments[left], lengths[left], segments[right], lengths[right]);
            if (compared == EncodedSortKeyComparator.FALLBACK) {
                DataRow leftRow = serializer.deserialize(segments[left].getBytes(0, lengths[left]));
                DataRow rightRow = serializer.deserialize(segments[right].getBytes(0, lengths[right]));
                compared = comparator.compare(leftRow, rightRow);
            }
            if (compared != 0) {
                return compared;
            }
        }
        return Long.compare(seqs[left], seqs[right]);
    }

    void appendSortedFrames(SpillRun run, int[] order) {
        for (int pointer : order) {
            int length = lengths[pointer];
            byte[] frame = new byte[Math.addExact(ExternalSortMemory.FRAME_HEADER_BYTES, length)];
            putLong(frame, 0, seqs[pointer]);
            putInt(frame, 8, length);
            segments[pointer].copyTo(0, frame, ExternalSortMemory.FRAME_HEADER_BYTES, length);
            run.append(frame);
        }
    }

    void releaseAllAndTrim() {
        Throwable failure = null;
        for (int i = 0; i < count; i++) {
            MemorySegment segment = segments[i];
            segments[i] = null;
            if (segment != null) {
                try {
                    pool.free(segment);
                } catch (RuntimeException | Error cleanup) {
                    failure = appendFailure(failure, cleanup);
                }
            }
        }
        count = 0;
        currentBytes = 0;
        try {
            pool.trim();
        } catch (RuntimeException | Error cleanup) {
            failure = appendFailure(failure, cleanup);
        }
        throwFailure(failure);
    }

    void discardAndTrim() { releaseAllAndTrim(); }

    private static Throwable appendFailure(Throwable primary, Throwable cleanup) {
        if (primary == null) {
            return cleanup;
        }
        if (primary != cleanup) {
            primary.addSuppressed(cleanup);
        }
        return primary;
    }

    private static void throwFailure(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
    }

    private static void putInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static void putLong(byte[] target, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            target[offset + i] = (byte) (value >>> (56 - i * 8));
        }
    }
}
