package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.mem.spill.SpillRunWriter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Owns the finished runs of an external sort and does only the run merge, the cursors and their lifecycle.
 * The resident encoded batch and primitive pointer sort belong to {@link EncodedSortBatch}; tie-breaking on built-in keys
 * in the run heap reuses {@link EncodedSortKeyComparator}, while complex types still fall back to the authoritative DataRow comparator.
 *
 * <p><b>Not thread-safe</b>: one SortedRunStore instance belongs to a single thread. Under the three-thread model each circular buffer has its own instance and ownership moves through a queue.
 */
public final class SortedRunStore {

    // Ceiling on concurrent external merge fan-in, which keeps file descriptors finite. The value is read from WorkerConfig.mergeFanIn() (128 by default, configurable),
    // so the engine has one source for the number and SpillingSegmentSortBuffer and SpillingSortBuffer agree with it.
    static final int MERGE_FANIN = WorkerConfig.mergeFanIn();

    private final DataSchema dataSchema;
    private final RowSerializer serializer;
    private final Comparator<DataRow> comparator;
    private final NormalizedKeyComputer normKey;
    private final EncodedSortKeyComparator encodedComparator;
    private final SpillStore spillStore;

    private final List<SpillRun> sortedRuns = new ArrayList<>();
    private final List<MergeIterator> activeIterators = new ArrayList<>();

    public SortedRunStore(DataSchema dataSchema, RowSerializer serializer, Comparator<DataRow> comparator,
                          NormalizedKeyComputer normKey, SpillStore spillStore) {
        this(dataSchema, serializer, comparator, normKey,
                EncodedSortKeyComparator.create(dataSchema, comparator, null), spillStore);
    }

    SortedRunStore(DataSchema dataSchema, RowSerializer serializer, Comparator<DataRow> comparator,
                   NormalizedKeyComputer normKey, EncodedSortKeyComparator encodedComparator,
                   SpillStore spillStore) {
        this.dataSchema = dataSchema;
        this.serializer = serializer;
        this.comparator = comparator;
        this.normKey = normKey;
        this.encodedComparator = encodedComparator;
        this.spillStore = spillStore;
    }

    /** Takes an ordered run written elsewhere, by the spill thread, into the final merge. */
    public void acceptRun(SpillRun run) {
        sortedRuns.add(run);
    }

    /** Pipelined keeps a single run at the end, disposing each input run once a later pass has replaced it. */
    public void mergeAcceptedRunsToSingle() {
        mergeAcceptedRunsToSingle(MERGE_FANIN);
    }

    /** Dynamic fan-in the caller has already derived from the memory credit. */
    public void mergeAcceptedRunsToSingle(int fanIn) {
        if (fanIn < 2) {
            throw new IllegalArgumentException("fanIn must be >= 2");
        }
        mergeToRunCount(1, true, fanIn);
    }

    /** Merges only down to the number of final k-way cursors the budget can hold whole, avoiding an unnecessary final full rewrite pass. */
    public void mergeAcceptedRunsToAtMost(int targetRunCount, int fanIn) {
        if (targetRunCount < 1) {
            throw new IllegalArgumentException("targetRunCount must be >= 1");
        }
        if (fanIn < 2) {
            throw new IllegalArgumentException("fanIn must be >= 2");
        }
        mergeToRunCount(targetRunCount, true, fanIn);
    }

    private void mergeToRunCount(int targetRunCount, boolean disposeInputs, int fanIn) {
        while (sortedRuns.size() > targetRunCount) {
            List<SpillRun> next = new ArrayList<>();
            List<SpillRun> created = new ArrayList<>();
            try {
                for (int i = 0; i < sortedRuns.size(); i += fanIn) {
                    List<SpillRun> group = new ArrayList<>(
                            sortedRuns.subList(i, Math.min(i + fanIn, sortedRuns.size())));
                    if (group.size() == 1) {
                        next.add(group.get(0));
                        continue;
                    }
                    SpillRun merged = SpillRunWriter.write(spillStore, target -> {
                        List<RunCursor> cursors = new ArrayList<>();
                        Throwable primary = null;
                        try {
                            PriorityQueue<RunCursor> heap = newMergeHeap(group, cursors);
                            while (!heap.isEmpty()) {
                                RunCursor c = heap.poll();
                                target.append(encodeFrame(c.head.seq, c.head.bytes));
                                if (c.advance()) {
                                    heap.add(c);
                                }
                            }
                        } catch (RuntimeException | Error failure) {
                            primary = failure;
                            throw failure;
                        } finally {
                            try {
                                closeCursors(cursors);
                            } catch (RuntimeException | Error cleanup) {
                                if (primary != null) {
                                    primary.addSuppressed(cleanup);
                                } else {
                                    throw cleanup;
                                }
                            }
                        }
                    });
                    if (disposeInputs) {
                        try {
                            disposeRuns(group);
                        } catch (RuntimeException | Error failure) {
                            try {
                                merged.dispose();
                            } catch (RuntimeException | Error cleanup) {
                                failure.addSuppressed(cleanup);
                            }
                            throw failure;
                        }
                    }
                    created.add(merged);
                    next.add(merged);
                }
            } catch (RuntimeException | Error failure) {
                try {
                    disposeRuns(created);
                } catch (RuntimeException | Error cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            sortedRuns.clear();
            sortedRuns.addAll(next);
        }
    }

    public int acceptedRunCount() {
        return sortedRuns.size();
    }

    private static void disposeRuns(List<SpillRun> runs) {
        Throwable failure = null;
        for (SpillRun run : runs) {
            try {
                run.dispose();
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
    }

    private PriorityQueue<RunCursor> newMergeHeap(List<SpillRun> runs, List<RunCursor> opened) {
        PriorityQueue<RunCursor> heap = new PriorityQueue<>((x, y) -> {
            int c = Long.compareUnsigned(x.head.normKey, y.head.normKey);
            if (c != 0) {
                return c;
            }
            if (!normKey.isFullyDetermined()) {
                int fc = encodedComparator.compare(x.head.bytes, y.head.bytes);
                if (fc == EncodedSortKeyComparator.FALLBACK) {
                    fc = comparator.compare(x.head.toRow(serializer), y.head.toRow(serializer));
                }
                if (fc != 0) {
                    return fc;
                }
            }
            return Long.compare(x.head.seq, y.head.seq);
        });
        for (SpillRun run : runs) {
            RunCursor cursor = new RunCursor(run.read(), serializer, normKey);
            if (opened != null) {
                opened.add(cursor);
            }
            if (cursor.advance()) {
                heap.add(cursor);
            } else {
                if (opened != null) {
                    opened.remove(opened.size() - 1);
                }
                cursor.close();
            }
        }
        return heap;
    }

    private static void closeCursors(List<RunCursor> cursors) {
        Throwable failure = null;
        for (RunCursor cursor : cursors) {
            try {
                cursor.close();
            } catch (RuntimeException | Error cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
    }

    /** After finishSorting: a streaming k-way merge over at most MERGE_FANIN runs, so concurrently open handles stay bounded. */
    public Iterator<DataRow> iterateSorted() {
        List<RunCursor> opened = new ArrayList<>();
        PriorityQueue<RunCursor> heap;
        Throwable primary = null;
        try {
            heap = newMergeHeap(sortedRuns, opened);
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            if (primary != null) {
                try {
                    closeCursors(opened);
                } catch (RuntimeException | Error cleanup) {
                    primary.addSuppressed(cleanup);
                }
            }
        }
        MergeIterator iterator = new MergeIterator(heap, opened);
        activeIterators.add(iterator);
        if (heap.isEmpty()) {
            iterator.close();
        }
        return iterator;
    }

    /** Releases only the runs and cursors this store owns, and never closes a SpillStore that sibling buffers may share. */
    public void disposeAll() {
        Throwable failure = null;
        for (MergeIterator iterator : new ArrayList<>(activeIterators)) {
            try {
                iterator.close();
            } catch (RuntimeException | Error cleanup) {
                failure = appendFailure(failure, cleanup);
            }
        }
        try {
            disposeRuns(new ArrayList<>(sortedRuns));
        } catch (RuntimeException | Error cleanup) {
            failure = appendFailure(failure, cleanup);
        } finally {
            sortedRuns.clear();
        }
        rethrow(failure);
    }

    private static Throwable appendFailure(Throwable first, Throwable next) {
        if (first == null) {
            return next;
        }
        if (first != next) {
            first.addSuppressed(next);
        }
        return first;
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
    }

    private final class MergeIterator implements Iterator<DataRow>, AutoCloseable {
        private final PriorityQueue<RunCursor> heap;
        private final List<RunCursor> cursors;
        private boolean closed;

        private MergeIterator(PriorityQueue<RunCursor> heap, List<RunCursor> cursors) {
            this.heap = heap;
            this.cursors = cursors;
        }

        @Override
        public boolean hasNext() {
            boolean has = !heap.isEmpty();
            if (!has) {
                close();
            }
            return has;
        }

        @Override
        public DataRow next() {
            if (heap.isEmpty()) {
                close();
                throw new NoSuchElementException();
            }
            try {
                RunCursor cursor = heap.poll();
                DataRow row = cursor.head.toRow(serializer);
                if (cursor.advance()) {
                    heap.add(cursor);
                } else {
                    cursor.close();
                }
                if (heap.isEmpty()) {
                    close();
                }
                return row;
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
            if (closed) {
                return;
            }
            closed = true;
            activeIterators.remove(this);
            heap.clear();
            closeCursors(cursors);
        }
    }

    // frame layout: [seq long][rowBytesLen int][rowBytes]
    private byte[] encodeFrame(long seq, byte[] rowBytes) {
        byte[] frame = new byte[Math.addExact(12, rowBytes.length)];
        putLong(frame, 0, seq);
        putInt(frame, 8, rowBytes.length);
        System.arraycopy(rowBytes, 0, frame, 12, rowBytes.length);
        return frame;
    }

    private static SeqBytes decodeFrame(byte[] frame, NormalizedKeyComputer normKey, DataSchema meta) {
        if (frame.length < 12) {
            throw new IllegalArgumentException("truncated sort frame: " + frame.length);
        }
        long seq = readLong(frame, 0);
        int len = readInt(frame, 8);
        if (len < 0 || len != frame.length - 12) {
            throw new IllegalArgumentException("invalid sort frame length: declared=" + len
                    + " actual=" + (frame.length - 12));
        }
        byte[] rowBytes = new byte[len];
        System.arraycopy(frame, 12, rowBytes, 0, len);
        return new SeqBytes(rowBytes, seq, normKey.computePrefixFromBytes(rowBytes, meta));
    }

    private static void putInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static int readInt(byte[] source, int offset) {
        return ((source[offset] & 0xFF) << 24)
                | ((source[offset + 1] & 0xFF) << 16)
                | ((source[offset + 2] & 0xFF) << 8)
                | (source[offset + 3] & 0xFF);
    }

    private static void putLong(byte[] target, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            target[offset + i] = (byte) (value >>> (56 - i * 8));
        }
    }

    private static long readLong(byte[] source, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (source[offset + i] & 0xFFL);
        }
        return value;
    }

    private static final class SeqBytes {
        final byte[] bytes;
        final long seq;
        final long normKey;

        SeqBytes(byte[] bytes, long seq, long normKey) {
            this.bytes = bytes;
            this.seq = seq;
            this.normKey = normKey;
        }

        DataRow toRow(RowSerializer serializer) {
            return serializer.deserialize(bytes);
        }
    }

    private final class RunCursor {
        private final Iterator<byte[]> it;
        private final RowSerializer ser;
        private final NormalizedKeyComputer nk;
        SeqBytes head;
        private boolean closed;

        RunCursor(Iterator<byte[]> it, RowSerializer ser, NormalizedKeyComputer nk) {
            this.it = it;
            this.ser = ser;
            this.nk = nk;
        }

        boolean advance() {
            if (it.hasNext()) {
                head = decodeFrame(it.next(), nk, dataSchema);
                return true;
            }
            head = null;
            return false;
        }

        void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (it instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) it).close();
                } catch (RuntimeException | Error failure) {
                    throw failure;
                } catch (Exception failure) {
                    throw new RuntimeException(failure);
                }
            }
        }
    }
}
