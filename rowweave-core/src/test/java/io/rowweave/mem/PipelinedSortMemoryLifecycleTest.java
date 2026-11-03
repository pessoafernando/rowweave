package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegment;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exact owner credit, physical caching and worker fail-stop. */
class PipelinedSortMemoryLifecycleTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType),
            new Field("v", DataType.StringType));
    private static final SortField[] SORT = {
            new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT)};

    private static DataRow row(int key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    @Test
    void emptyBufferNeverReleasesAnotherOwnersReservation() {
        MemoryManager memory = new MemoryManager(512);
        memory.reserve(400);
        PipelinedExternalSortBuffer buffer = new PipelinedExternalSortBuffer(
                META, new RowComparator(META, SORT), SORT, memory, 2, 2,
                new MemorySpillStore(), HeapPool::new);

        buffer.sort();
        buffer.close();

        assertEquals(400, memory.reservedBytes());
        memory.releaseExact(400);
    }

    @Test
    void oversizedFirstRowFailsBeforeAnySegmentAllocation() {
        MemoryManager memory = new MemoryManager(256);
        List<HeapPool> pools = new ArrayList<>();
        PipelinedExternalSortBuffer buffer = new PipelinedExternalSortBuffer(
                META, new RowComparator(META, SORT), SORT, memory, 2, 2,
                new MemorySpillStore(), () -> {
                    HeapPool pool = new HeapPool();
                    pools.add(pool);
                    return pool;
                });

        assertThrows(MemoryBudgetExceededException.class,
                () -> buffer.putRow(row(1, "x".repeat(16_384))));
        buffer.close();

        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pools.stream().mapToInt(HeapPool::outstanding).sum());
        assertEquals(0, pools.stream().mapToLong(HeapPool::cachedBytes).sum());
        assertFalse(buffer.workerThreadsAliveForTest());
    }

    @Test
    void comparatorFailureWakesSpillWorkerAndRethrowsSameRoot() {
        RuntimeException root = new RuntimeException("compare exploded");
        Comparator<DataRow> comparator = (left, right) -> {
            throw root;
        };
        SortField[] fallbackSort = {
                new SortField(1, SortField.Direction.ASC, SortField.NullOrder.DEFAULT)};
        MemoryManager memory = new MemoryManager(256 * 1024);
        PipelinedExternalSortBuffer buffer = new PipelinedExternalSortBuffer(
                META, comparator, fallbackSort, memory, 2, 2,
                new MemorySpillStore(), HeapPool::new);
        buffer.putRow(row(2, "same-prefix-B"));
        buffer.putRow(row(1, "same-prefix-A"));

        RuntimeException thrown = assertTimeoutPreemptively(Duration.ofSeconds(3),
                () -> assertThrows(RuntimeException.class, buffer::sort));
        assertSame(root, thrown);
        buffer.close();

        assertFalse(buffer.workerThreadsAliveForTest());
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, buffer.activeTokenCountForTest());
    }

    @Test
    void heapPoolTrimDropsAllCachedBucketsAndBytes() {
        HeapPool pool = new HeapPool();
        io.rowweave.mem.seg.MemorySegment first = pool.allocate(17);
        io.rowweave.mem.seg.MemorySegment second = pool.allocate(33);
        pool.free(first);
        pool.free(second);
        pool.trim();

        assertEquals(0, pool.cachedBytes());
        assertEquals(0, pool.freeBucketCount());
        assertEquals(0, pool.outstanding());
    }

    @Test
    void multiPassMergeDisposesOldRunsWithoutRewritingCreditFeasibleFinalRuns(@TempDir Path tempDir) {
        MemoryManager memory = new MemoryManager(256 * 1024);
        DiskSpillStore spillStore = new DiskSpillStore(tempDir.toFile());
        PipelinedExternalSortBuffer buffer = new PipelinedExternalSortBuffer(
                META, new RowComparator(META, SORT), SORT, memory, 1, 2,
                spillStore, HeapPool::new);
        for (int i = 0; i < 260; i++) {
            buffer.putRow(row(260 - i, "row-" + i));
        }

        buffer.sort();

        java.io.File[] files = tempDir.toFile().listFiles();
        int finalRuns = files == null ? 0 : files.length;
        assertTrue(finalRuns > 1,
                "credit-feasible final runs must not be rewritten into one extra full-data run");
        assertTrue(finalRuns <= WorkerConfig.mergeFanIn(),
                "final iterator fan-in must remain bounded");
        int rows = 0;
        for (DataRow ignored : buffer) {
            rows++;
            if (rows == 1) {
                assertEquals(finalRuns, spillStore.currentOpenReaders(),
                        "final k-way iterator must open exactly the credited final runs");
            }
        }
        assertEquals(260, rows);
        assertEquals(0, spillStore.currentOpenReaders());
        buffer.close();
        assertEquals(0, memory.reservedBytes());
        java.io.File[] after = tempDir.toFile().listFiles();
        assertEquals(0, after == null ? 0 : after.length);
    }

    @Test
    void segmentWriteFailureBecomesStableTerminalAndRollsBackCredit() {
        RuntimeException root = new RuntimeException("segment write failed");
        HeapPool delegate = new HeapPool();
        java.util.concurrent.atomic.AtomicBoolean failOnce = new java.util.concurrent.atomic.AtomicBoolean(true);
        MemorySegmentPool failingPool = new MemorySegmentPool() {
            @Override public MemorySegment allocate(int size) {
                MemorySegment segment = delegate.allocate(size);
                if (failOnce.compareAndSet(true, false)) {
                    return new DelegatingSegment(segment) {
                        @Override public void putByte(int offset, byte value) { throw root; }
                    };
                }
                return segment;
            }
            @Override public void free(MemorySegment segment) {
                delegate.free(segment instanceof DelegatingSegment
                        ? ((DelegatingSegment) segment).delegate : segment);
            }
            @Override public int outstanding() { return delegate.outstanding(); }
            @Override public void trim() { delegate.trim(); }
        };
        MemoryManager memory = new MemoryManager(64 * 1024);
        PipelinedExternalSortBuffer buffer = new PipelinedExternalSortBuffer(
                META, new RowComparator(META, SORT), SORT, memory, 2, 2,
                new MemorySpillStore(), () -> failingPool);

        RuntimeException first = assertThrows(RuntimeException.class, () -> buffer.putRow(row(1, "a")));
        RuntimeException second = assertThrows(RuntimeException.class, () -> buffer.putRow(row(2, "b")));

        assertSame(root, first);
        assertSame(root, second, "all later data entry must rethrow the same terminal instance");
        buffer.close();
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, delegate.outstanding());
        assertFalse(buffer.workerThreadsAliveForTest());
    }

    @Test
    void spillAppendFailureWakesBothWorkersAndKeepsRoot() {
        RuntimeException root = new RuntimeException("spill append failed");
        SpillStore failingStore = new SpillStore() {
            @Override public SpillRun createRun() {
                return new SpillRun() {
                    private boolean disposed;
                    @Override public void append(byte[] frame) { throw root; }
                    @Override public void finishWriting() { }
                    @Override public Iterator<byte[]> read() { return java.util.Collections.emptyIterator(); }
                    @Override public void abort() { disposed = true; }
                    @Override public void dispose() { disposed = true; }
                };
            }
            @Override public void close() { }
        };
        MemoryManager memory = new MemoryManager(64 * 1024);
        PipelinedExternalSortBuffer buffer = new PipelinedExternalSortBuffer(
                META, new RowComparator(META, SORT), SORT, memory, 1, 2,
                failingStore, HeapPool::new);
        buffer.putRow(row(1, "a"));

        RuntimeException thrown = assertTimeoutPreemptively(Duration.ofSeconds(3),
                () -> assertThrows(RuntimeException.class, buffer::sort));

        assertSame(root, thrown);
        buffer.close();
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, buffer.activeTokenCountForTest());
        assertFalse(buffer.workerThreadsAliveForTest());
    }

    @Test
    void earlyIteratorCloseReleasesHeadCreditAndInvalidatesCapturedIterator() {
        MemoryManager memory = new MemoryManager(64 * 1024);
        PipelinedExternalSortBuffer buffer = new PipelinedExternalSortBuffer(
                META, new RowComparator(META, SORT), SORT, memory, 4, 2,
                new MemorySpillStore(), HeapPool::new);
        for (int i = 10; i >= 1; i--) buffer.putRow(row(i, "v" + i));
        buffer.sort();
        Iterator<DataRow> iterator = buffer.iterator();
        iterator.next();

        buffer.close();

        assertEquals(0, memory.reservedBytes());
        assertThrows(IllegalStateException.class, iterator::hasNext);
    }

    @Test
    void mergeCursorCloseFailureCannotReplaceAppendRoot() {
        RuntimeException root = new RuntimeException("merge append failed");
        RuntimeException cleanup = new RuntimeException("cursor close failed");
        RowSerializer serializer = new RowSerializer(META);
        byte[] rowBytes = serializer.serialize(row(1, "a"));
        byte[] frame = frame(0, rowBytes);
        SpillStore outputStore = new SpillStore() {
            @Override public SpillRun createRun() {
                return new SpillRun() {
                    @Override public void append(byte[] ignored) { throw root; }
                    @Override public void finishWriting() { }
                    @Override public Iterator<byte[]> read() { return java.util.Collections.emptyIterator(); }
                };
            }
            @Override public void close() { }
        };
        SortedRunStore runs = new SortedRunStore(
                META, serializer, new RowComparator(META, SORT), NormalizedKeyComputer.of(META, SORT),
                outputStore);
        runs.acceptRun(closeFailingRun(frame, cleanup));
        runs.acceptRun(closeFailingRun(frame, cleanup));

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> runs.mergeAcceptedRunsToSingle(2));

        assertSame(root, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(cleanup, thrown.getSuppressed()[0]);
    }

    @Test
    void finalKWayOpenFailureClosesEarlierCursorsAndReleasesAuxCredit() {
        RuntimeException root = new RuntimeException("second final cursor open failed");
        java.util.concurrent.atomic.AtomicInteger runIndex = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger openCursors = new java.util.concurrent.atomic.AtomicInteger();
        MemorySpillStore delegate = new MemorySpillStore();
        SpillStore store = new SpillStore() {
            @Override public SpillRun createRun() {
                int index = runIndex.getAndIncrement();
                SpillRun inner = delegate.createRun();
                return new SpillRun() {
                    @Override public void append(byte[] frame) { inner.append(frame); }
                    @Override public void finishWriting() { inner.finishWriting(); }
                    @Override public Iterator<byte[]> read() {
                        if (index == 1) throw root;
                        return new TrackingCloseIterator(inner.read(), openCursors);
                    }
                    @Override public void abort() { inner.abort(); }
                    @Override public void dispose() { inner.dispose(); }
                };
            }
            @Override public void close() { delegate.close(); }
        };
        MemoryManager memory = new MemoryManager(64 * 1024);
        PipelinedExternalSortBuffer buffer = new PipelinedExternalSortBuffer(
                META, new RowComparator(META, SORT), SORT, memory, 1, 2, store, HeapPool::new);
        buffer.putRow(row(3, "c"));
        buffer.putRow(row(2, "b"));
        buffer.putRow(row(1, "a"));
        buffer.sort();

        try {
            RuntimeException thrown = assertThrows(RuntimeException.class, buffer::iterator);
            assertSame(root, thrown);
            assertEquals(0, openCursors.get(), "cursor 0 must close when cursor 1 open fails");
            assertEquals(0, memory.reservedBytes(), "failed final iterator must release all aux credit");
        } finally {
            buffer.close();
        }
    }

    private static SpillRun closeFailingRun(byte[] frame, RuntimeException cleanup) {
        return new SpillRun() {
            @Override public void append(byte[] ignored) { throw new UnsupportedOperationException(); }
            @Override public void finishWriting() { }
            @Override public Iterator<byte[]> read() { return new CloseFailingIterator(frame, cleanup); }
        };
    }

    private static byte[] frame(long seq, byte[] rowBytes) {
        byte[] frame = new byte[12 + rowBytes.length];
        for (int i = 0; i < 8; i++) frame[i] = (byte) (seq >>> (56 - i * 8));
        int length = rowBytes.length;
        frame[8] = (byte) (length >>> 24);
        frame[9] = (byte) (length >>> 16);
        frame[10] = (byte) (length >>> 8);
        frame[11] = (byte) length;
        System.arraycopy(rowBytes, 0, frame, 12, rowBytes.length);
        return frame;
    }

    private static final class CloseFailingIterator implements Iterator<byte[]>, AutoCloseable {
        private final byte[] frame;
        private final RuntimeException cleanup;
        private boolean emitted;

        private CloseFailingIterator(byte[] frame, RuntimeException cleanup) {
            this.frame = frame;
            this.cleanup = cleanup;
        }

        @Override public boolean hasNext() { return !emitted; }
        @Override public byte[] next() {
            if (emitted) throw new NoSuchElementException();
            emitted = true;
            return frame;
        }
        @Override public void close() { throw cleanup; }
    }

    private static final class TrackingCloseIterator implements Iterator<byte[]>, AutoCloseable {
        private final Iterator<byte[]> delegate;
        private final java.util.concurrent.atomic.AtomicInteger open;
        private boolean closed;

        private TrackingCloseIterator(Iterator<byte[]> delegate,
                                      java.util.concurrent.atomic.AtomicInteger open) {
            this.delegate = delegate;
            this.open = open;
            open.incrementAndGet();
        }

        @Override public boolean hasNext() { return delegate.hasNext(); }
        @Override public byte[] next() { return delegate.next(); }
        @Override public void close() {
            if (!closed) {
                closed = true;
                open.decrementAndGet();
            }
        }
    }

    private static class DelegatingSegment implements MemorySegment {
        final MemorySegment delegate;
        DelegatingSegment(MemorySegment delegate) { this.delegate = delegate; }
        @Override public int size() { return delegate.size(); }
        @Override public void putByte(int offset, byte value) { delegate.putByte(offset, value); }
        @Override public byte getByte(int offset) { return delegate.getByte(offset); }
        @Override public void putInt(int offset, int value) { delegate.putInt(offset, value); }
        @Override public int getInt(int offset) { return delegate.getInt(offset); }
        @Override public void putLong(int offset, long value) { delegate.putLong(offset, value); }
        @Override public long getLong(int offset) { return delegate.getLong(offset); }
        @Override public void putBytes(int offset, byte[] src) { delegate.putBytes(offset, src); }
        @Override public byte[] getBytes(int offset, int length) { return delegate.getBytes(offset, length); }
        @Override public void copyTo(int offset, byte[] target, int targetOffset, int length) {
            delegate.copyTo(offset, target, targetOffset, length);
        }
        @Override public void clear() { delegate.clear(); }
    }
}
