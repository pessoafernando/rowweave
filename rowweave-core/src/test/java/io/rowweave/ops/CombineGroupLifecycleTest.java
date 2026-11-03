package io.rowweave.ops;

import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.GroupCombineFunction;
import io.rowweave.GroupCombineReduceFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

    /** CombineGroup owns the lifecycle of the primary and partial blocks and of the injected SpillStore. */
class CombineGroupLifecycleTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    @TempDir
    Path tempDir;

    @Test
    void successfulCombineClosesAbandonedReaderRunAndStore() throws Exception {
        DiskSpillStore store = new DiskSpillStore(tempDir.toFile());
        GroupCombineFunction takeFirst = (rows, out) -> {
            Iterator<DataRow> iterator = rows.iterator();
            if (iterator.hasNext()) {
                out.collect(iterator.next());  // deliberately abandon the spill reader early
            }
        };
        CombineGroup combine = CombineGroup.of(META, takeFirst).withSpill(store);
        CollectingRowSink sink = new CollectingRowSink();

        combine.applyStream(RowStream.fromIterable(rows(1_100)), 1, sink);

        assertTrue(sink.sawEndOfStream());
        assertEquals(1, sink.rows().size());
        assertTrue(store.runsCreated() > 0);
        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
        assertEquals(0L, fileCount(tempDir));
        assertThrows(IllegalStateException.class,
                () -> combine.applyStream(RowStream.fromIterable(rows(1)), 1, new CollectingRowSink()),
                "once the owned store is closed CombineGroup is explicitly single-use");
    }

    @Test
    void userFailureRemainsRootAndStillClosesSpillResources() throws Exception {
        DiskSpillStore store = new DiskSpillStore(tempDir.toFile());
        RuntimeException business = new RuntimeException("combine-root");
        GroupCombineFunction failAfterFirst = (rows, out) -> {
            Iterator<DataRow> iterator = rows.iterator();
            assertTrue(iterator.hasNext());
            iterator.next();
            throw business;
        };
        CombineGroup combine = CombineGroup.of(META, failAfterFirst).withSpill(store);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> combine.applyStream(RowStream.fromIterable(rows(1_100)), 1, new CollectingRowSink()));

        assertSame(business, actual);
        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
        assertEquals(0L, fileCount(tempDir));
    }

    @Test
    void cancelAfterSpillClosesStoreInsteadOfOnlyAbortingCurrentRun() {
        DiskSpillStore store = new DiskSpillStore(tempDir.toFile());
        CombineGroup combine = CombineGroup.of(META, (rows, out) -> { }).withSpill(store);
        List<StreamEvent> events = new ArrayList<>();
        for (DataRow row : rows(1_100)) {
            events.add(StreamEvent.record(row));
        }
        events.add(StreamEvent.cancel());
        Iterator<StreamEvent> iterator = events.iterator();
        CollectingRowSink sink = new CollectingRowSink();

        combine.applyStream(() -> iterator.hasNext() ? iterator.next() : null, 1, sink);

        assertThrows(IllegalStateException.class, store::createRun, "the Combine owner closes the store");
        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
    }

    @Test
    void downstreamRootWinsAndStoreCloseFailureIsSuppressed() {
        RuntimeException business = new RuntimeException("downstream-root");
        RuntimeException cleanup = new RuntimeException("store-close-cleanup");
        AtomicInteger closeCalls = new AtomicInteger();
        SpillStore store = new SpillStore() {
            private final MemorySpillStore delegate = new MemorySpillStore();
            @Override public SpillRun createRun() { return delegate.createRun(); }
            @Override public void close() {
                closeCalls.incrementAndGet();
                delegate.close();
                throw cleanup;
            }
        };
        CombineGroup combine = CombineGroup.of(META, (rows, out) -> out.collect(rows.iterator().next()))
                .withSpill(store);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> combine.applyStream(RowStream.fromIterable(rows(1)), 1, event -> {
                    if (event.isRecord()) {
                        throw business;
                    }
                }));

        assertSame(business, actual);
        assertEquals(1, closeCalls.get());
        assertEquals(1, business.getSuppressed().length);
        assertSame(cleanup, business.getSuppressed()[0]);
    }

    @Test
    void combineReduceDisposesPartialRunBeforeClosingStore() {
        TrackingStore store = new TrackingStore();
        GroupCombineReduceFunction function = new GroupCombineReduceFunction() {
            @Override public DataSchema getResultSchema() { return META; }
            @Override public void combine(Iterable<DataRow> rows, Collector out) {
                DataRow first = rows.iterator().next();
                for (int i = 0; i < 1_100; i++) {
                    out.collect(first);  // force the partial block to spill
                }
            }
            @Override public void reduce(Iterable<DataRow> rows, Collector out) {
                out.collect(rows.iterator().next());  // abandon the partial reader early
            }
        };
        CombineGroup combine = CombineGroup.ofCombineReduce(META, function).withSpill(store);

        combine.applyStream(RowStream.fromIterable(rows(1)), 1, new CollectingRowSink());

        assertEquals(1, store.created.get());
        assertEquals(1, store.disposeCalls.get());
        assertEquals(0, store.liveAtClose, "a partial block disposes its own run first and then closes the store as a fallback");
        assertEquals(1, store.closeCalls.get());
    }

    private static List<DataRow> rows(int count) {
        List<DataRow> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            DataRow row = new DataRow(1);
            row.set(0, i);
            rows.add(row);
        }
        return rows;
    }

    private static long fileCount(Path dir) throws Exception {
        try (Stream<Path> files = Files.list(dir)) {
            return files.count();
        }
    }

    private static final class TrackingStore implements SpillStore {
        private final AtomicInteger created = new AtomicInteger();
        private final AtomicInteger disposeCalls = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();
        private int liveRuns;
        private int liveAtClose = -1;

        @Override
        public SpillRun createRun() {
            created.incrementAndGet();
            liveRuns++;
            return new SpillRun() {
                private final List<byte[]> frames = new ArrayList<>();
                private boolean disposed;
                @Override public void append(byte[] frame) { frames.add(frame.clone()); }
                @Override public void finishWriting() { }
                @Override public Iterator<byte[]> read() { return frames.iterator(); }
                @Override public void dispose() {
                    if (disposed) return;
                    disposed = true;
                    disposeCalls.incrementAndGet();
                    liveRuns--;
                    frames.clear();
                }
            };
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
            liveAtClose = liveRuns;
        }
    }
}
