package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.PipelinedExternalSortBuffer;
import io.rowweave.mem.SortBuffer;
import io.rowweave.mem.SpillingSortBuffer;
import io.rowweave.mem.TwoTierSortBuffer;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class OrderByTest {

    private static final DataSchema NUM = new DataSchema(new Field("n", DataType.IntegerType));

    private static DataRow row(Integer v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    private static List<Integer> col0(List<DataRow> rows) {
        List<Integer> out = new ArrayList<>();
        for (DataRow r : rows) {
            out.add((Integer) r.get(0));
        }
        return out;
    }

    private static List<Integer> expected(List<Integer> input, RowComparator cmp) {
        List<DataRow> rows = new ArrayList<>();
        for (Integer v : input) {
            rows.add(row(v));
        }
        rows.sort(cmp); // JDK stable sort = authority
        return col0(rows);
    }

    private static List<Integer> sort(OrderBy ob, List<Integer> input, int par, boolean parallel) {
        List<DataRow> rows = new ArrayList<>();
        for (Integer v : input) {
            rows.add(row(v));
        }
        CollectingRowSink sink = new CollectingRowSink();
        ob.applyStream(RowStream.fromIterable(rows), par, parallel, sink);
        assertTrue(sink.sawEndOfStream(), "OrderBy must emit EOS");
        return col0(sink.rows());
    }

    @Test
    void singleThreadMatchesAuthority() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        RowComparator cmp = new RowComparator(NUM, sf);
        OrderBy ob = OrderBy.of(NUM, sf);
        List<Integer> input = Arrays.asList(3, null, Integer.MIN_VALUE, 0, Integer.MAX_VALUE, -1, null, 2);
        assertEquals(expected(input, cmp), sort(ob, input, 1, false));
    }

    @Test
    void parallelismInvariantSameOutput() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        RowComparator cmp = new RowComparator(NUM, sf);
        OrderBy ob = OrderBy.of(NUM, sf);
        List<Integer> input = new ArrayList<>();
        for (int i = 50; i >= 1; i--) {
            input.add(i % 5 == 0 ? null : i);
        }
        List<Integer> authority = expected(input, cmp);
        assertEquals(authority, sort(ob, input, 1, false));
        assertEquals(authority, sort(ob, input, 2, true));
        assertEquals(authority, sort(ob, input, 3, true));
        assertEquals(authority, sort(ob, input, 7, true));
    }

    @Test
    void descAndNullsFirstLast() {
        for (SortField.NullOrder no : SortField.NullOrder.values()) {
            for (SortField.Direction dir : SortField.Direction.values()) {
                SortField sf = new SortField(0, dir, no);
                RowComparator cmp = new RowComparator(NUM, sf);
                OrderBy ob = OrderBy.of(NUM, sf);
                List<Integer> input = Arrays.asList(5, null, 1, Integer.MAX_VALUE, null, Integer.MIN_VALUE, 3);
                assertEquals(expected(input, cmp), sort(ob, input, 3, true),
                        "mismatch for dir=" + dir + " nullOrder=" + no);
            }
        }
    }

    @Test
    void stabilityEqualKeysKeepInsertionOrder() {
        DataSchema meta = new DataSchema(
                new Field("k", DataType.IntegerType),
                new Field("w", DataType.IntegerType)); // witness
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        OrderBy ob = OrderBy.of(meta, sf);
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            DataRow r = new DataRow(2);
            r.set(0, 1);   // all equal key
            r.set(1, i);   // witness = insertion order
            rows.add(r);
        }
        CollectingRowSink sink = new CollectingRowSink();
        ob.applyStream(RowStream.fromIterable(rows), 3, true, sink);
        List<Integer> witnesses = new ArrayList<>();
        for (DataRow r : sink.rows()) {
            witnesses.add((Integer) r.get(1));
        }
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), witnesses);
    }

    @Test
    void spillingSortBufferDoesNotOom() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        RowComparator cmp = new RowComparator(NUM, sf);
        OrderBy ob = OrderBy.of(NUM, sf)
                .withSortBufferFactory((m, c) -> new SpillingSortBuffer(
                        m, c, new MemoryManager(64 * 1024), 2));
        List<Integer> input = new ArrayList<>();
        for (int i = 300; i >= 1; i--) {
            input.add(i % 9 == 0 ? null : i);
        }
        assertEquals(expected(input, cmp), sort(ob, input, 2, true));
    }

    @Test
    void emptyAndSingle() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        OrderBy ob = OrderBy.of(NUM, sf);
        assertTrue(sort(ob, new ArrayList<>(), 3, true).isEmpty());
        assertEquals(Arrays.asList(42), sort(ob, Arrays.asList(42), 3, true));
    }

    @Test
    void parallelismMustBePositive() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        OrderBy ob = OrderBy.of(NUM, sf);
        assertThrows(IllegalArgumentException.class,
                () -> ob.applyStream(RowStream.fromIterable(new ArrayList<>()), 0, false, new CollectingRowSink()));
    }

    @Test
    void requiresAtLeastOneSortField() {
        assertThrows(IllegalArgumentException.class, () -> OrderBy.of(NUM));
    }

    @Test
    void errorEventForwardedAndStops() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        OrderBy ob = OrderBy.of(NUM, sf);
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row(3)),
                StreamEvent.error(boom),
                StreamEvent.record(row(1)));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;
        CollectingRowSink sink = new CollectingRowSink();
        ob.applyStream(in, 1, false, sink);
        // breaker must not emit sorted data when upstream errored mid-stream
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
        assertFalse(sink.sawEndOfStream());
    }

    @Test
    void rowWidthMismatchRejected() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        OrderBy ob = OrderBy.of(NUM, sf); // expects width 1
        DataRow wide = new DataRow(2); // width 2 != meta width 1
        wide.set(0, 1);
        wide.set(1, 99);
        CollectingRowSink sink = new CollectingRowSink();
        assertThrows(IllegalStateException.class,
                () -> ob.applyStream(RowStream.fromIterable(Arrays.asList(wide)), 1, false, sink));
    }

    @Test
    void upstreamControlFailureStillClosesEveryConstructedBuffer() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        AtomicInteger closes = new AtomicInteger();
        OrderBy orderBy = OrderBy.of(NUM, sf).withSortBufferFactory((meta, comparator) -> new SortBuffer() {
            @Override public void putRow(DataRow row) { }
            @Override public void sort() { }
            @Override public long size() { return 0; }
            @Override public Iterator<DataRow> iterator() { return java.util.Collections.emptyIterator(); }
            @Override public void close() { closes.incrementAndGet(); }
        });
        RuntimeException root = new RuntimeException("upstream");
        List<StreamEvent> events = Arrays.asList(StreamEvent.record(row(1)), StreamEvent.error(root));
        Iterator<StreamEvent> iterator = events.iterator();

        orderBy.applyStream(() -> iterator.hasNext() ? iterator.next() : null,
                3, true, new CollectingRowSink());

        assertEquals(3, closes.get());
    }

    @Test
    void parallelPartitionFailureIsPropagatedAndAllBuffersClose() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        AtomicInteger created = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        RuntimeException root = new RuntimeException("partition-2");
        OrderBy orderBy = OrderBy.of(NUM, sf).withSortBufferFactory((meta, comparator) -> {
            int index = created.getAndIncrement();
            return new SortBuffer() {
                @Override public void putRow(DataRow row) { }
                @Override public void sort() { if (index == 2) throw root; }
                @Override public long size() { return 0; }
                @Override public Iterator<DataRow> iterator() { return java.util.Collections.emptyIterator(); }
                @Override public void close() { closes.incrementAndGet(); }
            };
        });

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> orderBy.applyStream(RowStream.fromIterable(Arrays.asList(row(3), row(2), row(1))),
                        3, true, new CollectingRowSink()));

        assertSame(root, thrown);
        assertEquals(3, closes.get());
    }

    @Test
    void onePartitionSpillCoordinatesAllPartitionsToExternalAndReleasesSharedMemory() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        RowComparator authority = new RowComparator(NUM, sf);
        MemoryManager memory = new MemoryManager(64 * 1024);
        List<TwoTierSortBuffer> created = new ArrayList<>();
        OrderBy orderBy = OrderBy.of(NUM, sf).withSortBufferFactory((meta, comparator) -> {
            TwoTierSortBuffer buffer = new TwoTierSortBuffer(meta, comparator, memory,
                    comparator.getSortFields(), 8, 3,
                    () -> new PipelinedExternalSortBuffer(meta, comparator, comparator.getSortFields(),
                            memory, 8, 3, new MemorySpillStore(), HeapPool::new));
            created.add(buffer);
            return buffer;
        });
        List<Integer> input = new ArrayList<>();
        for (int i = 240; i >= 1; i--) {
            input.add(i % 17 == 0 ? null : i);
        }

        List<Integer> actual = sort(orderBy, input, 4, true);

        assertEquals(expected(input, authority), actual);
        assertEquals(4, created.size());
        assertTrue(created.stream().allMatch(TwoTierSortBuffer::spilledForTest),
                "any secondary transition must externalize all partitions before further ingest/sort");
        assertEquals(0, memory.reservedBytes(), "OrderBy finally closes all partition buffers");
        assertTrue(memory.peakReservedBytes() <= memory.budget());
    }

    @Test
    void constructorFailureClosesOnlyPreviouslyCreatedBuffers() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        AtomicInteger index = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        RuntimeException root = new RuntimeException("factory-2");
        OrderBy orderBy = OrderBy.of(NUM, sf).withSortBufferFactory((meta, comparator) -> {
            if (index.getAndIncrement() == 2) throw root;
            return emptyTrackingBuffer(closes);
        });

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> orderBy.applyStream(RowStream.fromIterable(new ArrayList<>()),
                        4, true, new CollectingRowSink()));

        assertSame(root, thrown);
        assertEquals(2, closes.get());
    }

    @Test
    void callerInterruptCancelsThenJoinsWorkersAndRestoresFlag() throws Exception {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        CountDownLatch workersStarted = new CountDownLatch(3);
        AtomicInteger closes = new AtomicInteger();
        AtomicReference<Throwable> seen = new AtomicReference<>();
        AtomicBoolean interruptedAtExit = new AtomicBoolean();
        OrderBy orderBy = OrderBy.of(NUM, sf).withSortBufferFactory((meta, comparator) -> new SortBuffer() {
            @Override public void putRow(DataRow row) { }
            @Override public void sort() {
                workersStarted.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            @Override public long size() { return 0; }
            @Override public Iterator<DataRow> iterator() { return java.util.Collections.emptyIterator(); }
            @Override public void close() { closes.incrementAndGet(); }
        });
        Thread caller = new Thread(() -> {
            try {
                orderBy.applyStream(RowStream.fromIterable(Arrays.asList(row(3), row(2), row(1))),
                        3, true, new CollectingRowSink());
            } catch (Throwable failure) {
                seen.set(failure);
            } finally {
                interruptedAtExit.set(Thread.currentThread().isInterrupted());
            }
        }, "orderby-interrupt-caller");
        caller.start();
        assertTrue(workersStarted.await(2, java.util.concurrent.TimeUnit.SECONDS));

        caller.interrupt();
        caller.join(3000);

        assertFalse(caller.isAlive());
        assertTrue(seen.get() instanceof RuntimeException);
        assertTrue(interruptedAtExit.get(), "caller interrupt flag restored after all workers joined");
        assertEquals(3, closes.get());
    }

    @Test
    void lowerIndexInducedCancellationCannotReplaceHigherIndexOriginalFailure() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        AtomicInteger index = new AtomicInteger();
        CountDownLatch lowStarted = new CountDownLatch(1);
        RuntimeException root = new RuntimeException("real-partition-2");
        RuntimeException induced = new RuntimeException("cancelled-partition-0");
        OrderBy orderBy = OrderBy.of(NUM, sf).withSortBufferFactory((meta, comparator) -> {
            int partition = index.getAndIncrement();
            return new SortBuffer() {
                @Override public void putRow(DataRow row) { }
                @Override public void sort() {
                    if (partition == 0) {
                        lowStarted.countDown();
                        try {
                            new CountDownLatch(1).await();
                        } catch (InterruptedException ignored) {
                            throw induced;
                        }
                    }
                    if (partition == 2) {
                        try {
                            lowStarted.await();
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                        throw root;
                    }
                }
                @Override public long size() { return 0; }
                @Override public Iterator<DataRow> iterator() { return java.util.Collections.emptyIterator(); }
            };
        });

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> orderBy.applyStream(RowStream.fromIterable(Arrays.asList(row(3), row(2), row(1))),
                        3, true, new CollectingRowSink()));

        assertSame(root, thrown);
        assertTrue(Arrays.asList(root.getSuppressed()).contains(induced));
    }

    private static SortBuffer emptyTrackingBuffer(AtomicInteger closes) {
        return new SortBuffer() {
            @Override public void putRow(DataRow row) { }
            @Override public void sort() { }
            @Override public long size() { return 0; }
            @Override public Iterator<DataRow> iterator() { return java.util.Collections.emptyIterator(); }
            @Override public void close() { closes.incrementAndGet(); }
        };
    }
}
