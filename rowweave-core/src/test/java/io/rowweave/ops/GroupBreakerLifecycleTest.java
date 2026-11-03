package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.GroupAggBuffer;
import io.rowweave.mem.HeapGroupAggBuffer;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

    /** Distinct and GroupBy are the only lifecycle owners of their GroupAggBuffer. */
class GroupBreakerLifecycleTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("v", DataType.BigDecimalType));

    @Test
    void distinctClosesBufferAfterSuccessAndErrorControlEvent() {
        AtomicReference<TrackingBuffer> successBuffer = new AtomicReference<>();
        Distinct success = Distinct.allColumns(META).withBufferFactory(factory ->
                capture(successBuffer, factory, null));
        CollectingRowSink successSink = new CollectingRowSink();
        success.dedup(RowStream.fromIterable(Arrays.asList(row("a", "1"), row("a", "1"))), successSink);
        assertTrue(successSink.sawEndOfStream());
        assertEquals(1, successBuffer.get().closeCalls);

        AtomicReference<TrackingBuffer> errorBuffer = new AtomicReference<>();
        Distinct error = Distinct.allColumns(META).withBufferFactory(factory ->
                capture(errorBuffer, factory, null));
        RuntimeException root = new RuntimeException("upstream-error");
        CollectingRowSink errorSink = new CollectingRowSink();
        error.dedup(events(StreamEvent.record(row("a", "1")), StreamEvent.error(root)), errorSink);
        assertSame(root, errorSink.error());
        assertEquals(1, errorBuffer.get().closeCalls);
    }

    @Test
    void groupByClosesBufferAfterSuccessAndCancelControlEvent() {
        AtomicReference<TrackingBuffer> successBuffer = new AtomicReference<>();
        ParallelGroupByAgg success = ParallelGroupByAgg.of(META, "k").sum("v")
                .withBufferFactory(factory -> capture(successBuffer, factory, null));
        CollectingRowSink successSink = new CollectingRowSink();
        success.aggregate(RowStream.fromIterable(Arrays.asList(row("a", "1"), row("a", "2"))), successSink);
        assertTrue(successSink.sawEndOfStream());
        assertEquals(1, successBuffer.get().closeCalls);

        AtomicReference<TrackingBuffer> cancelBuffer = new AtomicReference<>();
        ParallelGroupByAgg cancel = ParallelGroupByAgg.of(META, "k").sum("v")
                .withBufferFactory(factory -> capture(cancelBuffer, factory, null));
        CollectingRowSink cancelSink = new CollectingRowSink();
        cancel.aggregate(events(StreamEvent.record(row("a", "1")), StreamEvent.cancel()), cancelSink);
        assertTrue(!cancelSink.sawEndOfStream());
        assertEquals(1, cancelBuffer.get().closeCalls);
    }

    @Test
    void downstreamFailureStaysRootAndCloseFailureIsSuppressed() {
        RuntimeException business = new RuntimeException("downstream-root");
        RuntimeException cleanup = new RuntimeException("close-cleanup");
        AtomicReference<TrackingBuffer> buffer = new AtomicReference<>();
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("v")
                .withBufferFactory(factory -> capture(buffer, factory, cleanup));
        RowSink failingSink = event -> {
            if (event.isRecord()) {
                throw business;
            }
        };

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> agg.aggregate(RowStream.fromIterable(Arrays.asList(row("a", "1"))), failingSink));

        assertSame(business, actual);
        assertEquals(1, buffer.get().closeCalls);
        assertEquals(1, business.getSuppressed().length);
        assertSame(cleanup, business.getSuppressed()[0]);
    }

    @Test
    void distinctClosesBufferWhenDownstreamRejectsFirstRow() {
        RuntimeException business = new RuntimeException("distinct-downstream");
        AtomicReference<TrackingBuffer> buffer = new AtomicReference<>();
        Distinct distinct = Distinct.allColumns(META).withBufferFactory(factory ->
                capture(buffer, factory, null));

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> distinct.dedup(RowStream.fromIterable(Arrays.asList(row("a", "1"))), event -> {
                    if (event.isRecord()) {
                        throw business;
                    }
                }));

        assertSame(business, actual);
        assertEquals(1, buffer.get().closeCalls);
    }

    private static TrackingBuffer capture(AtomicReference<TrackingBuffer> target,
                                          Supplier<Object[]> factory, RuntimeException closeFailure) {
        TrackingBuffer buffer = new TrackingBuffer(factory, closeFailure);
        target.set(buffer);
        return buffer;
    }

    private static DataRow row(String key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, new BigDecimal(value));
        return row;
    }

    private static RowStream events(StreamEvent... events) {
        Iterator<StreamEvent> iterator = Arrays.asList(events).iterator();
        return () -> iterator.hasNext() ? iterator.next() : null;
    }

    private static final class TrackingBuffer implements GroupAggBuffer, AutoCloseable {
        private final HeapGroupAggBuffer delegate;
        private final RuntimeException closeFailure;
        private int closeCalls;

        private TrackingBuffer(Supplier<Object[]> factory, RuntimeException closeFailure) {
            this.delegate = new HeapGroupAggBuffer(factory);
            this.closeFailure = closeFailure;
        }

        @Override
        public Object[] getOrCreate(byte[] keyBytes) {
            return delegate.getOrCreate(keyBytes);
        }

        @Override
        public long groupCount() {
            return delegate.groupCount();
        }

        @Override
        public Iterator<Group> iterator() {
            return delegate.iterator();
        }

        @Override
        public void close() {
            closeCalls++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
