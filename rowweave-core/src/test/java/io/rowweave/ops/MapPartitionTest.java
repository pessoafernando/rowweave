package io.rowweave.ops;

import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.MapPartitionEmissionLimit;
import io.rowweave.MapPartitionFunction;
import io.rowweave.PartitionDataRowCursor;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapPartitionTest {
    private static final DataSchema META = new DataSchema(
            new Field("value", DataType.LongType));

    @Test
    void invokesEmptyPartitionOnceAndMayEmit() {
        AtomicInteger calls = new AtomicInteger();
        MapPartition operator = MapPartition.of(function((rows, out) -> {
            calls.incrementAndGet();
            assertEquals(null, rows.next());
            out.collect(row(41L));
        }), META, new MapPartitionEmissionLimit(1L, 64L));
        CollectingRowSink sink = new CollectingRowSink();

        operator.apply(RowStream.fromIterable(Collections.emptyList()), sink);

        assertEquals(1, calls.get());
        assertEquals(Collections.singletonList(41L), values(sink));
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void emitsWhileCursorIsStillStreamingThenDrainsEarlyReturn() {
        AtomicInteger pulls = new AtomicInteger();
        java.util.Iterator<DataRow> input = Arrays.asList(
                row(1L), row(2L), row(3L)).iterator();
        RowStream stream = () -> {
            pulls.incrementAndGet();
            return input.hasNext() ? StreamEvent.record(input.next()) : null;
        };
        AtomicReference<PartitionDataRowCursor> retainedRows = new AtomicReference<>();
        AtomicReference<Collector> retainedCollector = new AtomicReference<>();
        CollectingRowSink sink = new CollectingRowSink();
        MapPartition operator = MapPartition.of(function((rows, out) -> {
            retainedRows.set(rows);
            retainedCollector.set(out);
            out.collect(rows.next());
            assertEquals(1, pulls.get(), "first output must precede full input drain");
            // Deliberately return with two records unread. Runtime must drain them.
        }), META, new MapPartitionEmissionLimit(3L, 64L));

        operator.apply(stream, sink);

        assertEquals(Collections.singletonList(1L), values(sink));
        assertEquals(4, pulls.get(), "three records plus terminal pull must be consumed");
        assertThrows(IllegalStateException.class,
                () -> retainedRows.get().next());
        assertThrows(IllegalStateException.class,
                () -> retainedCollector.get().collect(row(9L)));
    }

    @Test
    void preservesFunctionFailureAsThePrimaryCause() {
        RuntimeException boom = new RuntimeException("boom");
        MapPartition operator = MapPartition.of(function((rows, out) -> {
            throw boom;
        }), META, new MapPartitionEmissionLimit(1L, 64L));

        RuntimeException failure = assertThrows(RuntimeException.class,
                () -> operator.apply(
                        RowStream.fromIterable(Collections.singletonList(row(1L))),
                        event -> { }));

        assertSame(boom, failure);
    }

    @Test
    void rejectsRowsBytesAndMalformedOutputBeforeDownstreamEffect() {
        AtomicInteger downstream = new AtomicInteger();
        MapPartition rowsExceeded = MapPartition.of(function((rows, out) -> {
            out.collect(row(1L));
            out.collect(row(2L));
        }), META, new MapPartitionEmissionLimit(1L, 64L));
        IllegalStateException rowsFailure = assertThrows(
                IllegalStateException.class,
                () -> rowsExceeded.apply(RowStream.fromIterable(
                        Collections.emptyList()), event -> downstream.incrementAndGet()));
        assertTrue(rowsFailure.getMessage().contains(
                "MAP_PARTITION_OUTPUT_ROWS_LIMIT_EXCEEDED"));
        assertEquals(1, downstream.get());

        downstream.set(0);
        MapPartition bytesExceeded = MapPartition.of(function((rows, out) ->
                out.collect(row(1L))), META,
                new MapPartitionEmissionLimit(1L, 8L));
        IllegalStateException bytesFailure = assertThrows(
                IllegalStateException.class,
                () -> bytesExceeded.apply(RowStream.fromIterable(
                        Collections.emptyList()), event -> downstream.incrementAndGet()));
        assertTrue(bytesFailure.getMessage().contains(
                "MAP_PARTITION_OUTPUT_BYTES_LIMIT_EXCEEDED"));
        assertEquals(0, downstream.get());

        MapPartition nullOutput = MapPartition.of(function((rows, out) ->
                out.collect(null)), META,
                new MapPartitionEmissionLimit(1L, 64L));
        assertThrows(IllegalArgumentException.class,
                () -> nullOutput.apply(RowStream.fromIterable(
                        Collections.emptyList()), event -> downstream.incrementAndGet()));

        MapPartition wrongWidth = MapPartition.of(function((rows, out) ->
                out.collect(new DataRow(2))), META,
                new MapPartitionEmissionLimit(1L, 64L));
        assertThrows(IllegalArgumentException.class,
                () -> wrongWidth.apply(RowStream.fromIterable(
                        Collections.emptyList()), event -> downstream.incrementAndGet()));
        assertEquals(0, downstream.get());
    }

    private static MapPartitionFunction function(Body body) {
        return new MapPartitionFunction() {
            private static final long serialVersionUID = 1L;
            @Override public void mapPartition(
                    PartitionDataRowCursor rows, Collector collector) {
                body.run(rows, collector);
            }
            @Override public DataSchema getResultSchema() { return META; }
        };
    }

    private static DataRow row(long value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static java.util.List<Long> values(CollectingRowSink sink) {
        java.util.List<Long> values = new java.util.ArrayList<>();
        for (DataRow row : sink.rows()) values.add((Long) row.get(0));
        return values;
    }

    @FunctionalInterface
    private interface Body {
        void run(PartitionDataRowCursor rows, Collector collector);
    }
}
