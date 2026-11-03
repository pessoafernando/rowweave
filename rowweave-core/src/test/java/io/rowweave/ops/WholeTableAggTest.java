package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WholeTableAggTest {

    private static final DataSchema META = new DataSchema(
            new Field("amt", DataType.BigDecimalType),
            new Field("q", DataType.IntegerType));

    private static DataRow row(BigDecimal amt, Integer q) {
        DataRow r = new DataRow(2);
        r.set(0, amt);
        r.set(1, q);
        return r;
    }

    private static Object aggOne(WholeTableAgg agg, List<DataRow> rows) {
        CollectingRowSink sink = new CollectingRowSink();
        agg.apply(RowStream.fromIterable(rows), sink);
        assertTrue(sink.sawEndOfStream());
        assertEquals(1, sink.rows().size(), "whole-table agg must emit exactly 1 row");
        return sink.rows().get(0).get(0);
    }

    @Test
    void countAllCountsEveryRowIncludingNulls() {
        WholeTableAgg agg = WholeTableAgg.of(META, WholeTableAgg.Kind.COUNT_ALL, null);
        List<DataRow> in = Arrays.asList(row(null, null), row(new BigDecimal("1"), 1), row(null, 2));
        assertEquals(3L, aggOne(agg, in));
    }

    @Test
    void countFieldCountsNonNullOnly() {
        WholeTableAgg agg = WholeTableAgg.of(META, WholeTableAgg.Kind.COUNT_FIELD, "amt");
        List<DataRow> in = Arrays.asList(row(new BigDecimal("1"), 1), row(null, 2), row(new BigDecimal("3"), 3));
        assertEquals(2L, aggOne(agg, in));
    }

    @Test
    void sumScale10HalfUpSkipsNull() {
        WholeTableAgg agg = WholeTableAgg.of(META, WholeTableAgg.Kind.SUM, "amt");
        List<DataRow> in = Arrays.asList(
                row(new BigDecimal("10.5"), 1), row(null, 2), row(new BigDecimal("4.5"), 3));
        assertEquals(new BigDecimal("15.0000000000"), aggOne(agg, in)); // 10.5+4.5, scale10
    }

    @Test
    void sumEmptyOrAllNullReturnsNull() {
        WholeTableAgg agg = WholeTableAgg.of(META, WholeTableAgg.Kind.SUM, "amt");
        assertNull(aggOne(agg, new ArrayList<>())); // empty table
        assertNull(aggOne(agg, Arrays.asList(row(null, 1), row(null, 2)))); // all null
    }

    @Test
    void maxMinIgnoreNullEmptyReturnsNull() {
        WholeTableAgg max = WholeTableAgg.of(META, WholeTableAgg.Kind.MAX, "q");
        WholeTableAgg min = WholeTableAgg.of(META, WholeTableAgg.Kind.MIN, "q");
        List<DataRow> in = Arrays.asList(row(null, 5), row(null, null), row(null, 2), row(null, 9));
        assertEquals(9, aggOne(max, in));
        assertEquals(2, aggOne(min, in));
        assertNull(aggOne(WholeTableAgg.of(META, WholeTableAgg.Kind.MAX, "q"), new ArrayList<>()));
    }

    @Test
    void countAllEmptyIsZero() {
        WholeTableAgg agg = WholeTableAgg.of(META, WholeTableAgg.Kind.COUNT_ALL, null);
        assertEquals(0L, aggOne(agg, new ArrayList<>()));
    }

    @Test
    void errorEventForwardedAndStops() {
        WholeTableAgg agg = WholeTableAgg.of(META, WholeTableAgg.Kind.SUM, "amt");
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row(new BigDecimal("1"), 1)),
                StreamEvent.error(boom));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;
        CollectingRowSink sink = new CollectingRowSink();
        agg.apply(in, sink);
        assertTrue(sink.rows().isEmpty()); // breaker errored before producing
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
        assertFalse(sink.sawEndOfStream());
    }

    @Test
    void unknownFieldRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> WholeTableAgg.of(META, WholeTableAgg.Kind.SUM, "nope"));
    }
}
