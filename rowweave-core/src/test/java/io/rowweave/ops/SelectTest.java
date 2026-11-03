package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SelectTest {

    private static final DataSchema IN_META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("name", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));

    private static DataRow row(Object a, Object b, Object c) {
        DataRow r = new DataRow(3);
        r.set(0, a);
        r.set(1, b);
        r.set(2, c);
        return r;
    }

    @Test
    void prunesColumnsByIndex() {
        Select s = Select.byIndex(IN_META, 0, 2); // keep id, amount
        DataRow out = s.project(row(1L, "x", new java.math.BigDecimal("9.50")));
        assertEquals(2, out.size());
        assertEquals(1L, out.get(0));
        assertEquals(new java.math.BigDecimal("9.50"), out.get(1));
    }

    @Test
    void outputSchemaTakesSelectedFields() {
        Select s = Select.byIndex(IN_META, 0, 2);
        DataSchema out = s.getOutputSchema();
        assertEquals(2, out.getFieldCount());
        assertEquals("id", out.getField(0).getName());
        assertEquals(DataType.LongType, out.getField(0).getDataType());
        assertEquals("amount", out.getField(1).getName());
        assertEquals(DataType.BigDecimalType, out.getField(1).getDataType());
    }

    @Test
    void reordersColumns() {
        Select s = Select.byIndex(IN_META, 2, 1, 0);
        DataRow out = s.project(row(1L, "x", new java.math.BigDecimal("9.50")));
        assertEquals(new java.math.BigDecimal("9.50"), out.get(0));
        assertEquals("x", out.get(1));
        assertEquals(1L, out.get(2));
    }

    @Test
    void allowsDuplicateColumn() {
        Select s = Select.byIndex(IN_META, 1, 1);
        DataRow out = s.project(row(1L, "x", null));
        assertEquals("x", out.get(0));
        assertEquals("x", out.get(1));
    }

    @Test
    void nullValuesArePreserved() {
        Select s = Select.byIndex(IN_META, 1, 2);
        DataRow out = s.project(row(1L, null, null));
        assertNull(out.get(0));
        assertNull(out.get(1));
    }

    @Test
    void streamProjectsRecordsAndSignalsEos() {
        Select s = Select.byIndex(IN_META, 0, 1);
        CollectingRowSink sink = new CollectingRowSink();
        List<DataRow> in = Arrays.asList(row(1L, "a", null), row(2L, "b", null));
        s.apply(RowStream.fromIterable(in), sink);
        assertEquals(2, sink.rows().size());
        assertEquals(1L, sink.rows().get(0).get(0));
        assertEquals("a", sink.rows().get(0).get(1));
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void emptyInputSignalsEos() {
        Select s = Select.byIndex(IN_META, 0);
        CollectingRowSink sink = new CollectingRowSink();
        s.apply(RowStream.fromIterable(new ArrayList<>()), sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void barrierPassedThroughErrorStops() {
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row(1L, "a", null)),
                StreamEvent.barrier(5L),
                StreamEvent.error(new RuntimeException("boom")),
                StreamEvent.record(row(2L, "b", null)));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;

        Select s = Select.byIndex(IN_META, 0);
        CollectingRowSink sink = new CollectingRowSink();
        s.apply(in, sink);
        assertEquals(1, sink.rows().size()); // only the row before the error
        assertEquals(1, sink.barrierCount());
        assertTrue(sink.sawError());
    }

    @Test
    void emptyIndicesRejected() {
        assertThrows(IllegalArgumentException.class, () -> Select.byIndex(IN_META));
    }

    @Test
    void indexOutOfBoundsRejected() {
        assertThrows(IllegalArgumentException.class, () -> Select.byIndex(IN_META, 0, 5));
        assertThrows(IllegalArgumentException.class, () -> Select.byIndex(IN_META, -1));
    }
}
