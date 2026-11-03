package io.rowweave.ops;

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

class MapTest {

    private static DataRow row(Object v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    private static List<Object> col0(List<DataRow> rows) {
        List<Object> out = new ArrayList<>();
        for (DataRow r : rows) {
            out.add(r.get(0));
        }
        return out;
    }

    @Test
    void mapsEachRowOneToOne() {
        Map m = Map.of(r -> row((Integer) r.get(0) * 10));
        CollectingRowSink sink = new CollectingRowSink();
        m.apply(RowStream.fromIterable(Arrays.asList(row(1), row(2), row(3))), sink);
        assertEquals(Arrays.asList(10, 20, 30), col0(sink.rows()));
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void emptyInputSignalsEos() {
        Map m = Map.of(r -> r);
        CollectingRowSink sink = new CollectingRowSink();
        m.apply(RowStream.fromIterable(new ArrayList<>()), sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void barrierPassedThroughErrorStops() {
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row(1)),
                StreamEvent.barrier(9L),
                StreamEvent.error(new RuntimeException("boom")),
                StreamEvent.record(row(2)));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;

        Map m = Map.of(r -> row((Integer) r.get(0) * 10));
        CollectingRowSink sink = new CollectingRowSink();
        m.apply(in, sink);
        assertEquals(Arrays.asList(10), col0(sink.rows()));
        assertEquals(1, sink.barrierCount());
        assertTrue(sink.sawError());
    }
}
