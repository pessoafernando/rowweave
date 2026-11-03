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

class FlatMapTest {

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
    void oneToManyExpansion() {
        // each row n -> n copies of n
        FlatMap fm = FlatMap.of((r, collector) -> {
            int n = (Integer) r.get(0);
            for (int i = 0; i < n; i++) {
                collector.collect(row(n));
            }
        });
        CollectingRowSink sink = new CollectingRowSink();
        fm.apply(RowStream.fromIterable(Arrays.asList(row(1), row(2), row(3))), sink);
        assertEquals(Arrays.asList(1, 2, 2, 3, 3, 3), col0(sink.rows()));
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void zeroOutputRowsAllowed() {
        // drop everything (N=0)
        FlatMap fm = FlatMap.of((r, collector) -> { /* emit nothing */ });
        CollectingRowSink sink = new CollectingRowSink();
        fm.apply(RowStream.fromIterable(Arrays.asList(row(1), row(2))), sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void emptyInputSignalsEos() {
        FlatMap fm = FlatMap.of((r, collector) -> collector.collect(r));
        CollectingRowSink sink = new CollectingRowSink();
        fm.apply(RowStream.fromIterable(new ArrayList<>()), sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void barrierPassedThroughErrorStops() {
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row(2)),
                StreamEvent.barrier(3L),
                StreamEvent.error(new RuntimeException("boom")),
                StreamEvent.record(row(5)));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;

        FlatMap fm = FlatMap.of((r, collector) -> {
            int n = (Integer) r.get(0);
            for (int i = 0; i < n; i++) {
                collector.collect(row(n));
            }
        });
        CollectingRowSink sink = new CollectingRowSink();
        fm.apply(in, sink);
        assertEquals(Arrays.asList(2, 2), col0(sink.rows())); // only row(2) before error
        assertEquals(1, sink.barrierCount());
        assertTrue(sink.sawError());
    }
}
