package io.rowweave.ops;

import io.rowweave.FilterFunction;
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

class FilterTest {

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
    void keepsRowsThatPassPredicate() {
        Filter f = Filter.of(r -> (Integer) r.get(0) > 2);
        CollectingRowSink sink = new CollectingRowSink();
        f.apply(RowStream.fromIterable(Arrays.asList(row(1), row(2), row(3), row(4))), sink);
        assertEquals(Arrays.asList(3, 4), col0(sink.rows()));
        assertTrue(sink.sawEndOfStream(), "filter must emit EOS at end");
    }

    @Test
    void emptyInputEmitsNothingButSignalsEos() {
        Filter f = Filter.of(r -> true);
        CollectingRowSink sink = new CollectingRowSink();
        f.apply(RowStream.fromIterable(new ArrayList<>()), sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void predicateFalseForAllDropsEverything() {
        Filter f = Filter.of(r -> false);
        CollectingRowSink sink = new CollectingRowSink();
        f.apply(RowStream.fromIterable(Arrays.asList(row("a"), row("b"))), sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void barrierIsPassedThrough() {
        // upstream: record(1), barrier(7), record(3) — predicate keeps >2
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row(1)),
                StreamEvent.barrier(7L),
                StreamEvent.record(row(3)));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;

        Filter f = Filter.of(r -> (Integer) r.get(0) > 2);
        CollectingRowSink sink = new CollectingRowSink();
        f.apply(in, sink);
        assertEquals(Arrays.asList(3), col0(sink.rows()));
        assertEquals(1, sink.barrierCount(), "barrier must be forwarded, not swallowed");
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void errorIsForwardedAndStops() {
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row(3)),
                StreamEvent.error(boom),
                StreamEvent.record(row(4))); // must NOT be processed after error
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;

        Filter f = Filter.of(r -> true);
        CollectingRowSink sink = new CollectingRowSink();
        f.apply(in, sink);
        assertEquals(Arrays.asList(3), col0(sink.rows()));
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
    }

    @Test
    void testMethodEvaluatesPredicateOnSingleRow() {
        Filter f = Filter.of(r -> (Integer) r.get(0) > 2);
        assertTrue(f.test(row(5)));
        assertFalse(f.test(row(1)));
    }
}
