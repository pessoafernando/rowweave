package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TopNTest {

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

    private static List<Integer> topN(int n, List<Integer> input) {
        List<DataRow> rows = new ArrayList<>();
        for (Integer v : input) {
            rows.add(row(v));
        }
        CollectingRowSink sink = new CollectingRowSink();
        TopN.of(NUM, n).applyStream(RowStream.fromIterable(rows), sink);
        assertTrue(sink.sawEndOfStream());
        return col0(sink.rows());
    }

    @Test
    void takesSourceOrderPrefixNotSorted() {
        // unsorted input; TopN takes first n in SOURCE order (no sorting)
        assertEquals(Arrays.asList(5, 1, 9), topN(3, Arrays.asList(5, 1, 9, 3, 7)));
    }

    @Test
    void nZeroEmpty() {
        assertTrue(topN(0, Arrays.asList(1, 2, 3)).isEmpty());
    }

    @Test
    void nGreaterThanInputReturnsAll() {
        assertEquals(Arrays.asList(1, 2), topN(10, Arrays.asList(1, 2)));
    }

    @Test
    void earlyEmitBeforeUpstreamDrained() {
        // prove first n rows are emitted before later rows are pulled (streaming early-out)
        List<Integer> pulled = new ArrayList<>();
        List<DataRow> backing = Arrays.asList(row(10), row(20), row(30), row(40));
        Iterator<DataRow> bit = backing.iterator();
        RowStream in = () -> {
            if (!bit.hasNext()) {
                return null;
            }
            DataRow r = bit.next();
            pulled.add((Integer) r.get(0));
            return StreamEvent.record(r);
        };
        List<Integer> emittedAtFirst = new ArrayList<>();
        RowSink sink = event -> {
            if (event.isRecord()) {
                emittedAtFirst.add((Integer) event.row().get(0));
                // when the 2nd (last for n=2) row is emitted, upstream must not have been fully drained yet
                if (emittedAtFirst.size() == 2) {
                    assertTrue(pulled.size() <= 3, "early-out: not all upstream pulled when n-th row emitted");
                }
            }
        };
        TopN.of(NUM, 2).applyStream(in, sink);
        assertEquals(Arrays.asList(10, 20), emittedAtFirst);
    }

    @Test
    void errorForwardedAndStops() {
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row(1)),
                StreamEvent.error(boom),
                StreamEvent.record(row(2)));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;
        CollectingRowSink sink = new CollectingRowSink();
        TopN.of(NUM, 5).applyStream(in, sink);
        assertEquals(Arrays.asList(1), col0(sink.rows()));
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
        assertFalse(sink.sawEndOfStream());
    }

    @Test
    void emptyInput() {
        assertTrue(topN(3, new ArrayList<>()).isEmpty());
    }

    @Test
    void rejectsNegativeN() {
        assertThrows(IllegalArgumentException.class, () -> TopN.of(NUM, -1));
    }
}
