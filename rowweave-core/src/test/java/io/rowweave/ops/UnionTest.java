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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UnionTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

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

    private static RowStream stream(Object... vs) {
        List<DataRow> rows = new ArrayList<>();
        for (Object v : vs) {
            rows.add(row(v));
        }
        return RowStream.fromIterable(rows);
    }

    @Test
    void concatTwoInputsInOrder() {
        Union u = Union.of(META);
        CollectingRowSink sink = new CollectingRowSink();
        u.applyStream(Arrays.asList(stream(1, 2), stream(3, 4)), sink);
        assertEquals(Arrays.asList(1, 2, 3, 4), col0(sink.rows()));
    }

    @Test
    void emitsSingleEosAfterAllInputsDrained() {
        Union u = Union.of(META);
        CollectingRowSink sink = new CollectingRowSink();
        u.applyStream(Arrays.asList(stream(1), stream(2), stream(3)), sink);
        assertTrue(sink.sawEndOfStream());
        // per-input EOS must NOT be forwarded; CollectingRowSink only flips sawEndOfStream,
        // so we assert all rows present + exactly the concat order (single logical end).
        assertEquals(Arrays.asList(1, 2, 3), col0(sink.rows()));
    }

    @Test
    void unionAllKeepsDuplicates() {
        Union u = Union.of(META);
        CollectingRowSink sink = new CollectingRowSink();
        u.applyStream(Arrays.asList(stream(1, 2), stream(2, 1)), sink);
        assertEquals(Arrays.asList(1, 2, 2, 1), col0(sink.rows())); // no dedup
    }

    @Test
    void emptyInputsListSignalsEosOnly() {
        Union u = Union.of(META);
        CollectingRowSink sink = new CollectingRowSink();
        u.applyStream(Collections.emptyList(), sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void someEmptyStreamsAmongNonEmpty() {
        Union u = Union.of(META);
        CollectingRowSink sink = new CollectingRowSink();
        u.applyStream(Arrays.asList(stream(), stream(1), stream(), stream(2)), sink);
        assertEquals(Arrays.asList(1, 2), col0(sink.rows()));
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void perInputBarrierNotForwarded() {
        // input 0 carries a barrier; union must swallow it (not union's logical boundary)
        List<StreamEvent> ev0 = Arrays.asList(
                StreamEvent.record(row(1)),
                StreamEvent.barrier(7L),
                StreamEvent.record(row(2)));
        Iterator<StreamEvent> it0 = ev0.iterator();
        RowStream in0 = () -> it0.hasNext() ? it0.next() : null;

        Union u = Union.of(META);
        CollectingRowSink sink = new CollectingRowSink();
        u.applyStream(Arrays.asList(in0, stream(3)), sink);
        assertEquals(Arrays.asList(1, 2, 3), col0(sink.rows()));
        assertEquals(0, sink.barrierCount(), "per-input barrier must not be forwarded");
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void errorInInputForwardedAndTerminates() {
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> ev0 = Arrays.asList(
                StreamEvent.record(row(1)),
                StreamEvent.error(boom),
                StreamEvent.record(row(2)));
        Iterator<StreamEvent> it0 = ev0.iterator();
        RowStream in0 = () -> it0.hasNext() ? it0.next() : null;

        Union u = Union.of(META);
        CollectingRowSink sink = new CollectingRowSink();
        u.applyStream(Arrays.asList(in0, stream(99)), sink); // input 1 must NOT be consumed
        assertEquals(Arrays.asList(1), col0(sink.rows()));
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
        assertFalse(sink.sawEndOfStream(), "error terminates without normal EOS");
    }
}
