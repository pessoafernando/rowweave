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

class TopTest {

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

    private static List<Integer> top(Top t, List<Integer> input, int par) {
        List<DataRow> rows = new ArrayList<>();
        for (Integer v : input) {
            rows.add(row(v));
        }
        CollectingRowSink sink = new CollectingRowSink();
        t.applyStream(RowStream.fromIterable(rows), par, sink);
        assertTrue(sink.sawEndOfStream());
        return col0(sink.rows());
    }

    @Test
    void topNAscMatchesSortedPrefix() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        Top t = Top.of(NUM, 3, sf);
        List<Integer> in = Arrays.asList(5, 1, 9, 3, 7, 2, 8);
        assertEquals(Arrays.asList(1, 2, 3), top(t, in, 1)); // smallest 3 ascending
    }

    @Test
    void topNDesc() {
        SortField sf = new SortField(0, SortField.Direction.DESC, SortField.NullOrder.DEFAULT);
        Top t = Top.of(NUM, 3, sf);
        List<Integer> in = Arrays.asList(5, 1, 9, 3, 7, 2, 8);
        assertEquals(Arrays.asList(9, 8, 7), top(t, in, 1)); // largest 3 descending
    }

    @Test
    void parallelismInvariant() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        Top t = Top.of(NUM, 4, sf);
        List<Integer> in = new ArrayList<>();
        for (int i = 30; i >= 1; i--) {
            in.add(i % 6 == 0 ? null : i);
        }
        List<Integer> p1 = top(t, in, 1);
        assertEquals(p1, top(t, in, 2));
        assertEquals(p1, top(t, in, 3));
    }

    @Test
    void nullOrderingDefaultAndAbsolute() {
        for (SortField.NullOrder no : SortField.NullOrder.values()) {
            SortField sf = new SortField(0, SortField.Direction.ASC, no);
            Top t = Top.of(NUM, 2, sf);
            RowComparator cmp = new RowComparator(NUM, sf);
            List<Integer> in = Arrays.asList(3, null, 1, null, 2);
            // authority: stable-sort all, take first 2
            List<DataRow> rows = new ArrayList<>();
            for (Integer v : in) {
                rows.add(row(v));
            }
            rows.sort(cmp);
            assertEquals(col0(rows).subList(0, 2), top(t, in, 2), "nullOrder=" + no);
        }
    }

    @Test
    void nZeroEmptyAndNGreaterThanInput() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        assertTrue(top(Top.of(NUM, 0, sf), Arrays.asList(1, 2, 3), 2).isEmpty());
        assertEquals(Arrays.asList(1, 2, 3), top(Top.of(NUM, 10, sf), Arrays.asList(3, 1, 2), 2));
    }

    @Test
    void equalKeysStablePrefersEarlierInput() {
        // two-field: sort by col0 only; equal keys -> earlier input wins (witness col1)
        DataSchema meta = new DataSchema(
                new Field("k", DataType.IntegerType),
                new Field("w", DataType.IntegerType));
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        Top t = Top.of(meta, 2, sf);
        List<DataRow> in = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            DataRow r = new DataRow(2);
            r.set(0, 1);   // all equal key
            r.set(1, i);   // witness = input order
            in.add(r);
        }
        CollectingRowSink sink = new CollectingRowSink();
        t.applyStream(RowStream.fromIterable(in), 3, sink);
        // top 2 of equal keys = first two inputs (witness 0,1)
        assertEquals(Arrays.asList(0, 1),
                Arrays.asList((Integer) sink.rows().get(0).get(1), (Integer) sink.rows().get(1).get(1)));
    }

    @Test
    void errorEventForwardedAndStops() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        Top t = Top.of(NUM, 3, sf);
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row(5)),
                StreamEvent.error(boom),
                StreamEvent.record(row(1)));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;
        CollectingRowSink sink = new CollectingRowSink();
        t.applyStream(in, 1, sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
        assertFalse(sink.sawEndOfStream());
    }

    @Test
    void emptyInput() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        assertTrue(top(Top.of(NUM, 3, sf), new ArrayList<>(), 2).isEmpty());
    }

    @Test
    void rejectsNegativeNAndNoSortField() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        assertThrows(IllegalArgumentException.class, () -> Top.of(NUM, -1, sf));
        assertThrows(IllegalArgumentException.class, () -> Top.of(NUM, 3));
    }

    @Test
    void parallelismMustBePositive() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        Top t = Top.of(NUM, 3, sf);
        assertThrows(IllegalArgumentException.class,
                () -> t.applyStream(RowStream.fromIterable(new ArrayList<>()), 0, new CollectingRowSink()));
    }
}
