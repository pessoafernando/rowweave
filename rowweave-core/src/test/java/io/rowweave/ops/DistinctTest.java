package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.SpillingSegmentGroupAggBuffer;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DistinctTest {

    private static final DataSchema META = new DataSchema(
            new Field("a", DataType.StringType),
            new Field("b", DataType.IntegerType));

    private static DataRow row(String a, Integer b) {
        DataRow r = new DataRow(2);
        r.set(0, a);
        r.set(1, b);
        return r;
    }

    private static List<DataRow> dedup(Distinct d, List<DataRow> rows) {
        CollectingRowSink sink = new CollectingRowSink();
        d.dedup(RowStream.fromIterable(rows), sink);
        assertTrue(sink.sawEndOfStream(), "dedup must emit EOS");
        return sink.rows();
    }

    private static List<String> pairs(List<DataRow> rows) {
        List<String> out = new ArrayList<>();
        for (DataRow r : rows) {
            out.add(r.get(0) + "|" + r.get(1));
        }
        return out;
    }

    @Test
    void allColumnsRemovesFullRowDuplicatesFirstSeenOrder() {
        Distinct d = Distinct.allColumns(META);
        List<DataRow> in = Arrays.asList(
                row("a", 1), row("b", 2), row("a", 1), row("c", 3), row("b", 2));
        assertEquals(Arrays.asList("a|1", "b|2", "c|3"), pairs(dedup(d, in)));
    }

    @Test
    void byColumnsDedupOnSubsetKeepsFirstSeenRow() {
        Distinct d = Distinct.byColumns(META, "a"); // dedup by col a only
        List<DataRow> in = Arrays.asList(
                row("a", 1), row("a", 99), row("b", 2)); // a's first-seen row is (a,1)
        List<DataRow> out = dedup(d, in);
        assertEquals(Arrays.asList("a|1", "b|2"), pairs(out));
    }

    @Test
    void nullIsEquatableValue() {
        Distinct d = Distinct.allColumns(META);
        List<DataRow> in = Arrays.asList(
                row(null, null), row(null, null), row("a", null), row(null, 1));
        List<DataRow> out = dedup(d, in);
        // (null,null) dedup'd; (a,null) and (null,1) distinct
        assertEquals(3, out.size());
        assertEquals(Arrays.asList("null|null", "a|null", "null|1"), pairs(out));
    }

    @Test
    void bigDecimalScaleCountsInKey() {
        DataSchema meta = new DataSchema(new Field("d", DataType.BigDecimalType));
        Distinct d = Distinct.allColumns(meta);
        DataRow r1 = new DataRow(1); r1.set(0, new BigDecimal("1.0"));
        DataRow r2 = new DataRow(1); r2.set(0, new BigDecimal("1.00")); // different scale -> distinct
        DataRow r3 = new DataRow(1); r3.set(0, new BigDecimal("1.0"));  // dup of r1
        CollectingRowSink sink = new CollectingRowSink();
        d.dedup(RowStream.fromIterable(Arrays.asList(r1, r2, r3)), sink);
        assertEquals(2, sink.rows().size(), "1.0 and 1.00 differ by scale -> distinct");
    }

    @Test
    void highCardinalitySpillDoesNotOom() {
        Distinct d = Distinct.allColumns(META)
                .withBufferFactory(f -> new SpillingSegmentGroupAggBuffer(
                        META, new String[]{"a", "b"}, f, new MemoryManager(128), new HeapPool()));
        List<DataRow> in = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            in.add(row("k" + i, i));   // all distinct
            in.add(row("k" + i, i));   // each duplicated once
        }
        List<DataRow> out = dedup(d, in);
        assertEquals(300, out.size());
        Set<String> keys = new HashSet<>(pairs(out));
        assertEquals(300, keys.size());
    }

    @Test
    void spillAcrossRoundsKeepsFirstNonKeyColumn() {
        Distinct d = Distinct.byColumns(META, "a")
                .withBufferFactory(f -> new SpillingSegmentGroupAggBuffer(
                        META, new String[]{"a"}, f, new MemoryManager(128), new HeapPool()));
        List<DataRow> in = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            in.add(row("k" + i, i));
        }
        for (int i = 0; i < 40; i++) {
            in.add(row("k" + i, 10_000 + i));
        }

        Map<String, Integer> representatives = new HashMap<>();
        for (DataRow row : dedup(d, in)) {
            representatives.put((String) row.get(0), (Integer) row.get(1));
        }
        assertEquals(40, representatives.size());
        for (int i = 0; i < 40; i++) {
            assertEquals(i, representatives.get("k" + i),
                    "after a spilled merge the earliest representative row is kept rather than a later non-key column");
        }
    }

    @Test
    void emptyInputEmitsOnlyEos() {
        Distinct d = Distinct.allColumns(META);
        assertTrue(dedup(d, new ArrayList<>()).isEmpty());
    }

    @Test
    void errorEventForwardedAndStops() {
        Distinct d = Distinct.allColumns(META);
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row("a", 1)),
                StreamEvent.error(boom));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;
        CollectingRowSink sink = new CollectingRowSink();
        d.dedup(in, sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
        assertFalse(sink.sawEndOfStream());
    }

    @Test
    void byColumnsRejectsEmptyAndUnknown() {
        assertThrows(IllegalArgumentException.class, () -> Distinct.byColumns(META));
        assertThrows(IllegalArgumentException.class, () -> Distinct.byColumns(META, "nope"));
    }
}
