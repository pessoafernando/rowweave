package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.OpenAddressingGroupAggBuffer;
import io.rowweave.mem.SpillingSegmentGroupAggBuffer;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ParallelGroupByAggTest {

    // schema: k (String key), amt (BigDecimal measure), q (Integer measure)
    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType),
            new Field("q", DataType.IntegerType));

    private static DataRow row(String k, BigDecimal amt, Integer q) {
        DataRow r = new DataRow(3);
        r.set(0, k);
        r.set(1, amt);
        r.set(2, q);
        return r;
    }

    private static List<DataRow> aggregate(ParallelGroupByAgg agg, List<DataRow> rows) {
        CollectingRowSink sink = new CollectingRowSink();
        agg.aggregate(RowStream.fromIterable(rows), sink);
        assertTrue(sink.sawEndOfStream(), "agg must emit EOS");
        return sink.rows();
    }

    /** index result rows by key (col 0) for order-independent assertions. */
    private static Map<String, DataRow> byKey(List<DataRow> rows) {
        Map<String, DataRow> m = new HashMap<>();
        for (DataRow r : rows) {
            m.put((String) r.get(0), r);
        }
        return m;
    }

    @Test
    void sumScale10HalfUpRoundedOnceAtOutput() {
        // three values that, summed at full precision then rounded, differ from rounding-each-then-summing
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("amt");
        List<DataRow> in = Arrays.asList(
                row("a", new BigDecimal("0.00000000004"), 1),  // 4e-11
                row("a", new BigDecimal("0.00000000004"), 1),
                row("a", new BigDecimal("0.00000000004"), 1));  // sum = 1.2e-10 -> round HALF_UP scale10 = 0.0000000001
        List<DataRow> out = aggregate(agg, in);
        assertEquals(1, out.size());
        assertEquals(new BigDecimal("0.0000000001"), out.get(0).get(1));
        assertEquals(10, ((BigDecimal) out.get(0).get(1)).scale());
    }

    @Test
    void sumSkipsNullMeasure() {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("amt");
        List<DataRow> in = Arrays.asList(
                row("a", new BigDecimal("10"), 1),
                row("a", null, 1),                 // null skipped
                row("a", new BigDecimal("5"), 1),
                row("b", null, 1));                // group b: all null -> sum null
        Map<String, DataRow> out = byKey(aggregate(agg, in));
        assertEquals(new BigDecimal("15.0000000000"), out.get("a").get(1));
        assertNull(out.get("b").get(1), "group with only null measures -> sum null");
    }

    @Test
    void countStarCountsEveryRowIncludingNulls() {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").count();
        List<DataRow> in = Arrays.asList(
                row("a", new BigDecimal("1"), null),
                row("a", null, null),
                row("b", new BigDecimal("2"), 5));
        Map<String, DataRow> out = byKey(aggregate(agg, in));
        assertEquals(2L, out.get("a").get(1));
        assertEquals(1L, out.get("b").get(1));
    }

    @Test
    void countFieldOnlyCountsNonNull() {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").countField("amt");
        List<DataRow> in = Arrays.asList(
                row("a", new BigDecimal("1"), null),
                row("a", null, null),              // amt null -> not counted
                row("a", new BigDecimal("3"), null));
        Map<String, DataRow> out = byKey(aggregate(agg, in));
        assertEquals(2L, out.get("a").get(1));
    }

    @Test
    void maxMinIgnoreNullEmptyGroupNull() {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").max("q").min("q");
        List<DataRow> in = Arrays.asList(
                row("a", null, 5),
                row("a", null, null),   // null ignored
                row("a", null, 2),
                row("a", null, 9),
                row("b", null, null));  // all null -> max/min null
        Map<String, DataRow> out = byKey(aggregate(agg, in));
        assertEquals(9, out.get("a").get(1)); // max
        assertEquals(2, out.get("a").get(2)); // min
        assertNull(out.get("b").get(1));
        assertNull(out.get("b").get(2));
    }

    @Test
    void nullKeyParticipatesAsOwnGroup() {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").count();
        List<DataRow> in = Arrays.asList(
                row(null, new BigDecimal("1"), 1),
                row("a", new BigDecimal("1"), 1),
                row(null, new BigDecimal("1"), 1));
        List<DataRow> out = aggregate(agg, in);
        assertEquals(2, out.size()); // null group + "a" group
        Map<String, DataRow> m = byKey(out);
        assertEquals(2L, m.get(null).get(1)); // two null-key rows grouped together
        assertEquals(1L, m.get("a").get(1));
    }

    @Test
    void multipleAggregatesAndKeys() {
        DataSchema meta2 = new DataSchema(
                new Field("k1", DataType.StringType),
                new Field("k2", DataType.IntegerType),
                new Field("v", DataType.BigDecimalType));
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(meta2, "k1", "k2").sum("v").count();
        List<DataRow> in = new ArrayList<>();
        DataRow r1 = new DataRow(3); r1.set(0, "x"); r1.set(1, 1); r1.set(2, new BigDecimal("10")); in.add(r1);
        DataRow r2 = new DataRow(3); r2.set(0, "x"); r2.set(1, 1); r2.set(2, new BigDecimal("20")); in.add(r2);
        DataRow r3 = new DataRow(3); r3.set(0, "x"); r3.set(1, 2); r3.set(2, new BigDecimal("5")); in.add(r3);
        CollectingRowSink sink = new CollectingRowSink();
        agg.aggregate(RowStream.fromIterable(in), sink);
        assertEquals(2, sink.rows().size()); // (x,1) and (x,2)
        // result row = [k1, k2, sum, count]
        for (DataRow r : sink.rows()) {
            if ("x".equals(r.get(0)) && Integer.valueOf(1).equals(r.get(1))) {
                assertEquals(new BigDecimal("30.0000000000"), r.get(2));
                assertEquals(2L, r.get(3));
            } else {
                assertEquals(new BigDecimal("5.0000000000"), r.get(2));
                assertEquals(1L, r.get(3));
            }
        }
    }

    @Test
    void orderIndependentSum() {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("amt");
        List<DataRow> in = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            in.add(row("a", new BigDecimal("0." + String.format("%010d", i)), 1));
        }
        List<DataRow> forward = aggregate(agg, in);
        List<DataRow> shuffled = new ArrayList<>(in);
        Collections.reverse(shuffled);
        List<DataRow> backward = aggregate(ParallelGroupByAgg.of(META, "k").sum("amt"), shuffled);
        assertEquals(forward.get(0).get(1), backward.get(0).get(1), "sum must be order-independent");
    }

    @Test
    void spillDoesNotOom() {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("amt").count()
                .withBufferFactory(f -> new SpillingSegmentGroupAggBuffer(
                        new DataSchema(new Field("k", DataType.StringType)), new String[]{"k"},
                        f, new MemoryManager(128), new HeapPool()));
        List<DataRow> in = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            in.add(row("key" + i, new BigDecimal(i), 1));
        }
        // each key appears 3x
        for (int round = 0; round < 2; round++) {
            for (int i = 0; i < 200; i++) {
                in.add(row("key" + i, new BigDecimal(i), 1));
            }
        }
        List<DataRow> out = aggregate(agg, in);
        assertEquals(200, out.size());
        Map<String, DataRow> m = byKey(out);
        // key5 appears 3 times with value 5 -> sum 15, count 3
        assertEquals(new BigDecimal("15.0000000000"), m.get("key5").get(1));
        assertEquals(3L, m.get("key5").get(2));
    }

    @Test
    void spillAcrossRoundsPreservesNullRulesAndFinalRounding() {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k")
                .sum("amt").count().countField("amt").max("q").min("q")
                .withBufferFactory(f -> new OpenAddressingGroupAggBuffer(
                        new DataSchema(new Field("k", DataType.StringType)), new String[]{"k"},
                        f, new MemoryManager(128), new HeapPool()));
        List<DataRow> in = new ArrayList<>();
        in.add(row("target", new BigDecimal("0.00000000004"), null));
        for (int i = 0; i < 40; i++) {
            in.add(row("filler-a-" + i, BigDecimal.ONE, 1));
        }
        in.add(row("target", new BigDecimal("0.00000000004"), 7));
        for (int i = 0; i < 40; i++) {
            in.add(row("filler-b-" + i, BigDecimal.ONE, 1));
        }
        in.add(row("target", null, 3));

        DataRow target = byKey(aggregate(agg, in)).get("target");
        assertEquals(new BigDecimal("0.0000000001"), target.get(1),
                "the two 4e-11 values merge at full precision and are rounded once at the end");
        assertEquals(10, ((BigDecimal) target.get(1)).scale());
        assertEquals(3L, target.get(2), "count(*) includes a null measure");
        assertEquals(2L, target.get(3), "count(field) ignores a null measure");
        assertEquals(7, target.get(4), "max ignores null");
        assertEquals(3, target.get(5), "min ignores null");
    }

    @Test
    void emptyInputEmitsOnlyEos() {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("amt");
        List<DataRow> out = aggregate(agg, new ArrayList<>());
        assertTrue(out.isEmpty());
    }

    @Test
    void errorEventForwardedAndStops() {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("amt");
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row("a", new BigDecimal("1"), 1)),
                StreamEvent.error(boom));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;
        CollectingRowSink sink = new CollectingRowSink();
        agg.aggregate(in, sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
        assertFalse(sink.sawEndOfStream());
    }

    @Test
    void unknownKeyFieldRejected() {
        assertThrows(IllegalArgumentException.class, () -> ParallelGroupByAgg.of(META, "nope"));
    }
}
