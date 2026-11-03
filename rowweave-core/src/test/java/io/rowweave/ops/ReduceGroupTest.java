package io.rowweave.ops;

import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.GroupReduceFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReduceGroupTest {

    // schema: k (String key), v (Integer)
    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("v", DataType.IntegerType));

    private static DataRow row(String k, Integer v) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, v);
        return r;
    }

    /** reduce fn: emit one row [key, sum(v)] per group. */
    private static final GroupReduceFunction SUM_PER_GROUP = new GroupReduceFunction() {
        @Override
        public DataSchema getResultSchema() {
            return META;
        }

        @Override
        public void reduce(Iterable<DataRow> iter, Collector collector) {
            String key = null;
            int sum = 0;
            for (DataRow r : iter) {
                key = (String) r.get(0);
                if (r.get(1) != null) {
                    sum += (Integer) r.get(1);
                }
            }
            DataRow out = new DataRow(2);
            out.set(0, key);
            out.set(1, sum);
            collector.collect(out);
        }
    };

    private static List<DataRow> reduce(ReduceGroup rg, List<DataRow> rows) {
        CollectingRowSink sink = new CollectingRowSink();
        rg.reduce(RowStream.fromIterable(rows), sink);
        assertTrue(sink.sawEndOfStream(), "reduce must emit EOS");
        return sink.rows();
    }

    @Test
    void groupsOutputInKeySortedOrder() {
        // group order follows the key order under NULLS_LAST; rows within a group keep input order because the sort is stable.
        ReduceGroup rg = ReduceGroup.of(META, "k", SUM_PER_GROUP);
        List<DataRow> in = Arrays.asList(
                row("c", 1), row("a", 10), row("c", 2), row("b", 100), row("a", 20));
        // key-sorted order: a, b, c
        List<DataRow> out = reduce(rg, in);
        assertEquals(Arrays.asList("a", "b", "c"),
                Arrays.asList((String) out.get(0).get(0), (String) out.get(1).get(0), (String) out.get(2).get(0)));
        assertEquals(30, out.get(0).get(1)); // a: 10+20
        assertEquals(100, out.get(1).get(1)); // b
        assertEquals(3, out.get(2).get(1)); // c: 1+2
    }

    @Test
    void nullKeyIsItsOwnGroupSortedLast() {
        // NULLS_LAST puts the null-key group last
        ReduceGroup rg = ReduceGroup.of(META, "k", SUM_PER_GROUP);
        List<DataRow> in = Arrays.asList(
                row(null, 1), row("a", 5), row(null, 2));
        List<DataRow> out = reduce(rg, in);
        // key-sorted, nulls last: a, null
        assertEquals("a", out.get(0).get(0));
        assertNull(out.get(1).get(0));
        assertEquals(3, out.get(1).get(1)); // null group: 1+2
    }

    /** Sort equality must imply group equality, otherwise interleaved scales split one group into two runs. */
    @Test
    void interleavedBigDecimalScalesRemainOneGroupPerExactKey() {
        DataSchema decimalMeta = new DataSchema(
                new Field("k", DataType.BigDecimalType),
                new Field("v", DataType.IntegerType));
        GroupReduceFunction count = new GroupReduceFunction() {
            @Override
            public DataSchema getResultSchema() {
                return decimalMeta;
            }

            @Override
            public void reduce(Iterable<DataRow> rows, Collector collector) {
                BigDecimal key = null;
                int size = 0;
                for (DataRow value : rows) {
                    key = (BigDecimal) value.get(0);
                    size++;
                }
                DataRow out = new DataRow(2);
                out.set(0, key);
                out.set(1, size);
                collector.collect(out);
            }
        };
        DataRow scale2a = new DataRow(2); scale2a.set(0, new BigDecimal("5.00")); scale2a.set(1, 1);
        DataRow scale0 = new DataRow(2); scale0.set(0, new BigDecimal("5")); scale0.set(1, 2);
        DataRow scale2b = new DataRow(2); scale2b.set(0, new BigDecimal("5.00")); scale2b.set(1, 3);

        List<DataRow> out = reduce(ReduceGroup.of(decimalMeta, "k", count),
                Arrays.asList(scale2a, scale0, scale2b));

        assertEquals(2, out.size(), "5.00 rows must remain contiguous despite interleaved 5");
        assertEquals(new BigDecimal("5"), out.get(0).get(0));
        assertEquals(1, out.get(0).get(1));
        assertEquals(new BigDecimal("5.00"), out.get(1).get(0));
        assertEquals(2, out.get(1).get(1));
    }

    @Test
    void multiKeyWithMiddleFieldNull() {
        // 3 fields: k1 (String), k2 (String, MIDDLE key, nullable), v (Integer); group by k1,k2
        DataSchema meta3 = new DataSchema(
                new Field("k1", DataType.StringType),
                new Field("k2", DataType.StringType),
                new Field("v", DataType.IntegerType));
        GroupReduceFunction countFn = new GroupReduceFunction() {
            @Override
            public DataSchema getResultSchema() {
                return meta3;
            }
            @Override
            public void reduce(Iterable<DataRow> iter, Collector collector) {
                String k1 = null;
                Object k2 = "__unset__";
                int n = 0;
                for (DataRow r : iter) {
                    k1 = (String) r.get(0);
                    k2 = r.get(1);
                    n++;
                }
                DataRow out = new DataRow(3);
                out.set(0, k1);
                out.set(1, k2);
                out.set(2, n);
                collector.collect(out);
            }
        };
        ReduceGroup rg = ReduceGroup.of(meta3, new String[]{"k1", "k2"}, countFn);
        List<DataRow> in = new ArrayList<>();
        DataRow r1 = new DataRow(3); r1.set(0, "x"); r1.set(1, null); r1.set(2, 1); in.add(r1);
        DataRow r2 = new DataRow(3); r2.set(0, "x"); r2.set(1, "m"); r2.set(2, 1); in.add(r2);
        DataRow r3 = new DataRow(3); r3.set(0, "x"); r3.set(1, null); r3.set(2, 1); in.add(r3); // same group as r1
        CollectingRowSink sink = new CollectingRowSink();
        rg.reduce(RowStream.fromIterable(in), sink);
        // groups: (x,null) count 2 [r1,r3], (x,m) count 1 — middle null must NOT split (x,null) wrongly.
        // key-sorted NULLS_LAST: (x,"m") before (x,null).
        assertEquals(2, sink.rows().size());
        assertEquals("m", sink.rows().get(0).get(1));
        assertEquals(1, sink.rows().get(0).get(2));
        assertNull(sink.rows().get(1).get(1));
        assertEquals(2, sink.rows().get(1).get(2)); // (x,null) = r1 + r3
    }

    @Test
    void reduceCanEmitZeroOrManyRows() {
        // fn: emit nothing for group "drop"; emit 2 rows for others
        GroupReduceFunction fn = new GroupReduceFunction() {
            @Override
            public DataSchema getResultSchema() {
                return META;
            }
            @Override
            public void reduce(Iterable<DataRow> iter, Collector collector) {
                String key = null;
                for (DataRow r : iter) {
                    key = (String) r.get(0);
                }
                if ("drop".equals(key)) {
                    return; // 0 rows
                }
                collector.collect(row(key, 1));
                collector.collect(row(key, 2)); // 2 rows
            }
        };
        ReduceGroup rg = ReduceGroup.of(META, "k", fn);
        List<DataRow> out = reduce(rg, Arrays.asList(row("a", 0), row("drop", 0), row("b", 0)));
        // a -> 2 rows, drop -> 0, b -> 2 rows = 4 total
        assertEquals(4, out.size());
    }

    @Test
    void fnCanIterateGroupMultipleTimes() {
        // fn: first pass count, second pass sum, emit [key, count, sum]
        DataSchema out3 = new DataSchema(
                new Field("k", DataType.StringType),
                new Field("cnt", DataType.IntegerType),
                new Field("sum", DataType.IntegerType));
        GroupReduceFunction fn = new GroupReduceFunction() {
            @Override
            public DataSchema getResultSchema() {
                return out3;
            }
            @Override
            public void reduce(Iterable<DataRow> iter, Collector collector) {
                String key = null;
                int cnt = 0;
                for (DataRow r : iter) { // first pass
                    key = (String) r.get(0);
                    cnt++;
                }
                int sum = 0;
                for (DataRow r : iter) { // second pass (block must be re-readable)
                    sum += (Integer) r.get(1);
                }
                DataRow o = new DataRow(3);
                o.set(0, key); o.set(1, cnt); o.set(2, sum);
                collector.collect(o);
            }
        };
        ReduceGroup rg = ReduceGroup.of(META, "k", fn);
        List<DataRow> out = reduce(rg, Arrays.asList(row("a", 5), row("a", 10), row("a", 15)));
        assertEquals(1, out.size());
        assertEquals(3, out.get(0).get(1));  // count
        assertEquals(30, out.get(0).get(2)); // sum
    }

    @Test
    void largeGroupAndHighCardinalitySpillDoesNotOom(@TempDir Path tmp) {
        ReduceGroup rg = ReduceGroup.of(META, "k", SUM_PER_GROUP)
                .withSpill(new MemoryManager(64 * 1024), new DiskSpillStore(tmp.toFile()), new HeapPool());
        List<DataRow> in = new ArrayList<>();
        // 50 groups x 30 rows each = 1500 rows, small budget forces sort spill + large-group block spill
        for (int g = 0; g < 50; g++) {
            for (int i = 0; i < 30; i++) {
                in.add(row("g" + g, 1));
            }
        }
        List<DataRow> out = reduce(rg, in);
        assertEquals(50, out.size());
        for (DataRow r : out) {
            assertEquals(30, r.get(1)); // each group sums to 30
        }
    }

    @Test
    void keyedReduceRetainsReplayScratchUntilEqualKeyBlockCloses() {
        DataSchema wide = new DataSchema(
                new Field("k", DataType.StringType),
                new Field("v", DataType.StringType));
        GroupReduceFunction count = new GroupReduceFunction() {
            @Override public DataSchema getResultSchema() { return wide; }
            @Override public void reduce(Iterable<DataRow> rows, Collector collector) {
                int size = 0;
                for (DataRow ignored : rows) size++;
                DataRow out = new DataRow(2);
                out.set(0, "count");
                out.set(1, Integer.toString(size));
                collector.collect(out);
            }
        };
        StringBuilder keyBuilder = new StringBuilder(256);
        for (int i = 0; i < 256; i++) keyBuilder.append('k');
        String key = keyBuilder.toString();
        List<DataRow> input = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            StringBuilder value = new StringBuilder(1024 + i);
            for (int j = 0; j < 1024 + i; j++) value.append((char) ('a' + i % 20));
            DataRow row = new DataRow(2);
            row.set(0, key);
            row.set(1, value.toString());
            input.add(row);
        }

        MemoryManager memory = new MemoryManager(24 * 1024);
        ReduceGroup reduce = ReduceGroup.of(wide, "k", count)
                .withSpill(memory, new MemorySpillStore(), new HeapPool());
        CollectingRowSink sink = new CollectingRowSink();

        reduce.reduce(RowStream.fromIterable(input), sink);

        assertTrue(sink.sawEndOfStream());
        assertEquals(1, sink.rows().size());
        assertEquals("40", sink.rows().get(0).get(1));
        assertEquals(0, memory.reservedBytes(), "sort and grouped replay ownership must return together");
    }

    /** A user-function failure must still close sort, block and store at once and keep the user cause first. */
    @Test
    void userFailureImmediatelyReclaimsSpillResources(@TempDir Path tmp) throws Exception {
        RuntimeException root = new RuntimeException("reduce-root");
        GroupReduceFunction failing = new GroupReduceFunction() {
            @Override public DataSchema getResultSchema() { return META; }
            @Override public void reduce(Iterable<DataRow> rows, Collector collector) { throw root; }
        };
        MemoryManager memory = new MemoryManager(64 * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore spill = new DiskSpillStore(tmp.toFile());
        ReduceGroup reduce = ReduceGroup.of(META, "k", failing).withSpill(memory, spill, pool);
        List<DataRow> input = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            input.add(row("k" + (i % 17), i));
        }

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> reduce.reduce(RowStream.fromIterable(input), new CollectingRowSink()));

        assertSame(root, actual);
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0, spill.currentOpenReaders());
        try (java.util.stream.Stream<Path> files = Files.list(tmp)) {
            assertEquals(0, files.count());
        }
        assertThrows(IllegalStateException.class, spill::createRun,
                "ReduceGroup owns and closes its operator SpillStore");
    }

    @Test
    void emptyInputEmitsOnlyEos() {
        ReduceGroup rg = ReduceGroup.of(META, "k", SUM_PER_GROUP);
        assertTrue(reduce(rg, new ArrayList<>()).isEmpty());
    }

    @Test
    void errorEventForwardedAndStops() {
        ReduceGroup rg = ReduceGroup.of(META, "k", SUM_PER_GROUP);
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row("a", 1)),
                StreamEvent.error(boom));
        Iterator<StreamEvent> it = events.iterator();
        RowStream in = () -> it.hasNext() ? it.next() : null;
        CollectingRowSink sink = new CollectingRowSink();
        rg.reduce(in, sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
        assertFalse(sink.sawEndOfStream());
    }

    @Test
    void unknownKeyFieldRejected() {
        assertThrows(IllegalArgumentException.class, () -> ReduceGroup.of(META, "nope", SUM_PER_GROUP));
    }
}
