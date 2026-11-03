package io.rowweave.ops.join;

import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BinaryDrainOrderEquivalenceTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    @AfterEach
    void cleanup() {
        System.clearProperty("rowweave.binary.concurrentDrain");
    }

    @Test
    void allJoinTypesKeepExactRowOrderAcrossSerialConcurrentAndSpill() {
        for (JoinType type : JoinType.values()) {
            List<String> expected = runJoin(type, false, 64 * 1024);
            assertEquals(expected, runJoin(type, true, 64 * 1024), type + " concurrent memory");
            assertEquals(expected, runJoin(type, false, 32 * 1024), type + " serial spill");
            assertEquals(expected, runJoin(type, true, 32 * 1024), type + " concurrent spill");
        }
    }

    @Test
    void coGroupKeepsGroupAndIterableOrderAcrossSerialConcurrentAndSpill() {
        List<String> expected = runCoGroup(false, 64 * 1024);
        assertEquals(expected, runCoGroup(true, 64 * 1024));
        assertEquals(expected, runCoGroup(false, 32 * 1024));
        assertEquals(expected, runCoGroup(true, 32 * 1024));
    }

    private static List<String> runJoin(JoinType type, boolean concurrent, long budget) {
        System.setProperty("rowweave.binary.concurrentDrain", Boolean.toString(concurrent));
        SortMergeJoin join = SortMergeJoin.of(
                        LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, type)
                .withSpill(new MemoryManager(budget), new MemorySpillStore(), new HeapPool());
        CollectingRowSink sink = new CollectingRowSink();
        join.join(stream(leftRows()), stream(rightRows()), sink);
        List<String> result = new ArrayList<>();
        for (DataRow row : sink.rows()) {
            result.add(fingerprint(row));
        }
        return result;
    }

    private static List<String> runCoGroup(boolean concurrent, long budget) {
        System.setProperty("rowweave.binary.concurrentDrain", Boolean.toString(concurrent));
        DataSchema output = new DataSchema(new Field("group", DataType.StringType));
        CoGroupFunction function = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return output; }
            @Override public void coGroup(Iterable<DataRow> left, Iterable<DataRow> right, Collector collector) {
                StringBuilder value = new StringBuilder();
                for (DataRow row : left) value.append("L:").append(row.get(1)).append(';');
                for (DataRow row : right) value.append("R:").append(row.get(1)).append(';');
                DataRow out = new DataRow(1);
                out.set(0, value.toString());
                collector.collect(out);
            }
        };
        CoGroup coGroup = CoGroup.of(
                        LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, function)
                .withSpill(new MemoryManager(budget), new MemorySpillStore(), new HeapPool());
        CollectingRowSink sink = new CollectingRowSink();
        coGroup.coGroup(stream(leftRows()), stream(rightRows()), sink);
        List<String> result = new ArrayList<>();
        for (DataRow row : sink.rows()) result.add((String) row.get(0));
        return result;
    }

    private static List<DataRow> leftRows() {
        return Arrays.asList(row(2, "l2a"), row(1, "l1"), row(2, "l2b"),
                row(null, "ln1"), row(2, "l2c"), row(null, "ln2"));
    }

    private static List<DataRow> rightRows() {
        return Arrays.asList(row(2, "r2a"), row(3, "r3"), row(2, "r2b"),
                row(null, "rn1"), row(2, "r2c"), row(null, "rn2"));
    }

    private static DataRow row(Integer key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static String fingerprint(DataRow row) {
        StringBuilder value = new StringBuilder();
        for (int i = 0; i < row.size(); i++) value.append(row.get(i)).append('|');
        return value.toString();
    }

    private static RowStream stream(List<DataRow> rows) {
        return new RowStream() {
            private final Iterator<DataRow> iterator = rows.iterator();
            private boolean cancelled;
            @Override public StreamEvent next() {
                return cancelled || !iterator.hasNext() ? null : StreamEvent.record(iterator.next());
            }
            @Override public boolean supportsCancellation() { return true; }
            @Override public boolean cancel(Throwable cause) { cancelled = true; return true; }
        };
    }
}
