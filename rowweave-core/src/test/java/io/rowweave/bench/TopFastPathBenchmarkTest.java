package io.rowweave.bench;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Manual wall-clock evidence for a single shared heap against per-parallelism local heaps with a global merge. */
@Tag("profile")
class TopFastPathBenchmarkTest {

    private static final DataSchema META = new DataSchema(
            new Field("key", DataType.IntegerType),
            new Field("witness", DataType.IntegerType));
    private static final SortField KEY_ASC =
            new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);

    @Test
    void compareLegacyPartitionHeapsWithSingleHeap() {
        int rows = Integer.getInteger("rowweave.bench.top.rows", 300_000);
        int n = Integer.getInteger("rowweave.bench.top.n", 1_000);
        List<DataRow> input = input(rows);
        for (int parallelism : new int[]{1, 4}) {
            legacy(input.subList(0, Math.min(20_000, rows)), n, parallelism);
            currentManaged(input.subList(0, Math.min(20_000, rows)), n, parallelism);

            long legacyStart = System.nanoTime();
            List<DataRow> legacy = legacy(input, n, parallelism);
            long legacyNanos = System.nanoTime() - legacyStart;
            long currentStart = System.nanoTime();
            List<DataRow> current = currentManaged(input, n, parallelism);
            long currentNanos = System.nanoTime() - currentStart;

            assertRowsEqual(legacy, current);
            System.out.printf("TOP-FAST P%d rows=%d n=%d legacy=%.2fms single=%.2fms speedup=%.2fx%n",
                    parallelism, rows, n, legacyNanos / 1e6, currentNanos / 1e6,
                    (double) legacyNanos / currentNanos);
        }
    }

    private static List<DataRow> input(int count) {
        List<DataRow> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            DataRow row = new DataRow(2);
            row.set(0, (i * 7919) % Math.max(1, count / 3));
            row.set(1, i);
            rows.add(row);
        }
        return rows;
    }

    private static List<DataRow> currentManaged(List<DataRow> input, int n, int parallelism) {
        CollectingRowSink output = new CollectingRowSink();
        Top.of(META, n, KEY_ASC)
                .withSpill(new MemoryManager(64L * 1024 * 1024), (meta, comparator) -> {
                    throw new AssertionError("benchmark fast path unexpectedly spilled");
                })
                .applyStream(RowStream.fromIterable(input), parallelism, output);
        return output.rows();
    }

    /** Equivalent test-only snapshot: P local heaps of size N, then merged into one global heap of N. */
    private static List<DataRow> legacy(List<DataRow> input, int n, int parallelism) {
        DataSchema augmentedMeta = new DataSchema(
                new Field("key", DataType.IntegerType),
                new Field("witness", DataType.IntegerType),
                new Field("__seq", DataType.LongType));
        RowComparator comparator = new RowComparator(augmentedMeta, KEY_ASC,
                new SortField(2, SortField.Direction.ASC, SortField.NullOrder.DEFAULT));
        List<PriorityQueue<DataRow>> heaps = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            heaps.add(new PriorityQueue<>((left, right) -> comparator.compare(right, left)));
        }
        for (int i = 0; i < input.size(); i++) {
            DataRow source = input.get(i);
            DataRow augmented = new DataRow(3);
            augmented.set(0, source.get(0));
            augmented.set(1, source.get(1));
            augmented.set(2, (long) i);
            offer(heaps.get(i % parallelism), augmented, n, comparator);
        }
        PriorityQueue<DataRow> global = new PriorityQueue<>((left, right) -> comparator.compare(right, left));
        for (PriorityQueue<DataRow> heap : heaps) {
            for (DataRow candidate : heap) {
                offer(global, candidate, n, comparator);
            }
        }
        List<DataRow> sorted = new ArrayList<>(global);
        sorted.sort(comparator);
        List<DataRow> output = new ArrayList<>(sorted.size());
        for (DataRow augmented : sorted) {
            DataRow row = new DataRow(2);
            row.set(0, augmented.get(0));
            row.set(1, augmented.get(1));
            output.add(row);
        }
        return output;
    }

    private static void offer(PriorityQueue<DataRow> heap, DataRow row, int n, RowComparator comparator) {
        if (heap.size() < n) {
            heap.add(row);
        } else if (comparator.compare(row, heap.peek()) < 0) {
            heap.poll();
            heap.add(row);
        }
    }

    private static void assertRowsEqual(List<DataRow> expected, List<DataRow> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(Arrays.asList(expected.get(i).get(0), expected.get(i).get(1)),
                    Arrays.asList(actual.get(i).get(0), actual.get(i).get(1)), "row=" + i);
        }
    }
}
