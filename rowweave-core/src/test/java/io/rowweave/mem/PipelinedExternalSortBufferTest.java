package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * PipelinedExternalSortBuffer (three threads: read, sort, spill) produces the **same row order** as
 * PointerSortBuffer (same comparator plus a global sequence number, so ties stay stable). Correctness
 * edges: BigDecimal scale, null handling, NULLS_LAST and runs of equal keys.
 */
class PipelinedExternalSortBufferTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType),
            new Field("name", DataType.StringType));
    private static final SortField[] SORT = {
            new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};

    private static DataRow row(Integer k, String amt, String name) {
        DataRow r = new DataRow(3);
        r.set(0, k);
        r.set(1, amt == null ? null : new BigDecimal(amt));
        r.set(2, name);
        return r;
    }

    private static List<DataRow> sortVia(SortBuffer buf, List<DataRow> input) {
        for (DataRow r : input) buf.putRow(r);
        buf.sort();
        List<DataRow> out = new ArrayList<>();
        for (DataRow r : buf) out.add(r);
        return out;
    }

    private static String fp(List<DataRow> rows) {
        StringBuilder sb = new StringBuilder();
        for (DataRow r : rows) {
            sb.append(r.get(0)).append('|')
              .append(r.get(1) == null ? "N" : ((BigDecimal) r.get(1)).toPlainString()).append('|')
              .append(r.get(2)).append('\n');
        }
        return sb.toString();
    }

    private PointerSortBuffer pointer(long budget, int runThreshold) {
        RowComparator cmp = new RowComparator(META, SORT);
        return new PointerSortBuffer(META, cmp, SORT, new HeapPool(),
                new MemoryManager(budget), runThreshold, new MemorySpillStore());
    }

    private PipelinedExternalSortBuffer pipelined(long budget, int runThreshold, int nBuffers) {
        RowComparator cmp = new RowComparator(META, SORT);
        return new PipelinedExternalSortBuffer(META, cmp, SORT, new MemoryManager(budget),
                runThreshold, nBuffers);
    }

    /** Row-for-row agreement: with a large data set and many equal keys, where ties follow the stable global sequence, the three-thread buffer matches the single-threaded PointerSortBuffer. */
    @Test
    void rowOrderMatchesPointerSortBuffer() {
        List<DataRow> input = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            input.add(row(i % 100, Integer.toString(i % 50) + ".5", "n" + i));  // many equal keys, since k only takes 100 values
        }
        input.add(row(null, "1.0", "nullkey"));

        List<DataRow> expect = sortVia(pointer(64L * 1024 * 1024, 256), new ArrayList<>(input));
        List<DataRow> actual = sortVia(pipelined(64L * 1024 * 1024, 256, 3), new ArrayList<>(input));
        assertEquals(expect.size(), actual.size(), "the row counts match");
        assertEquals(fp(expect), fp(actual), "the rows come out in the same order (the global seq breaks ties stably)");
    }

    /** A small budget forces heavy spilling and backpressure: the three-thread run falls back on spilling without an OOM and still matches the single-threaded output row for row. */
    @Test
    void heavySpillMatchesAndNoOom() {
        List<DataRow> input = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            input.add(row((i * 31) % 500, Integer.toString(i % 1000), "row" + i));
        }
        List<DataRow> expect = sortVia(pointer(32 * 1024, 128), new ArrayList<>(input));
        List<DataRow> actual = sortVia(pipelined(32 * 1024, 128, 3), new ArrayList<>(input)); // tiny budget + 3 buffer
        assertEquals(fp(expect), fp(actual), "three threads under heavy spill produce the same row order as one thread");
    }

    /** Empty input. */
    @Test
    void emptyInput() {
        List<DataRow> actual = sortVia(pipelined(1024 * 1024, 64, 2), new ArrayList<>());
        assertEquals(0, actual.size());
    }
}
