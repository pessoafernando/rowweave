package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The four SortBuffer implementations compared uniformly: the sorted result must be
 * bit-identical to sorting directly with RowComparator, which is the JDK stable sort, across
 * nulls, extremes, spilling with a small runThreshold and a small budget, and stability. This
 * keeps a class of mis-sorting from reappearing.
 */
class SortBufferTest {

    private static final DataSchema NUM = new DataSchema(new Field("n", DataType.IntegerType));

    private static DataRow row(Integer v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    private static List<Integer> drain(SortBuffer buf) {
        List<Integer> out = new ArrayList<>();
        for (DataRow r : buf) {
            out.add((Integer) r.get(0));
        }
        return out;
    }

    private static List<Integer> expectedSorted(List<Integer> input, RowComparator cmp) {
        List<DataRow> rows = new ArrayList<>();
        for (Integer v : input) {
            rows.add(row(v));
        }
        rows.sort(cmp); // JDK stable sort = authority
        List<Integer> out = new ArrayList<>();
        for (DataRow r : rows) {
            out.add((Integer) r.get(0));
        }
        return out;
    }

    // each factory builds a SortBuffer for the given comparator/sortFields with a SMALL run threshold
    // so spilling/merge paths are exercised even on tiny inputs.
    private interface Factory {
        SortBuffer create(RowComparator cmp, SortField[] sf);
    }

    private void assertSortMatchesAuthority(Factory factory, SortField sf, List<Integer> input) {
        RowComparator cmp = new RowComparator(NUM, sf);
        SortBuffer buf = factory.create(cmp, new SortField[]{sf});
        for (Integer v : input) {
            buf.putRow(row(v));
        }
        assertEquals(input.size(), buf.size());
        buf.sort();
        assertEquals(expectedSorted(input, cmp), drain(buf));
    }

    private List<Factory> factories() {
        List<Factory> fs = new ArrayList<>();
        fs.add((cmp, sf) -> new HeapSortBuffer(NUM, cmp));
        fs.add((cmp, sf) -> new SpillingSortBuffer(NUM, cmp, new MemoryManager(64 * 1024), 2));
        fs.add((cmp, sf) -> {
            MemorySegmentPool pool = new HeapPool();
            return new SpillingSegmentSortBuffer(NUM, cmp, pool, new MemoryManager(64 * 1024), 2);
        });
        fs.add((cmp, sf) -> {
            MemorySegmentPool pool = new HeapPool();
            return new PointerSortBuffer(NUM, cmp, sf, pool, new MemoryManager(64 * 1024), 2);
        });
        return fs;
    }

    @Test
    void ascDefaultWithNullsAndExtremes() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        List<Integer> input = Arrays.asList(3, null, Integer.MIN_VALUE, 0, Integer.MAX_VALUE, -1, null, 2);
        for (Factory f : factories()) {
            assertSortMatchesAuthority(f, sf, input);
        }
    }

    @Test
    void descDefaultWithNulls() {
        SortField sf = new SortField(0, SortField.Direction.DESC, SortField.NullOrder.DEFAULT);
        List<Integer> input = Arrays.asList(5, null, 1, 3, null, Integer.MAX_VALUE, Integer.MIN_VALUE);
        for (Factory f : factories()) {
            assertSortMatchesAuthority(f, sf, input);
        }
    }

    @Test
    void nullsFirstAbsolute() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_FIRST);
        List<Integer> input = Arrays.asList(2, null, Integer.MIN_VALUE, null, 1);
        for (Factory f : factories()) {
            assertSortMatchesAuthority(f, sf, input);
        }
    }

    @Test
    void nullsLastAbsolute() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        List<Integer> input = Arrays.asList(2, null, Integer.MAX_VALUE, null, 1);
        for (Factory f : factories()) {
            assertSortMatchesAuthority(f, sf, input);
        }
    }

    @Test
    void emptyAndSingle() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        for (Factory f : factories()) {
            assertSortMatchesAuthority(f, sf, new ArrayList<>());
            assertSortMatchesAuthority(f, sf, Arrays.asList(42));
        }
    }

    @Test
    void largeInputForcesMultipleSpillRuns() {
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        List<Integer> input = new ArrayList<>();
        for (int i = 200; i >= 1; i--) {
            input.add(i % 7 == 0 ? null : i); // sprinkle nulls
        }
        for (Factory f : factories()) {
            assertSortMatchesAuthority(f, sf, input);
        }
    }

    @Test
    void stabilityEqualKeysKeepInsertionOrder() {
        // two-field rows but sort only on field 0; equal keys must keep insertion order (field 1 as witness)
        DataSchema meta = new DataSchema(
                new Field("k", DataType.IntegerType),
                new Field("seq", DataType.IntegerType));
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        RowComparator cmp = new RowComparator(meta, sf);

        List<Function<Void, SortBuffer>> fs = new ArrayList<>();
        fs.add(v -> new HeapSortBuffer(meta, cmp));
        fs.add(v -> new SpillingSortBuffer(meta, cmp, new MemoryManager(64 * 1024), 2));
        fs.add(v -> new SpillingSegmentSortBuffer(meta, cmp, new HeapPool(),
                new MemoryManager(64 * 1024), 2));
        fs.add(v -> new PointerSortBuffer(meta, cmp, new SortField[]{sf}, new HeapPool(),
                new MemoryManager(64 * 1024), 2));

        for (Function<Void, SortBuffer> factory : fs) {
            SortBuffer buf = factory.apply(null);
            // all key=1; witnesses 0..9 in insertion order
            for (int i = 0; i < 10; i++) {
                DataRow r = new DataRow(2);
                r.set(0, 1);
                r.set(1, i);
                buf.putRow(r);
            }
            buf.sort();
            List<Integer> witnesses = new ArrayList<>();
            for (DataRow r : buf) {
                witnesses.add((Integer) r.get(1));
            }
            assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), witnesses,
                    "equal keys must preserve insertion order (stable)");
        }
    }
}
