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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SortBufferResourceTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("v", DataType.StringType));
    private static final SortField[] FIELDS = {
            new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};

    @Test
    void pointerBufferFailsBeforeOversizedAllocationAndCloseBalancesResources() {
        MemoryManager memory = new MemoryManager(32);
        HeapPool pool = new HeapPool();
        PointerSortBuffer buffer = new PointerSortBuffer(META, comparator(), FIELDS,
                pool, memory, 1024, new MemorySpillStore());

        assertThrows(MemoryBudgetExceededException.class,
                () -> buffer.putRow(row(1, repeat('x', 4096))));
        buffer.close();

        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
    }

    @Test
    void segmentBufferFailsBeforeOversizedAllocationAndCloseBalancesResources() {
        MemoryManager memory = new MemoryManager(32);
        HeapPool pool = new HeapPool();
        SpillingSegmentSortBuffer buffer = new SpillingSegmentSortBuffer(
                META, comparator(), pool, memory, 1024, new MemorySpillStore());

        assertThrows(MemoryBudgetExceededException.class,
                () -> buffer.putRow(row(1, repeat('y', 4096))));
        buffer.close();

        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
    }

    @Test
    void heapSpillingBufferDoesNotKeepGhostReservationAfterFailure() {
        MemoryManager memory = new MemoryManager(32);
        SpillingSortBuffer buffer = new SpillingSortBuffer(
                META, comparator(), memory, 1024, new MemorySpillStore());

        assertThrows(MemoryBudgetExceededException.class,
                () -> buffer.putRow(row(1, repeat('z', 4096))));
        buffer.close();

        assertEquals(0, memory.reservedBytes());
    }

    private static RowComparator comparator() {
        return new RowComparator(META, FIELDS);
    }

    private static DataRow row(int key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static String repeat(char value, int count) {
        StringBuilder result = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            result.append(value);
        }
        return result.toString();
    }
}
