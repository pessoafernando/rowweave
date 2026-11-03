package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The non-spilling in-heap buffers (HeapGroupAggBuffer and HeapSortBuffer) are the defaults when a
 * PlanGraph is bypassed and they are used directly. A hard row-count safety valve makes them fail loudly
 * past the limit (an IllegalStateException telling the caller to inject a spilling implementation) instead
 * of letting the heap die — failing fast is preferred to running out of memory. The production path uses
 * OpenAddressing and PointerSortBuffer and is not affected.
 */
class HeapBufferSafetyValveTest {

    private static final DataSchema NUM = new DataSchema(new Field("v", DataType.IntegerType));

    private static Supplier<Object[]> factory() {
        return () -> new Object[]{0L};
    }

    private static byte[] key(int i) {
        return new byte[]{(byte) i, (byte) (i >> 8)};
    }

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    /** HeapGroupAggBuffer past its row ceiling fails loudly instead of silently running out of memory. */
    @Test
    void groupAggBufferExceedsCapFailsLoud() {
        HeapGroupAggBuffer buf = new HeapGroupAggBuffer(factory(), 100);  // a small ceiling, to exercise the safety valve
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            for (int i = 0; i < 200; i++) {
                buf.getOrCreate(key(i));  // 200 distinct keys, above the ceiling of 100
            }
        });
        assertTrue(ex.getMessage().contains("spill") || ex.getMessage().contains("limit"),
                "the message should point at an injected spill implementation, observed: " + ex.getMessage());
    }

    /** Normal work inside the ceiling, and reuse of the same key does not count against it because only distinct keys are counted. */
    @Test
    void groupAggBufferWithinCapWorks() {
        HeapGroupAggBuffer buf = new HeapGroupAggBuffer(factory(), 100);
        for (int round = 0; round < 5; round++) {
            for (int i = 0; i < 50; i++) {
                buf.getOrCreate(key(i));  // 50 distinct keys replayed five rounds, so still 50 groups, below 100
            }
        }
        assertEquals(50, buf.groupCount());
    }

    /** HeapSortBuffer past its row ceiling fails loudly. */
    @Test
    void sortBufferExceedsCapFailsLoud() {
        HeapSortBuffer buf = new HeapSortBuffer(NUM, Comparator.comparingInt(r -> (int) r.get(0)), 100);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            for (int i = 0; i < 200; i++) {
                buf.putRow(row(i));
            }
        });
        assertTrue(ex.getMessage().contains("spill") || ex.getMessage().contains("limit"),
                "the message should point at an injected spill implementation, observed: " + ex.getMessage());
    }

    /** The default constructor, with no explicit ceiling, uses a large default that a small data set never reaches. */
    @Test
    void defaultCapDoesNotBreakSmallData() {
        HeapGroupAggBuffer gab = new HeapGroupAggBuffer(factory());
        for (int i = 0; i < 1000; i++) {
            gab.getOrCreate(key(i));
        }
        assertEquals(1000, gab.groupCount());

        HeapSortBuffer sb = new HeapSortBuffer(NUM, Comparator.comparingInt(r -> (int) r.get(0)));
        for (int i = 0; i < 1000; i++) {
            sb.putRow(row(i));
        }
        assertEquals(1000, sb.size());
    }
}
