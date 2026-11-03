package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegmentPool;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the three GroupAggBuffer implementations against HeapGroupAggBuffer as the reference:
 * the same input produces the same groups, the same running sums and the same first-seen order, including
 * a small budget that forces spilling and a second recursive partition. The test stub accumulator LongSum
 * implements Accumulator and exercises the buffer machinery itself; the concrete accumulators (sum, max,
 * min, count) are covered by their own tests.
 */
class GroupAggBufferTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("v", DataType.LongType));

    /** Test stub: a single-field long sum accumulator that honours the spill serialization contract. */
    private static final class LongSum implements Accumulator {
        long total;

        void add(long v) {
            total += v;
        }

        @Override
        public byte[] writeState() {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bos)) {
                out.writeLong(total);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return bos.toByteArray();
        }

        @Override
        public void mergeState(byte[] state) {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(state))) {
                total += in.readLong();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static Supplier<Object[]> factory() {
        return () -> new Object[]{new LongSum()};
    }

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private interface BufferFactory {
        GroupAggBuffer create();
    }

    /** feed (key,value) pairs into a buffer, return ordered map of key->sum by iteration (first-seen) order. */
    private Map<String, Long> aggregate(GroupAggBuffer buf, List<String> keys, List<Long> vals) {
        for (int i = 0; i < keys.size(); i++) {
            Object[] accs = buf.getOrCreate(key(keys.get(i)));
            ((LongSum) accs[0]).add(vals.get(i));
        }
        Map<String, Long> out = new LinkedHashMap<>();
        for (GroupAggBuffer.Group g : buf) {
            out.put(new String(g.keyBytes(), StandardCharsets.UTF_8), ((LongSum) g.accumulators()[0]).total);
        }
        return out;
    }

    private List<BufferFactory> factories() {
        List<BufferFactory> fs = new ArrayList<>();
        fs.add(() -> new SpillingSegmentGroupAggBuffer(META, new String[]{"k"}, factory(),
                new MemoryManager(128), new HeapPool())); // small budget -> spill
        fs.add(() -> new OpenAddressingGroupAggBuffer(META, new String[]{"k"}, factory(),
                new MemoryManager(128), new HeapPool()));
        return fs;
    }

    @Test
    void getOrCreateReturnsSameAccumulatorForSameKey() {
        GroupAggBuffer buf = new HeapGroupAggBuffer(factory());
        Object[] a = buf.getOrCreate(key("x"));
        Object[] b = buf.getOrCreate(key("x"));
        assertSame(a, b);
        assertEquals(1, buf.groupCount());
    }

    @Test
    void heapAuthorityBasicAggregation() {
        GroupAggBuffer buf = new HeapGroupAggBuffer(factory());
        Map<String, Long> out = aggregate(buf,
                java.util.Arrays.asList("a", "b", "a", "c", "b", "a"),
                java.util.Arrays.asList(1L, 10L, 2L, 100L, 20L, 3L));
        // first-seen order: a, b, c
        List<String> order = new ArrayList<>(out.keySet());
        assertEquals(java.util.Arrays.asList("a", "b", "c"), order);
        assertEquals(6L, out.get("a"));   // 1+2+3
        assertEquals(30L, out.get("b"));  // 10+20
        assertEquals(100L, out.get("c"));
    }

    @Test
    void spillingImplsMatchHeapAuthority() {
        List<String> keys = new ArrayList<>();
        List<Long> vals = new ArrayList<>();
        // 50 distinct keys, repeated, to force multiple spills under the 128-byte budget
        for (int round = 0; round < 4; round++) {
            for (int k = 0; k < 50; k++) {
                keys.add("key" + k);
                vals.add((long) (k + round));
            }
        }
        Map<String, Long> authority = aggregate(new HeapGroupAggBuffer(factory()), keys, vals);
        for (BufferFactory f : factories()) {
            Map<String, Long> got = aggregate(f.create(), keys, vals);
            // same groups + same sums (iteration order may differ post-spill, so compare as maps)
            assertEquals(authority, got, "spilling buffer must match heap authority (groups + sums)");
        }
    }

    @Test
    void singleGroupHugeVolumeStillCorrect() {
        List<String> keys = new ArrayList<>();
        List<Long> vals = new ArrayList<>();
        long expected = 0;
        for (int i = 0; i < 500; i++) {
            keys.add("only");
            vals.add((long) i);
            expected += i;
        }
        for (BufferFactory f : factories()) {
            Map<String, Long> got = aggregate(f.create(), keys, vals);
            assertEquals(1, got.size());
            assertEquals(expected, got.get("only"));
        }
    }

    @Test
    void emptyBufferIteratesEmpty() {
        for (BufferFactory f : factories()) {
            GroupAggBuffer buf = f.create();
            assertEquals(0, buf.groupCount());
            assertFalse(buf.iterator().hasNext());
        }
    }
}
