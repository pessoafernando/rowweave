package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
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
 * OpenAddressingGroupAggBuffer.iterator() moves from a full List to a streaming Iterator.
 * The streaming order matches a full materialize group for group, keeping depth-first preorder,
 * and each group gets a fresh accumulator that is never reused across yields. Under high
 * cardinality, with many distinct keys and a tiny pool that spills, iterating no longer runs out
 * of memory because nothing is held in a List.
 */
class StreamingMaterializeTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("v", DataType.IntegerType));

    /** A LongSum accumulator implementing Accumulator, matching the authoritative one in GroupAggBufferTest, where writeState and mergeState survive a spill round trip. */
    static final class LongSum implements Accumulator {
        long total;
        void add(long v) { total += v; }
        @Override public byte[] writeState() {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bos)) { out.writeLong(total); }
            catch (IOException e) { throw new UncheckedIOException(e); }
            return bos.toByteArray();
        }
        @Override public void mergeState(byte[] state) {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(state))) { total += in.readLong(); }
            catch (IOException e) { throw new UncheckedIOException(e); }
        }
    }

    private static Supplier<Object[]> factory() {
        return () -> new Object[]{new LongSum()};
    }

    private static byte[] key(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Drain the buffer into an ordered list of (key, sum), keeping the iteration order so it can be compared position by position. */
    private static List<String> drainOrdered(GroupAggBuffer buf) {
        List<String> order = new ArrayList<>();
        for (GroupAggBuffer.Group g : buf) {
            order.add(new String(g.keyBytes(), StandardCharsets.UTF_8)
                    + "=" + ((LongSum) g.accumulators()[0]).total);
        }
        return order;
    }

    private static void feed(GroupAggBuffer buf, List<String> keys, List<Long> vals) {
        for (int i = 0; i < keys.size(); i++) {
            Object[] accs = buf.getOrCreate(key(keys.get(i)));
            ((LongSum) accs[0]).add(vals.get(i));
        }
    }

    /**
     * With spilling, the streaming group order and values match an oracle that materializes the
     * whole buffer into a List first. The old logic is not reachable by reflection here, so the
     * same instance is drained twice and the two orders compared: read is re-entrant, so the two
     * passes must agree exactly, which shows the streaming path is deterministic and repeatable.
     */
    @Test
    void streamingOrderDeterministicAndRepeatable() {
        List<String> keys = new ArrayList<>();
        List<Long> vals = new ArrayList<>();
        for (int round = 0; round < 4; round++) {
            for (int k = 0; k < 60; k++) {
                keys.add("key" + k);
                vals.add((long) (k + round));
            }
        }
        // a 128B budget forces spilling and recursive re-partitioning
        GroupAggBuffer buf = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, factory(), new MemoryManager(128), new HeapPool());
        feed(buf, keys, vals);

        List<String> first = drainOrdered(buf);
        List<String> second = drainOrdered(buf);  // read is re-entrant, so a second pass must give the identical order
        assertEquals(first, second, "the streaming iteration is deterministic and repeatable (same order, same values)");
        assertEquals(60, first.size(), "60 distinct key");
        // value correctness: the sum for keyK is the total of (K+round) for round 0..3, which is 4K+6
        for (String entry : first) {
            String[] kv = entry.split("=");
            int k = Integer.parseInt(kv[0].substring(3));
            assertEquals(4L * k + 6, Long.parseLong(kv[1]), "key" + k + " sum");
        }
    }

    /** Within one iteration each group's accumulator is a separate reference, never the same array reused across yields. */
    @Test
    void eachGroupHasFreshAccumulatorReference() {
        List<String> keys = new ArrayList<>();
        List<Long> vals = new ArrayList<>();
        for (int round = 0; round < 3; round++) {
            for (int k = 0; k < 50; k++) {
                keys.add("k" + k);
                vals.add(1L);
            }
        }
        GroupAggBuffer buf = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, factory(), new MemoryManager(128), new HeapPool());
        feed(buf, keys, vals);

        // Collect the accumulator reference for every group and read its total right away, as the consumer's buildResultRow does.
        List<Object[]> accRefs = new ArrayList<>();
        Map<String, Long> totals = new LinkedHashMap<>();
        for (GroupAggBuffer.Group g : buf) {
            accRefs.add(g.accumulators());
            totals.put(new String(g.keyBytes(), StandardCharsets.UTF_8), ((LongSum) g.accumulators()[0]).total);
        }
        // Any two groups must hold different accumulator array references, with no scratch array reused across yields.
        for (int i = 0; i < accRefs.size(); i++) {
            for (int j = i + 1; j < accRefs.size(); j++) {
                assertNotSame(accRefs.get(i), accRefs.get(j),
                        "group " + i + " and " + j + " must not share one accumulator reference");
            }
        }
        assertEquals(50, totals.size());
        for (long t : totals.values()) {
            assertEquals(3L, t, "each key accumulates 3 times");
        }
    }

    /**
     * Shows the iteration really is lazy rather than a full materialize: with a counting
     * SpillStore, only part of the runs has been read after the first group is consumed. An eager
     * materialize would read every run before the iterator is returned or its first next, so the
     * read count would cover all runs.
     */
    @Test
    void iterationIsLazyNotEager() {
        // many distinct keys plus a tiny budget, so several spill partitions and several runs.
        java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
        SpillStore counting = new SpillStore() {
            private final SpillStore delegate = new MemorySpillStore();
            @Override public SpillRun createRun() {
                SpillRun r = delegate.createRun();
                return new SpillRun() {
                    @Override public void append(byte[] f) { r.append(f); }
                    @Override public void finishWriting() { r.finishWriting(); }
                    @Override public java.util.Iterator<byte[]> read() {
                        reads.incrementAndGet();  // count each run as it is read
                        return r.read();
                    }
                };
            }
            @Override public void close() { delegate.close(); }
        };
        GroupAggBuffer buf = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, factory(), new MemoryManager(128), new HeapPool(), counting);
        List<String> keys = new ArrayList<>();
        List<Long> vals = new ArrayList<>();
        for (int k = 0; k < 100; k++) { keys.add("key" + k); vals.add(1L); }
        feed(buf, keys, vals);

        java.util.Iterator<GroupAggBuffer.Group> it = buf.iterator();
        assertTrue(it.hasNext());
        it.next();  // consume only the first group
        int readsAfterFirst = reads.get();

        // keep consuming to the end
        while (it.hasNext()) { it.next(); }
        int readsAfterAll = reads.get();

        // Lazy: fewer runs were read while consuming the first group than after consuming all of them, otherwise it was eager.
        assertTrue(readsAfterFirst < readsAfterAll,
                "streaming reads runs lazily on demand (reads after the first group=" + readsAfterFirst + " < reads after all groups " + readsAfterAll + ")");
    }

    /** High-cardinality spilling does not exhaust memory: many distinct keys, a tiny pool, and groups consumed one at a time through the stream instead of a List. */
    @Test
    void highCardinalityStreamingNoOom() {
        GroupAggBuffer buf = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, factory(), new MemoryManager(256), new HeapPool());
        int distinct = 200_000;
        for (int i = 0; i < distinct; i++) {
            Object[] accs = buf.getOrCreate(key("k" + i));
            ((LongSum) accs[0]).add(1L);
        }
        // Consume as a stream: only count and accumulate to check, without collecting into a List, as emitting to a bounded sink does.
        long count = 0;
        long sumOfSums = 0;
        for (GroupAggBuffer.Group g : buf) {
            count++;
            sumOfSums += ((LongSum) g.accumulators()[0]).total;
        }
        assertEquals(distinct, count, "every one of the 200k distinct keys is emitted");
        assertEquals(distinct, sumOfSums, "each group sums to 1 so the total equals the group count");
    }
}
