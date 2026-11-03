package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The legacy segmented class keeps only a compatibility entry point, so a second spill and merge algorithm is never maintained. */
class SpillingSegmentCompatibilityTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("v", DataType.LongType));

    private static final class LongSum implements Accumulator {
        private long total;

        @Override
        public byte[] writeState() {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeLong(total);
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
            return bytes.toByteArray();
        }

        @Override
        public void mergeState(byte[] state) {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(state))) {
                total += in.readLong();
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }
    }

    private static Supplier<Object[]> accumulators() {
        return () -> new Object[]{new LongSum()};
    }

    private static byte[] key(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void feed(GroupAggBuffer buffer, int distinct, int rounds) {
        for (int round = 0; round < rounds; round++) {
            for (int i = 0; i < distinct; i++) {
                ((LongSum) buffer.getOrCreate(key("key-" + i))[0]).total += i + round;
            }
        }
    }

    private static List<String> drain(GroupAggBuffer buffer) {
        List<String> rows = new ArrayList<>();
        for (GroupAggBuffer.Group group : buffer) {
            rows.add(new String(group.keyBytes(), StandardCharsets.UTF_8)
                    + "=" + ((LongSum) group.accumulators()[0]).total);
        }
        return rows;
    }

    private static GroupAggBuffer legacy(long budget, HeapPool pool, MemoryManager memory) {
        return new SpillingSegmentGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, pool, new MemorySpillStore());
    }

    private static GroupAggBuffer primary(long budget, HeapPool pool, MemoryManager memory) {
        return new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, accumulators(), memory, pool, new MemorySpillStore());
    }

    @Test
    void compatibilityEntryIsDeprecatedAndContainsNoDuplicateMergeAlgorithm() throws Exception {
        assertTrue(SpillingSegmentGroupAggBuffer.class.isAnnotationPresent(Deprecated.class));
        String source = source("SpillingSegmentGroupAggBuffer.java");
        assertFalse(source.contains("Map<KeyWrapper"));
        assertFalse(source.contains("List<Group>"));
        assertFalse(source.contains("mergeRun("));
        assertTrue(source.contains("OpenAddressingGroupAggBuffer"), "the legacy entry delegates to the production implementation");
    }

    @Test
    void noSpillAndSpillStayExactlyInPrimaryOrder() {
        assertSameSequence(4096, 3, 2);
        assertSameSequence(128, 80, 3);
    }

    @Test
    void sequentialRepeatAndGroupCountRemainCompatible() {
        MemoryManager memory = new MemoryManager(128);
        SpillingSegmentGroupAggBuffer buffer = (SpillingSegmentGroupAggBuffer)
                legacy(128, new HeapPool(), memory);
        feed(buffer, 60, 3);

        List<String> first = drain(buffer);
        assertEquals(60L, buffer.groupCount());
        List<String> second = drain(buffer);
        assertEquals(first, second, "repeating a full iteration in order yields the same rows in the same order");
    }

    @Test
    void explicitSpillIsTheResourceZeroBoundaryAndDataRemainsReadable() {
        MemoryManager memory = new MemoryManager(128);
        HeapPool pool = new HeapPool();
        SpillingSegmentGroupAggBuffer buffer = (SpillingSegmentGroupAggBuffer) legacy(128, pool, memory);
        feed(buffer, 2, 2);

        assertTrue(memory.reservedBytes() > 0);
        assertTrue(pool.outstanding() > 0);
        buffer.spill();
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(2, drain(buffer).size(), "iteration is still correct after an explicit spill");
        assertEquals(0L, memory.reservedBytes(), "a full spill iterator releases the merge reservation");
    }

    private static void assertSameSequence(long budget, int distinct, int rounds) {
        GroupAggBuffer legacy = legacy(budget, new HeapPool(), new MemoryManager(budget));
        GroupAggBuffer primary = primary(budget, new HeapPool(), new MemoryManager(budget));
        feed(legacy, distinct, rounds);
        feed(primary, distinct, rounds);
        assertEquals(drain(primary), drain(legacy));
    }

    private static String source(String fileName) throws Exception {
        Path cwd = Paths.get("").toAbsolutePath();
        Path path = cwd.resolve("src/main/java/io/rowweave/mem").resolve(fileName);
        if (!Files.exists(path)) {
            path = cwd.resolve("rowweave-core/src/main/java/io/rowweave/mem").resolve(fileName);
        }
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
