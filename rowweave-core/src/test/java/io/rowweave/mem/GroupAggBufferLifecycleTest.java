package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A group breaker must deterministically release every managed resource at the operator boundary. */
class GroupAggBufferLifecycleTest {

    private static final DataSchema META = new DataSchema(new Field("k", DataType.StringType));

    @TempDir
    Path tempDir;

    @Test
    void noSpillCloseReleasesResidentOwnershipAndUnregistersFromMemoryManager() throws Exception {
        MemoryManager memory = new MemoryManager(256);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.toFile());
        OpenAddressingGroupAggBuffer buffer = buffer(memory, pool, store);
        buffer.getOrCreate(key("a"));
        buffer.getOrCreate(key("b"));

        assertEquals(128L, memory.reservedBytes());
        assertEquals(2, pool.outstanding());
        assertTrue(AutoCloseable.class.isAssignableFrom(GroupAggBuffer.class));

        close(buffer);
        close(buffer);

        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0L, fileCount(tempDir));
        assertThrows(IllegalStateException.class, buffer::groupCount, "the buffer stays terminal after close");

        memory.reserve(memory.budget());
        assertThrows(MemoryBudgetExceededException.class, () -> memory.reserve(1),
                "a closed buffer unregisters and receives no further spill callback");
        memory.releaseExact(memory.budget());
    }

    @Test
    void closeReleasesAbandonedMergeReservationAndAllSpillRuns() throws Exception {
        MemoryManager memory = new MemoryManager(128);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.toFile());
        OpenAddressingGroupAggBuffer buffer = buffer(memory, pool, store);
        for (int i = 0; i < 100; i++) {
            ((LongSum) buffer.getOrCreate(key("key-" + i))[0]).total++;
        }

        Iterator<GroupAggBuffer.Group> iterator = buffer.iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertTrue(store.runsCreated() > 0, "a tiny budget proves the spill path is taken");
        assertTrue(memory.reservedBytes() > 0, "the current merge frame still holds its reservation when abandoned early");

        close(buffer);

        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
        assertEquals(0L, fileCount(tempDir));
        assertThrows(IllegalStateException.class, iterator::hasNext,
                "an iterator captured before buffer close still terminates");
    }

    @Test
    void spillStoreCloseFailureDoesNotBlockResidentOrAccountingCleanup() {
        MemoryManager memory = new MemoryManager(128);
        HeapPool pool = new HeapPool();
        RuntimeException storeFailure = new RuntimeException("store-close-root");
        SpillStore store = new SpillStore() {
            private final MemorySpillStore delegate = new MemorySpillStore();

            @Override
            public SpillRun createRun() {
                return delegate.createRun();
            }

            @Override
            public void close() {
                delegate.close();
                throw storeFailure;
            }
        };
        OpenAddressingGroupAggBuffer buffer = new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, () -> new Object[]{new LongSum()}, memory, pool, store);
        buffer.getOrCreate(key("resident"));

        assertSame(storeFailure, assertThrows(RuntimeException.class, buffer::close));
        assertEquals(0L, memory.reservedBytes(), "a failed store cleanup does not block returning the memory manager");
        assertEquals(0, pool.outstanding(), "a failed store cleanup does not block returning segments");
        buffer.close();  // close must stay idempotent after a throwing close, and never retry a store of unknown ownership
    }

    private static OpenAddressingGroupAggBuffer buffer(MemoryManager memory, HeapPool pool,
                                                        DiskSpillStore store) {
        return new OpenAddressingGroupAggBuffer(
                META, new String[]{"k"}, () -> new Object[]{new LongSum()}, memory, pool, store);
    }

    private static void close(Object resource) throws Exception {
        ((AutoCloseable) resource).close();
    }

    private static byte[] key(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static long fileCount(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.count();
        }
    }

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
}
