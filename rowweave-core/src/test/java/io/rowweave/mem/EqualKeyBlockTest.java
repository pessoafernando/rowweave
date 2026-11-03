package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.EqualKeyBlock;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class EqualKeyBlockTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    private static List<Integer> drain(Iterator<DataRow> it) {
        List<Integer> out = new ArrayList<>();
        while (it.hasNext()) {
            out.add((Integer) it.next().get(0));
        }
        return out;
    }

    @Test
    void heapModeBelowThresholdPreservesOrder() {
        try (EqualKeyBlock block = new EqualKeyBlock(META, new MemorySpillStore(), 100)) {
            for (int i = 0; i < 5; i++) {
                block.add(row(i));
            }
            block.finish();
            assertEquals(java.util.Arrays.asList(0, 1, 2, 3, 4), drain(block.iterator()));
        }
    }

    @Test
    void iteratorIsReReadableMultipleTimes() {
        try (EqualKeyBlock block = new EqualKeyBlock(META, new MemorySpillStore(), 100)) {
            for (int i = 0; i < 3; i++) {
                block.add(row(i));
            }
            block.finish();
            assertEquals(java.util.Arrays.asList(0, 1, 2), drain(block.iterator()));
            assertEquals(java.util.Arrays.asList(0, 1, 2), drain(block.iterator())); // again, from top
            assertEquals(java.util.Arrays.asList(0, 1, 2), drain(block.iterator()));
        }
    }

    @Test
    void spillModeAboveThresholdPreservesOrder() {
        // threshold=3 forces spill once the 4th+ rows arrive
        try (EqualKeyBlock block = new EqualKeyBlock(META, new MemorySpillStore(), 3)) {
            for (int i = 0; i < 10; i++) {
                block.add(row(i));
            }
            block.finish();
            assertEquals(java.util.Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), drain(block.iterator()));
        }
    }

    @Test
    void spilledBlockIsReReadable() {
        try (EqualKeyBlock block = new EqualKeyBlock(META, new MemorySpillStore(), 2)) {
            for (int i = 0; i < 6; i++) {
                block.add(row(i));
            }
            block.finish();
            List<Integer> expected = java.util.Arrays.asList(0, 1, 2, 3, 4, 5);
            assertEquals(expected, drain(block.iterator()));
            assertEquals(expected, drain(block.iterator())); // re-read after spill
        }
    }

    @Test
    void spillToRealDiskPreservesOrder(@TempDir Path tmp) {
        try (DiskSpillStore store = new DiskSpillStore(tmp.toFile());
             EqualKeyBlock block = new EqualKeyBlock(META, store, 4)) {
            for (int i = 0; i < 20; i++) {
                block.add(row(i * 7));
            }
            block.finish();
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                expected.add(i * 7);
            }
            assertEquals(expected, drain(block.iterator()));
            assertEquals(expected, drain(block.iterator()));
        }
    }

    @Test
    void nullSpillStoreStaysHeapEvenAboveThreshold() {
        // no SpillStore -> degrade to pure heap (threshold ignored for spill)
        try (EqualKeyBlock block = new EqualKeyBlock(META, null, 2)) {
            for (int i = 0; i < 5; i++) {
                block.add(row(i));
            }
            block.finish();
            assertEquals(java.util.Arrays.asList(0, 1, 2, 3, 4), drain(block.iterator()));
        }
    }

    @Test
    void emptyBlockIteratesEmpty() {
        try (EqualKeyBlock block = new EqualKeyBlock(META, new MemorySpillStore(), 10)) {
            block.finish();
            assertFalse(block.iterator().hasNext());
        }
    }

    @Test
    void closeDisposesFinishedRunAndAbandonedReader(@TempDir Path tmp) throws Exception {
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        EqualKeyBlock block = new EqualKeyBlock(META, store, 1);
        for (int i = 0; i < 20; i++) {
            block.add(row(i));
        }
        block.finish();
        Iterator<DataRow> abandoned = block.iterator();
        assertEquals(0, abandoned.next().get(0));
        assertEquals(1, store.currentOpenReaders());
        assertTrue(fileCount(tmp) > 0);

        block.close();
        block.close();

        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
        assertEquals(0L, fileCount(tmp));
        assertThrows(IllegalStateException.class, () -> block.add(row(99)));
        assertThrows(IllegalStateException.class, block::finish);
        assertThrows(IllegalStateException.class, block::iterator);
        store.close();
    }

    @Test
    void throwingDisposeIsAttemptedOnceAndBlockStillBecomesClosed() {
        AtomicInteger disposeCalls = new AtomicInteger();
        RuntimeException disposeFailure = new RuntimeException("dispose-root");
        SpillStore store = new SpillStore() {
            @Override
            public SpillRun createRun() {
                return new SpillRun() {
                    @Override public void append(byte[] frame) { }
                    @Override public void finishWriting() { }
                    @Override public Iterator<byte[]> read() {
                        return java.util.Collections.<byte[]>emptyList().iterator();
                    }
                    @Override public void dispose() {
                        disposeCalls.incrementAndGet();
                        throw disposeFailure;
                    }
                };
            }
            @Override public void close() { }
        };
        EqualKeyBlock block = new EqualKeyBlock(META, store, 1);
        block.add(row(1));
        block.add(row(2));
        block.finish();

        assertSame(disposeFailure, assertThrows(RuntimeException.class, block::close));
        block.close();

        assertEquals(1, disposeCalls.get(), "dispose is attempted only once for a run of unknown ownership");
        assertThrows(IllegalStateException.class, block::iterator);
    }

    private static long fileCount(Path dir) throws Exception {
        try (Stream<Path> files = Files.list(dir)) {
            return files.count();
        }
    }
}
