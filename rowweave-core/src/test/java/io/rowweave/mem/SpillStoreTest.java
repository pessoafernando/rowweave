package io.rowweave.mem;

import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.BoundedReplayRun;
import io.rowweave.mem.spill.BudgetedFrameReader;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.mem.spill.SpillRunWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SpillStoreTest {

    private static List<byte[]> drain(SpillRun run) {
        List<byte[]> out = new ArrayList<>();
        Iterator<byte[]> it = run.read();
        while (it.hasNext()) {
            out.add(it.next());
        }
        return out;
    }

    @Test
    void memoryStoreRoundTripPreservesFrameOrderAndBytes() {
        try (SpillStore store = new MemorySpillStore()) {
            SpillRun run = store.createRun();
            run.append(new byte[]{1, 2, 3});
            run.append(new byte[]{});      // empty frame preserved
            run.append(new byte[]{9});
            run.finishWriting();

            List<byte[]> frames = drain(run);
            assertEquals(3, frames.size());
            assertArrayEquals(new byte[]{1, 2, 3}, frames.get(0));
            assertArrayEquals(new byte[]{}, frames.get(1));
            assertArrayEquals(new byte[]{9}, frames.get(2));
        }
    }

    @Test
    void memoryStoreRunIsReReadable() {
        try (SpillStore store = new MemorySpillStore()) {
            SpillRun run = store.createRun();
            run.append(new byte[]{7});
            run.finishWriting();
            assertEquals(1, drain(run).size());
            assertEquals(1, drain(run).size()); // second read from the top
        }
    }

    @Test
    void diskStoreRoundTripAndCleanup(@TempDir Path tmp) {
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        SpillRun run = store.createRun();
        run.append(new byte[]{1, 2, 3});
        run.append(new byte[]{});
        run.finishWriting();

        List<byte[]> frames = drain(run);
        assertEquals(2, frames.size());
        assertArrayEquals(new byte[]{1, 2, 3}, frames.get(0));
        assertArrayEquals(new byte[]{}, frames.get(1));
        assertEquals(1, store.runsCreated());

        File[] before = tmp.toFile().listFiles();
        assertNotNull(before);
        assertTrue(before.length >= 1);
        store.close();
        File[] after = tmp.toFile().listFiles();
        assertTrue(after == null || after.length == 0, "close must delete spill files");
    }

    @Test
    void budgetedReaderHoldsExactlyOneFrameLeaseAndDoesNotPrefetch(@TempDir Path tmp) {
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        BoundedReplayRun run = store.createRun();
        run.append(new byte[]{1, 2, 3});
        run.append(new byte[]{4, 5});
        run.finishWriting();
        MemoryManager memory = new MemoryManager(64 * 1024);

        BudgetedFrameReader reader = run.openBudgetedReader(memory, 1, 3);
        assertArrayEquals(new byte[]{1, 2, 3}, reader.nextFrame());
        long firstLease = memory.reservedBytes();
        assertTrue(firstLease > 0);
        assertArrayEquals(new byte[]{4, 5}, reader.nextFrame());
        assertTrue(memory.reservedBytes() <= firstLease,
                "next must release the previous lease before reserving another");
        assertNull(reader.nextFrame());
        assertEquals(0L, memory.reservedBytes());
        assertEquals(0, store.currentOpenReaders());
        store.close();
    }

    @Test
    void budgetedReaderEscrowsDescriptorMaximumBeforeDownstreamConsumesTheBudget(
            @TempDir Path tmp) {
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        BoundedReplayRun run = store.createRun();
        run.append(new byte[]{1, 2, 3});
        run.finishWriting();
        MemoryManager memory = new MemoryManager(64 * 1024);

        BudgetedFrameReader reader = run.openBudgetedReader(memory, 1, 3);
        long downstreamCredit = memory.budget() - memory.reservedBytes();
        assertTrue(memory.tryReserve(downstreamCredit),
                "downstream may consume every byte not escrowed by the replay descriptor");
        try {
            assertArrayEquals(new byte[]{1, 2, 3}, reader.nextFrame(),
                    "the admitted replay frame must still make progress at the owner cap");
        } finally {
            memory.releaseExact(downstreamCredit);
            reader.close();
            store.close();
        }
        assertEquals(0L, memory.reservedBytes());
    }

    @Test
    void budgetedReaderRejectsDescriptorViolationBeforeFrameAllocation(@TempDir Path tmp) {
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        BoundedReplayRun run = store.createRun();
        run.append(new byte[]{1, 2, 3, 4});
        run.finishWriting();
        MemoryManager memory = new MemoryManager(64 * 1024);

        BudgetedFrameReader reader = run.openBudgetedReader(memory, 1, 3);
        assertThrows(IllegalArgumentException.class, reader::nextFrame);
        assertEquals(0L, memory.reservedBytes(), "invalid length must close I/O and release all credits");
        assertEquals(0, store.currentOpenReaders());
        store.close();
    }

    @Test
    void diskRunRejectsReadBeforeFinish(@TempDir Path tmp) {
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        SpillRun run = store.createRun();
        run.append(new byte[]{1});
        assertThrows(IllegalStateException.class, run::read);
        store.close();
    }

    @Test
    void diskRunRejectsAppendAfterFinish(@TempDir Path tmp) {
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        SpillRun run = store.createRun();
        run.finishWriting();
        assertThrows(IllegalStateException.class, () -> run.append(new byte[]{1}));
        store.close();
    }

    @Test
    void unfinishedDiskRunCanBeAbortedWithoutLeakingWriterOrFile(@TempDir Path tmp) {
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        SpillRun run = store.createRun();
        run.append(new byte[]{1, 2, 3});

        run.abort();
        run.abort(); // idempotent

        assertThrows(IllegalStateException.class, () -> run.append(new byte[]{4}));
        assertThrows(IllegalStateException.class, run::read);
        assertEquals(0, store.currentOpenWriters());
        File[] files = tmp.toFile().listFiles();
        assertTrue(files == null || files.length == 0, "aborted partial run must be deleted immediately");
        store.close();
    }

    @Test
    void storeCloseClosesUnfinishedWriter(@TempDir Path tmp) {
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        SpillRun run = store.createRun();
        run.append(new byte[]{9});
        assertEquals(1, store.currentOpenWriters());

        store.close();

        assertEquals(0, store.currentOpenWriters());
        assertThrows(IllegalStateException.class, () -> run.append(new byte[]{10}));
        File[] files = tmp.toFile().listFiles();
        assertTrue(files == null || files.length == 0);
    }

    @Test
    void finishedMemoryRunDisposeIsIdempotentAndInvalidatesPayload() {
        MemorySpillStore store = new MemorySpillStore();
        SpillRun run = store.createRun();
        run.append(new byte[]{1, 2, 3});
        run.finishWriting();

        run.dispose();
        run.dispose();

        assertThrows(IllegalStateException.class, run::read);
        assertThrows(IllegalStateException.class, () -> run.append(new byte[]{4}));
        store.close();
    }

    @Test
    void finishedDiskRunDisposeClosesItsReaderAndDeletesOnlyItsFile(@TempDir Path tmp) {
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        SpillRun disposed = store.createRun();
        disposed.append(new byte[]{1});
        disposed.finishWriting();
        SpillRun retained = store.createRun();
        retained.append(new byte[]{2});
        retained.finishWriting();

        Iterator<byte[]> openReader = disposed.read();
        assertTrue(openReader.hasNext());
        assertEquals(1, store.currentOpenReaders());
        disposed.dispose();
        disposed.dispose();

        assertEquals(0, store.currentOpenReaders(), "dispose must close readers owned by that run");
        assertThrows(IllegalStateException.class, disposed::read);
        assertEquals(1, drain(retained).size(), "disposing one run must not invalidate siblings");
        File[] remaining = tmp.toFile().listFiles();
        assertNotNull(remaining);
        assertEquals(1, remaining.length, "finished run file must be deleted immediately");
        store.close();
    }

    @Test
    void writerAbortFailureIsSuppressedBehindBodyRoot() {
        RuntimeException root = new RuntimeException("append failed");
        RuntimeException cleanup = new RuntimeException("abort failed");
        SpillStore store = new SpillStore() {
            @Override public SpillRun createRun() {
                return new SpillRun() {
                    @Override public void append(byte[] frame) { throw root; }
                    @Override public void finishWriting() { }
                    @Override public Iterator<byte[]> read() { return java.util.Collections.emptyIterator(); }
                    @Override public void abort() { throw cleanup; }
                };
            }
            @Override public void close() { }
        };

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> SpillRunWriter.write(store, run -> run.append(new byte[]{1})));

        assertSame(root, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(cleanup, thrown.getSuppressed()[0]);
    }
}
