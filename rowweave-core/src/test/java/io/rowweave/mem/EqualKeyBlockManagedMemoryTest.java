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
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.ops.SortField;
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

/** EqualKeyBlock must be managed by bytes rather than by row count alone. */
class EqualKeyBlockManagedMemoryTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType), new Field("payload", DataType.StringType));

    @Test
    void wideRowsSpillBeforeLargeRowThresholdAndReleaseEverything(@TempDir Path tmp) throws Exception {
        MemoryManager memory = new MemoryManager(64 * 1024);
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        EqualKeyBlock block = new EqualKeyBlock(META, store, 10_000, memory);
        for (int i = 0; i < 80; i++) {
            block.add(row(i, 2_048));
        }
        block.finish();

        assertEquals(range(80), ids(block.iterator()));
        assertEquals(range(80), ids(block.iterator()), "a spilled block can be read again");
        assertTrue(store.runsCreated() > 0, "80 rows is below the threshold of 10000, so only the byte budget can force a real disk run");
        assertTrue(memory.peakReservedBytes() > 0);
        assertTrue(memory.peakReservedBytes() <= memory.budget());
        assertTrue(memory.spillCount() > 0, "a byte-triggered spill is visible in the metrics");

        block.close();
        block.close();
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
        assertEquals(0, fileCount(tmp));
        store.close();
    }

    @Test
    void fastHeapPathKeepsAndThenReleasesReservation(@TempDir Path tmp) {
        MemoryManager memory = new MemoryManager(1024 * 1024);
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        EqualKeyBlock block = new EqualKeyBlock(META, store, 100, memory);
        block.add(row(1, 32));
        block.add(row(2, 32));
        block.finish();

        assertEquals(java.util.Arrays.asList(1, 2), ids(block.iterator()));
        assertEquals(0, store.runsCreated());
        assertTrue(memory.reservedBytes() > 0);

        block.close();
        assertEquals(0, memory.reservedBytes());
        store.close();
    }

    @Test
    void appendRootSurvivesDisposeFailureAndReservationIsReleased() {
        MemoryManager memory = new MemoryManager(1024 * 1024);
        RuntimeException appendRoot = new RuntimeException("append-root");
        RuntimeException disposeFailure = new RuntimeException("dispose-cleanup");
        AtomicInteger disposeCalls = new AtomicInteger();
        SpillStore store = new SpillStore() {
            @Override
            public SpillRun createRun() {
                return new SpillRun() {
                    private int appends;
                    @Override public void append(byte[] frame) {
                        if (++appends == 2) throw appendRoot;
                    }
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
        EqualKeyBlock block = new EqualKeyBlock(META, store, 2, memory);
        block.add(row(1, 64));
        block.add(row(2, 64));

        RuntimeException actual = assertThrows(RuntimeException.class, () -> block.add(row(3, 64)));

        assertSame(appendRoot, actual);
        assertArrayEquals(new Throwable[]{disposeFailure}, actual.getSuppressed());
        assertEquals(0, memory.reservedBytes());
        assertEquals(1, disposeCalls.get());
        block.close();
        assertEquals(1, disposeCalls.get(), "after fail-stop a run of unknown ownership is not retried");
    }

    @Test
    void managedBlockWithoutSpillStoreFailsLoudInsteadOfGrowingUnaccounted() {
        MemoryManager memory = new MemoryManager(4 * 1024);
        EqualKeyBlock block = new EqualKeyBlock(META, null, 10_000, memory);

        assertThrows(MemoryBudgetExceededException.class, () -> block.add(row(1, 8_192)));

        assertEquals(0, memory.reservedBytes());
        block.close();
    }

    @Test
    void spillConversionKeepsSerializationEscrowAcrossSharedPoolContention() {
        ManagedMemory pool = ManagedMemory.ofBytes(4096);
        MemoryManager[] owners = pool.acquireOwners(
                ManagedMemory.OwnerSpec.elastic(128, 4096),
                ManagedMemory.OwnerSpec.elastic(128, 4096));
        MemorySpillStore store = new MemorySpillStore();
        EqualKeyBlock block = new EqualKeyBlock(META, store, 1, owners[0]);
        block.add(row(1, 32));
        block.add(row(2, 32));

        long spillEscrow = owners[0].reservedBytes();
        assertTrue(spillEscrow > 0, "spill mode must retain reusable serialization scratch");
        long borrowed = owners[1].availableBytes();
        assertTrue(owners[1].tryReserve(borrowed));
        block.add(row(3, 32));
        block.finish();
        assertEquals(java.util.Arrays.asList(1, 2, 3), ids(block.iterator()));

        block.close();
        owners[1].releaseExact(borrowed);
        pool.release(owners[0]);
        pool.release(owners[1]);
        assertEquals(4096, pool.availableBytes());
        store.close();
    }

    @Test
    void freshSpillBlockCanBorrowCallerHeldSerializationEscrow() {
        ManagedMemory pool = ManagedMemory.ofBytes(128 * 1024);
        MemoryManager[] owners = pool.acquireOwners(
                ManagedMemory.OwnerSpec.partitioned(16 * 1024, 64 * 1024, 2),
                ManagedMemory.OwnerSpec.elastic(16 * 1024, 128 * 1024));
        MemoryManager side = owners[0].partitionEvenly(2)[0];
        HeapPool heap = new HeapPool();
        SortField[] sort = {new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        SortBuffer sorted = ExternalSortBuffers.pointer(
                META, new io.rowweave.ops.RowComparator(META, sort), sort,
                heap, side, 1, new MemorySpillStore());
        sorted.putRow(row(1, 32));
        sorted.putRow(row(2, 32));
        sorted.sort();
        for (DataRow ignored : sorted) {
            // leave only replay scratch owned by the sort buffer
        }
        long replayScratch = sorted.retainedReplayScratchBytes();
        assertTrue(replayScratch > 0);
        long borrowed = owners[1].availableBytes();
        assertTrue(owners[1].tryReserve(borrowed));
        long sideFiller = side.availableBytes();
        assertTrue(sideFiller > 0);
        assertTrue(side.tryReserve(sideFiller));
        assertEquals(0, side.availableBytes(), "fresh block must start with no reservable capacity");

        MemorySpillStore store = new MemorySpillStore();
        EqualKeyBlock block = new EqualKeyBlock(META, store, 1, side);
        block.add(row(1, 32));
        block.add(row(2, 32));
        block.finish();
        assertEquals(java.util.Arrays.asList(1, 2), ids(block.iterator()));

        block.close();
        assertEquals(Math.addExact(replayScratch, sideFiller), side.reservedBytes(),
                "borrowed escrow remains owned by the caller");
        side.releaseExact(sideFiller);
        sorted.close();
        assertEquals(0, side.reservedBytes());
        owners[1].releaseExact(borrowed);
        pool.release(owners[0]);
        pool.release(owners[1]);
        assertEquals(0, heap.outstanding());
        store.close();
    }

    private static DataRow row(int id, int width) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        char[] payload = new char[width];
        java.util.Arrays.fill(payload, (char) ('a' + id % 20));
        row.set(1, new String(payload));
        return row;
    }

    private static List<Integer> ids(Iterator<DataRow> rows) {
        List<Integer> ids = new ArrayList<>();
        while (rows.hasNext()) ids.add((Integer) rows.next().get(0));
        return ids;
    }

    private static List<Integer> range(int count) {
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < count; i++) values.add(i);
        return values;
    }

    private static long fileCount(Path dir) throws Exception {
        try (Stream<Path> files = Files.list(dir)) {
            return files.count();
        }
    }
}
