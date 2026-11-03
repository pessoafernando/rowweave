package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillRunWriter;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A run is owned precisely by its buffer, and store or job close is only the last fallback. */
class ExternalSortRunOwnershipTest {

    private static final DataSchema META = new DataSchema(new Field("k", DataType.IntegerType));
    private static final SortField[] FIELDS = {
            new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};

    @Test
    void legacyEntriesAreThinAdaptersAndProductionCallersUseSharedCore() throws IOException {
        assertTrue(SpillingSegmentSortBuffer.class.isAnnotationPresent(Deprecated.class));
        assertTrue(SpillingSortBuffer.class.isAnnotationPresent(Deprecated.class));
        for (String file : new String[]{"SpillingSegmentSortBuffer.java", "SpillingSortBuffer.java"}) {
            String source = memorySource(file);
            assertTrue(source.contains("PointerSortBuffer.comparatorOnly"));
            assertFalse(source.contains("PriorityQueue<"));
            assertFalse(source.contains("SpillRunWriter"));
            assertFalse(source.contains("encodeFrame("));
        }
        for (String source : new String[]{opsSource("ReduceGroup.java"),
                joinSource("CoGroup.java"), joinSource("SortMergeJoin.java")}) {
            assertFalse(source.contains("new SpillingSegmentSortBuffer"));
            assertFalse(source.contains("new SpillingSortBuffer"));
        }
    }

    @Test
    void mergeDisposesObsoleteRunsAndCloseDisposesFinalRuns(@TempDir Path tmp) throws IOException {
        DiskSpillStore spill = new DiskSpillStore(tmp.toFile());
        MemoryManager memory = new MemoryManager(512 * 1024);
        HeapPool pool = new HeapPool();
        PointerSortBuffer buffer = pointer(spill, memory, pool, new RowComparator(META, FIELDS));
        int rows = WorkerConfig.mergeFanIn() * 2 + 3;
        for (int i = 0; i < rows; i++) {
            buffer.putRow(row(rows - i));
        }

        buffer.sort();

        long afterMerge = fileCount(tmp);
        assertTrue(afterMerge <= WorkerConfig.mergeFanIn(),
                "obsolete input runs must be deleted after every successful merge pass: " + afterMerge);
        int previous = Integer.MIN_VALUE;
        for (DataRow value : buffer) {
            int current = (Integer) value.get(0);
            assertTrue(current >= previous);
            previous = current;
        }
        assertEquals(0, spill.currentOpenReaders());

        buffer.close();

        assertEquals(0, fileCount(tmp));
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        assertEquals(0, spill.currentOpenReaders());

        SpillRun proof = SpillRunWriter.write(spill, run -> run.append(new byte[]{1}));
        assertEquals(1, fileCount(tmp), "buffer close must not close the shared store");
        proof.dispose();
        assertEquals(0, fileCount(tmp));
        spill.close();
    }

    @Test
    void closeStopsAbandonedIteratorAndKeepsSiblingBufferReadable(@TempDir Path tmp) throws IOException {
        DiskSpillStore spill = new DiskSpillStore(tmp.toFile());
        PointerSortBuffer left = pointer(spill, new MemoryManager(512 * 1024), new HeapPool(),
                new RowComparator(META, FIELDS), 5);
        PointerSortBuffer right = pointer(spill, new MemoryManager(512 * 1024), new HeapPool(),
                new RowComparator(META, FIELDS), 5);
        for (int i = 0; i < 100; i++) {
            left.putRow(row(200 - i));
            right.putRow(row(i));
        }
        left.sort();
        right.sort();
        long bothBuffers = fileCount(tmp);
        Iterator<DataRow> abandoned = left.iterator();
        assertTrue(abandoned.hasNext());
        abandoned.next();
        assertTrue(spill.currentOpenReaders() > 0);

        left.close();

        assertEquals(0, spill.currentOpenReaders(), "left close must close its abandoned readers");
        assertTrue(fileCount(tmp) < bothBuffers, "left close must delete only its own runs");
        List<Integer> rightRows = new ArrayList<>();
        for (DataRow value : right) {
            rightRows.add((Integer) value.get(0));
        }
        assertEquals(100, rightRows.size(), "closing a sibling must not invalidate this buffer");
        assertEquals(Integer.valueOf(0), rightRows.get(0));
        assertEquals(Integer.valueOf(99), rightRows.get(99));

        right.close();
        assertEquals(0, fileCount(tmp));
        spill.close();
    }

    @Test
    void comparatorFailureKeepsRootAndCloseReclaimsEveryRun(@TempDir Path tmp) throws IOException {
        DiskSpillStore spill = new DiskSpillStore(tmp.toFile());
        MemoryManager memory = new MemoryManager(512 * 1024);
        HeapPool pool = new HeapPool();
        RuntimeException root = new RuntimeException("compare-root");
        java.util.Comparator<DataRow> failing = (left, right) -> {
            throw root;
        };
        PointerSortBuffer buffer = pointer(spill, memory, pool, failing);
        buffer.putRow(row(1));
        buffer.putRow(row(1));

        buffer.sort();
        RuntimeException actual = assertThrows(RuntimeException.class, buffer::iterator);
        assertSame(root, actual);

        buffer.close();
        assertEquals(0, fileCount(tmp));
        assertEquals(0, spill.currentOpenReaders());
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        spill.close();
    }

    private static PointerSortBuffer pointer(DiskSpillStore spill, MemoryManager memory,
                                             HeapPool pool, java.util.Comparator<DataRow> comparator) {
        return pointer(spill, memory, pool, comparator, 1);
    }

    private static PointerSortBuffer pointer(DiskSpillStore spill, MemoryManager memory,
                                             HeapPool pool, java.util.Comparator<DataRow> comparator,
                                             int runThreshold) {
        return new PointerSortBuffer(META, comparator, FIELDS, pool, memory, runThreshold, spill);
    }

    private static DataRow row(int key) {
        DataRow row = new DataRow(1);
        row.set(0, key);
        return row;
    }

    private static long fileCount(Path path) throws IOException {
        try (java.util.stream.Stream<Path> files = Files.list(path)) {
            return files.count();
        }
    }

    private static String memorySource(String file) throws IOException {
        return source("src/main/java/io/rowweave/mem/" + file,
                "rowweave-core/src/main/java/io/rowweave/mem/" + file);
    }

    private static String opsSource(String file) throws IOException {
        return source("src/main/java/io/rowweave/ops/" + file,
                "rowweave-core/src/main/java/io/rowweave/ops/" + file);
    }

    private static String joinSource(String file) throws IOException {
        return source("src/main/java/io/rowweave/ops/join/" + file,
                "rowweave-core/src/main/java/io/rowweave/ops/join/" + file);
    }

    private static String source(String moduleRelative, String rootRelative) throws IOException {
        Path cwd = Paths.get("").toAbsolutePath();
        Path path = cwd.resolve(moduleRelative);
        if (!Files.exists(path)) {
            path = cwd.resolve(rootRelative);
        }
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
