package io.rowweave.ops;

import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.GroupCombineFunction;
import io.rowweave.GroupReduceFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.ManagedMemory;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

    /** All four owner kinds must route EqualKeyBlock through the existing operator memory manager. */
class EqualKeyBlockOwnerMemoryTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("payload", DataType.StringType));
    private static final DataSchema COUNT_META = new DataSchema(new Field("count", DataType.IntegerType));

    private static final GroupCombineFunction FIRST = (rows, out) -> {
        java.util.Iterator<DataRow> iterator = rows.iterator();
        if (iterator.hasNext()) out.collect(iterator.next());
    };

    @Test
    void planCombineActuallyAcquiresMemoryAndByteSpills() {
        List<DataRow> rows = rows(800, 1, 512);
        Map<String, String> params = new LinkedHashMap<>();
        params.put("shards", "4");
        PlanNode combine = PlanNode.unaryFnWithParams("combineGroup", META, 0,
                params, Collections.emptyMap(), FIRST);
        PlanGraph graph = new PlanGraph(Arrays.asList(PlanNode.source(META, rows), combine), 4);
        CollectingSink output = new CollectingSink(META);

        ExecutionReport report = new PushDagExecutor(4, ManagedMemory.ofBytes(256 * 1024))
                .executeToOutput(graph, output);

        assertEquals(4, output.getRows().size());
        assertTrue(report.acquiredBytes() > 0, "needsMemory=true corresponds to a real acquire");
        assertTrue(report.peakReservedBytes() > 0);
        assertTrue(report.spillCount() > 0, report.summary());
        assertTrue(report.withinBudget(), report.summary());
    }

    @Test
    void reduceWholeTableUsesManagedBlock(@TempDir Path tmp) {
        MemoryManager memory = new MemoryManager(64 * 1024);
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        GroupReduceFunction count = new GroupReduceFunction() {
            @Override public DataSchema getResultSchema() { return COUNT_META; }
            @Override public void reduce(Iterable<DataRow> rows, Collector out) {
                int n = 0;
                for (DataRow ignored : rows) n++;
                DataRow result = new DataRow(1);
                result.set(0, n);
                out.collect(result);
            }
        };
        ReduceGroup reduce = ReduceGroup.of(META, new String[0], count)
                .withSpill(memory, store, new HeapPool());
        CollectingRowSink output = new CollectingRowSink();

        reduce.reduce(RowStream.fromIterable(rows(120, 1, 1_024)), output);

        assertEquals(120, output.rows().get(0).get(0));
        assertTrue(memory.spillCount() > 0, "an unordered whole-table reduce spills only through EqualKeyBlock");
        assertEquals(0, memory.reservedBytes());
    }

    @Test
    void managedCombineUserFailureReleasesBlockAndDiskRun(@TempDir Path tmp) throws Exception {
        MemoryManager memory = new MemoryManager(64 * 1024);
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        RuntimeException business = new RuntimeException("combine-business");
        CombineGroup combine = CombineGroup.of(META, (rows, out) -> {
                    rows.iterator().next();
                    throw business;
                })
                .withSpill(memory, store);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> combine.applyStream(RowStream.fromIterable(rows(120, 1, 1_024)),
                        1, new CollectingRowSink()));

        assertSame(business, actual);
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, store.currentOpenReaders());
        assertEquals(0, store.currentOpenWriters());
        assertEquals(0, fileCount(tmp));
    }

    @Test
    void sortMergeJoinRightBlockUsesRightSideMemory(@TempDir Path tmp) {
        MemoryManager memory = new MemoryManager(2L * 1024 * 1024);
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        SortMergeJoin join = SortMergeJoin.of(META, META,
                        new String[]{"k"}, new String[]{"k"}, JoinType.INNER)
                .withSpill(memory, store, new HeapPool())
                .withEqualKeyBlockThreshold(10_000);
        CollectingRowSink output = new CollectingRowSink();

        join.join(RowStream.fromIterable(rows(1, 7, 16)),
                RowStream.fromIterable(rows(320, 7, 2_048)), output);

        assertEquals(320, output.rows().size());
        assertTrue(memory.spillCount() > 0, "320 is below the threshold, so the right block spills on its own side byte budget");
        assertEquals(0, memory.reservedBytes());
    }

    @Test
    void coGroupBothBlocksUseTheirSideMemory(@TempDir Path tmp) {
        MemoryManager memory = new MemoryManager(2L * 1024 * 1024);
        DiskSpillStore store = new DiskSpillStore(tmp.toFile());
        CoGroupFunction counts = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return COUNT_META; }
            @Override public void coGroup(Iterable<DataRow> left, Iterable<DataRow> right, Collector out) {
                int n = 0;
                for (DataRow ignored : left) n++;
                for (DataRow ignored : right) n++;
                DataRow result = new DataRow(1);
                result.set(0, n);
                out.collect(result);
            }
        };
        CoGroup coGroup = CoGroup.of(META, META,
                        new String[]{"k"}, new String[]{"k"}, counts)
                .withSpill(memory, store, new HeapPool())
                .withEqualKeyBlockThreshold(10_000);
        CollectingRowSink output = new CollectingRowSink();

        coGroup.coGroup(RowStream.fromIterable(rows(260, 9, 2_048)),
                RowStream.fromIterable(rows(260, 9, 2_048)), output);

        assertEquals(520, output.rows().get(0).get(0));
        assertTrue(memory.spillCount() >= 2, "the left and right side blocks each spill on bytes");
        assertEquals(0, memory.reservedBytes());
    }

    private static List<DataRow> rows(int count, int key, int width) {
        List<DataRow> rows = new ArrayList<>(count);
        char[] payload = new char[width];
        Arrays.fill(payload, 'x');
        String value = new String(payload);
        for (int i = 0; i < count; i++) {
            DataRow row = new DataRow(2);
            row.set(0, key);
            row.set(1, value + i);
            rows.add(row);
        }
        return rows;
    }

    private static long fileCount(Path dir) throws Exception {
        try (Stream<Path> files = Files.list(dir)) {
            return files.count();
        }
    }
}
