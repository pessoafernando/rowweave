package io.rowweave.ops;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.ManagedMemory;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.EscrowedExternalSortBuffer;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;
import io.rowweave.plan.PushDagExecutor;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Production budget, exact spill handover and resource root cause for Top. */
class TopManagedMemoryTest {

    private static final DataSchema WIDE = new DataSchema(
            new Field("key", DataType.IntegerType),
            new Field("payload", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));
    private static final SortField KEY_ASC =
            new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);

    @Test
    void productionTopIsManagedAndSpillsWithinTinyBudget() {
        assertTrue(PlanOps.needsMemory("top"), "a sorted Top is a bounded-input breaker and belongs in ExecutionMemory");

        List<DataRow> input = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) {
            int key = (i * 7919) % 5_000;
            input.add(row(key, repeat((char) ('a' + i % 20), 512), new BigDecimal(key + ".12345678901")));
        }
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(WIDE, input),
                PlanNode.unary("top", WIDE, 0)
                        .param("n", "1000")
                        .listParam("sort", Collections.singletonList(PlanOps.encodeSortField(KEY_ASC)))
                        .build()));
        CollectingSink output = new CollectingSink(WIDE);
        ExecutionReport report = new PushDagExecutor(4, ManagedMemory.ofBytes(256 * 1024))
                .executeToOutput(graph, output);

        assertEquals(1_000, output.getRows().size());
        for (int i = 0; i < 1_000; i++) {
            assertEquals(i, output.getRows().get(i).get(0));
        }
        assertTrue(report.acquiredBytes() > 0, "Top really takes the operator budget");
        assertTrue(report.peakReservedBytes() > 0, "the retained Top working set must be accounted for");
        assertTrue(report.withinBudget(), report.summary());
        assertTrue(report.spillCount() > 0, "256 KiB cannot hold 1000 wide rows so the spill is real: " + report.summary());
    }

    @Test
    void fastPathUsesOneManagedHeapAndReleasesItsReservation() {
        MemoryManager memory = new MemoryManager(1024 * 1024);
        AtomicInteger fallbackCreated = new AtomicInteger();
        Top top = Top.of(WIDE, 3, KEY_ASC).withSpill(memory, (meta, comparator) -> {
            fallbackCreated.incrementAndGet();
            return new TrackingSortBuffer(comparator, null);
        });
        CollectingRowSink output = new CollectingRowSink();

        top.applyStream(RowStream.fromIterable(Arrays.asList(
                row(5, "e", BigDecimal.ONE), row(1, "a", BigDecimal.TEN),
                row(4, "d", BigDecimal.ZERO), row(2, "b", BigDecimal.ONE),
                row(3, "c", BigDecimal.TEN))), 4, output);

        assertEquals(Arrays.asList(1, 2, 3), keys(output.rows()));
        assertEquals(0, fallbackCreated.get(), "with a sufficient budget no serialization or external-sort cost is paid");
        assertTrue(memory.peakReservedBytes() > 0);
        assertEquals(0, memory.reservedBytes(), "the fast-path credit is released once the output succeeds");
    }

    @Test
    void forcedFallbackMatchesStableFullSortAtP1AndP4() {
        List<DataRow> input = Arrays.asList(
                row(2, "first-2", new BigDecimal("1.00000000001")),
                row(null, "null-1", new BigDecimal("2.00000000001")),
                row(1, "first-1", new BigDecimal("3.00000000001")),
                row(2, "second-2", new BigDecimal("4.00000000001")),
                row(null, "null-2", new BigDecimal("5.00000000001")),
                row(1, "second-1", new BigDecimal("6.00000000001")),
                row(0, "zero", new BigDecimal("7.00000000001")));
        List<DataRow> oracle = new ArrayList<>(input);
        oracle.sort(new RowComparator(WIDE, KEY_ASC));
        oracle = new ArrayList<>(oracle.subList(0, 5));

        for (int parallelism : new int[]{1, 4}) {
            MemoryManager memory = new MemoryManager(1);  // the first candidate already triggers the fallback
            TrackingFactory factory = new TrackingFactory();
            Top top = Top.of(WIDE, 5, KEY_ASC).withSpill(memory, factory::create);
            CollectingRowSink output = new CollectingRowSink();

            top.applyStream(RowStream.fromIterable(input), parallelism, output);

            assertRowsEqual(oracle, output.rows());
            assertEquals(1, factory.created.get());
            assertEquals(1, factory.last.closeCount);
            assertEquals(0, memory.reservedBytes());
        }
    }

    @Test
    void controlAndDownstreamFailureCloseFallbackAndPreserveBusinessRoot() {
        RuntimeException closeFailure = new RuntimeException("close-failure");
        TrackingFactory controlFactory = new TrackingFactory();
        Top controlled = Top.of(WIDE, 2, KEY_ASC)
                .withSpill(new MemoryManager(1), controlFactory::create);
        RuntimeException upstreamFailure = new RuntimeException("upstream");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(row(2, "b", BigDecimal.ONE)),
                StreamEvent.error(upstreamFailure));
        Iterator<StreamEvent> eventIterator = events.iterator();
        CollectingRowSink controlOut = new CollectingRowSink();

        controlled.applyStream(() -> eventIterator.hasNext() ? eventIterator.next() : null, 1, controlOut);

        assertTrue(controlOut.sawError());
        assertSame(upstreamFailure, controlOut.error());
        assertEquals(1, controlFactory.last.closeCount);

        TrackingFactory failingFactory = new TrackingFactory(closeFailure);
        Top failing = Top.of(WIDE, 2, KEY_ASC)
                .withSpill(new MemoryManager(1), failingFactory::create);
        RuntimeException business = new RuntimeException("downstream-business");
        RowSink throwing = event -> {
            if (event.isRecord()) {
                throw business;
            }
        };

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> failing.applyStream(RowStream.fromIterable(Arrays.asList(
                        row(2, "b", BigDecimal.ONE), row(1, "a", BigDecimal.TEN))), 1, throwing));

        assertSame(business, actual);
        assertEquals(1, failingFactory.last.closeCount);
        assertArrayEquals(new Throwable[]{closeFailure}, actual.getSuppressed());
    }

    private static DataRow row(Integer key, String payload, BigDecimal amount) {
        DataRow row = new DataRow(3);
        row.set(0, key);
        row.set(1, payload);
        row.set(2, amount);
        return row;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static List<Integer> keys(List<DataRow> rows) {
        List<Integer> keys = new ArrayList<>();
        for (DataRow row : rows) {
            keys.add((Integer) row.get(0));
        }
        return keys;
    }

    private static void assertRowsEqual(List<DataRow> expected, List<DataRow> actual) {
        assertEquals(expected.size(), actual.size());
        for (int row = 0; row < expected.size(); row++) {
            assertEquals(expected.get(row).size(), actual.get(row).size());
            for (int column = 0; column < expected.get(row).size(); column++) {
                assertEquals(expected.get(row).get(column), actual.get(row).get(column),
                        "row=" + row + " column=" + column);
            }
        }
    }

    private static final class TrackingFactory {
        final AtomicInteger created = new AtomicInteger();
        final RuntimeException closeFailure;
        TrackingSortBuffer last;

        TrackingFactory() {
            this(null);
        }

        TrackingFactory(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        EscrowedExternalSortBuffer create(DataSchema ignored, RowComparator comparator) {
            created.incrementAndGet();
            last = new TrackingSortBuffer(comparator, closeFailure);
            return last;
        }
    }

    private static final class TrackingSortBuffer implements EscrowedExternalSortBuffer {
        private final Comparator<DataRow> comparator;
        private final RuntimeException closeFailure;
        private final List<DataRow> rows = new ArrayList<>();
        private int closeCount;

        TrackingSortBuffer(Comparator<DataRow> comparator, RuntimeException closeFailure) {
            this.comparator = comparator;
            this.closeFailure = closeFailure;
        }

        @Override
        public void putRow(DataRow row) {
            rows.add(row);
        }

        @Override
        public void adoptAuxEscrow(long comparatorCredit, long headCredit) {
            // test double does not own a real MemoryManager
        }

        @Override
        public void importEscrowedRow(DataRow row, int serializedSize, long rowEstimate, long scratch,
                                      long credit, boolean batchStart, boolean batchEnd) {
            rows.add(row);
        }

        @Override
        public void sort() {
            rows.sort(comparator);
        }

        @Override
        public long size() {
            return rows.size();
        }

        @Override
        public Iterator<DataRow> iterator() {
            return rows.iterator();
        }

        @Override
        public void close() {
            closeCount++;
            rows.clear();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
