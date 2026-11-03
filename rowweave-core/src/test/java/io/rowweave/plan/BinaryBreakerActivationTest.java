package io.rowweave.plan;

import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.ExecutionMemory;
import io.rowweave.mem.ManagedMemory;
import io.rowweave.ops.SortField;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** The real sort-merge join and co-group owners activate on their two-input dependency. */
class BinaryBreakerActivationTest {

    private static final long MAX = 4L * 1024 * 1024;
    private static final long MIN = 256L * 1024;
    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema JOIN_OUT = SortMergeJoin.of(
            LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER).getOutputSchema();
    private static final DataSchema COGROUP_OUT = new DataSchema(
            new Field("k", DataType.IntegerType),
            new Field("leftCount", DataType.IntegerType), new Field("rightCount", DataType.IntegerType));
    private static final CoGroupFunction COUNT_GROUP = new CoGroupFunction() {
        @Override public DataSchema getResultSchema() { return COGROUP_OUT; }
        @Override public void coGroup(Iterable<DataRow> left, Iterable<DataRow> right, Collector out) {
            Integer key = null;
            int leftCount = 0;
            for (DataRow row : left) {
                key = (Integer) row.get(0);
                leftCount++;
            }
            int rightCount = 0;
            for (DataRow row : right) {
                if (key == null) key = (Integer) row.get(0);
                rightCount++;
            }
            DataRow result = new DataRow(3);
            result.set(0, key);
            result.set(1, leftCount);
            result.set(2, rightCount);
            out.collect(result);
        }
    };

    private ManagedMemory pool;
    private final List<Long> grants = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void configureMemory() {
        System.setProperty("rowweave.worker.operator.maxMemory", Long.toString(MAX));
        System.setProperty("rowweave.worker.operator.minMemory", Long.toString(MIN));
        pool = ManagedMemory.ofBytes(MAX);
        ExecutionMemory.setAcquireListenerForTest((level, granted, waited, rounds) -> grants.add(granted));
    }

    @AfterEach
    void resetMemory() {
        ExecutionMemory.setAcquireListenerForTest(null);
        System.clearProperty("rowweave.worker.operator.maxMemory");
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    @Test
    void p1ExplicitSortMergeWaitsForLeftInputBeforeAcquire() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        PlanGraph graph = joinGraph(
                PlanNode.sourceFromInputs(LEFT, Collections.singletonList(
                        gatedInput(LEFT, Collections.singletonList(left(1, "L")), entered, release))),
                PlanNode.sourceFromInputs(RIGHT, Collections.singletonList(inputOf(
                        RIGHT, Collections.singletonList(right(1, "R"))))),
                JoinType.INNER, true, null);
        List<DataRow> output = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread execution = new Thread(() -> {
            try {
                new PushDagExecutor(1, pool).execute(graph, output::add);
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "p1-smj-activation");
        execution.start();

        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertTrue(grants.isEmpty(), "at parallelism 1 the sort-merge join acquires nothing before the first left event");
        assertEquals(0, pool.snapshot().reservedBytes(),
                "the JOIN admission guarantee is not an actual working-set reservation");
        assertEquals(MIN, pool.snapshot().guaranteedBytes());
        release.countDown();
        execution.join(20_000);

        assertFalse(execution.isAlive());
        assertNull(failure.get());
        assertEquals(1, output.size());
        assertEquals(Arrays.asList(1, "L", 1, "R"), cells(output.get(0)));
        assertEquals(Collections.singletonList(MAX), grants);
        assertEquals(MAX, pool.availableBytes());
    }

    @Test
    void bothEmptySkipsLeaseButRightOuterAndCoGroupRightOnlyRemainCorrect() {
        List<DataRow> empty = new ArrayList<>();
        new PushDagExecutor(1, pool).execute(joinGraph(
                PlanNode.source(LEFT, empty), PlanNode.source(RIGHT, empty),
                JoinType.FULL, true, null), row -> fail("a join over two empty inputs emits nothing"));
        assertTrue(grants.isEmpty(), "a sort-merge join over two empty inputs needs no lease");

        List<DataRow> rights = Arrays.asList(right(3, "A"), right(3, "B"));
        List<DataRow> joined = new ArrayList<>();
        new PushDagExecutor(1, pool).execute(joinGraph(
                PlanNode.source(LEFT, empty), PlanNode.source(RIGHT, rights),
                JoinType.RIGHT, true, null), joined::add);
        assertEquals(2, joined.size());
        for (DataRow row : joined) {
            assertNull(row.get(0));
            assertNull(row.get(1));
            assertEquals(3, row.get(2));
        }
        assertEquals(Collections.singletonList(MAX), grants);

        grants.clear();
        List<DataRow> grouped = new ArrayList<>();
        new PushDagExecutor(1, pool).execute(coGroupGraph(
                PlanNode.source(LEFT, empty), PlanNode.source(RIGHT, rights), null), grouped::add);
        assertEquals(1, grouped.size());
        assertEquals(Arrays.asList(3, 0, 2), cells(grouped.get(0)));
        assertEquals(Collections.singletonList(MAX), grants);
        assertEquals(MAX, pool.availableBytes());
    }

    @Test
    void p4OnlyPartitionWithRowsAcquiresLeaseAfterSourceGate() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        PlanNode leftSource = PlanNode.sourceFromInputs(LEFT, Collections.singletonList(
                gatedInput(LEFT, Collections.singletonList(left(7, "L")), entered, release)));
        PlanNode rightSource = PlanNode.sourceFromInputs(RIGHT, Collections.singletonList(
                inputOf(RIGHT, Collections.singletonList(right(7, "R")))));
        CollectingSink output = new CollectingSink(JOIN_OUT);
        PlanGraph graph = joinGraph(leftSource, rightSource, JoinType.INNER, true, output);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<ExecutionReport> report = new AtomicReference<>();
        Thread execution = new Thread(() -> {
            try {
                report.set(new PushDagExecutor(4, pool).runSinkGraphReported(graph));
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "p4-smj-activation");
        execution.start();

        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertTrue(grants.isEmpty(), "at parallelism 4 no consumer acquires in bulk before the router emits its first row");
        assertEquals(0, pool.snapshot().reservedBytes(),
                "the four JOIN owners that never saw a first row hold only the guarantee");
        assertEquals(4 * MIN, pool.snapshot().guaranteedBytes());
        release.countDown();
        execution.join(20_000);

        assertFalse(execution.isAlive());
        assertNull(failure.get());
        assertEquals(1, output.getRows().size());
        assertNotNull(report.get());
        assertEquals(TelemetryCoverage.COMPLETE, report.get().resourceTelemetryCoverage());
        assertEquals(TelemetryCoverage.COMPLETE, report.get().partitionTelemetryCoverage());
        assertEquals(PartitionRole.JOIN_CONSUMER,
                report.get().partitionDistributions().get(0).role());
        assertEquals(Collections.singletonList(MAX), grants,
                "of the 4 consumers only the partition whose key is hit activates its lease");
        assertEquals(MAX, pool.availableBytes());
    }

    @Test
    void mixedUnaryAndJoinOwnersRemainCorrectWhenJoinActivatesFirst() {
        List<DataRow> output = new ArrayList<>();

        new PushDagExecutor(1, pool).execute(mixedOwnerGraph(
                inputOf(LEFT, Collections.singletonList(left(9, "L"))),
                delayedInput(RIGHT, Collections.singletonList(right(9, "R"))),
                false), output::add);

        assertEquals(1, output.size());
        assertEquals(Arrays.asList(9, "L", 9, "R"), cells(output.get(0)));
        assertEquals(MAX, pool.availableBytes());
        assertEquals(0, pool.snapshot().activeOwnerCount());
    }

    @Test
    void mixedUnaryAndJoinOwnersRemainCorrectWhenUnaryActivatesFirst() {
        List<DataRow> output = new ArrayList<>();

        new PushDagExecutor(1, pool).execute(mixedOwnerGraph(
                delayedInput(LEFT, Collections.singletonList(left(7, "L"))),
                inputOf(RIGHT, Collections.singletonList(right(7, "R"))),
                false), output::add);

        assertEquals(1, output.size());
        assertEquals(Arrays.asList(7, "L", 7, "R"), cells(output.get(0)));
        assertEquals(MAX, pool.availableBytes());
        assertEquals(0, pool.snapshot().activeOwnerCount());
    }

    @Test
    void mixedUnaryAndCoGroupOwnersSelectCompatibleTokensAndCleanUp() {
        List<DataRow> output = new ArrayList<>();

        new PushDagExecutor(1, pool).execute(mixedOwnerGraph(
                inputOf(LEFT, Collections.singletonList(left(5, "L"))),
                delayedInput(RIGHT, Collections.singletonList(right(5, "R"))),
                true), output::add);

        assertEquals(1, output.size());
        assertEquals(Arrays.asList(5, 1, 1), cells(output.get(0)));
        assertEquals(MAX, pool.availableBytes());
        assertEquals(0, pool.snapshot().activeOwnerCount());
    }

    private static PlanGraph mixedOwnerGraph(Source leftInput, Source rightInput,
                                             boolean coGroup) {
        PlanNode left = PlanNode.sourceFromInputs(
                LEFT, Collections.singletonList(leftInput));
        PlanNode right = PlanNode.sourceFromInputs(
                RIGHT, Collections.singletonList(rightInput));
        PlanNode order = PlanNode.unary("orderBy", RIGHT, 1)
                .listParam("sort", Collections.singletonList(
                        PlanOps.encodeSortField(new SortField(0,
                                SortField.Direction.ASC,
                                SortField.NullOrder.NULLS_LAST))))
                .build();
        PlanNode binary;
        if (coGroup) {
            Map<String, List<String>> lists = new LinkedHashMap<>();
            lists.put("leftKeys", Collections.singletonList("lk"));
            lists.put("rightKeys", Collections.singletonList("rk"));
            binary = PlanNode.binaryFn("coGroup", COGROUP_OUT, 0, 2,
                    Collections.emptyMap(), lists, COUNT_GROUP);
        } else {
            binary = PlanNode.binary("join", JOIN_OUT, 0, 2)
                    .param("joinType", JoinType.INNER.name())
                    .param("joinHint", "SORT_MERGE")
                    .listParam("leftKeys", Collections.singletonList("lk"))
                    .listParam("rightKeys", Collections.singletonList("rk"))
                    .build();
        }
        return new PlanGraph(Arrays.asList(left, right, order, binary), 1);
    }

    private static PlanGraph joinGraph(PlanNode left, PlanNode right, JoinType type,
                                       boolean explicitSortMerge, CollectingSink output) {
        DataSchema outMeta = SortMergeJoin.of(
                LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, type).getOutputSchema();
        PlanNode.Builder builder = PlanNode.binary("join", outMeta, 0, 1)
                .param("joinType", type.name())
                .listParam("leftKeys", Collections.singletonList("lk"))
                .listParam("rightKeys", Collections.singletonList("rk"));
        if (explicitSortMerge) builder.param("joinHint", "SORT_MERGE");
        List<PlanNode> nodes = new ArrayList<>(Arrays.asList(left, right, builder.build()));
        if (output != null) nodes.add(PlanNode.sink(outMeta, 2, output).build());
        return new PlanGraph(nodes, output == null ? 1 : 4);
    }

    private static PlanGraph coGroupGraph(PlanNode left, PlanNode right, CollectingSink output) {
        Map<String, List<String>> lists = new LinkedHashMap<>();
        lists.put("leftKeys", Collections.singletonList("lk"));
        lists.put("rightKeys", Collections.singletonList("rk"));
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(left);
        nodes.add(right);
        nodes.add(PlanNode.binaryFn("coGroup", COGROUP_OUT, 0, 1,
                Collections.emptyMap(), lists, COUNT_GROUP));
        if (output != null) nodes.add(PlanNode.sink(COGROUP_OUT, 2, output).build());
        return new PlanGraph(nodes, output == null ? 1 : 4);
    }

    private static Source gatedInput(DataSchema meta, List<DataRow> rows,
                                    CountDownLatch entered, CountDownLatch release) {
        return new Source() {
            @Override public DataSchema getSchema() { return meta; }
            @Override public Iterator<DataRow> iterator() {
                Iterator<DataRow> delegate = rows.iterator();
                return new Iterator<DataRow>() {
                    private boolean first = true;
                    @Override public boolean hasNext() {
                        if (first) {
                            first = false;
                            entered.countDown();
                            try {
                                if (!release.await(5, TimeUnit.SECONDS)) {
                                    throw new AssertionError("source gate timeout");
                                }
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(interrupted);
                            }
                        }
                        return delegate.hasNext();
                    }
                    @Override public DataRow next() { return delegate.next(); }
                };
            }
        };
    }

    private static Source inputOf(DataSchema meta, List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return meta; }
            @Override public Iterator<DataRow> iterator() { return rows.iterator(); }
        };
    }

    private static Source delayedInput(DataSchema meta, List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return meta; }
            @Override public Iterator<DataRow> iterator() {
                Iterator<DataRow> delegate = rows.iterator();
                return new Iterator<DataRow>() {
                    private boolean delayed;
                    @Override public boolean hasNext() {
                        if (!delayed) {
                            delayed = true;
                            try {
                                Thread.sleep(500L);
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(interrupted);
                            }
                        }
                        return delegate.hasNext();
                    }
                    @Override public DataRow next() { return delegate.next(); }
                };
            }
        };
    }

    private static DataRow left(int key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static DataRow right(int key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static List<Object> cells(DataRow row) {
        List<Object> cells = new ArrayList<>(row.size());
        for (int i = 0; i < row.size(); i++) cells.add(row.get(i));
        return cells;
    }
}
