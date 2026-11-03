package io.rowweave.plan;

import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.GroupReduceFunction;
import io.rowweave.GroupCombineReduceFunction;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.ExecutionMemory;
import io.rowweave.mem.ManagedMemory;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** A real PlanOps unary breaker asks ManagedMemory only once its data dependency activates. */
class UnaryBreakerActivationTest {

    private static final long MAX = 4L * 1024 * 1024;
    private static final long MIN = 256L * 1024;
    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GROUPED = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();
    private static final DataSchema COUNT_OUT = new DataSchema(new Field("count", DataType.IntegerType));

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
    void compilingStagesDoesNotAcquireOperatorMemory() {
        PlanGraph graph = threeBreakerGraph(PlanNode.source(META, wideRows()));
        ExecutionMemory memory = ExecutionMemory.forJob(pool, PlanOps.countBreakers(graph));
        try {
            List<io.rowweave.sched.PipelineExecutor.Stage> stages =
                    new PlanCompiler(1, memory).compile(graph);
            assertEquals(4, stages.size());
            assertTrue(grants.isEmpty(), "compilation only describes the execution and requests no operator memory");
            assertEquals(0, pool.snapshot().reservedBytes(),
                    "the admission guarantee is not an actual buffer reservation");
            assertEquals(3 * MIN, pool.snapshot().guaranteedBytes());
        } finally {
            memory.close();
        }
    }

    @Test
    void blockedSourceKeepsPoolFreeThenDependencyOrderRecoversFinalBudget() throws Exception {
        List<DataRow> rows = wideRows();
        CountDownLatch sourceEntered = new CountDownLatch(1);
        CountDownLatch releaseSource = new CountDownLatch(1);
        Source gated = new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                Iterator<DataRow> delegate = rows.iterator();
                return new Iterator<DataRow>() {
                    private boolean first = true;
                    @Override public boolean hasNext() {
                        if (first) {
                            first = false;
                            sourceEntered.countDown();
                            try {
                                if (!releaseSource.await(5, TimeUnit.SECONDS)) {
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
        PlanGraph graph = threeBreakerGraph(
                PlanNode.sourceFromInputs(META, Collections.singletonList(gated)));
        List<DataRow> output = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<ExecutionReport> report = new AtomicReference<>();
        Thread execution = new Thread(() -> {
            try {
                report.set(new PushDagExecutor(1, pool).execute(graph, output::add));
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "unary-breaker-activation");
        execution.start();

        assertTrue(sourceEntered.await(5, TimeUnit.SECONDS));
        assertTrue(grants.isEmpty(), "while the first source row is blocked every downstream breaker acquires nothing");
        assertEquals(0, pool.snapshot().reservedBytes(),
                "a blocked source may hold the guarantee but must not occupy a buffer");
        assertEquals(3 * MIN, pool.snapshot().guaranteedBytes());

        releaseSource.countDown();
        execution.join(30_000);
        assertFalse(execution.isAlive(), "pipeline must complete");
        assertNull(failure.get());
        assertNotNull(report.get());
        assertEquals(800, output.size());
        assertEquals(Arrays.asList(MAX, MAX, MAX), grants,
                "three breakers activated one after another each borrow the freed allowance up to the cap");
        assertEquals(MAX * 3, report.get().acquiredBytes());
        assertEquals(MAX, pool.availableBytes(), "the pool is back to zero once the job finishes");
        for (int i = 1; i < output.size(); i++) {
            assertTrue(String.valueOf(output.get(i - 1).get(0))
                    .compareTo(String.valueOf(output.get(i).get(0))) <= 0, "the final orderBy produces a total order");
        }
    }

    @Test
    void emptyGroupBySkipsLeaseButWholeTableReducersStillRunOnce() {
        PlanGraph emptyGroup = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.unary("groupBySum", GROUPED, 0)
                        .listParam("keys", Collections.singletonList("k"))
                        .param("measure", "amt").build()));
        List<DataRow> grouped = new ArrayList<>();
        new PushDagExecutor(1, pool).execute(emptyGroup, grouped::add);
        assertTrue(grouped.isEmpty());
        assertTrue(grants.isEmpty(), "an empty groupBy needs no operator lease");

        AtomicInteger calls = new AtomicInteger();
        GroupReduceFunction fn = new GroupReduceFunction() {
            @Override public DataSchema getResultSchema() { return COUNT_OUT; }
            @Override public void reduce(Iterable<DataRow> values, Collector out) {
                calls.incrementAndGet();
                int count = 0;
                for (DataRow ignored : values) count++;
                DataRow row = new DataRow(1);
                row.set(0, count);
                out.collect(row);
            }
        };
        Map<String, List<String>> lists = new LinkedHashMap<>();
        lists.put("keys", Collections.emptyList());
        PlanGraph emptyReduce = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.unaryFnWithParams("reduceGroup", COUNT_OUT, 0,
                        Collections.emptyMap(), lists, fn)));
        List<DataRow> reduced = new ArrayList<>();
        new PushDagExecutor(1, pool).execute(emptyReduce, reduced::add);

        assertEquals(1, calls.get(), "a whole-table reduce over an empty stream still calls the user function once");
        assertEquals(1, reduced.size());
        assertEquals(0, reduced.get(0).get(0));
        assertEquals(Collections.singletonList(MAX), grants,
                "the whole-table reduce activates only once EOF arrives");

        AtomicInteger combineCalls = new AtomicInteger();
        AtomicInteger finalReduceCalls = new AtomicInteger();
        GroupCombineReduceFunction combineReduce = new GroupCombineReduceFunction() {
            @Override public DataSchema getResultSchema() { return COUNT_OUT; }
            @Override public void combine(Iterable<DataRow> values, Collector out) {
                combineCalls.incrementAndGet();
            }
            @Override public void reduce(Iterable<DataRow> values, Collector out) {
                finalReduceCalls.incrementAndGet();
                DataRow row = new DataRow(1);
                row.set(0, 0);
                out.collect(row);
            }
        };
        Map<String, String> combineParams = new LinkedHashMap<>();
        combineParams.put("shards", "4");
        PlanGraph emptyCombineReduce = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.unaryFnWithParams("combineReduceGroup", COUNT_OUT, 0,
                        combineParams, Collections.emptyMap(), combineReduce)));
        List<DataRow> combined = new ArrayList<>();
        new PushDagExecutor(1, pool).execute(emptyCombineReduce, combined::add);
        assertEquals(0, combineCalls.get(), "empty input forms no shard and combine is never called");
        assertEquals(1, finalReduceCalls.get(), "combineReduce over empty input still calls the final reduce once");
        assertEquals(1, combined.size());
        assertEquals(Arrays.asList(MAX, MAX), grants,
                "both empty-table reductions activate briefly only after EOF");
        assertEquals(MAX, pool.availableBytes());
    }

    private static PlanGraph threeBreakerGraph(PlanNode source) {
        PlanNode group = PlanNode.unary("groupBySum", GROUPED, 0)
                .listParam("keys", Collections.singletonList("k"))
                .param("measure", "amt").build();
        PlanNode distinct = PlanNode.unary("distinct", GROUPED, 1)
                .listParam("keys", Collections.singletonList("k")).build();
        PlanNode order = PlanNode.unary("orderBy", GROUPED, 2)
                .listParam("sort", Collections.singletonList(PlanOps.encodeSortField(new SortField(
                        0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)))).build();
        return new PlanGraph(Arrays.asList(source, group, distinct, order), 1);
    }

    private static List<DataRow> wideRows() {
        List<DataRow> rows = new ArrayList<>(1_600);
        char[] chars = new char[1_024];
        Arrays.fill(chars, 'x');
        String pad = new String(chars);
        for (int key = 0; key < 800; key++) {
            String value = String.format("k%04d-", key) + pad;
            for (int copy = 0; copy < 2; copy++) {
                DataRow row = new DataRow(2);
                row.set(0, value);
                row.set(1, BigDecimal.ONE);
                rows.add(row);
            }
        }
        return rows;
    }
}
