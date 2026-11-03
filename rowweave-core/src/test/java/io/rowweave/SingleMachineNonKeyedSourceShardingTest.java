package io.rowweave;

import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.TelemetryCoverage;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A source through a pass-through to a SINK, which is a non-keyed shape with no breaker, has its splits read in parallel over several Sources
 * through R routers rather than concatenated on one thread, judged by observing the name of the reading thread inside Source.iterator().
 *
 * <p>The invariant: this shape must not degrade into a single-threaded concat.
 */
class SingleMachineNonKeyedSourceShardingTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));

    private static DataRow row(String k, int amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    /** A Source that observes the reading thread, recording the name of the thread that calls iterator(). */
    private static Source probeInput(List<DataRow> rows, Set<String> readerThreads) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                readerThreads.add(Thread.currentThread().getName());
                return rows.iterator();
            }
        };
    }

    /** A source of N splits, then filter, then a SINK, collected by a CollectingSink. */
    private static PlanGraph filterGraph(List<Source> inputs, Sink out) {
        PlanNode src = PlanNode.sourceFromInputs(META, inputs);
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "amt >= 0").build();
        PlanNode sink = PlanNode.sink(META, 1, out).build();
        return new PlanGraph(Arrays.asList(src, filter, sink));
    }

    /**
     * A non-keyed source through filter to SINK with 4 splits at P=3 should show at least 2 reading threads, which is R-way parallelism.
     * R routers fan in to a single channel, with the pass-through chain running on the pipeline task.
     */
    @Test
    void nonKeyedMultiInputParallelRead() {
        Set<String> readerThreads = ConcurrentHashMap.newKeySet();
        List<Source> inputs = new ArrayList<>();
        for (int shard = 0; shard < 4; shard++) {
            List<DataRow> data = new ArrayList<>();
            for (int i = 0; i < 50; i++) data.add(row("k" + ((shard * 50 + i) % 10), 1));
            inputs.add(probeInput(data, readerThreads));
        }
        CollectingSink out = new CollectingSink(META);
        ExecutionReport report = new PushDagExecutor(3)
                .runSinkGraphReported(filterGraph(inputs, out));
        assertEquals(200, out.getRows().size(), "all 4x50 rows read (filter amt>=0 is always true)");
        assertEquals(TelemetryCoverage.COMPLETE, report.resourceTelemetryCoverage());
        assertEquals(0L, report.acquiredBytes(), "pass-through source sharding has a real zero");
        assertTrue(readerThreads.size() >= 2,
                "non-keyed multi-split input should be read by at least 2 threads in parallel (a single-threaded concat uses only 1), actual: " + readerThreads);
    }

    /** A non-replayable Source with several splits at P>1: each split is read exactly once, losing nothing and duplicating nothing, which is the correctness of a parallel read. */
    @Test
    void nonReplayableReadExactlyOnce() {
        List<DataRow> all = new ArrayList<>();
        for (int i = 0; i < 300; i++) all.add(row("k" + (i % 10), 1));
        List<List<DataRow>> buckets = new ArrayList<>();
        for (int i = 0; i < 5; i++) buckets.add(new ArrayList<>());
        for (int i = 0; i < all.size(); i++) buckets.get(i % 5).add(all.get(i));
        List<Source> oneShot = new ArrayList<>();
        for (List<DataRow> b : buckets) {
            final List<DataRow> data = b;
            final AtomicInteger consumed = new AtomicInteger(0);
            oneShot.add(new Source() {
                @Override public DataSchema getSchema() { return META; }
                @Override public Iterator<DataRow> iterator() {
                    if (consumed.getAndIncrement() > 0) return Collections.emptyIterator();
                    return data.iterator();
                }
            });
        }
        CollectingSink out = new CollectingSink(META);
        new PushDagExecutor(3).runSinkGraph(filterGraph(oneShot, out));
        assertEquals(300, out.getRows().size(), "five single-use cursors each read once -> 300 rows with no loss and no duplicates");
    }
}
