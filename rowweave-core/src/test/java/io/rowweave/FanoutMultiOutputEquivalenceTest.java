package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A full comparison across several outputs, plus a performance measurement.
 * One fan-out dag, where a shared source flows through filter into 5 distinct downstream outputs, submitted as a whole graph:
 * every output result is set-equal to the oracle run on its own, which is the correctness check;
 * the shared source is read once, showing the shared subgraph is computed once instead of copied per sink;
 * performance: the whole-graph tee against the older per-sink copy, measured in wall clock to quantify the gain, as a measurement rather than an assertion.
 */
class FanoutMultiOutputEquivalenceTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static List<DataRow> data(int n) {
        List<DataRow> rs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            DataRow r = new DataRow(2); r.set(0, i % 20); r.set(1, new BigDecimal(i % 100)); rs.add(r);
        }
        return rs;
    }

    /** A counting Source: each iterator() call is one full read, and the calls are counted. */
    private static final class CountingInput implements Source {
        final List<DataRow> rows; final AtomicInteger reads = new AtomicInteger();
        CountingInput(List<DataRow> rows) { this.rows = rows; }
        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() { reads.incrementAndGet(); return new ArrayList<>(rows).iterator(); }
    }

    private static List<String> fp(List<DataRow> rows) {
        List<String> s = new ArrayList<>();
        for (DataRow r : rows) {
            StringBuilder sb = new StringBuilder();
            for (int f = 0; f < r.size(); f++) {
                Object v = r.get(f);
                sb.append(v instanceof BigDecimal ? ((BigDecimal) v).stripTrailingZeros().toPlainString() : v).append('|');
            }
            s.add(sb.toString());
        }
        java.util.Collections.sort(s);
        return s;
    }

    /** Five downstream branches off a shared mid, filter amt>=10, each a different operator leading to its own output. */
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    @Test
    void fiveOutputFanoutCorrectAndComputedOnce() {
        List<DataRow> rows = data(5000);
        CountingInput src = new CountingInput(rows);

        // a fan-out job in which mid, filter amt>=10, feeds 5 downstream outputs.
        BatchSession s = RowWeave.createBatchSession("fanout5");
        DataSet mid = s.fromSource(src).filter("amt >= 10");
        WorkerCollectingOutput o0 = new WorkerCollectingOutput(mid.getSchema()); mid.filter("k >= 5").sinkTo(o0);
        WorkerCollectingOutput o1 = new WorkerCollectingOutput(mid.getSchema()); mid.filter("k < 5").sinkTo(o1);
        WorkerCollectingOutput o2 = new WorkerCollectingOutput(GB); mid.groupBy(ParallelGroupByAgg.of(META, "k").sum("amt")).sinkTo(o2);
        WorkerCollectingOutput o3 = new WorkerCollectingOutput(mid.getSchema()); mid.filter("amt >= 50").sinkTo(o3);
        WorkerCollectingOutput o4 = new WorkerCollectingOutput(GB); mid.groupBy(ParallelGroupByAgg.of(META, "k").sum("amt")).sinkTo(o4);
        s.execute();

        // the shared source is read once and broadcast from that one computation, where the older per-sink copy would read 5 times.
        assertEquals(1, src.reads.get(), "shared source read exactly once (whole-graph tee); per-sink copies would read 5 times");

        // each output is set-equal to the oracle run on its own.
        assertEquals(fp(oracleFilter(rows, "k >= 5")), fp(o0.getRows()), "o0 filter k>=5");
        assertEquals(fp(oracleFilter(rows, "k < 5")), fp(o1.getRows()), "o1 filter k<5");
        assertEquals(fp(oracleGroupBy(rows)), fp(o2.getRows()), "o2 groupBy");
        assertEquals(fp(oracleFilter(rows, "amt >= 50")), fp(o3.getRows()), "o3 filter amt>=50");
        assertEquals(fp(oracleGroupBy(rows)), fp(o4.getRows()), "o4 groupBy");
        assertTrue(o0.getRows().size() > 0 && o2.getRows().size() > 0, "branches emit output");
    }

    /**
     * The performance comparison: a whole-graph tee that computes one expensive shared subgraph once, against a per-sink path that runs the same shared subgraph once per output. Measured, not asserted.
     * The point is that the saving only shows up when the shared subgraph is expensive, with the source read N times and the breaker computed N times, so the shared path here is
     * source through filter into groupBy, a breaker, with 5 downstreams each post-processing the result. A whole-graph tee computes that groupBy once, where a per-sink copy computes it 5 times.
     * When the shared subgraph is cheap, the extra threads and copying make the tee slightly worse, and the measurement reports that as it is.
     */
    @Test
    void perfWholeGraphTeeVsPerSink() {
        List<DataRow> rows = data(200_000);
        runWholeGraph(rows);  // warmed up
        runPerSinkSimulated(rows);

        long t0 = System.nanoTime();
        long teeReads = runWholeGraph(rows);
        long teeMs = (System.nanoTime() - t0) / 1_000_000;

        t0 = System.nanoTime();
        long perSinkReads = runPerSinkSimulated(rows);
        long perSinkMs = (System.nanoTime() - t0) / 1_000_000;

        System.out.printf("[FANOUT-PERF] 5-output fan-out over a costly shared subgraph (source->filter->groupBy, 200k rows):%n"
                + "  whole-graph tee  = %4dms (shared source read %d time, groupBy computed once)%n"
                + "  per-sink copies  = %4dms (shared source read %d times, groupBy computed 5 times)%n"
                + "  -> speedup %.2fx%n",
                teeMs, teeReads, perSinkMs, perSinkReads, perSinkMs / (double) Math.max(1, teeMs));
        assertTrue(teeMs >= 0 && perSinkMs >= 0, "wall-clock measurement completed");
    }

    /** A whole-graph tee, where a shared source flows through filter into groupBy, an expensive breaker computed once, and broadcasts to 5 branches that post-process it; returns how many times the source was read, which is 1. */
    private long runWholeGraph(List<DataRow> rows) {
        CountingInput src = new CountingInput(rows);
        BatchSession s = RowWeave.createBatchSession("perf-tee");
        DataSet mid = s.fromSource(src).filter("amt >= 10")
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));  // the shared expensive breaker
        for (int i = 0; i < 5; i++) {
            mid.filter("sum0 >= " + (i * 10)).sinkTo(new WorkerCollectingOutput(mid.getSchema()));
        }
        s.execute();
        return src.reads.get();
    }

    /** A stand-in for the older per-sink approach, where each output opens its own session and runs the shared source, filter and groupBy again, so the source is read 5 times and groupBy computed 5 times. */
    private long runPerSinkSimulated(List<DataRow> rows) {
        CountingInput src = new CountingInput(rows);
        for (int i = 0; i < 5; i++) {
            BatchSession s = RowWeave.createBatchSession("perf-persink-" + i);
            s.fromSource(src).filter("amt >= 10")
                    .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                    .filter("sum0 >= " + (i * 10)).sinkTo(new WorkerCollectingOutput(GB));
            s.execute();
        }
        return src.reads.get();
    }

    private static List<DataRow> oracleFilter(List<DataRow> rows, String expr) {
        BatchSession s = RowWeave.createBatchSession("oracle-f");
        WorkerCollectingOutput o = new WorkerCollectingOutput(META);
        s.fromSource(fixed(rows)).filter("amt >= 10").filter(expr).sinkTo(o);
        s.execute();
        return o.getRows();
    }

    private static List<DataRow> oracleGroupBy(List<DataRow> rows) {
        BatchSession s = RowWeave.createBatchSession("oracle-gb");
        WorkerCollectingOutput o = new WorkerCollectingOutput(GB);
        s.fromSource(fixed(rows)).filter("amt >= 10").groupBy(ParallelGroupByAgg.of(META, "k").sum("amt")).sinkTo(o);
        s.execute();
        return o.getRows();
    }

    private static Source fixed(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }
}
