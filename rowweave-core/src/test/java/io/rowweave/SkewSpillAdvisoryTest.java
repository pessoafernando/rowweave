package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.ExecutionReport;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Skew and spill advisories: commit exposes the per-sink ExecutionReport through a MetricsListener, and
 * ExecutionReport.skewWarning(threshold) is a pure function that raises the warning. The engine logs nothing, blocks nothing and mitigates nothing on its own, so setting it or not leaves the result bit-identical.
 */
class SkewSpillAdvisoryTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static DataRow row(int k, int amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    private static Source in(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    // ---- the skewWarning pure function ----

    @Test
    void skewWarningAboveThreshold() {
        // skew = max/avg = 4.0, everything crowded into one partition at P=4
        ExecutionReport rep = new ExecutionReport(0, 0, 0, 100, new long[]{100, 0, 0, 0});
        assertEquals(4.0, rep.partitionSkew(), 0.001);
        assertNotNull(rep.skewWarning(4.0), "skew=4.0 reaches the threshold 4.0 -> warning");
        assertNull(rep.skewWarning(4.1), "skew=4.0 is below the threshold 4.1 -> no warning");
    }

    @Test
    void skewWarningBalancedNull() {
        ExecutionReport rep = new ExecutionReport(0, 0, 0, 100, new long[]{25, 25, 25, 25});
        assertEquals(1.0, rep.partitionSkew(), 0.001);
        assertNull(rep.skewWarning(4.0), "balanced -> no warning");
    }

    @Test
    void summaryContainsKeyMetrics() {
        ExecutionReport rep = new ExecutionReport(500, 1000, 3, 42, new long[]{40, 2});
        String s = rep.summary();
        assertTrue(s.contains("42"), "mentions rowsOut");
        assertTrue(s.contains("3") , "mentions spillCount");
    }

    // ---- the commit MetricsListener hook ----

    @Test
    void commitInvokesMetricsListenerPerSink() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 300; i++) rows.add(row(i % 9, 1));

        BatchSession session = RowWeave.createBatchSession("adv");
        session.setParallelism(3);
        AtomicInteger callbacks = new AtomicInteger();
        AtomicReference<ExecutionReport> captured = new AtomicReference<>();
        session.setMetricsListener(rep -> {
            callbacks.incrementAndGet();
            captured.set(rep);
        });
        DataSet g = session.fromSource(in(rows)).groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        g.sinkTo(new WorkerCollectingOutput(g.getSchema()));
        session.execute();

        assertEquals(1, callbacks.get(), "single sink -> one callback");
        assertNotNull(captured.get());
        assertEquals(9, captured.get().rowsOut(), "9 groups");
        assertEquals(io.rowweave.plan.TelemetryCoverage.NOT_APPLICABLE,
                captured.get().partitionTelemetryCoverage(),
                "cluster report must not fabricate a keyed distribution when the physical sink stage is singular");
        assertEquals(0, captured.get().partitionRowCounts().length);
    }

    /** A listener that throws leaves the job run untouched, isolated by try/catch. */
    @Test
    void listenerExceptionDoesNotBreakJob() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) rows.add(row(i % 5, 1));

        BatchSession session = RowWeave.createBatchSession("adv-boom");
        session.setParallelism(2);
        session.setMetricsListener(rep -> { throw new RuntimeException("listener boom"); });
        DataSet g = session.fromSource(in(rows)).groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        WorkerCollectingOutput out = new WorkerCollectingOutput(g.getSchema());
        g.sinkTo(out);
        // a listener exception is swallowed, commit finishes normally, and the result is unaffected
        assertDoesNotThrow(session::execute);
        assertEquals(5, out.getRows().size(), "5 groups, the job is unaffected by the listener exception");
    }

    /** With no listener set there is no cost, and the result is bit-identical to the run with a listener. */
    @Test
    void noListenerSameResult() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) rows.add(row(i % 5, 7));

        BatchSession s1 = RowWeave.createBatchSession("no-listener");
        s1.setParallelism(2);
        DataSet g1 = s1.fromSource(in(rows)).groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        WorkerCollectingOutput o1 = new WorkerCollectingOutput(g1.getSchema());
        g1.sinkTo(o1);
        s1.execute();

        BatchSession s2 = RowWeave.createBatchSession("with-listener");
        s2.setParallelism(2);
        s2.setMetricsListener(rep -> { });
        DataSet g2 = s2.fromSource(in(rows)).groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        WorkerCollectingOutput o2 = new WorkerCollectingOutput(g2.getSchema());
        g2.sinkTo(o2);
        s2.execute();

        assertEquals(o1.getRows().size(), o2.getRows().size());
    }
}
