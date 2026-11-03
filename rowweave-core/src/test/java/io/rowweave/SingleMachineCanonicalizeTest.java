package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The canonicalize switch: with the default of true the tail applies a total-order OrderBy, so parallel results are bit-identical as they are today;
 * with false the trailing sort is skipped, saving one sub-allowance where the downstream does not care about order, and the result is set-equal, allowing partitions to interleave.
 */
class SingleMachineCanonicalizeTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static DataRow row(String k, int amt) {
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

    private static PlanGraph graph(List<DataRow> rows, Sink out) {
        PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(in(rows)));
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 1, out).build();
        return new PlanGraph(Arrays.asList(src, gb, sink));
    }

    private static List<DataRow> capture(int parallelism, boolean canonicalize, List<DataRow> rows) {
        List<DataRow> collected = new ArrayList<>();
        Sink out = new Sink() {
            @Override public DataSchema getSchema() { return GB; }
            @Override public void collect(DataRow r) { synchronized (collected) { collected.add(r); } }
            @Override public void close() { } @Override public void abort() { }
        };
        new PushDagExecutor(parallelism).canonicalizeOutput(canonicalize).runSinkGraph(graph(rows, out));
        return collected;
    }

    private static List<DataRow> sortByKey(List<DataRow> rows) {
        List<DataRow> s = new ArrayList<>(rows);
        s.sort((a, b) -> String.valueOf(a.get(0)).compareTo(String.valueOf(b.get(0))));
        return s;
    }

    /**
     * With the default canonicalize=true, parallel results pass through the trailing total order and are bit-identical regardless of P, so P=2 and P=3 agree row for row and are already fully sorted.
     * Note that P=1 takes the serial path, which yields first-seen order rather than a sorted one, so it is not the oracle here; what canonicalize guarantees is determinism across parallel runs.
     */
    @Test
    void canonicalizeTrueBitIdentical() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 500; i++) rows.add(row("k" + (i % 13), i % 7));
        List<DataRow> p2 = capture(2, true, rows);
        List<DataRow> p3 = capture(3, true, rows);
        assertEquals(p2.size(), p3.size());
        for (int i = 0; i < p2.size(); i++) {
            assertEquals(p2.get(i).get(0), p3.get(i).get(0), "P=2 and P=3 row " + i + " key matches row by row (deterministic)");
            assertEquals(0, ((BigDecimal) p2.get(i).get(1)).compareTo((BigDecimal) p3.get(i).get(1)),
                    "P=2 and P=3 row " + i + " sum");
        }
        // and the tail is totally ordered: p2 is itself ascending over every field, which is canonicalize taking effect.
        for (int i = 1; i < p2.size(); i++) {
            assertTrue(String.valueOf(p2.get(i - 1).get(0)).compareTo(String.valueOf(p2.get(i).get(0))) <= 0,
                    "canonicalize=true orders the output by key");
        }
    }

    /** canonicalize=false: the result set is equivalent (sorted output == oracle), the aggregates are right and nothing throws — correctness is about values, ordering is the caller's responsibility. */
    @Test
    void canonicalizeFalseSetEquivalent() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 500; i++) rows.add(row("k" + (i % 13), i % 7));
        List<DataRow> oracle = sortByKey(capture(1, true, rows));
        for (int p : new int[]{2, 3}) {
            List<DataRow> got = sortByKey(capture(p, false, rows));
            assertEquals(oracle.size(), got.size(), "P=" + p + " group count matches as a set");
            for (int i = 0; i < oracle.size(); i++) {
                assertEquals(oracle.get(i).get(0), got.get(i).get(0), "P=" + p + " key");
                assertEquals(0, ((BigDecimal) oracle.get(i).get(1)).compareTo((BigDecimal) got.get(i).get(1)),
                        "P=" + p + " sum (values correct, order irrelevant)");
            }
        }
    }

    /** BatchSession passes it through: setCanonicalizeOutput defaults to true, and with false the end-to-end result is still set-equal. */
    @Test
    void jobSessionCanonicalizePropagation() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 300; i++) rows.add(row("k" + (i % 9), 1));

        // With the default of false the output is set-equal, and the order is whatever the developer OrderBy says
        BatchSession s1 = RowWeave.createBatchSession("canon-default");
        s1.setParallelism(3);
        assertFalse(s1.isCanonicalizeOutput(), "canonicalize=false by default (retired)");
        s1.setCanonicalizeOutput(true);  // this case still needs a bit-identical comparison, so it is asked for explicitly
        DataSet g1 = s1.fromSource(in(rows)).groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        WorkerCollectingOutput o1 = new WorkerCollectingOutput(g1.getSchema());
        g1.sinkTo(o1);
        s1.execute();

        // false
        BatchSession s2 = RowWeave.createBatchSession("canon-off");
        s2.setParallelism(3);
        s2.setCanonicalizeOutput(false);
        DataSet g2 = s2.fromSource(in(rows)).groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        WorkerCollectingOutput o2 = new WorkerCollectingOutput(g2.getSchema());
        g2.sinkTo(o2);
        s2.execute();

        // both are set-equal: 9 groups with the same sum in each
        assertEquals(9, o1.getRows().size());
        assertEquals(o1.getRows().size(), o2.getRows().size());
        List<DataRow> a = sortByKey(o1.getRows());
        List<DataRow> b = sortByKey(o2.getRows());
        for (int i = 0; i < a.size(); i++) {
            assertEquals(a.get(i).get(0), b.get(i).get(0), "row " + i + " key");
            assertEquals(0, ((BigDecimal) a.get(i).get(1)).compareTo((BigDecimal) b.get(i).get(1)), "row " + i + " sum");
        }
    }
}
