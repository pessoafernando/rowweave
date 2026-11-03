package io.rowweave;

import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import io.rowweave.cluster.LocalCluster;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end comparison over complex dags: PushDagExecutor on a single machine against LocalCluster determinism, plus a hand-worked oracle.
 * The expected values are worked out from the operator language and the sensitive-point rules.
 */
class EndToEndEquivalenceTest {

    private static final DataSchema META = new DataSchema(
            new Field("grp", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(String grp, BigDecimal amt) {
        DataRow r = new DataRow(2);
        r.set(0, grp);
        r.set(1, amt);
        return r;
    }

    /** filter into groupBySum into orderBy end to end on a single machine, with the sum at scale 10 and the order right, worked out against the oracle. */
    @Test
    void filterGroupByOrderBySingleMachine() {
        List<DataRow> rows = Arrays.asList(
                row("a", new BigDecimal("10")),
                row("b", new BigDecimal("5")),
                row("a", new BigDecimal("20")),
                row("c", new BigDecimal("1")),  // dropped by filter amt>2
                row("b", new BigDecimal("7")));
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        // DataSet: source, filter(amt>2), groupBySum(grp) then orderBy(sum desc), run through commit on the IR path
        BatchSession session = RowWeave.createBatchSession("e2e");
        final List<DataRow> srcRows = rows;
        DataSet ds = session.fromSource(new Source() {
                    @Override public DataSchema getSchema() { return META; }
                    @Override public java.util.Iterator<DataRow> iterator() { return srcRows.iterator(); }
                })
                .filter("amt > 2")
                .groupBy(ParallelGroupByAgg.of(META, "grp").sum("amt"))
                .orderBy(OrderBy.of(gbMeta, new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)),
                        1, false);
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        session.execute();
        // oracle: a=30, b=12, and c is dropped by the filter, so descending gives a(30) then b(12)
        List<DataRow> r = out.getRows();
        assertEquals(2, r.size());
        assertEquals("a", r.get(0).get(0));
        assertEquals(0, new BigDecimal("30.0000000000").compareTo((BigDecimal) r.get(0).get(1)));
        assertEquals("b", r.get(1).get(0));
        assertEquals(0, new BigDecimal("12.0000000000").compareTo((BigDecimal) r.get(1).get(1)));
    }

    /** groupBySum run by PushDagExecutor on a single machine against the cluster submitKeyed is deterministic, with the result sets equal for par={1,2,3}. */
    @Test
    void groupBySingleVsClusterDeterminism() {
        List<DataRow> rows = new ArrayList<>();
        String[] grps = {"a", "b", "c", "d", "e"};
        for (int i = 0; i < 200; i++) {
            rows.add(row(grps[i % grps.length], new BigDecimal(Integer.toString(i % 7))));
        }
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();

        // single-machine oracle: PushDagExecutor executes without a SINK
        PlanGraph single = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("groupBySum", gbMeta, 0)
                        .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build()));
        java.util.Map<String, BigDecimal> oracle = new java.util.TreeMap<>();
        new PushDagExecutor().execute(single, rx -> oracle.put((String) rx.get(0), (BigDecimal) rx.get(1)));

        // cluster at par={1,2,3}: submitKeyed, with the result set compared to the oracle
        for (int par : new int[]{1, 2, 3}) {
            WorkerCollectingOutput out = new WorkerCollectingOutput(gbMeta);
            PlanGraph cluster = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, rows),
                    PlanNode.unary("groupBySum", gbMeta, 0)
                            .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build(),
                    PlanNode.sink(gbMeta, 1, out).build()));
            new LocalCluster(par).submitKeyed(cluster);
            java.util.Map<String, BigDecimal> got = new java.util.TreeMap<>();
            for (DataRow rx : out.getRows()) {
                got.put((String) rx.get(0), (BigDecimal) rx.get(1));
            }
            assertEquals(oracle.keySet(), got.keySet(), "key set par=" + par);
            for (String k : oracle.keySet()) {
                assertEquals(0, oracle.get(k).compareTo(got.get(k)), "sum[" + k + "] par=" + par);
            }
        }
    }
}
