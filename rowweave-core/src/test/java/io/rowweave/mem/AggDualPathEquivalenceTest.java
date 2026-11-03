package io.rowweave.mem;

import io.rowweave.RowWeave;
import io.rowweave.WorkerCollectingOutput;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The first-tier in-memory hash path and the second-tier partitioned external merge path
 * for aggregation are multiset-equivalent.
 *
 * <p>The same data, carrying BigDecimal high precision, null keys that group on their own
 * and several sum, count and max aggregates, runs twice: (a) with a large budget, which
 * stays in the in-memory hash path and never spills, and (b) with a tiny 512K budget,
 * which forces partition spill plus an external merge through StreamingMergeIterator. Each
 * group must hold exactly the same aggregate values on both paths, so the scale-10 HALF_UP
 * accumulation, the null grouping and the column order are all independent of spilling.
 */
class AggDualPathEquivalenceTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType),
            new Field("qty", DataType.BigDecimalType));

    private static final int ROWS = 200_000;
    private static final int GROUPS = 8_000;  // high cardinality, so a small budget always triggers a partition spill

    static final class GenInput implements Source {
        public DataSchema getSchema() { return META; }
        public Iterator<DataRow> iterator() {
            return new Iterator<DataRow>() {
                int i = 0;
                public boolean hasNext() { return i < ROWS; }
                public DataRow next() {
                    DataRow r = new DataRow(3);
                    // one null key every 100 rows to group on its own; the rest group by i % GROUPS
                    boolean nullKey = (i % 100) == 0;
                    r.set(0, nullKey ? null : "k" + (i % GROUPS));
                    r.set(1, new BigDecimal(i % 777).movePointLeft(3));  // fractional values, to check high-precision accumulation
                    r.set(2, new BigDecimal(i % 50));
                    i++;
                    return r;
                }
            };
        }
    }

    /** Runs groupBy(sum, count, max) under the given pool budget and returns key to [sum, count, max], with a sentinel for the null key. */
    private Map<Object, DataRow> runWithBudget(long poolBytes) {
        BatchSession s = RowWeave.createBatchSession("agg-" + poolBytes);
        DataSet ds = s.fromSource(new GenInput())
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt").count().max("qty"));
        ds.sinkTo(new WorkerCollectingOutput(ds.getSchema()));
        PlanGraph g = s.toPlanGraph();
        ManagedMemory pool = ManagedMemory.ofBytes(poolBytes);
        Map<Object, DataRow> out = new HashMap<>();
        for (int i = 0; i < g.nodeCount(); i++) {
            if (g.node(i).kind() == PlanNode.Kind.SINK) {
                new PushDagExecutor(1, pool).runSinkGraph(g.subGraphRootedAt(i));
                WorkerCollectingOutput o = (WorkerCollectingOutput) g.node(i).output();
                for (DataRow r : o.getRows()) {
                    Object key = r.get(0) == null ? "<NULL>" : r.get(0);
                    out.put(key, r);
                }
            }
        }
        return out;
    }

    @Test
    void memoryHashPathEqualsPartitionedExternalMergePath() {
        Map<Object, DataRow> memPath = runWithBudget(64L * 1024 * 1024);  // large budget: the whole run stays in the in-memory hash path
        Map<Object, DataRow> extPath = runWithBudget(512L * 1024);  // 512K: forces the partitioned external merge

        assertEquals(memPath.keySet(), extPath.keySet(), "both aggregation paths group the same key set (the null group included)");
        for (Object k : memPath.keySet()) {
            DataRow a = memPath.get(k), b = extPath.get(k);
            // row layout is [k, sum(amt), count, max(qty)]
            assertEquals(0, ((BigDecimal) a.get(1)).compareTo((BigDecimal) b.get(1)), "sum[" + k + "] agrees at full precision");
            assertEquals(a.get(2), b.get(2), "count[" + k + "] agrees");
            assertEquals(0, ((BigDecimal) a.get(3)).compareTo((BigDecimal) b.get(3)), "max[" + k + "] agrees");
        }
    }
}
