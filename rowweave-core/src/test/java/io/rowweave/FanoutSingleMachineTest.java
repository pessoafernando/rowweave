package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A single-machine fan-out, tee with no re-join, where one intermediate DataSet is reused by several downstream operators,
 * not merely by several outputs. Graph construction has to accept the reuse, by dropping the error it used to raise and returning the index already built when a node is visited again, and execution has to let each sink commit
 * copy the shared subgraph through subGraphRootedAt and run it, which is correct for a replayable source. Checked: each downstream result is bit-identical to running that sub-chain on its own.
 */
class FanoutSingleMachineTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static Source input(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    private static List<DataRow> rows() {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            DataRow r = new DataRow(2); r.set(0, i % 10); r.set(1, new BigDecimal(i % 100)); rs.add(r);
        }
        return rs;
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

    /** An intermediate DataSet, filter amt>=10, reused by two downstream operators: one branch filters again into out1, the other groups into out2. */
    @Test
    void oneIntermediateFedToTwoDownstreams() {
        List<DataRow> data = rows();

        // a fan-out job in which mid is reused by two downstreams, a filter and a groupBy each leading to its own output.
        ExecutionEnvironment s = RowWeave.createEnvironment("fanout");
        DataSet mid = s.fromSource(input(data)).filter("amt >= 10");
        DataSet b1 = mid.filter("k >= 5");
        WorkerCollectingOutput o1 = new WorkerCollectingOutput(b1.getSchema());
        b1.sinkTo(o1);
        DataSet b2 = mid.groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        WorkerCollectingOutput o2 = new WorkerCollectingOutput(b2.getSchema());
        b2.sinkTo(o2);
        s.execute();

        // oracle: each branch run on its own, with no fan-out.
        ExecutionEnvironment s1 = RowWeave.createEnvironment("o1");
        DataSet e1 = s1.fromSource(input(data)).filter("amt >= 10").filter("k >= 5");
        WorkerCollectingOutput eo1 = new WorkerCollectingOutput(e1.getSchema());
        e1.sinkTo(eo1); s1.execute();

        ExecutionEnvironment s2 = RowWeave.createEnvironment("o2");
        DataSet e2 = s2.fromSource(input(data)).filter("amt >= 10")
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        WorkerCollectingOutput eo2 = new WorkerCollectingOutput(e2.getSchema());
        e2.sinkTo(eo2); s2.execute();

        assertEquals(fp(eo1.getRows()), fp(o1.getRows()), "fan-out branch 1 (filter) matches the independent run");
        assertEquals(fp(eo2.getRows()), fp(o2.getRows()), "fan-out branch 2 (groupBy) matches the independent run");
        assertTrue(o1.getRows().size() > 0 && o2.getRows().size() > 0, "both branches emit output");
    }

    /**
     * The shared subgraph is computed once rather than copied per sink. A counting Source records how often it is read: after a whole-graph tee is submitted,
     * the source is read once and broadcast from that one computation, instead of once per sink, which is what the older per-sink copy would do.
     */
    @Test
    void sharedSubgraphComputedOnce() {
        List<DataRow> data = rows();
        java.util.concurrent.atomic.AtomicInteger sourceReads = new java.util.concurrent.atomic.AtomicInteger();
        Source counting = new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                sourceReads.incrementAndGet();  // each iterator() is one full read
                return new ArrayList<>(data).iterator();
            }
        };
        ExecutionEnvironment s = RowWeave.createEnvironment("fanout-once");
        DataSet mid = s.fromSource(counting).filter("amt >= 10");  // shared: source and filter
        DataSet b1 = mid.filter("k >= 5");
        b1.sinkTo(new WorkerCollectingOutput(b1.getSchema()));
        DataSet b2 = mid.groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        b2.sinkTo(new WorkerCollectingOutput(b2.getSchema()));
        s.execute();
        assertEquals(1, sourceReads.get(),
                "shared source read exactly once (whole-graph tee broadcasts a single computation); per-sink copies would read " + 2 + " times");
    }
}
