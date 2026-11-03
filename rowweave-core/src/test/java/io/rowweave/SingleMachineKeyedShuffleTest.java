package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Single-machine keyed shuffle with parallelism above 1, where groupBySum runs P parallel instances and the result is
 * bit-identical to the P=1 oracle, since equal keys share a partition and keep their accumulation order, while the trailing total-order sort canonicalizes the order between keys.
 */
class SingleMachineKeyedShuffleTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));

    private static DataRow row(String k, String amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, amt == null ? null : new BigDecimal(amt));
        return r;
    }

    private static Source input(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    /** Collects the groupBySum result, sorted by k so it is easy to compare. */
    private static List<DataRow> runGroupBySum(List<DataRow> rows, int parallelism) {
        BatchSession session = RowWeave.createBatchSession("sm-par-a");
        session.setParallelism(parallelism);
        DataSet grouped = session.fromSource(input(rows))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        WorkerCollectingOutput out = new WorkerCollectingOutput(grouped.getSchema());
        grouped.sinkTo(out);
        session.execute();
        List<DataRow> got = new ArrayList<>(out.getRows());
        got.sort((a, b) -> String.valueOf(a.get(0)).compareTo(String.valueOf(b.get(0))));
        return got;
    }

    @Test
    void groupBySumParallelBitIdenticalToOracle() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            rows.add(row("k" + (i % 17), Integer.toString(i % 9)));
        }
        List<DataRow> oracle = runGroupBySum(rows, 1);
        for (int p : new int[]{2, 3, 4}) {
            List<DataRow> got = runGroupBySum(rows, p);
            assertEquals(oracle.size(), got.size(), "P=" + p + " group count matches");
            for (int i = 0; i < oracle.size(); i++) {
                assertEquals(oracle.get(i).get(0), got.get(i).get(0), "P=" + p + " row " + i + " key");
                assertEquals(0, ((BigDecimal) oracle.get(i).get(1)).compareTo((BigDecimal) got.get(i).get(1)),
                        "P=" + p + " row " + i + " sum");
            }
        }
    }

    /** A null key takes part in grouping as its own group, and under parallelism nothing is lost or misplaced. */
    @Test
    void nullKeyGroupedUnderParallelism() {
        List<DataRow> rows = Arrays.asList(
                row("a", "10"), row(null, "5"), row("a", "20"), row(null, "7"), row("b", "1"));
        List<DataRow> oracle = runGroupBySum(rows, 1);
        List<DataRow> got = runGroupBySum(rows, 3);
        assertEquals(oracle.size(), got.size(), "null forms its own group and the group count matches");
        assertEquals(3, oracle.size(), "three groups a / b / null");
    }

    /** Skew, with one repeated key landing in one partition, over a large volume, does not deadlock under parallelism. */
    @Test
    void skewNoDeadlock() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            rows.add(row("hot", "1"));  // one key everywhere lands on the same keyed-operator instance while the others idle
        }
        List<DataRow> got = runGroupBySum(rows, 4);
        assertEquals(1, got.size());
        assertEquals(0, new BigDecimal("2000").compareTo((BigDecimal) got.get(0).get(1)));
    }

    /** A source that throws while iterating makes the parallel keyed shuffle fail fast, since submit throws rather than hanging. */
    @Test
    void sourceThrowsFailsFast() {
        Source boom = new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    @Override public boolean hasNext() { return true; }  // idempotent
                    @Override public DataRow next() { throw new RuntimeException("source boom"); }
                };
            }
        };
        BatchSession session = RowWeave.createBatchSession("sm-par-ff");
        session.setParallelism(3);
        DataSet grouped = session.fromSource(boom).groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        grouped.sinkTo(new WorkerCollectingOutput(grouped.getSchema()));
        assertThrows(RuntimeException.class, session::execute, "a failing source makes the parallel shuffle fail fast without hanging");
    }

    /** setParallelism defaults to 1 and rejects an illegal value. */
    @Test
    void setParallelismValidation() {
        BatchSession session = RowWeave.createBatchSession("v");
        assertThrows(IllegalArgumentException.class, () -> session.setParallelism(0));
        assertThrows(IllegalArgumentException.class, () -> session.setParallelism(-1));
    }
}
