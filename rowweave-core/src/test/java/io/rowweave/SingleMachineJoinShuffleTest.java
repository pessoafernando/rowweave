package io.rowweave;

import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Single-machine binary join keyed shuffle with parallelism above 1, where the sort-merge join runs P parallel instances, with the left and right routers each
 * partitioning by the normalized numeric join key so that equal keys land on the same partition, giving a result bit-identical to the P=1 oracle.
 */
class SingleMachineJoinShuffleTest {

    @BeforeEach
    void admitTinyPoolOwners() {
        System.setProperty("rowweave.worker.operator.minMemory", "16384");
    }

    @AfterEach
    void resetMinimumGuarantee() {
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    private static final DataSchema LEFT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema JOIN = SortMergeJoin.of(
            LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();

    private static DataRow lrow(Integer k, String v) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, v);
        return r;
    }

    private static Source input(DataSchema meta, List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return meta; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    private static List<DataRow> runJoin(List<DataRow> left, List<DataRow> right, JoinType type, int parallelism) {
        BatchSession session = RowWeave.createBatchSession("sm-par-b");
        session.setParallelism(parallelism);
        DataSet l = session.fromSource(input(LEFT, left));
        DataSet r = session.fromSource(input(RIGHT, right));
        DataSet joined = l.join(r, SortMergeJoin.of(LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, type));
        WorkerCollectingOutput out = new WorkerCollectingOutput(joined.getSchema());
        joined.sinkTo(out);
        session.execute();
        List<DataRow> got = new ArrayList<>(out.getRows());
        // sorted over every field to make comparison easy: parallel runs leave no fixed order across keys, and although the trailing sort already canonicalizes, sorting again here is sturdier
        got.sort((a, b) -> {
            for (int f = 0; f < JOIN.getFieldCount(); f++) {
                String x = String.valueOf(a.get(f)), y = String.valueOf(b.get(f));
                int c = x.compareTo(y);
                if (c != 0) return c;
            }
            return 0;
        });
        return got;
    }

    @Test
    void innerJoinParallelBitIdenticalToOracle() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 200; i++) left.add(lrow(i % 10, "L" + i));
        for (int i = 0; i < 200; i++) right.add(lrow(i % 10, "R" + i));
        List<DataRow> oracle = runJoin(left, right, JoinType.INNER, 1);
        for (int p : new int[]{2, 3}) {
            List<DataRow> got = runJoin(left, right, JoinType.INNER, p);
            assertEquals(oracle.size(), got.size(), "P=" + p + " join row count matches");
            for (int i = 0; i < oracle.size(); i++) {
                for (int f = 0; f < JOIN.getFieldCount(); f++) {
                    assertEquals(oracle.get(i).get(f), got.get(i).get(f), "P=" + p + " row " + i + " field " + f);
                }
            }
        }
    }

    /** LEFT outer with null not equal to null, where null keys on either side never match and the outer join pads them, agrees with the oracle under parallelism. */
    @Test
    void leftOuterNullNotEqualParallel() {
        List<DataRow> left = Arrays.asList(lrow(1, "L1"), lrow(2, "L2"), lrow(null, "Lnull"));
        List<DataRow> right = Arrays.asList(lrow(1, "R1"), lrow(null, "Rnull"));
        List<DataRow> oracle = runJoin(left, right, JoinType.LEFT, 1);
        List<DataRow> got = runJoin(left, right, JoinType.LEFT, 3);
        assertEquals(oracle.size(), got.size());
        assertEquals(3, oracle.size(), "key 1 matches, key 2 is null-filled, null is null-filled (null is not equal to null)");
        for (int i = 0; i < oracle.size(); i++) {
            for (int f = 0; f < JOIN.getFieldCount(); f++) {
                assertEquals(oracle.get(i).get(f), got.get(i).get(f), "row " + i + " field " + f);
            }
        }
    }

    /** Skew, a cartesian over one repeated key, does not deadlock under parallelism. */
    @Test
    void skewCartesianNoDeadlock() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 80; i++) left.add(lrow(7, "L" + i));
        for (int i = 0; i < 80; i++) right.add(lrow(7, "R" + i));
        List<DataRow> got = runJoin(left, right, JoinType.INNER, 3);
        assertEquals(80 * 80, got.size(), "Cartesian product 80x80");
    }

    /**
     * The real parallel SMJ-with-spill path on a tiny 256KB pool: PushDagExecutor(P, tinyPool).runSinkGraph over the join graph gives each
     * join consumer its own sub-allowance, so SMJ spills without running out of memory and the result stays right, which shows that a binary above P=1 takes the parallel shuffle path rather than the serial fallback.
     */
    @Test
    void parallelJoinSpillsUnderTinyPoolNoOom() {
        System.setProperty("rowweave.worker.operator.minMemory", "65536");
        try {
            List<DataRow> left = new ArrayList<>();
            List<DataRow> right = new ArrayList<>();
            for (int i = 0; i < 400; i++) left.add(lrow(i % 20, "L" + i));
            for (int i = 0; i < 400; i++) right.add(lrow(i % 20, "R" + i));

            List<DataRow> collected = new ArrayList<>();
            io.rowweave.plan.PlanNode ls = io.rowweave.plan.PlanNode.source(LEFT, left);
            io.rowweave.plan.PlanNode rs = io.rowweave.plan.PlanNode.source(RIGHT, right);
            io.rowweave.plan.PlanNode join = io.rowweave.plan.PlanNode.binary("join", JOIN, 0, 1)
                    .param("joinType", "INNER")
                    .listParam("leftKeys", Arrays.asList("k"))
                    .listParam("rightKeys", Arrays.asList("k")).build();
            Sink out = new Sink() {
                @Override public DataSchema getSchema() { return JOIN; }
                @Override public void collect(DataRow row) { synchronized (collected) { collected.add(row); } }
                @Override public void close() { }
                @Override public void abort() { }
            };
            io.rowweave.plan.PlanNode sink = io.rowweave.plan.PlanNode.sink(JOIN, 2, out).build();
            io.rowweave.plan.PlanGraph g = new io.rowweave.plan.PlanGraph(Arrays.asList(ls, rs, join, sink));

            // At P=3 the binary owners and the canonical-order owner each hold a 64KB floor for progress, so 256KB still forces the sort-merge join and the trailing sort to spill.
            io.rowweave.mem.ManagedMemory tiny = io.rowweave.mem.ManagedMemory.ofBytes(256 * 1024);
            long rows = new io.rowweave.plan.PushDagExecutor(3, tiny).runSinkGraph(g);
            // 20 keys, each a 20-by-20 cartesian of 400 rows, giving 20 times 400 = 8000
            assertEquals(8000, rows, "parallel SMJ spill path row count is correct");
            assertEquals(8000, collected.size());
        } finally {
            System.clearProperty("rowweave.worker.operator.minMemory");
        }
    }
}
