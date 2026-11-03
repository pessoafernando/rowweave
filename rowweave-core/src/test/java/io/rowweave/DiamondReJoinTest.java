package io.rowweave;

import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A diamond re-join: src into a tee, then two filtered branches into join(branch 1, branch 2).
 * Two branches after a tee meeting again at one binary. This checks whether the subgraph copy at hand, which turns the shared src into two
 * independent source stages through subGraphRootedAt, runs correctly for a replayable source. Two independent sources are not a single producer broadcasting, so there is no deadlock or double read.
 */
class DiamondReJoinTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema JOIN = SortMergeJoin.of(
            META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();

    private static Source input(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    private static List<DataRow> rows() {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < 500; i++) { DataRow r = new DataRow(2); r.set(0, i % 8); r.set(1, new BigDecimal(i % 50)); rs.add(r); }
        return rs;
    }

    /** A diamond where one src is reused by two filtered branches that then join. With a replayable source this is correct, each copy reading its own share. */
    @Test
    void diamondSrcTeeThenJoin() {
        List<DataRow> data = rows();
        ExecutionEnvironment s = RowWeave.createEnvironment("diamond");
        DataSet mid = s.fromSource(input(data)).filter("amt >= 5");
        DataSet b1 = mid.filter("k >= 2");  // branch 1
        DataSet b2 = mid.filter("k <= 6");  // branch 2
        DataSet joined = b1.join(b2, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        WorkerCollectingOutput out = new WorkerCollectingOutput(joined.getSchema());
        joined.sinkTo(out);
        s.execute();
        assertTrue(out.getRows().size() > 0, "diamond re-join emits output");

        // oracle: each branch run on its own, then a hand-worked join for the row count, over rows with k in [2,6] and amt>=5, cartesian on k between the branches.
        // A simplified check: the result row count is the sum over k of |branch 1 holding k| times |branch 2 holding k|, worked out from the two branches run on their own.
        List<DataRow> l = new ArrayList<>(), r = new ArrayList<>();
        for (DataRow x : data) {
            int k = (Integer) x.get(0); BigDecimal a = (BigDecimal) x.get(1);
            if (a.compareTo(new BigDecimal(5)) >= 0 && k >= 2) l.add(x);
            if (a.compareTo(new BigDecimal(5)) >= 0 && k <= 6) r.add(x);
        }
        long expect = 0;
        for (int k = 0; k < 8; k++) {
            final int kk = k;
            long lc = l.stream().filter(x -> (Integer) x.get(0) == kk).count();
            long rc = r.stream().filter(x -> (Integer) x.get(0) == kk).count();
            expect += lc * rc;
        }
        assertEquals(expect, out.getRows().size(), "diamond join rows = Cartesian product of the two branches on k (shared source copy is correct)");
    }
}
