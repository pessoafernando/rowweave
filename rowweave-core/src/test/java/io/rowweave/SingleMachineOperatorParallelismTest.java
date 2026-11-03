package io.rowweave;

import io.rowweave.ops.OrderBy;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * OrderBy and Top parallelized inside the operator above parallelism 1, with a synthetic global __seq column as the ascending tie-breaker
 * giving a total-order comparator and a result bit-identical regardless of P, including where the order key ties heavily, which checks that a Top tie-break is settled by the whole row.
 */
class SingleMachineOperatorParallelismTest {

    private static final DataSchema META = new DataSchema(
            new Field("g", DataType.StringType), new Field("amt", DataType.BigDecimalType));

    private static DataRow row(String g, String amt) {
        DataRow r = new DataRow(2);
        r.set(0, g);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    private static Source input(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    private static List<DataRow> runOrderBy(List<DataRow> rows, int parallelism) {
        ExecutionEnvironment session = RowWeave.createEnvironment("sm-par-c-ob");
        session.setParallelism(parallelism);
        // sorted on amt alone, with g left out of the sort key, so many rows tie and the __seq total order has to settle the result deterministically
        DataSet ds = session.fromSource(input(rows))
                .orderBy(OrderBy.of(META, new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                        parallelism, parallelism > 1);
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        session.execute();
        return new ArrayList<>(out.getRows());
    }

    private static List<DataRow> runTop(List<DataRow> rows, int n, int parallelism) {
        ExecutionEnvironment session = RowWeave.createEnvironment("sm-par-c-top");
        session.setParallelism(parallelism);
        DataSet ds = session.fromSource(input(rows))
                .top(Top.of(META, n, new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                        parallelism);
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        session.execute();
        return new ArrayList<>(out.getRows());
    }

    /** OrderBy on amt alone, which leaves g free to tie: at P=2, 3 or 4 the output is bit-identical to the P=1 oracle row for row, because __seq supplies the total order. */
    @Test
    void orderByParallelBitIdentical() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            rows.add(row("g" + (i % 7), Integer.toString(i % 5)));  // amt only spans 0..4, so many rows share a key
        }
        List<DataRow> oracle = runOrderBy(rows, 1);
        for (int p : new int[]{2, 3, 4}) {
            List<DataRow> got = runOrderBy(rows, p);
            assertEquals(oracle.size(), got.size(), "P=" + p);
            for (int i = 0; i < oracle.size(); i++) {
                assertEquals(oracle.get(i).get(0), got.get(i).get(0), "P=" + p + " row " + i + " g");
                assertEquals(0, ((BigDecimal) oracle.get(i).get(1)).compareTo((BigDecimal) got.get(i).get(1)),
                        "P=" + p + " row " + i + " amt");
            }
        }
    }

    /** Top-N with many ties on the order key: at P=2, 3 or 4 the rows chosen agree with the P=1 oracle, since the tie-break follows the __seq total order. */
    @Test
    void topParallelTieBreakDeterministic() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            rows.add(row("g" + i, Integer.toString(i % 3)));  // amt only 0, 1 or 2, so every Top-N boundary ties
        }
        List<DataRow> oracle = runTop(rows, 30, 1);
        for (int p : new int[]{2, 3, 4}) {
            List<DataRow> got = runTop(rows, 30, p);
            assertEquals(oracle.size(), got.size(), "P=" + p + " Top-N row count");
            for (int i = 0; i < oracle.size(); i++) {
                assertEquals(oracle.get(i).get(0), got.get(i).get(0), "P=" + p + " row " + i + " g (tie-break deterministic)");
                assertEquals(0, ((BigDecimal) oracle.get(i).get(1)).compareTo((BigDecimal) got.get(i).get(1)),
                        "P=" + p + " row " + i + " amt");
            }
        }
    }
}
