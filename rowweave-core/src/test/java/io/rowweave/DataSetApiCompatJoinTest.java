package io.rowweave;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JoinDataSet.select and .withFunc with their getters, plus ExecutionEnvironment.fromSource(Source...).
 * Syntactic sugar at graph-build time over several Sources. A single-machine commit returns the right rows.
 */
class DataSetApiCompatJoinTest {

    private static final DataSchema ORDER = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema DIM = new DataSchema(
            new Field("k", DataType.StringType), new Field("label", DataType.IntegerType));

    private static DataRow o(String k, String amt) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, new BigDecimal(amt)); return r;
    }

    private static DataRow d(String k, Integer label) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, label); return r;
    }

    private static List<DataRow> orders() {
        return new ArrayList<>(Arrays.asList(o("a", "10"), o("b", "20"), o("a", "5"), o("c", "7")));
    }

    private static List<DataRow> dims() {
        return new ArrayList<>(Arrays.asList(d("a", 100), d("b", 200)));
    }

    private static List<DataRow> run(ExecutionEnvironment s, DataSet ds) {
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        s.execute();
        return out.getRows();
    }

    // JoinDataSet.withFunc(JoinFunction): a custom join output row
    @Test void joinWithFunc() {
        ExecutionEnvironment s = RowWeave.createEnvironment("jf");
        DataSet ords = s.fromSource(new CollectionSource(ORDER, orders()));
        DataSet dim = s.fromSource(new CollectionSource(DIM, dims()));
        DataSchema outMeta = new DataSchema(new Field("k", DataType.StringType), new Field("amtxlabel", DataType.BigDecimalType));
        JoinFunction fn = new JoinFunction() {
            @Override public DataSchema getResultSchema() { return outMeta; }
            @Override public DataRow join(DataRow left, DataRow right) {
                DataRow r = new DataRow(2);
                r.set(0, left.get(0));
                BigDecimal amt = (BigDecimal) left.get(1);
                int label = (Integer) right.get(1);
                r.set(1, amt.multiply(new BigDecimal(label)));
                return r;
            }
        };
        DataSet joined = ords.join(dim).withFunc("k", fn);
        List<DataRow> res = run(s, joined);
        assertEquals(2, joined.getSchema().getFieldCount());
        assertEquals("amtxlabel", joined.getSchema().getField(1).getName());
        assertEquals(3, res.size()); // a×2 + b×1
    }

    // JoinDataSet.select(String...): choosing columns of the join result
    @Test void joinSelect() {
        ExecutionEnvironment s = RowWeave.createEnvironment("js");
        DataSet ords = s.fromSource(new CollectionSource(ORDER, orders()));
        DataSet dim = s.fromSource(new CollectionSource(DIM, dims()));
        // the join output is [k, amt, k, label], the whole left then the whole right, and select keeps amt and label.
        DataSet joined = ords.join(dim).on("k").select("amt", "label");
        List<DataRow> res = run(s, joined);
        assertEquals(2, joined.getSchema().getFieldCount());
        assertEquals(3, res.size());
    }

    // fromSource(Source...) reads several Sources in parallel, concatenating rows of one shape
    @Test void fromMultipleInputs() {
        ExecutionEnvironment s = RowWeave.createEnvironment("mi");
        DataSet ds = s.fromSource(
                new CollectionSource(ORDER, orders()),
                new CollectionSource(ORDER, orders()));
        List<DataRow> res = run(s, ds);
        assertEquals(8, res.size()); // 4 + 4
    }
}
