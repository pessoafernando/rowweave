package io.rowweave;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataSetTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("name", DataType.StringType),
            new Field("amt", DataType.IntegerType));

    private static DataRow row(Integer id, String name, Integer amt) {
        DataRow r = new DataRow(3);
        r.set(0, id);
        r.set(1, name);
        r.set(2, amt);
        return r;
    }

    private static List<DataRow> rows(DataRow... rs) {
        return new ArrayList<>(Arrays.asList(rs));
    }

    /** A Source that can be traversed repeatedly, bound to a session through BatchSession.fromSource. */
    private static Source input(List<DataRow> rs) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return rs.iterator(); }
        };
    }

    /** Executes through commit, the same IR path production takes with per-operator spill, and reads the SINK Sink. */
    private static List<DataRow> run(BatchSession s, DataSet ds) {
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        s.execute();
        return out.getRows();
    }

    @Test
    void sourceFilterSelectOutputEndToEnd() {
        BatchSession s = RowWeave.createBatchSession("t");
        DataSet ds = s.fromSource(input(rows(row(1, "a", 100), row(2, "b", 200), row(3, "c", 50))))
                .filter(r -> (Integer) r.get(2) >= 100)   // amt >= 100
                .select(0, 1);                             // keep id, name
        assertEquals(2, ds.getSchema().getFieldCount());
        List<DataRow> out = run(s, ds);
        assertEquals(2, out.size());
        assertEquals(1, out.get(0).get(0));
        assertEquals("a", out.get(0).get(1));
        assertEquals(2, out.get(1).get(0));
        assertEquals("b", out.get(1).get(1));
    }

    @Test
    void sourceMapFlatMap() {
        BatchSession s = RowWeave.createBatchSession("t");
        DataSet ds = s.fromSource(input(rows(row(1, "x", 2), row(2, "y", 3))))
                .map(r -> {
                    DataRow o = new DataRow(3);
                    o.set(0, (Integer) r.get(0) * 10);
                    o.set(1, r.get(1));
                    o.set(2, r.get(2));
                    return o;
                })
                .flatMap((r, c) -> {
                    int n = (Integer) r.get(2);
                    for (int i = 0; i < n; i++) {
                        c.collect(r);
                    }
                });
        List<DataRow> out = run(s, ds);
        assertEquals(5, out.size()); // 2 copies of id10 + 3 copies of id20
    }

    @Test
    void chainedOperatorsOrderPreserved() {
        BatchSession s = RowWeave.createBatchSession("t");
        DataSet ds = s.fromSource(input(rows(
                        row(1, "a", 10), row(2, "b", 20), row(3, "c", 30), row(4, "d", 40))))
                .filter(r -> (Integer) r.get(0) >= 2)    // id>=2 -> b,c,d
                .select(0, 2)                            // id, amt
                .filter(r -> (Integer) r.get(1) <= 30)   // amt<=30 -> b(20),c(30)
                .map(r -> {
                    DataRow o = new DataRow(2);
                    o.set(0, r.get(0));
                    o.set(1, (Integer) r.get(1) + 1);
                    return o;
                });
        List<DataRow> out = run(s, ds);
        assertEquals(2, out.size());
        assertEquals(2, out.get(0).get(0));
        assertEquals(21, out.get(0).get(1));
        assertEquals(3, out.get(1).get(0));
        assertEquals(31, out.get(1).get(1));
    }

    @Test
    void selectMetaNameAndTypePreserved() {
        // Meta only, without execution: a source with no session is enough, since getSchema needs none.
        DataSet ds = DataSet.source(META, rows()).select(2, 0); // amt, id
        DataSchema m = ds.getSchema();
        assertEquals(2, m.getFieldCount());
        assertEquals("amt", m.getField(0).getName());
        assertEquals(DataType.IntegerType, m.getField(0).getDataType());
        assertEquals("id", m.getField(1).getName());
    }

    @Test
    void emptySourceProducesNothing() {
        BatchSession s = RowWeave.createBatchSession("t");
        DataSet ds = s.fromSource(input(rows())).filter(r -> true).select(0);
        assertTrue(run(s, ds).isEmpty());
    }

    @Test
    void reExecuteSameDagIsRepeatable() {
        // The same dag run twice, each with its own session and commit, gives the same result, accumulating nothing and failing nowhere.
        BatchSession s1 = RowWeave.createBatchSession("r1");
        DataSet d1 = s1.fromSource(input(rows(row(1, "a", 100), row(2, "b", 50))))
                .filter(r -> (Integer) r.get(2) >= 100);
        assertEquals(1, run(s1, d1).size());
        BatchSession s2 = RowWeave.createBatchSession("r2");
        DataSet d2 = s2.fromSource(input(rows(row(1, "a", 100), row(2, "b", 50))))
                .filter(r -> (Integer) r.get(2) >= 100);
        assertEquals(1, run(s2, d2).size()); // second run must not accumulate / fail
    }

    @Test
    void sourceMetaExposed() {
        DataSet ds = DataSet.source(META, rows());
        assertEquals(3, ds.getSchema().getFieldCount());
        assertEquals("id", ds.getSchema().getField(0).getName());
    }
}
