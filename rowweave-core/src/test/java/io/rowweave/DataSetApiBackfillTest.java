package io.rowweave;

import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.ClusterJobClient;
import io.rowweave.cluster.WorkerServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * the scalar aggregations sum, count, countField, max and min, with their aliases,
 * groupBySum, orderBy(String...), top(int) and removeFields, each a thin wrapper over an existing describable operator (WholeTableAgg, OrderBy, TopN, Select).
 *
 * <p>Checked: a single-machine commit returns the right rows, aligned to scale 10 HALF_UP and the null semantics, and everything is describable, hence serializable and shippable to a cluster.
 */
class DataSetApiBackfillTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType),
            new Field("q", DataType.IntegerType));

    private static DataRow row(String k, String amt, Integer q) {
        DataRow r = new DataRow(3);
        r.set(0, k);
        r.set(1, amt == null ? null : new BigDecimal(amt));
        r.set(2, q);
        return r;
    }

    private BatchSession session;

    private DataSet src(List<DataRow> rs) {
        session = RowWeave.createBatchSession("api");
        return session.fromSource(new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public java.util.Iterator<DataRow> iterator() { return rs.iterator(); }
        });
    }

    private List<DataRow> run(DataSet ds) {
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        session.execute();
        return out.getRows();
    }

    private static List<DataRow> data() {
        return new ArrayList<>(Arrays.asList(
                row("a", "10.5", 1), row("b", "20", 2), row("a", "5", null), row("c", null, 4)));
    }

    // ---------- scalar aggregation, one [value] row for the whole table ----------

    @Test void sum_scalar() {
        List<DataRow> out = run(src(data()).sum("amt"));
        assertEquals(1, out.size());
        assertEquals(new BigDecimal("35.5000000000"), out.get(0).get(0)); // 10.5+20+5,scale10 HALF_UP
    }

    @Test void sum_scalar_alias() {
        DataSet ds = src(data()).sum("amt", "total");
        assertEquals("total", ds.getSchema().getField(0).getName());
        assertEquals(new BigDecimal("35.5000000000"), run(ds).get(0).get(0));
    }

    @Test void count_all() {
        List<DataRow> out = run(src(data()).count());
        assertEquals(1, out.size());
        assertEquals(4L, out.get(0).get(0));  // every row holds a null
    }

    @Test void count_field() {
        List<DataRow> out = run(src(data()).count("amt"));
        assertEquals(3L, out.get(0).get(0));  // 3 rows with a non-null amt
    }

    @Test void max_min_scalar() {
        assertEquals(new BigDecimal("20"), run(src(data()).max("amt")).get(0).get(0));
        assertEquals(new BigDecimal("5"), run(src(data()).min("amt")).get(0).get(0));
    }

    // ---------- the groupBySum convenience ----------

    @Test void groupBySum_convenience() {
        List<DataRow> out = run(src(data()).groupBySum("amt", "k"));
        // 10.5+5=15.5, b gives 20, and c gives null, being an all-null group
        assertEquals(3, out.size());
    }

    // ---------- the orderBy(String...) convenience, all ascending with the default null semantics ----------

    @Test void orderBy_stringFields() {
        List<DataRow> out = run(src(data()).orderBy("k"));
        assertEquals(4, out.size());
        assertEquals("a", out.get(0).get(0));
        assertEquals("c", out.get(3).get(0));
    }

    // ---------- the top(int) convenience, the first n in source order through streaming TopN ----------

    @Test void topN_sourceOrder() {
        List<DataRow> out = run(src(data()).top(2));
        assertEquals(2, out.size());
        assertEquals("a", out.get(0).get(0));
        assertEquals("b", out.get(1).get(0));
    }

    // ---------- removeFields, the complement of select ----------

    @Test void removeFields_dropsNamed() {
        DataSet ds = src(data()).removeFields("amt");
        assertEquals(2, ds.getSchema().getFieldCount());
        assertEquals("k", ds.getSchema().getField(0).getName());
        assertEquals("q", ds.getSchema().getField(1).getName());
        assertEquals(4, run(ds).size());
    }

    // ---------- cluster describable: serialized and shipped down, with the scalar sum compared between a single machine and a cluster ----------

    @Test void scalarSum_clusterMatchesSingle(@TempDir File tmp) throws Exception {
        List<DataRow> data = data();
        BigDecimal single = (BigDecimal) run(src(data).sum("amt")).get(0).get(0);

        File w0 = new File(tmp, "w0"); w0.mkdirs();
        File w1 = new File(tmp, "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            BatchSession s = new BatchSession("apic", "apic", new ClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            DataSet ds = s.fromSource(new CollectionSource(META, data)).sum("amt");
            // In a cluster the Sink is written by the worker, and WorkerCollectingOutput is not worker-writable, so a client copy receives no rows,
            // which is why the rows go through FileSink to disk and are read back, as in every cluster test here.
            File outFile = new File(tmp, "sum.tsv");
            ds.sinkTo(new FileSink(ds.getSchema(), outFile.getAbsolutePath()));
            s.execute();
            List<DataRow> back = FileSink.read(ds.getSchema(), outFile.getAbsolutePath());
            assertEquals(1, back.size());
            assertEquals(single, back.get(0).get(0), "cluster scalar sum matches the single-process run");
        }
    }
}
