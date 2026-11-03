package io.rowweave.monitor.http;

import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.RealClusterJobClient;
import io.rowweave.cluster.WorkerServer;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * A runner that really runs (not a unit test): start a real cluster with a master and two
 * workers plus the monitor HTTP server on port 18080, then commit a slowed full-operator DAG in
 * a loop. The DAG spans several stages (filter, select, map, addFields, removeFields,
 * partitionByHash, groupBy.sum, orderBy, top plus a join branch), so the browser shows a genuine
 * multi-stage run with per-stage row counts. Everything the monitor displays comes from real
 * execution rather than demo fixtures. Run it from main and point a browser at 18080.
 */
public final class MonitorLiveJobRunner {

    // The wide ORDER table: id/dept/region/grade/amount/qty, enough columns to build multi-operator DAGs.
    static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("grade", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType),
            new Field("qty", DataType.IntegerType));
    static final DataSchema DIM = new DataSchema(
            new Field("dept", DataType.IntegerType), new Field("bonus", DataType.IntegerType));

    /** A slow map that sleeps per row, which stretches the stage so the RUNNING intermediate state is visible. */
    static final MapFunction SLOW = in -> {
        try { Thread.sleep(1); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        return in;
    };

    private static List<DataRow> orders(int n) {
        List<DataRow> rs = new ArrayList<>();
        for (long id = 1; id <= n; id++) {
            DataRow r = new DataRow(6);
            r.set(0, id); r.set(1, (int)(id % 50)); r.set(2, (int)(id % 10));
            r.set(3, (int)(id % 5)); r.set(4, new BigDecimal(id % 1000)); r.set(5, (int)(id % 20));
            rs.add(r);
        }
        return rs;
    }

    private static List<DataRow> dim() {
        List<DataRow> rs = new ArrayList<>();
        for (int d = 0; d < 50; d++) { DataRow r = new DataRow(2); r.set(0, d); r.set(1, d * 10); rs.add(r); }
        return rs;
    }

    private static SortField desc(int i) {
        return new SortField(i, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST);
    }

    public static void main(String[] args) throws Exception {
        File dir = Files.createTempDirectory("mon-live").toFile();
        File w0 = new File(dir, "w0"); w0.mkdirs();
        File w1 = new File(dir, "w1"); w1.mkdirs();
        InMemoryJobMonitor mon = new InMemoryJobMonitor(100);

        MasterServer m = new MasterServer(10000, 0, new InProcessRegistry(), mon);
        WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
        WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1);

        MonitorHttpServer http = new MonitorHttpServer(mon, 18080, null);
        int hport = http.start();
        System.out.println("LIVE_MONITOR_PORT=" + hport);

        Thread submitter = new Thread(() -> {
            int n = 0;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ExecutionEnvironment s = new ExecutionEnvironment("live" + n, "full_op_dag_" + (n++),
                            new RealClusterJobClient("127.0.0.1", m.port()));
                    s.setParallelism(2);
                    File f = new File(dir, "out" + n + ".tsv");
                    // A full-operator DAG: filter, select, slow map, join with dim, select, addFields, removeFields,
                    // partitionByHash, groupBy.sum, orderBy, top. Several stages over source, join, keyed and sink, so the run is genuinely multi-stage.
                    DataSet joined = s.fromSource(new CollectionSource(ORDER, orders(1200)))
                            .filter("amount >= 0")
                            .select("id", "dept", "region", "grade", "amount", "qty")
                            .map(SLOW)
                            .join(s.fromSource(new CollectionSource(DIM, dim()))).on("dept")
                            .select("id", "dept", "region", "grade", "amount", "qty")  // Disambiguate back to ORDER.
                            .addFields(new Field[]{new Field("tag", DataType.IntegerType)}, new Object[]{1})
                            .removeFields("tag", "qty")
                            .partitionByHash("region");
                    DataSchema jm = joined.getSchema();
                    DataSchema g = ParallelGroupByAgg.of(jm, "region").sum("amount").getOutputSchema();
                    joined.groupBy(ParallelGroupByAgg.of(jm, "region").sum("amount"))
                            .orderBy(OrderBy.of(g, desc(1)), 1, false)
                            .top(Top.of(g, 5, desc(1)), 1)
                            .sinkTo(new FileSink(g, f.toString()));
                    s.execute();
                    Thread.sleep(1500);
                } catch (Throwable t) {
                    if (t instanceof InterruptedException) { Thread.currentThread().interrupt(); return; }
                }
            }
        });
        submitter.setDaemon(true);
        submitter.start();

        System.out.println("LIVE_READY");
        Thread.currentThread().join();
    }

    private MonitorLiveJobRunner() { }
}
