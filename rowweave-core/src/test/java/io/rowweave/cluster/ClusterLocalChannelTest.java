package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterLocalChannelTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private Path dir;
    private MasterServer master;
    private final List<WorkerServer> workers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (WorkerServer w : workers) w.close();
        workers.clear();
        if (master != null) master.close();
        if (dir != null) try { deleteRec(dir.toFile()); } catch (RuntimeException ignored) { }
    }

    private void startCluster(int nWorkers, int parallelism, boolean reuse) throws Exception {
        dir = Files.createTempDirectory("clusterlocal");
        master = new MasterServer(5000).setObjectReuse(reuse);
        for (int i = 0; i < nWorkers; i++) {
            File d = new File(dir.toFile(), "w" + i); d.mkdirs();
            workers.add(WorkerServer.newUnguarded("w" + i, 8, "127.0.0.1", master.port(), d));
        }
    }

    private static void deleteRec(File f) {
        File[] ch = f.listFiles();
        if (ch != null) for (File c : ch) deleteRec(c);
        f.delete();
    }

    private static DataRow row(String k, int amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    private static Source input(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return rows.iterator(); }
        };
    }

    private static List<Source> shards(List<DataRow> rows, int n) {
        List<List<DataRow>> b = new ArrayList<>();
        for (int i = 0; i < n; i++) b.add(new ArrayList<>());
        for (int i = 0; i < rows.size(); i++) b.get(i % n).add(rows.get(i));
        List<Source> ins = new ArrayList<>();
        for (List<DataRow> x : b) ins.add(input(x));
        return ins;
    }

    private Map<String, Long> runGroupBy(List<DataRow> rows, int nWorkers, int parallelism, boolean reuse) throws Exception {
        startCluster(nWorkers, parallelism, reuse);
        Path outPath = dir.resolve("out.tsv");
        io.rowweave.FileSink out = new io.rowweave.FileSink(GB, outPath.toString());
        io.rowweave.ExecutionEnvironment s = new io.rowweave.ExecutionEnvironment("local", "local",
                new RealClusterJobClient("127.0.0.1", master.port()));
        s.setParallelism(parallelism);
        List<Source> ins = shards(rows, parallelism);
        s.fromSource(ins.toArray(new Source[0]))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                .sinkTo(out);
        s.execute();
        Map<String, Long> got = new HashMap<>();
        for (DataRow r : io.rowweave.FileSink.read(GB, outPath.toString())) {
            got.put((String) r.get(0), ((BigDecimal) r.get(1)).longValue());
        }
        return got;
    }

    private static List<DataRow> data(int n) {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < n; i++) rows.add(row("k" + (i % 13), i % 7));
        return rows;
    }

    private static Map<String, Long> oracle(List<DataRow> rows) {
        Map<String, Long> m = new HashMap<>();
        for (DataRow r : rows) m.merge((String) r.get(0), ((BigDecimal) r.get(1)).longValue(), Long::sum);
        return m;
    }

    @Test
    void singleWorkerAllLocalCorrect() throws Exception {
        List<DataRow> rows = data(300);
        Map<String, Long> got = runGroupBy(rows, 1, 3, true);
        assertEquals(oracle(rows), got, "single worker with an all-local shuffle, groupBySum must equal the reference");
    }

    @Test
    void reuseOffSameResult() throws Exception {
        List<DataRow> rows = data(300);
        Map<String, Long> reuseOn = runGroupBy(rows, 1, 3, true);
        tearDown();
        Map<String, Long> reuseOff = runGroupBy(rows, 1, 3, false);
        assertEquals(reuseOn, reuseOff, "results are equivalent with row reuse on and off");
        assertEquals(oracle(rows), reuseOff, "reuse=false also equals the reference");
    }

    /** Two workers with a mixed transport (partly local, partly socket) produce output equal to the oracle and equal to one worker: a guard that mixed fan-out does not corrupt results. */
    @Test
    void twoWorkerHybridSameAsOracle() throws Exception {
        List<DataRow> rows = data(300);
        Map<String, Long> hybrid = runGroupBy(rows, 2, 4, true);
        assertEquals(oracle(rows), hybrid, "two workers with a mixed local and socket shuffle, groupBySum must equal the reference");
    }
}

