package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClusterSourceShardingTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private Path dir;
    private MasterServer master;
    private WorkerServer w0, w1;

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("clustershard");
        master = new MasterServer(5000);
        File d0 = new File(dir.toFile(), "w0"); d0.mkdirs();
        File d1 = new File(dir.toFile(), "w1"); d1.mkdirs();
        w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), d0);
        w1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), d1);
    }

    @AfterAll
    void tearDown() {
        if (w0 != null) w0.close();
        if (w1 != null) w1.close();
        if (master != null) master.close();
        try { deleteRec(dir.toFile()); } catch (RuntimeException ignored) { }
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

    private static List<Source> shards(List<DataRow> rows, int nInputs) {
        List<List<DataRow>> buckets = new ArrayList<>();
        for (int i = 0; i < nInputs; i++) buckets.add(new ArrayList<>());
        for (int i = 0; i < rows.size(); i++) buckets.get(i % nInputs).add(rows.get(i));
        List<Source> ins = new ArrayList<>();
        for (List<DataRow> b : buckets) ins.add(input(b));
        return ins;
    }

    private PlanGraph groupBySumGraph(List<Source> inputs, io.rowweave.Sink out) {
        PlanNode src = PlanNode.sourceFromInputs(META, inputs);
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 1, out).build();
        return new PlanGraph(Arrays.asList(src, gb, sink));
    }

    @Test
    void multiInputSourceClusterReadsAllSplits() {
        List<DataRow> rows = new ArrayList<>();
        long expectedTotal = 0;
        for (int i = 0; i < 300; i++) {
            int amt = i % 7;
            rows.add(row("k" + (i % 13), amt));
            expectedTotal += amt;
        }
        Path outPath = dir.resolve("shard-out.tsv");
        io.rowweave.FileSink out = new io.rowweave.FileSink(GB, outPath.toString());
        RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", master.port());
        client.submitToSink(groupBySumGraph(shards(rows, 3), out));
        long gotTotal = 0;
        for (DataRow r : io.rowweave.FileSink.read(GB, outPath.toString())) {
            gotTotal += ((BigDecimal) r.get(1)).longValue();
        }
        assertEquals(expectedTotal, gotTotal,
                "a cluster multi-split source must sum to the full input under groupBySum; reading only inputs[0] would drop splits");
    }
}

