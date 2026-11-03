package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class GeneralDagClusterTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static DataRow row(String k, int amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    private static PlanGraph chain(List<DataRow> rows, io.rowweave.Sink out, int parallelism) {
        PlanNode src = PlanNode.source(META, rows);
        PlanNode flt = PlanNode.unary("filter", META, 0).param("expr", "amt >= 5").build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 1)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 2).listParam("sort", Arrays.asList(
                PlanOps.encodeSortField(new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST))
        )).build();
        PlanNode sink = PlanNode.sink(GB, 3, out).build();
        return new PlanGraph(Arrays.asList(src, flt, gb, ob, sink), parallelism);
    }

    @Test
    void multiHopChainP1ClusterVsSingle(@TempDir Path tmp) throws Exception {
        runMultiHopChain(tmp, 1);
    }

    @Test
    void multiHopChainP2ClusterVsSingle(@TempDir Path tmp) throws Exception {
        runMultiHopChain(tmp, 2);
    }

    @Test
    void multiHopChainP3ClusterVsSingle(@TempDir Path tmp) throws Exception {
        runMultiHopChain(tmp, 3);
    }

    private void runMultiHopChain(Path tmp, int parallelism) throws Exception {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            rows.add(row("k" + (i % 12), i % 9));
        }

        List<DataRow> single = new ArrayList<>();
        PlanNode src = PlanNode.source(META, rows);
        PlanNode flt = PlanNode.unary("filter", META, 0).param("expr", "amt >= 5").build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 1)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 2).listParam("sort", Arrays.asList(
                PlanOps.encodeSortField(new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST))
        )).build();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(src, flt, gb, ob)), single::add);
        assertTrue(single.size() > 0, "the reference is non-empty");

        File outFile = tmp.resolve("dag.tsv").toFile();
        FileSink output = new FileSink(GB, outFile.getAbsolutePath());
        PlanGraph graph = chain(rows, output, parallelism);

        File w0 = tmp.resolve("w0").toFile(); w0.mkdirs();
        File w1 = tmp.resolve("w1").toFile(); w1.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer ws0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer ws1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            long rowCount = new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
            List<DataRow> cluster = FileSink.read(GB, outFile.getAbsolutePath());
            assertEquals(single.size(), cluster.size(), "P=" + parallelism + " cluster group count matches the single-process run");
            assertEquals(single.size(), rowCount, "P=" + parallelism + " JobResult rowCount");
            assertEquals(parallelism + 2, master.lastPhysicalPlan().instances().size(),
                    "P=" + parallelism + " total instances=source 1+groupBy P+sink 1 (proves a real fan-out rather than a P=1 fallback)");
            for (int i = 0; i < single.size(); i++) {
                assertEquals(single.get(i).get(0), cluster.get(i).get(0), "P=" + parallelism + " row " + i + " key");
                assertEquals(0, ((BigDecimal) single.get(i).get(1)).compareTo((BigDecimal) cluster.get(i).get(1)),
                        "P=" + parallelism + " row " + i + " sum");
            }
        }
    }
}

