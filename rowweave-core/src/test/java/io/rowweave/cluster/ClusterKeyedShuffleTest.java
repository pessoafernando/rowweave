package io.rowweave.cluster;

import io.rowweave.CollectionSource;
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
class ClusterKeyedShuffleTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static DataRow row(String k, String amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    @Test
    void crossWorkerKeyedGroupBySumMatchesOracle(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            rows.add(row("k" + (i % 15), Integer.toString(i % 40)));
        }
        List<DataRow> oracle = singleMachineOracle(rows);

        File outFile = tmp.resolve("cluster-shuffle.tsv").toFile();
        FileSink output = new FileSink(GB, outFile.getAbsolutePath());

        PlanNode src = PlanNode.source(META, rows);
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 1, output).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(src, gb, sink));

        File w0tmp = tmp.resolve("w0").toFile(); w0tmp.mkdirs();
        File w1tmp = tmp.resolve("w1").toFile(); w1tmp.mkdirs();

        try (MasterServer master = new MasterServer(5000);
             WorkerServer w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0tmp);
             WorkerServer w1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1tmp)) {

            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", master.port());
            long rowCount = client.submitToSink(graph);

            List<DataRow> got = FileSink.read(GB, outFile.getAbsolutePath());
            assertEquals(oracle.size(), got.size(), "group count matches");
            assertEquals(oracle.size(), rowCount, "JobResult rowCount = rows written by the sink");
            for (int i = 0; i < oracle.size(); i++) {
                assertEquals(oracle.get(i).get(0), got.get(i).get(0), "row " + i + " key");
                assertEquals(0, ((BigDecimal) oracle.get(i).get(1)).compareTo((BigDecimal) got.get(i).get(1)),
                        "row " + i + " sum");
            }
        }
    }

    private static List<DataRow> singleMachineOracle(List<DataRow> rows) {
        PlanNode src = PlanNode.source(META, rows);
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 1)
                .listParam("sort", Arrays.asList(
                        PlanOps.encodeSortField(new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                        PlanOps.encodeSortField(new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST))))
                .build();
        List<DataRow> out = new ArrayList<>();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(src, gb, ob)), out::add);
        return out;
    }
}

