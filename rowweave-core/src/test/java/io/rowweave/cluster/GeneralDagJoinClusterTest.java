package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
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
class GeneralDagJoinClusterTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("rg", DataType.StringType));
    private static final DataSchema JOIN = SortMergeJoin.of(
            LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();
    private static final DataSchema GB = ParallelGroupByAgg.of(JOIN, "rg").sum("amt").getOutputSchema();

    private static DataRow lrow(int k, int amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    private static DataRow rrow(int k, String g) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, g);
        return r;
    }

    private static PlanGraph joinAggGraph(List<DataRow> left, List<DataRow> right, io.rowweave.Sink out, int parallelism) {
        PlanNode ls = PlanNode.source(LEFT, left);
        PlanNode rs = PlanNode.source(RIGHT, right);
        PlanNode join = PlanNode.binary("join", JOIN, 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 2)
                .listParam("keys", Arrays.asList("rg")).param("measure", "amt").build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 3).listParam("sort", Arrays.asList(
                PlanOps.encodeSortField(new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST))
        )).build();
        PlanNode sink = PlanNode.sink(GB, 4, out).build();
        return new PlanGraph(Arrays.asList(ls, rs, join, gb, ob, sink), parallelism);
    }

    private static List<DataRow> oracle(List<DataRow> left, List<DataRow> right) {
        List<DataRow> out = new ArrayList<>();
        PlanNode ls = PlanNode.source(LEFT, left);
        PlanNode rs = PlanNode.source(RIGHT, right);
        PlanNode join = PlanNode.binary("join", JOIN, 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 2)
                .listParam("keys", Arrays.asList("rg")).param("measure", "amt").build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 3).listParam("sort", Arrays.asList(
                PlanOps.encodeSortField(new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST))
        )).build();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(ls, rs, join, gb, ob)), out::add);
        return out;
    }

    @Test
    void joinThenAggP2(@TempDir Path tmp) throws Exception {
        runJoinAgg(tmp, 2);
    }

    @Test
    void joinThenAggP3(@TempDir Path tmp) throws Exception {
        runJoinAgg(tmp, 3);
    }

    private void runJoinAgg(Path tmp, int parallelism) throws Exception {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            left.add(lrow(i % 10, i % 7));        // key k = 0..9
        }
        for (int i = 0; i < 600; i++) {
            right.add(rrow(i % 10, "g" + (i % 5)));
        }
        List<DataRow> single = oracle(left, right);
        assertTrue(single.size() > 0, "the reference is non-empty");

        File outFile = tmp.resolve("joinagg.tsv").toFile();
        FileSink output = new FileSink(GB, outFile.getAbsolutePath());
        PlanGraph graph = joinAggGraph(left, right, output, parallelism);

        File w0 = tmp.resolve("w0").toFile(); w0.mkdirs();
        File w1 = tmp.resolve("w1").toFile(); w1.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer ws0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer ws1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            long rowCount = new ClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
            List<DataRow> cluster = FileSink.read(GB, outFile.getAbsolutePath());
            assertEquals(single.size(), cluster.size(), "P=" + parallelism + " group count matches the single-process run");
            assertEquals(single.size(), rowCount, "P=" + parallelism + " JobResult rowCount");
            assertEquals(2 * parallelism + 3, master.lastPhysicalPlan().instances().size(),
                    "P=" + parallelism + " total instances=left 1+right 1+join P+groupBy P+sink 1");
            for (int i = 0; i < single.size(); i++) {
                assertEquals(single.get(i).get(0), cluster.get(i).get(0), "P=" + parallelism + " row " + i + " rg");
                assertEquals(0, ((BigDecimal) single.get(i).get(1)).compareTo((BigDecimal) cluster.get(i).get(1)),
                        "P=" + parallelism + " row " + i + " sum");
            }
        }
    }
}

