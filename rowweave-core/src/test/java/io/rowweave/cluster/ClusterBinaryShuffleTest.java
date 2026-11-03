package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterBinaryShuffleTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema JOIN = SortMergeJoin.of(
            LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();

    private static DataRow lrow(int k, String v) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, v);
        return r;
    }

    private static PlanGraph joinSinkGraph(List<DataRow> left, List<DataRow> right, io.rowweave.Sink output) {
        PlanNode ls = PlanNode.source(LEFT, left);
        PlanNode rs = PlanNode.source(RIGHT, right);
        PlanNode join = PlanNode.binary("join", JOIN, 0, 1)
                .param("joinType", "INNER")
                .param("joinHint", "SORT_MERGE")
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
        PlanNode sink = PlanNode.sink(JOIN, 2, output).build();
        return new PlanGraph(Arrays.asList(ls, rs, join, sink));
    }

    @Test
    void crossWorkerInnerJoinMatchesOracle(@TempDir Path tmp) throws Exception {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            left.add(lrow(i % 12, "L" + i));
        }
        for (int i = 0; i < 300; i++) {
            right.add(lrow(i % 12, "R" + i));
        }
        List<DataRow> oracle = singleMachineOracle(left, right);

        File outFile = tmp.resolve("cluster-join.tsv").toFile();
        FileSink output = new FileSink(JOIN, outFile.getAbsolutePath());
        PlanGraph graph = joinSinkGraph(left, right, output);

        File w0tmp = tmp.resolve("w0").toFile(); w0tmp.mkdirs();
        File w1tmp = tmp.resolve("w1").toFile(); w1tmp.mkdirs();

        try (MasterServer master = new MasterServer(5000);
             WorkerServer w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0tmp);
             WorkerServer w1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1tmp)) {

            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", master.port());
            long rowCount = client.submitToSink(graph);

            List<DataRow> got = FileSink.read(JOIN, outFile.getAbsolutePath());
            assertEquals(oracle.size(), got.size(), "join row count matches");
            assertEquals(oracle.size(), rowCount, "JobResult rowCount = rows written by the sink");
            for (int i = 0; i < oracle.size(); i++) {
                for (int f = 0; f < JOIN.getFieldCount(); f++) {
                    assertEquals(oracle.get(i).get(f), got.get(i).get(f), "row " + i + " field " + f);
                }
            }
        }
    }

    @Test
    void crossWorkerSkewSpillMatchesOracle(@TempDir Path tmp) throws Exception {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 1500; i++) {
            left.add(lrow(7, "L" + i));
        }
        for (int i = 0; i < 2; i++) {
            right.add(lrow(7, "R" + i));
        }
        List<DataRow> oracle = singleMachineOracle(left, right);
        assertEquals(3000, oracle.size(), "Cartesian product 1500x2");

        File outFile = tmp.resolve("cluster-skew.tsv").toFile();
        FileSink output = new FileSink(JOIN, outFile.getAbsolutePath());
        PlanGraph graph = joinSinkGraph(left, right, output);
        File w0tmp = tmp.resolve("w0").toFile(); w0tmp.mkdirs();
        File w1tmp = tmp.resolve("w1").toFile(); w1tmp.mkdirs();

        try (MasterServer master = new MasterServer(5000);
             WorkerServer w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0tmp);
             WorkerServer w1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1tmp)) {
            long rowCount = new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
            List<DataRow> got = FileSink.read(JOIN, outFile.getAbsolutePath());
            assertEquals(oracle.size(), rowCount);
            assertEquals(oracle.size(), got.size());
            for (int i = 0; i < oracle.size(); i++) {
                for (int f = 0; f < JOIN.getFieldCount(); f++) {
                    assertEquals(oracle.get(i).get(f), got.get(i).get(f), "row " + i + " field " + f);
                }
            }
        }
    }

    @Test
    void crossWorkerLeftOuterNullMatchesOracle(@TempDir Path tmp) throws Exception {
        List<DataRow> left = Arrays.asList(lrow(1, "L1"), lrow(2, "L2"), nullKeyRow("Lnull"));
        List<DataRow> right = Arrays.asList(lrow(1, "R1"), nullKeyRow("Rnull"));
        List<DataRow> oracle = oracleWithType(left, right, JoinType.LEFT);

        File outFile = tmp.resolve("cluster-leftouter.tsv").toFile();
        FileSink output = new FileSink(JOIN, outFile.getAbsolutePath());
        PlanGraph graph = leftJoinSinkGraph(left, right, output);
        File w0tmp = tmp.resolve("w0").toFile(); w0tmp.mkdirs();

        try (MasterServer master = new MasterServer(5000);
             WorkerServer w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0tmp)) {
            long rowCount = new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
            List<DataRow> got = FileSink.read(JOIN, outFile.getAbsolutePath());
            assertEquals(3, oracle.size());
            assertEquals(oracle.size(), rowCount);
            assertEquals(oracle.size(), got.size());
            for (int i = 0; i < oracle.size(); i++) {
                for (int f = 0; f < JOIN.getFieldCount(); f++) {
                    assertEquals(oracle.get(i).get(f), got.get(i).get(f), "row " + i + " field " + f);
                }
            }
        }
    }

    private static DataRow nullKeyRow(String v) {
        DataRow r = new DataRow(2);
        r.set(0, null);
        r.set(1, v);
        return r;
    }

    private static PlanGraph leftJoinSinkGraph(List<DataRow> left, List<DataRow> right, io.rowweave.Sink output) {
        PlanNode ls = PlanNode.source(LEFT, left);
        PlanNode rs = PlanNode.source(RIGHT, right);
        PlanNode join = PlanNode.binary("join", JOIN, 0, 1)
                .param("joinType", "LEFT")
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
        PlanNode sink = PlanNode.sink(JOIN, 2, output).build();
        return new PlanGraph(Arrays.asList(ls, rs, join, sink));
    }

    private static List<DataRow> oracleWithType(List<DataRow> left, List<DataRow> right, JoinType type) {
        PlanNode ls = PlanNode.source(LEFT, left);
        PlanNode rs = PlanNode.source(RIGHT, right);
        PlanNode join = PlanNode.binary("join", JOIN, 0, 1)
                .param("joinType", type.name())
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
        List<String> sort = new ArrayList<>();
        for (int f = 0; f < JOIN.getFieldCount(); f++) {
            sort.add(PlanOps.encodeSortField(
                    new SortField(f, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)));
        }
        PlanNode ob = PlanNode.unary("orderBy", JOIN, 2).listParam("sort", sort).build();
        List<DataRow> out = new ArrayList<>();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(ls, rs, join, ob)), out::add);
        return out;
    }

    private static List<DataRow> singleMachineOracle(List<DataRow> left, List<DataRow> right) {
        PlanNode ls = PlanNode.source(LEFT, left);
        PlanNode rs = PlanNode.source(RIGHT, right);
        PlanNode join = PlanNode.binary("join", JOIN, 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
        List<String> sort = new ArrayList<>();
        for (int f = 0; f < JOIN.getFieldCount(); f++) {
            sort.add(PlanOps.encodeSortField(
                    new SortField(f, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)));
        }
        PlanNode ob = PlanNode.unary("orderBy", JOIN, 2).listParam("sort", sort).build();
        List<DataRow> out = new ArrayList<>();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(ls, rs, join, ob)), out::add);
        return out;
    }
}

