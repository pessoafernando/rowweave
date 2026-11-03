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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class BigDataClusterVsSingleTest {

    private static final int N = 30_000;
    private static final int KEYS = 150;

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static DataRow row(String k, int amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    private static List<DataRow> bigRows() {
        List<DataRow> rows = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            rows.add(row("k" + (i % KEYS), i % 89));
        }
        return rows;
    }

    private static List<String> sortedRows(List<DataRow> rows, DataSchema meta) {
        List<DataRow> copy = new ArrayList<>(rows);
        copy.sort((a, b) -> {
            for (int f = 0; f < meta.getFieldCount(); f++) {
                String x = String.valueOf(a.get(f)), y = String.valueOf(b.get(f));
                int c = x.compareTo(y);
                if (c != 0) return c;
            }
            return 0;
        });
        List<String> out = new ArrayList<>(copy.size());
        for (DataRow r : copy) {
            StringBuilder sb = new StringBuilder();
            for (int f = 0; f < meta.getFieldCount(); f++) {
                Object v = r.get(f);
                sb.append(v instanceof BigDecimal ? ((BigDecimal) v).stripTrailingZeros().toPlainString()
                        : String.valueOf(v)).append('|');
            }
            out.add(sb.toString());
        }
        return out;
    }

    @Test
    void groupBySumClusterVsSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = bigRows();
        PlanNode clusterSource = ClusterTestFileInput.source(
                tmp.resolve("gb-source.tsv"), META, rows);

        List<DataRow> single = new ArrayList<>();
        PlanNode s1 = PlanNode.source(META, rows);
        PlanNode g1 = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(s1, g1)), single::add);

        File outFile = tmp.resolve("gb.tsv").toFile();
        FileSink output = new FileSink(GB, outFile.getAbsolutePath());
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 1, output).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(clusterSource, gb, sink));

        File w0 = tmp.resolve("w0").toFile(); w0.mkdirs();
        File w1 = tmp.resolve("w1").toFile(); w1.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer ws0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer ws1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            long rowCount = new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
            List<DataRow> cluster = FileSink.read(GB, outFile.getAbsolutePath());
            assertEquals(KEYS, single.size(), "single-process group count");
            assertEquals(single.size(), cluster.size(), "cluster group count matches the single-process run");
            assertEquals(single.size(), rowCount, "JobResult rowCount");
            assertEquals(sortedRows(single, GB), sortedRows(cluster, GB), "groupBySum is byte-identical between the single-process run and the cluster");
        }
    }

    @Test
    void joinClusterVsSingle(@TempDir Path tmp) throws Exception {
        DataSchema left = new DataSchema(new Field("k", DataType.IntegerType), new Field("lv", DataType.StringType));
        DataSchema right = new DataSchema(new Field("k", DataType.IntegerType), new Field("rv", DataType.StringType));
        DataSchema jm = SortMergeJoin.of(left, right, new String[]{"k"}, new String[]{"k"}, JoinType.INNER)
                .getOutputSchema();

        List<DataRow> l = new ArrayList<>();
        List<DataRow> r = new ArrayList<>();
        for (int i = 0; i < N; i++) { DataRow x = new DataRow(2); x.set(0, i % KEYS); x.set(1, "L" + i); l.add(x); }
        for (int i = 0; i < KEYS; i++) { DataRow x = new DataRow(2); x.set(0, i); x.set(1, "R" + i); r.add(x); }
        PlanNode clusterLeft = ClusterTestFileInput.source(
                tmp.resolve("join-left-source.tsv"), left, l);
        PlanNode clusterRight = ClusterTestFileInput.source(
                tmp.resolve("join-right-source.tsv"), right, r);

        List<DataRow> single = new ArrayList<>();
        PlanNode ls = PlanNode.source(left, l);
        PlanNode rs = PlanNode.source(right, r);
        PlanNode jn = PlanNode.binary("join", jm, 0, 1).param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("k")).listParam("rightKeys", Arrays.asList("k")).build();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(ls, rs, jn)), single::add);

        File outFile = tmp.resolve("join.tsv").toFile();
        FileSink output = new FileSink(jm, outFile.getAbsolutePath());
        PlanNode jn2 = PlanNode.binary("join", jm, 0, 1).param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("k")).listParam("rightKeys", Arrays.asList("k")).build();
        PlanNode sink = PlanNode.sink(jm, 2, output).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(clusterLeft, clusterRight, jn2, sink));

        File w0 = tmp.resolve("jw0").toFile(); w0.mkdirs();
        File w1 = tmp.resolve("jw1").toFile(); w1.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer ws0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer ws1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
            List<DataRow> cluster = FileSink.read(jm, outFile.getAbsolutePath());
            assertEquals(N, single.size(), "single-process join row count");
            assertEquals(single.size(), cluster.size(), "cluster join row count matches the single-process run");
            assertEquals(sortedRows(single, jm), sortedRows(cluster, jm), "the join is byte-identical between the single-process run and the cluster");
        }
    }

    @Test
    void distinctClusterVsSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = bigRows();
        DataSchema dout = META;
        PlanNode clusterSource = ClusterTestFileInput.source(
                tmp.resolve("distinct-source.tsv"), META, rows);

        List<DataRow> single = new ArrayList<>();
        PlanNode s1 = PlanNode.source(META, rows);
        PlanNode d1 = PlanNode.unary("distinct", dout, 0).listParam("keys", Arrays.asList("k")).build();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(s1, d1)), single::add);

        File outFile = tmp.resolve("distinct.tsv").toFile();
        FileSink output = new FileSink(dout, outFile.getAbsolutePath());
        PlanNode d = PlanNode.unary("distinct", dout, 0).listParam("keys", Arrays.asList("k")).build();
        PlanNode sink = PlanNode.sink(dout, 1, output).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(clusterSource, d, sink));

        File w0 = tmp.resolve("dw0").toFile(); w0.mkdirs();
        File w1 = tmp.resolve("dw1").toFile(); w1.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer ws0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer ws1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
            List<DataRow> cluster = FileSink.read(dout, outFile.getAbsolutePath());
            assertEquals(KEYS, single.size(), "single-process distinct key count");
            assertEquals(single.size(), cluster.size(), "cluster distinct count matches the single-process run");
            assertEquals(sortedRows(single, dout), sortedRows(cluster, dout), "the distinct result is byte-identical between the single-process run and the cluster");
        }
    }

    @Test
    void filterWholeGraphClusterVsSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = bigRows();
        PlanNode clusterSource = ClusterTestFileInput.source(
                tmp.resolve("filter-source.tsv"), META, rows);
        // filter amt < 40
        long expected = rows.stream().filter(x -> ((BigDecimal) x.get(1)).intValue() < 40).count();

        File outFile = tmp.resolve("filter.tsv").toFile();
        FileSink output = new FileSink(META, outFile.getAbsolutePath());
        PlanNode flt = PlanNode.unary("filter", META, 0).param("expr", "amt < 40").build();
        PlanNode sink = PlanNode.sink(META, 1, output).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(clusterSource, flt, sink));

        File w0 = tmp.resolve("fw0").toFile(); w0.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer ws0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0)) {
            long rowCount = new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
            assertEquals(expected, rowCount, "WHOLE_GRAPH filter cluster row count matches the single-process expectation");
        }
    }
}

