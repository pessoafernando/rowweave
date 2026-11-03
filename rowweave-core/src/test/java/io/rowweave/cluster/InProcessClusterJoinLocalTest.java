package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class InProcessClusterJoinLocalTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amount", DataType.BigDecimalType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("tag", DataType.IntegerType));
    private static final DataSchema JOIN = SortMergeJoin.of(
            LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();
    private static final DataSchema GB = ParallelGroupByAgg.of(JOIN, "k").sum("amount").getOutputSchema();

    private static DataRow lrow(int k, int amt) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, new BigDecimal(amt)); return r;
    }
    private static DataRow rrow(int k, int tag) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, tag); return r;
    }

    private void buildDag(ExecutionEnvironment s, List<DataRow> left, List<DataRow> right, FileSink out) {
        DataSet l = s.fromSource(new CollectionSource(LEFT, left));
        DataSet r = s.fromSource(new CollectionSource(RIGHT, right));
        l.join(r, SortMergeJoin.of(LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER))
                .groupBy(ParallelGroupByAgg.of(JOIN, "k").sum("amount"))
                .sinkTo(out);
    }

    private List<String> fingerprint(Path p) {
        List<String> rows = new ArrayList<>();
        for (DataRow r : FileSink.read(GB, p.toString())) {
            BigDecimal sum = ((BigDecimal) r.get(1)).setScale(10, java.math.RoundingMode.HALF_UP);
            rows.add(r.get(0) + "|" + sum.toPlainString());
        }
        rows.sort(null);
        return rows;
    }

    private List<String> runVia(List<DataRow> left, List<DataRow> right, int par,
                                io.rowweave.cluster.JobClient client, Path outPath) {
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment("jl", "jl", client) : RowWeave.createEnvironment("jl");
        s.setParallelism(par);
        buildDag(s, left, right, new FileSink(GB, outPath.toString()));
        s.execute();
        return fingerprint(outPath);
    }

    private static List<DataRow> leftData(int n) {
        List<DataRow> l = new ArrayList<>();
        for (int i = 0; i < n; i++) l.add(lrow(i % 6, i % 10));
        return l;
    }
    private static List<DataRow> rightData(int n) {
        List<DataRow> r = new ArrayList<>();
        for (int i = 0; i < n; i++) r.add(rrow(i % 6, i));
        return r;
    }

    @Test
    void joinP1MatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> left = leftData(120), right = rightData(60);
        List<String> single = runVia(left, right, 1, null, tmp.resolve("s1.tsv"));
        try (InProcessCluster lc = new InProcessCluster(1, new File(tmp.toFile(), "lc1"))) {
            assertEquals(single, runVia(left, right, 1, lc, tmp.resolve("c1.tsv")), "local cluster P1 matches the single-process baseline");
        }
    }

    @Test
    void joinP4MatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> left = leftData(120), right = rightData(60);
        List<String> single = runVia(left, right, 1, null, tmp.resolve("s-base.tsv"));
        try (InProcessCluster lc = new InProcessCluster(4, new File(tmp.toFile(), "lc4"))) {
            assertEquals(single, runVia(left, right, 4, lc, tmp.resolve("c4.tsv")), "local cluster P4 matches the single-process baseline");
        }
    }

    private static final DataSchema GB_LEFT = ParallelGroupByAgg.of(LEFT, "k").sum("amount").getOutputSchema();

    private List<String> runFanout(List<DataRow> left, List<DataRow> right, int par,
                                   io.rowweave.cluster.JobClient client, Path dir, String tag) {
        Path o1 = dir.resolve(tag + "-o1.tsv"); // join→groupBy
        Path o2 = dir.resolve(tag + "-o2.tsv");
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment("fo", "fo", client) : RowWeave.createEnvironment("fo");
        s.setParallelism(par);
        DataSet l = s.fromSource(new CollectionSource(LEFT, left));
        DataSet r = s.fromSource(new CollectionSource(RIGHT, right));
        l.join(r, SortMergeJoin.of(LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER))
                .groupBy(ParallelGroupByAgg.of(JOIN, "k").sum("amount"))
                .sinkTo(new FileSink(GB, o1.toString()));
        l.groupBy(ParallelGroupByAgg.of(LEFT, "k").sum("amount"))
                .sinkTo(new FileSink(GB_LEFT, o2.toString()));
        s.execute();
        List<String> fp = new ArrayList<>(fingerprint(o1));
        for (DataRow row : FileSink.read(GB_LEFT, o2.toString())) {
            BigDecimal sum = ((BigDecimal) row.get(1)).setScale(10, java.math.RoundingMode.HALF_UP);
            fp.add("o2:" + row.get(0) + "|" + sum.toPlainString());
        }
        fp.sort(null);
        return fp;
    }

    @Test
    void fanoutJoinP4MatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> left = leftData(120), right = rightData(60);
        List<String> single = runFanout(left, right, 1, null, tmp, "s");
        try (InProcessCluster lc = new InProcessCluster(4, new File(tmp.toFile(), "lc4"))) {
            assertEquals(single, runFanout(left, right, 4, lc, tmp, "c"), "fan-out on the local cluster at P4 matches the single-process baseline");
        }
    }

    private static final DataSchema GB2 = ParallelGroupByAgg.of(LEFT, "k").sum("amount").getOutputSchema();
    private static final DataSchema FULLJOIN = SortMergeJoin.of(
            GB2, GB2, new String[]{"k"}, new String[]{"k"}, JoinType.FULL).getOutputSchema();

    private List<String> runSharedSourceFullJoin(List<DataRow> left, int par,
                                                 io.rowweave.cluster.JobClient client, Path outPath) {
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment("ssfj", "ssfj", client) : RowWeave.createEnvironment("ssfj");
        s.setParallelism(par);
        DataSet src = s.fromSource(new CollectionSource(LEFT, left));
        DataSet b1 = src.groupBy(ParallelGroupByAgg.of(LEFT, "k").sum("amount"));
        DataSet b2 = src.groupBy(ParallelGroupByAgg.of(LEFT, "k").sum("amount"));
        b1.fullJoin(b2).on(new String[]{"k"}, new String[]{"k"})
                .sinkTo(new FileSink(FULLJOIN, outPath.toString()));
        s.execute();
        List<String> rows = new ArrayList<>();
        for (DataRow r : FileSink.read(FULLJOIN, outPath.toString())) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < FULLJOIN.getFieldCount(); i++) {
                Object v = r.get(i);
                if (v instanceof BigDecimal) v = ((BigDecimal) v).setScale(10, java.math.RoundingMode.HALF_UP).toPlainString();
                sb.append(v).append('|');
            }
            rows.add(sb.toString());
        }
        rows.sort(null);
        return rows;
    }

    @Test
    void sharedSourceFullJoinP4MatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> left = leftData(120);
        List<String> single = runSharedSourceFullJoin(left, 1, null, tmp.resolve("ssfj-s.tsv"));
        try (InProcessCluster lc = new InProcessCluster(4, new File(tmp.toFile(), "lc-ssfj"))) {
            assertEquals(single, runSharedSourceFullJoin(left, 4, lc, tmp.resolve("ssfj-c.tsv")),
                    "a full join over a shared source on the local cluster at P4 matches the single-process baseline");
        }
    }

    private static final io.rowweave.GroupReduceFunction RG_COUNT = new io.rowweave.GroupReduceFunction() {
        private final DataSchema out = new DataSchema(new Field("k", DataType.IntegerType), new Field("c", DataType.IntegerType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void reduce(Iterable<DataRow> rows, io.rowweave.Collector c) {
            java.util.TreeMap<Integer, Integer> cnt = new java.util.TreeMap<>();
            for (DataRow r : rows) cnt.merge((Integer) r.get(0), 1, Integer::sum);
            for (java.util.Map.Entry<Integer, Integer> e : cnt.entrySet()) { DataRow o = new DataRow(2); o.set(0, e.getKey()); o.set(1, e.getValue()); c.collect(o); }
        }
    };
    private static final DataSchema RG_OUT = new DataSchema(new Field("k", DataType.IntegerType), new Field("c", DataType.IntegerType));
    private static final DataSchema ASYM_JOIN = SortMergeJoin.of(
            GB2, RG_OUT, new String[]{"k"}, new String[]{"k"}, JoinType.FULL).getOutputSchema();

    private List<String> runAsymJoin(List<DataRow> left, int par, io.rowweave.cluster.JobClient client, Path outPath) {
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment("asym", "asym", client) : RowWeave.createEnvironment("asym");
        s.setParallelism(par);
        DataSet src = s.fromSource(new CollectionSource(LEFT, left));
        DataSet gbP = src.groupBy(ParallelGroupByAgg.of(LEFT, "k").sum("amount"));
        DataSet rg1 = src.reduceGroup(RG_COUNT);
        gbP.fullJoin(rg1).on(new String[]{"k"}, new String[]{"k"})
                .sinkTo(new FileSink(ASYM_JOIN, outPath.toString()));
        s.execute();
        List<String> rows = new ArrayList<>();
        for (DataRow r : FileSink.read(ASYM_JOIN, outPath.toString())) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < ASYM_JOIN.getFieldCount(); i++) {
                Object v = r.get(i);
                if (v instanceof BigDecimal) v = ((BigDecimal) v).setScale(10, java.math.RoundingMode.HALF_UP).toPlainString();
                sb.append(v).append('|');
            }
            rows.add(sb.toString());
        }
        rows.sort(null);
        return rows;
    }

    @Test
    void asymmetricJoinP4MatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> left = leftData(120);
        List<String> single = runAsymJoin(left, 1, null, tmp.resolve("asym-s.tsv"));
        try (InProcessCluster lc = new InProcessCluster(4, new File(tmp.toFile(), "lc-asym"))) {
            assertEquals(single, runAsymJoin(left, 4, lc, tmp.resolve("asym-c.tsv")),
                    "an asymmetric join (P by 1) on the local cluster at P4 matches the single-process baseline");
        }
    }
}

