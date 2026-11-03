package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.GroupReduceFunction;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.Union;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("multiprocess")
class InProcessClusterUnionLocalTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amount", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amount").getOutputSchema();

    private static DataRow row(int k, int amt) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, new BigDecimal(amt)); return r;
    }

    private static List<DataRow> data(int base, int n) {
        List<DataRow> l = new ArrayList<>();
        for (int i = 0; i < n; i++) l.add(row(base + i % 8, i % 10));
        return l;
    }

    private List<String> fingerprint(DataSchema meta, Path p) {
        List<String> rows = new ArrayList<>();
        for (DataRow r : FileSink.read(meta, p.toString())) {
            BigDecimal v = ((BigDecimal) r.get(1)).setScale(10, java.math.RoundingMode.HALF_UP);
            rows.add(r.get(0) + "|" + v.toPlainString());
        }
        rows.sort(null);
        return rows;
    }

    private static BatchSession session(io.rowweave.cluster.JobClient client, String name, int par) {
        BatchSession s = client != null ? new BatchSession(name, name, client) : RowWeave.createBatchSession(name);
        s.setParallelism(par);
        return s;
    }

    private List<String> runSymUnion(List<DataRow> a, List<DataRow> b, int par,
                                     io.rowweave.cluster.JobClient client, Path out) {
        BatchSession s = session(client, "usym", par);
        DataSet ga = s.fromSource(new CollectionSource(META, a))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amount"));
        DataSet gb = s.fromSource(new CollectionSource(META, b))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amount"));
        ga.union(gb, Union.of(GB)).filter("sum0 >= 5").sinkTo(new FileSink(GB, out.toString()));
        s.execute();
        return fingerprint(GB, out);
    }

    @Test
    void symmetricUnionP4MatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> a = data(0, 240), b = data(3, 300);
        List<String> single = runSymUnion(a, b, 1, null, tmp.resolve("sym-s.tsv"));
        try (InProcessCluster lc = new InProcessCluster(4, new File(tmp.toFile(), "lc-sym"))) {
            assertEquals(single, runSymUnion(a, b, 4, lc, tmp.resolve("sym-c.tsv")),
                    "a symmetric union on the local cluster at P4 matches the single-process baseline");
        }
    }

    private static final GroupReduceFunction RG_SUM = new GroupReduceFunction() {
        @Override public DataSchema getResultSchema() { return GB; }
        @Override public void reduce(Iterable<DataRow> rows, Collector c) {
            TreeMap<Integer, BigDecimal> agg = new TreeMap<>();
            for (DataRow r : rows) agg.merge((Integer) r.get(0), (BigDecimal) r.get(1), BigDecimal::add);
            for (java.util.Map.Entry<Integer, BigDecimal> e : agg.entrySet()) {
                DataRow o = new DataRow(2); o.set(0, e.getKey()); o.set(1, e.getValue()); c.collect(o);
            }
        }
    };

    private List<String> runAsymUnion(List<DataRow> a, List<DataRow> b, int par,
                                      io.rowweave.cluster.JobClient client, Path out) {
        BatchSession s = session(client, "uasym", par);
        DataSet gp = s.fromSource(new CollectionSource(META, a))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amount"));
        DataSet rg = s.fromSource(new CollectionSource(META, b))
                .reduceGroup(RG_SUM);
        gp.union(rg, Union.of(GB)).sinkTo(new FileSink(GB, out.toString()));
        s.execute();
        return fingerprint(GB, out);
    }

    @Test
    void asymmetricUnionP4MatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> a = data(0, 240), b = data(3, 300);
        List<String> single = runAsymUnion(a, b, 1, null, tmp.resolve("asym-s.tsv"));
        try (InProcessCluster lc = new InProcessCluster(4, new File(tmp.toFile(), "lc-asym"))) {
            assertEquals(single, runAsymUnion(a, b, 4, lc, tmp.resolve("asym-c.tsv")),
                    "an asymmetric union (P by 1) on the local cluster at P4 matches the single-process baseline");
        }
    }

    private List<String> runSourceUnion(List<DataRow> a, List<DataRow> b, int par,
                                        io.rowweave.cluster.JobClient client, Path out) {
        BatchSession s = session(client, "usrc", par);
        DataSet ua = s.fromSource(new CollectionSource(META, a));
        DataSet ub = s.fromSource(new CollectionSource(META, b));
        ua.union(ub, Union.of(META)).filter("amount >= 3").sinkTo(new FileSink(META, out.toString()));
        s.execute();
        return fingerprint(META, out);
    }

    @Test
    void sourceUnionP4MatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> a = data(0, 240), b = data(3, 300);
        List<String> single = runSourceUnion(a, b, 1, null, tmp.resolve("src-s.tsv"));
        try (InProcessCluster lc = new InProcessCluster(4, new File(tmp.toFile(), "lc-src"))) {
            assertEquals(single, runSourceUnion(a, b, 4, lc, tmp.resolve("src-c.tsv")),
                    "a source union on the local cluster at P4 matches the single-process baseline");
        }
    }
}

