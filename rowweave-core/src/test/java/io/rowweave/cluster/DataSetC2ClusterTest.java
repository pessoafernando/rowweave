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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class DataSetC2ClusterTest {

    private static final DataSchema ORDER = new DataSchema(
            new Field("k", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType));

    private static List<DataRow> orders() {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            DataRow r = new DataRow(2); r.set(0, i % 20); r.set(1, new BigDecimal(i % 100)); rs.add(r);
        }
        return rs;
    }

    private static long[] fingerprint(Path p) {
        long count = 0, sum = 0;
        try (BufferedReader r = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                count++;
                long h = 1125899906842597L;
                for (int i = 0; i < line.length(); i++) h = 31 * h + line.charAt(i);
                sum += h;
            }
        } catch (Exception e) { throw new RuntimeException(e); }
        return new long[]{count, sum};
    }

    // ---- addFields ----

    @Test void addFields_singleMachine(@TempDir Path dir) {
        Field[] add = {
                new Field("tag", DataType.IntegerType),
                new Field("rate", DataType.BigDecimalType),
                new Field("src", DataType.StringType)};
        Object[] vals = {7, new BigDecimal("1.2500000000"), "batch\tX"};
        ExecutionEnvironment s = RowWeave.createEnvironment("af1");
        s.setParallelism(1);
        DataSet ds = s.fromSource(new CollectionSource(ORDER, orders())).addFields(add, vals);
        assertEquals(5, ds.getSchema().getFieldCount());
        Path f = dir.resolve("af.tsv");
        ds.sinkTo(new FileSink(ds.getSchema(), f.toString()));
        s.execute();
        List<DataRow> back = FileSink.read(ds.getSchema(), f.toString());
        assertEquals(4000, back.size());
        assertEquals(7, back.get(0).get(2));
        assertEquals(new BigDecimal("1.2500000000"), back.get(0).get(3));
        assertEquals("batch\tX", back.get(0).get(4));
    }

    @Test void addFields_clusterMatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> data = orders();
        Field[] add = {new Field("tag", DataType.IntegerType), new Field("rate", DataType.BigDecimalType)};
        Object[] vals = {42, new BigDecimal("3.1400000000")};
        DataSchema outMeta = new DataSchema(ORDER.getField(0), ORDER.getField(1), add[0], add[1]);

        long[] single;
        {
            ExecutionEnvironment s = RowWeave.createEnvironment("afs"); s.setParallelism(1);
            Path f = tmp.resolve("af-s.tsv");
            s.fromSource(new CollectionSource(ORDER, data)).addFields(add, vals)
                    .sinkTo(new FileSink(outMeta, f.toString()));
            s.execute();
            single = fingerprint(f);
        }

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            ExecutionEnvironment s = new ExecutionEnvironment("afc", "afc", new ClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            Path f = tmp.resolve("af-c.tsv");
            s.fromSource(new CollectionSource(ORDER, data)).addFields(add, vals)
                    .sinkTo(new FileSink(outMeta, f.toString()));
            s.execute();
            assertArrayEquals(single, fingerprint(f), "cluster addFields versus the single-process run, as multisets");
        }
    }

    // ---- partitionByHash ----

    @Test void partitionByHash_singleMachine(@TempDir Path dir) {
        DataSchema gb = ParallelGroupByAgg.of(ORDER, "k").sum("amt").getOutputSchema();
        ExecutionEnvironment s = RowWeave.createEnvironment("ph1");
        s.setParallelism(1);
        DataSet ds = s.fromSource(new CollectionSource(ORDER, orders()))
                .partitionByHash("k")
                .groupBy(ParallelGroupByAgg.of(ORDER, "k").sum("amt"));
        Path f = dir.resolve("ph.tsv");
        ds.sinkTo(new FileSink(gb, f.toString()));
        s.execute();
        assertEquals(20, fingerprint(f)[0]);
    }

    @Test void partitionByHash_clusterMatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> data = orders();
        DataSchema gb = ParallelGroupByAgg.of(ORDER, "k").sum("amt").getOutputSchema();

        long[] single;
        {
            ExecutionEnvironment s = RowWeave.createEnvironment("phs"); s.setParallelism(1);
            Path f = tmp.resolve("ph-s.tsv");
            s.fromSource(new CollectionSource(ORDER, data))
                    .partitionByHash("k")
                    .groupBy(ParallelGroupByAgg.of(ORDER, "k").sum("amt"))
                    .sinkTo(new FileSink(gb, f.toString()));
            s.execute();
            single = fingerprint(f);
        }

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            ExecutionEnvironment s = new ExecutionEnvironment("phc", "phc", new ClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            Path f = tmp.resolve("ph-c.tsv");
            s.fromSource(new CollectionSource(ORDER, data))
                    .partitionByHash("k")
                    .groupBy(ParallelGroupByAgg.of(ORDER, "k").sum("amt"))
                    .sinkTo(new FileSink(gb, f.toString()));
            s.execute();
            assertArrayEquals(single, fingerprint(f), "cluster partitionByHash then groupBy versus the single-process run, as multisets");
        }
    }
}

