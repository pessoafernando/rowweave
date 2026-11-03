package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.GroupCombineFunction;
import io.rowweave.GroupCombineReduceFunction;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class CombineGroupTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema SUM1 = new DataSchema(new Field("total", DataType.BigDecimalType));

    private static List<DataRow> data() {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < 5000; i++) { DataRow r = new DataRow(2); r.set(0, i % 10); r.set(1, new BigDecimal(i % 100)); rs.add(r); }
        return rs;
    }

    private static final GroupCombineReduceFunction SUM_CR = new GroupCombineReduceFunction() {
        @Override public DataSchema getResultSchema() { return SUM1; }
        @Override public void combine(Iterable<DataRow> it, Collector c) {
            BigDecimal s = BigDecimal.ZERO; boolean any = false;
            for (DataRow r : it) { BigDecimal v = (BigDecimal) r.get(1); if (v != null) { s = s.add(v); any = true; } }
            DataRow o = new DataRow(2); o.set(0, 0); o.set(1, any ? s : null); c.collect(o);
        }
        @Override public void reduce(Iterable<DataRow> it, Collector c) {
            BigDecimal s = BigDecimal.ZERO;
            for (DataRow r : it) { BigDecimal v = (BigDecimal) r.get(1); if (v != null) s = s.add(v); }
            DataRow o = new DataRow(1); o.set(0, s.setScale(10, RoundingMode.HALF_UP)); c.collect(o);
        }
    };

    private static final GroupCombineFunction SUM_C = new GroupCombineFunction() {
        @Override public void combine(Iterable<DataRow> it, Collector c) {
            BigDecimal s = BigDecimal.ZERO; boolean any = false;
            for (DataRow r : it) { BigDecimal v = (BigDecimal) r.get(1); if (v != null) { s = s.add(v); any = true; } }
            DataRow o = new DataRow(2); o.set(0, 0); o.set(1, any ? s : null); c.collect(o);
        }
    };

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

    // ---- combineReduceGroup: global sum(amt); the shard layout makes the intermediate state sensitive, reduce converges it to one fixed result ----

    @Test void combineReduce_singleMachine(@TempDir Path dir) {
        BigDecimal expect = BigDecimal.ZERO;
        for (int i = 0; i < 5000; i++) expect = expect.add(new BigDecimal(i % 100));
        expect = expect.setScale(10, RoundingMode.HALF_UP);

        ExecutionEnvironment s = RowWeave.createEnvironment("cr1"); s.setParallelism(1);
        DataSet ds = s.fromSource(new CollectionSource(META, data())).combineReduceGroup(SUM_CR, 4);
        Path f = dir.resolve("cr.tsv");
        ds.sinkTo(new FileSink(SUM1, f.toString()));
        s.execute();
        List<DataRow> back = FileSink.read(SUM1, f.toString());
        assertEquals(1, back.size());
        assertEquals(expect, back.get(0).get(0), "combineReduce global sum");
    }

    @Test void combineReduce_clusterMatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> d = data();
        long[] single;
        {
            ExecutionEnvironment s = RowWeave.createEnvironment("crs"); s.setParallelism(1);
            Path f = tmp.resolve("cr-s.tsv");
            s.fromSource(new CollectionSource(META, d)).combineReduceGroup(SUM_CR, 4).sinkTo(new FileSink(SUM1, f.toString()));
            s.execute();
            single = fingerprint(f);
        }
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            ExecutionEnvironment s = new ExecutionEnvironment("crc", "crc", new ClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            Path f = tmp.resolve("cr-c.tsv");
            s.fromSource(new CollectionSource(META, d)).combineReduceGroup(SUM_CR, 4).sinkTo(new FileSink(SUM1, f.toString()));
            s.execute();
            assertArrayEquals(single, fingerprint(f), "cluster combineReduce versus the single-process run (single-instance upstream chain)");
        }
    }

    @Test void combineGroup_singleMachine(@TempDir Path dir) {
        ExecutionEnvironment s = RowWeave.createEnvironment("cg1"); s.setParallelism(1);
        DataSet ds = s.fromSource(new CollectionSource(META, data())).combineGroup(SUM_C, 4);
        Path f = dir.resolve("cg.tsv");
        ds.sinkTo(new FileSink(META, f.toString()));
        s.execute();
        List<DataRow> back = FileSink.read(META, f.toString());
        assertEquals(4, back.size(), "four shards each pre-aggregate to 1 row");
    }

    @Test void combineGroup_clusterMatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> d = data();
        long[] single;
        {
            ExecutionEnvironment s = RowWeave.createEnvironment("cgs"); s.setParallelism(1);
            Path f = tmp.resolve("cg-s.tsv");
            s.fromSource(new CollectionSource(META, d)).combineGroup(SUM_C, 4).sinkTo(new FileSink(META, f.toString()));
            s.execute();
            single = fingerprint(f);
        }
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            ExecutionEnvironment s = new ExecutionEnvironment("cgc", "cgc", new ClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            Path f = tmp.resolve("cg-c.tsv");
            s.fromSource(new CollectionSource(META, d)).combineGroup(SUM_C, 4).sinkTo(new FileSink(META, f.toString()));
            s.execute();
            assertArrayEquals(single, fingerprint(f), "cluster combineGroup versus the single-process run (single-instance upstream chain, same shards)");
        }
    }
}

