package io.rowweave.cluster;

import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterDiamondPerfCompareTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static final AtomicInteger SOURCE_READS = new AtomicInteger();

    @AfterEach
    void clear() {
        System.clearProperty("rowweave.cluster.materializeFanout");
    }

    private static Source countingInput(String csvPath) {
        return new Source() {
            private final io.rowweave.Source delegate = new io.rowweave.bench.CsvInput(csvPath, META);
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                SOURCE_READS.incrementAndGet();
                return delegate.iterator();
            }
        };
    }

    private static String writeCsv(List<DataRow> data, Path dir, String name) throws Exception {
        Path f = dir.resolve(name + "-src.tsv");
        io.rowweave.FileSink out = new io.rowweave.FileSink(META, f.toString());
        for (DataRow r : data) out.collect(r);
        out.close();
        return f.toString();
    }

    private static List<DataRow> rows(int n) {
        List<DataRow> rs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            DataRow r = new DataRow(2);
            r.set(0, i % 500);
            r.set(1, new BigDecimal(i % 1000));
            rs.add(r);
        }
        return rs;
    }

    private long runDiamond(String name, JobClient client, Source in, Path dir) {
        Path f = dir.resolve(name + ".tsv");
        ExecutionEnvironment s = new ExecutionEnvironment(name, name, client);
        s.setParallelism(1);
        DataSet mid = s.fromSource(in).filter("amt >= 100")
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        DataSchema midMeta = mid.getSchema();
        String k0 = midMeta.getField(0).getName();
        DataSet b1 = mid.filter(k0 + " >= 100");
        DataSet b2 = mid.filter(k0 + " <= 400");
        DataSet joined = b1.join(b2,
                SortMergeJoin.of(midMeta, midMeta, new String[]{k0}, new String[]{k0}, JoinType.INNER));
        joined.sinkTo(new FileSink(joined.getSchema(), f.toString()));
        s.execute();
        long count = 0;
        try (java.io.BufferedReader r = java.nio.file.Files.newBufferedReader(f)) {
            while (r.readLine() != null) {
                count++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    private long[] runMultiOutput(String name, JobClient client, Source in, Path dir, int fanoutN) {
        ExecutionEnvironment s = new ExecutionEnvironment(name, name, client);
        s.setParallelism(1);
        DataSet mid = s.fromSource(in).filter("amt >= 100")
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        DataSchema midMeta = mid.getSchema();
        String k0 = midMeta.getField(0).getName();
        for (int i = 0; i < fanoutN; i++) {
            int lo = i * 60;
            DataSet branch = mid.filter(k0 + " >= " + lo);
            branch.sinkTo(new FileSink(branch.getSchema(), dir.resolve(name + "-o" + i + ".tsv").toString()));
        }
        s.execute();
        long total = 0;
        for (int i = 0; i < fanoutN; i++) {
            try (java.io.BufferedReader r = java.nio.file.Files.newBufferedReader(dir.resolve(name + "-o" + i + ".tsv"))) {
                while (r.readLine() != null) {
                    total++;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return new long[]{total};
    }

    @Test
    void compareBlockingVsDoubleCompute(@TempDir Path tmp) throws Exception {
        List<DataRow> data = rows(200_000);
        String csv = writeCsv(data, tmp, "diamond");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();

        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", m.port());

            System.setProperty("rowweave.cluster.materializeFanout", "true");
            runDiamond("warmup", client, countingInput(csv), tmp);

            System.setProperty("rowweave.cluster.materializeFanout", "true");
            SOURCE_READS.set(0);
            long tA0 = System.nanoTime();
            long rowsA = runDiamond("blocking", client, countingInput(csv), tmp);
            long msA = (System.nanoTime() - tA0) / 1_000_000;
            int readsA = SOURCE_READS.get();

            System.setProperty("rowweave.cluster.materializeFanout", "false");
            SOURCE_READS.set(0);
            long tB0 = System.nanoTime();
            long rowsB = runDiamond("double", client, countingInput(csv), tmp);
            long msB = (System.nanoTime() - tB0) / 1_000_000;
            int readsB = SOURCE_READS.get();

            System.out.printf("[FANOUT-DUP] cluster diamond over a costly shared subgraph (source->filter->groupBy 200k rows->re-join):%n"
                    + "  A materialize ON (computed once)  = %5dms (shared source read %d times)%n"
                    + "  B materialize OFF (duplicate)     = %5dms (shared source read %d times)%n"
                    + "  -> speedup %.2fx%n",
                    msA, readsA, msB, readsB, (double) msB / Math.max(1, msA));

            assertEquals(rowsA, rowsB, "materialization does not change the algorithm: both configurations emit the same row count");
            assertEquals(1, readsA, "materialize ON: the shared source is read once");
            assertEquals(2, readsB, "materialize OFF: the shared source is read twice (each recursive leg once)");
        }
    }

}

