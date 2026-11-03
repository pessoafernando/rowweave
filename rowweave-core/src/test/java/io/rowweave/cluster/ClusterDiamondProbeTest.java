package io.rowweave.cluster;

import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
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
class ClusterDiamondProbeTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static final AtomicInteger SOURCE_READS = new AtomicInteger();

    private static Source countingInput(List<DataRow> data) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                SOURCE_READS.incrementAndGet();
                return new ArrayList<>(data).iterator();
            }
        };
    }

    private static List<DataRow> rows() {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            DataRow r = new DataRow(2);
            r.set(0, i % 10);
            r.set(1, new BigDecimal(i % 50));
            rs.add(r);
        }
        return rs;
    }

    private long runDiamond(String name, int parallelism, JobClient client, Source in, Path dir) {
        Path f = dir.resolve(name + ".tsv");
        BatchSession s = client != null ? new BatchSession(name, name, client) : new BatchSession(name, name);
        s.setParallelism(parallelism);
        DataSet mid = s.fromSource(in).filter("amt >= 5");
        DataSet b1 = mid.filter("k >= 2");
        DataSet b2 = mid.filter("k <= 7");
        DataSet joined = b1.join(b2, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
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

    @Test
    void clusterDiamondNoDeadlockAndProbeSourceReads(@TempDir Path tmp) throws Exception {
        List<DataRow> data = rows();
        long oracle = runDiamond("probe-oracle", 1, null,
                new Source() {
                    @Override public DataSchema getSchema() { return META; }
                    @Override public Iterator<DataRow> iterator() { return new ArrayList<>(data).iterator(); }
                }, tmp);
        assertTrue(oracle > 0, "the diamond emits output");

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        SOURCE_READS.set(0);
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(60), () -> {
                long c1 = runDiamond("probe-c1", 1, new ClusterJobClient("127.0.0.1", m.port()), countingInput(data), tmp);
                assertEquals(oracle, c1, "cluster P1 diamond result matches the single-process reference (no deadlock, correct)");
            });
        }
        int reads = SOURCE_READS.get();
        System.out.println("[diamond-probe] cluster P1 shared source read count = " + reads + "(1 with materialization on, 2 for the unmaterialized baseline)");
        assertTrue(reads >= 1, "read at least once (with materialization on by default this is 1; -Drowweave.cluster.materializeFanout=false falls back to 2)");
    }
}

