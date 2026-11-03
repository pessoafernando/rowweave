package io.rowweave.cluster;

import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterDiamondBlockingTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static final AtomicInteger SOURCE_READS = new AtomicInteger();

    @AfterEach
    void clearFlag() {
        System.clearProperty("rowweave.cluster.materializeFanout");
    }

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

    private long runDiamond(String name, JobClient client, Source in, Path dir) {
        Path f = dir.resolve(name + ".tsv");
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : new ExecutionEnvironment(name, name);
        s.setParallelism(1);
        DataSet mid = s.fromSource(in).filter("amt >= 5");
        DataSet b1 = mid.filter("k >= 2");
        DataSet b2 = mid.filter("k <= 7");
        DataSet joined = b1.join(b2, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        joined.sinkTo(new FileSink(joined.getSchema(), f.toString()));
        s.execute();
        long count = 0;
        try (BufferedReader r = Files.newBufferedReader(f)) {
            while (r.readLine() != null) {
                count++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    @Test
    void clusterDiamondBlockingSourceReadOnce(@TempDir Path tmp) throws Exception {
        List<DataRow> data = rows();
        long oracle = runDiamond("blk-oracle", null,
                new Source() {
                    @Override public DataSchema getSchema() { return META; }
                    @Override public Iterator<DataRow> iterator() { return new ArrayList<>(data).iterator(); }
                }, tmp);
        assertTrue(oracle > 0, "the diamond emits output");

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        System.setProperty("rowweave.cluster.materializeFanout", "true");
        System.setProperty("rowweave.pipeline.deadlockTimeoutMs", "15000");
        SOURCE_READS.set(0);
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
                long c = runDiamond("blk-c1", new ClusterJobClient("127.0.0.1", m.port()), countingInput(data), tmp);
                assertEquals(oracle, c, "cluster BLOCKING diamond result matches the single-process reference");
            });
        } finally {
            System.clearProperty("rowweave.pipeline.deadlockTimeoutMs");
        }
        assertEquals(1, SOURCE_READS.get(),
                "BLOCKING materialization: the shared source is read once (duplicate computation removed; the per-sink or doubly recursive baseline reads twice)");
    }

    private long runSelfJoin(String name, JobClient client, Source in, Path dir) {
        Path f = dir.resolve(name + ".tsv");
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : new ExecutionEnvironment(name, name);
        s.setParallelism(1);
        DataSet mid = s.fromSource(in).filter("amt >= 5");
        DataSet joined = mid.join(mid, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        joined.sinkTo(new FileSink(joined.getSchema(), f.toString()));
        s.execute();
        long count = 0;
        try (BufferedReader r = Files.newBufferedReader(f)) {
            while (r.readLine() != null) {
                count++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    @Test
    void clusterSelfJoinBlockingSourceReadOnce(@TempDir Path tmp) throws Exception {
        List<DataRow> data = rows();
        long oracle = runSelfJoin("sj-oracle", null,
                new Source() {
                    @Override public DataSchema getSchema() { return META; }
                    @Override public Iterator<DataRow> iterator() { return new ArrayList<>(data).iterator(); }
                }, tmp);
        assertTrue(oracle > 0, "self-join emits output");

        File w0 = new File(tmp.toFile(), "sjw0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "sjw1"); w1.mkdirs();
        System.setProperty("rowweave.cluster.materializeFanout", "true");
        System.setProperty("rowweave.pipeline.deadlockTimeoutMs", "15000");
        SOURCE_READS.set(0);
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("sjw0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("sjw1", 32, "127.0.0.1", m.port(), w1)) {
            assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
                long c = runSelfJoin("sj-c1", new ClusterJobClient("127.0.0.1", m.port()), countingInput(data), tmp);
                assertEquals(oracle, c, "cluster self-join BLOCKING result matches the single-process reference (both legs are fed, so no row is missing)");
            });
        } finally {
            System.clearProperty("rowweave.pipeline.deadlockTimeoutMs");
        }
        assertEquals(1, SOURCE_READS.get(), "the self-join reads the shared source once");
    }
}

