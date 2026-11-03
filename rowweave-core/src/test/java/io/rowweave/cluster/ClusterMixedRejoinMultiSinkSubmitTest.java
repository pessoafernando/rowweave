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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterMixedRejoinMultiSinkSubmitTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static final AtomicInteger SOURCE_READS = new AtomicInteger();

    @AfterEach
    void clearFlags() {
        System.clearProperty("rowweave.cluster.materializeFanout");
        System.clearProperty("rowweave.pipeline.deadlockTimeoutMs");
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

    private static Source plainInput(List<DataRow> data) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(data).iterator(); }
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

    private static void buildMixedDag(BatchSession s, Source in, Path dir, String name) {
        DataSet mid = s.fromSource(in).filter("amt >= 5");
        DataSet joined = mid.join(mid,
                SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        joined.sinkTo(new FileSink(joined.getSchema(), dir.resolve(name + "-A.tsv").toString()));
        DataSet audit = mid.filter("amt >= 20");
        audit.sinkTo(new FileSink(audit.getSchema(), dir.resolve(name + "-B.tsv").toString()));
        s.execute();
    }

    private static List<String> multisetLines(Path p) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(p)) {
            String line;
            while ((line = r.readLine()) != null) {
                lines.add(line);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<String> sorted = new ArrayList<>(lines);
        Collections.sort(sorted);
        return sorted;
    }

    @Test
    void mixedRejoinMultiSinkWholeGraphSourceReadOnce(@TempDir Path tmp) throws Exception {
        List<DataRow> data = rows();

        BatchSession oracle = new BatchSession("mix-oracle", "mix-oracle");
        buildMixedDag(oracle, plainInput(data), tmp, "oracle");
        List<String> oracleA = multisetLines(tmp.resolve("oracle-A.tsv"));
        List<String> oracleB = multisetLines(tmp.resolve("oracle-B.tsv"));
        assertTrue(oracleA.size() > 0, "join output A is non-empty");
        assertTrue(oracleB.size() > 0, "audit output B is non-empty");

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        System.setProperty("rowweave.cluster.materializeFanout", "true");
        System.setProperty("rowweave.pipeline.deadlockTimeoutMs", "15000");
        SOURCE_READS.set(0);
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
                BatchSession s = new BatchSession("mix-c1", "mix-c1",
                        new ClusterJobClient("127.0.0.1", m.port()));
                s.setParallelism(1);
                buildMixedDag(s, countingInput(data), tmp, "cluster");
            });
        }

        assertEquals(oracleA, multisetLines(tmp.resolve("cluster-A.tsv")),
                "cluster mixed DAG output A (join) matches the single-process reference multiset");
        assertEquals(oracleB, multisetLines(tmp.resolve("cluster-B.tsv")),
                "cluster mixed DAG output B (audit) matches the single-process reference multiset");
        assertEquals(1, SOURCE_READS.get(),
                "a mixed DAG submitted as one whole graph: the shared source is read once");
    }

    @Test
    void mixedRejoinMultiSinkWholeGraphSourceReadOnceP2(@TempDir Path tmp) throws Exception {
        List<DataRow> data = rows();

        BatchSession oracle = new BatchSession("mix-oracle2", "mix-oracle2");
        buildMixedDag(oracle, plainInput(data), tmp, "oracle2");
        List<String> oracleA = multisetLines(tmp.resolve("oracle2-A.tsv"));
        List<String> oracleB = multisetLines(tmp.resolve("oracle2-B.tsv"));
        assertTrue(oracleA.size() > 0, "join output A is non-empty");
        assertTrue(oracleB.size() > 0, "audit output B is non-empty");

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        System.setProperty("rowweave.cluster.materializeFanout", "true");
        System.setProperty("rowweave.pipeline.deadlockTimeoutMs", "15000");
        SOURCE_READS.set(0);
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
                BatchSession s = new BatchSession("mix-c2", "mix-c2",
                        new ClusterJobClient("127.0.0.1", m.port()));
                s.setParallelism(2);
                buildMixedDag(s, countingInput(data), tmp, "cluster2");
            });
        }

        assertEquals(oracleA, multisetLines(tmp.resolve("cluster2-A.tsv")),
                "cluster mixed DAG output A (join) matches the single-process reference multiset");
        assertEquals(oracleB, multisetLines(tmp.resolve("cluster2-B.tsv")),
                "cluster mixed DAG output B (audit) matches the single-process reference multiset");
        assertEquals(1, SOURCE_READS.get(),
                "a mixed DAG submitted as one whole graph: the shared source is read once");
    }

    @Test
    void mixedRejoinMultiSinkSourceBoomFailsFastNoHang(@TempDir Path tmp) throws Exception {
        Source boom = new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    private int n = 0;
                    @Override public boolean hasNext() { return n++ < 1; }
                    @Override public DataRow next() { throw new RuntimeException("source boom"); }
                };
            }
        };

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        System.setProperty("rowweave.cluster.materializeFanout", "true");
        System.setProperty("rowweave.pipeline.deadlockTimeoutMs", "15000");
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
                BatchSession s = new BatchSession("mix-boom", "mix-boom",
                        new ClusterJobClient("127.0.0.1", m.port()));
                s.setParallelism(1);
                assertThrows(RuntimeException.class,
                        () -> buildMixedDag(s, boom, tmp, "boom"),
                        "a source throwing on the first row makes the whole multi-sink job fail fast without hanging");
            });
        }
    }
}

