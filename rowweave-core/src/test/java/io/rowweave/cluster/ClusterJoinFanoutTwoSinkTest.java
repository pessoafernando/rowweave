package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.ops.Union;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@Tag("multiprocess")
class ClusterJoinFanoutTwoSinkTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("leftAmt", DataType.BigDecimalType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("rightAmt", DataType.BigDecimalType));
    private static final DataSchema CO_GROUP = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("leftCount", DataType.IntegerType),
            new Field("rightCount", DataType.IntegerType));
    private static final CoGroupFunction COUNT_GROUPS = new CoGroupFunction() {
        @Override
        public DataSchema getResultSchema() {
            return CO_GROUP;
        }

        @Override
        public void coGroup(Iterable<DataRow> left, Iterable<DataRow> right, Collector collector) {
            int leftCount = 0;
            int rightCount = 0;
            Object key = null;
            for (DataRow row : left) {
                leftCount++;
                key = row.get(0);
            }
            for (DataRow row : right) {
                rightCount++;
                if (key == null) {
                    key = row.get(0);
                }
            }
            DataRow result = new DataRow(3);
            result.set(0, key);
            result.set(1, leftCount);
            result.set(2, rightCount);
            collector.collect(result);
        }
    };

    @AfterEach
    void clearFlags() {
        System.clearProperty("rowweave.cluster.jobHardLimitMs");
    }

    @Test
    void joinOutputBroadcastsToBothSinkBranches(@TempDir Path tmp) throws Exception {
        assertJoinFanout(tmp, 1, true);
    }

    @Test
    void joinOutputBroadcastsWithParallelismTwoAndObjectReuseOff(@TempDir Path tmp) throws Exception {
        assertJoinFanout(tmp, 2, false);
    }

    @Test
    void unionOutputBroadcastsToBothSinkBranches(@TempDir Path tmp) throws Exception {
        List<DataRow> leftRows = rows(LEFT, 30, 3);
        List<DataRow> rightRows = rows(LEFT, 30, 33);
        assertBinaryFanout(tmp, "union", leftRows, rightRows, 1, true,
                (session, left, right, dir, name) -> {
                    DataSet unioned = session.fromSource(new CollectionSource(LEFT, left))
                            .union(session.fromSource(new CollectionSource(LEFT, right)), Union.of(LEFT));
                    DataSet branchA = unioned.filter("leftAmt >= 20");
                    DataSet branchB = unioned.filter("leftAmt <= 40");
                    branchA.sinkTo(new FileSink(branchA.getSchema(), dir.resolve(name + "-a.tsv").toString()));
                    branchB.sinkTo(new FileSink(branchB.getSchema(), dir.resolve(name + "-b.tsv").toString()));
                });
    }

    @Test
    void coGroupOutputBroadcastsToBothSinkBranches(@TempDir Path tmp) throws Exception {
        List<DataRow> leftRows = rows(LEFT, 30, 3);
        List<DataRow> rightRows = rows(RIGHT, 30, 13);
        assertBinaryFanout(tmp, "cogroup", leftRows, rightRows, 1, true,
                (session, left, right, dir, name) -> {
                    DataSet grouped = session.fromSource(new CollectionSource(LEFT, left))
                            .coGroup(session.fromSource(new CollectionSource(RIGHT, right)),
                                    CoGroup.of(LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, COUNT_GROUPS));
                    DataSet branchA = grouped.filter("leftCount >= 1");
                    DataSet branchB = grouped.filter("rightCount >= 1");
                    branchA.sinkTo(new FileSink(branchA.getSchema(), dir.resolve(name + "-a.tsv").toString()));
                    branchB.sinkTo(new FileSink(branchB.getSchema(), dir.resolve(name + "-b.tsv").toString()));
                });
    }

    @Test
    void binaryOutputRejoinBroadcastsEveryConsumerEdge(@TempDir Path tmp) throws Exception {
        List<DataRow> leftRows = rows(LEFT, 24, 3);
        List<DataRow> rightRows = rows(RIGHT, 24, 11);
        runJoinRejoin("oracle-rejoin", null, leftRows, rightRows, tmp);
        List<String> oracle = multisetLines(tmp.resolve("oracle-rejoin.tsv"));

        File w0 = new File(tmp.toFile(), "w0");
        File w1 = new File(tmp.toFile(), "w1");
        w0.mkdirs();
        w1.mkdirs();
        System.setProperty("rowweave.cluster.jobHardLimitMs", "5000");
        try (MasterServer master = new MasterServer(5000);
             WorkerServer worker0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    runJoinRejoin("cluster-rejoin", new RealClusterJobClient("127.0.0.1", master.port()),
                            leftRows, rightRows, tmp));
        }
        assertEquals(oracle, multisetLines(tmp.resolve("cluster-rejoin.tsv")),
                "both edges of the join output re-entering one union must receive complete records and EOS");
    }

    @Test
    void emptyBinaryOutputStillClosesEveryRejoinEdge(@TempDir Path tmp) throws Exception {
        List<DataRow> leftRows = rows(LEFT, 12, 3);
        List<DataRow> rightRows = rows(RIGHT, 12, 11);
        for (DataRow right : rightRows) {
            right.set(0, ((Integer) right.get(0)) + 100);
        }
        runJoinRejoin("oracle-empty-rejoin", null, leftRows, rightRows, tmp);
        assertEquals(Collections.emptyList(), multisetLines(tmp.resolve("oracle-empty-rejoin.tsv")));

        File w0 = new File(tmp.toFile(), "w0");
        File w1 = new File(tmp.toFile(), "w1");
        w0.mkdirs();
        w1.mkdirs();
        System.setProperty("rowweave.cluster.jobHardLimitMs", "5000");
        try (MasterServer master = new MasterServer(5000);
             WorkerServer worker0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    runJoinRejoin("cluster-empty-rejoin", new RealClusterJobClient("127.0.0.1", master.port()),
                            leftRows, rightRows, tmp));
        }
        assertEquals(Collections.emptyList(), multisetLines(tmp.resolve("cluster-empty-rejoin.tsv")),
                "even an empty join output must send EOS on every input edge of the same union");
    }

    @Test
    void sourceSharedByJoinAndIndependentSinkWorksWithObjectReuseOff(@TempDir Path tmp) throws Exception {
        List<DataRow> leftRows = rows(LEFT, 30, 3);
        List<DataRow> rightRows = rows(RIGHT, 30, 11);
        runSideBranch("oracle-side", null, leftRows, rightRows, tmp);
        List<String> oracleJoin = multisetLines(tmp.resolve("oracle-side-join.tsv"));
        List<String> oracleSide = multisetLines(tmp.resolve("oracle-side-side.tsv"));

        File w0 = new File(tmp.toFile(), "w0");
        File w1 = new File(tmp.toFile(), "w1");
        w0.mkdirs();
        w1.mkdirs();
        System.setProperty("rowweave.cluster.jobHardLimitMs", "5000");
        try (MasterServer master = new MasterServer(5000);
             WorkerServer worker0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            master.setObjectReuse(false);
            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    runSideBranch("cluster-side", new RealClusterJobClient("127.0.0.1", master.port()),
                            leftRows, rightRows, tmp));
        }
        assertEquals(oracleJoin, multisetLines(tmp.resolve("cluster-side-join.tsv")),
                "the join root sharing a source must execute fully");
        assertEquals(oracleSide, multisetLines(tmp.resolve("cluster-side-side.tsv")),
                "the independent Sink root sharing a source must execute fully");
    }

    private static void assertJoinFanout(Path tmp, int parallelism, boolean objectReuse) throws Exception {
        List<DataRow> leftRows = rows(LEFT, 60, 3);
        List<DataRow> rightRows = rows(RIGHT, 60, 5);

        assertBinaryFanout(tmp, "join", leftRows, rightRows, parallelism, objectReuse,
                (session, left, right, dir, name) -> {
                    DataSet joined = session.fromSource(new CollectionSource(LEFT, left))
                            .join(session.fromSource(new CollectionSource(RIGHT, right)),
                                    SortMergeJoin.of(LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
                    DataSet branchA = joined.filter("leftAmt >= 20");
                    DataSet branchB = joined.filter("rightAmt >= 20");
                    branchA.sinkTo(new FileSink(branchA.getSchema(), dir.resolve(name + "-a.tsv").toString()));
                    branchB.sinkTo(new FileSink(branchB.getSchema(), dir.resolve(name + "-b.tsv").toString()));
                });
    }

    private static void assertBinaryFanout(Path tmp, String operator, List<DataRow> leftRows, List<DataRow> rightRows,
                                            int parallelism, boolean objectReuse, JobGraph graph) throws Exception {
        String suffix = operator + "-p" + parallelism + (objectReuse ? "-reuse" : "-copy");
        run("oracle-" + suffix, null, leftRows, rightRows, tmp, 1, graph);
        List<String> oracleA = multisetLines(tmp.resolve("oracle-" + suffix + "-a.tsv"));
        List<String> oracleB = multisetLines(tmp.resolve("oracle-" + suffix + "-b.tsv"));
        assertFalse(oracleA.isEmpty(), "the reference result A is non-empty");
        assertFalse(oracleB.isEmpty(), "the reference result B is non-empty");

        File w0 = new File(tmp.toFile(), "w0");
        File w1 = new File(tmp.toFile(), "w1");
        w0.mkdirs();
        w1.mkdirs();
        System.setProperty("rowweave.cluster.jobHardLimitMs", "5000");
        try (MasterServer master = new MasterServer(5000);
             WorkerServer worker0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            master.setObjectReuse(objectReuse);
            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    run("cluster-" + suffix, new RealClusterJobClient("127.0.0.1", master.port()),
                            leftRows, rightRows, tmp, parallelism, graph));
        }

        assertEquals(oracleA, multisetLines(tmp.resolve("cluster-" + suffix + "-a.tsv")),
                "join fan-out branch A matches the single-process reference as a full multiset");
        assertEquals(oracleB, multisetLines(tmp.resolve("cluster-" + suffix + "-b.tsv")),
                "join fan-out branch B matches the single-process reference as a full multiset");
    }

    private static void run(String name, JobClient client, List<DataRow> leftRows, List<DataRow> rightRows, Path dir,
                            int parallelism, JobGraph graph) {
        ExecutionEnvironment session = client == null ? new ExecutionEnvironment(name, name) : new ExecutionEnvironment(name, name, client);
        session.setParallelism(parallelism);
        graph.build(session, leftRows, rightRows, dir, name);
        session.execute();
    }

    private static void runJoinRejoin(String name, JobClient client, List<DataRow> leftRows, List<DataRow> rightRows,
                                      Path dir) {
        ExecutionEnvironment session = client == null ? new ExecutionEnvironment(name, name) : new ExecutionEnvironment(name, name, client);
        session.setParallelism(1);
        DataSet joined = session.fromSource(new CollectionSource(LEFT, leftRows))
                .join(session.fromSource(new CollectionSource(RIGHT, rightRows)),
                        SortMergeJoin.of(LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        DataSet rejoined = joined.union(joined, Union.of(joined.getSchema()));
        rejoined.sinkTo(new FileSink(rejoined.getSchema(), dir.resolve(name + ".tsv").toString()));
        session.execute();
    }

    private static void runSideBranch(String name, JobClient client, List<DataRow> leftRows, List<DataRow> rightRows,
                                      Path dir) {
        ExecutionEnvironment session = client == null ? new ExecutionEnvironment(name, name) : new ExecutionEnvironment(name, name, client);
        session.setParallelism(1);
        DataSet left = session.fromSource(new CollectionSource(LEFT, leftRows));
        DataSet joined = left.join(session.fromSource(new CollectionSource(RIGHT, rightRows)),
                SortMergeJoin.of(LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        joined.sinkTo(new FileSink(joined.getSchema(), dir.resolve(name + "-join.tsv").toString()));
        DataSet side = left.filter("leftAmt >= 20");
        side.sinkTo(new FileSink(side.getSchema(), dir.resolve(name + "-side.tsv").toString()));
        session.execute();
    }

    @FunctionalInterface
    private interface JobGraph {
        void build(ExecutionEnvironment session, List<DataRow> left, List<DataRow> right, Path dir, String name);
    }

    private static List<DataRow> rows(DataSchema meta, int count, int offset) {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            DataRow row = new DataRow(2);
            row.set(0, i % 6);
            row.set(1, new BigDecimal(i + offset));
            rows.add(row);
        }
        return rows;
    }

    private static List<String> multisetLines(Path path) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Collections.sort(lines);
        return lines;
    }
}

