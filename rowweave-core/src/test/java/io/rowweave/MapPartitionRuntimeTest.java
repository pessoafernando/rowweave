package io.rowweave;

import io.rowweave.cluster.InProcessCluster;
import io.rowweave.cluster.JobClient;
import io.rowweave.local.LocalJobClient;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MapPartitionRuntimeTest {
    private static final DataSchema META = new DataSchema(
            new Field("value", DataType.LongType));

    @Test
    void publicApiBuildsOneBoundedFunctionNode() {
        ExecutionEnvironment session = new ExecutionEnvironment(
                "map-partition-plan", "map-partition-plan",
                new LocalJobClient(false));
        DataSet result = session.fromSource(new CollectionSource(
                        META, Collections.singletonList(row(1L))))
                .mapPartition(new CopyFunction(),
                        new MapPartitionEmissionLimit(7L, 99L));
        result.sinkTo(new WorkerCollectingOutput(META));

        PlanGraph graph = session.toPlanGraph();
        PlanNode node = graph.node(1);
        assertEquals("mapPartition", node.typeId());
        assertEquals("7", node.param("maxOutputRowsPerPartition"));
        assertEquals("99", node.param("maxOutputBytesPerPartition"));
        assertEquals(CopyFunction.class, node.userFunction().getClass());

        assertThrows(IllegalArgumentException.class,
                () -> session.fromSource(new CollectionSource(
                                META, Collections.emptyList()))
                        .mapPartition(new NullMetaFunction(),
                                new MapPartitionEmissionLimit(1L, 1L)));
    }

    @Test
    void legacyLocalAndInProcessClusterUseTheSameRuntimeSemantics(
            @TempDir Path temp) throws Exception {
        List<Long> expected = Arrays.asList(0L, 1L, 2L, 3L);
        List<Long> local = run(new LocalJobClient(false),
                1, temp.resolve("local.tsv"));
        try (InProcessCluster cluster = new InProcessCluster(
                1, new File(temp.toFile(), "cluster"))) {
            List<Long> inProcess = run(
                    cluster, 1, temp.resolve("in-process.tsv"));
            assertEquals(expected, local);
            assertEquals(local, inProcess);
        }
    }

    @Test
    void inProcessInvokesOnceForEveryPhysicalSourceTask(
            @TempDir Path temp) throws Exception {
        try (InProcessCluster cluster = new InProcessCluster(
                2, new File(temp.toFile(), "cluster-p2"))) {
            assertEquals(Arrays.asList(0L, 0L, 1L, 2L, 3L),
                    run(cluster, 2, temp.resolve("in-process-p2.tsv")));
        }
    }

    @Test
    void localAndInProcessPreserveEveryPhysicalSourcePartition(
            @TempDir Path temp) throws Exception {
        List<Long> expected = Arrays.asList(3L, 7L);
        List<Long> local = runPartitionSums(
                new LocalJobClient(false), temp.resolve("local-sums.tsv"),
                Arrays.asList(row(1L), row(2L)),
                Arrays.asList(row(3L), row(4L)), false);
        try (InProcessCluster cluster = new InProcessCluster(
                2, new File(temp.toFile(), "cluster-sums"))) {
            List<Long> inProcess = runPartitionSums(
                    cluster, temp.resolve("in-process-sums.tsv"),
                    Arrays.asList(row(1L), row(2L)),
                    Arrays.asList(row(3L), row(4L)), false);
            assertAll(
                    () -> assertEquals(expected, local,
                            "legacy local must not merge source partitions"),
                    () -> assertEquals(expected, inProcess,
                            "cluster runtime must not hash rows across source partitions"));
        }
    }

    @Test
    void inProcessPreservesEmptyPhysicalSourcePartition(
            @TempDir Path temp) throws Exception {
        try (InProcessCluster cluster = new InProcessCluster(
                2, new File(temp.toFile(), "cluster-empty"))) {
            assertEquals(Arrays.asList(0L, 11L), runPartitionSums(
                    cluster, temp.resolve("in-process-empty.tsv"),
                    Collections.emptyList(),
                    Arrays.asList(row(5L), row(6L)), false));
        }
    }

    @Test
    void inProcessPreservesExistingHashPartitions(
            @TempDir Path temp) throws Exception {
        try (InProcessCluster cluster = new InProcessCluster(
                2, new File(temp.toFile(), "cluster-hash"))) {
            assertEquals(Arrays.asList(4L, 6L), runPartitionSums(
                    cluster, temp.resolve("in-process-hash.tsv"),
                    Arrays.asList(row(1L), row(2L)),
                    Arrays.asList(row(3L), row(4L)), true));
        }
    }

    private static List<Long> run(
            JobClient client, int parallelism, Path output) {
        ExecutionEnvironment session = new ExecutionEnvironment(
                "map-partition-runtime", "map-partition-runtime", client);
        session.setParallelism(parallelism);
        DataSet result = session.fromSource(
                        new CollectionSource(META,
                                Arrays.asList(row(1L), row(2L))),
                        new CollectionSource(META,
                                Collections.singletonList(row(3L))))
                .mapPartition(new PrefixAndCopyFunction(),
                        new MapPartitionEmissionLimit(4L, 1024L))
                .map(new IdentityFunction());
        result.sinkTo(new FileSink(META, output.toString()));
        session.execute();
        List<Long> values = new ArrayList<>();
        for (DataRow row : FileSink.read(META, output.toString())) {
            values.add((Long) row.get(0));
        }
        Collections.sort(values);
        return values;
    }

    private static List<Long> runPartitionSums(
            JobClient client, Path output, List<DataRow> first,
            List<DataRow> second, boolean hashFirst) {
        ExecutionEnvironment session = new ExecutionEnvironment(
                "map-partition-sums", "map-partition-sums", client);
        session.setParallelism(2);
        DataSet input = session.fromSource(
                new CollectionSource(META, "sum-a", first),
                new CollectionSource(META, "sum-b", second));
        if (hashFirst) {
            input = input.partitionByHash("value");
        }
        input.mapPartition(new SumFunction(),
                        new MapPartitionEmissionLimit(1L, 1024L))
                .sinkTo(new FileSink(META, output.toString()));
        session.execute();
        List<Long> values = new ArrayList<>();
        for (DataRow row : FileSink.read(META, output.toString())) {
            values.add(row.getLong(0));
        }
        Collections.sort(values);
        return values;
    }

    private static DataRow row(long value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    public static final class CopyFunction implements MapPartitionFunction {
        private static final long serialVersionUID = 1L;
        @Override public void mapPartition(
                PartitionDataRowCursor rows, Collector collector) {
            DataRow row;
            while ((row = rows.next()) != null) collector.collect(row);
        }
        @Override public DataSchema getResultSchema() { return META; }
    }

    public static final class PrefixAndCopyFunction
            implements MapPartitionFunction {
        private static final long serialVersionUID = 1L;
        @Override public void mapPartition(
                PartitionDataRowCursor rows, Collector collector) {
            collector.collect(row(0L));
            DataRow row;
            while ((row = rows.next()) != null) collector.collect(row);
        }
        @Override public DataSchema getResultSchema() { return META; }
    }

    public static final class IdentityFunction implements MapFunction {
        private static final long serialVersionUID = 1L;
        @Override public DataRow map(DataRow row) { return row; }
    }

    public static final class SumFunction implements MapPartitionFunction {
        private static final long serialVersionUID = 1L;
        @Override public void mapPartition(
                PartitionDataRowCursor rows, Collector collector) {
            long sum = 0L;
            DataRow current;
            while ((current = rows.next()) != null) {
                sum = Math.addExact(sum, current.getLong(0));
            }
            collector.collect(row(sum));
        }
        @Override public DataSchema getResultSchema() { return META; }
    }

    private static final class NullMetaFunction
            implements MapPartitionFunction {
        private static final long serialVersionUID = 1L;
        @Override public void mapPartition(
                PartitionDataRowCursor rows, Collector collector) { }
        @Override public DataSchema getResultSchema() { return null; }
    }
}
