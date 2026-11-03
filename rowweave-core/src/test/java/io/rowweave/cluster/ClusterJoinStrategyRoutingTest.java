package io.rowweave.cluster;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.join.JoinRuntimeFactory;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("multiprocess")
class ClusterJoinStrategyRoutingTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema JOIN = SortMergeJoin.of(
            LEFT, RIGHT, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();

    @Test
    void realSocketTwoWorkersPreserveHashSortMergeAndAutoWithRuntimeWitness(@TempDir Path tmp) throws Exception {
        List<DataRow> left = rows(160, "L");
        List<DataRow> right = rows(19, "R");
        List<String> expected = expected(left, right);

        Logger logger = (Logger) LoggerFactory.getLogger(JoinRuntimeFactory.class);
        Level previousLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        ListAppender<ILoggingEvent> events = new ListAppender<>();
        events.start();
        logger.addAppender(events);
        File w0tmp = tmp.resolve("w0").toFile(); w0tmp.mkdirs();
        File w1tmp = tmp.resolve("w1").toFile(); w1tmp.mkdirs();
        try (MasterServer master = new MasterServer(5_000);
             WorkerServer w0 = WorkerServer.newUnguarded(
                     "strategy-w0", 32, "127.0.0.1", master.port(), w0tmp);
             WorkerServer w1 = WorkerServer.newUnguarded(
                     "strategy-w1", 32, "127.0.0.1", master.port(), w1tmp)) {
            ClusterJobClient client = new ClusterJobClient("127.0.0.1", master.port());
            assertStrategy(client, left, right, "HASH", "HASH", expected, 2, tmp, events);
            assertStrategy(client, left, right, "SORT_MERGE", "SORT_MERGE", expected, 2, tmp, events);
            assertStrategy(client, left, right, null, "AUTO", expected, 2, tmp, events);
        } finally {
            logger.detachAppender(events);
            logger.setLevel(previousLevel);
            events.stop();
        }
    }

    @Test
    void inProcessP3PreservesAllStrategiesAndRuntimeWitnesses(@TempDir Path tmp) throws Exception {
        List<DataRow> left = rows(160, "L");
        List<DataRow> right = rows(19, "R");
        List<String> expected = expected(left, right);
        Logger logger = (Logger) LoggerFactory.getLogger(JoinRuntimeFactory.class);
        Level previousLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        ListAppender<ILoggingEvent> events = new ListAppender<>();
        events.start();
        logger.addAppender(events);
        try (InProcessCluster cluster = new InProcessCluster(3, tmp.resolve("in-process").toFile())) {
            assertStrategy(cluster, left, right, "HASH", "HASH", expected, 3, tmp, events);
            assertStrategy(cluster, left, right, "SORT_MERGE", "SORT_MERGE", expected, 3, tmp, events);
            assertStrategy(cluster, left, right, null, "AUTO", expected, 3, tmp, events);
        } finally {
            logger.detachAppender(events);
            logger.setLevel(previousLevel);
            events.stop();
        }
    }

    private static void assertStrategy(
            JobClient client, List<DataRow> left, List<DataRow> right,
            String hint, String expectedStrategy, List<String> expected,
            int parallelism, Path tmp, ListAppender<ILoggingEvent> events) {
        events.list.clear();
        Path outputPath = tmp.resolve("join-" + expectedStrategy + ".tsv");
        PlanGraph graph = graph(
                left, right, hint, parallelism, new FileSink(JOIN, outputPath.toString()));

        long count = client.submitToSink(graph);

        List<String> actual = fingerprints(FileSink.read(JOIN, outputPath.toString()));
        assertEquals(expected.size(), count);
        assertEquals(expected, actual);
        long witnesses = events.list.stream().map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("ROWWEAVE_JOIN_RUNTIME")
                        && message.contains("strategy=" + expectedStrategy)
                        && message.contains("decision="))
                .count();
        assertTrue(witnesses >= parallelism, "P=" + parallelism
                        + " must execute all partition consumers; got " + witnesses
                        + " witnesses for " + expectedStrategy + ": " + messages(events));
    }

    private static PlanGraph graph(
            List<DataRow> left, List<DataRow> right, String hint, int parallelism, FileSink output) {
        PlanNode.Builder join = PlanNode.binary("join", JOIN, 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k"));
        if (hint != null) join.param("joinHint", hint);
        return new PlanGraph(Arrays.asList(
                PlanNode.source(LEFT, left), PlanNode.source(RIGHT, right),
                join.build(), PlanNode.sink(JOIN, 2, output).build()), parallelism);
    }

    private static List<DataRow> rows(int count, String prefix) {
        List<DataRow> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            DataRow row = new DataRow(2);
            row.set(0, i % 11);
            row.set(1, prefix + i);
            result.add(row);
        }
        return result;
    }

    private static List<String> expected(List<DataRow> left, List<DataRow> right) {
        List<String> result = new ArrayList<>();
        for (DataRow l : left) {
            for (DataRow r : right) {
                if (l.get(0).equals(r.get(0))) {
                    result.add(l.get(0) + "|" + l.get(1) + "|" + r.get(0) + "|" + r.get(1));
                }
            }
        }
        result.sort(null);
        return result;
    }

    private static List<String> fingerprints(List<DataRow> rows) {
        List<String> result = new ArrayList<>();
        for (DataRow row : rows) {
            result.add(row.get(0) + "|" + row.get(1) + "|" + row.get(2) + "|" + row.get(3));
        }
        result.sort(null);
        return result;
    }

    private static List<String> messages(ListAppender<ILoggingEvent> events) {
        List<String> result = new ArrayList<>();
        for (ILoggingEvent event : events.list) result.add(event.getFormattedMessage());
        return result;
    }
}

