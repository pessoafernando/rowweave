package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.MapFunction;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterSubmitChainTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));
    private static final MapFunction SHORT_ROW_MAP = new MapFunction() {
        @Override
        public DataRow map(DataRow input) {
            DataRow shortRow = new DataRow(1);
            shortRow.set(0, input.get(0));
            return shortRow;
        }
    };
    private static final MapFunction WIDE_ROW_MAP = new MapFunction() {
        @Override
        public DataRow map(DataRow input) {
            DataRow wideRow = new DataRow(3);
            wideRow.set(0, input.get(0));
            wideRow.set(1, input.get(1));
            wideRow.set(2, "must-not-be-truncated");
            return wideRow;
        }
    };

    private static DataRow row(String k, String amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    @Test
    void submitWholeGraphViaClusterRpc(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(row("k" + i, Integer.toString(i)));
        }
        File outFile = tmp.resolve("cluster-result.tsv").toFile();
        Files.write(outFile.toPath(), "previous-cluster-result\n".getBytes(StandardCharsets.UTF_8));
        FileSink output = new FileSink(META, outFile.getAbsolutePath());

        PlanNode src = PlanNode.source(META, rows);
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "amt >= 50").build();
        PlanNode sink = PlanNode.sink(META, 1, output).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(src, filter, sink));

        File workerTmp = tmp.resolve("worker").toFile();
        workerTmp.mkdirs();

        try (MasterServer master = new MasterServer(2000);
             WorkerServer worker = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), workerTmp)) {
            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", master.port());
            long rowCount = client.submitToSink(graph);

            assertEquals(50, rowCount, "JobResult rowCount = rows after the filter");
            List<DataRow> got = FileSink.read(META, outFile.getAbsolutePath());
            assertEquals(50, got.size(), "result file row count");
            for (DataRow r : got) {
                assertTrue(((BigDecimal) r.get(1)).intValue() >= 50, "the filter took effect");
            }
        }
    }

    @Test
    void submitWholeGraphReingestsFileInputOnWorker(@TempDir Path tmp) throws Exception {
        File sourceFile = tmp.resolve("cluster-source.tsv").toFile();
        FileSink sourceOutput = new FileSink(META, sourceFile.getAbsolutePath());
        sourceOutput.collect(row("k1", "10"));
        sourceOutput.collect(row("k2", "60"));
        sourceOutput.collect(row("k3", "70"));
        sourceOutput.close();
        File resultFile = tmp.resolve("cluster-reingested.tsv").toFile();

        PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(
                new FileSource(META, sourceFile.getAbsolutePath())));
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "amt >= 50").build();
        PlanNode sink = PlanNode.sink(META, 1,
                new FileSink(META, resultFile.getAbsolutePath())).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(src, filter, sink));

        File workerTmp = tmp.resolve("file-input-worker").toFile();
        workerTmp.mkdirs();
        try (MasterServer master = new MasterServer(2000);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "file-input-worker", 32, "127.0.0.1", master.port(), workerTmp)) {
            long rows = new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph);
            assertEquals(2L, rows);
        }

        try (FileSource.Reader result = new FileSource(META, resultFile.getAbsolutePath()).openReader()) {
            assertEquals(new BigDecimal("60"), result.next().get(1));
            assertEquals(new BigDecimal("70"), result.next().get(1));
            assertFalse(result.hasNext());
        }
    }

    @Test
    void clusterRejectsShortOutputRowAndPreservesPreviousCommit(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("cluster-schema-guard.tsv");
        byte[] previous = "previous-cluster-result\n".getBytes(StandardCharsets.UTF_8);
        Files.write(target, previous);

        PlanNode source = PlanNode.source(META, Collections.singletonList(row("k1", "1")));
        PlanNode malformed = PlanNode.unaryFn("map", META, 0, SHORT_ROW_MAP);
        PlanNode sink = PlanNode.sink(META, 1, new FileSink(META, target.toString())).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, malformed, sink));

        File workerTmp = tmp.resolve("schema-worker").toFile();
        workerTmp.mkdirs();
        try (MasterServer master = new MasterServer(2000);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "schema-worker", 32, "127.0.0.1", master.port(), workerTmp)) {
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph));
            String messages = causeMessages(failure);
            assertTrue(messages.contains("output row width mismatch"), messages);
            assertTrue(messages.contains("expected=2"), messages);
            assertTrue(messages.contains("actual=1"), messages);
        }

        assertArrayEquals(previous, Files.readAllBytes(target),
                "a Worker schema failure must keep the last published version");
        try (java.util.stream.Stream<Path> files = Files.list(tmp)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().startsWith(".rowweave-output-")),
                    "a Worker schema failure must leave no FileSink attempt behind");
        }
    }

    @Test
    void clusterShuffleRejectsWideRowBeforeLossySerialization(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("cluster-shuffle-schema-guard.tsv");
        byte[] previous = "previous-shuffle-result\n".getBytes(StandardCharsets.UTF_8);
        Files.write(target, previous);

        PlanNode source = PlanNode.source(META, Arrays.asList(
                row("k0", "1"), row("k1", "2"), row("k2", "3"), row("k3", "4")));
        PlanNode malformed = PlanNode.unaryFn("map", META, 0, WIDE_ROW_MAP);
        PlanNode partition = PlanNode.unary("partitionByHash", META, 1)
                .listParam("keys", Collections.singletonList("k"))
                .build();
        PlanNode sink = PlanNode.sink(META, 2, new FileSink(META, target.toString())).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, malformed, partition, sink), 2);

        File w0 = tmp.resolve("schema-shuffle-w0").toFile();
        File w1 = tmp.resolve("schema-shuffle-w1").toFile();
        assertTrue(w0.mkdirs());
        assertTrue(w1.mkdirs());
        try (MasterServer master = new MasterServer(2000);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "schema-shuffle-w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "schema-shuffle-w1", 32, "127.0.0.1", master.port(), w1)) {
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph));
            String messages = causeMessages(failure);
            assertTrue(messages.contains("row width mismatch"), messages);
            assertTrue(messages.contains("expected=2"), messages);
            assertTrue(messages.contains("actual=3"), messages);
        }

        assertArrayEquals(previous, Files.readAllBytes(target),
                "a shuffle schema failure must keep the last published version");
        try (java.util.stream.Stream<Path> files = Files.list(tmp)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().startsWith(".rowweave-output-")),
                    "a shuffle schema failure must leave no FileSink attempt behind");
        }
    }

    @Test
    void clusterMultiOutputFailureRestoresEarlierPreparedFile(@TempDir Path tmp) throws Exception {
        Path firstTarget = tmp.resolve("cluster-multi-first.tsv");
        Path failingTarget = tmp.resolve("cluster-multi-failing.tsv");
        byte[] firstOld = "cluster-first-old\n".getBytes(StandardCharsets.UTF_8);
        byte[] failingOld = "cluster-failing-old\n".getBytes(StandardCharsets.UTF_8);
        Files.write(firstTarget, firstOld);
        Files.write(failingTarget, failingOld);
        Path firstTerminalMarker = tmp.resolve("first-terminal.marker");

        PlanNode source = PlanNode.source(META, Collections.singletonList(row("k1", "1")));
        PlanNode partition = PlanNode.unary("partitionByHash", META, 0)
                .listParam("keys", Collections.singletonList("k"))
                .build();
        PlanNode firstBranch = PlanNode.unary("filter", META, 1).param("expr", "amt >= 0").build();
        PlanNode firstSink = PlanNode.sink(
                META, 2, new SignallingFileOutput(META, firstTarget.toString(), firstTerminalMarker.toString())).build();
        PlanNode failingBranch = PlanNode.unaryFn(
                "map", META, 1, new WaitForMarkerShortRowMap(firstTerminalMarker.toString()));
        PlanNode failingSink = PlanNode.sink(
                META, 4, new FileSink(META, failingTarget.toString())).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(
                source, partition, firstBranch, firstSink, failingBranch, failingSink), 2);

        File w0 = tmp.resolve("multi-w0").toFile();
        File w1 = tmp.resolve("multi-w1").toFile();
        assertTrue(w0.mkdirs());
        assertTrue(w1.mkdirs());
        try (MasterServer master = new MasterServer(2000);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "multi-w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "multi-w1", 32, "127.0.0.1", master.port(), w1)) {
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph));
            assertTrue(causeMessages(failure).contains("row width"), causeMessages(failure));
        }

        assertArrayEquals(firstOld, Files.readAllBytes(firstTarget),
                "a cluster Job failure must roll back the FileSink of an already completed SINK");
        assertArrayEquals(failingOld, Files.readAllBytes(failingTarget));
        try (java.util.stream.Stream<Path> files = Files.list(tmp)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().startsWith(".rowweave-output-")),
                    "a cluster failure must leave no attempt or backup behind");
        }
    }

    @Test
    void clusterSecondPublishFailureRestoresFirstPublishedFile(@TempDir Path tmp) throws Exception {
        Path firstTarget = tmp.resolve("cluster-publish-first.tsv");
        Path secondTarget = tmp.resolve("cluster-publish-second.tsv");
        byte[] firstOld = "cluster-publish-first-old\n".getBytes(StandardCharsets.UTF_8);
        byte[] secondOld = "cluster-publish-second-old\n".getBytes(StandardCharsets.UTF_8);
        Files.write(firstTarget, firstOld);
        Files.write(secondTarget, secondOld);

        PlanNode source = PlanNode.source(META, Arrays.asList(row("k1", "1"), row("k2", "2")));
        PlanNode partition = PlanNode.unary("partitionByHash", META, 0)
                .listParam("keys", Collections.singletonList("k"))
                .build();
        PlanNode firstSink = PlanNode.sink(
                META, 1, new FileSink(META, firstTarget.toString())).build();
        PlanNode secondSink = PlanNode.sink(
                META, 1, new CommitFailingFileOutput(META, secondTarget.toString())).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, partition, firstSink, secondSink), 2);

        File w0 = tmp.resolve("publish-w0").toFile();
        File w1 = tmp.resolve("publish-w1").toFile();
        assertTrue(w0.mkdirs());
        assertTrue(w1.mkdirs());
        try (MasterServer master = new MasterServer(2000);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "publish-w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "publish-w1", 32, "127.0.0.1", master.port(), w1)) {
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> new RealClusterJobClient("127.0.0.1", master.port()).submitToSink(graph));
            assertTrue(causeMessages(failure).contains("injected cluster publish failure"),
                    causeMessages(failure));
            assertEquals(0, worker0.preparedOutputActiveCount());
            assertEquals(0, worker1.preparedOutputActiveCount());
        }

        assertArrayEquals(firstOld, Files.readAllBytes(firstTarget));
        assertArrayEquals(secondOld, Files.readAllBytes(secondTarget));
        try (java.util.stream.Stream<Path> files = Files.list(tmp)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().startsWith(".rowweave-output-")));
        }
    }

    @Test
    void clusterFinalizeRetrySucceedsAndReleasesPreparedOutputs(@TempDir Path tmp) throws Exception {
        Path firstTarget = tmp.resolve("cluster-finalize-first.tsv");
        Path secondTarget = tmp.resolve("cluster-finalize-second.tsv");
        PlanNode source = PlanNode.source(META, Arrays.asList(row("k1", "1"), row("k2", "2")));
        PlanNode partition = PlanNode.unary("partitionByHash", META, 0)
                .listParam("keys", Collections.singletonList("k"))
                .build();
        PlanNode firstSink = PlanNode.sink(
                META, 1, new FileSink(META, firstTarget.toString())).build();
        PlanNode secondSink = PlanNode.sink(
                META, 1, new FinalizeOnceFailingFileOutput(META, secondTarget.toString())).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, partition, firstSink, secondSink), 2);

        File w0 = tmp.resolve("finalize-w0").toFile();
        File w1 = tmp.resolve("finalize-w1").toFile();
        assertTrue(w0.mkdirs());
        assertTrue(w1.mkdirs());
        try (MasterServer master = new MasterServer(2000);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "finalize-w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "finalize-w1", 32, "127.0.0.1", master.port(), w1)) {
            long rows = new RealClusterJobClient(
                    "127.0.0.1", master.port()).submitToSink(graph);

            assertEquals(4L, rows, "the rowCount of both sinks should add up");
            assertEquals(0, worker0.preparedOutputActiveCount());
            assertEquals(0, worker1.preparedOutputActiveCount());
        }
        assertEquals(2, FileSink.read(META, firstTarget.toString()).size());
        assertEquals(2, FileSink.read(META, secondTarget.toString()).size());
    }

    @Test
    void submitWithNoWorkerFailsFast() {
        try (MasterServer master = new MasterServer(2000)) {
            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", master.port());
            PlanNode src = PlanNode.source(META, Collections.emptyList());
            PlanNode sink = PlanNode.sink(META, 0, new io.rowweave.DiscardSink(META)).build();
            PlanGraph g = new PlanGraph(Arrays.asList(src, sink));
            assertThrows(RuntimeException.class, () -> client.submitToSink(g), "with no worker the submit must fail fast instead of hanging");
        }
    }

    private static String causeMessages(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current.getMessage() != null) {
                messages.append(current.getMessage()).append('\n');
            }
        }
        return messages.toString();
    }

    private static final class SignallingFileOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final FileSink delegate;
        private final String marker;

        private SignallingFileOutput(DataSchema meta, String target, String marker) {
            this.delegate = new FileSink(meta, target);
            this.marker = marker;
        }

        @Override public DataSchema getSchema() { return delegate.getSchema(); }
        @Override public void collect(DataRow row) { delegate.collect(row); }
        @Override public void close() { delegate.close(); signal(); }
        @Override public void abort() { delegate.abort(); }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { delegate.prepareCommit(); signal(); }
        @Override public void commitPrepared() { delegate.commitPrepared(); }
        @Override public void rollbackCommit() { delegate.rollbackCommit(); }
        @Override public void finalizeCommit() { delegate.finalizeCommit(); }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public String exclusiveDestinationId() { return delegate.exclusiveDestinationId(); }

        private void signal() {
            try {
                Files.write(Paths.get(marker), new byte[]{1});
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
    }

    private static final class WaitForMarkerShortRowMap implements MapFunction {
        private static final long serialVersionUID = 1L;
        private final String marker;

        private WaitForMarkerShortRowMap(String marker) {
            this.marker = marker;
        }

        @Override
        public DataRow map(DataRow input) {
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10L);
            while (!Files.exists(Paths.get(marker))) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("timed out waiting for first output terminal marker");
                }
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(interrupted);
                }
            }
            DataRow shortRow = new DataRow(1);
            shortRow.set(0, input.get(0));
            return shortRow;
        }
    }

    private static final class CommitFailingFileOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final FileSink delegate;

        private CommitFailingFileOutput(DataSchema meta, String target) {
            this.delegate = new FileSink(meta, target);
        }

        @Override public DataSchema getSchema() { return delegate.getSchema(); }
        @Override public void collect(DataRow row) { delegate.collect(row); }
        @Override public void close() { delegate.close(); }
        @Override public void abort() { delegate.abort(); }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { delegate.prepareCommit(); }
        @Override public void commitPrepared() {
            throw new IllegalStateException("injected cluster publish failure");
        }
        @Override public void rollbackCommit() { delegate.rollbackCommit(); }
        @Override public void finalizeCommit() { delegate.finalizeCommit(); }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public String exclusiveDestinationId() { return delegate.exclusiveDestinationId(); }
    }

    private static final class FinalizeOnceFailingFileOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final FileSink delegate;
        private int failuresRemaining = 1;

        private FinalizeOnceFailingFileOutput(DataSchema meta, String target) {
            this.delegate = new FileSink(meta, target);
        }

        @Override public DataSchema getSchema() { return delegate.getSchema(); }
        @Override public void collect(DataRow row) { delegate.collect(row); }
        @Override public void close() { delegate.close(); }
        @Override public void abort() { delegate.abort(); }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { delegate.prepareCommit(); }
        @Override public void commitPrepared() { delegate.commitPrepared(); }
        @Override public void rollbackCommit() { delegate.rollbackCommit(); }
        @Override public void finalizeCommit() {
            if (failuresRemaining-- > 0) {
                throw new IllegalStateException("injected transient finalize failure");
            }
            delegate.finalizeCommit();
        }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public String exclusiveDestinationId() { return delegate.exclusiveDestinationId(); }
    }
}

