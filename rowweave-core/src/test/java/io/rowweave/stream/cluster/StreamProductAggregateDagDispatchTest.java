package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobClientRecoveryContract;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.runtime.FileStreamSavepointStore;
import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** keyed aggregate is a managed UPSERT operator in a real cluster. */
@Tag("multiprocess")
class StreamProductAggregateDagDispatchTest {

    @Test
    void retractAggregateCompensatesOnRealWorkersAndCheckpointsState(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("retract", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client, "retract").submit(
                    StreamSubmissionId.of("aggregate-retract-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("aggregate-result"), 5_000L);
            assertEquals("INSERT:A=1.0000000001\n"
                            + "UPDATE_BEFORE:A=1.0000000001\n"
                            + "UPDATE_AFTER:A=3.0000000001\n"
                            + "UPDATE_BEFORE:A=3.0000000001\n"
                            + "UPDATE_AFTER:A=1.0000000001\n"
                            + "INSERT:B=4.2000000000\n",
                    text(exchange.resolve("aggregate-result")));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                                    "aggregate-retract-checkpoint"))
                            .awaitCompletion(5_000L));
            job.cancel(StreamCommandId.of("aggregate-retract-cancel"))
                    .awaitCompletion(5_000L);
            assertEquals(StreamJobStatus.CANCELED,
                    job.awaitTermination(5_000L));
        } finally {
            cluster.close();
        }
    }

    @Test
    void publicAggregateEmitsCanonicalUpsertsAndCheckpointsItsState(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("normal", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client, "normal").submit(
                    StreamSubmissionId.of("aggregate-submit"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("aggregate-result"), 5_000L);
            assertEquals("A=1.0000000001\nA=3.0000000001\nB=4.2000000000\n",
                    text(exchange.resolve("aggregate-result")));
            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("aggregate-checkpoint"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            StreamCheckpointHandle nextCheckpoint = job.triggerCheckpoint(
                    StreamCommandId.of("aggregate-checkpoint-next"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    nextCheckpoint.awaitCompletion(5_000L));
            assertEquals(2L, job.progress().completedCheckpoints());
            FileStreamSavepointStore retained = new FileStreamSavepointStore(
                    exchange.resolve("aggregate-checkpoint-store"));
            assertEquals(null, retained.get(checkpoint.checkpointId()),
                    "default retention must remove the older checkpoint cut");
            assertTrue(retained.get(nextCheckpoint.checkpointId()) != null);
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("aggregate-cancel"))
                    .awaitCompletion(5_000L);
            assertEquals(StreamJobStatus.CANCELED,
                    job.awaitTermination(5_000L));
        } finally {
            cluster.close();
        }
    }

    @Test
    void processingTtlResetsKeysAndReclaimsExpiredStateUnderSpillPressure(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("ttl-pressure", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client, "ttl-pressure").submit(
                    StreamSubmissionId.of("aggregate-ttl-submit"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("aggregate-result"), 5_000L);
            assertEquals("A=1.0000000001\nB=1.0000000000\n"
                            + "C=1.0000000000\nA=2.0000000001\n"
                            + "D=1.0000000000\nE=1.0000000000\n",
                    text(exchange.resolve("aggregate-result")));
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("aggregate-ttl-cancel"))
                    .awaitCompletion(5_000L);
        } finally {
            cluster.close();
        }
    }

    @Test
    void stalledRemoteSinkBackpressuresAggregateAndSourceWithinCredits(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("backpressure", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client, "backpressure").submit(
                    StreamSubmissionId.of("aggregate-backpressure-submit"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("aggregate-sink-stalled"), 5_000L);
            Thread.sleep(250L);
            int produced = Integer.parseInt(text(
                    exchange.resolve("aggregate-source-count")));
            // Four physical node-to-node lanes each hold at most eight events;
            // the five task bodies may each own one event while blocked.
            assertTrue(produced <= 37,
                    "bounded task-per-node credits allowed " + produced
                            + " source records while the Sink was stalled");
            Files.write(exchange.resolve("aggregate-release-sink"),
                    new byte[] {1});
            awaitFile(exchange.resolve("aggregate-result"), 10_000L);
            assertEquals("1000:1000.0000000000",
                    text(exchange.resolve("aggregate-result")));
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("aggregate-backpressure-cancel"))
                    .awaitCompletion(5_000L);
        } finally {
            cluster.close();
        }
    }

    @Test
    void realWorkersRestoreAnOfflineRescaledAggregate(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("rescale", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobClientRecoveryContract.verifySavepointOfflineRescale(
                    client, session(client, "rescale", 1),
                    session(client, "rescale", 2), "real-rescale", 2, 2,
                    () -> {
                        awaitFile(exchange.resolve("aggregate-result"), 5_000L);
                        assertEquals("A=1.0000000000\nB=2.0000000000\n",
                                text(exchange.resolve("aggregate-result")));
                    },
                    () -> Files.delete(exchange.resolve("aggregate-result")),
                    () -> {
                        awaitFile(exchange.resolve("aggregate-result"), 5_000L);
                        assertEquals("A=4.0000000000\nB=6.0000000000",
                                canonicalLines(text(exchange.resolve(
                                        "aggregate-result"))));
                    });
        } finally {
            cluster.close();
        }
    }

    @Test
    void realWorkersRestoreANewJobFromExactCheckpoint(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("restore", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobClientRecoveryContract.verifyExactCheckpointRestore(
                    client, session(client, "restore", 1), "real-exact",
                    1, 128,
                    () -> {
                        awaitFile(exchange.resolve("aggregate-result"), 5_000L);
                        assertEquals("A=1.0000000000\nB=2.0000000000\n",
                                text(exchange.resolve("aggregate-result")));
                    },
                    () -> Files.delete(exchange.resolve("aggregate-result")),
                    () -> {
                        awaitFile(exchange.resolve("aggregate-result"), 5_000L);
                        assertEquals("A=4.0000000000\nB=6.0000000000",
                                canonicalLines(text(exchange.resolve(
                                        "aggregate-result"))));
                    });
        } finally {
            cluster.close();
        }
    }

    @Test
    void sourceWorkerLossRestartsWholeGraphFromLatestCompletedCheckpoint(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = startWorkerLoss(exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamSession definition = session(client, "worker-loss");
            definition.setCheckpointConfig(
                    StreamCheckpointConfig.manualOnlyUnaligned(
                            5_000L, 2));
            StreamJobHandle job = definition.submit(
                    StreamSubmissionId.of("aggregate-worker-loss-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("aggregate-result"), 5_000L);
            assertEquals("A=1.0000000000\nB=2.0000000000\n",
                    text(exchange.resolve("aggregate-result")));
            StreamCheckpointHandle beforeLoss = job.triggerCheckpoint(
                    StreamCommandId.of("aggregate-before-worker-loss"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    beforeLoss.awaitCompletion(5_000L));
            assertEquals(4L, new FileStreamSavepointStore(
                    exchange.resolve("aggregate-checkpoint-store"))
                    .get(beforeLoss.checkpointId()).checkpointManifest()
                    .taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(evidence -> evidence.kind()
                            == CheckpointEvidence.Kind.CHANNEL_STATE)
                    .count());

            Files.delete(exchange.resolve("aggregate-result"));
            cluster.killPrimarySource();
            awaitRestart(job, 10_000L);
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(10_000L));
            awaitFile(exchange.resolve("aggregate-result"), 10_000L);

            assertEquals(StreamJobStatus.RUNNING, job.status());
            assertEquals(1, job.progress().restarts());
            assertEquals("A=4.0000000000\nB=6.0000000000",
                    canonicalLines(text(
                            exchange.resolve("aggregate-result"))));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "aggregate-after-worker-loss"))
                            .awaitCompletion(5_000L));
            try (StreamJobClient replacementMaster =
                         cluster.replaceMasterAfterWorkerLoss()) {
                StreamJobHandle attached = replacementMaster.attach(job.id());
                assertEquals(StreamJobStatus.RUNNING,
                        attached.awaitRunning(5_000L));
                assertEquals(1, attached.progress().restarts());
                attached.cancel(StreamCommandId.of(
                                "aggregate-worker-loss-cancel"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            cluster.close();
        }
    }

    @Test
    void sinkWorkerLossRestartsSourceStateAndSinkAsOneAttempt(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = startSinkWorkerLoss(exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client, "worker-loss").submit(
                    StreamSubmissionId.of("aggregate-sink-loss-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("aggregate-result"), 5_000L);
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                            "aggregate-before-sink-loss"))
                            .awaitCompletion(5_000L));

            Files.delete(exchange.resolve("aggregate-result"));
            cluster.killPrimarySink();
            awaitRestart(job, 10_000L);
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(10_000L));
            awaitFile(exchange.resolve("aggregate-result"), 10_000L);
            assertEquals("A=4.0000000000\nB=6.0000000000",
                    canonicalLines(text(
                            exchange.resolve("aggregate-result"))));
            job.cancel(StreamCommandId.of("aggregate-sink-loss-cancel"))
                    .awaitCompletion(5_000L);
        } finally {
            cluster.close();
        }
    }

    @Test
    void workerLossWithoutACompletedCheckpointFailsClosed(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = startWorkerLoss(exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client, "worker-loss").submit(
                    StreamSubmissionId.of(
                            "aggregate-uncheckpointed-loss-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("aggregate-result"), 5_000L);

            cluster.killPrimarySource();

            assertEquals(StreamJobStatus.FAILED,
                    job.awaitTermination(10_000L));
            assertEquals(1, job.progress().restarts());
        } finally {
            cluster.close();
        }
    }

    private static StreamSession session(StreamJobClient client, String mode) {
        return session(client, mode, 1);
    }

    private static StreamSession session(
            StreamJobClient client, String mode, int parallelism) {
        boolean ttlPressure = "ttl-pressure".equals(mode);
        boolean retract = "retract".equals(mode);
        StreamSession session = RowWeave.createStreamSession(
                "keyed-aggregate", "keyed-aggregate", client);
        session.setChangelogMode(retract
                ? StreamChangelogMode.RETRACT : StreamChangelogMode.UPSERT);
        session.setParallelism(parallelism);
        if ("rescale".equals(mode)) session.setMaxParallelism(2);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 8_192L, 0, 0,
                32_768L, 5, 32, ttlPressure ? 200L : 1_048_576L));
        StreamSourceSpec source = retract
                ? StreamSourceSpec.retractExactOrdered(
                schema(), Collections.singletonList("adaptor"),
                StreamProductAggregateWorkerProcessFixture.SOURCE_PROVIDER,
                StreamProductAggregateWorkerProcessFixture.SOURCE_CONTRACT,
                new byte[] {1}, Collections.singletonList(
                new StreamSplitSpec(
                        StreamProductAggregateWorkerProcessFixture
                                .digest("aggregate-retract-split"),
                        "sequence-v2", new byte[] {2})))
                : StreamSourceSpec.exactOrdered(
                schema(), StreamProductAggregateWorkerProcessFixture
                        .SOURCE_PROVIDER,
                StreamProductAggregateWorkerProcessFixture.SOURCE_CONTRACT,
                new byte[] {1}, Collections.singletonList(
                new StreamSplitSpec(
                        StreamProductAggregateWorkerProcessFixture
                                .digest("aggregate-split"),
                        "sequence-v1", new byte[] {2})));
        DataStream keyed = session.fromSource(source)
                .uid("source")
                .calc(identityCalc(), schema()).uid("pre-aggregate-calc")
                .keyBy("adaptor").uid("adaptor-key");
        keyed.keyedAggregate(
                        retract
                                ? StreamProductAggregateWorkerProcessFixture
                                .retractFunction()
                                : StreamProductAggregateWorkerProcessFixture
                                .function(),
                        schema(), new StreamStateSpec(
                                "readout", StreamStateSpec.Kind.KEYED_AGGREGATING,
                                retract ? "aggregate-decimal-state-v2"
                                        : "aggregate-decimal-state-v1",
                                retract ? 2 : 1,
                                StreamStateSpec.TtlMode.PROCESSING_TIME,
                                ttlPressure ? 100L : 60_000L))
                .uid("readout-aggregate")
                .sinkTo("sink", retract
                        ? StreamSinkSpec.retractEagerAtLeastOnce(
                        StreamProductAggregateWorkerProcessFixture.SINK_PROVIDER,
                        StreamProductAggregateWorkerProcessFixture.SINK_CONTRACT,
                        new byte[] {3})
                        : StreamSinkSpec.eagerAtLeastOnce(
                        StreamProductAggregateWorkerProcessFixture.SINK_PROVIDER,
                        StreamProductAggregateWorkerProcessFixture.SINK_CONTRACT,
                        new byte[] {3}));
        return session;
    }

    private static StreamCalcSpec identityCalc() {
        return StreamCalcSpec.of(null, Arrays.asList(
                StreamCalcSpec.projection("adaptor",
                        StreamExpression.input("adaptor",
                                PortableDataType.STRING, false)),
                StreamCalcSpec.projection("amount",
                        StreamExpression.input("amount",
                                PortableDataType.BIG_DECIMAL, false))));
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, false)));
    }

    private static RunningCluster start(String mode, Path exchange)
            throws Exception {
        Process source = launch("source", mode, exchange);
        Process sink = launch("sink", mode, exchange);
        awaitFile(exchange.resolve("aggregate-source.port"), 5_000L);
        awaitFile(exchange.resolve("aggregate-sink.port"), 5_000L);
        RemoteStreamSourceSinkRunFactory runs =
                new RemoteStreamSourceSinkRunFactory(
                        exchange.resolve("aggregate-run-authority"), 1L,
                        "aggregate-master-incarnation", Arrays.asList(
                        endpoint(exchange, "source"), endpoint(exchange, "sink")),
                        exchange.resolve("aggregate-checkpoint-store"));
        return new RunningCluster(exchange, source, sink,
                new StreamProductJobMasterServer(
                        exchange.resolve("aggregate-job-authority"), runs));
    }

    private static RunningCluster startWorkerLoss(Path exchange)
            throws Exception {
        Process primary = launch("source-a", "worker-loss", exchange);
        Process replacement = launch("source-b", "worker-loss", exchange);
        Process sink = launch("sink", "worker-loss", exchange);
        awaitFile(exchange.resolve("aggregate-source-a.port"), 5_000L);
        awaitFile(exchange.resolve("aggregate-source-b.port"), 5_000L);
        awaitFile(exchange.resolve("aggregate-sink.port"), 5_000L);
        RemoteStreamSourceSinkRunFactory runs =
                new RemoteStreamSourceSinkRunFactory(
                        exchange.resolve("aggregate-run-authority"), 1L,
                        "aggregate-master-incarnation", Arrays.asList(
                        endpoint(exchange, "source-a"),
                        endpoint(exchange, "source-b"),
                        endpoint(exchange, "sink")),
                        exchange.resolve("aggregate-checkpoint-store"));
        return new RunningCluster(exchange, primary, replacement, sink,
                null,
                new StreamProductJobMasterServer(
                        exchange.resolve("aggregate-job-authority"), runs));
    }

    private static RunningCluster startSinkWorkerLoss(Path exchange)
            throws Exception {
        Process source = launch("source", "worker-loss", exchange);
        Process primary = launch("sink-a", "worker-loss", exchange);
        Process replacement = launch("sink-b", "worker-loss", exchange);
        awaitFile(exchange.resolve("aggregate-source.port"), 5_000L);
        awaitFile(exchange.resolve("aggregate-sink-a.port"), 5_000L);
        awaitFile(exchange.resolve("aggregate-sink-b.port"), 5_000L);
        RemoteStreamSourceSinkRunFactory runs =
                new RemoteStreamSourceSinkRunFactory(
                        exchange.resolve("aggregate-run-authority"), 1L,
                        "aggregate-master-incarnation", Arrays.asList(
                        endpoint(exchange, "source"),
                        endpoint(exchange, "sink-a"),
                        endpoint(exchange, "sink-b")),
                        exchange.resolve("aggregate-checkpoint-store"));
        return new RunningCluster(exchange, source, null, primary,
                replacement, new StreamProductJobMasterServer(
                        exchange.resolve("aggregate-job-authority"), runs));
    }

    private static Process launch(String role, String mode, Path exchange)
            throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductAggregateWorkerProcessFixture.class.getName(),
                role, mode, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "aggregate-" + role + ".log").toFile()).start();
    }

    private static RemoteStreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                Integer.parseInt(text(exchange.resolve(
                        "aggregate-" + role + ".port"))));
    }

    private static void awaitFile(Path file, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while ((!Files.exists(file) || Files.size(file) == 0L)
                && System.nanoTime() < deadline) Thread.sleep(5L);
        assertTrue(Files.exists(file) && Files.size(file) > 0L,
                "process did not publish " + file);
    }

    private static void awaitRestart(
            StreamJobHandle job, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (job.progress().restarts() == 0
                && !job.status().terminal()
                && System.nanoTime() < deadline) Thread.sleep(10L);
        assertEquals(1, job.progress().restarts(),
                () -> "job did not restart; status=" + job.status());
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static String canonicalLines(String value) {
        String[] lines = value.trim().split("\\n");
        Arrays.sort(lines);
        return String.join("\n", lines);
    }

    private static final class RunningCluster implements AutoCloseable {
        private final Path exchange;
        private final Process source;
        private final Process replacementSource;
        private final Process sink;
        private final Process replacementSink;
        private StreamProductJobMasterServer master;

        private RunningCluster(Path exchange, Process source, Process sink,
                StreamProductJobMasterServer master) {
            this(exchange, source, null, sink, master);
        }

        private RunningCluster(Path exchange, Process source,
                Process replacementSource, Process sink,
                StreamProductJobMasterServer master) {
            this(exchange, source, replacementSource, sink, null, master);
        }

        private RunningCluster(Path exchange, Process source,
                Process replacementSource, Process sink,
                Process replacementSink,
                StreamProductJobMasterServer master) {
            this.exchange = exchange;
            this.source = source;
            this.replacementSource = replacementSource;
            this.sink = sink;
            this.replacementSink = replacementSink;
            this.master = master;
        }

        StreamJobClient client() {
            return RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", master.port());
        }

        void killPrimarySource() throws Exception {
            source.destroyForcibly();
            assertTrue(source.waitFor(5L, TimeUnit.SECONDS),
                    "primary source Worker did not terminate");
        }

        void killPrimarySink() throws Exception {
            sink.destroyForcibly();
            assertTrue(sink.waitFor(5L, TimeUnit.SECONDS),
                    "primary Sink Worker did not terminate");
        }

        StreamJobClient replaceMasterAfterWorkerLoss() throws Exception {
            if (replacementSource == null) {
                throw new IllegalStateException(
                        "replacement source Worker is unavailable");
            }
            master.close();
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("aggregate-run-authority"), 2L,
                            "aggregate-master-replacement", Arrays.asList(
                            endpoint(exchange, "source-b"),
                            endpoint(exchange, "sink")),
                            exchange.resolve("aggregate-checkpoint-store"));
            master = new StreamProductJobMasterServer(
                    exchange.resolve("aggregate-job-authority"), runs);
            return client();
        }

        @Override public void close() throws Exception {
            master.close();
            Files.write(exchange.resolve("aggregate-shutdown"), new byte[] {1});
            if (source.isAlive()) assertExit(source,
                    replacementSource == null ? "source" : "source-a");
            if (replacementSource != null) {
                assertExit(replacementSource, "source-b");
            }
            if (sink.isAlive()) assertExit(sink,
                    replacementSink == null ? "sink" : "sink-a");
            if (replacementSink != null) {
                assertExit(replacementSink, "sink-b");
            }
        }

        private void assertExit(Process process, String role) throws Exception {
            boolean exited = process.waitFor(5L, TimeUnit.SECONDS);
            process.destroyForcibly();
            assertTrue(exited, () -> log(role));
            assertEquals(0, process.exitValue(), () -> log(role));
        }

        private String log(String role) {
            try {
                return text(exchange.resolve("aggregate-" + role + ".log"));
            } catch (Exception ignored) {
                return "missing process log";
            }
        }
    }
}
