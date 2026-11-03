package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamTemporalJoinSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.test.ForkedJvm;
import io.rowweave.portable.PortableDataType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public finite temporal join executes on a real M+2W cluster. */
@Tag("multiprocess")
class StreamProductTemporalJoinDagDispatchTest {

    @Test
    void keyPreservingCalcPropagatesTemporalUpsertTombstone() {
        StreamTestGraph admitted =
                StreamTestGraph.from(
                        session(null).toPlanArtifact());
        RecordingOutput output = new RecordingOutput();
        Map<String, StreamChannelOutput> outputs =
                Collections.singletonMap("sink", output);
        StreamStatelessDagOutput graph = new StreamStatelessDagOutput(
                admitted.executionGraph, outputs, reference -> {
                    throw new UnsupportedOperationException(
                            "identity calc has no function deployment");
                });
        graph.start();
        byte[] stableKey = new byte[] {1, 2, 3};
        StreamChannelOutput input = graph.input(
                "temporal-join", output.identity);
        input.send(StreamEnvelope.record(output.identity, 1L,
                StreamUpsertRecordCodec.encode(
                        StreamUpsertRecord.tombstone(stableKey))));

        assertEquals(1, output.events.size());
        StreamUpsertRecord propagated = StreamUpsertRecordCodec.decode(
                output.events.get(0).recordFrame(), 8_192, 8_192);
        assertFalse(propagated.hasAfterImage());
        assertArrayEquals(stableKey, propagated.stableKeyFrame());
        graph.cancel();
    }

    @Test
    void publicTemporalJoinFansInTwoSourcesAndEmitsCanonicalUpserts(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start(exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("temporal-submit"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("temporal-result"), 5_000L);
            assertEquals("A=3.0000000001\nA=4.0000000001\n",
                    text(exchange.resolve("temporal-result")));
            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("temporal-checkpoint"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(1L, job.progress().completedCheckpoints());
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("temporal-cancel"))
                    .awaitCompletion(5_000L);
            assertEquals(StreamJobStatus.CANCELED,
                    job.awaitTermination(5_000L));
        } finally {
            cluster.close();
        }
    }

    @Test
    void temporalJoinStateSurvivesLiveScaleUpAndDown(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start(exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("temporal-live-rescale"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("temporal-result"), 5_000L);

            Files.write(exchange.resolve("temporal-delay-bootstrap"),
                    new byte[] {1});
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                                    "temporal-live-scale-up"), 2)
                            .awaitCompletion(10_000L));
            awaitFile(exchange.resolve("temporal-bootstrap-delayed"),
                    5_000L);
            StreamCheckpointHandle afterScaleUp = job.triggerCheckpoint(
                    StreamCommandId.of("temporal-after-scale-up"));
            java.util.concurrent.CompletableFuture<Void> releaseBootstrap =
                    java.util.concurrent.CompletableFuture.runAsync(() -> {
                        try {
                            Thread.sleep(1_000L);
                            Files.write(exchange.resolve(
                                    "temporal-release-bootstrap"),
                                    new byte[] {1});
                        } catch (Exception failure) {
                            throw new IllegalStateException(failure);
                        }
                    });
            try {
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        afterScaleUp.awaitCompletion(5_000L));
            } finally {
                Files.write(exchange.resolve("temporal-release-bootstrap"),
                        new byte[] {1});
            }
            releaseBootstrap.join();
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.rescale(StreamCommandId.of(
                                    "temporal-live-scale-down"), 1)
                            .awaitCompletion(10_000L));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                                    "temporal-after-scale-down"))
                            .awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("temporal-rescale-cancel"))
                    .awaitCompletion(5_000L);
        } finally {
            cluster.close();
        }
    }

    @Test
    void oneTemporalEvaluationFeedsTwoIsolatedSinkWorkersAndOneCheckpoint(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start(exchange, true);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client, true).submit(
                    StreamSubmissionId.of("temporal-multi-sink-submit"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("temporal-result"), 5_000L);
            awaitFile(exchange.resolve("temporal-second-result"), 5_000L);
            String oracle = "A=3.0000000001\nA=4.0000000001\n";
            assertEquals(oracle, text(exchange.resolve("temporal-result")));
            assertEquals(oracle,
                    text(exchange.resolve("temporal-second-result")));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    job.triggerCheckpoint(StreamCommandId.of(
                                    "temporal-multi-sink-checkpoint"))
                            .awaitCompletion(5_000L));
            assertEquals(1L, job.progress().completedCheckpoints());
            job.cancel(StreamCommandId.of("temporal-multi-sink-cancel"))
                    .awaitCompletion(5_000L);
            assertEquals(StreamJobStatus.CANCELED,
                    job.awaitTermination(5_000L));
        } finally {
            cluster.close();
        }
    }

    @Test
    void parallelTemporalJoinRestoresEverySubtaskFromOneSavepoint(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start(exchange, true);
        try (StreamJobClient client = cluster.client()) {
            StreamSession definition = session(client, false, 2);
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("parallel-temporal-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            awaitFile(exchange.resolve("temporal-result"), 5_000L);
            StreamSavepointHandle savepoint = original.stopWithSavepoint(
                    StreamCommandId.of("parallel-temporal-savepoint"),
                    new StreamSavepointRequest(
                            "file-savepoint-v1", new byte[0]));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    savepoint.awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                    original.awaitTermination(5_000L));

            StreamJobHandle restored = client.restore(
                    StreamSubmissionId.of("parallel-temporal-restored"),
                    artifact, new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.savepoint(
                                    savepoint.savepointId()), 2, 128));
            assertEquals(StreamJobStatus.RUNNING,
                    restored.awaitRunning(5_000L));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    restored.triggerCheckpoint(StreamCommandId.of(
                            "parallel-temporal-restored-checkpoint"))
                            .awaitCompletion(5_000L));
            restored.cancel(StreamCommandId.of(
                    "parallel-temporal-restored-cancel"))
                    .awaitCompletion(5_000L);
            assertEquals(StreamJobStatus.CANCELED,
                    restored.awaitTermination(5_000L));
        } finally {
            cluster.close();
        }
    }

    private static StreamSession session(StreamJobClient client) {
        return session(client, false);
    }

    private static StreamSession session(
            StreamJobClient client, boolean secondSink) {
        return session(client, secondSink, 1);
    }

    private static StreamSession session(
            StreamJobClient client, boolean secondSink, int parallelism) {
        StreamSession session = client == null
                ? RowWeave.createStreamSession("temporal-join")
                : RowWeave.createStreamSession(
                        "temporal-join", "temporal-join", client);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setParallelism(parallelism);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 64 * 1024L, 0, 0,
                64 * 1024L, 8, 256, 1_048_576L));
        DataStream left = session.fromSource(source(
                        StreamProductTemporalJoinWorkerProcessFixture.LEFT_SOURCE,
                        StreamProductTemporalJoinWorkerProcessFixture
                                .LEFT_CONTRACT, "left"))
                .uid("left-source")
                .calc(identityCalc(), schema()).uid("left-calc")
                .keyBy("adaptor").uid("left-key");
        DataStream right = session.fromSource(source(
                        StreamProductTemporalJoinWorkerProcessFixture.RIGHT_SOURCE,
                        StreamProductTemporalJoinWorkerProcessFixture
                                .RIGHT_CONTRACT, "right"))
                .uid("right-source")
                .calc(identityCalc(), schema()).uid("right-calc")
                .keyBy("adaptor").uid("right-key");
        DataStream joined = left.temporalJoin(right,
                        new StreamTemporalJoinSpec("adaptor", "adaptor",
                                -10L, 20L,
                                Collections.singletonList("adaptor")),
                        StreamProductTemporalJoinWorkerProcessFixture.function(),
                        schema(), state("left-state"), state("right-state"))
                .uid("temporal-join");
        DataStream projected = joined.calc(identityCalc(), schema())
                .uid("post-join-calc");
        projected.sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        StreamProductTemporalJoinWorkerProcessFixture.SINK,
                        StreamProductTemporalJoinWorkerProcessFixture
                                .SINK_CONTRACT, new byte[] {3}));
        if (secondSink) {
            projected.sinkTo("second-sink", StreamSinkSpec.eagerAtLeastOnce(
                    StreamProductTemporalJoinWorkerProcessFixture.SECOND_SINK,
                    StreamProductTemporalJoinWorkerProcessFixture
                            .SECOND_SINK_CONTRACT, new byte[] {4}));
        }
        return session;
    }

    private static StreamSourceSpec source(
            String provider, String contract, String side) {
        return StreamSourceSpec.exactOrderedEventTime(
                schema(), provider, contract, new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        StreamProductTemporalJoinWorkerProcessFixture
                                .digest(side + "-split"),
                        "sequence-v1", new byte[] {2})),
                StreamWatermarkPolicy.providerEvents(30_000L));
    }

    private static StreamStateSpec state(String name) {
        return new StreamStateSpec(name, StreamStateSpec.Kind.KEYED_MAP,
                "temporal-record-v1", 1,
                StreamStateSpec.TtlMode.EVENT_TIME, 30L);
    }

    static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, false)));
    }

    private static StreamCalcSpec identityCalc() {
        return StreamCalcSpec.of(null, Arrays.asList(
                StreamCalcSpec.projection("adaptor", StreamExpression.input(
                        "adaptor", PortableDataType.STRING, false)),
                StreamCalcSpec.projection("amount", StreamExpression.input(
                        "amount", PortableDataType.BIG_DECIMAL, false))));
    }

    private static RunningCluster start(Path exchange) throws Exception {
        return start(exchange, false);
    }

    private static RunningCluster start(
            Path exchange, boolean secondSink) throws Exception {
        List<String> roles = new ArrayList<>(Arrays.asList("source", "sink"));
        if (secondSink) roles.add("sink-second");
        List<Process> processes = new ArrayList<>();
        for (String role : roles) processes.add(launch(role, exchange));
        awaitFile(exchange.resolve("temporal-source.port"), 5_000L);
        awaitFile(exchange.resolve("temporal-sink.port"), 5_000L);
        if (secondSink) {
            awaitFile(exchange.resolve("temporal-sink-second.port"), 5_000L);
        }
        List<RemoteStreamWorkerEndpoint> endpoints = new ArrayList<>();
        for (String role : roles) endpoints.add(endpoint(exchange, role));
        RemoteStreamSourceSinkRunFactory runs =
                new RemoteStreamSourceSinkRunFactory(
                        exchange.resolve("temporal-run-authority"), 1L,
                        "temporal-master-incarnation", endpoints,
                        exchange.resolve("temporal-checkpoint-store"));
        return new RunningCluster(exchange, roles, processes,
                new StreamProductJobMasterServer(
                        exchange.resolve("temporal-job-authority"), runs));
    }

    private static Process launch(String role, Path exchange) throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductTemporalJoinWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "temporal-" + role + ".log").toFile()).start();
    }

    private static RemoteStreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                Integer.parseInt(text(exchange.resolve(
                        "temporal-" + role + ".port"))));
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

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static final class RecordingOutput implements StreamChannelOutput {
        private final StreamChannelIdentity identity =
                new StreamChannelIdentity(
                        "run", "attempt", 1L, 1L,
                        "temporal-upsert-output", 1L);
        private final List<StreamEnvelope> events = new ArrayList<>();

        @Override public StreamChannelIdentity channelIdentity() {
            return identity;
        }

        @Override public void send(StreamEnvelope envelope) {
            events.add(envelope);
        }

        @Override public void close() { }
        @Override public void cancel() { }
        @Override public void fail(Throwable cause) {
            throw new AssertionError(cause);
        }
    }

    private static final class RunningCluster implements AutoCloseable {
        private final Path exchange;
        private final List<String> roles;
        private final List<Process> processes;
        private final StreamProductJobMasterServer master;

        private RunningCluster(Path exchange, List<String> roles,
                List<Process> processes,
                StreamProductJobMasterServer master) {
            this.exchange = exchange;
            this.roles = roles;
            this.processes = processes;
            this.master = master;
        }

        StreamJobClient client() {
            return RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", master.port());
        }

        @Override public void close() throws Exception {
            master.close();
            Files.write(exchange.resolve("temporal-shutdown"), new byte[] {1});
            for (int index = 0; index < processes.size(); index++) {
                assertExit(processes.get(index), roles.get(index));
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
                return text(exchange.resolve("temporal-" + role + ".log"));
            } catch (Exception ignored) {
                return "missing process log";
            }
        }
    }
}
