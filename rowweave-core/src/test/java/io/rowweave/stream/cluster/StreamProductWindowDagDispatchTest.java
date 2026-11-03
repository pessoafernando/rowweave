package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.api.StreamWindowSpec;
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

/** event-time managed window is part of the public cluster DAG. */
@Tag("multiprocess")
class StreamProductWindowDagDispatchTest {

    @Test
    void publicWindowKeepsFullPrecisionUntilOneFinalDecimalRounding(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("normal", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("window-decimal-submit"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("window-result"), 10_000L);
            assertEquals("A:0:1000:3.0000000001",
                    text(exchange.resolve("window-result")));
            assertEquals(StreamJobStatus.RUNNING, job.status(),
                    "watermark propagation must not poison the eager Sink");
        } finally {
            cluster.close();
        }
    }

    @Test
    void checkpointIncludesPendingWindowStateBeforeForwardingItsBarrier(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("checkpoint", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client, 15_000L).submit(
                    StreamSubmissionId.of("window-checkpoint-submit"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            awaitFile(exchange.resolve("window-before-checkpoint"), 5_000L);
            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("window-checkpoint-command"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            StreamCheckpointHandle nextCheckpoint = job.triggerCheckpoint(
                    StreamCommandId.of("window-checkpoint-command-next"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    nextCheckpoint.awaitCompletion(5_000L));
            assertEquals(2L, job.progress().completedCheckpoints());
            Files.write(exchange.resolve("window-release-watermark"),
                    new byte[] {1});
            awaitFile(exchange.resolve("window-result"), 10_000L);
            assertEquals("A:0:1000:3.0000000001",
                    text(exchange.resolve("window-result")));
        } finally {
            cluster.close();
        }
    }

    @Test
    void lateRecordAfterFinalWatermarkFailsThePublicJobClosed(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("late", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("window-late-submit"));
            assertEquals(StreamJobStatus.FAILED,
                    job.awaitTermination(10_000L));
        } finally {
            cluster.close();
        }
    }

    @Test
    void activeEventTimeSourceWithoutProgressFailsWithinItsIdleTimeout(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("stall", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client, 500L).submit(
                    StreamSubmissionId.of("window-idle-timeout-submit"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            assertEquals(StreamJobStatus.FAILED,
                    job.awaitTermination(10_000L));
        } finally {
            cluster.close();
        }
    }

    @Test
    void explicitIdleStatusSuspendsTheSourceWatchdog(
            @TempDir Path exchange) throws Exception {
        RunningCluster cluster = start("idle", exchange);
        try (StreamJobClient client = cluster.client()) {
            StreamJobHandle job = session(client, 500L).submit(
                    StreamSubmissionId.of("window-explicit-idle-submit"));
            assertEquals(StreamJobStatus.RUNNING, job.awaitRunning(5_000L));
            Thread.sleep(800L);
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("cancel-explicit-idle"))
                    .awaitCompletion(5_000L);
        } finally {
            cluster.close();
        }
    }

    private static StreamSession session(StreamJobClient client) {
        return session(client, 1_000L);
    }

    private static StreamSession session(
            StreamJobClient client, long idleTimeoutMillis) {
        StreamSession session = RowWeave.createStreamSession(
                "event-window", "event-window", client);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 4_096L, 16, 16,
                16_384L, 5, 32, 1_048_576L));
        DataStream keyed = session.fromSource(source(idleTimeoutMillis))
                .uid("source")
                .calc(inputIdentityCalc(), inputSchema())
                .uid("pre-window-calc")
                .keyBy("adaptor").uid("adaptor-key");
        keyed.eventTimeDecimalWindow(
                        StreamWindowSpec.tumblingDecimal(
                                "event-time", "amount", 1_000L,
                                "window-start", "window-end", "sum"),
                        outputSchema(), new StreamStateSpec(
                                "adaptor-window", StreamStateSpec.Kind.KEYED_VALUE,
                                StreamProductWindowWorkerProcessFixture
                                        .stateSerializer(),
                                1, StreamStateSpec.TtlMode.NONE, 0L))
                .uid("adaptor-window")
                .calc(outputIdentityCalc(), outputSchema())
                .uid("post-window-calc")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        StreamProductWindowWorkerProcessFixture.SINK_PROVIDER,
                        StreamProductWindowWorkerProcessFixture.SINK_CONTRACT,
                        new byte[] {3}));
        return session;
    }

    private static StreamCalcSpec inputIdentityCalc() {
        return StreamCalcSpec.of(null, Arrays.asList(
                StreamCalcSpec.projection("adaptor",
                        StreamExpression.input("adaptor",
                                PortableDataType.STRING, false)),
                StreamCalcSpec.projection("event-time",
                        StreamExpression.input("event-time",
                                PortableDataType.TIMESTAMP, false)),
                StreamCalcSpec.projection("amount",
                        StreamExpression.input("amount",
                                PortableDataType.BIG_DECIMAL, true))));
    }

    private static StreamCalcSpec outputIdentityCalc() {
        return StreamCalcSpec.of(null, Arrays.asList(
                StreamCalcSpec.projection("adaptor",
                        StreamExpression.input("adaptor",
                                PortableDataType.STRING, false)),
                StreamCalcSpec.projection("window-start",
                        StreamExpression.input("window-start",
                                PortableDataType.LONG, false)),
                StreamCalcSpec.projection("window-end",
                        StreamExpression.input("window-end",
                                PortableDataType.LONG, false)),
                StreamCalcSpec.projection("sum",
                        StreamExpression.input("sum",
                                PortableDataType.BIG_DECIMAL, false))));
    }

    private static StreamSourceSpec source(long idleTimeoutMillis) {
        return StreamSourceSpec.exactOrderedEventTime(
                inputSchema(),
                StreamProductWindowWorkerProcessFixture.SOURCE_PROVIDER,
                StreamProductWindowWorkerProcessFixture.SOURCE_CONTRACT,
                new byte[] {1}, Collections.singletonList(new StreamSplitSpec(
                        StreamProductWindowWorkerProcessFixture.digest("window-split"),
                        "sequence-v1", new byte[] {2})),
                StreamWatermarkPolicy.providerEvents(idleTimeoutMillis));
    }

    private static StreamSchema inputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("event-time", "eventTime", "eventTime",
                        DataType.TimestampType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    private static StreamSchema outputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("window-start", "windowStart", "windowStart",
                        DataType.LongType, false),
                new StreamField("window-end", "windowEnd", "windowEnd",
                        DataType.LongType, false),
                new StreamField("sum", "sum", "sum",
                        DataType.BigDecimalType, false)));
    }

    private static RunningCluster start(String mode, Path exchange)
            throws Exception {
        Process source = launch("source", mode, exchange);
        Process sink = launch("sink", mode, exchange);
        awaitFile(exchange.resolve("window-source.port"), 5_000L);
        awaitFile(exchange.resolve("window-sink.port"), 5_000L);
        RemoteStreamSourceSinkRunFactory runs =
                new RemoteStreamSourceSinkRunFactory(
                        exchange.resolve("window-run-authority"), 1L,
                        "window-master-incarnation", Arrays.asList(
                        endpoint(exchange, "source"),
                        endpoint(exchange, "sink")),
                        exchange.resolve("window-checkpoint-store"));
        StreamProductJobMasterServer master = new StreamProductJobMasterServer(
                exchange.resolve("window-job-authority"), runs);
        return new RunningCluster(exchange, source, sink, master);
    }

    private static Process launch(String role, String mode, Path exchange)
            throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductWindowWorkerProcessFixture.class.getName(),
                role, mode, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "window-" + role + ".log").toFile()).start();
    }

    private static RemoteStreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                Integer.parseInt(text(exchange.resolve(
                        "window-" + role + ".port"))));
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

    private static final class RunningCluster implements AutoCloseable {
        private final Path exchange;
        private final Process source;
        private final Process sink;
        private final StreamProductJobMasterServer master;

        private RunningCluster(Path exchange, Process source, Process sink,
                StreamProductJobMasterServer master) {
            this.exchange = exchange;
            this.source = source;
            this.sink = sink;
            this.master = master;
        }

        StreamJobClient client() {
            return RowWeave.createClusterStreamJobClient(
                    "127.0.0.1", master.port());
        }

        @Override public void close() throws Exception {
            master.close();
            Files.write(exchange.resolve("window-shutdown"), new byte[] {1});
            assertExit(source, "source");
            assertExit(sink, "sink");
        }

        private void assertExit(Process process, String role) throws Exception {
            boolean exited = process.waitFor(5L, TimeUnit.SECONDS);
            process.destroyForcibly();
            assertTrue(exited, () -> log(role));
            assertEquals(0, process.exitValue(), () -> log(role));
        }

        private String log(String role) {
            try {
                return text(exchange.resolve("window-" + role + ".log"));
            } catch (Exception ignored) {
                return "missing process log";
            }
        }
    }
}
