package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.stream.api.StreamAccumulationMode;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamGenericWindowSpec;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.api.StreamWindowAssignerSpec;
import io.rowweave.stream.api.StreamWindowResourceSpec;
import io.rowweave.stream.api.StreamWindowTriggerSpec;
import io.rowweave.test.ForkedJvm;
import io.rowweave.portable.PortableDataType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** generic windows run at p=2 in independent Worker JVMs. */
@Tag("multiprocess")
class StreamProductGenericWindowDagDispatchTest {

    @Test
    void realClusterRunsParallelGenericWindowsAndCheckpoints(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("generic-window-source.port"), 5_000L);
            awaitFile(exchange.resolve("generic-window-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("generic-window-run-authority"),
                            1L, "generic-window-master-incarnation",
                            Arrays.asList(endpoint(exchange, "source"),
                                    endpoint(exchange, "sink")),
                            exchange.resolve("generic-window-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve(
                                         "generic-window-job-authority"),
                                 runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamJobHandle job = session(client).submit(
                        StreamSubmissionId.of("generic-window-real-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitLines(exchange.resolve("generic-window-result"),
                        2, 10_000L);
                assertEquals(Arrays.asList(
                                "A:0:100:1.0000000000",
                                "B:0:100:2.0000000000"),
                        canonicalLines(exchange.resolve(
                                "generic-window-result")));
                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("generic-window-real-checkpoint"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L));
                assertEquals(1L, job.progress().completedCheckpoints());
                job.cancel(StreamCommandId.of("generic-window-real-cancel"))
                        .awaitCompletion(5_000L);
                assertEquals(StreamJobStatus.CANCELED,
                        job.awaitTermination(5_000L));
            }
        } finally {
            Files.write(exchange.resolve("generic-window-shutdown"),
                    new byte[] {1});
            assertExit(source, exchange, "source");
            assertExit(sink, exchange, "sink");
        }
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "generic-window-real", "generic-window-real", client);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setParallelism(2);
        session.setMaxParallelism(2);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 16_384L, 32, 32,
                65_536L, 5, 64, 1_048_576L));
        StreamSourceSpec source = StreamSourceSpec.exactOrderedEventTime(
                StreamGenericWindowProductAdmissionTest.inputSchema(),
                StreamProductGenericWindowWorkerProcessFixture.SOURCE_PROVIDER,
                StreamProductGenericWindowWorkerProcessFixture.SOURCE_CONTRACT,
                new byte[] {1}, Collections.singletonList(
                new StreamSplitSpec(
                        StreamGenericWindowProductAdmissionTest
                                .digest("cluster-split"),
                        "sequence-v1", new byte[] {0})),
                StreamWatermarkPolicy.providerEvents(1_000L));
        DataStream keyed = session.fromSource(source).uid("source")
                .calc(identityCalc("adaptor", PortableDataType.STRING,
                                false, "event-time",
                                PortableDataType.TIMESTAMP, false,
                                "amount", PortableDataType.BIG_DECIMAL, true),
                        StreamGenericWindowProductAdmissionTest.inputSchema())
                .uid("pre-window-calc")
                .keyBy("adaptor").uid("key");
        keyed.genericWindow(StreamGenericWindowSpec.aggregate(
                        StreamWindowAssignerSpec.eventTimeTumbling(
                                "event-time", 100L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING, 0L,
                        StreamGenericWindowProductAdmissionTest.function(),
                        StreamGenericWindowProductAdmissionTest.outputSchema(),
                        Collections.singletonList("adaptor"),
                        new StreamStateSpec("window-state",
                                StreamStateSpec.Kind.KEYED_AGGREGATING,
                                "generic-window-state-v1", 1,
                                StreamStateSpec.TtlMode.NONE, 0L),
                        StreamWindowResourceSpec.of(8, 1_000L,
                                8_192L, 32_768L, 8,
                                StreamEmissionLimit.of(1, 4_096L))))
                .uid("window")
                .calc(identityCalc("adaptor", PortableDataType.STRING,
                                false, "window-start",
                                PortableDataType.LONG, false,
                                "window-end", PortableDataType.LONG, false,
                                "total", PortableDataType.BIG_DECIMAL, false),
                        StreamGenericWindowProductAdmissionTest.outputSchema())
                .uid("post-window-calc").sinkTo("sink",
                        StreamSinkSpec.eagerAtLeastOnce(
                                StreamProductGenericWindowWorkerProcessFixture
                                        .SINK_PROVIDER,
                                StreamProductGenericWindowWorkerProcessFixture
                                        .SINK_CONTRACT,
                                new byte[] {3}));
        return session;
    }

    private static StreamCalcSpec identityCalc(Object... fields) {
        java.util.ArrayList<StreamCalcSpec.Projection> projections =
                new java.util.ArrayList<>();
        for (int index = 0; index < fields.length; index += 3) {
            String field = (String) fields[index];
            PortableDataType type = (PortableDataType) fields[index + 1];
            boolean nullable = (Boolean) fields[index + 2];
            projections.add(StreamCalcSpec.projection(field,
                    StreamExpression.input(field, type, nullable)));
        }
        return StreamCalcSpec.of(null, projections);
    }

    private static Process launch(String role, Path exchange)
            throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductGenericWindowWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "generic-window-" + role + ".log").toFile())
                .start();
    }

    private static RemoteStreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                Integer.parseInt(text(exchange.resolve(
                        "generic-window-" + role + ".port")).trim()));
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

    private static void awaitLines(Path file, int count, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (Files.exists(file)
                    && Files.readAllLines(file, StandardCharsets.UTF_8).size()
                    >= count) return;
            Thread.sleep(5L);
        }
        assertEquals(count, Files.exists(file)
                ? Files.readAllLines(file, StandardCharsets.UTF_8).size() : 0);
    }

    private static List<String> canonicalLines(Path file) throws Exception {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        Collections.sort(lines);
        return lines;
    }

    private static void assertExit(Process process, Path exchange,
            String role) throws Exception {
        boolean exited = process.waitFor(5L, TimeUnit.SECONDS);
        process.destroyForcibly();
        assertTrue(exited, () -> log(exchange, role));
        assertEquals(0, process.exitValue(), () -> log(exchange, role));
    }

    private static String log(Path exchange, String role) {
        try {
            return text(exchange.resolve(
                    "generic-window-" + role + ".log"));
        } catch (Exception ignored) {
            return "missing " + role + " process log";
        }
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }
}
