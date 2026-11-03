package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** broadcast co-process runs in independent Worker JVMs. */
@Tag("multiprocess")
class StreamProductKeyedCoProcessDagDispatchTest {

    @Test
    void realClusterFansOutBroadcastAndCheckpointsCompletedSide(
            @TempDir Path exchange) throws Exception {
        Process source = launch("broadcast-source", exchange);
        Process sink = launch("broadcast-sink", exchange);
        try {
            awaitFile(exchange.resolve(
                    "stateless-broadcast-source.port"), 5_000L);
            awaitFile(exchange.resolve(
                    "stateless-broadcast-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("broadcast-run-authority"), 1L,
                            "broadcast-master-incarnation",
                            Arrays.asList(
                                    endpoint(exchange, "broadcast-source"),
                                    endpoint(exchange, "broadcast-sink")),
                            exchange.resolve("stateless-checkpoint-store"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("broadcast-job-authority"),
                                 runs);
                 StreamJobClient client = RowWeave.createClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamSession session =
                        StreamProductKeyedCoProcessPlanAdmissionTest
                                .broadcastSession(client, 2);
                StreamJobHandle job = session.submit(
                        StreamSubmissionId.of("broadcast-real-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("broadcast-result"), 10_000L);
                assertEquals("approved:gallery",
                        text(exchange.resolve("broadcast-result")));

                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("broadcast-real-checkpoint"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L),
                        () -> logs(exchange));
                assertEquals(1L, job.progress().completedCheckpoints());
                job.cancel(StreamCommandId.of("broadcast-real-cancel"))
                        .awaitCompletion(5_000L);
                assertEquals(StreamJobStatus.CANCELED,
                        job.awaitTermination(5_000L));
            }
        } finally {
            Files.write(exchange.resolve("stateless-shutdown"),
                    new byte[] {1});
            assertExit(source, exchange, "broadcast-source");
            assertExit(sink, exchange, "broadcast-sink");
        }
    }

    private static Process launch(String role, Path exchange)
            throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductStatelessWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "stateless-" + role + ".log").toFile())
                .start();
    }

    private static RemoteStreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                Integer.parseInt(text(exchange.resolve(
                        "stateless-" + role + ".port"))));
    }

    private static void awaitFile(Path file, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while ((!Files.exists(file) || Files.size(file) == 0L)
                && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(Files.exists(file) && Files.size(file) > 0L,
                "process did not publish " + file);
    }

    private static void assertExit(Process process, Path exchange,
            String role) throws Exception {
        boolean exited = process.waitFor(5L, TimeUnit.SECONDS);
        process.destroyForcibly();
        assertTrue(exited, () -> log(exchange, role));
        assertEquals(0, process.exitValue(), () -> log(exchange, role));
    }

    private static String logs(Path exchange) {
        return log(exchange, "broadcast-source") + '\n'
                + log(exchange, "broadcast-sink");
    }

    private static String log(Path exchange, String role) {
        try {
            return text(exchange.resolve("stateless-" + role + ".log"));
        } catch (Exception ignored) {
            return "missing " + role + " process log";
        }
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }
}
