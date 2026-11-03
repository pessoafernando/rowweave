package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.stream.api.StreamAsyncOrdering;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.api.StreamAsyncResourceSpec;
import io.rowweave.stream.api.StreamAsyncWaitSpec;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.test.ForkedJvm;
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

/** async wait runs in independent Worker JVMs. */
@Tag("multiprocess")
class StreamProductAsyncWaitDagDispatchTest {

    @Test
    void realClusterRunsOrderedAsyncWaitAndCheckpoints(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("async-wait-source.port"), 5_000L);
            awaitFile(exchange.resolve("async-wait-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("async-wait-run-authority"),
                            1L, "async-wait-master-incarnation",
                            Arrays.asList(endpoint(exchange, "source"),
                                    endpoint(exchange, "sink")),
                            exchange.resolve("async-wait-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("async-wait-job-authority"),
                                 runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamJobHandle job = session(client).submit(
                        StreamSubmissionId.of("async-wait-real-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitLines(exchange.resolve("async-wait-result"),
                        3, 10_000L);
                assertEquals(Arrays.asList(
                                "1:customer-1",
                                "2:customer-2",
                                "3:customer-3"),
                        lines(exchange.resolve("async-wait-result")));
                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("async-wait-real-checkpoint"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L));
                assertEquals(1L, job.progress().completedCheckpoints());
                job.cancel(StreamCommandId.of("async-wait-real-cancel"))
                        .awaitCompletion(5_000L);
                assertEquals(StreamJobStatus.CANCELED,
                        job.awaitTermination(5_000L));
            }
        } finally {
            Files.write(exchange.resolve("async-wait-shutdown"),
                    new byte[] {1});
            assertExit(source, exchange, "source");
            assertExit(sink, exchange, "sink");
        }
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "async-wait-real", "async-wait-real", client);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 16_384, 65_536L, 0, 0,
                65_536L, 8, 32, 0L));
        StreamSourceSpec source = StreamSourceSpec.exactOrdered(
                StreamAsyncWaitProductAdmissionTest.inputSchema(),
                StreamProductAsyncWaitWorkerProcessFixture.SOURCE_PROVIDER,
                StreamProductAsyncWaitWorkerProcessFixture.SOURCE_CONTRACT,
                new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(
                                StreamAsyncWaitProductAdmissionTest
                                        .digest("cluster-split"),
                                "sequence-v1", new byte[] {0})));
        DataStream input = session.fromSource(source).uid("source");
        input.asyncWait(StreamAsyncWaitSpec.of(
                        StreamAsyncWaitProductAdmissionTest.function(),
                        StreamAsyncWaitProductAdmissionTest.outputSchema(),
                        Collections.singletonList("id"),
                        StreamAsyncOrdering.ORDERED,
                        StreamAsyncReplaySpec.identityKeyed(
                                "request-id-v1",
                                StreamAsyncWaitProductAdmissionTest
                                        .digest("cluster-replay")),
                        StreamAsyncResourceSpec.of(4, 32_768L,
                                1_000L, 3, 10L, 100L,
                                StreamEmissionLimit.of(1, 4_096L))))
                .uid("lookup")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        StreamProductAsyncWaitWorkerProcessFixture
                                .SINK_PROVIDER,
                        StreamProductAsyncWaitWorkerProcessFixture
                                .SINK_CONTRACT,
                        new byte[] {3}));
        return session;
    }

    private static Process launch(String role, Path exchange)
            throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductAsyncWaitWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "async-wait-" + role + ".log").toFile())
                .start();
    }

    private static RemoteStreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                Integer.parseInt(text(exchange.resolve(
                        "async-wait-" + role + ".port")).trim()));
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

    private static List<String> lines(Path file) throws Exception {
        return Files.readAllLines(file, StandardCharsets.UTF_8);
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
            return text(exchange.resolve("async-wait-" + role + ".log"));
        } catch (Exception ignored) {
            return "missing " + role + " process log";
        }
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }
}
