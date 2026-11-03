package io.rowweave;

import io.rowweave.external.StreamPublicWorkerBootstrapFixture;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamWorkerEndpoint;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** applications bootstrap Cluster without internal imports. */
@Tag("multiprocess")
class StreamPublicClusterBootstrapTest {

    @Test
    void publicBootstrapStartsWorkersAndFencedReplacementMaster(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("public-source.port"), 5_000L);
            awaitFile(exchange.resolve("public-sink.port"), 5_000L);
            java.util.List<StreamWorkerEndpoint> workers = Arrays.asList(
                    endpoint(exchange, "source"), endpoint(exchange, "sink"));
            StreamSubmissionId submission =
                    StreamSubmissionId.of("public-bootstrap-submit");
            StreamJobId jobId;
            try (StreamMasterRuntime master = RowWeave.startStreamMaster(
                    exchange.resolve("cluster"), workers);
                 StreamJobClient client = master.createJobClient()) {
                assertThrows(IllegalStateException.class,
                        () -> RowWeave.startStreamMaster(
                                exchange.resolve("cluster"), workers));
                StreamJobHandle job = session(client).submit(submission);
                jobId = job.id();
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("public-result"), 5_000L);
                assertEquals("public-bootstrap", text(
                        exchange.resolve("public-result")));
            }

            try (StreamMasterRuntime replacement = RowWeave.startStreamMaster(
                    exchange.resolve("cluster"), workers);
                 StreamJobClient client = replacement.createJobClient()) {
                StreamJobHandle attached = client.attachSubmission(submission);
                assertEquals(jobId, attached.id());
                assertEquals(StreamJobStatus.RUNNING,
                        attached.awaitRunning(5_000L));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        attached.cancel(StreamCommandId.of(
                                "public-bootstrap-cancel"))
                                .awaitCompletion(5_000L));
            }
        } finally {
            Files.write(exchange.resolve("public-shutdown"), new byte[] {1});
            boolean sourceExited = source.waitFor(5L, TimeUnit.SECONDS);
            boolean sinkExited = sink.waitFor(5L, TimeUnit.SECONDS);
            source.destroyForcibly();
            sink.destroyForcibly();
            assertTrue(sourceExited, () -> log(exchange, "source"));
            assertTrue(sinkExited, () -> log(exchange, "sink"));
        }
        assertEquals(0, source.exitValue(), () -> log(exchange, "source"));
        assertEquals(0, sink.exitValue(), () -> log(exchange, "sink"));
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("value", "value", "value",
                        DataType.StringType, false)));
        StreamSession session = RowWeave.createStreamSession(
                "public-bootstrap", "public-bootstrap", client);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.exactOrdered(
                        schema, StreamPublicWorkerBootstrapFixture.SOURCE,
                        StreamPublicWorkerBootstrapFixture.SOURCE_CONTRACT,
                        new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                StreamPublicWorkerBootstrapFixture.digest(
                                        "public-bootstrap-split"),
                                "int32-v1", new byte[] {2}))))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        StreamPublicWorkerBootstrapFixture.SINK,
                        StreamPublicWorkerBootstrapFixture.SINK_CONTRACT,
                        new byte[] {3}));
        return session;
    }

    private static StreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return new StreamWorkerEndpoint("127.0.0.1", Integer.parseInt(
                text(exchange.resolve("public-" + role + ".port"))));
    }

    private static Process launch(String role, Path exchange) throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamPublicWorkerBootstrapFixture.class.getName(), role,
                exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "public-" + role + ".log").toFile())
                .start();
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

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static String log(Path exchange, String role) {
        try {
            return text(exchange.resolve("public-" + role + ".log"));
        } catch (Exception ignored) {
            return "missing process log";
        }
    }
}
