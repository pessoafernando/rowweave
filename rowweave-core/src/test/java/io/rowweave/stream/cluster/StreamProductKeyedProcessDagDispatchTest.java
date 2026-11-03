package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamProcessResult;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamManagedStateSpec;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamTimerSpec;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** KEYED_PROCESS executes across real Worker JVMs. */
@Tag("multiprocess")
class StreamProductKeyedProcessDagDispatchTest {

    @Test
    void realClusterRunsParallelValueStateAndTaggedOutputCheckpoint(
            @TempDir Path exchange) throws Exception {
        Process source = launch("process-source", exchange);
        Process mainSink = launch("process-main-sink", exchange);
        Process tagSink = launch("process-tag-sink", exchange);
        try {
            awaitFile(exchange.resolve(
                    "stateless-process-source.port"), 5_000L);
            awaitFile(exchange.resolve(
                    "stateless-process-main-sink.port"), 5_000L);
            awaitFile(exchange.resolve(
                    "stateless-process-tag-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("process-run-authority"), 1L,
                            "process-master-incarnation",
                            Arrays.asList(endpoint(exchange, "process-source"),
                                    endpoint(exchange, "process-main-sink"),
                                    endpoint(exchange, "process-tag-sink")),
                            exchange.resolve("stateless-checkpoint-store"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("process-job-authority"),
                                 runs);
                 StreamJobClient client = RowWeave.createClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamSession definition = session(client);
                PlanArtifact artifact = definition.toPlanArtifact();
                StreamJobHandle job = definition.submit(
                        StreamSubmissionId.of("keyed-process-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L), () -> logs(exchange));
                awaitFile(exchange.resolve("process-main-result"), 10_000L);
                awaitFile(exchange.resolve("process-tag-result"), 10_000L);
                assertEquals("30:50", text(
                        exchange.resolve("process-main-result")));
                assertEquals("10:20", text(
                        exchange.resolve("process-tag-result")));

                StreamSavepointHandle savepoint = job.stopWithSavepoint(
                        StreamCommandId.of("keyed-process-savepoint"),
                        new StreamSavepointRequest(
                                "file-savepoint-v1", new byte[0]));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        savepoint.awaitCompletion(5_000L),
                        () -> logs(exchange));
                assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                        job.awaitTermination(5_000L));

                deleteTree(exchange.resolve(
                        "stateless-checkpoint-store-process-source"));
                deleteTree(exchange.resolve(
                        "stateless-checkpoint-store-process-main-sink"));
                deleteTree(exchange.resolve(
                        "stateless-checkpoint-store-process-tag-sink"));

                StreamJobHandle restored = client.restore(
                        StreamSubmissionId.of("keyed-process-restored"),
                        artifact, new StreamRestoreRequest(job.id(),
                        StreamRecoveryPointId.savepoint(
                                savepoint.savepointId()), 2, 128));
                assertEquals(StreamJobStatus.RUNNING,
                        restored.awaitRunning(5_000L));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        restored.triggerCheckpoint(StreamCommandId.of(
                                "keyed-process-restored-checkpoint"))
                                .awaitCompletion(5_000L));
                restored.cancel(StreamCommandId.of(
                        "keyed-process-restored-cancel"))
                        .awaitCompletion(5_000L);
                assertEquals(StreamJobStatus.CANCELED,
                        restored.awaitTermination(5_000L));
            }
        } finally {
            Files.write(exchange.resolve("stateless-shutdown"),
                    new byte[] {1});
            assertExit(source, exchange, "process-source");
            assertExit(mainSink, exchange, "process-main-sink");
            assertExit(tagSink, exchange, "process-tag-sink");
        }
    }

    static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "keyed-process-cluster", "keyed-process-cluster", client);
        session.setParallelism(2);
        session.setMaxParallelism(128);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 32_768L, 32, 0,
                1_048_576L, 8, 64, 1_048_576L));
        StreamManagedStateSpec state = StreamManagedStateSpec.bounded(
                new StreamStateSpec("count",
                        StreamStateSpec.Kind.KEYED_VALUE,
                        "signed-long-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                1, 10L, 4_096L, Long.BYTES);
        StreamProcessSpec process = StreamProcessSpec.keyed(
                StreamProductStatelessWorkerProcessFixture.function(
                        "keyed-count", FunctionAbi.KEYED_PROCESS_V1,
                        "keyed-count"),
                        StreamProductStatelessWorkerProcessFixture
                        .processOutputSchema(),
                Collections.singletonList("value"),
                Collections.singletonList(state),
                Collections.singletonList(StreamTimerSpec.processingTime(
                        "emit-latest", 1, 10L, 4_096L)),
                StreamEmissionLimit.of(1, 1_024L),
                StreamEmissionLimit.of(1, 1_024L))
                .withOutputTags(Collections.singletonList(
                        StreamProductStatelessWorkerProcessFixture
                                .processTag()));
        StreamProcessResult result = session.fromSource(source()).uid("source")
                .keyBy("value").uid("key")
                .keyedProcessWithOutputs(process, "process");
        result.main().sinkTo("main-sink", sink(
                StreamProductStatelessWorkerProcessFixture
                        .PROCESS_MAIN_SINK_PROVIDER));
        result.output("second-seen").filter(
                StreamProductStatelessWorkerProcessFixture.function(
                        "tag-filter", FunctionAbi.FILTER_V1, "tag-filter"))
                .uid("tag-filter").sinkTo("tag-sink", sink(
                StreamProductStatelessWorkerProcessFixture
                        .PROCESS_TAG_SINK_PROVIDER));
        return session;
    }

    private static StreamSourceSpec source() {
        return StreamSourceSpec.exactOrdered(inputSchema(),
                StreamProductStatelessWorkerProcessFixture
                        .PROCESS_SOURCE_PROVIDER,
                StreamProductStatelessWorkerProcessFixture.SOURCE_CONTRACT,
                new byte[] {1}, Collections.singletonList(new StreamSplitSpec(
                        StreamProductStatelessWorkerProcessFixture.digestText(
                                "process-split"),
                        "sequence-v1", new byte[] {2})));
    }

    private static StreamSchema inputSchema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("value", "value", "value",
                        DataType.IntegerType, false)));
    }

    private static StreamSinkSpec sink(String providerKey) {
        return StreamSinkSpec.eagerAtLeastOnce(providerKey,
                StreamProductStatelessWorkerProcessFixture.SINK_CONTRACT,
                new byte[] {3});
    }

    private static Process launch(String role, Path exchange) throws Exception {
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
        return log(exchange, "process-source") + '\n'
                + log(exchange, "process-main-sink") + '\n'
                + log(exchange, "process-tag-sink");
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

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) return;
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override public FileVisitResult visitFile(
                    Path file, BasicFileAttributes attributes)
                    throws java.io.IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult postVisitDirectory(
                    Path directory, java.io.IOException failure)
                    throws java.io.IOException {
                if (failure != null) throw failure;
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
