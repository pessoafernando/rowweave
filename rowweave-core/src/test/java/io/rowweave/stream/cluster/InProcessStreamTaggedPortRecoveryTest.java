package io.rowweave.stream.cluster;

import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Local deployment preserves mixed tagged-port contracts through restore. */
class InProcessStreamTaggedPortRecoveryTest {

    @Test
    void localClusterExecutesAndRestoresDifferingTagSchemaKeyAndChangelog(
            @TempDir Path root) throws Exception {
        Map<String, DurableStreamSourceProvider> sources = new LinkedHashMap<>();
        DurableStreamSourceProvider source =
                StreamProductStatelessWorkerProcessFixture
                        .processSourceProvider();
        sources.put(source.providerKey(), source);
        Map<String, EagerStreamSinkProvider> sinks = new LinkedHashMap<>();
        EagerStreamSinkProvider main =
                StreamProductStatelessWorkerProcessFixture
                        .processMainSinkProvider(root.resolve("main-result"));
        EagerStreamSinkProvider tag =
                StreamProductStatelessWorkerProcessFixture
                        .processTagSinkProvider(root.resolve("tag-result"));
        sinks.put(main.providerKey(), main);
        sinks.put(tag.providerKey(), tag);
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root, sources, sinks,
                reference -> StreamProductStatelessWorkerProcessFixture
                        .resolveFunction(reference, root),
                Collections.singletonList(
                        StreamProductStatelessWorkerProcessFixture
                                .DEPLOYMENT_SET))) {
            StreamSession definition = StreamProductKeyedProcessDagDispatchTest
                    .session(cluster);
            PlanArtifact artifact = definition.toPlanArtifact();
            StreamJobHandle original = definition.submit(
                    StreamSubmissionId.of("local-tagged-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    original.awaitRunning(5_000L));
            awaitFile(root.resolve("main-result"));
            awaitFile(root.resolve("tag-result"));
            assertEquals("30:50", text(root.resolve("main-result")));
            assertEquals("10:20", text(root.resolve("tag-result")));

            StreamSavepointHandle savepoint = original.stopWithSavepoint(
                    StreamCommandId.of("local-tagged-savepoint"),
                    new StreamSavepointRequest(
                            "file-savepoint-v1", new byte[0]));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    savepoint.awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                    original.awaitTermination(5_000L));

            StreamJobHandle restored = cluster.restore(
                    StreamSubmissionId.of("local-tagged-restored"), artifact,
                    new StreamRestoreRequest(original.id(),
                            StreamRecoveryPointId.savepoint(
                                    savepoint.savepointId()), 2, 128));
            assertEquals(StreamJobStatus.RUNNING,
                    restored.awaitRunning(5_000L));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    restored.triggerCheckpoint(StreamCommandId.of(
                            "local-tagged-restored-checkpoint"))
                            .awaitCompletion(5_000L));
            restored.cancel(StreamCommandId.of(
                    "local-tagged-restored-cancel"))
                    .awaitCompletion(5_000L);
            assertEquals(StreamJobStatus.CANCELED,
                    restored.awaitTermination(5_000L));
        }
    }

    private static void awaitFile(Path path) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while ((!Files.exists(path) || Files.size(path) == 0L)
                && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(Files.exists(path) && Files.size(path) > 0L,
                "result was not published: " + path);
    }

    private static String text(Path path) throws Exception {
        return new String(Files.readAllBytes(path),
                java.nio.charset.StandardCharsets.US_ASCII);
    }
}
