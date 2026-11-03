package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.JobClient;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** default Batch sessions share the socket-free cluster runtime. */
class DefaultBatchRuntimeContractTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));

    @BeforeEach
    @AfterEach
    void resetProcessRuntime() {
        InProcessBatchRuntime.resetProcessWideForTesting();
    }

    @Test
    void defaultSessionsReuseOneSocketFreeRuntimeWithoutOwningItsLifecycle(@TempDir Path tmp)
            throws Exception {
        Path input = tmp.resolve("input.tsv");
        Files.write(input, "7\n8\n".getBytes(StandardCharsets.UTF_8));

        commitFileJob("first", input, tmp.resolve("first.tsv"));
        InProcessBatchRuntime.Snapshot first = InProcessBatchRuntime.snapshotForTesting();

        assertTrue(first.running());
        assertEquals(1L, first.startCount());
        assertEquals(1L, first.completedSubmissionCount());
        assertEquals(0, first.controlListenerCount());
        assertEquals(0, first.controlAcceptThreadCount());
        assertEquals(0, first.dataListenerCount());
        assertTrue(first.handlerThreadCount() > 0,
                "in-process handlers must run outside the submitting thread");

        commitFileJob("second", input, tmp.resolve("second.tsv"));
        InProcessBatchRuntime.Snapshot second = InProcessBatchRuntime.snapshotForTesting();

        assertEquals(first.runtimeId(), second.runtimeId());
        assertEquals(1L, second.startCount());
        assertEquals(2L, second.completedSubmissionCount());
        assertTrue(second.running(), "BatchSession must not close the shared runtime");
        assertEquals(2, FileSink.read(META, tmp.resolve("first.tsv").toString()).size());
        assertEquals(2, FileSink.read(META, tmp.resolve("second.tsv").toString()).size());
    }

    @Test
    void unsupportedLocalAttachedOutputFailsBeforeRuntimeOrOutputSideEffects(@TempDir Path tmp)
            throws Exception {
        Path input = tmp.resolve("input.tsv");
        Files.write(input, "9\n".getBytes(StandardCharsets.UTF_8));
        CollectingSink output = new CollectingSink(META);
        BatchSession session = RowWeave.createBatchSession("unsupported-local-attached");
        session.fromSource(new FileSource(META, input.toString())).sinkTo(output);

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class, session::execute);

        assertTrue(failure.getMessage().contains("CollectingSink"), failure.getMessage());
        assertTrue(output.getRows().isEmpty());
        InProcessBatchRuntime.Snapshot snapshot = InProcessBatchRuntime.snapshotForTesting();
        assertFalse(snapshot.running());
        assertEquals(0L, snapshot.startCount());
        assertEquals(0L, snapshot.completedSubmissionCount());
    }

    @Test
    void explicitJobClientRemainsAuthoritativeAndDoesNotStartDefaultRuntime(@TempDir Path tmp)
            throws Exception {
        Path input = tmp.resolve("input.tsv");
        Files.write(input, "11\n".getBytes(StandardCharsets.UTF_8));
        AtomicInteger submissions = new AtomicInteger();
        JobClient injected = new JobClient() {
            @Override
            public long submitToSink(PlanGraph sinkGraph) {
                throw new AssertionError("artifact submit must remain the unique entry");
            }

            @Override
            public ExecutionReport submit(PlanArtifact artifact) {
                submissions.incrementAndGet();
                return new ExecutionReport(0L, 0L, 0L, 0L, null);
            }
        };
        BatchSession session = new BatchSession("injected", "injected", injected);
        session.fromSource(new FileSource(META, input.toString()))
                .sinkTo(new FileSink(META, tmp.resolve("unused.tsv").toString()));

        session.execute();

        assertEquals(1, submissions.get());
        assertFalse(InProcessBatchRuntime.snapshotForTesting().running());
    }

    @Test
    void explicitShutdownIsIdempotentAndNextDefaultSubmissionCreatesANewGeneration(
            @TempDir Path tmp) throws Exception {
        Path input = tmp.resolve("input.tsv");
        Files.write(input, "13\n".getBytes(StandardCharsets.UTF_8));
        commitFileJob("before-shutdown", input, tmp.resolve("before.tsv"));
        String firstRuntimeId = InProcessBatchRuntime.snapshotForTesting().runtimeId();

        InProcessBatchRuntime.shutdownProcessWideForTesting();
        InProcessBatchRuntime.shutdownProcessWideForTesting();
        assertFalse(InProcessBatchRuntime.snapshotForTesting().running());

        commitFileJob("after-shutdown", input, tmp.resolve("after.tsv"));
        InProcessBatchRuntime.Snapshot restarted = InProcessBatchRuntime.snapshotForTesting();
        assertTrue(restarted.running());
        assertNotEquals(firstRuntimeId, restarted.runtimeId());
        assertEquals(2L, restarted.startCount());
    }

    @Test
    void processWideFactoryIsSharedAndLegacyClientHasNoProductionConstructionSites()
            throws Exception {
        assertTrue(io.rowweave.local.LocalJobClient.class
                .isAnnotationPresent(Deprecated.class));
        assertEquals(BatchRuntimes.processWideInProcess(),
                BatchRuntimes.processWideInProcess());

        Path current = Paths.get("").toAbsolutePath();
        Path sources = Files.isDirectory(current.resolve("src/main/java"))
                ? current.resolve("src/main/java")
                : current.resolve("rowweave-core/src/main/java");
        List<String> offenders = new ArrayList<>();
        try (Stream<Path> files = Files.walk(sources)) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.getFileName().toString()
                            .equals("LocalJobClient.java"))
                    .forEach(path -> {
                        try {
                            String source = new String(Files.readAllBytes(path),
                                    StandardCharsets.UTF_8);
                            if (source.contains("LocalJobClient(")) {
                                offenders.add(sources.relativize(path).toString());
                            }
                        } catch (java.io.IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    });
        }
        assertTrue(offenders.isEmpty(),
                "retired LocalJobClient must remain explicit external compatibility only: "
                        + offenders);
    }

    private static void commitFileJob(String name, Path input, Path output) {
        BatchSession session = RowWeave.createBatchSession(name);
        session.fromSource(new FileSource(META, input.toString()))
                .sinkTo(new FileSink(META, output.toString()));
        session.execute();
    }
}
