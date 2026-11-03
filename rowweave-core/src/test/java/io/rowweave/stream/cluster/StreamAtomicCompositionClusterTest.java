package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamMasterRuntime;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamWorkerEndpoint;
import io.rowweave.test.ForkedJvm;
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** /GDF4: M+5W executes and recovers one composed graph. */
@Tag("multiprocess")
class StreamAtomicCompositionClusterTest {
    private static final List<String> ROLES = Arrays.asList(
            "source", "compute-a", "compute-b", "sink-1", "sink-2");

    @Test
    void publicClusterRunsMixedGraphAndCheckpoint(@TempDir Path exchange)
            throws Exception {
        List<Process> workers = new ArrayList<>();
        try {
            for (String role : ROLES) workers.add(launch(role, exchange));
            for (String role : ROLES) awaitFile(port(exchange, role), 5_000L);
            List<StreamWorkerEndpoint> endpoints = new ArrayList<>();
            for (String role : ROLES) {
                endpoints.add(new StreamWorkerEndpoint("127.0.0.1",
                        Integer.parseInt(text(port(exchange, role)).trim())));
            }
            try (StreamMasterRuntime master = RowWeave.startStreamMaster(
                    exchange.resolve("atomic-composition-master"), endpoints);
                 StreamJobClient client = master.createJobClient()) {
                StreamSession definition = StreamAtomicCompositionProductTest
                        .session(client);
                PlanArtifact artifact = definition.toPlanArtifact();
                StreamJobHandle job = definition.submit(StreamSubmissionId.of(
                                "atomic-composition-real-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitLines(result(exchange, 1), 4, 10_000L);
                awaitLines(result(exchange, 2), 4, 10_000L);
                assertEquals(Arrays.asList(1L, 2L, 3L, 4L),
                        sortedValues(result(exchange, 1)));
                assertEquals(sortedValues(result(exchange, 1)),
                        sortedValues(result(exchange, 2)));
                awaitFile(functions(exchange, "compute-a"), 5_000L);
                awaitFile(functions(exchange, "compute-b"), 5_000L);
                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("atomic-composition-real-checkpoint"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L));
                assertEquals(1L, job.progress().completedCheckpoints());

                StreamSavepointHandle savepoint = job.stopWithSavepoint(
                        StreamCommandId.of(
                                "atomic-composition-real-savepoint"),
                        new StreamSavepointRequest(
                                "file-savepoint-v1", new byte[0]));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        savepoint.awaitCompletion(5_000L));
                assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                        job.awaitTermination(5_000L));
                StreamJobHandle restored = client.restore(
                        StreamSubmissionId.of(
                                "atomic-composition-real-restored"),
                        artifact, new StreamRestoreRequest(job.id(),
                                StreamRecoveryPointId.savepoint(
                                        savepoint.savepointId()), 2, 128));
                assertEquals(StreamJobStatus.RUNNING,
                        restored.awaitRunning(5_000L));
                StreamCheckpointHandle restoredCheckpoint =
                        restored.triggerCheckpoint(StreamCommandId.of(
                                "atomic-composition-real-restored-checkpoint"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        restoredCheckpoint.awaitCompletion(5_000L));
                assertEquals(Arrays.asList(1L, 2L, 3L, 4L),
                        sortedValues(result(exchange, 1)));
                assertEquals(sortedValues(result(exchange, 1)),
                        sortedValues(result(exchange, 2)));
                restored.cancel(StreamCommandId.of(
                        "atomic-composition-real-cancel"))
                        .awaitCompletion(5_000L);
                assertEquals(StreamJobStatus.CANCELED,
                        restored.awaitTermination(5_000L));
            }
        } finally {
            Files.write(exchange.resolve("atomic-composition-shutdown"),
                    new byte[] {1});
            for (int index = 0; index < workers.size(); index++) {
                if (workers.get(index) != null) {
                    assertExit(workers.get(index), exchange, ROLES.get(index));
                }
            }
        }
    }

    @Test
    void computeWorkerLossRestoresWholeMixedGraphWithoutReplay(
            @TempDir Path exchange) throws Exception {
        List<Process> workers = new ArrayList<>();
        try {
            for (String role : ROLES) workers.add(launch(role, exchange));
            for (String role : ROLES) awaitFile(port(exchange, role), 5_000L);
            List<StreamWorkerEndpoint> endpoints = new ArrayList<>();
            for (String role : ROLES) {
                endpoints.add(new StreamWorkerEndpoint("127.0.0.1",
                        Integer.parseInt(text(port(exchange, role)).trim())));
            }
            try (StreamMasterRuntime master = RowWeave.startStreamMaster(
                    exchange.resolve("atomic-composition-master"), endpoints);
                 StreamJobClient client = master.createJobClient()) {
                StreamJobHandle job = StreamAtomicCompositionProductTest
                        .session(client).submit(StreamSubmissionId.of(
                                "atomic-composition-worker-loss"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitLines(result(exchange, 1), 4, 10_000L);
                awaitLines(result(exchange, 2), 4, 10_000L);
                awaitFile(functions(exchange, "compute-a"), 5_000L);
                awaitFile(functions(exchange, "compute-b"), 5_000L);
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        job.triggerCheckpoint(StreamCommandId.of(
                                "atomic-composition-before-worker-loss"))
                                .awaitCompletion(5_000L));

                int failedIndex = ROLES.indexOf("compute-a");
                Process failed = workers.get(failedIndex);
                failed.destroyForcibly();
                assertTrue(failed.waitFor(5L, TimeUnit.SECONDS));
                workers.set(failedIndex, null);
                awaitRestart(job, 15_000L);
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(15_000L));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        job.triggerCheckpoint(StreamCommandId.of(
                                "atomic-composition-after-worker-loss"))
                                .awaitCompletion(10_000L));
                assertEquals(1, job.progress().restarts());
                assertEquals(Arrays.asList(1L, 2L, 3L, 4L),
                        sortedValues(result(exchange, 1)));
                assertEquals(sortedValues(result(exchange, 1)),
                        sortedValues(result(exchange, 2)));
                job.cancel(StreamCommandId.of(
                        "atomic-composition-worker-loss-cancel"))
                        .awaitCompletion(5_000L);
                assertEquals(StreamJobStatus.CANCELED,
                        job.awaitTermination(5_000L));
            }
        } finally {
            Files.write(exchange.resolve("atomic-composition-shutdown"),
                    new byte[] {1});
            for (int index = 0; index < workers.size(); index++) {
                if (workers.get(index) != null) {
                    assertExit(workers.get(index), exchange, ROLES.get(index));
                }
            }
        }
    }

    private static Process launch(String role, Path exchange)
            throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamAtomicCompositionWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "atomic-composition-" + role + ".log").toFile())
                .start();
    }

    private static Path port(Path exchange, String role) {
        return exchange.resolve("atomic-composition-" + role + ".port");
    }

    private static Path result(Path exchange, int sink) {
        return exchange.resolve("atomic-composition-result-" + sink);
    }

    private static Path functions(Path exchange, String role) {
        return exchange.resolve("atomic-composition-functions-" + role);
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
                    && Files.readAllLines(file, StandardCharsets.US_ASCII)
                    .size() >= count) return;
            Thread.sleep(5L);
        }
        assertEquals(count, Files.exists(file)
                ? Files.readAllLines(file, StandardCharsets.US_ASCII).size()
                : 0);
    }

    private static List<Long> sortedValues(Path file) throws Exception {
        List<Long> values = new ArrayList<>();
        for (String line : Files.readAllLines(file,
                StandardCharsets.US_ASCII)) values.add(Long.parseLong(line));
        Collections.sort(values);
        return values;
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
        return new String(Files.readAllBytes(file), StandardCharsets.US_ASCII);
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
                    "atomic-composition-" + role + ".log"));
        } catch (Exception ignored) {
            return "missing " + role + " process log";
        }
    }
}
