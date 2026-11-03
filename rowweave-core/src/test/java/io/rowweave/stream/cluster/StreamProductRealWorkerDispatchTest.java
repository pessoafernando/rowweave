package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** the public cluster path dispatches to two real Worker JVMs. */
@Tag("multiprocess")
class StreamProductRealWorkerDispatchTest {

    @Test
    void acceptedSubmissionWaitsForWorkerRegistrationWithoutBecomingTerminal(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("source.port"), 5_000L);
            awaitFile(exchange.resolve("sink.port"), 5_000L);
            RemoteStreamWorkerEndpoint sourceEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", port(exchange, "source"));
            RemoteStreamWorkerEndpoint sinkEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", port(exchange, "sink"));
            AtomicReference<List<RemoteStreamWorkerEndpoint>> workers =
                    new AtomicReference<>(Collections.emptyList());
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("run-authority"), 1L,
                            "master-incarnation-1", workers::get,
                            exchange.resolve("checkpoint-store"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("job-authority"), runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamSubmissionId submission = StreamSubmissionId.of(
                        "submit-before-worker-registration");
                StreamJobHandle job = session(client).submit(submission);
                assertEquals(StreamJobStatus.STARTING, job.status());
                assertEquals(job.id(), client.attachSubmission(submission).id());

                workers.set(Arrays.asList(sourceEndpoint, sinkEndpoint));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(10_000L));
                awaitFile(exchange.resolve("result"), 10_000L);
                assertEquals(expectedResult(), text(exchange.resolve("result")));
                job.cancel(StreamCommandId.of("cancel-after-registration"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            Files.write(exchange.resolve("shutdown"), new byte[] {1});
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

    @Test
    void publicSubmissionRunsSourceAndSinkInIndependentWorkers(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("source.port"), 5_000L);
            awaitFile(exchange.resolve("sink.port"), 5_000L);
            RemoteStreamWorkerEndpoint sourceEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", port(exchange, "source"));
            RemoteStreamWorkerEndpoint sinkEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", port(exchange, "sink"));
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("run-authority"), 1L,
                            "master-incarnation-1",
                            () -> Arrays.asList(sourceEndpoint, sinkEndpoint),
                            exchange.resolve("checkpoint-store"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("job-authority"), runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamSession session = session(client);
                StreamJobHandle job = session.submit(
                        StreamSubmissionId.of("real-worker-submit"));

                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("result"), 10_000L);
                assertEquals(expectedResult(), text(exchange.resolve("result")));
                assertTrue(source.isAlive());
                assertTrue(sink.isAlive());
                assertNotEquals(ManagementFactory.getRuntimeMXBean().getName(),
                        text(exchange.resolve("source.identity")));
                assertNotEquals(ManagementFactory.getRuntimeMXBean().getName(),
                        text(exchange.resolve("sink.identity")));
                assertNotEquals(text(exchange.resolve("source.identity")),
                        text(exchange.resolve("sink.identity")));

                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("checkpoint-real-workers"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L));
                assertEquals(1L,
                        job.progress().completedCheckpoints());

                job.cancel(StreamCommandId.of("cancel-real-workers"))
                        .awaitCompletion(5_000L);
                assertEquals(StreamJobStatus.CANCELED,
                        job.awaitTermination(5_000L));
            }
        } finally {
            Files.write(exchange.resolve("shutdown"), new byte[] {1});
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

    @Test
    void hardKilledMasterReplacementTakesOverSameRunOnLiveWorkers(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        Process firstMaster = null;
        Process secondMaster = null;
        try {
            awaitFile(exchange.resolve("source.port"), 5_000L);
            awaitFile(exchange.resolve("sink.port"), 5_000L);
            firstMaster = launchMaster(1L, exchange);
            awaitFile(exchange.resolve("master-1.port"), 5_000L);
            String firstMasterIdentity = text(
                    exchange.resolve("master-1.identity"));
            assertNotEquals(ManagementFactory.getRuntimeMXBean().getName(),
                    firstMasterIdentity);
            assertNotEquals(text(exchange.resolve("source.identity")),
                    firstMasterIdentity);
            assertNotEquals(text(exchange.resolve("sink.identity")),
                    firstMasterIdentity);
            StreamSubmissionId submission =
                    StreamSubmissionId.of("process-master-replacement");
            io.rowweave.stream.api.StreamJobId jobId;
            try (StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", port(exchange, "master-1"))) {
                StreamJobHandle job = session(client).submit(submission);
                jobId = job.id();
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("result"), 10_000L);
            }

            killMaster(firstMaster, 1L, exchange);
            firstMaster = null;
            secondMaster = launchMaster(2L, exchange);
            awaitFile(exchange.resolve("master-2.port"), 5_000L);
            assertNotEquals(firstMasterIdentity,
                    text(exchange.resolve("master-2.identity")));
            try (StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", port(exchange, "master-2"))) {
                StreamJobHandle attached = client.attachSubmission(submission);
                assertEquals(jobId, attached.id());
                assertEquals(StreamJobStatus.RUNNING,
                        attached.awaitRunning(5_000L));
                assertEquals(expectedResult(), text(exchange.resolve("result")));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        attached.triggerCheckpoint(StreamCommandId.of(
                                "checkpoint-after-hard-master-kill"))
                                .awaitCompletion(5_000L));
                attached.cancel(StreamCommandId.of("cancel-after-replacement"))
                        .awaitCompletion(5_000L);
            }

            stopMaster(secondMaster, 2L, exchange);
            secondMaster = null;
        } finally {
            if (firstMaster != null) firstMaster.destroyForcibly();
            if (secondMaster != null) secondMaster.destroyForcibly();
            Files.write(exchange.resolve("shutdown"), new byte[] {1});
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

    @Test
    void replacementMasterRestartsFromCheckpointWhenAssignedWorkerAlsoDied(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        Process replacementSource = null;
        Process firstMaster = null;
        Process secondMaster = null;
        try {
            awaitFile(exchange.resolve("source.port"), 5_000L);
            awaitFile(exchange.resolve("sink.port"), 5_000L);
            firstMaster = launchMaster(1L, exchange);
            awaitFile(exchange.resolve("master-1.port"), 5_000L);
            StreamSubmissionId submission = StreamSubmissionId.of(
                    "master-and-worker-loss");
            io.rowweave.stream.api.StreamJobId jobId;
            try (StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", port(exchange, "master-1"))) {
                StreamJobHandle job = session(client).submit(submission);
                jobId = job.id();
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("result"), 10_000L);
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        job.triggerCheckpoint(StreamCommandId.of(
                                "before-master-and-worker-loss"))
                                .awaitCompletion(5_000L));
            }

            killMaster(firstMaster, 1L, exchange);
            firstMaster = null;
            source.destroyForcibly();
            assertTrue(source.waitFor(5L, TimeUnit.SECONDS));
            replacementSource = launch("source-b", exchange);
            awaitFile(exchange.resolve("source-b.port"), 5_000L);
            Files.write(exchange.resolve("use-source-b"), new byte[] {1});
            secondMaster = launchMaster(2L, exchange);
            awaitFile(exchange.resolve("master-2.port"), 5_000L);

            try (StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", port(exchange, "master-2"))) {
                StreamJobHandle attached = client.attach(jobId);
                assertEquals(StreamJobStatus.RUNNING,
                        attached.awaitRunning(10_000L),
                        "replacement must combine coordinator takeover with worker-loss restart");
                awaitRestart(attached, 10_000L);
                assertEquals(StreamJobStatus.RUNNING,
                        attached.awaitRunning(10_000L),
                        "the restarted attempt must become active");
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        attached.triggerCheckpoint(StreamCommandId.of(
                                "after-master-and-worker-loss"))
                                .awaitCompletion(5_000L));
                attached.cancel(StreamCommandId.of(
                                "cancel-after-master-and-worker-loss"))
                        .awaitCompletion(5_000L);
            }
            stopMaster(secondMaster, 2L, exchange);
            secondMaster = null;
        } finally {
            if (firstMaster != null) firstMaster.destroyForcibly();
            if (secondMaster != null) secondMaster.destroyForcibly();
            Files.write(exchange.resolve("shutdown"), new byte[] {1});
            if (source.isAlive()) source.destroyForcibly();
            if (replacementSource != null) {
                boolean exited = replacementSource.waitFor(
                        5L, TimeUnit.SECONDS);
                replacementSource.destroyForcibly();
                assertTrue(exited, log(exchange, "source-b"));
            }
            boolean sinkExited = sink.waitFor(5L, TimeUnit.SECONDS);
            sink.destroyForcibly();
            assertTrue(sinkExited, () -> log(exchange, "sink"));
        }
        assertEquals(0, replacementSource.exitValue(),
                log(exchange, "source-b"));
        assertEquals(0, sink.exitValue(), () -> log(exchange, "sink"));
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "real-worker-job", "real-worker-job", client);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0,
                65_536L, 4, 16, 0L));
        session.fromSource(source()).uid("source").sinkTo("sink", sink());
        return session;
    }

    private static StreamSourceSpec source() {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false)));
        return StreamSourceSpec.exactOrdered(schema, "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static Process launch(String role, Path exchange) throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(role + ".log").toFile())
                .start();
    }

    private static Process launchMaster(long term, Path exchange)
            throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductMasterProcessFixture.class.getName(),
                exchange.toString(), Long.toString(term)))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "master-" + term + ".log").toFile())
                .start();
    }

    private static void stopMaster(Process master, long term, Path exchange)
            throws Exception {
        Files.write(exchange.resolve("master-" + term + ".shutdown"),
                new byte[] {1});
        boolean exited = master.waitFor(5L, TimeUnit.SECONDS);
        if (!exited) master.destroyForcibly();
        assertTrue(exited, () -> log(exchange, "master-" + term));
        assertEquals(0, master.exitValue(),
                () -> log(exchange, "master-" + term));
    }

    private static void killMaster(Process master, long term, Path exchange)
            throws Exception {
        master.destroyForcibly();
        assertTrue(master.waitFor(5L, TimeUnit.SECONDS),
                () -> "hard-killed Master did not exit\n"
                        + log(exchange, "master-" + term));
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

    private static void awaitRestart(StreamJobHandle job,
            long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (job.progress().restarts() < 1L
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(job.progress().restarts() >= 1L,
                "replacement did not complete the worker-loss restart");
    }

    private static int port(Path exchange, String role) throws Exception {
        return Integer.parseInt(text(exchange.resolve(role + ".port")));
    }

    private static String expectedResult() {
        MessageDigest digest = digest();
        for (int index = 0; index < 1_000; index++) digest.update((byte) index);
        return "1000:" + hex(digest.digest());
    }

    private static String sha256(String value) {
        return hex(digest().digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static String log(Path exchange, String role) {
        try {
            return text(exchange.resolve(role + ".log"));
        } catch (Exception ignored) {
            return "missing process log";
        }
    }

    private static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int current = value[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }
}
