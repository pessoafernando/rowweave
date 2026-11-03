package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public resident CDC checkpoints and restores on real M+2W. */
@Tag("multiprocess")
class StreamProductResidentCdcRealWorkerDispatchTest {

    @Test
    void snapshotHandoffRestoresAtExactLogPositionAcrossWorkerJvms(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("cdc-source.port"), 5_000L);
            awaitFile(exchange.resolve("cdc-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("cdc-run-authority"), 1L,
                            "cdc-master-incarnation",
                            Arrays.asList(endpoint(exchange, "source"),
                                    endpoint(exchange, "sink")),
                            exchange.resolve("cdc-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("cdc-job-authority"), runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamSession session = session(client);
                PlanArtifact artifact = session.toPlanArtifact();
                StreamJobHandle initial = session.submit(
                        StreamSubmissionId.of("cdc-m2w-initial"));
                assertEquals(StreamJobStatus.RUNNING,
                        initial.awaitRunning(5_000L));
                assertEquals(StreamCdcPhase.SNAPSHOT,
                        initial.progress().cdcPhase());
                awaitFile(exchange.resolve("cdc-initial-log-emitted"), 5_000L);
                assertFalse(Files.exists(exchange.resolve("cdc-result")));

                StreamCheckpointHandle completed = initial.triggerCheckpoint(
                        StreamCommandId.of("cdc-m2w-initial-cut"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        completed.awaitCompletion(5_000L));
                awaitText(exchange.resolve("cdc-result"), "A=12.50", 5_000L);
                assertEquals(StreamCdcPhase.LOG,
                        initial.progress().cdcPhase());
                initial.cancel(StreamCommandId.of("cdc-m2w-initial-cancel"))
                        .awaitCompletion(5_000L);

                StreamJobHandle restored = client.restore(
                        StreamSubmissionId.of("cdc-m2w-restored"), artifact,
                        new StreamRestoreRequest(initial.id(),
                                StreamRecoveryPointId.checkpoint(
                                        completed.checkpointId()), 2, 128));
                assertEquals(StreamJobStatus.RUNNING,
                        restored.awaitRunning(5_000L));
                assertEquals(StreamCdcPhase.LOG,
                        restored.progress().cdcPhase());
                awaitFile(exchange.resolve("cdc-restored-log-emitted"), 5_000L);
                assertEquals("A=12.50", text(
                        exchange.resolve("cdc-result")));
                StreamCheckpointHandle restoredCut = restored.triggerCheckpoint(
                        StreamCommandId.of("cdc-m2w-restored-cut"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        restoredCut.awaitCompletion(5_000L));
                awaitText(exchange.resolve("cdc-result"),
                        "A=<deleted>", 5_000L);
                assertEquals(StreamCdcPhase.LOG,
                        restored.progress().cdcPhase());
                assertEquals("SNAPSHOT:10\nLOG:11\n",
                        text(exchange.resolve("cdc-open-phases")));
                restored.cancel(StreamCommandId.of("cdc-m2w-restored-cancel"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            Files.write(exchange.resolve("cdc-shutdown"), new byte[] {1});
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
        StreamSession session = RowWeave.createStreamSession(
                "cdc-m2w", "cdc-m2w", client);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setParallelism(2);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                60_000L, 5_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.residentCdc(
                        StreamProductResidentCdcWorkerProcessFixture.ROW_SCHEMA,
                        Collections.singletonList("id"),
                        StreamProductResidentCdcWorkerProcessFixture.SOURCE,
                        StreamProductResidentCdcWorkerProcessFixture
                                .SOURCE_CONTRACT,
                        new byte[] {1},
                        StreamProductResidentCdcWorkerProcessFixture.SNAPSHOT,
                        10L, Collections.singletonList("snapshot-0")))
                .uid("source")
                .keyBy("id").uid("key")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        StreamProductResidentCdcWorkerProcessFixture.SINK,
                        StreamProductResidentCdcWorkerProcessFixture
                                .SINK_CONTRACT,
                        new byte[] {2}));
        return session;
    }

    private static RemoteStreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                Integer.parseInt(text(exchange.resolve(
                        "cdc-" + role + ".port"))));
    }

    private static Process launch(String role, Path exchange) throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductResidentCdcWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "cdc-" + role + ".log").toFile())
                .start();
    }

    private static void awaitFile(Path file, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!Files.exists(file) && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(Files.exists(file), "process did not publish " + file);
    }

    private static void awaitText(
            Path file, String expected, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (Files.exists(file) && expected.equals(text(file))) return;
            Thread.sleep(5L);
        }
        assertEquals(expected, Files.exists(file) ? text(file) : null);
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static String log(Path exchange, String role) {
        try {
            return text(exchange.resolve("cdc-" + role + ".log"));
        } catch (Exception ignored) {
            return "missing process log";
        }
    }
}
