package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceDiscoverySpec;
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

/** current Worker binaries with a frozen v1 capability profile. */
@Tag("multiprocess")
class StreamMixedVersionRealClusterTest {

    @Test
    void currentMasterRunsV1AndKeepsV2UnplacedBeforeCompatibleWorkerRegistration(
            @TempDir Path exchange) throws Exception {
        Process source = launch("current-source", exchange);
        Process sink = launch("previous-sink", exchange);
        try {
            awaitNonEmpty(exchange.resolve("current-source.port"), 10_000L);
            awaitNonEmpty(exchange.resolve("previous-sink.port"), 10_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("mixed-version-runs"), 1L,
                            "mixed-version-master", Arrays.asList(
                            endpoint(exchange, "current-source"),
                            endpoint(exchange, "previous-sink")),
                            exchange.resolve("mixed-version-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("mixed-version-jobs"), runs);
                 StreamJobClient client =
                         RowWeave.createRealClusterStreamJobClient(
                                 "127.0.0.1", master.port())) {
                StreamJobHandle v1 = fixedSession(client).submit(
                        StreamSubmissionId.of("mixed-version-v1"));
                assertEquals(StreamJobStatus.RUNNING,
                        v1.awaitRunning(10_000L));
                awaitNonEmpty(exchange.resolve("mixed-version-result"),
                        10_000L);
                assertEquals("mixed-v1", new io.rowweave.stream.api
                        .StreamDataRowCodec(
                                StreamMixedVersionWorkerProcessFixture.SCHEMA)
                        .decode(Files.readAllBytes(exchange.resolve(
                                "mixed-version-result"))).getString(0));
                v1.cancel(StreamCommandId.of("mixed-version-v1-cancel"))
                        .awaitCompletion(10_000L);

                StreamSubmissionId dynamicId = StreamSubmissionId.of(
                        "mixed-version-v2");
                StreamJobHandle dynamic = dynamicSession(client).submit(dynamicId);
                assertUnplacedAndCancelable(client, dynamicId, dynamic, exchange,
                        "mixed-version-v2-cancel");
                assertFalse(Files.exists(
                        exchange.resolve("dynamic-enumerator-opened")),
                        "v2 provider opened while compatible placement was unavailable");

                StreamSubmissionId boundedId = StreamSubmissionId.of(
                        "mixed-version-bounded");
                StreamJobHandle bounded = boundedSession(client).submit(boundedId);
                assertUnplacedAndCancelable(client, boundedId, bounded, exchange,
                        "mixed-version-bounded-cancel");
                assertFalse(Files.exists(
                        exchange.resolve("bounded-source-opened")),
                        "bounded provider opened while compatible placement was unavailable");
            }
        } finally {
            Files.write(exchange.resolve("mixed-version-stop"),
                    new byte[] {1});
            assertExit(source, exchange.resolve("current-source.log"));
            assertExit(sink, exchange.resolve("previous-sink.log"));
        }
    }

    private static void assertUnplacedAndCancelable(
            StreamJobClient client, StreamSubmissionId submission,
            StreamJobHandle job, Path exchange, String cancel) {
        assertEquals(StreamJobStatus.STARTING, job.status());
        StreamJobHandle attached = client.attachSubmission(submission);
        assertEquals(job.id(), attached.id());
        assertEquals(StreamJobStatus.STARTING, attached.status());
        assertFalse(new FileStreamClusterRunAuthority(
                exchange.resolve("mixed-version-runs"))
                .exists(job.id().value()),
                "missing capabilities must not create an execution run or placement");
        assertEquals(StreamOperationStatus.SUCCEEDED,
                job.cancel(StreamCommandId.of(cancel)).awaitCompletion(5_000L));
        assertEquals(StreamJobStatus.CANCELED, job.awaitTermination(5_000L));
        assertFalse(new FileStreamClusterRunAuthority(
                exchange.resolve("mixed-version-runs"))
                .exists(job.id().value()),
                "canceling a pending job must not start incompatible tasks");
    }

    private static StreamSession fixedSession(StreamJobClient client) {
        StreamSession session = base(client, "mixed-v1");
        session.fromSource(StreamSourceSpec.exactOrdered(
                        StreamMixedVersionWorkerProcessFixture.SCHEMA,
                        StreamMixedVersionWorkerProcessFixture.SOURCE,
                        StreamMixedVersionWorkerProcessFixture.SOURCE_CONTRACT,
                        new byte[] {1}, Collections.singletonList(
                                StreamMixedVersionWorkerProcessFixture.split())))
                .uid("source")
                .sinkTo("sink", sink());
        return session;
    }

    private static StreamSession dynamicSession(StreamJobClient client) {
        StreamSession session = base(client, "mixed-v2");
        session.fromSource(StreamSourceSpec.dynamicExactOrdered(
                        StreamMixedVersionWorkerProcessFixture.SCHEMA,
                        StreamMixedVersionWorkerProcessFixture.SOURCE,
                        StreamMixedVersionWorkerProcessFixture.SOURCE_CONTRACT,
                        new byte[] {1}, new StreamSourceDiscoverySpec(
                                25L, 1_000L, 4, 4, 2, 64, 64)))
                .uid("source")
                .sinkTo("sink", sink());
        return session;
    }

    private static StreamSession boundedSession(StreamJobClient client) {
        StreamSession session = base(client, "mixed-bounded");
        session.fromSource(StreamSourceSpec.boundedExactOrdered(
                        StreamMixedVersionWorkerProcessFixture.SCHEMA,
                        StreamMixedVersionWorkerProcessFixture.SOURCE,
                        StreamMixedVersionWorkerProcessFixture.SOURCE_CONTRACT,
                        new byte[] {9}, Collections.singletonList(
                                StreamMixedVersionWorkerProcessFixture.split())))
                .uid("source")
                .sinkTo("sink", sink());
        return session;
    }

    private static StreamSession base(StreamJobClient client, String name) {
        StreamSession session = RowWeave.createStreamSession(name, name, client);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        return session;
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                StreamMixedVersionWorkerProcessFixture.SINK,
                StreamMixedVersionWorkerProcessFixture.SINK_CONTRACT,
                new byte[] {3});
    }

    private static Process launch(String role, Path exchange)
            throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamMixedVersionWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(role + ".log").toFile())
                .start();
    }

    private static RemoteStreamWorkerEndpoint endpoint(
            Path exchange, String role) throws Exception {
        return RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                Integer.parseInt(text(exchange.resolve(role + ".port"))));
    }

    private static void awaitNonEmpty(Path file, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline
                && (!Files.exists(file) || Files.size(file) == 0L)) {
            Thread.sleep(10L);
        }
        assertTrue(Files.exists(file) && Files.size(file) > 0L,
                "process did not publish " + file);
    }

    private static void assertExit(Process process, Path log)
            throws Exception {
        boolean exited = process.waitFor(10L, TimeUnit.SECONDS);
        if (!exited) process.destroyForcibly();
        String output = Files.exists(log) ? text(log) : "missing log";
        assertTrue(exited, output);
        assertEquals(0, process.exitValue(), output);
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
                .trim();
    }
}
