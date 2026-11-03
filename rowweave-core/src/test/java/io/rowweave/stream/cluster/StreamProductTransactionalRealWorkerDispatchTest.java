package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.runtime.FileCheckpointAuthority;
import io.rowweave.stream.runtime.FileCheckpointMemberStore;
import io.rowweave.stream.runtime.FileStreamSavepointStore;
import io.rowweave.stream.runtime.InMemoryStreamCheckpointAuthority;
import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public transactional connectors execute on real M+2W. */
@Tag("multiprocess")
class StreamProductTransactionalRealWorkerDispatchTest {

    @Test
    void boundedSourceFinishesTransactionAcrossIndependentWorkerJvms(
            @TempDir Path exchange) throws Exception {
        Files.write(exchange.resolve("transactional-bounded-source"),
                new byte[] {1});
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("transactional-source.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("bounded-run-authority"), 1L,
                            "bounded-master-incarnation",
                            Arrays.asList(
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "source")),
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "sink"))),
                            exchange.resolve("bounded-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("bounded-job-authority"),
                                 runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamJobHandle job = session(client, true).submit(
                        StreamSubmissionId.of("bounded-m2w-submit"));
                assertEquals(StreamJobStatus.FINISHED,
                        job.awaitTermination(10_000L));
                awaitFile(exchange.resolve("transactional-result-0"), 5_000L);
                awaitFile(exchange.resolve("transactional-result-1"), 5_000L);
                assertEquals(1L, job.progress().completedCheckpoints());
                StreamJobEvidence evidence = job.evidence();
                assertTrue(evidence.available());
                assertTrue(evidence.complete(), () -> "incomplete terminal evidence: "
                        + evidence.reportedTaskCount() + "/"
                        + evidence.placementTaskCount());
                assertTrue(evidence.latestCompletedCheckpointId()
                        .startsWith("stream-checkpoint:"));
                assertEquals(1L,
                        evidence.latestCompletedCheckpointSequence());
                assertTrue(evidence.latestCheckpointBytes() > 0L);
                assertTrue(source.isAlive());
                assertTrue(sink.isAlive());
            }
        } finally {
            Files.write(exchange.resolve("transactional-shutdown"),
                    new byte[] {1});
            boolean sourceExited = source.waitFor(5L, TimeUnit.SECONDS);
            boolean sinkExited = sink.waitFor(5L, TimeUnit.SECONDS);
            source.destroyForcibly();
            sink.destroyForcibly();
            assertTrue(sourceExited, () -> log(exchange, "source"));
            assertTrue(sinkExited, () -> log(exchange, "sink"));
        }
    }

    @Test
    void boundedSourceWorkerLossBeforeEndCannotPublishFinished(
            @TempDir Path exchange) throws Exception {
        Files.write(exchange.resolve("transactional-bounded-source"),
                new byte[] {1});
        Files.write(exchange.resolve("transactional-pause-before-end"),
                new byte[] {1});
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("transactional-source.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("loss-run-authority"), 1L,
                            "loss-master-incarnation",
                            Arrays.asList(
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "source")),
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "sink"))),
                            exchange.resolve("loss-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("loss-job-authority"), runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamJobHandle job = session(client, true).submit(
                        StreamSubmissionId.of("bounded-loss-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve(
                        "transactional-source-before-end"), 5_000L);
                source.destroyForcibly();
                assertTrue(source.waitFor(5L, TimeUnit.SECONDS));
                assertEquals(StreamJobStatus.FAILED,
                        job.awaitTermination(10_000L));
                assertFalse(Files.exists(
                        exchange.resolve("transactional-result-0")));
                assertFalse(Files.exists(
                        exchange.resolve("transactional-result-1")));
                assertEquals(0L, job.progress().completedCheckpoints());
                assertTrue(sink.isAlive());
            }
        } finally {
            Files.write(exchange.resolve("transactional-shutdown"),
                    new byte[] {1});
            if (source.isAlive()) source.destroyForcibly();
            boolean sinkExited = sink.waitFor(5L, TimeUnit.SECONDS);
            sink.destroyForcibly();
            assertTrue(sinkExited, () -> log(exchange, "sink"));
        }
    }

    @Test
    void publicTransactionalSinkCommitsOnlyAtCheckpointAcrossTwoWorkerJvms(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("transactional-source.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink.port"), 5_000L);
            RemoteStreamWorkerEndpoint sourceEndpoint =
                    RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                            port(exchange, "source"));
            RemoteStreamWorkerEndpoint sinkEndpoint =
                    RemoteStreamWorkerEndpoint.connect("127.0.0.1",
                            port(exchange, "sink"));
            RemoteStreamSourceSinkRunFactory unavailable =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("run-authority-no-store"), 1L,
                            "transactional-no-store-incarnation",
                            Arrays.asList(sourceEndpoint, sinkEndpoint));
            try (StreamProductJobMasterServer rejectedMaster =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("job-authority-no-store"),
                                 unavailable);
                 StreamJobClient rejectedClient =
                         RowWeave.createRealClusterStreamJobClient(
                                 "127.0.0.1", rejectedMaster.port())) {
                assertThrows(StreamControlException.class,
                        () -> session(rejectedClient).submit(
                                StreamSubmissionId.of(
                                        "transactional-no-store-submit")));
                assertFalse(Files.exists(exchange.resolve(
                        "transactional-source-emitted")),
                        "missing checkpoint authority must reject before Worker launch");
            }
            InMemoryStreamClusterRunStateStore runStates =
                    new InMemoryStreamClusterRunStateStore();
            InMemoryStreamCheckpointAuthority checkpointAuthority =
                    new InMemoryStreamCheckpointAuthority();
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            runStates, checkpointAuthority, 1L,
                            "transactional-master-incarnation",
                            () -> Arrays.asList(
                                    sourceEndpoint, sinkEndpoint),
                            exchange.resolve("transactional-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("job-authority"), runs);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", master.port())) {
                StreamJobHandle job = session(client).submit(
                        StreamSubmissionId.of("transactional-m2w-submit"));

                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L),
                        () -> log(exchange, "source") + "\n"
                                + log(exchange, "sink"));
                awaitFile(exchange.resolve("transactional-source-emitted"),
                        5_000L);
                assertFalse(Files.exists(
                        exchange.resolve("transactional-result-0")),
                        "transactional rows must not be visible before checkpoint");
                assertFalse(Files.exists(
                        exchange.resolve("transactional-result-1")),
                        "every Sink subtask must remain private before checkpoint");

                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("transactional-m2w-cut"));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L));
                awaitFile(exchange.resolve("transactional-result-0"), 5_000L);
                awaitFile(exchange.resolve("transactional-result-1"), 5_000L);
                assertEquals("one", text(
                        exchange.resolve("transactional-result-0")));
                assertEquals("two", text(
                        exchange.resolve("transactional-result-1")));
                assertEquals(1L, job.progress().completedCheckpoints());
                assertEquals(CheckpointDecision.Status.COMPLETED,
                        checkpointAuthority.decisionsForRun(
                                job.id().value()).get(0).status());

                String current = ManagementFactory.getRuntimeMXBean().getName();
                String sourceIdentity = text(exchange.resolve(
                        "transactional-source.identity"));
                String sinkIdentity = text(exchange.resolve(
                        "transactional-sink.identity"));
                assertNotEquals(current, sourceIdentity);
                assertNotEquals(current, sinkIdentity);
                assertNotEquals(sourceIdentity, sinkIdentity);
                assertTrue(source.isAlive());
                assertTrue(sink.isAlive());

                job.cancel(StreamCommandId.of("transactional-m2w-cancel"))
                        .awaitCompletion(5_000L);
                assertEquals(StreamJobStatus.CANCELED,
                        job.awaitTermination(5_000L));
            }
        } finally {
            Files.write(exchange.resolve("transactional-shutdown"),
                    new byte[] {1});
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
    void checkpointTimeoutCannotPublishFailedBeforeLateTransactionConverges(
            @TempDir Path exchange) throws Exception {
        Files.write(exchange.resolve("transactional-long-delay-commit"),
                new byte[] {1});
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("transactional-source.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("run-authority"), 1L,
                            "transactional-timeout-master",
                            Arrays.asList(
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "source")),
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "sink"))),
                            exchange.resolve("transactional-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("job-authority"), runs);
                 StreamJobClient client =
                         RowWeave.createRealClusterStreamJobClient(
                                 "127.0.0.1", master.port())) {
                StreamSession delayed = session(client);
                delayed.setCheckpointConfig(
                        StreamCheckpointConfig.periodic(
                                60_000L, 500L, 2));
                StreamJobHandle job = delayed.submit(
                        StreamSubmissionId.of(
                                "transactional-timeout-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("transactional-source-emitted"),
                        5_000L);

                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("transactional-timeout-cut"));

                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(5_000L),
                        "COMMIT_DECIDED must reconcile instead of publishing FAILED");
                awaitFile(exchange.resolve("transactional-result-0"), 5_000L);
                awaitFile(exchange.resolve("transactional-result-1"), 5_000L);
                job.cancel(StreamCommandId.of(
                                "transactional-timeout-cancel"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            Files.write(exchange.resolve("transactional-shutdown"),
                    new byte[] {1});
            boolean sourceExited = source.waitFor(5L, TimeUnit.SECONDS);
            boolean sinkExited = sink.waitFor(5L, TimeUnit.SECONDS);
            source.destroyForcibly();
            sink.destroyForcibly();
            assertTrue(sourceExited, () -> log(exchange, "source"));
            assertTrue(sinkExited, () -> log(exchange, "sink"));
        }
    }

    @Test
    void replacementConvergesOldAttemptDecisionBeforeWorkerLossRestart(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        Process replacementSink = null;
        StreamProductJobMasterServer firstMaster = null;
        StreamProductJobMasterServer replacementMaster = null;
        try {
            awaitFile(exchange.resolve("transactional-source.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink.port"), 5_000L);
            Path runRoot = exchange.resolve("run-authority");
            Path jobRoot = exchange.resolve("job-authority");
            Path checkpointRoot = exchange.resolve("transactional-checkpoints");
            RemoteStreamWorkerEndpoint sourceEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", port(exchange, "source"));
            RemoteStreamWorkerEndpoint sinkEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", port(exchange, "sink"));
            firstMaster = new StreamProductJobMasterServer(jobRoot,
                    new RemoteStreamSourceSinkRunFactory(
                            runRoot, 1L, "pending-master-1",
                            Arrays.asList(sourceEndpoint, sinkEndpoint),
                            checkpointRoot));
            StreamJobId jobId;
            StreamCommandId pending = StreamCommandId.of(
                    "pending-before-master-sink-loss");
            try (StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", firstMaster.port())) {
                StreamSession configured = session(client);
                configured.setCheckpointConfig(StreamCheckpointConfig.periodic(
                        60_000L, 500L, 3));
                StreamJobHandle job = configured.submit(StreamSubmissionId.of(
                        "pending-before-master-sink-loss-submit"));
                jobId = job.id();
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("transactional-source-emitted"),
                        5_000L);
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        job.triggerCheckpoint(StreamCommandId.of(
                                "baseline-before-master-sink-loss"))
                                .awaitCompletion(5_000L));
                Files.write(exchange.resolve(
                                "transactional-long-delay-commit"),
                        new byte[] {1});
                StreamCheckpointHandle checkpoint =
                        job.triggerCheckpoint(pending);
                assertEquals(StreamOperationStatus.RUNNING,
                        checkpoint.status(),
                        "the injected timeout must leave a durable decision pending");
                awaitCommitDecision(checkpointRoot, jobId, 5_000L);
            }

            firstMaster.close();
            firstMaster = null;
            sink.destroyForcibly();
            assertTrue(sink.waitFor(5L, TimeUnit.SECONDS));
            Files.deleteIfExists(exchange.resolve(
                    "transactional-long-delay-commit"));
            replacementSink = launch("sink-b", exchange);
            awaitFile(exchange.resolve("transactional-sink-b.port"), 5_000L);

            replacementMaster = new StreamProductJobMasterServer(jobRoot,
                    new RemoteStreamSourceSinkRunFactory(
                            runRoot, 2L, "pending-master-2",
                            Arrays.asList(sourceEndpoint,
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "sink-b"))),
                            checkpointRoot));
            try (StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", replacementMaster.port())) {
                StreamJobHandle attached = client.attach(jobId);
                assertEquals(StreamJobStatus.RUNNING,
                        attached.awaitRunning(10_000L));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        client.attachCheckpoint(jobId, pending)
                                .awaitCompletion(5_000L));
                StreamClusterRunState state = awaitAttempt(
                        runRoot, jobId, "attempt-2", 10_000L);
                assertEquals(2L,
                        state.latestCompletedCheckpoint()
                                .checkpointSequence(),
                        "old-attempt decision must publish before attempt advance");
                assertEquals("attempt-2", state.attemptId());
                attached.cancel(StreamCommandId.of(
                                "cancel-after-pending-restart"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            if (firstMaster != null) firstMaster.close();
            if (replacementMaster != null) replacementMaster.close();
            Files.write(exchange.resolve("transactional-shutdown"),
                    new byte[] {1});
            if (source.isAlive()) source.destroyForcibly();
            if (sink.isAlive()) sink.destroyForcibly();
            if (replacementSink != null && replacementSink.isAlive()) {
                replacementSink.destroyForcibly();
            }
            source.waitFor(5L, TimeUnit.SECONDS);
            sink.waitFor(5L, TimeUnit.SECONDS);
            if (replacementSink != null) {
                replacementSink.waitFor(5L, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void masterOnlyReplacementRelaunchesAfterDecisionRecovery(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        StreamProductJobMasterServer firstMaster = null;
        StreamProductJobMasterServer replacementMaster = null;
        try {
            awaitFile(exchange.resolve("transactional-source.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink.port"), 5_000L);
            Path runRoot = exchange.resolve("run-authority");
            Path jobRoot = exchange.resolve("job-authority");
            Path checkpointRoot = exchange.resolve("transactional-checkpoints");
            RemoteStreamWorkerEndpoint sourceEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", port(exchange, "source"));
            RemoteStreamWorkerEndpoint sinkEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", port(exchange, "sink"));
            firstMaster = new StreamProductJobMasterServer(jobRoot,
                    new RemoteStreamSourceSinkRunFactory(
                            runRoot, 1L, "master-only-1",
                            Arrays.asList(sourceEndpoint, sinkEndpoint),
                            checkpointRoot));
            StreamJobId jobId;
            StreamCommandId pending = StreamCommandId.of(
                    "master-only-pending-cut");
            try (StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", firstMaster.port())) {
                StreamSession configured = session(client);
                configured.setCheckpointConfig(StreamCheckpointConfig.periodic(
                        60_000L, 500L, 3));
                StreamJobHandle job = configured.submit(StreamSubmissionId.of(
                        "master-only-pending-submit"));
                jobId = job.id();
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("transactional-source-emitted"),
                        5_000L);
                Files.write(exchange.resolve(
                                "transactional-long-delay-commit"),
                        new byte[] {1});
                assertEquals(StreamOperationStatus.RUNNING,
                        job.triggerCheckpoint(pending).status());
                awaitCommitDecision(checkpointRoot, jobId, 5_000L);
            }

            firstMaster.close();
            firstMaster = null;
            Files.deleteIfExists(exchange.resolve(
                    "transactional-long-delay-commit"));
            replacementMaster = new StreamProductJobMasterServer(jobRoot,
                    new RemoteStreamSourceSinkRunFactory(
                            runRoot, 2L, "master-only-2",
                            Arrays.asList(sourceEndpoint, sinkEndpoint),
                            checkpointRoot));
            try (StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", replacementMaster.port())) {
                StreamJobHandle attached = client.attach(jobId);
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        client.attachCheckpoint(jobId, pending)
                                .awaitCompletion(10_000L));
                assertEquals(StreamJobStatus.RUNNING,
                        attached.awaitRunning(10_000L),
                        "same-attempt relaunch must advance past recovery control");
                attached.cancel(StreamCommandId.of(
                                "master-only-replacement-cancel"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            if (firstMaster != null) firstMaster.close();
            if (replacementMaster != null) replacementMaster.close();
            Files.write(exchange.resolve("transactional-shutdown"),
                    new byte[] {1});
            if (source.isAlive()) source.destroyForcibly();
            if (sink.isAlive()) sink.destroyForcibly();
            source.waitFor(5L, TimeUnit.SECONDS);
            sink.waitFor(5L, TimeUnit.SECONDS);
        }
    }

    @Test
    void replacementRepairsCompletedReferenceBeforeRunPointer(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        StreamProductJobMasterServer firstMaster = null;
        StreamProductJobMasterServer replacementMaster = null;
        try {
            awaitFile(exchange.resolve("transactional-source.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink.port"), 5_000L);
            Path runRoot = exchange.resolve("run-authority");
            Path jobRoot = exchange.resolve("job-authority");
            Path checkpointRoot = exchange.resolve("transactional-checkpoints");
            RemoteStreamWorkerEndpoint sourceEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", port(exchange, "source"));
            RemoteStreamWorkerEndpoint sinkEndpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            "127.0.0.1", port(exchange, "sink"));
            firstMaster = new StreamProductJobMasterServer(jobRoot,
                    new RemoteStreamSourceSinkRunFactory(
                            runRoot, 1L, "before-pointer-1",
                            Arrays.asList(sourceEndpoint, sinkEndpoint),
                            checkpointRoot));
            StreamJobId jobId;
            StreamCommandId pending = StreamCommandId.of(
                    "completed-before-pointer-cut");
            try (StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", firstMaster.port())) {
                StreamSession configured = session(client);
                configured.setCheckpointConfig(StreamCheckpointConfig.periodic(
                        60_000L, 500L, 3));
                StreamJobHandle job = configured.submit(StreamSubmissionId.of(
                        "completed-before-pointer-submit"));
                jobId = job.id();
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("transactional-source-emitted"),
                        5_000L);
                Files.write(exchange.resolve(
                                "transactional-long-delay-commit"),
                        new byte[] {1});
                assertEquals(StreamOperationStatus.RUNNING,
                        job.triggerCheckpoint(pending).status());
                awaitCommitDecision(checkpointRoot, jobId, 5_000L);
            }
            firstMaster.close();
            firstMaster = null;
            Files.deleteIfExists(exchange.resolve(
                    "transactional-long-delay-commit"));

            FileStreamProductJobAuthority jobs =
                    new FileStreamProductJobAuthority(jobRoot);
            StreamProductJobSnapshot snapshot = jobs.byJob(jobId);
            StreamClusterRunState run =
                    new FileStreamClusterRunAuthority(runRoot)
                            .current(jobId.value());
            StreamProductExecutionGraph admitted =
                    StreamProductExecutionGraph.from(snapshot.artifact());
            FileCheckpointAuthority decisions = new FileCheckpointAuthority(
                    checkpointRoot.resolve("authority"));
            CheckpointDecision decided = null;
            for (CheckpointDecision candidate
                    : decisions.decisionsForRun(jobId.value())) {
                if (candidate.status()
                        == CheckpointDecision.Status.COMMIT_DECIDED) {
                    decided = candidate;
                }
            }
            assertTrue(decided != null,
                    "fixture must expose the complete-before-pointer window");
            CheckpointIdentity identity = decided.checkpointIdentity();
            byte[] checkpointBundle = StreamCheckpointTransferBundleCodec
                    .encode(decided,
                            decisions.manifest(decided.manifestDigest()),
                            new FileCheckpointMemberStore(checkpointRoot)
                                    .resolve(decided, decisions));
            long sequence = Math.addExact(identity.checkpointSequence(), 1L);
            for (StreamWorkerPlacement.Assignment assignment
                    : run.placement().assignments()) {
                PortableStreamNode node = admitted.executionGraph
                        .node(assignment.operatorUid()).portableNode();
                if (node.opcode() != StreamOperatorCode.SINK) continue;
                RemoteStreamWorkerEndpoint endpoint = assignment.workerId()
                        .equals(sourceEndpoint.profile().workerId())
                        ? sourceEndpoint : sinkEndpoint;
                byte[] control = StreamControlMessageCodec.encode(
                        new StreamControlMessage(
                                StreamControlMessage.Kind.CHECKPOINT_TRIGGER,
                                run.coordinatorTerm(),
                                run.coordinatorIncarnation(), run.runId(),
                                identity.attemptId(), identity.fenceToken(),
                                run.assignmentGeneration(),
                                assignment.workerIncarnation(), sequence,
                                admitted.commandDigest,
                                StreamCheckpointControlCodec.encode(identity)));
                endpoint.recoverCheckpoint(control,
                        PlanArtifactCodec.encode(snapshot.artifact()),
                        assignment.operatorUid(), assignment.subtaskIndex(),
                        5_000, checkpointBundle);
            }
            CheckpointDecision completed = decisions.complete(
                    CheckpointDecision.completed(decided));
            String reference = snapshot.checkpoint(pending).checkpointId();
            new FileStreamSavepointStore(checkpointRoot).create(
                    reference, identity, decisions);
            assertTrue(new FileStreamClusterRunAuthority(runRoot)
                            .current(jobId.value())
                            .latestCompletedCheckpoint() == null,
                    "the injected crash window must precede run-pointer publication");

            replacementMaster = new StreamProductJobMasterServer(jobRoot,
                    new RemoteStreamSourceSinkRunFactory(
                            runRoot, 2L, "before-pointer-2",
                            Arrays.asList(sourceEndpoint, sinkEndpoint),
                            checkpointRoot));
            try (StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                    "127.0.0.1", replacementMaster.port())) {
                StreamJobHandle attached = client.attach(jobId);
                awaitFile(exchange.resolve("transactional-source-open-2"),
                        5_000L);
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        client.attachCheckpoint(jobId, pending)
                                .awaitCompletion(10_000L));
                assertEquals(identity,
                        new FileStreamClusterRunAuthority(runRoot)
                                .current(jobId.value())
                                .latestCompletedCheckpoint(),
                        "reference publication must repair the run pointer before replay");
                assertEquals(StreamJobStatus.RUNNING,
                        attached.awaitRunning(10_000L));
                attached.cancel(StreamCommandId.of(
                                "before-pointer-replacement-cancel"))
                        .awaitCompletion(5_000L);
            }
            assertEquals(CheckpointDecision.Status.COMPLETED,
                    completed.status());
        } finally {
            if (firstMaster != null) firstMaster.close();
            if (replacementMaster != null) replacementMaster.close();
            Files.write(exchange.resolve("transactional-shutdown"),
                    new byte[] {1});
            if (source.isAlive()) source.destroyForcibly();
            if (sink.isAlive()) sink.destroyForcibly();
            source.waitFor(5L, TimeUnit.SECONDS);
            sink.waitFor(5L, TimeUnit.SECONDS);
        }
    }

    @Test
    void sameMasterConvergesOldDecisionBeforeRestartingASurvivingSink(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process spareSource = launch("source-b", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("transactional-source.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-source-b.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink.port"), 5_000L);
            Path runRoot = exchange.resolve("run-authority");
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            runRoot, 1L, "same-master",
                            Arrays.asList(
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "source")),
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "sink")),
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "source-b"))),
                            exchange.resolve("transactional-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("job-authority"), runs);
                 StreamJobClient client =
                         RowWeave.createRealClusterStreamJobClient(
                                 "127.0.0.1", master.port())) {
                StreamSession configured = session(client);
                configured.setCheckpointConfig(StreamCheckpointConfig.periodic(
                        60_000L, 500L, 3));
                configured.setRestartPolicy(StreamRestartPolicy.fixedDelay(
                        1, 0L));
                StreamJobHandle job = configured.submit(StreamSubmissionId.of(
                        "same-master-pending-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("transactional-source-emitted"),
                        5_000L);
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        job.triggerCheckpoint(StreamCommandId.of(
                                "same-master-baseline"))
                                .awaitCompletion(5_000L));
                Files.write(exchange.resolve(
                                "transactional-long-delay-commit"),
                        new byte[] {1});
                StreamCommandId pending = StreamCommandId.of(
                        "same-master-pending-cut");
                StreamCheckpointHandle checkpoint =
                        job.triggerCheckpoint(pending);
                assertEquals(StreamOperationStatus.RUNNING,
                        checkpoint.status());

                source.destroyForcibly();
                assertTrue(source.waitFor(5L, TimeUnit.SECONDS));
                Files.deleteIfExists(exchange.resolve(
                        "transactional-long-delay-commit"));

                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(10_000L));
                StreamClusterRunState state = awaitAttempt(runRoot, job.id(),
                        "attempt-2", 10_000L);
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(10_000L),
                        () -> log(exchange, "source-b") + "\n"
                                + log(exchange, "sink"));
                assertEquals(2L,
                        state.latestCompletedCheckpoint()
                                .checkpointSequence());
                assertEquals("attempt-2", state.attemptId());
                job.cancel(StreamCommandId.of("same-master-cancel"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            Files.write(exchange.resolve("transactional-shutdown"),
                    new byte[] {1});
            if (source.isAlive()) source.destroyForcibly();
            if (spareSource.isAlive()) spareSource.destroyForcibly();
            if (sink.isAlive()) sink.destroyForcibly();
            source.waitFor(5L, TimeUnit.SECONDS);
            spareSource.waitFor(5L, TimeUnit.SECONDS);
            sink.waitFor(5L, TimeUnit.SECONDS);
        }
    }

    @Test
    void restartBudgetCannotRetireAnUnpublishedDecisionOrDeadFirstProvider(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process sink = launch("sink", exchange);
        Process spareSink = launch("sink-b", exchange);
        try {
            awaitFile(exchange.resolve("transactional-source.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink-b.port"), 5_000L);
            Path runRoot = exchange.resolve("run-authority");
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            runRoot, 1L, "budget-terminal-master",
                            Arrays.asList(
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "source")),
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "sink")),
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "sink-b"))),
                            exchange.resolve("transactional-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("job-authority"), runs);
                 StreamJobClient client =
                         RowWeave.createRealClusterStreamJobClient(
                                 "127.0.0.1", master.port())) {
                StreamSession configured = session(client);
                configured.setCheckpointConfig(StreamCheckpointConfig.periodic(
                        60_000L, 500L, 3));
                configured.setRestartPolicy(StreamRestartPolicy.fixedDelay(
                        0, 0L));
                StreamJobHandle job = configured.submit(StreamSubmissionId.of(
                        "budget-terminal-pending-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("transactional-source-emitted"),
                        5_000L);
                Files.write(exchange.resolve(
                                "transactional-long-delay-commit"),
                        new byte[] {1});
                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("budget-terminal-pending-cut"));
                assertEquals(StreamOperationStatus.RUNNING,
                        checkpoint.status());

                sink.destroyForcibly();
                assertTrue(sink.waitFor(5L, TimeUnit.SECONDS));
                Files.deleteIfExists(exchange.resolve(
                        "transactional-long-delay-commit"));

                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(10_000L),
                        "durable decision must publish before restart budget failure");
                assertEquals(StreamJobStatus.FAILED,
                        job.awaitTermination(10_000L));
            }
        } finally {
            Files.write(exchange.resolve("transactional-shutdown"),
                    new byte[] {1});
            if (source.isAlive()) source.destroyForcibly();
            if (sink.isAlive()) sink.destroyForcibly();
            if (spareSink.isAlive()) spareSink.destroyForcibly();
            source.waitFor(5L, TimeUnit.SECONDS);
            sink.waitFor(5L, TimeUnit.SECONDS);
            spareSink.waitFor(5L, TimeUnit.SECONDS);
        }
    }

    @Test
    void cancelDuringRestartKeepsDurableDecisionPendingUntilRecovery(
            @TempDir Path exchange) throws Exception {
        Process source = launch("source", exchange);
        Process spareSource = launch("source-b", exchange);
        Process sink = launch("sink", exchange);
        try {
            awaitFile(exchange.resolve("transactional-source.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-source-b.port"), 5_000L);
            awaitFile(exchange.resolve("transactional-sink.port"), 5_000L);
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            exchange.resolve("run-authority"), 1L,
                            "cancel-restarting-master",
                            Arrays.asList(
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "source")),
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "sink")),
                                    RemoteStreamWorkerEndpoint.connect(
                                            "127.0.0.1",
                                            port(exchange, "source-b"))),
                            exchange.resolve("transactional-checkpoints"));
            try (StreamProductJobMasterServer master =
                         new StreamProductJobMasterServer(
                                 exchange.resolve("job-authority"), runs);
                 StreamJobClient client =
                         RowWeave.createRealClusterStreamJobClient(
                                 "127.0.0.1", master.port())) {
                StreamSession configured = session(client);
                configured.setCheckpointConfig(StreamCheckpointConfig.periodic(
                        60_000L, 500L, 3));
                configured.setRestartPolicy(StreamRestartPolicy.fixedDelay(
                        1, 2_000L));
                StreamJobHandle job = configured.submit(StreamSubmissionId.of(
                        "cancel-restarting-pending-submit"));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(5_000L));
                awaitFile(exchange.resolve("transactional-source-emitted"),
                        5_000L);
                Files.write(exchange.resolve(
                                "transactional-long-delay-commit"),
                        new byte[] {1});
                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("cancel-restarting-pending-cut"));
                assertEquals(StreamOperationStatus.RUNNING,
                        checkpoint.status());
                source.destroyForcibly();
                assertTrue(source.waitFor(5L, TimeUnit.SECONDS));
                awaitStatus(job, StreamJobStatus.RESTARTING, 5_000L);

                assertThrows(StreamControlException.class,
                        () -> job.cancel(StreamCommandId.of(
                                "cancel-while-decision-pending")));
                assertEquals(StreamOperationStatus.RUNNING,
                        checkpoint.status(),
                        "cancel must not rewrite an irreversible cut as FAILED");
                Files.deleteIfExists(exchange.resolve(
                        "transactional-long-delay-commit"));

                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.awaitCompletion(10_000L));
                assertEquals(StreamJobStatus.RUNNING,
                        job.awaitRunning(10_000L));
                job.cancel(StreamCommandId.of(
                                "cancel-after-decision-recovery"))
                        .awaitCompletion(5_000L);
            }
        } finally {
            Files.write(exchange.resolve("transactional-shutdown"),
                    new byte[] {1});
            if (source.isAlive()) source.destroyForcibly();
            if (spareSource.isAlive()) spareSource.destroyForcibly();
            if (sink.isAlive()) sink.destroyForcibly();
            source.waitFor(5L, TimeUnit.SECONDS);
            spareSource.waitFor(5L, TimeUnit.SECONDS);
            sink.waitFor(5L, TimeUnit.SECONDS);
        }
    }

    private static CheckpointDecision awaitCommitDecision(
            Path checkpointRoot, StreamJobId jobId, long timeoutMillis)
            throws InterruptedException {
        FileCheckpointAuthority decisions = new FileCheckpointAuthority(
                checkpointRoot.resolve("authority"));
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        do {
            for (CheckpointDecision candidate
                    : decisions.decisionsForRun(jobId.value())) {
                if (candidate.status()
                        == CheckpointDecision.Status.COMMIT_DECIDED) {
                    return candidate;
                }
            }
            Thread.sleep(10L);
        } while (System.nanoTime() < deadline);
        throw new AssertionError(
                "checkpoint did not reach a durable commit decision");
    }

    private static StreamClusterRunState awaitAttempt(
            Path runRoot, StreamJobId jobId, String expected,
            long timeoutMillis) throws InterruptedException {
        FileStreamClusterRunAuthority runs =
                new FileStreamClusterRunAuthority(runRoot);
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        StreamClusterRunState current;
        do {
            current = runs.current(jobId.value());
            if (expected.equals(current.attemptId())) return current;
            Thread.sleep(10L);
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Stream attempt did not advance to "
                + expected + "; actual=" + current.attemptId());
    }

    private static StreamSession session(StreamJobClient client) {
        return session(client, false);
    }

    private static StreamSession session(
            StreamJobClient client, boolean bounded) {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("value", "value", "value",
                        DataType.StringType, false)));
        StreamSession session = RowWeave.createStreamSession(
                "transactional-m2w", "transactional-m2w", client);
        session.setParallelism(2);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                60_000L, 5_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        StreamSplitSpec split = new StreamSplitSpec(
                StreamProductTransactionalWorkerProcessFixture
                        .digest("transactional-split"),
                "int32-v1", new byte[] {2});
        StreamSourceSpec source = bounded
                ? StreamSourceSpec.boundedExactOrdered(schema,
                StreamProductTransactionalWorkerProcessFixture.SOURCE,
                StreamProductTransactionalWorkerProcessFixture
                        .SOURCE_CONTRACT, new byte[] {1},
                Collections.singletonList(split))
                : StreamSourceSpec.exactOrdered(schema,
                StreamProductTransactionalWorkerProcessFixture.SOURCE,
                StreamProductTransactionalWorkerProcessFixture
                        .SOURCE_CONTRACT, new byte[] {1},
                Collections.singletonList(split));
        session.fromSource(source)
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        StreamProductTransactionalWorkerProcessFixture.SINK,
                        StreamProductTransactionalWorkerProcessFixture
                                .SINK_CONTRACT,
                        new byte[] {3}));
        return session;
    }

    private static Process launch(String role, Path exchange) throws Exception {
        return new ProcessBuilder(ForkedJvm.command(
                StreamProductTransactionalWorkerProcessFixture.class.getName(),
                role, exchange.toString()))
                .redirectErrorStream(true)
                .redirectOutput(exchange.resolve(
                        "transactional-" + role + ".log").toFile())
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

    private static int port(Path exchange, String role) throws Exception {
        return Integer.parseInt(text(exchange.resolve(
                "transactional-" + role + ".port")));
    }

    private static void awaitStatus(StreamJobHandle job,
            StreamJobStatus expected, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (job.status() != expected && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(expected, job.status());
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static String log(Path exchange, String role) {
        try {
            return text(exchange.resolve(
                    "transactional-" + role + ".log"));
        } catch (Exception ignored) {
            return "missing process log";
        }
    }
}
