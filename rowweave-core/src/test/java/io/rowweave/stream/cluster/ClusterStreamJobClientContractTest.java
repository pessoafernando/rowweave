package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamCommandHandle;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamJobClientLifecycleContract;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public Cluster handles use durable Master control RPC. */
class ClusterStreamJobClientContractTest {

    @Test
    void oneJobsCheckpointCannotBlockAnotherJobsSubmission(
            @TempDir Path root) throws Exception {
        BlockingCheckpointRunFactory runs = new BlockingCheckpointRunFactory();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobHandle first = session("blocking-cut", client).submit(
                    StreamSubmissionId.of("blocking-cut-submit"));
            assertEquals(StreamJobStatus.RUNNING, first.awaitRunning(5_000L));
            ExecutorService requests = Executors.newFixedThreadPool(2);
            try {
                requests.submit(() -> first.triggerCheckpoint(
                        StreamCommandId.of("blocking-cut-command")));
                assertTrue(runs.checkpointEntered.await(5L, TimeUnit.SECONDS));
                // Let the recovery loop inspect the in-flight cut. Old code
                // enters failure() and blocks; isolated code skips that probe.
                runs.recoveryProbeEntered.await(1L, TimeUnit.SECONDS);
                Future<StreamJobHandle> second = requests.submit(
                        () -> session("independent-job", client).submit(
                                StreamSubmissionId.of("independent-submit")));
                assertEquals(StreamJobStatus.RUNNING,
                        second.get(2L, TimeUnit.SECONDS)
                                .awaitRunning(2_000L));
            } finally {
                runs.releaseCheckpoint.countDown();
                requests.shutdown();
                if (!requests.awaitTermination(5L, TimeUnit.SECONDS)) {
                    requests.shutdownNow();
                }
            }
        }
    }

    @Test
    void configuredRestartPolicyRecoversOperatorFailure(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamSession configured = session("operator-restart", client);
            configured.setRestartPolicy(StreamRestartPolicy.fixedDelay(
                    1, 0L));
            StreamJobHandle job = configured.submit(
                    StreamSubmissionId.of("operator-restart-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));

            runs.failure = new IllegalStateException(
                    "injected operator failure");

            long deadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.SECONDS.toNanos(5L);
            while (runs.restarts.get() == 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(1, runs.restarts.get());
            assertEquals(StreamJobStatus.RUNNING, job.status());
            assertEquals(1, job.progress().restarts());
        }
    }

    @Test
    void failedRunRecoveryDoesNotWaitForAdvisoryProgressSample(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.blockProgressAfterFailure = true;
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamSession configured = session(
                    "failed-progress-must-not-fence-recovery", client);
            configured.setRestartPolicy(StreamRestartPolicy.fixedDelay(
                    1, 0L));
            StreamJobHandle job = configured.submit(StreamSubmissionId.of(
                    "failed-progress-must-not-fence-recovery-submit"));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(5_000L));

            runs.failure = new IllegalStateException("injected worker loss");
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(2L);
            try {
                while (runs.restarts.get() == 0
                        && System.nanoTime() < deadline) {
                    Thread.sleep(10L);
                }
                assertEquals(1, runs.restarts.get(),
                        "a failed run must restart without waiting for "
                                + "advisory progress sampling");
                assertEquals(0, runs.progressAfterFailureCalls.get(),
                        "failure recovery must not sample a run whose "
                                + "progress path may be blocked");
            } finally {
                runs.releaseBlockedProgress.countDown();
            }
        }
    }

    @Test
    void blockedCheckpointSocketDoesNotBlockDurableStatusRead(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.blockCheckpoint = new CountDownLatch(1);
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobHandle job = session(
                    "socket-status-responsive", client).submit(
                    StreamSubmissionId.of(
                            "socket-status-responsive-submit"));
            AtomicReference<Throwable> checkpointFailure =
                    new AtomicReference<>();
            Thread checkpoint = new Thread(() -> {
                try {
                    job.triggerCheckpoint(StreamCommandId.of(
                            "socket-status-responsive-checkpoint"));
                } catch (Throwable failure) {
                    checkpointFailure.set(failure);
                }
            }, "socket-blocked-checkpoint");
            checkpoint.start();
            assertTrue(runs.checkpointEntered.await(1L, TimeUnit.SECONDS),
                    "checkpoint RPC must reach the blocking Worker cut");

            AtomicReference<StreamJobStatus> observed =
                    new AtomicReference<>();
            AtomicReference<Throwable> statusFailure =
                    new AtomicReference<>();
            Thread status = new Thread(() -> {
                try {
                    observed.set(job.status());
                } catch (Throwable failure) {
                    statusFailure.set(failure);
                }
            }, "socket-durable-status-read");
            status.start();
            status.join(500L);
            boolean responsive = !status.isAlive();

            runs.blockCheckpoint.countDown();
            checkpoint.join(5_000L);
            status.join(5_000L);
            assertTrue(responsive,
                    "an independent status RPC must not queue behind a "
                            + "blocking checkpoint connection");
            assertEquals(null, checkpointFailure.get());
            assertEquals(null, statusFailure.get());
            assertEquals(StreamJobStatus.RUNNING, observed.get());
        } finally {
            runs.blockCheckpoint.countDown();
        }
    }

    @Test
    void submitAttachStatusAndCancelConvergeThroughMasterSocket(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobClientLifecycleContract.verify(
                    client, session("network-job", client), "real-cluster");
            assertEquals(1, runs.starts.get());
            assertEquals(1, runs.cancels.get());
            assertEquals(1, runs.checkpoints.get());
        }
    }

    @Test
    void finiteAwaitTimeoutDoesNotCancelOrChangeDurableStatus(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobHandle handle = session("await-job", client).submit(
                    StreamSubmissionId.of("await-submit"));

            StreamControlException timeout = assertThrows(
                    StreamControlException.class,
                    () -> handle.awaitTermination(10L));
            assertEquals(StreamControlException.Code.WAIT_TIMEOUT,
                    timeout.code());
            assertEquals(StreamJobStatus.RUNNING, handle.status());
            assertEquals(0, runs.cancels.get());
        }
    }

    @Test
    void conflictingSubmissionIsRejectedWithoutStartingSecondRun(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamSubmissionId submission = StreamSubmissionId.of("conflict-submit");
            session("definition-a", client).submit(submission);

            StreamControlException conflict = assertThrows(
                    StreamControlException.class,
                    () -> session("definition-b", client).submit(submission));
            assertEquals(StreamControlException.Code.SUBMISSION_CONFLICT,
                    conflict.code());
            assertEquals(1, runs.starts.get());
        }
    }

    @Test
    void submitAckLossConvergesByClientOwnedSubmissionIdentity(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamSubmissionId submission =
                    StreamSubmissionId.of("submit-ack-loss");
            StreamSession session = session("ack-loss-job", client);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(
                        "127.0.0.1", master.port()), 5_000);
                DataOutputStream output = new DataOutputStream(
                        new BufferedOutputStream(socket.getOutputStream()));
                StreamProductControlWire.writeRequest(output,
                        StreamProductControlWire.Request.submit(
                                submission.value(), PlanArtifactCodec.encode(
                                session.toPlanArtifact())));
                // Close without reading the response: outcome is deliberately unknown.
            }

            StreamJobHandle attached = eventuallyAttached(client, submission);
            StreamJobHandle retried = session.submit(submission);
            assertEquals(attached.id(), retried.id());
            assertEquals(1, runs.starts.get());
        }
    }

    @Test
    void replacementMasterRehydratesAcceptedRunFromDurableAuthority(
            @TempDir Path root) throws Exception {
        StreamSubmissionId submission =
                StreamSubmissionId.of("master-replacement-submit");
        RecordingRunFactory firstRuns = new RecordingRunFactory();
        io.rowweave.stream.api.StreamJobId jobId;
        try (StreamProductJobMasterServer first =
                     new StreamProductJobMasterServer(root, firstRuns);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", first.port())) {
            jobId = session("replacement-job", client)
                    .submit(submission).id();
        }

        RecordingRunFactory replacementRuns = new RecordingRunFactory();
        try (StreamProductJobMasterServer replacement =
                     new StreamProductJobMasterServer(root, replacementRuns);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", replacement.port())) {
            StreamJobHandle attached = client.attachSubmission(submission);
            assertEquals(jobId, attached.id());
            assertEquals(StreamJobStatus.RUNNING, attached.status());
            assertEquals(1, firstRuns.starts.get());
            // A durable RUNNING read is answered without touching the recovery
            // path, so the replacement Master materializes the accepted run on
            // its own recovery pass. Await that pass, let a few more passes run,
            // then require exactly one start: neither zero rehydration nor a
            // second materialization by a later pass survives this assertion.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
            while (replacementRuns.starts.get() == 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            Thread.sleep(200L);
            assertEquals(1, replacementRuns.starts.get(),
                    "the replacement Master must rehydrate the accepted run "
                            + "exactly once from durable authority");
        }
    }

    @Test
    void handleReadsDurableOperationalEvidenceAcrossTheControlBoundary(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobHandle handle = session("evidence-job", client).submit(
                    StreamSubmissionId.of("evidence-submit"));
            StreamJobProgress progress = new StreamJobProgress(
                    0, 0L, 42L,
                    io.rowweave.stream.api.StreamCdcPhase.LOG, 0,
                    10L, 9L, 100L, 90L, 1L, 10L,
                    2, 20L, 30L, 3, 1, 4L);
            StreamJobEvidence expected = new StreamJobEvidence(
                    handle.id(), "run-control", "attempt-control", 0,
                    3L, 2L, 1L, null, 0L, 0L, 0L,
                    progress, progress, 2, 2, 1_000L);
            new FileStreamProductJobAuthority(root).updateEvidence(
                    handle.id(), expected);

            assertEquals(expected, handle.evidence());
            assertEquals(expected, client.attach(handle.id()).evidence());
        }
    }

    @Test
    void masterSamplesRunProgressIntoDurableReconnectableEvidence(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        StreamJobProgress observed = new StreamJobProgress(
                0, 0L, 77L,
                io.rowweave.stream.api.StreamCdcPhase.NOT_APPLICABLE, 0,
                5L, 4L, 50L, 40L, 2L, 20L,
                3, 30L, 10L, 1, 2, 4L);
        runs.progress = new StreamRunProgressSnapshot(
                "run-observed", "attempt-observed", 3L, 2L, 1L,
                new StreamTaskProgressAggregate(observed, 2, 1));
        io.rowweave.stream.api.StreamJobId jobId;
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobHandle handle = session("sampled-evidence", client)
                    .submit(StreamSubmissionId.of("sampled-evidence-submit"));
            jobId = handle.id();
            StreamJobEvidence evidence = awaitAvailableEvidence(handle);
            assertEquals("run-observed", evidence.runId());
            assertEquals(2, evidence.placementTaskCount());
            assertEquals(1, evidence.reportedTaskCount());
            assertFalse(evidence.complete());
            assertEquals(5L, evidence.attemptProgress().recordsRead());
            assertEquals(4L, evidence.lifetimeProgress().recordsWritten());
        }
        assertTrue(new FileStreamProductJobAuthority(root)
                .byJob(jobId).evidence().available());
    }

    @Test
    void terminalTransitionForcesFinalDurableProgressSample(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.progress = progressSnapshot(2L, 1L);
        io.rowweave.stream.api.StreamJobId jobId;
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobHandle handle = session("terminal-evidence", client)
                    .submit(StreamSubmissionId.of("terminal-evidence-submit"));
            jobId = handle.id();
            awaitAvailableEvidence(handle);
            runs.progress = progressSnapshot(9L, 7L);
            runs.finished = true;

            assertEquals(StreamJobStatus.FINISHED,
                    handle.awaitTermination(5_000L));
            assertEquals(9L, handle.evidence()
                    .attemptProgress().recordsRead());
            assertEquals(7L, handle.evidence()
                    .attemptProgress().recordsWritten());
        }

        assertEquals(9L, new FileStreamProductJobAuthority(root)
                .byJob(jobId).evidence().attemptProgress().recordsRead());
    }

    @Test
    void checkpointCommandIsDurableReconnectableAndIdempotent(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        StreamCommandId command = StreamCommandId.of("checkpoint-command-1");
        io.rowweave.stream.api.StreamJobId jobId;
        String checkpointId;
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobHandle job = session("checkpoint-job", client).submit(
                    StreamSubmissionId.of("checkpoint-submit"));
            jobId = job.id();
            StreamCheckpointHandle checkpoint =
                    job.triggerCheckpoint(command);
            checkpointId = checkpoint.checkpointId();
            assertTrue(checkpointId.startsWith("stream-checkpoint:"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(1L, job.progress().completedCheckpoints());
            assertEquals(checkpointId,
                    job.triggerCheckpoint(command).checkpointId());
            assertEquals(1, runs.checkpoints.get());
        }

        try (StreamProductJobMasterServer replacement =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", replacement.port())) {
            StreamCheckpointHandle attached =
                    client.attachCheckpoint(jobId, command);
            assertEquals(checkpointId, attached.checkpointId());
            assertEquals(StreamOperationStatus.SUCCEEDED, attached.status());
            assertEquals(1L,
                    client.attach(jobId).progress().completedCheckpoints());
        }
    }

    @Test
    void completedCheckpointEvidenceIsPublishedOnceAndSurvivesReplacement(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        StreamJobProgress observed = new StreamJobProgress(
                0, 0L, 17L,
                io.rowweave.stream.api.StreamCdcPhase.NOT_APPLICABLE, 0,
                1L, 1L, 10L, 10L, 0L, 0L,
                0, 0L, 5L, 0, 0, 0L);
        runs.progress = new StreamRunProgressSnapshot(
                "run-checkpoint-evidence", "attempt-checkpoint-evidence",
                3L, 2L, 1L,
                new StreamTaskProgressAggregate(observed, 1, 1));
        runs.checkpointDelayMillis = 10L;
        io.rowweave.stream.api.StreamJobId jobId;
        String checkpointId;
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobHandle job = session("checkpoint-evidence-job", client)
                    .submit(StreamSubmissionId.of(
                            "checkpoint-evidence-submit"));
            jobId = job.id();
            awaitAvailableEvidence(job);

            StreamCheckpointHandle completed = job.triggerCheckpoint(
                    StreamCommandId.of("checkpoint-evidence-success"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    completed.awaitCompletion(5_000L));
            checkpointId = completed.checkpointId();
            StreamJobEvidence evidence = job.evidence();
            assertEquals(checkpointId,
                    evidence.latestCompletedCheckpointId());
            assertEquals(1L,
                    evidence.latestCompletedCheckpointSequence());
            assertTrue(evidence.latestCheckpointDurationMillis() >= 5L);
            assertEquals(30L, evidence.latestCheckpointBytes());
            assertEquals(20L,
                    evidence.attemptProgress().managedStateBytes());

            runs.failCheckpoint = true;
            StreamCheckpointHandle failed = job.triggerCheckpoint(
                    StreamCommandId.of("checkpoint-evidence-failure"));
            assertEquals(StreamOperationStatus.FAILED,
                    failed.awaitCompletion(5_000L));
            assertEquals(checkpointId,
                    job.evidence().latestCompletedCheckpointId());
        }

        try (StreamProductJobMasterServer replacement =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", replacement.port())) {
            StreamJobEvidence recovered = client.attach(jobId).evidence();
            assertEquals(checkpointId,
                    recovered.latestCompletedCheckpointId());
            assertEquals(1L,
                    recovered.latestCompletedCheckpointSequence());
            assertEquals(30L, recovered.latestCheckpointBytes());
            assertTrue(recovered.latestCheckpointDurationMillis() >= 5L);
        }
    }

    @Test
    void checkpointAckLossConvergesWithoutExecutingASecondCut(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobHandle job = session("checkpoint-ack-job", client).submit(
                    StreamSubmissionId.of("checkpoint-ack-submit"));
            StreamCommandId command = StreamCommandId.of("checkpoint-ack-loss");
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(
                        "127.0.0.1", master.port()), 5_000);
                DataOutputStream output = new DataOutputStream(
                        new BufferedOutputStream(socket.getOutputStream()));
                StreamProductControlWire.writeRequest(output,
                        StreamProductControlWire.Request.triggerCheckpoint(
                                job.id().value(), command.value()));
            }

            StreamCheckpointHandle attached = eventuallyAttachedCheckpoint(
                    client, job.id(), command);
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    attached.awaitCompletion(5_000L));
            assertEquals(attached.checkpointId(),
                    job.triggerCheckpoint(command).checkpointId());
            assertEquals(1, runs.checkpoints.get());
        }
    }

    @Test
    void stopWithSavepointIsDurableReconnectableAndIdempotent(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        StreamCommandId command = StreamCommandId.of("savepoint-command-1");
        io.rowweave.stream.api.StreamJobId jobId;
        String savepointId;
        try (StreamProductJobMasterServer master =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", master.port())) {
            StreamJobHandle job = session("savepoint-job", client).submit(
                    StreamSubmissionId.of("savepoint-submit"));
            jobId = job.id();
            StreamSavepointRequest request = new StreamSavepointRequest(
                    "file-savepoint-v1", new byte[] {7, 8, 9});
            StreamSavepointHandle savepoint =
                    job.stopWithSavepoint(command, request);
            savepointId = savepoint.savepointId();

            assertTrue(savepointId.startsWith("stream-savepoint:"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    savepoint.awaitCompletion(5_000L));
            assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                    job.awaitTermination(5_000L));
            assertEquals(savepointId,
                    job.stopWithSavepoint(command, request).savepointId());
            StreamControlException conflict = assertThrows(
                    StreamControlException.class,
                    () -> job.stopWithSavepoint(command,
                            new StreamSavepointRequest(
                                    "file-savepoint-v1", new byte[] {9})));
            assertEquals(StreamControlException.Code.COMMAND_CONFLICT,
                    conflict.code());
            assertEquals(1, runs.savepoints.get());
        }

        try (StreamProductJobMasterServer replacement =
                     new StreamProductJobMasterServer(root, runs);
             StreamJobClient client = RowWeave.createClusterStreamJobClient(
                     "127.0.0.1", replacement.port())) {
            StreamSavepointHandle attached =
                    client.attachSavepoint(jobId, command);
            assertEquals(savepointId, attached.savepointId());
            assertEquals(StreamOperationStatus.SUCCEEDED, attached.status());
            assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT,
                    client.attach(jobId).status());
            assertEquals(1, runs.savepoints.get());
        }
    }

    private static StreamJobHandle eventuallyAttached(
            StreamJobClient client, StreamSubmissionId submission)
            throws Exception {
        long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.SECONDS.toNanos(5L);
        StreamControlException last = null;
        while (System.nanoTime() < deadline) {
            try {
                return client.attachSubmission(submission);
            } catch (StreamControlException failure) {
                last = failure;
                assertTrue(failure.code()
                        == StreamControlException.Code.JOB_NOT_FOUND);
                Thread.sleep(5L);
            }
        }
        if (last != null) throw last;
        throw new AssertionError("submission was not accepted");
    }

    private static StreamCheckpointHandle eventuallyAttachedCheckpoint(
            StreamJobClient client,
            io.rowweave.stream.api.StreamJobId jobId,
            StreamCommandId command) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(5L);
        StreamControlException last = null;
        while (System.nanoTime() < deadline) {
            try {
                return client.attachCheckpoint(jobId, command);
            } catch (StreamControlException unavailable) {
                last = unavailable;
                assertEquals(StreamControlException.Code.COMMAND_NOT_FOUND,
                        unavailable.code());
                Thread.sleep(5L);
            }
        }
        if (last != null) throw last;
        throw new AssertionError("checkpoint command was not accepted");
    }

    private static StreamSession session(String name, StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(name, name, client);
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

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static StreamJobEvidence awaitAvailableEvidence(
            StreamJobHandle handle) throws InterruptedException {
        long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.SECONDS.toNanos(5L);
        do {
            StreamJobEvidence evidence = handle.evidence();
            if (evidence.available()) return evidence;
            Thread.sleep(10L);
        } while (System.nanoTime() - deadline < 0L);
        throw new AssertionError("Stream evidence was not sampled");
    }

    private static StreamRunProgressSnapshot progressSnapshot(
            long recordsRead, long recordsWritten) {
        StreamJobProgress progress = new StreamJobProgress(
                0, 0L, 11L,
                io.rowweave.stream.api.StreamCdcPhase.NOT_APPLICABLE, 0,
                recordsRead, recordsWritten,
                Math.multiplyExact(recordsRead, 10L),
                Math.multiplyExact(recordsWritten, 10L),
                0L, 0L, 0, 0L, 0L, 0, 0, 0L);
        return new StreamRunProgressSnapshot(
                "run-terminal", "attempt-terminal", 2L, 1L, 1L,
                new StreamTaskProgressAggregate(progress, 1, 1));
    }

    private static final class RecordingRunFactory
            implements StreamProductRunFactory {
        private final AtomicInteger starts = new AtomicInteger();
        private final AtomicInteger cancels = new AtomicInteger();
        private final AtomicInteger checkpoints = new AtomicInteger();
        private final AtomicInteger savepoints = new AtomicInteger();
        private final AtomicInteger restarts = new AtomicInteger();
        private final AtomicInteger progressAfterFailureCalls =
                new AtomicInteger();
        private final CountDownLatch releaseBlockedProgress =
                new CountDownLatch(1);
        private final CountDownLatch checkpointEntered =
                new CountDownLatch(1);
        private volatile CountDownLatch blockCheckpoint;
        private volatile StreamRunProgressSnapshot progress;
        private volatile StreamCompletedCheckpointEvidence checkpointEvidence;
        private volatile long checkpointDelayMillis;
        private volatile boolean failCheckpoint;
        private volatile boolean finished;
        private volatile boolean blockProgressAfterFailure;
        private volatile Throwable failure;

        @Override
        public StreamProductRun create(StreamProductJobSnapshot snapshot) {
            return new StreamProductRun() {
                @Override public void start() { starts.incrementAndGet(); }
                @Override public void cancel() { cancels.incrementAndGet(); }
                @Override public void triggerCheckpoint(
                        StreamCommandId commandId, String checkpointId) {
                    checkpoints.incrementAndGet();
                    CountDownLatch blocker = blockCheckpoint;
                    if (blocker != null) {
                        checkpointEntered.countDown();
                        try {
                            blocker.await(5L, TimeUnit.SECONDS);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                    }
                    if (checkpointDelayMillis > 0L) {
                        try {
                            Thread.sleep(checkpointDelayMillis);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                    }
                    if (failCheckpoint) {
                        throw new IllegalStateException(
                                "injected checkpoint failure");
                    }
                    checkpointEvidence = new StreamCompletedCheckpointEvidence(
                            checkpointId, 1L, 30L, 20L);
                }
                @Override public void stopWithSavepoint(
                        StreamCommandId commandId, String savepointId,
                        StreamSavepointRequest request) {
                    savepoints.incrementAndGet();
                }
                @Override public StreamRunProgressSnapshot progressSnapshot() {
                    if (blockProgressAfterFailure && failure != null) {
                        progressAfterFailureCalls.incrementAndGet();
                        try {
                            releaseBlockedProgress.await(30L,
                                    TimeUnit.SECONDS);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                    }
                    return progress;
                }
                @Override public boolean finished() { return finished; }
                @Override public Throwable failure() { return failure; }
                @Override public StreamCompletedCheckpointEvidence
                        completedCheckpointEvidence() {
                    return checkpointEvidence;
                }
                @Override public void close() { }
            };
        }

        @Override
        public StreamProductRun restart(StreamProductJobSnapshot snapshot,
                Throwable observed) {
            restarts.incrementAndGet();
            failure = null;
            return create(snapshot);
        }
    }

    private static final class BlockingCheckpointRunFactory
            implements StreamProductRunFactory {
        private final CountDownLatch checkpointEntered =
                new CountDownLatch(1);
        private final CountDownLatch recoveryProbeEntered =
                new CountDownLatch(1);
        private final CountDownLatch releaseCheckpoint =
                new CountDownLatch(1);

        @Override
        public StreamProductRun create(StreamProductJobSnapshot snapshot) {
            boolean blocker = "blocking-cut".equals(
                    snapshot.artifact().definitionId());
            return new StreamProductRun() {
                private volatile StreamCompletedCheckpointEvidence
                        checkpointEvidence;
                @Override public void start() { }
                @Override public void cancel() { }
                @Override public void triggerCheckpoint(
                        StreamCommandId commandId, String checkpointId) {
                    if (!blocker) return;
                    synchronized (this) {
                        checkpointEntered.countDown();
                        try {
                            releaseCheckpoint.await(10L, TimeUnit.SECONDS);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                    }
                    checkpointEvidence = new StreamCompletedCheckpointEvidence(
                            checkpointId, 1L, 30L, 20L);
                }
                @Override public Throwable failure() {
                    if (blocker && checkpointEntered.getCount() == 0L
                            && releaseCheckpoint.getCount() != 0L
                            && Thread.currentThread().getName().equals(
                            "rowweave-stream-product-recovery")) {
                        recoveryProbeEntered.countDown();
                    }
                    synchronized (this) {
                        return null;
                    }
                }
                @Override public void close() { }
                @Override public StreamCompletedCheckpointEvidence
                        completedCheckpointEvidence() {
                    return checkpointEvidence;
                }
            };
        }

        @Override
        public StreamProductRun restart(StreamProductJobSnapshot snapshot,
                Throwable observed) {
            return create(snapshot);
        }
    }
}
