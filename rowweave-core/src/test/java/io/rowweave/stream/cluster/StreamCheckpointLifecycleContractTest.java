package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamSession;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamJobId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Product checkpoint configuration must drive runtime lifecycle, not only codecs. */
class StreamCheckpointLifecycleContractTest {

    @Test
    void exactlyOnceRejectsZeroCheckpointAuthorityAtAdmission() {
        StreamSession session = session("exactly-once-zero", null, true);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                10L, 1_000L, 1));
        session.setResourceProfile(withCheckpointBytes(0L));

        StreamApiException failure = assertThrows(
                StreamApiException.class, session::toPlanArtifact);
        assertEquals(StreamApiException.Code.INVALID_CONFIGURATION,
                failure.code());
    }

    @Test
    void zeroCheckpointAuthorityFailsBeforeCallingRun(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamSession session = session("manual-zero", client, false);
            session.setResourceProfile(withCheckpointBytes(0L));
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("manual-zero-submit"));

            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("manual-zero-checkpoint"));

            assertEquals(StreamOperationStatus.FAILED, checkpoint.status());
            assertEquals(0, runs.checkpoints.get());
        }
    }

    @Test
    void masterSchedulesPeriodicCutsWithoutOverlap(@TempDir Path root)
            throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.checkpointDelayMillis = 35L;
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamSession session = session("periodic-job", client, false);
            session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    20L, 1_000L, 2));
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("periodic-submit"));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
            while (runs.checkpoints.get() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }

            assertTrue(runs.checkpoints.get() >= 2,
                    "periodic configuration must trigger checkpoint cuts");

            long progressDeadline =
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
            while (job.progress().completedCheckpoints() < 2L
                    && System.nanoTime() < progressDeadline) {
                Thread.sleep(10L);
            }

            assertEquals(1, runs.maximumConcurrentCheckpoints.get(),
                    "aligned periodic cuts must have only one in flight");
            assertTrue(job.progress().completedCheckpoints() >= 2L);
        }
    }

    @Test
    void blockedProductionPeriodicCutDoesNotBlockFailureRecovery(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.ownsCheckpointDeadline = true;
        runs.blockCheckpoints = new CountDownLatch(1);
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamSession session = session(
                    "periodic-recovery-responsive", client, false);
            session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    20L, 5_000L, 1));
            session.setRestartPolicy(StreamRestartPolicy.fixedDelay(2, 0L));
            session.submit(StreamSubmissionId.of(
                    "periodic-recovery-responsive-submit"));

            long cutDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(2L);
            while (runs.concurrentCheckpoints.get() == 0
                    && System.nanoTime() < cutDeadline) {
                Thread.sleep(5L);
            }
            assertEquals(1, runs.concurrentCheckpoints.get(),
                    "the production-owned periodic cut must be in flight");

            runs.nextFailure.set(new IllegalStateException(
                    "injected Worker loss during checkpoint"));
            long recoveryDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(1L);
            while (runs.starts.get() < 2
                    && System.nanoTime() < recoveryDeadline) {
                Thread.sleep(5L);
            }
            assertTrue(runs.starts.get() >= 2,
                    "the recovery loop must cancel the stale cut and restart");
        } finally {
            runs.blockCheckpoints.countDown();
        }
    }

    @Test
    void healthyPeriodicCutUsesMonitorIndependentProbeAndKeepsControlsResponsive(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.ownsCheckpointDeadline = true;
        runs.blockCheckpoints = new CountDownLatch(1);
        runs.rejectOrdinaryProbeDuringCheckpoint = true;
        try (StreamJobClient client = new InProcessStreamJobClient(root, runs)) {
            StreamSession definition = session("healthy-blocked-cut", client, false);
            definition.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    20L, 5_000L, 1));
            StreamJobHandle blocked = definition.submit(
                    StreamSubmissionId.of("healthy-blocked-cut-submit"));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
            while (runs.checkpointFailureProbes.get() < 2
                    && System.nanoTime() < deadline) {
                Thread.sleep(5L);
            }
            assertTrue(runs.checkpointFailureProbes.get() >= 2,
                    "an active cut must continue monitor-independent failure probes");
            long started = System.nanoTime();
            assertEquals(StreamJobStatus.RUNNING, blocked.status());
            session("other-job-during-cut", client, false).submit(
                    StreamSubmissionId.of("other-job-during-cut-submit"));
            assertTrue(TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - started) < 500L,
                    "healthy cuts must not block unrelated controls");
            assertEquals(0, runs.unsafeFailureProbes.get());
        } finally {
            runs.blockCheckpoints.countDown();
        }
    }

    @Test
    void activeCheckpointDoesNotBlockDurableStatusReads(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.blockCheckpoints = new CountDownLatch(1);
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamJobHandle job = session(
                    "responsive-status", client, false).submit(
                    StreamSubmissionId.of("responsive-status-submit"));
            AtomicReference<StreamCheckpointHandle> result =
                    new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread checkpoint = new Thread(() -> {
                try {
                    result.set(job.triggerCheckpoint(StreamCommandId.of(
                            "responsive-status-checkpoint")));
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            }, "checkpoint-status-responsiveness");
            checkpoint.start();
            long enteredDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(1L);
            while (runs.concurrentCheckpoints.get() == 0
                    && System.nanoTime() < enteredDeadline) {
                Thread.sleep(5L);
            }
            assertEquals(1, runs.concurrentCheckpoints.get());

            long started = System.nanoTime();
            assertEquals(io.rowweave.stream.api.StreamJobStatus.RUNNING,
                    job.status());
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - started);
            assertTrue(elapsedMillis < 500L,
                    "durable status reads must not wait for a Worker cut");

            runs.blockCheckpoints.countDown();
            checkpoint.join(5_000L);
            assertTrue(!checkpoint.isAlive());
            assertEquals(null, failure.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    result.get().status());
        } finally {
            runs.blockCheckpoints.countDown();
        }
    }

    @Test
    void concurrentCheckpointCommandsSerializeWithoutLosingIdentity(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.blockCheckpoints = new CountDownLatch(1);
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamJobHandle job = session(
                    "checkpoint-command-serialization", client, false)
                    .submit(StreamSubmissionId.of(
                            "checkpoint-command-serialization-submit"));
            AtomicReference<StreamCheckpointHandle> first =
                    new AtomicReference<>();
            AtomicReference<StreamCheckpointHandle> second =
                    new AtomicReference<>();
            AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            AtomicReference<Throwable> secondFailure = new AtomicReference<>();
            Thread firstThread = new Thread(() -> {
                try {
                    first.set(job.triggerCheckpoint(StreamCommandId.of(
                            "serialized-checkpoint-first")));
                } catch (Throwable failure) {
                    firstFailure.set(failure);
                }
            }, "serialized-checkpoint-first");
            Thread secondThread = new Thread(() -> {
                try {
                    second.set(job.triggerCheckpoint(StreamCommandId.of(
                            "serialized-checkpoint-second")));
                } catch (Throwable failure) {
                    secondFailure.set(failure);
                }
            }, "serialized-checkpoint-second");

            firstThread.start();
            long enteredDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(1L);
            while (runs.concurrentCheckpoints.get() == 0
                    && System.nanoTime() < enteredDeadline) {
                Thread.sleep(5L);
            }
            assertEquals(1, runs.concurrentCheckpoints.get());
            secondThread.start();
            Thread.sleep(50L);
            assertEquals(1, runs.maximumConcurrentCheckpoints.get());

            runs.blockCheckpoints.countDown();
            firstThread.join(2_000L);
            secondThread.join(2_000L);

            assertTrue(!firstThread.isAlive() && !secondThread.isAlive());
            assertEquals(null, firstFailure.get());
            assertEquals(null, secondFailure.get());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    first.get().status());
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    second.get().status());
            assertEquals(2, runs.checkpoints.get());
            assertEquals(1, runs.maximumConcurrentCheckpoints.get());
        } finally {
            runs.blockCheckpoints.countDown();
        }
    }

    @Test
    void replacementMasterResumesPeriodicJobWithoutClientAttach(
            @TempDir Path root) throws Exception {
        RecordingRunFactory firstRuns = new RecordingRunFactory();
        try (StreamJobClient first =
                     new InProcessStreamJobClient(root, firstRuns)) {
            StreamSession session = session("periodic-recovery", first, false);
            session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    80L, 1_000L, 2));
            session.submit(StreamSubmissionId.of("periodic-recovery-submit"));
        }

        RecordingRunFactory replacementRuns = new RecordingRunFactory();
        try (StreamProductJobMasterServer ignored =
                     StreamProductJobMasterServer.inProcess(
                             root, replacementRuns)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
            while (replacementRuns.checkpoints.get() == 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(1, replacementRuns.starts.get(),
                    "replacement Master must rehydrate durable RUNNING jobs");
            assertTrue(replacementRuns.checkpoints.get() >= 1,
                    "periodic scheduling must not depend on a client attach");
        }
    }

    @Test
    void replacementRecoveryDoesNotBlockDurableStatusReads(
            @TempDir Path root) throws Exception {
        StreamJobId jobId;
        RecordingRunFactory firstRuns = new RecordingRunFactory();
        try (StreamProductJobMasterServer first =
                     new StreamProductJobMasterServer(root, firstRuns);
             StreamJobClient client =
                     RowWeave.createRealClusterStreamJobClient(
                             "127.0.0.1", first.port())) {
            jobId = session("responsive-recovery", client, false)
                    .submit(StreamSubmissionId.of(
                            "responsive-recovery-submit")).id();
        }

        RecordingRunFactory replacementRuns = new RecordingRunFactory();
        CountDownLatch release = new CountDownLatch(1);
        replacementRuns.startEntered = new CountDownLatch(1);
        replacementRuns.blockStarts = release;
        try (StreamProductJobMasterServer replacement =
                     new StreamProductJobMasterServer(root, replacementRuns);
             StreamJobClient client =
                     RowWeave.createRealClusterStreamJobClient(
                             "127.0.0.1", replacement.port())) {
            try {
                assertTrue(replacementRuns.startEntered.await(
                        2L, TimeUnit.SECONDS));
                long started = System.nanoTime();
                assertEquals(
                        io.rowweave.stream.api.StreamJobStatus.RUNNING,
                        client.attach(jobId).status());
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - started);
                assertTrue(elapsedMillis < 500L,
                        "status must not join replacement task deployment");
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void replacementMasterConvergesDurableRunningCheckpointWithoutAttach(
            @TempDir Path root) throws Exception {
        StreamCommandId command = StreamCommandId.of(
                "checkpoint-running-before-master-crash");
        StreamJobId jobId;
        RecordingRunFactory firstRuns = new RecordingRunFactory();
        try (StreamJobClient first =
                     new InProcessStreamJobClient(root, firstRuns)) {
            StreamSession session = session(
                    "checkpoint-operation-recovery", first, false);
            session.setCheckpointConfig(StreamCheckpointConfig.manualOnly(
                    1_000L, 2));
            jobId = session.submit(StreamSubmissionId.of(
                    "checkpoint-operation-recovery-submit")).id();
        }
        FileStreamProductJobAuthority durable =
                new FileStreamProductJobAuthority(root);
        durable.acceptCheckpoint(jobId, command);
        durable.startCheckpoint(jobId, command);

        RecordingRunFactory replacementRuns = new RecordingRunFactory();
        try (StreamProductJobMasterServer ignored =
                     StreamProductJobMasterServer.inProcess(
                             root, replacementRuns)) {
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(3L);
            while (durable.byJob(jobId).checkpoint(command).status()
                    == StreamOperationStatus.RUNNING
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    durable.byJob(jobId).checkpoint(command).status(),
                    "replacement Master must converge the durable operation");
            assertEquals(1, replacementRuns.checkpoints.get());
        }
    }

    @Test
    void recoveredProductionCheckpointDoesNotBlockMasterControlPlane(
            @TempDir Path root) throws Exception {
        StreamCommandId command = StreamCommandId.of(
                "blocked-checkpoint-before-master-crash");
        StreamJobId jobId;
        RecordingRunFactory firstRuns = new RecordingRunFactory();
        try (StreamJobClient first =
                     new InProcessStreamJobClient(root, firstRuns)) {
            StreamSession session = session(
                    "blocked-checkpoint-recovery", first, false);
            session.setCheckpointConfig(StreamCheckpointConfig.manualOnly(
                    5_000L, 1));
            jobId = session.submit(StreamSubmissionId.of(
                    "blocked-checkpoint-recovery-submit")).id();
        }
        FileStreamProductJobAuthority durable =
                new FileStreamProductJobAuthority(root);
        durable.acceptCheckpoint(jobId, command);
        durable.startCheckpoint(jobId, command);

        RecordingRunFactory replacementRuns = new RecordingRunFactory();
        replacementRuns.ownsCheckpointDeadline = true;
        replacementRuns.blockCheckpoints = new CountDownLatch(1);
        try (StreamProductJobMasterServer replacement =
                     StreamProductJobMasterServer.inProcess(
                             root, replacementRuns);
             StreamJobClient client = new RealClusterStreamJobClient(
                     new InProcessStreamProductControlTransport(
                             replacement))) {
            try {
                long cutDeadline = System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(2L);
                while (replacementRuns.concurrentCheckpoints.get() == 0
                        && System.nanoTime() < cutDeadline) {
                    Thread.sleep(5L);
                }
                assertEquals(1,
                        replacementRuns.concurrentCheckpoints.get());

                long started = System.nanoTime();
                session("independent-control-job", client, false).submit(
                        StreamSubmissionId.of("independent-control-submit"));
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - started);
                assertTrue(elapsedMillis < 500L,
                        "durable checkpoint convergence must not own the Master lifecycle lock");
            } finally {
                replacementRuns.blockCheckpoints.countDown();
            }
            long completionDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(2L);
            while (durable.byJob(jobId).checkpoint(command).status()
                    != StreamOperationStatus.SUCCEEDED
                    && System.nanoTime() < completionDeadline) {
                Thread.sleep(5L);
            }
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    durable.byJob(jobId).checkpoint(command).status());
        }
    }

    @Test
    void replacementMasterTerminatesUnreplayableRunningSavepointWithoutAttach(
            @TempDir Path root) throws Exception {
        StreamCommandId command = StreamCommandId.of(
                "savepoint-running-before-master-crash");
        StreamJobId jobId;
        RecordingRunFactory firstRuns = new RecordingRunFactory();
        try (StreamJobClient first =
                     new InProcessStreamJobClient(root, firstRuns)) {
            jobId = session("savepoint-operation-recovery", first, false)
                    .submit(StreamSubmissionId.of(
                            "savepoint-operation-recovery-submit")).id();
        }
        StreamSavepointRequest request = new StreamSavepointRequest(
                "durable-savepoint-provider", new byte[] {1});
        FileStreamProductJobAuthority durable =
                new FileStreamProductJobAuthority(root);
        durable.acceptSavepoint(jobId, command,
                StreamSavepointRequestCodec.digest(request));
        durable.startSavepoint(jobId, command);

        try (StreamProductJobMasterServer ignored =
                     StreamProductJobMasterServer.inProcess(
                             root, new RecordingRunFactory())) {
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(3L);
            while (durable.byJob(jobId).savepoint(command).status()
                    == StreamOperationStatus.RUNNING
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(StreamOperationStatus.FAILED,
                    durable.byJob(jobId).savepoint(command).status(),
                    "replacement must terminally fail a request body it cannot replay");
        }
    }

    @Test
    void replacementMasterCompletesAlreadyDurableRunningSavepointWithoutAttach(
            @TempDir Path root) throws Exception {
        StreamCommandId command = StreamCommandId.of(
                "durable-savepoint-before-master-crash");
        StreamJobId jobId;
        RecordingRunFactory firstRuns = new RecordingRunFactory();
        try (StreamJobClient first =
                     new InProcessStreamJobClient(root, firstRuns)) {
            jobId = session("durable-savepoint-recovery", first, false)
                    .submit(StreamSubmissionId.of(
                            "durable-savepoint-recovery-submit")).id();
        }
        StreamSavepointRequest request = new StreamSavepointRequest(
                "durable-savepoint-provider", new byte[] {1});
        FileStreamProductJobAuthority durable =
                new FileStreamProductJobAuthority(root);
        durable.acceptSavepoint(jobId, command,
                StreamSavepointRequestCodec.digest(request));
        durable.startSavepoint(jobId, command);

        RecordingRunFactory replacementRuns = new RecordingRunFactory();
        replacementRuns.durableSavepoint = true;
        try (StreamProductJobMasterServer ignored =
                     StreamProductJobMasterServer.inProcess(
                             root, replacementRuns)) {
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(3L);
            while (durable.byJob(jobId).savepoint(command).status()
                    == StreamOperationStatus.RUNNING
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    durable.byJob(jobId).savepoint(command).status(),
                    "a durable savepoint cut must win over missing request body");
            assertEquals(1, replacementRuns.recoveredSavepoints.get());
        }
    }

    @Test
    void durableSavepointDecisionOutweighsFailedControlPlaneCall(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.durableSavepointDecision = true;
        runs.nextSavepointFailure.set(
                new IllegalStateException("injected savepoint cut failure"));
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamJobHandle job = session(
                    "savepoint-decision-after-cut-failure", client, false)
                    .submit(StreamSubmissionId.of(
                            "savepoint-decision-after-cut-failure-submit"));

            StreamSavepointHandle savepoint = job.stopWithSavepoint(
                    StreamCommandId.of(
                            "savepoint-decision-after-cut-failure-command"),
                    new StreamSavepointRequest(
                            "file-savepoint-provider", new byte[] {1}));

            assertEquals(StreamOperationStatus.SUCCEEDED, savepoint.status(),
                    "a durably decided savepoint cut must not be published as FAILED");
            assertEquals(StreamJobStatus.STOPPED_WITH_SAVEPOINT, job.status());
            assertEquals(1, runs.retired.get());
        }
    }

    @Test
    void recoveryLoopSurvivesSpuriousInterruptAndConvergesDurableSavepoint(
            @TempDir Path root) throws Exception {
        StreamCommandId command = StreamCommandId.of(
                "savepoint-interrupted-recovery-command");
        StreamJobId jobId;
        RecordingRunFactory firstRuns = new RecordingRunFactory();
        try (StreamJobClient first =
                     new InProcessStreamJobClient(root, firstRuns)) {
            jobId = session("interrupted-recovery", first, false)
                    .submit(StreamSubmissionId.of(
                            "interrupted-recovery-submit")).id();
        }
        StreamSavepointRequest request = new StreamSavepointRequest(
                "durable-savepoint-provider", new byte[] {1});
        FileStreamProductJobAuthority durable =
                new FileStreamProductJobAuthority(root);
        durable.acceptSavepoint(jobId, command,
                StreamSavepointRequestCodec.digest(request));
        durable.startSavepoint(jobId, command);

        RecordingRunFactory replacementRuns = new RecordingRunFactory();
        replacementRuns.durableSavepoint = true;
        replacementRuns.interruptRecoveryOnce.set(true);
        try (StreamProductJobMasterServer ignored =
                     StreamProductJobMasterServer.inProcess(
                             root, replacementRuns)) {
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (durable.byJob(jobId).savepoint(command).status()
                    == StreamOperationStatus.RUNNING
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    durable.byJob(jobId).savepoint(command).status(),
                    "an interrupt unrelated to shutdown must not retire recovery");
            assertEquals(1, replacementRuns.recoveredSavepoints.get());
        }
    }

    @Test
    void cancelConvergesCommitDecidedCheckpointBeforeTerminalTransition(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.pendingCheckpointFailures.set(1);
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamJobHandle job = session(
                    "cancel-after-decision", client, false).submit(
                    StreamSubmissionId.of("cancel-after-decision-submit"));
            StreamCommandId checkpointId = StreamCommandId.of(
                    "cancel-after-decision-checkpoint");
            StreamCheckpointHandle checkpoint =
                    job.triggerCheckpoint(checkpointId);
            assertEquals(StreamOperationStatus.RUNNING, checkpoint.status());

            job.cancel(StreamCommandId.of("cancel-after-decision-cancel"))
                    .awaitCompletion(5_000L);

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    client.attachCheckpoint(job.id(), checkpointId).status(),
                    "terminal transition must first converge the decision");
            assertEquals(2, runs.checkpoints.get());
            assertEquals(1, runs.retired.get());
        }
    }

    @Test
    void cancelWaitsForProductionOwnedPeriodicCheckpointDecision(
            @TempDir Path root) throws Exception {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.ownsCheckpointDeadline = true;
        runs.blockCheckpoints = new CountDownLatch(1);
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamSession definition = session(
                    "cancel-during-owned-periodic-cut", client, false);
            definition.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    20L, 5_000L, 1));
            StreamJobHandle job = definition.submit(StreamSubmissionId.of(
                    "cancel-during-owned-periodic-cut-submit"));
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(2L);
            while (runs.concurrentCheckpoints.get() == 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(5L);
            }
            assertEquals(1, runs.concurrentCheckpoints.get(),
                    "the periodic decision must be in flight before cancel");

            Thread release = new Thread(() -> {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                runs.blockCheckpoints.countDown();
            }, "release-owned-checkpoint-for-cancel");
            release.start();
            try {
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        job.cancel(StreamCommandId.of(
                                "cancel-during-owned-periodic-cut-command"))
                                .awaitCompletion(5_000L));
                assertEquals(StreamJobStatus.CANCELED,
                        job.awaitTermination(5_000L));
            } finally {
                runs.blockCheckpoints.countDown();
                release.join(2_000L);
            }
        }
    }

    @Test
    void timeoutBoundsTheWholeCheckpointCall(@TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        runs.blockCheckpoints = new CountDownLatch(1);
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamSession session = session("timeout-job", client, false);
            session.setCheckpointConfig(StreamCheckpointConfig.manualOnly(
                    100L, 1));
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("timeout-submit"));
            long started = System.nanoTime();

            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("timeout-checkpoint"));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - started);

            assertEquals(StreamOperationStatus.FAILED, checkpoint.status());
            assertTrue(elapsedMillis < 1_000L,
                    "configured timeout must include the run/Worker cut");
        } finally {
            runs.blockCheckpoints.countDown();
        }
    }

    @Test
    void retainedHistoryDoesNotBecomeA128CheckpointLifetimeLimit(
            @TempDir Path root) {
        RecordingRunFactory runs = new RecordingRunFactory();
        try (StreamJobClient client =
                     new InProcessStreamJobClient(root, runs)) {
            StreamSession session = session("retained-job", client, false);
            session.setCheckpointConfig(StreamCheckpointConfig.manualOnly(
                    1_000L, 2));
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("retained-submit"));

            for (int index = 0; index < 130; index++) {
                StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                        StreamCommandId.of("retained-checkpoint-" + index));
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        checkpoint.status());
            }

            assertEquals(130L, job.progress().completedCheckpoints());
            assertEquals(130, runs.checkpoints.get());
        }
    }

    private static StreamSession session(String name, StreamJobClient client,
            boolean transactional) {
        StreamSession session = client == null
                ? RowWeave.createStreamSession(name, name)
                : RowWeave.createStreamSession(name, name, client);
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false)));
        StreamSourceSpec source = StreamSourceSpec.exactOrdered(
                schema, "source-v1", sha256("source-contract"),
                new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(sha256("split"),
                                "sequence-v1", new byte[] {2})));
        StreamSinkSpec sink = transactional
                ? StreamSinkSpec.checkpointTransactional(
                "sink-v1", sha256("sink-contract"), new byte[] {3})
                : StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
        session.fromSource(source).uid("source").sinkTo("sink", sink);
        return session;
    }

    private static StreamResourceProfile withCheckpointBytes(long bytes) {
        StreamResourceProfile defaults = StreamResourceProfile.defaults();
        return new StreamResourceProfile(defaults.mailboxEvents(),
                defaults.maximumFrameBytes(), defaults.channelCreditBytes(),
                defaults.managedStateBytes(), defaults.timerCount(),
                defaults.windowCount(), bytes, defaults.threadCount(),
                defaults.fileDescriptorCount(), defaults.spillDiskBytes());
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

    private static final class RecordingRunFactory
            implements StreamProductRunFactory {
        private final AtomicInteger checkpoints = new AtomicInteger();
        private final AtomicInteger starts = new AtomicInteger();
        private final AtomicInteger concurrentCheckpoints = new AtomicInteger();
        private final AtomicInteger maximumConcurrentCheckpoints =
                new AtomicInteger();
        private final AtomicInteger recoveredSavepoints = new AtomicInteger();
        private final AtomicInteger pendingCheckpointFailures =
                new AtomicInteger();
        private final AtomicInteger retired = new AtomicInteger();
        private volatile long checkpointDelayMillis;
        private volatile CountDownLatch blockCheckpoints;
        private volatile CountDownLatch startEntered =
                new CountDownLatch(0);
        private volatile CountDownLatch blockStarts;
        private volatile boolean durableSavepoint;
        private volatile boolean durableSavepointDecision;
        private volatile boolean ownsCheckpointDeadline;
        private volatile boolean rejectOrdinaryProbeDuringCheckpoint;
        private final AtomicInteger checkpointFailureProbes = new AtomicInteger();
        private final AtomicInteger unsafeFailureProbes = new AtomicInteger();
        private final AtomicReference<Throwable> nextFailure =
                new AtomicReference<>();
        private final AtomicReference<Throwable> nextSavepointFailure =
                new AtomicReference<>();
        private final AtomicBoolean interruptRecoveryOnce =
                new AtomicBoolean();

        @Override
        public StreamProductRun create(StreamProductJobSnapshot snapshot) {
            return new StreamProductRun() {
                private final java.util.concurrent.atomic.AtomicBoolean cutActive =
                        new java.util.concurrent.atomic.AtomicBoolean();
                @Override public void start() {
                    starts.incrementAndGet();
                    startEntered.countDown();
                    CountDownLatch blocker = blockStarts;
                    if (blocker == null) return;
                    try {
                        blocker.await(5L, TimeUnit.SECONDS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "start interrupted", interrupted);
                    }
                }
                @Override public void cancel() { }
                @Override public void triggerCheckpoint(
                        StreamCommandId commandId, String checkpointId) {
                    cutActive.set(true);
                    int active = concurrentCheckpoints.incrementAndGet();
                    maximumConcurrentCheckpoints.accumulateAndGet(
                            active, Math::max);
                    try {
                        CountDownLatch blocker = blockCheckpoints;
                        if (blocker != null) blocker.await(2L, TimeUnit.SECONDS);
                        if (checkpointDelayMillis > 0L) {
                            Thread.sleep(checkpointDelayMillis);
                        }
                        checkpoints.incrementAndGet();
                        if (pendingCheckpointFailures.getAndUpdate(
                                value -> Math.max(0, value - 1)) > 0) {
                            throw new CheckpointCommitPendingException(
                                    "durable decision awaits reconciliation",
                                    new IllegalStateException("injected ACK loss"));
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "checkpoint interrupted", interrupted);
                    } finally {
                        concurrentCheckpoints.decrementAndGet();
                        cutActive.set(false);
                    }
                }
                @Override public boolean ownsCheckpointDeadline() {
                    return ownsCheckpointDeadline;
                }
                @Override public Throwable failure() {
                    if (interruptRecoveryOnce.get()
                            && Thread.currentThread().getName().equals(
                                    "rowweave-stream-product-recovery")
                            && interruptRecoveryOnce.compareAndSet(true, false)) {
                        Thread.currentThread().interrupt();
                    }
                    if (rejectOrdinaryProbeDuringCheckpoint
                            && cutActive.get()) {
                        unsafeFailureProbes.incrementAndGet();
                        throw new IllegalStateException("ordinary probe owns the cut monitor");
                    }
                    return nextFailure.getAndSet(null);
                }
                @Override public Throwable failureDuringCheckpoint() {
                    checkpointFailureProbes.incrementAndGet();
                    return nextFailure.getAndSet(null);
                }
                @Override public void stopWithSavepoint(
                        StreamCommandId commandId, String savepointId,
                        StreamSavepointRequest request) {
                    Throwable injected = nextSavepointFailure.getAndSet(null);
                    if (injected != null) {
                        throw new IllegalStateException(
                                "injected savepoint cut failure", injected);
                    }
                }
                @Override public boolean recoverSavepoint(
                        StreamCommandId commandId, String savepointId) {
                    if (!durableSavepoint) return false;
                    recoveredSavepoints.incrementAndGet();
                    return true;
                }
                @Override public void close() { }
            };
        }

        @Override
        public StreamProductRun restart(
                StreamProductJobSnapshot snapshot, Throwable failure) {
            return create(snapshot);
        }

        @Override
        public StreamProductDecisionConvergence convergeDurableDecision(
                StreamProductJobSnapshot snapshot) {
            if (!durableSavepointDecision) return null;
            StreamProductSavepointOperation operation =
                    snapshot.savepointOrNull();
            return operation == null ? null
                    : new StreamProductDecisionConvergence(
                            operation.savepointId(), StreamCdcPhase.LOG);
        }

        @Override public void retire(StreamProductJobSnapshot snapshot) {
            retired.incrementAndGet();
        }
    }
}
