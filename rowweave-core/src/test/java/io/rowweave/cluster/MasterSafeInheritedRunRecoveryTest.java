package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.CollectionSource;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.ObjectStorageReadHandle;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.DataRowClassification;
import io.rowweave.TypedQualityManifest;
import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.ArtifactRepositoryProvider;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.ops.RowValidation;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunCancellationAuthority;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RetryAttemptIdentity;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.WholeAttemptRetryAuthority;
import io.rowweave.run.WholeAttemptRetryDecision;
import io.rowweave.monitor.InMemoryJobMonitor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterSafeInheritedRunRecoveryTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));
    private static final DataSchema QUALITY_META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));

    @Test
    void readyWithoutAttemptExecutesOnRealWorkerAndOpensAdmission(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        Path domain = tmp.resolve("ready-output");
        createReady(registry, "ready-recovery", "ready-definition", 1, 11L,
                new ManifestFileSink(META, domain.toString(), "ledger"));
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = worker(master, tmp, "ready-worker");
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));
            RunSnapshot recovered = registry.runStateStore().get("ready-recovery");
            assertEquals(RunStatus.SUCCEEDED, recovered.status());
            assertEquals(AttemptStatus.SUCCEEDED, recovered.currentAttempt().status());
            assertTrue(recovered.currentAttempt().attemptId()
                    .startsWith("recovery-attempt-v1-"));
            assertTrue(recovered.currentAttempt().publicationId()
                    .startsWith("recovery-publication-v1-"));
            assertEquals(1, recovered.attemptCount());
            assertManifestRows(domain);
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void createdAttemptPreservesIdentityAndExecutesOnRealWorker(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        Path domain = tmp.resolve("created-output");
        RunSnapshot ready = createReady(
                registry, "created-recovery", "created-definition", 1, 13L,
                new ManifestFileSink(META, domain.toString(), "ledger"));
        accepted(registry.runStateStore(), RunCommand.createAttempt(
                "original-create-attempt", ready.runId(), ready.revision(),
                "original-attempt", "original-publication", 17L));
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = worker(master, tmp, "created-worker");
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));
            RunSnapshot recovered = registry.runStateStore().get("created-recovery");
            assertEquals(RunStatus.SUCCEEDED, recovered.status());
            assertEquals("original-attempt", recovered.currentAttempt().attemptId());
            assertEquals("original-publication",
                    recovered.currentAttempt().publicationId());
            assertEquals(1L, recovered.currentAttempt().fenceToken());
            assertEquals(1, recovered.attemptCount());
            assertManifestRows(domain);
        }
    }

    @Test
    void retryReadyRunClaimsCommonIdentityAndExecutesOnRealWorker(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore store = registry.runStateStore();
        RunSnapshot ready = createReady(
                registry, "retry-ready-recovery", "retry-ready-definition",
                2, 13L);
        RunSnapshot first = accepted(store, RunCommand.createAttempt(
                "first-retry-ready", ready.runId(), ready.revision(),
                "attempt-1", "publication-1", 14L));
        WholeAttemptRetryAuthority retry = (WholeAttemptRetryAuthority) store;
        WholeAttemptRetryDecision scheduled = retry.scheduleRetry(
                new RunAttemptFence(first.runId(),
                        first.currentAttempt().attemptId(),
                        first.currentAttempt().fenceToken()));
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED, scheduled.code());
        RetryReadyEvaluation due;
        long dueTimeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        do {
            due = retry.evaluateRetryReady(ready.runId());
            if (due.code() == RetryReadyEvaluation.Code.DUE) break;
            Thread.sleep(10L);
        } while (System.nanoTime() < dueTimeout);
        assertEquals(RetryReadyEvaluation.Code.DUE, due.code());
        RetryAttemptIdentity expected = RetryAttemptIdentity.derive(
                ready.runId(), ready.artifactId(), 2);
        long residentBaseline = io.rowweave.cluster.rpc.ControlResidentBudget
                .processWide().snapshot().usedBytes();

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = worker(master, tmp, "retry-ready-worker");
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    generation, 10, TimeUnit.SECONDS));
            RunSnapshot recovered = store.get(ready.runId());
            assertEquals(RunStatus.SUCCEEDED, recovered.status());
            assertEquals(2, recovered.attemptCount());
            assertEquals(expected.attemptId(),
                    recovered.currentAttempt().attemptId());
            assertEquals(expected.publicationId(),
                    recovered.currentAttempt().publicationId());
            assertEquals(expected.fenceToken(),
                    recovered.currentAttempt().fenceToken());
            assertEquals(residentBaseline,
                    io.rowweave.cluster.rpc.ControlResidentBudget.processWide()
                            .snapshot().usedBytes());
        }
    }

    @Test
    void replacementLeaderExecutesFreshTypedQualityRetryAsOnePublication(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore store = registry.runStateStore();
        String runId = "quality-retry-recovery";
        String definitionId = "quality-retry-definition";
        Path domain = tmp.resolve("quality-retry-domain");
        RunSnapshot ready = createReady(registry, runId, definitionId, 2, 13L,
                qualityArtifact(definitionId, domain));
        RunSnapshot first = accepted(store, RunCommand.createAttempt(
                "first-quality-retry", runId, ready.revision(),
                "attempt-1", "publication-1", 14L));
        WholeAttemptRetryAuthority retry = (WholeAttemptRetryAuthority) store;
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                retry.scheduleRetry(new RunAttemptFence(
                        first.runId(), first.currentAttempt().attemptId(),
                        first.currentAttempt().fenceToken())).code());
        RetryReadyEvaluation due;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        do {
            due = retry.evaluateRetryReady(runId);
            if (due.code() == RetryReadyEvaluation.Code.DUE) break;
            Thread.sleep(10L);
        } while (System.nanoTime() < deadline);
        assertEquals(RetryReadyEvaluation.Code.DUE, due.code());
        RetryAttemptIdentity expected = RetryAttemptIdentity.derive(
                runId, ready.artifactId(), 2);

        File qualityWorkerDir = tmp.resolve("quality-retry-worker-spill").toFile();
        assertTrue(qualityWorkerDir.mkdirs());
        try (MasterServer replacement = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "quality-retry-worker", 64, "127.0.0.1",
                     replacement.port(), qualityWorkerDir);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", replacement.port()))) {
            replacement.bindLeadership(election);
            election.start();
            long inventory = replacement.recoveryInventoryGeneration();
            assertTrue(replacement.awaitRecoveryInventory(
                    inventory, 10, TimeUnit.SECONDS));

            RunSnapshot recovered = store.get(runId);
            assertEquals(RunStatus.SUCCEEDED, recovered.status(),
                    recovered.currentAttempt().reasonCode() + ": "
                            + recovered.currentAttempt().message());
            assertEquals(2, recovered.attemptCount());
            assertEquals(expected.attemptId(), recovered.currentAttempt().attemptId());
            assertEquals(expected.publicationId(),
                    recovered.currentAttempt().publicationId());
            CommitManifest manifest = new ManifestFileStore(domain).latest();
            assertEquals(3, manifest.members().size());
            assertEquals(expected.publicationId(), manifest.attemptId());
            assertEquals(7L, recovered.resultSummary().rowsOut());
            assertEquals(Arrays.asList(
                    "ACCEPTED_TOTAL|||-1||1",
                    "REJECTED_REASON|QUALITY_RULE_VIOLATION|adaptor.required|0|NOT_NULL|1",
                    "REJECTED_REASON|QUALITY_RULE_VIOLATION|period.range|1|INTEGER_RANGE_CLOSED|1",
                    "REJECTED_TOTAL|||-1||2"), qualitySummaryLines(domain));
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    replacement.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void replacementLeaderExecutesFreshObjectQualityRetryAsOnePublication(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore store = registry.runStateStore();
        String runId = "object-quality-retry-recovery";
        String definitionId = "object-quality-retry-definition";
        Path domain = tmp.resolve("object-quality-retry-domain");
        byte[] content = "7\nbad\n8\n".getBytes(StandardCharsets.UTF_8);
        RecoveryObjectProvider.reset(content);
        RecoveryObjectProvider.credentialGeneration.set(1);
        PlanArtifact artifact = objectQualityArtifact(
                definitionId, domain, content);
        RunSnapshot ready = createReady(
                registry, runId, definitionId, 2, 23L, artifact);
        RunSnapshot first = accepted(store, RunCommand.createAttempt(
                "first-object-quality-retry", runId, ready.revision(),
                "attempt-1", "publication-1", 24L));
        WholeAttemptRetryAuthority retry = (WholeAttemptRetryAuthority) store;
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                retry.scheduleRetry(new RunAttemptFence(
                        first.runId(), first.currentAttempt().attemptId(),
                        first.currentAttempt().fenceToken())).code());
        RetryReadyEvaluation due;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        do {
            due = retry.evaluateRetryReady(runId);
            if (due.code() == RetryReadyEvaluation.Code.DUE) break;
            Thread.sleep(10L);
        } while (System.nanoTime() < deadline);
        assertEquals(RetryReadyEvaluation.Code.DUE, due.code());
        RetryAttemptIdentity expected = RetryAttemptIdentity.derive(
                runId, ready.artifactId(), 2);

        RecoveryObjectProvider.credentialGeneration.set(2);
        File workerDir = tmp.resolve("object-quality-retry-worker-spill").toFile();
        assertTrue(workerDir.mkdirs());
        try (MasterServer replacement = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "object-quality-retry-worker", 32, "127.0.0.1",
                     replacement.port(), workerDir);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", replacement.port()))) {
            replacement.bindLeadership(election);
            election.start();
            long inventory = replacement.recoveryInventoryGeneration();
            assertTrue(replacement.awaitRecoveryInventory(
                    inventory, 10, TimeUnit.SECONDS));

            RunSnapshot recovered = store.get(runId);
            assertEquals(RunStatus.SUCCEEDED, recovered.status(),
                    recovered.currentAttempt().reasonCode() + ": "
                            + recovered.currentAttempt().message());
            assertEquals(2, recovered.attemptCount());
            assertEquals(expected.attemptId(),
                    recovered.currentAttempt().attemptId());
            assertEquals(expected.publicationId(),
                    recovered.currentAttempt().publicationId());
            assertEquals(6L, recovered.resultSummary().rowsOut());
            CommitManifest manifest = new ManifestFileStore(domain).latest();
            assertEquals(3, manifest.members().size());
            assertEquals(expected.publicationId(), manifest.attemptId());
            assertEquals(Arrays.asList(
                    "ACCEPTED_TOTAL|||-1||2",
                    "REJECTED_REASON|QUALITY_RAW_FORMAT_INVALID|OBJECT_ROW_DECODE_V1|-1|ROWWEAVE_ROW_TEXT_V1|1",
                    "REJECTED_TOTAL|||-1||1"), qualitySummaryLines(domain));
            assertEquals(0, worker.preparedOutputActiveCount());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    replacement.recoveryInventorySnapshot().state());
        }
        assertEquals(Collections.singletonList(2),
                RecoveryObjectProvider.openedCredentialGenerations);
        assertEquals(Collections.singletonList("credential-prod"),
                RecoveryObjectProvider.openedCredentialRefs);
        assertEquals(Collections.singletonList("generation-42"),
                RecoveryObjectProvider.openedVersions);
        assertEquals(1, RecoveryObjectProvider.opens.get());
        assertEquals(1, RecoveryObjectProvider.closes.get());
    }

    @Test
    void recoveredLeaderExecutesObjectSnapshotRetryWithCurrentCredentialGeneration(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore store = registry.runStateStore();
        byte[] content = "7\n8\n".getBytes(StandardCharsets.UTF_8);
        RecoveryObjectProvider.reset(content);
        RecoveryObjectProvider.credentialGeneration.set(1);
        ObjectStorageSnapshotSource input = new ObjectStorageSnapshotSource(
                META, new RecoveryObjectProvider(), "endpoint-prod", "credential-prod",
                "domain", "ledger/retry.tsv", "generation-42", content.length,
                sha256(content), ObjectStorageSnapshotSource.MIN_ROW_BYTES);
        String snapshotId = input.batchSourceSplit().snapshotId();
        PlanArtifact artifact = artifact(
                "object-retry-definition", input, new DiscardSink(META));
        RunSnapshot ready = createReady(
                registry, "object-retry-recovery", artifact.definitionId(),
                2, 41L, artifact);
        RunSnapshot first = accepted(store, RunCommand.createAttempt(
                "object-first-attempt", ready.runId(), ready.revision(),
                "attempt-1", "publication-1", 42L));
        WholeAttemptRetryAuthority retry = (WholeAttemptRetryAuthority) store;
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                retry.scheduleRetry(new RunAttemptFence(
                        ready.runId(), first.currentAttempt().attemptId(),
                        first.currentAttempt().fenceToken())).code());
        RetryReadyEvaluation due;
        long dueDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        do {
            due = retry.evaluateRetryReady(ready.runId());
            if (due.code() != RetryReadyEvaluation.Code.DUE) Thread.sleep(10L);
        } while (due.code() != RetryReadyEvaluation.Code.DUE
                && System.nanoTime() < dueDeadline);
        assertEquals(RetryReadyEvaluation.Code.DUE, due.code());

        RecoveryObjectProvider.credentialGeneration.set(2);
        try (MasterServer recoveredLeader = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = worker(
                     recoveredLeader, tmp, "object-retry-worker");
             MasterElection election = new MasterElection(
                     registry, new Endpoint(
                             "127.0.0.1", recoveredLeader.port()))) {
            recoveredLeader.bindLeadership(election);
            election.start();
            awaitState(recoveredLeader, LeaderRecoveryInventory.State.EMPTY_READY);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            RunSnapshot completed = store.get(ready.runId());
            while (completed.status() != RunStatus.SUCCEEDED
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
                completed = store.get(ready.runId());
            }
            assertEquals(RunStatus.SUCCEEDED, completed.status());
            assertEquals(2, completed.attemptCount());
        }
        assertEquals(snapshotId, input.batchSourceSplit().snapshotId());
        assertEquals(Collections.singletonList(2),
                RecoveryObjectProvider.openedCredentialGenerations);
        assertEquals(Collections.singletonList("credential-prod"),
                RecoveryObjectProvider.openedCredentialRefs);
        assertEquals(Collections.singletonList("generation-42"),
                RecoveryObjectProvider.openedVersions);
        assertEquals(1, RecoveryObjectProvider.opens.get());
        assertEquals(1, RecoveryObjectProvider.closes.get());
    }

    @Test
    void recoveredLeaderExecutesJdbcSnapshotRetryWithCurrentCredentialGeneration(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore store = registry.runStateStore();
        DurableJdbcTestProvider.reset(7, 8);
        DurableJdbcTestProvider.credentialGeneration.set(1);
        io.rowweave.JdbcSnapshotSource input = DurableJdbcTestProvider.input();
        String snapshotId = input.batchSourceSplit().snapshotId();
        PlanArtifact artifact = artifact(
                "jdbc-retry-definition", input, new DiscardSink(META));
        RunSnapshot ready = createReady(
                registry, "jdbc-retry-recovery", artifact.definitionId(),
                2, 51L, artifact);
        RunSnapshot first = accepted(store, RunCommand.createAttempt(
                "jdbc-first-attempt", ready.runId(), ready.revision(),
                "attempt-1", "publication-1", 52L));
        WholeAttemptRetryAuthority retry = (WholeAttemptRetryAuthority) store;
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                retry.scheduleRetry(new RunAttemptFence(
                        ready.runId(), first.currentAttempt().attemptId(),
                        first.currentAttempt().fenceToken())).code());
        RetryReadyEvaluation due;
        long dueDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        do {
            due = retry.evaluateRetryReady(ready.runId());
            if (due.code() != RetryReadyEvaluation.Code.DUE) Thread.sleep(10L);
        } while (due.code() != RetryReadyEvaluation.Code.DUE
                && System.nanoTime() < dueDeadline);
        assertEquals(RetryReadyEvaluation.Code.DUE, due.code());

        DurableJdbcTestProvider.credentialGeneration.set(2);
        try (MasterServer recoveredLeader = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = worker(
                     recoveredLeader, tmp, "jdbc-retry-worker");
             MasterElection election = new MasterElection(
                     registry, new Endpoint(
                             "127.0.0.1", recoveredLeader.port()))) {
            recoveredLeader.bindLeadership(election);
            election.start();
            awaitState(recoveredLeader, LeaderRecoveryInventory.State.EMPTY_READY);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            RunSnapshot completed = store.get(ready.runId());
            while (completed.status() != RunStatus.SUCCEEDED
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
                completed = store.get(ready.runId());
            }
            assertEquals(RunStatus.SUCCEEDED, completed.status());
            assertEquals(2, completed.attemptCount());
        }
        assertEquals(snapshotId, input.batchSourceSplit().snapshotId());
        assertEquals(Collections.singletonList(2),
                DurableJdbcTestProvider.openedCredentialGenerations);
        assertEquals(Collections.singletonList("credential-prod"),
                DurableJdbcTestProvider.openedCredentialRefs);
        assertEquals(1, DurableJdbcTestProvider.opens.get());
        assertEquals(1, DurableJdbcTestProvider.closes.get());
    }

    @Test
    void notDueRetryDoesNotBlockInitialGateAndRunsAfterAuthorityWake(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore store = registry.runStateStore();
        RunSnapshot ready = createReady(
                registry, "retry-waiting-recovery", "retry-waiting-definition",
                3, 21L);
        RunSnapshot first = accepted(store, RunCommand.createAttempt(
                "first-retry-waiting", ready.runId(), ready.revision(),
                "attempt-1", "publication-1", 22L));
        WholeAttemptRetryAuthority retry = (WholeAttemptRetryAuthority) store;
        WholeAttemptRetryDecision scheduled = retry.scheduleRetry(
                        new RunAttemptFence(
                                first.runId(), first.currentAttempt().attemptId(),
                                first.currentAttempt().fenceToken()));
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED, scheduled.code());
        RetryReadyEvaluation firstDue;
        long firstDueDeadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(5);
        do {
            firstDue = retry.evaluateRetryReady(ready.runId());
            if (firstDue.code() != RetryReadyEvaluation.Code.DUE) {
                Thread.sleep(10L);
            }
        } while (firstDue.code() != RetryReadyEvaluation.Code.DUE
                && System.nanoTime() < firstDueDeadline);
        assertEquals(RetryReadyEvaluation.Code.DUE, firstDue.code());
        io.rowweave.run.RetryAttemptClaim second =
                retry.claimDueRetry(ready.runId());
        assertEquals(io.rowweave.run.RetryAttemptClaim.Code.CLAIMED,
                second.code());
        WholeAttemptRetryDecision secondScheduled = retry.scheduleRetry(
                new RunAttemptFence(
                        second.snapshot().runId(),
                        second.snapshot().currentAttempt().attemptId(),
                        second.snapshot().currentAttempt().fenceToken()));
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                secondScheduled.code());
        assertEquals(RetryReadyEvaluation.Code.NOT_DUE,
                retry.evaluateRetryReady(ready.runId()).code());
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(10);

        try (MasterServer master = new MasterServer(
                5_000L, 0, registry, monitor);
             WorkerServer worker = worker(master, tmp, "retry-waiting-worker");
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long gateDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(2);
            while ((master.recoveryInventorySnapshot().state()
                    == LeaderRecoveryInventory.State.IDLE
                    || master.recoveryInventorySnapshot().state()
                    == LeaderRecoveryInventory.State.SCANNING
                    || master.recoveryInventorySnapshot().state()
                    == LeaderRecoveryInventory.State.BLOCKED_UNAVAILABLE)
                    && System.nanoTime() < gateDeadline) {
                Thread.sleep(10L);
            }
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state(),
                    master.recoveryInventorySnapshot().failure());
            RunSnapshot waiting = store.get(ready.runId());
            assertEquals(RunStatus.READY, waiting.status());
            assertEquals(2, waiting.attemptCount());
            assertEquals(1, monitor.retryRecovery().waitingRetries());
            assertEquals("LE_5S",
                    monitor.retryRecovery().nextDueDelayBucket());
            assertEquals("CYCLE_BOUNDARY",
                    monitor.retryRecovery().maintenanceCursorHealth());

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            RunSnapshot completed = waiting;
            while (completed.status() != RunStatus.SUCCEEDED
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
                completed = store.get(ready.runId());
            }
            assertEquals(RunStatus.SUCCEEDED, completed.status());
            assertEquals(3, completed.attemptCount());
        }
    }

    @Test
    void retryMaintenanceWakesForEarlierDeadline() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore store = registry.runStateStore();
        String runId = "retry-deadline-wake";
        String definitionId = "retry-deadline-definition";
        PlanArtifact artifact = artifact(
                definitionId, new DiscardSink(META));
        String artifactId = ((ArtifactRepositoryProvider) registry)
                .artifactRepository().put(PlanArtifactCodec.encode(artifact));
        long submittedAt = System.currentTimeMillis();
        RunSnapshot ready = accepted(store, RunCommand.createRun(
                "create-" + runId, runId, definitionId, artifactId,
                3, submittedAt + 4_000L, submittedAt));
        RunSnapshot first = accepted(store, RunCommand.createAttempt(
                "first-" + runId, runId, ready.revision(),
                "attempt-1", "publication-1", submittedAt));
        WholeAttemptRetryAuthority retry = (WholeAttemptRetryAuthority) store;
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                retry.scheduleRetry(new RunAttemptFence(
                        runId, first.currentAttempt().attemptId(),
                        first.currentAttempt().fenceToken())).code());
        RetryReadyEvaluation firstDue;
        long firstDueDeadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(5);
        do {
            firstDue = retry.evaluateRetryReady(runId);
            if (firstDue.code() != RetryReadyEvaluation.Code.DUE) {
                Thread.sleep(10L);
            }
        } while (firstDue.code() != RetryReadyEvaluation.Code.DUE
                && System.nanoTime() < firstDueDeadline);
        assertEquals(RetryReadyEvaluation.Code.DUE, firstDue.code());
        io.rowweave.run.RetryAttemptClaim second = retry.claimDueRetry(runId);
        assertEquals(io.rowweave.run.RetryAttemptClaim.Code.CLAIMED,
                second.code());
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                retry.scheduleRetry(new RunAttemptFence(
                        runId, second.snapshot().currentAttempt().attemptId(),
                        second.snapshot().currentAttempt().fenceToken())).code());

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            awaitState(master, LeaderRecoveryInventory.State.EMPTY_READY);

            long terminalDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(4);
            RunSnapshot terminal = store.get(runId);
            while (terminal.status() != RunStatus.FAILED
                    && System.nanoTime() < terminalDeadline) {
                Thread.sleep(10L);
                terminal = store.get(runId);
            }
            assertEquals(RunStatus.FAILED, terminal.status());
            assertEquals(2, terminal.attemptCount());
        }
    }

    @Test
    void replacementLeaderRecomputesPendingRetryWithoutDurableCursor(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore store = registry.runStateStore();
        RunSnapshot ready = createReady(
                registry, "retry-maintenance-failover",
                "retry-maintenance-failover-definition", 3, 31L);
        RunSnapshot first = accepted(store, RunCommand.createAttempt(
                "first-maintenance-failover", ready.runId(), ready.revision(),
                "attempt-1", "publication-1", 32L));
        WholeAttemptRetryAuthority retry = (WholeAttemptRetryAuthority) store;
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                retry.scheduleRetry(new RunAttemptFence(
                        ready.runId(), first.currentAttempt().attemptId(),
                        first.currentAttempt().fenceToken())).code());
        RetryReadyEvaluation firstDue;
        long firstDueDeadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(5);
        do {
            firstDue = retry.evaluateRetryReady(ready.runId());
            if (firstDue.code() != RetryReadyEvaluation.Code.DUE) {
                Thread.sleep(10L);
            }
        } while (firstDue.code() != RetryReadyEvaluation.Code.DUE
                && System.nanoTime() < firstDueDeadline);
        assertEquals(RetryReadyEvaluation.Code.DUE, firstDue.code());
        io.rowweave.run.RetryAttemptClaim second =
                retry.claimDueRetry(ready.runId());
        assertEquals(io.rowweave.run.RetryAttemptClaim.Code.CLAIMED,
                second.code());
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                retry.scheduleRetry(new RunAttemptFence(
                        ready.runId(),
                        second.snapshot().currentAttempt().attemptId(),
                        second.snapshot().currentAttempt().fenceToken())).code());

        try (MasterServer firstMaster = new MasterServer(5_000L, 0, registry);
             MasterElection firstElection = new MasterElection(
                     registry, new Endpoint(
                             "127.0.0.1", firstMaster.port()))) {
            firstMaster.bindLeadership(firstElection);
            firstElection.start();
            awaitState(firstMaster, LeaderRecoveryInventory.State.EMPTY_READY);
            assertEquals(RunStatus.READY,
                    store.get(ready.runId()).status());
        }

        try (MasterServer replacement = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = worker(
                     replacement, tmp, "retry-failover-worker");
             MasterElection replacementElection = new MasterElection(
                     registry, new Endpoint(
                             "127.0.0.1", replacement.port()))) {
            replacement.bindLeadership(replacementElection);
            replacementElection.start();
            awaitState(replacement, LeaderRecoveryInventory.State.EMPTY_READY);

            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(8);
            RunSnapshot completed = store.get(ready.runId());
            while (completed.status() != RunStatus.SUCCEEDED
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
                completed = store.get(ready.runId());
            }
            assertEquals(RunStatus.SUCCEEDED, completed.status());
            assertEquals(3, completed.attemptCount());
        }
    }

    @Test
    void noWorkerPreflightRetriesWithoutAnyRunMutation() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunSnapshot before = createReady(
                registry, "no-worker-recovery", "no-worker-definition", 1, 19L);
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            awaitState(master, LeaderRecoveryInventory.State.BLOCKED_UNAVAILABLE);
            RunSnapshot after = registry.runStateStore().get("no-worker-recovery");
            assertEquals(before.revision(), after.revision());
            assertEquals(0, after.attemptCount());
            assertEquals(RunStatus.READY, after.status());
        }
    }

    @Test
    void legacyReadySourceIsRejectedBeforeRecoveryAttemptMutation(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        PlanArtifact artifact = legacyArtifact("legacy-ready-definition",
                new DiscardSink(META));
        ArtifactRepository repository =
                ((ArtifactRepositoryProvider) registry).artifactRepository();
        String artifactId = repository.put(PlanArtifactCodec.encode(artifact));
        RunSnapshot before = accepted(registry.runStateStore(), RunCommand.createRun(
                "create-legacy-ready", "legacy-ready", "legacy-ready-definition",
                artifactId, 1, 0L, 21L));

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = worker(master, tmp, "legacy-ready-worker");
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            awaitState(master, LeaderRecoveryInventory.State.BLOCKED_CORRUPT);
            RunSnapshot after = registry.runStateStore().get("legacy-ready");
            assertEquals(before.revision(), after.revision());
            assertEquals(0, after.attemptCount());
            assertEquals(RunStatus.READY, after.status());
            assertTrue(master.recoveryInventorySnapshot().failure()
                    .contains("UNSUPPORTED_SOURCE_REPLAY"));
        }
    }

    @Test
    void missingPinnedSnapshotFailsRecoveredAttemptWithoutPublishingOutput(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        Path sourceDomain = tmp.resolve("missing-source");
        Path outputDomain = tmp.resolve("missing-output");
        long generation = publishSourceSnapshot(sourceDomain);
        RunSnapshot ready = createReady(registry, "missing-snapshot-recovery",
                "missing-snapshot-definition", 1, 25L,
                artifact("missing-snapshot-definition",
                        new ManifestFileSource(META, sourceDomain.toString(),
                                "ledger", generation),
                        new ManifestFileSink(META, outputDomain.toString(), "result")));
        Files.delete(sourceDomain.resolve("manifests").resolve(generation + ".manifest"));

        recoverAndAssertSourceFailure(registry, tmp, "missing-snapshot-worker",
                ready, outputDomain);
    }

    @Test
    void changedPinnedSnapshotFailsRecoveredAttemptWithoutPublishingOutput(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        Path sourceDomain = tmp.resolve("changed-source");
        Path outputDomain = tmp.resolve("changed-output");
        long generation = publishSourceSnapshot(sourceDomain);
        ManifestFileStore sourceStore = new ManifestFileStore(sourceDomain);
        CommitManifest manifest = sourceStore.exact(generation);
        Path sourceData = sourceDomain.resolve(
                manifest.members().get(0).relativeDataPath());
        RunSnapshot ready = createReady(registry, "changed-snapshot-recovery",
                "changed-snapshot-definition", 1, 27L,
                artifact("changed-snapshot-definition",
                        new ManifestFileSource(META, sourceDomain.toString(),
                                "ledger", generation),
                        new ManifestFileSink(META, outputDomain.toString(), "result")));
        Files.write(sourceData, new byte[] {0});

        recoverAndAssertSourceFailure(registry, tmp, "changed-snapshot-worker",
                ready, outputDomain);
    }

    @Test
    void insufficientSlotsExhaustTheOnlySupportedAttempt(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        createReady(registry, "slot-race", "slot-definition", 1, 23L);
        File spill = tmp.resolve("slot-worker-spill").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "slot-worker", 1, "127.0.0.1", master.port(), spill);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));
            RunSnapshot after = registry.runStateStore().get("slot-race");
            assertEquals(RunStatus.FAILED, after.status());
            assertEquals(AttemptStatus.FAILED, after.currentAttempt().status());
            assertEquals(1, after.attemptCount());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void unsafeAttemptMatrixHasNoAssignmentOutputOrRunMutation(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunStateStore store = registry.runStateStore();
        RunSnapshot dispatching = createAt(
                registry, "unsafe-dispatching", AttemptStatus.DISPATCHING);
        RunSnapshot running = createAt(
                registry, "unsafe-running", AttemptStatus.RUNNING);
        RunSnapshot committing = createAt(
                registry, "unsafe-committing", AttemptStatus.COMMITTING);
        RunSnapshot reconcilingBase = createAt(
                registry, "unsafe-reconciling", AttemptStatus.COMMITTING);
        RunSnapshot reconciling = accepted(store, RunCommand.requestCancellation(
                "reconcile-unsafe-reconciling", reconcilingBase.runId(),
                reconcilingBase.revision(), reconcilingBase.currentAttempt().attemptId(),
                reconcilingBase.currentAttempt().fenceToken(), CancellationCause.USER,
                40L, "TEST", "reconcile"));
        RunSnapshot[] before = {
                dispatching, running, committing, reconciling
        };

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = worker(master, tmp, "unsafe-worker");
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_CORRUPT,
                    master.recoveryInventorySnapshot().state());
            assertEquals(4, master.recoveryInventorySnapshot().activeRuns());
            for (RunSnapshot expected : before) {
                RunSnapshot actual = store.get(expected.runId());
                assertEquals(expected.revision(), actual.revision(), expected.runId());
                assertEquals(expected.status(), actual.status(), expected.runId());
                assertEquals(expected.currentAttempt().status(),
                        actual.currentAttempt().status(), expected.runId());
            }
        }
    }

    @Test
    void replacementLeaderCompletesCreatedUserCancellationWithoutWorkerOrAssignments()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        RunSnapshot created = createAt(
                registry, "cancel-created-recovery", AttemptStatus.CREATED);
        ((RunCancellationAuthority) registry.runStateStore())
                .requestUserCancellation(created.runId(), "request-created-recovery");

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));
            RunSnapshot canceled = registry.runStateStore().get(created.runId());
            assertEquals(RunStatus.CANCELED, canceled.status());
            assertEquals(AttemptStatus.CANCELED,
                    canceled.currentAttempt().status());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
            assertNull(registry.durableStageAssignmentAuthority()
                    .assignmentSnapshot(new io.rowweave.run.RunAttemptFence(
                            created.runId(), created.currentAttempt().attemptId(),
                            created.currentAttempt().fenceToken())));
        }
    }

    private static WorkerServer worker(MasterServer master, Path tmp, String workerId) {
        File spill = tmp.resolve(workerId + "-spill").toFile();
        assertTrue(spill.mkdirs());
        return WorkerServer.newUnguarded(
                workerId, 8, "127.0.0.1", master.port(), spill);
    }

    private static RunSnapshot createReady(InProcessRegistry registry, String runId,
                                           String definitionId, int maxAttempts,
                                           long eventTime) {
        return createReady(registry, runId, definitionId, maxAttempts, eventTime,
                new DiscardSink(META));
    }

    private static RunSnapshot createReady(InProcessRegistry registry, String runId,
                                           String definitionId, int maxAttempts,
                                           long eventTime, Sink output) {
        PlanArtifact artifact = artifact(definitionId, output);
        ArtifactRepository repository =
                ((ArtifactRepositoryProvider) registry).artifactRepository();
        String artifactId = repository.put(PlanArtifactCodec.encode(artifact));
        return accepted(registry.runStateStore(), RunCommand.createRun(
                "create-" + runId, runId, definitionId, artifactId,
                maxAttempts, 0L, eventTime));
    }

    private static RunSnapshot createReady(InProcessRegistry registry, String runId,
                                           String definitionId, int maxAttempts,
                                           long eventTime, PlanArtifact artifact) {
        ArtifactRepository repository =
                ((ArtifactRepositoryProvider) registry).artifactRepository();
        String artifactId = repository.put(PlanArtifactCodec.encode(artifact));
        return accepted(registry.runStateStore(), RunCommand.createRun(
                "create-" + runId, runId, definitionId, artifactId,
                maxAttempts, 0L, eventTime));
    }

    private static RunSnapshot createAt(InProcessRegistry registry, String runId,
                                        AttemptStatus target) {
        RunStateStore store = registry.runStateStore();
        RunSnapshot snapshot = createReady(
                registry, runId, "unsafe-definition", 1, 1L);
        snapshot = accepted(store, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt-" + runId, "publication-" + runId, 2L));
        if (target == AttemptStatus.CREATED) return snapshot;
        AttemptStatus[] path = {
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING, AttemptStatus.COMMITTING
        };
        long eventTime = 3L;
        for (AttemptStatus step : path) {
            snapshot = accepted(store, RunCommand.advanceAttempt(
                    "advance-" + runId + '-' + step.name(), runId,
                    snapshot.revision(), snapshot.currentAttempt().attemptId(),
                    snapshot.currentAttempt().fenceToken(), step, false,
                    eventTime++, null, null));
            if (step == target) return snapshot;
        }
        throw new IllegalArgumentException("unsupported target " + target);
    }

    private static RunSnapshot accepted(RunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static PlanArtifact artifact(String definitionId, Sink output) {
        return artifact(definitionId,
                new CollectionSource(META, definitionId + "-split",
                        Arrays.asList(row(7), row(8))), output);
    }

    private static PlanArtifact artifact(String definitionId, io.rowweave.Source input,
                                         Sink output) {
        return LegacyPlanArtifactAdapter.wrap(definitionId,
                new PlanGraph(Arrays.asList(
                        PlanNode.sourceFromInputs(META,
                                        java.util.Collections.singletonList(input))
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, output).build()
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), 1));
    }

    private static PlanArtifact qualityArtifact(String definitionId, Path domain) {
        ExecutionEnvironment session = new ExecutionEnvironment("quality-recovery", "quality recovery");
        session.setDefinitionId(definitionId);
        session.setParallelism(2);
        DataRowClassification classified = session.fromSource(new CollectionSource(
                        QUALITY_META, "quality-recovery-split", Arrays.asList(
                        qualityRow("A", 1), qualityRow(null, 2), qualityRow("B", 13))))
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(RowValidation.builder(QUALITY_META)
                        .notNull("adaptor.required", 0)
                        .integerRange("period.range", 1, 1, 12)
                        .build());
        classified.writeToManifest(TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary"));
        return session.toPlanArtifact();
    }

    private static PlanArtifact objectQualityArtifact(
            String definitionId, Path domain, byte[] content) throws Exception {
        ObjectStorageSnapshotSource input = new ObjectStorageSnapshotSource(
                META, new RecoveryObjectProvider(), "endpoint-prod", "credential-prod",
                "domain", "ledger/retry.tsv", "generation-42", content.length,
                sha256(content), ObjectStorageSnapshotSource.MIN_ROW_BYTES);
        ExecutionEnvironment session = new ExecutionEnvironment(
                "object-quality-recovery", "object quality recovery");
        session.setDefinitionId(definitionId);
        session.setParallelism(2);
        session.classifyObjectStorageRows("ledger.object.source", input)
                .writeToManifest(TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary"));
        return session.toPlanArtifact();
    }

    private static DataRow qualityRow(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static List<String> qualitySummaryLines(Path domain) {
        List<String> result = new java.util.ArrayList<>();
        try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                TypedQualityManifest.summarySchema(), domain.toString(), "summary")
                .openReader()) {
            while (reader.hasNext()) {
                DataRow row = reader.next();
                result.add(row.getString(0) + "|" + row.getString(1) + "|"
                        + row.getString(2) + "|" + row.getInteger(3) + "|"
                        + row.getString(4) + "|" + row.getLong(5));
            }
        }
        return result;
    }

    private static PlanArtifact legacyArtifact(String definitionId, Sink output) {
        return LegacyPlanArtifactAdapter.wrap(definitionId,
                new PlanGraph(Arrays.asList(
                        PlanNode.sourceFromInputs(META, java.util.Collections.singletonList(
                                        new CollectionSource(META,
                                                Arrays.asList(row(7), row(8)))))
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, output).build()
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), 1));
    }

    private static DataRow row(int value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static String sha256(byte[] value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
        char[] output = new char[digest.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < digest.length; i++) {
            output[i * 2] = digits[(digest[i] >>> 4) & 0xf];
            output[i * 2 + 1] = digits[digest[i] & 0xf];
        }
        return new String(output);
    }

    public static final class RecoveryObjectProvider
            implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        private static volatile byte[] content;
        static final AtomicInteger credentialGeneration = new AtomicInteger();
        static final AtomicInteger opens = new AtomicInteger();
        static final AtomicInteger closes = new AtomicInteger();
        static final List<Integer> openedCredentialGenerations =
                new CopyOnWriteArrayList<>();
        static final List<String> openedCredentialRefs =
                new CopyOnWriteArrayList<>();
        static final List<String> openedVersions = new CopyOnWriteArrayList<>();

        public RecoveryObjectProvider() { }

        static void reset(byte[] bytes) {
            content = bytes.clone();
            credentialGeneration.set(0);
            opens.set(0);
            closes.set(0);
            openedCredentialGenerations.clear();
            openedCredentialRefs.clear();
            openedVersions.clear();
        }

        @Override public String providerType() { return "recovery-fixture"; }
        @Override public int providerVersion() { return 1; }

        @Override
        public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            opens.incrementAndGet();
            openedCredentialGenerations.add(credentialGeneration.get());
            openedCredentialRefs.add(request.credentialRef());
            openedVersions.add(request.versionId());
            return new RecoveryObjectHandle(request, content.clone());
        }
    }

    private static final class RecoveryObjectHandle
            implements ObjectStorageReadHandle {
        private final ObjectStorageReadRequest request;
        private final InputStream stream;
        private boolean closed;

        private RecoveryObjectHandle(ObjectStorageReadRequest request, byte[] content) {
            this.request = request;
            this.stream = new ByteArrayInputStream(content);
        }

        @Override public String providerType() { return "recovery-fixture"; }
        @Override public int providerVersion() { return 1; }
        @Override public String versionId() { return request.versionId(); }
        @Override public long contentLength() { return request.contentLength(); }
        @Override public InputStream stream() { return stream; }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            RecoveryObjectProvider.closes.incrementAndGet();
        }
    }

    private static long publishSourceSnapshot(Path domain) {
        ExecutionEnvironment session = io.rowweave.RowWeave.createEnvironment("recovery-source-snapshot");
        session.fromSource(new CollectionSource(META, Arrays.asList(row(7), row(8))))
                .sinkTo(new ManifestFileSink(META, domain.toString(), "ledger"));
        session.execute();
        return new ManifestFileStore(domain).latest().generation();
    }

    private static void recoverAndAssertSourceFailure(
            InProcessRegistry registry, Path tmp, String workerId,
            RunSnapshot ready, Path outputDomain) throws Exception {
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = worker(master, tmp, workerId);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(inventory, 10, TimeUnit.SECONDS));
            RunSnapshot failed = registry.runStateStore().get(ready.runId());
            assertEquals(RunStatus.FAILED, failed.status());
            assertEquals(AttemptStatus.FAILED, failed.currentAttempt().status());
            assertEquals(1, failed.attemptCount());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
            assertNull(new ManifestFileStore(outputDomain).latest());
        }
    }

    private static void assertManifestRows(Path domain) throws Exception {
        try (io.rowweave.FileSource.Reader reader =
                     new ManifestFileSource(META, domain.toString(), "ledger").openReader()) {
            assertEquals(7, reader.next().get(0));
            assertEquals(8, reader.next().get(0));
            assertFalse(reader.hasNext());
        }
    }

    private static void awaitState(MasterServer master,
                                   LeaderRecoveryInventory.State expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (master.recoveryInventorySnapshot().state() != expected
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(expected, master.recoveryInventorySnapshot().state());
    }
}
