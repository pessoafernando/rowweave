package io.rowweave.cluster.registry.zk;

import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.DurableExecutionEvidenceView;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RetentionToken;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunPurgeProgress;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunExecutionEventPageCodec;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RunStateStoreUnavailableException;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageManifestPublication;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** deterministic interleavings for coherent durable Run reads. */
class ZkRunPublicationReadCoherenceTest {

    @Test
    void bareCreateAfterEvidenceSnapshotReadConvergesToSnapshotOnly() throws Exception {
        try (Harness harness = new Harness("evidence-bare")) {
            harness.precreateLayout();
            RunCommand create = harness.create("create", "definition");
            harness.publishOnceAt(
                    ZkRunStateStore.EvidenceReadPhase.EVIDENCE_AFTER_INITIAL_SNAPSHOT,
                    () -> accepted(harness.writer, create));

            DurableExecutionEvidenceView view =
                    harness.reader.executionEvidence(harness.runId);

            assertEquals(DurableExecutionEvidenceView.State.SNAPSHOT_ONLY, view.state());
            assertEquals(harness.runId, view.snapshot().runId());
        }
    }

    @Test
    void assignedCreateAfterEvidenceSnapshotReadConvergesToPresent() throws Exception {
        try (Harness harness = new Harness("evidence-present")) {
            harness.precreateLayout();
            RunCommand create = harness.create("create", "definition");
            harness.publishOnceAt(
                    ZkRunStateStore.EvidenceReadPhase.EVIDENCE_AFTER_INITIAL_SNAPSHOT,
                    () -> {
                        RunSnapshot run = accepted(harness.writer, create);
                        run = accepted(harness.writer, RunCommand.createAttempt(
                                "attempt", harness.runId, run.revision(),
                                "attempt", "publication", 2L));
                        assertEquals(StageManifestPublication.Code.PUBLISHED,
                                harness.writer.publish(manifest(run)).code());
                    });

            DurableExecutionEvidenceView view =
                    harness.reader.executionEvidence(harness.runId);

            assertEquals(DurableExecutionEvidenceView.State.PRESENT, view.state());
            assertEquals(harness.runId, view.snapshot().runId());
        }
    }

    @Test
    void sameCreateAfterInitialSnapshotReadReplays() throws Exception {
        assertCreateRaceReplays(
                "apply-after-snapshot",
                ZkRunStateStore.EvidenceReadPhase.APPLY_AFTER_INITIAL_SNAPSHOT);
    }

    @Test
    void sameCreateAfterInitialLifecycleReadReplays() throws Exception {
        assertCreateRaceReplays(
                "apply-after-lifecycle",
                ZkRunStateStore.EvidenceReadPhase.APPLY_AFTER_INITIAL_LIFECYCLE);
    }

    @Test
    void sameCreateAfterReceiptReadReplays() throws Exception {
        assertCreateRaceReplays(
                "apply-after-receipt",
                ZkRunStateStore.EvidenceReadPhase.APPLY_AFTER_RECEIPT);
    }

    @Test
    void differentCreateCommandAfterLifecycleReadKeepsRunAlreadyExists() throws Exception {
        try (Harness harness = new Harness("apply-different-command")) {
            RunCommand reader = harness.create("reader-command", "definition");
            RunCommand writer = harness.create("writer-command", "definition");
            harness.publishOnceAt(
                    ZkRunStateStore.EvidenceReadPhase.APPLY_AFTER_INITIAL_LIFECYCLE,
                    () -> accepted(harness.writer, writer));

            assertEquals(RunDecision.Code.RUN_ALREADY_EXISTS,
                    harness.reader.apply(reader).code());
        }
    }

    @Test
    void reusedCommandIdWithDifferentDigestAfterLifecycleReadKeepsConflict()
            throws Exception {
        try (Harness harness = new Harness("apply-command-conflict")) {
            RunCommand reader = harness.create("shared-command", "definition-a");
            RunCommand writer = harness.create("shared-command", "definition-b");
            harness.publishOnceAt(
                    ZkRunStateStore.EvidenceReadPhase.APPLY_AFTER_INITIAL_LIFECYCLE,
                    () -> accepted(harness.writer, writer));

            assertEquals(RunDecision.Code.COMMAND_ID_CONFLICT,
                    harness.reader.apply(reader).code());
        }
    }

    @Test
    void cancellationAfterInitialSnapshotReadUsesPublishedRun() throws Exception {
        try (Harness harness = new Harness("cancel-publication")) {
            harness.precreateLayout();
            harness.publishOnceAt(
                    ZkRunStateStore.EvidenceReadPhase.CANCEL_AFTER_INITIAL_SNAPSHOT,
                    () -> accepted(harness.writer,
                            harness.create("create", "definition")));

            RunCancellationResult result = harness.reader
                    .requestUserCancellation(harness.runId, "request");

            assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                    result.disposition());
        }
    }

    @Test
    void firstEventPublishedAfterInitialSnapshotReadIsNotAnOrphan() throws Exception {
        assertFirstEventPublication(
                "events-after-snapshot",
                ZkRunStateStore.EvidenceReadPhase.EVENTS_AFTER_INITIAL_SNAPSHOT);
    }

    @Test
    void firstEventPublishedAfterInitialLifecycleReadIsNotAnOrphan() throws Exception {
        assertFirstEventPublication(
                "events-after-lifecycle",
                ZkRunStateStore.EvidenceReadPhase.EVENTS_AFTER_INITIAL_LIFECYCLE);
    }

    @Test
    void eventAppendAfterChildrenReadIsDetectedByFinalVersionFence() throws Exception {
        try (Harness harness = new Harness("events-append")) {
            RunSnapshot run = accepted(harness.writer,
                    harness.create("create", "definition"));
            RunSnapshot initial = run;
            harness.publishOnceAt(
                    ZkRunStateStore.EvidenceReadPhase.EVENTS_AFTER_CHILDREN,
                    () -> accepted(harness.writer, RunCommand.createAttempt(
                            "attempt", harness.runId, initial.revision(),
                            "attempt", "publication", 2L)));

            List<ExecutionEvent> events = harness.reader.events(harness.runId, 0L, 10);

            assertEquals(2, events.size());
            assertEquals(2L, events.get(1).sequence());
        }
    }

    @Test
    void coreAbsentWithPersistentEventChildrenRemainsCorruption() throws Exception {
        try (Harness harness = new Harness("events-orphan")) {
            accepted(harness.writer, harness.create("create", "definition"));
            harness.deleteCoreAuthority();

            assertThrows(IllegalStateException.class,
                    () -> harness.reader.events(harness.runId, 0L, 10));
        }
    }

    @Test
    void coreAbsentWithPersistentReceiptAndCatalogRemainsCorruption()
            throws Exception {
        try (Harness harness = new Harness("apply-orphan")) {
            RunCommand create = harness.create("create", "definition");
            accepted(harness.writer, create);
            harness.deleteCoreAuthority();

            assertThrows(IllegalStateException.class,
                    () -> harness.reader.apply(create));
        }
    }

    @Test
    void confirmationCoreRemovalWithLayoutResidueIsCorruptionNotSnapshotOnly()
            throws Exception {
        try (Harness harness = new Harness("confirmation-layout-residue")) {
            RunSnapshot run = accepted(harness.writer,
                    harness.create("create", "definition"));
            run = accepted(harness.writer, RunCommand.createAttempt(
                    "attempt", harness.runId, run.revision(),
                    "attempt", "publication", 2L));
            assertEquals(StageManifestPublication.Code.PUBLISHED,
                    harness.writer.publish(manifest(run)).code());
            harness.publishOnceAt(
                    ZkRunStateStore.EvidenceReadPhase.EVIDENCE_AFTER_CONFIRMATION_SNAPSHOT,
                    harness::deleteCoreAuthority);

            assertThrows(IllegalStateException.class,
                    () -> harness.reader.executionEvidence(harness.runId));
        }
    }

    @Test
    void initialSnapshotOnlyOrphanIsCorruption() throws Exception {
        try (Harness harness = new Harness("initial-snapshot-only")) {
            accepted(harness.writer, harness.create("create", "definition"));
            harness.deleteLifecycle();

            assertThrows(IllegalStateException.class,
                    () -> harness.reader.executionEvidence(harness.runId));
        }
    }

    @Test
    void initialLifecycleOnlyOrphanIsCorruption() throws Exception {
        try (Harness harness = new Harness("initial-lifecycle-only")) {
            accepted(harness.writer, harness.create("create", "definition"));
            harness.deleteSnapshot();

            assertThrows(IllegalStateException.class,
                    () -> harness.reader.executionEvidence(harness.runId));
        }
    }

    @Test
    void confirmationSnapshotOnlyOrphanIsCorruption() throws Exception {
        try (Harness harness = new Harness("confirmation-snapshot-only")) {
            assignedRun(harness);
            harness.publishOnceAt(
                    ZkRunStateStore.EvidenceReadPhase.EVIDENCE_AFTER_CONFIRMATION_SNAPSHOT,
                    harness::deleteLifecycle);

            assertThrows(IllegalStateException.class,
                    () -> harness.reader.executionEvidence(harness.runId));
        }
    }

    @Test
    void confirmationLifecycleOnlyOrphanIsCorruption() throws Exception {
        try (Harness harness = new Harness("confirmation-lifecycle-only")) {
            assignedRun(harness);
            harness.publishOnceAt(
                    ZkRunStateStore.EvidenceReadPhase.EVIDENCE_AFTER_CONFIRMATION_SNAPSHOT,
                    harness::deleteSnapshot);

            assertThrows(IllegalStateException.class,
                    () -> harness.reader.executionEvidence(harness.runId));
        }
    }

    @Test
    void confirmationCoreMismatchThenConnectionLossCannotUseOldSnapshot()
            throws Exception {
        assertMismatchThenTransportUnavailable(
                "mismatch-connection-loss", true);
    }

    @Test
    void confirmationCoreMismatchThenOperationTimeoutCannotUseOldSnapshot()
            throws Exception {
        assertMismatchThenTransportUnavailable(
                "mismatch-operation-timeout", false);
    }

    @Test
    void confirmationVersionChangeThenTransportCannotUseOldSnapshot()
            throws Exception {
        try (Harness harness = new Harness("version-change-transport")) {
            RunSnapshot assigned = assignedRun(harness);
            AtomicBoolean advanced = new AtomicBoolean();
            AtomicBoolean failOnce = new AtomicBoolean(true);
            harness.reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase.BEFORE_CONFIRMATION
                        && advanced.compareAndSet(false, true)) {
                    accepted(harness.writer, RunCommand.advanceAttempt(
                            "dispatch", harness.runId, assigned.revision(),
                            assigned.currentAttempt().attemptId(),
                            assigned.currentAttempt().fenceToken(),
                            AttemptStatus.DISPATCHING, false,
                            3L, null, null));
                } else if (phase == ZkRunStateStore.EvidenceReadPhase
                        .EVIDENCE_AFTER_INITIAL_SNAPSHOT
                        && advanced.get() && failOnce.compareAndSet(true, false)) {
                    throw new org.apache.zookeeper.KeeperException
                            .ConnectionLossException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> harness.reader.executionEvidence(harness.runId));
        }
    }

    @Test
    void initialMismatchConfirmationTransportCannotUseOldSnapshot()
            throws Exception {
        try (Harness harness = new Harness("initial-mismatch-transport")) {
            RunSnapshot assigned = assignedRun(harness);
            AtomicBoolean advanced = new AtomicBoolean();
            AtomicBoolean removed = new AtomicBoolean();
            harness.reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase.BEFORE_CONFIRMATION
                        && advanced.compareAndSet(false, true)) {
                    accepted(harness.writer, RunCommand.advanceAttempt(
                            "dispatch", harness.runId, assigned.revision(),
                            assigned.currentAttempt().attemptId(),
                            assigned.currentAttempt().fenceToken(),
                            AttemptStatus.DISPATCHING, false,
                            3L, null, null));
                } else if (phase == ZkRunStateStore.EvidenceReadPhase
                        .EVIDENCE_AFTER_INITIAL_SNAPSHOT
                        && advanced.get() && removed.compareAndSet(false, true)) {
                    harness.deleteLifecycle();
                } else if (phase == ZkRunStateStore.EvidenceReadPhase
                        .EVIDENCE_BEFORE_INITIAL_MISMATCH_CONFIRMATION
                        && removed.get()) {
                    throw new org.apache.zookeeper.KeeperException
                            .OperationTimeoutException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> harness.reader.executionEvidence(harness.runId));
        }
    }

    @Test
    void confirmationSnapshotAbsenceThenTransportCannotUseOldSnapshot()
            throws Exception {
        try (Harness harness = new Harness("confirmation-absence-transport")) {
            assignedRun(harness);
            AtomicBoolean removed = new AtomicBoolean();
            harness.reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase.BEFORE_CONFIRMATION
                        && removed.compareAndSet(false, true)) {
                    harness.deleteCoreAuthority();
                } else if (phase == ZkRunStateStore.EvidenceReadPhase
                        .EVIDENCE_AFTER_CONFIRMATION_SNAPSHOT
                        && removed.get()) {
                    throw new org.apache.zookeeper.KeeperException
                            .ConnectionLossException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> harness.reader.executionEvidence(harness.runId));
        }
    }

    @Test
    void catalogOnlyDependentOrphanIsCorruption() throws Exception {
        try (Harness harness = new Harness("catalog-only-orphan")) {
            RunCommand original = harness.create("writer-create", "definition");
            accepted(harness.writer, original);
            harness.deleteCoreAuthority();
            harness.deleteEventAndReceipt(original);

            assertThrows(IllegalStateException.class,
                    () -> harness.reader.apply(
                            harness.create("reader-create", "definition")));
        }
    }

    @Test
    void contentAddressedReferenceOnlyDependentOrphanIsCorruption()
            throws Exception {
        try (Harness harness = new Harness("reference-only-orphan")) {
            String artifactId = sha256Hex("content-addressed-artifact");
            RunCommand original = RunCommand.createRun(
                    "writer-create", harness.runId, "definition",
                    artifactId, 2, 0L, 1L);
            accepted(harness.writer, original);
            harness.deleteCoreAuthority();
            harness.deleteEventAndReceipt(original);
            harness.deleteCatalog();

            RunCommand retry = RunCommand.createRun(
                    "reader-create", harness.runId, "definition",
                    artifactId, 2, 0L, 1L);
            assertThrows(IllegalStateException.class,
                    () -> harness.reader.apply(retry));
        }
    }

    @Test
    void masterRechecksRealZkEventsWhenSecondSessionCreatesBeforeGet()
            throws Exception {
        try (Harness harness = new Harness("master-zk-publication")) {
            harness.precreateLayout();
            CreateBeforeGetStore store = new CreateBeforeGetStore(
                    harness.reader, harness.writer,
                    harness.create("create", "definition"));
            try (MasterServer master = new MasterServer(
                    60_000L, 0, new StoreRegistry(store))) {
                RpcMessage response = master.handler().apply(
                        RpcMessage.queryRunEvents(harness.runId, 0L, 2));

                assertTrue(response.success(), response.errorMessage());
                RunExecutionEventPage page = RunExecutionEventPageCodec.decode(
                        response.runEventPageBytes());
                assertEquals(RunExecutionEventPage.State.FOUND, page.state());
                assertEquals(1, page.events().size());
                assertEquals(2, store.eventReads);
            }
        }
    }

    @Test
    void finalPurgeAfterConfirmationSnapshotConvergesToMissing() throws Exception {
        try (Harness harness = new Harness("confirmation-final-purge")) {
            RunSnapshot run = accepted(harness.writer,
                    harness.create("create", "definition"));
            run = accepted(harness.writer, RunCommand.createAttempt(
                    "attempt", harness.runId, run.revision(),
                    "attempt", "publication", 2L));
            assertEquals(StageManifestPublication.Code.PUBLISHED,
                    harness.writer.publish(manifest(run)).code());
            RunDecision terminal = harness.writer.apply(RunCommand.advanceAttempt(
                    "terminal", harness.runId, run.revision(), "attempt", 1L,
                    AttemptStatus.FAILED, false, 3L, "FAILED", "terminal"));
            assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
            RunLifecycleRef lifecycle = harness.writer.lifecycle(harness.runId);
            RunRetentionClaim claim = harness.writer.claim(
                    harness.runId, terminal.snapshot().revision(),
                    lifecycle.terminalAtMillis(), new RetentionOwner("master", 1L), 0L);
            assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
            RetentionToken token = claim.token();
            harness.publishOnceAt(
                    ZkRunStateStore.EvidenceReadPhase.EVIDENCE_AFTER_CONFIRMATION_SNAPSHOT,
                    () -> purge(harness.writer, token));

            assertEquals(DurableExecutionEvidenceView.State.MISSING,
                    harness.reader.executionEvidence(harness.runId).state());
        }
    }

    private static void assertCreateRaceReplays(
            String runId, ZkRunStateStore.EvidenceReadPhase phase) throws Exception {
        try (Harness harness = new Harness(runId)) {
            RunCommand create = harness.create("create", "definition");
            harness.publishOnceAt(phase, () -> accepted(harness.writer, create));

            assertEquals(RunDecision.Code.ALREADY_APPLIED,
                    harness.reader.apply(create).code());
        }
    }

    private static void assertFirstEventPublication(
            String runId, ZkRunStateStore.EvidenceReadPhase phase) throws Exception {
        try (Harness harness = new Harness(runId)) {
            harness.precreateLayout();
            harness.publishOnceAt(phase, () -> accepted(harness.writer,
                    harness.create("create", "definition")));

            List<ExecutionEvent> events = harness.reader.events(runId, 0L, 10);

            assertEquals(1, events.size());
            assertEquals(1L, events.get(0).sequence());
        }
    }

    private static void purge(ZkRunStateStore store, RetentionToken token) {
        RunPurgeProgress progress;
        do {
            progress = store.purge(token, 1000);
        } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);
        assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
    }

    private static RunSnapshot assignedRun(Harness harness) {
        RunSnapshot run = accepted(harness.writer,
                harness.create("create", "definition"));
        run = accepted(harness.writer, RunCommand.createAttempt(
                "attempt", harness.runId, run.revision(),
                "attempt", "publication", 2L));
        assertEquals(StageManifestPublication.Code.PUBLISHED,
                harness.writer.publish(manifest(run)).code());
        return run;
    }

    private static void assertMismatchThenTransportUnavailable(
            String runId, boolean connectionLoss) throws Exception {
        try (Harness harness = new Harness(runId)) {
            assignedRun(harness);
            AtomicBoolean removed = new AtomicBoolean();
            AtomicBoolean failOnce = new AtomicBoolean(true);
            harness.reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .EVIDENCE_AFTER_CONFIRMATION_SNAPSHOT
                        && removed.compareAndSet(false, true)) {
                    harness.deleteLifecycle();
                } else if (phase == ZkRunStateStore.EvidenceReadPhase
                        .EVIDENCE_AFTER_INITIAL_SNAPSHOT
                        && removed.get() && failOnce.compareAndSet(true, false)) {
                    if (connectionLoss) {
                        throw new org.apache.zookeeper.KeeperException
                                .ConnectionLossException();
                    }
                    throw new org.apache.zookeeper.KeeperException
                            .OperationTimeoutException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> harness.reader.executionEvidence(runId));
        }
    }

    private static RunSnapshot accepted(ZkRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static StageAssignmentManifest manifest(RunSnapshot run) {
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 1L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 7001, -1, "incarnation");
        return new StageAssignmentManifest(
                fence, run.artifactId(), 1L, Arrays.asList(record));
    }

    private static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            char[] out = new char[digest.length * 2];
            char[] alphabet = "0123456789abcdef".toCharArray();
            for (int i = 0; i < digest.length; i++) {
                out[i * 2] = alphabet[(digest[i] >>> 4) & 0xf];
                out[i * 2 + 1] = alphabet[digest[i] & 0xf];
            }
            return new String(out);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static CuratorFramework connectedClient(TestingServer server)
            throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .sessionTimeoutMs(1_000)
                .connectionTimeoutMs(250)
                .retryPolicy(new ExponentialBackoffRetry(10, 1))
                .build();
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
        return client;
    }

    private static final class Harness implements AutoCloseable {
        private final String runId;
        private final TestingServer server;
        private final CuratorFramework readerClient;
        private final CuratorFramework writerClient;
        private final ZkRunStateStore reader;
        private final ZkRunStateStore writer;

        private Harness(String runId) throws Exception {
            this.runId = runId;
            this.server = new TestingServer(true);
            this.readerClient = connectedClient(server);
            this.writerClient = connectedClient(server);
            this.reader = new ZkRunStateStore(readerClient);
            this.writer = new ZkRunStateStore(writerClient);
        }

        private RunCommand create(String commandId, String definitionId) {
            return RunCommand.createRun(
                    commandId, runId, definitionId, "artifact", 2, 0L, 1L);
        }

        private void publishOnceAt(
                ZkRunStateStore.EvidenceReadPhase target,
                CheckedRunnable publication) {
            AtomicBoolean once = new AtomicBoolean(true);
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == target && once.compareAndSet(true, false)) {
                    publication.run();
                }
            });
        }

        private void precreateLayout() throws Exception {
            String run = runPath();
            readerClient.create().forPath(run, new byte[0]);
            readerClient.create().forPath(
                    run + "/identity", runId.getBytes(StandardCharsets.UTF_8));
            readerClient.create().forPath(run + "/events", new byte[0]);
            readerClient.create().forPath(run + "/commands", new byte[0]);
            readerClient.create().forPath(run + "/sink-abort", new byte[0]);
        }

        private void deleteCoreAuthority() throws Exception {
            deleteSnapshot();
            deleteLifecycle();
        }

        private void deleteSnapshot() throws Exception {
            deleteIfPresent(runPath() + "/snapshot");
        }

        private void deleteLifecycle() throws Exception {
            deleteIfPresent(lifecyclePath());
        }

        private void deleteEventAndReceipt(RunCommand command) throws Exception {
            deleteIfPresent(runPath() + "/events/00000000000000000001");
            deleteIfPresent(runPath() + "/commands/"
                    + sha256Hex(command.commandId()));
        }

        private void deleteCatalog() throws Exception {
            deleteIfPresent(catalogPath());
        }

        private void deleteIfPresent(String path) throws Exception {
            if (readerClient.checkExists().forPath(path) != null) {
                readerClient.delete().forPath(path);
            }
        }

        private String runPath() {
            return "/rowweave/run-state-v1/runs/" + sha256Hex(runId);
        }

        private String lifecyclePath() {
            String digest = sha256Hex(runId);
            int shard = Integer.parseInt(digest.substring(0, 2), 16);
            return "/rowweave/run-lifecycle-v1/shards/"
                    + String.format(Locale.ROOT, "%02x", shard)
                    + "/entries/" + digest;
        }

        private String catalogPath() {
            String digest = sha256Hex(runId);
            int shard = Integer.parseInt(digest.substring(0, 2), 16);
            return "/rowweave/run-recovery-v1/shards/"
                    + String.format(Locale.ROOT, "%02x", shard)
                    + "/entries/" + digest;
        }

        @Override
        public void close() throws Exception {
            writerClient.close();
            readerClient.close();
            server.close();
        }
    }

    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private static final class CreateBeforeGetStore implements RunStateStore {
        private final ZkRunStateStore reader;
        private final ZkRunStateStore writer;
        private final RunCommand create;
        private final AtomicBoolean once = new AtomicBoolean(true);
        private int eventReads;

        private CreateBeforeGetStore(
                ZkRunStateStore reader, ZkRunStateStore writer,
                RunCommand create) {
            this.reader = reader;
            this.writer = writer;
            this.create = create;
        }

        @Override
        public RunDecision apply(RunCommand command) {
            return reader.apply(command);
        }

        @Override
        public RunSnapshot get(String runId) {
            if (once.compareAndSet(true, false)) accepted(writer, create);
            return reader.get(runId);
        }

        @Override
        public List<ExecutionEvent> events(
                String runId, long afterSequence, int limit) {
            eventReads++;
            return reader.events(runId, afterSequence, limit);
        }
    }

    private static final class StoreRegistry
            implements ServiceRegistry, RunStateStoreProvider {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final RunStateStore store;

        private StoreRegistry(RunStateStore store) {
            this.store = store;
        }

        @Override public RunStateStore runStateStore() { return store; }
        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() { return delegate.liveWorkers(); }
        @Override public RegistryWorker worker(String workerId) {
            return delegate.worker(workerId);
        }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }
}
