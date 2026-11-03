package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.RecoverableRunCatalog;
import io.rowweave.run.RecoverableRunPage;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunRecoveryOwnership;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderRecoveryInventoryTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));

    @Test
    void maintenancePublishesBoundedPageCursorBeforeContinuing() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        AtomicInteger scans = new AtomicInteger();
        CountDownLatch firstMaintenancePage = new CountDownLatch(1);
        RecoverableRunCatalog catalog = (cursor, limit) -> {
            int call = scans.incrementAndGet();
            if (call == 1) {
                return new RecoverableRunPage(
                        Collections.emptyList(), "initial-complete", true);
            }
            if (call == 2) {
                firstMaintenancePage.countDown();
                return new RecoverableRunPage(
                        Collections.emptyList(), "maintenance-next", false);
            }
            return new RecoverableRunPage(
                    Collections.emptyList(), "maintenance-complete", true);
        };

        try (LeaderRecoveryInventory inventory = new LeaderRecoveryInventory(
                store, catalog, store, repository,
                new LeaderRecoveryInventory.Config(
                        1, 1_000L, 1_000L, 1, 1_000L))) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 200L), () -> true);
            assertTrue(inventory.awaitSettled(
                    generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    inventory.snapshot().state());

            inventory.signal();
            assertTrue(firstMaintenancePage.await(5, TimeUnit.SECONDS));
            long published = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(1);
            while (!"maintenance-next".equals(
                    inventory.snapshot().maintenanceCursor())
                    && System.nanoTime() < published) {
                Thread.sleep(10L);
            }
            assertEquals("maintenance-next",
                    inventory.snapshot().maintenanceCursor());
            assertEquals(2, scans.get(),
                    "one maintenance wake must read at most one catalog page");
        }
    }

    @Test
    void maintenanceEventuallyVisitsEveryPageWithOnlyOneRecoveryInFlight()
            throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        String artifactId = repository.put(artifact("fair-maintenance-definition"));
        List<String> expected = Arrays.asList("fair-a", "fair-b", "fair-c");
        for (String runId : expected) {
            accepted(store, RunCommand.createRun(
                    "create-" + runId, runId, "fair-maintenance-definition",
                    artifactId, 1, 0L, 1L));
        }
        AtomicBoolean initialGate = new AtomicBoolean(true);
        RecoverableRunCatalog catalog = (cursor, limit) -> {
            if (initialGate.compareAndSet(true, false)) {
                return new RecoverableRunPage(
                        Collections.emptyList(), "initial-complete", true);
            }
            return store.scan(cursor, limit);
        };
        List<String> visited = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        InheritedRunRecovery.Handler handler = candidate -> {
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            try {
                visited.add(candidate.snapshot().runId());
                RunDecision canceled = store.applyRecovered(candidate.token(),
                        RunCommand.cancelReadyRun(
                                "fair-cancel-" + candidate.snapshot().runId(),
                                candidate.snapshot().runId(),
                                candidate.snapshot().revision(),
                                candidate.snapshot().updatedAtMillis() + 1L,
                                "RECOVERY_TEST", "resolved"));
                assertEquals(RunDecision.Code.ACCEPTED, canceled.code());
                return InheritedRunRecovery.Result.resolved();
            } finally {
                inFlight.decrementAndGet();
            }
        };

        try (LeaderRecoveryInventory inventory = new LeaderRecoveryInventory(
                store, catalog, store, repository, null,
                new LeaderRecoveryInventory.Config(1, 1L, 4L, 3, 1L),
                handler, null, true)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 201L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            inventory.signal();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (visited.size() < expected.size() && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(new HashSet<>(expected), new HashSet<>(visited));
            assertEquals(expected.size(), visited.size(),
                    "bounded cursor must not revisit or starve a Run in one catalog cycle");
            for (String runId : expected) {
                while (store.get(runId).status()
                        != io.rowweave.run.RunStatus.CANCELED
                        && System.nanoTime() < deadline) {
                    Thread.sleep(10L);
                }
                assertEquals(io.rowweave.run.RunStatus.CANCELED,
                        store.get(runId).status());
            }
            assertEquals(1, maxInFlight.get());
        }
    }

    @Test
    void configBoundsAndEmptyCatalogBecomeReadyWithoutRetainedRows() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> new LeaderRecoveryInventory.Config(0, 1L, 2L, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new LeaderRecoveryInventory.Config(1001, 1L, 2L, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new LeaderRecoveryInventory.Config(1, 0L, 2L, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new LeaderRecoveryInventory.Config(1, 3L, 2L, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new LeaderRecoveryInventory.Config(1, 1L, 2L, 0));

        InMemoryRunStateStore store = new InMemoryRunStateStore();
        try (LeaderRecoveryInventory inventory = inventory(store, store, store,
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L), 2)) {
            AtomicBoolean active = new AtomicBoolean(true);
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 11L), active::get);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            LeaderRecoveryInventory.Snapshot snapshot = inventory.snapshot();
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY, snapshot.state());
            assertEquals(0, snapshot.activeRuns());
            assertTrue(snapshot.samples().isEmpty());
            assertTrue(inventory.ready(generation));
        }
    }

    @Test
    void inheritedRunUsesExactReadClaimRereadArtifactOrderAndBlocksAdmission() throws Exception {
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        InMemoryRunStateStore raw = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        byte[] artifact = artifact("definition");
        String artifactId = repository.put(artifact);
        assertEquals(RunDecision.Code.ACCEPTED, raw.apply(RunCommand.createRun(
                "create", "run", "definition", artifactId, 2, 0L, 1L)).code());

        RunStateStore state = recordingState(raw, order);
        RecoverableRunCatalog catalog = (cursor, limit) -> {
            order.add("scan");
            return raw.scan(cursor, limit);
        };
        RunRecoveryOwnership ownership = recordingOwnership(raw, order);
        ArtifactRepository artifacts = recordingRepository(repository, order);
        try (LeaderRecoveryInventory inventory = inventory(
                state, catalog, ownership, artifacts, 2)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 11L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            LeaderRecoveryInventory.Snapshot snapshot = inventory.snapshot();
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_PENDING_RECOVERY,
                    snapshot.state());
            assertEquals(1, snapshot.activeRuns());
            assertEquals(Collections.singletonList("run:READY_NO_ATTEMPT"),
                    snapshot.samples());
            assertFalse(inventory.ready(generation));
            assertSubsequence(order, "scan", "get", "ownership", "claim",
                    "get", "current", "artifact");
        }
    }

    @Test
    void missingArtifactIsBoundedCorruptionAndNeverOpensAdmission() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        assertEquals(RunDecision.Code.ACCEPTED, store.apply(RunCommand.createRun(
                "create-missing", "missing", "definition",
                String.join("", Collections.nCopies(64, "a")), 1, 0L, 1L)).code());
        try (LeaderRecoveryInventory inventory = inventory(store, store, store,
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L), 1)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 12L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            LeaderRecoveryInventory.Snapshot snapshot = inventory.snapshot();
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_CORRUPT, snapshot.state());
            assertEquals(1, snapshot.activeRuns());
            assertEquals(1, snapshot.samples().size());
            assertTrue(snapshot.samples().get(0).startsWith(
                    "missing:MISSING_OR_INVALID_ARTIFACT"));
            assertFalse(inventory.ready(generation));
        }
    }

    @Test
    void corruptOrIdentityMismatchedArtifactNeverOpensAdmission() throws Exception {
        byte[] expected = artifact("definition");
        String expectedId = PlanArtifactCodec.decode(expected).artifactId();

        assertArtifactCorruption(expectedId, readOnlyRepository(artifactId -> {
            throw new IllegalStateException("corrupt artifact bytes");
        }), "corrupt-artifact", 121L);
        assertArtifactCorruption(expectedId,
                readOnlyRepository(artifactId -> artifact("other-definition")),
                "mismatched-artifact", 122L);
    }

    @Test
    void diagnosticsAreSampleBoundedAcrossMultipleCatalogPages() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        byte[] artifact = artifact("definition");
        String artifactId = repository.put(artifact);
        for (int i = 0; i < 3; i++) {
            assertEquals(RunDecision.Code.ACCEPTED, store.apply(RunCommand.createRun(
                    "create-" + i, "run-" + i, "definition", artifactId,
                    1, 0L, 1L)).code());
        }
        try (LeaderRecoveryInventory inventory = new LeaderRecoveryInventory(
                store, store, store, repository,
                new LeaderRecoveryInventory.Config(1, 1L, 4L, 2))) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 13L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(3, inventory.snapshot().activeRuns());
            assertEquals(2, inventory.snapshot().samples().size());
        }
    }

    @Test
    void leadershipLossFencesLateScanCompletionBeforeAnyClaim() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecoverableRunCatalog blocking = (cursor, limit) -> {
            entered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    release.await();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
            return store.scan(cursor, limit);
        };
        try (LeaderRecoveryInventory inventory = inventory(store, blocking, store,
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L), 2)) {
            long oldGeneration = inventory.begin(
                    new RecoveryOwner("master-a", 14L), () -> true);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            inventory.loseLeadership();
            release.countDown();
            assertFalse(inventory.awaitSettled(oldGeneration, 1, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.LEADERSHIP_LOST,
                    inventory.snapshot().state());
            assertEquals(0, inventory.snapshot().activeRuns());
        }
    }

    @Test
    void transientBackendFailureRetriesSeriallyAndEventuallyBecomesReady() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        RecoverableRunCatalog flaky = (cursor, limit) -> {
            int concurrent = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(concurrent, Math::max);
            try {
                if (calls.getAndIncrement() == 0) {
                    throw new io.rowweave.run.RunStateStoreUnavailableException(
                            null, "temporary", null);
                }
                return store.scan(cursor, limit);
            } finally {
                inFlight.decrementAndGet();
            }
        };
        try (LeaderRecoveryInventory inventory = inventory(store, flaky, store,
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L), 2)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 15L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    inventory.snapshot().state());
            assertTrue(calls.get() >= 2);
            assertEquals(1, maxInFlight.get());
        }
    }

    @RepeatedTest(50)
    void closeInterruptsAndJoinsInventoryWorker() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        RecoverableRunCatalog blocking = (cursor, limit) -> {
            workerThread.set(Thread.currentThread());
            entered.countDown();
            try {
                new CountDownLatch(1).await();
                throw new AssertionError("unreachable");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new io.rowweave.run.RunStateStoreUnavailableException(
                        null, "interrupted", interrupted);
            }
        };
        LeaderRecoveryInventory inventory = inventory(store, blocking, store,
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L), 2);
        long generation = inventory.begin(
                new RecoveryOwner("master-a", 151L), () -> true);
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        inventory.close();

        assertFalse(workerThread.get().isAlive());
        assertEquals(LeaderRecoveryInventory.State.CLOSED, inventory.snapshot().state());
        assertFalse(inventory.ready(generation));
        assertThrows(IllegalStateException.class, () -> inventory.begin(
                new RecoveryOwner("master-b", 152L), () -> true));
    }

    @Test
    void terminalRaceIsIgnoredInsteadOfInventingRecoveryWork() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        String artifactId = repository.put(artifact("definition"));
        accepted(store, RunCommand.createRun(
                "create-terminal", "terminal", "definition", artifactId,
                1, 0L, 1L));
        AtomicBoolean first = new AtomicBoolean(true);
        RecoverableRunCatalog terminalAfterScan = (cursor, limit) -> {
            RecoverableRunPage page = store.scan(cursor, limit);
            if (!page.runs().isEmpty() && first.compareAndSet(true, false)) {
                accepted(store, RunCommand.cancelReadyRun(
                        "cancel-terminal", "terminal", 1L, 2L, "USER", "stop"));
            }
            return page;
        };
        try (LeaderRecoveryInventory inventory = inventory(
                store, terminalAfterScan, store, repository, 2)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 16L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    inventory.snapshot().state());
            assertEquals(0, inventory.snapshot().activeRuns());
            assertEquals(null, store.ownership("terminal"));
        }
    }

    @Test
    void lostClaimRaceRetriesFromANewCycleBeforeClassifying() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        String artifactId = repository.put(artifact("definition"));
        accepted(store, RunCommand.createRun(
                "create-lost", "lost", "definition", artifactId,
                1, 0L, 1L));
        AtomicInteger claims = new AtomicInteger();
        RunRecoveryOwnership flakyOwnership = new RunRecoveryOwnership() {
            @Override public RecoveryToken ownership(String runId) {
                return store.ownership(runId);
            }
            @Override public RecoveryClaim claim(String runId, long expectedRevision,
                                                 long expectedOwnershipEpoch,
                                                 RecoveryOwner owner) {
                if (claims.getAndIncrement() == 0) return RecoveryClaim.lostRace();
                return store.claim(runId, expectedRevision, expectedOwnershipEpoch, owner);
            }
            @Override public boolean current(RecoveryToken token) {
                return store.current(token);
            }
        };
        try (LeaderRecoveryInventory inventory = inventory(
                store, store, flakyOwnership, repository, 2)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 17L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_PENDING_RECOVERY,
                    inventory.snapshot().state());
            assertTrue(claims.get() >= 2);
        }
    }

    @Test
    void nonTerminalStaleCatalogRefRestartsCycleAgainstCurrentRevision() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        String artifactId = repository.put(artifact("definition"));
        accepted(store, RunCommand.createRun(
                "create-stale-ref", "stale-ref", "definition", artifactId,
                2, 0L, 1L));
        AtomicBoolean mutate = new AtomicBoolean(true);
        RecoverableRunCatalog staleOnce = (cursor, limit) -> {
            RecoverableRunPage page = store.scan(cursor, limit);
            if (!page.runs().isEmpty() && mutate.compareAndSet(true, false)) {
                accepted(store, RunCommand.createAttempt(
                        "attempt-stale-ref", "stale-ref", 1L,
                        "attempt-stale-ref", "publication-stale-ref", 2L));
            }
            return page;
        };
        try (LeaderRecoveryInventory inventory = inventory(
                store, staleOnce, store, repository, 2)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 19L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_PENDING_RECOVERY,
                    inventory.snapshot().state());
            assertEquals(Collections.singletonList("stale-ref:ATTEMPT_CREATED"),
                    inventory.snapshot().samples());
        }
    }

    @Test
    void everyNonTerminalAttemptPhaseIsClassifiedWithoutExecution() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(16, 16L * 1024L * 1024L);
        String artifactId = repository.put(artifact("definition"));
        createAt(store, artifactId, "created", null);
        createAt(store, artifactId, "dispatching", AttemptStatus.DISPATCHING);
        createAt(store, artifactId, "running", AttemptStatus.RUNNING);
        createAt(store, artifactId, "committing", AttemptStatus.COMMITTING);
        createAt(store, artifactId, "reconciling", AttemptStatus.RECONCILING);
        RunSnapshot canceling = createAt(
                store, artifactId, "canceling", AttemptStatus.RUNNING);
        accepted(store, RunCommand.requestCancellation(
                "cancel-canceling", "canceling", canceling.revision(),
                "attempt-canceling", canceling.currentAttempt().fenceToken(),
                CancellationCause.USER, 20L, "USER", "stop"));

        try (LeaderRecoveryInventory inventory = new LeaderRecoveryInventory(
                store, store, store, repository,
                new LeaderRecoveryInventory.Config(2, 1L, 4L, 16))) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 18L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(6, inventory.snapshot().activeRuns());
            HashSet<String> samples = new HashSet<>(inventory.snapshot().samples());
            assertTrue(samples.contains("created:ATTEMPT_CREATED"));
            assertTrue(samples.contains("dispatching:ATTEMPT_DISPATCHING"));
            assertTrue(samples.contains("running:ATTEMPT_RUNNING"));
            assertTrue(samples.contains("committing:ATTEMPT_COMMITTING"));
            assertTrue(samples.contains("reconciling:ATTEMPT_RECONCILING"));
            assertTrue(samples.contains("canceling:ATTEMPT_CANCELING"));
        }
    }

    @Test
    void validatedReadyCandidateCanResolveOnlyThroughTerminalAuthority() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        String artifactId = repository.put(artifact("definition"));
        accepted(store, RunCommand.createRun(
                "create-handler", "handler", "definition", artifactId,
                1, 0L, 7L));
        AtomicInteger calls = new AtomicInteger();
        InheritedRunRecovery.Handler handler = candidate -> {
            calls.incrementAndGet();
            assertEquals("handler", candidate.snapshot().runId());
            assertEquals(artifactId, candidate.artifact().artifactId());
            assertTrue(store.current(candidate.token()));
            RunDecision terminal = store.applyRecovered(candidate.token(),
                    RunCommand.cancelReadyRun(
                            "recover-cancel", "handler",
                            candidate.snapshot().revision(),
                            candidate.snapshot().updatedAtMillis(),
                            "RECOVERY_TEST", "resolved"));
            assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
            return InheritedRunRecovery.Result.resolved();
        };

        try (LeaderRecoveryInventory inventory = inventory(
                store, store, store, repository, 2, handler)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 181L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    inventory.snapshot().state());
            assertEquals(1, calls.get());
        }
    }

    @Test
    void deploymentMismatchStaysCapabilityBlockedBeforeRecoveryMaterialization() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        PlanArtifact foreign = PlanArtifactDeploymentBinding.bind(
                PlanArtifactCodec.decode(artifact("definition")), repeat('b', 64));
        String artifactId = repository.put(PlanArtifactCodec.encode(foreign));
        accepted(store, RunCommand.createRun(
                "create-deployment-mismatch", "deployment-mismatch", "definition",
                artifactId, 1, 0L, 1L));
        AtomicInteger materializations = new AtomicInteger();

        try (LeaderRecoveryInventory inventory = new LeaderRecoveryInventory(
                store, store, store, repository, null,
                new LeaderRecoveryInventory.Config(8, 1L, 4L, 2), candidate -> {
                    materializations.incrementAndGet();
                    return InheritedRunRecovery.Result.resolved();
                }, repeat('a', 64), true)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 186L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_CAPABILITY,
                    inventory.snapshot().state());
            assertEquals(Collections.singletonList(
                    "deployment-mismatch:DEPLOYMENT_SET_MISMATCH"),
                    inventory.snapshot().samples());
            assertEquals(0, materializations.get());
            assertFalse(inventory.ready(generation));
        }
    }

    @Test
    void handlerCannotDeclareResolvedWithoutTerminalAuthority() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        String artifactId = repository.put(artifact("definition"));
        accepted(store, RunCommand.createRun(
                "create-false-resolved", "false-resolved", "definition",
                artifactId, 1, 0L, 1L));

        try (LeaderRecoveryInventory inventory = inventory(
                store, store, store, repository, 2,
                candidate -> InheritedRunRecovery.Result.resolved())) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 182L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_PENDING_RECOVERY,
                    inventory.snapshot().state());
            assertEquals(1, inventory.snapshot().activeRuns());
        }
    }

    @Test
    void postHandlerOwnerReplacementRestartsSerialCycle() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        String artifactId = repository.put(artifact("definition"));
        accepted(store, RunCommand.createRun(
                "create-owner-race", "owner-race", "definition",
                artifactId, 1, 0L, 3L));
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        InheritedRunRecovery.Handler handler = candidate -> {
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            try {
                if (calls.getAndIncrement() == 0) {
                    RecoveryClaim replacement = store.claim(
                            candidate.snapshot().runId(), candidate.snapshot().revision(),
                            candidate.token().ownershipEpoch(),
                            new RecoveryOwner("master-b", 999L));
                    assertEquals(RecoveryClaim.Code.ACQUIRED, replacement.code());
                    return InheritedRunRecovery.Result.pending("OWNER_REPLACED");
                }
                RunDecision terminal = store.applyRecovered(candidate.token(),
                        RunCommand.cancelReadyRun(
                                "recover-owner-race", "owner-race",
                                candidate.snapshot().revision(),
                                candidate.snapshot().updatedAtMillis(),
                                "RECOVERY_TEST", "resolved"));
                assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
                return InheritedRunRecovery.Result.resolved();
            } finally {
                inFlight.decrementAndGet();
            }
        };

        try (LeaderRecoveryInventory inventory = inventory(
                store, store, store, repository, 2, handler)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 183L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    inventory.snapshot().state());
            assertTrue(calls.get() >= 2);
            assertEquals(1, maxInFlight.get());
        }
    }

    @Test
    void unsafeAttemptPhaseNeverInvokesRecoveryHandler() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        String artifactId = repository.put(artifact("definition"));
        createAt(store, artifactId, "unsafe", AttemptStatus.DISPATCHING);
        AtomicInteger calls = new AtomicInteger();

        try (LeaderRecoveryInventory inventory = inventory(
                store, store, store, repository, 2, candidate -> {
                    calls.incrementAndGet();
                    return InheritedRunRecovery.Result.resolved();
                })) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 184L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_PENDING_RECOVERY,
                    inventory.snapshot().state());
            assertEquals(0, calls.get());
        }
    }

    @Test
    void claimedManifestClassifiesFencedNoneVersusAcceptedEvidence() throws Exception {
        assertAssignmentEvidence("none-evidence", false, "ASSIGNMENTS_NONE_FENCED");
        assertAssignmentEvidence("accepted-evidence", true, "ASSIGNMENTS_ACCEPTED_1");
    }

    private static void assertAssignmentEvidence(
            String runId, boolean accepted, String expected) throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        InMemoryArtifactRepository repository =
                new InMemoryArtifactRepository(8, 8L * 1024L * 1024L);
        String artifactId = repository.put(artifact("definition"));
        RunSnapshot run = createAt(store, artifactId, runId, AttemptStatus.DISPATCHING);
        io.rowweave.run.RunAttemptFence fence = new io.rowweave.run.RunAttemptFence(
                runId, run.currentAttempt().attemptId(), run.currentAttempt().fenceToken());
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, artifactId, 1L, Collections.singletonList(
                StageAssignmentRecord.create(
                        fence, 1L, 0, 101L, 0, 0,
                        StageAssignmentRole.WHOLE_GRAPH, "worker", "127.0.0.1",
                        7001, -1, "incarnation")));
        StageAssignmentSnapshot assignments = store.publish(manifest).snapshot();
        if (accepted) {
            byte[] digest = new byte[32];
            Arrays.fill(digest, (byte) 7);
            assertEquals(io.rowweave.run.StageAcceptanceDecision.Code.ACCEPTED,
                    store.accept(new StageAssignmentAcceptance(
                            assignments.token(0), digest, 10L)).code());
        }

        try (LeaderRecoveryInventory inventory = new LeaderRecoveryInventory(
                store, store, store, repository, store,
                new LeaderRecoveryInventory.Config(8, 1L, 4L, 2), null)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", 185L), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_PENDING_RECOVERY,
                    inventory.snapshot().state());
            assertEquals(Collections.singletonList(runId + ':' + expected),
                    inventory.snapshot().samples());
        }
    }

    private static RunSnapshot createAt(InMemoryRunStateStore store, String artifactId,
                                        String runId, AttemptStatus target) {
        RunSnapshot snapshot = accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", artifactId,
                4, 0L, 1L));
        snapshot = accepted(store, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt-" + runId, "publication-" + runId, 2L));
        AttemptStatus[] path = { AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                AttemptStatus.COMMITTING, AttemptStatus.RECONCILING };
        for (AttemptStatus step : path) {
            if (target == null) break;
            snapshot = accepted(store, RunCommand.advanceAttempt(
                    "advance-" + runId + '-' + step.name(), runId,
                    snapshot.revision(), "attempt-" + runId,
                    snapshot.currentAttempt().fenceToken(), step, false,
                    snapshot.revision() + 2L, null, null));
            if (step == target) break;
        }
        return snapshot;
    }

    private static RunSnapshot accepted(InMemoryRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static void assertArtifactCorruption(
            String expectedArtifactId, ArtifactRepository repository,
            String runId, long ownerTerm) throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", expectedArtifactId,
                1, 0L, 1L));
        try (LeaderRecoveryInventory inventory = inventory(
                store, store, store, repository, 1)) {
            long generation = inventory.begin(
                    new RecoveryOwner("master-a", ownerTerm), () -> true);
            assertTrue(inventory.awaitSettled(generation, 5, TimeUnit.SECONDS));
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_CORRUPT,
                    inventory.snapshot().state());
            assertEquals(Collections.singletonList(
                    runId + ":MISSING_OR_INVALID_ARTIFACT"),
                    inventory.snapshot().samples());
            assertFalse(inventory.ready(generation));
        }
    }

    private static LeaderRecoveryInventory inventory(
            RunStateStore state, RecoverableRunCatalog catalog,
            RunRecoveryOwnership ownership, ArtifactRepository repository,
            int sampleLimit) {
        return new LeaderRecoveryInventory(state, catalog, ownership, repository,
                new LeaderRecoveryInventory.Config(8, 1L, 4L, sampleLimit));
    }

    private static LeaderRecoveryInventory inventory(
            RunStateStore state, RecoverableRunCatalog catalog,
            RunRecoveryOwnership ownership, ArtifactRepository repository,
            int sampleLimit, InheritedRunRecovery.Handler handler) {
        return new LeaderRecoveryInventory(state, catalog, ownership, repository,
                new LeaderRecoveryInventory.Config(8, 1L, 4L, sampleLimit), handler);
    }

    private static byte[] artifact(String definitionId) {
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList())
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return PlanArtifactCodec.encode(LegacyPlanArtifactAdapter.wrap(definitionId, graph));
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static RunStateStore recordingState(
            InMemoryRunStateStore delegate, List<String> order) {
        return new RunStateStore() {
            @Override public RunDecision apply(RunCommand command) {
                return delegate.apply(command);
            }
            @Override public RunSnapshot get(String runId) {
                order.add("get");
                return delegate.get(runId);
            }
            @Override public List<ExecutionEvent> events(
                    String runId, long afterSequence, int limit) {
                return delegate.events(runId, afterSequence, limit);
            }
        };
    }

    private static RunRecoveryOwnership recordingOwnership(
            InMemoryRunStateStore delegate, List<String> order) {
        return new RunRecoveryOwnership() {
            @Override public RecoveryToken ownership(String runId) {
                order.add("ownership");
                return delegate.ownership(runId);
            }
            @Override public RecoveryClaim claim(
                    String runId, long expectedRevision,
                    long expectedOwnershipEpoch, RecoveryOwner owner) {
                order.add("claim");
                return delegate.claim(runId, expectedRevision,
                        expectedOwnershipEpoch, owner);
            }
            @Override public boolean current(RecoveryToken token) {
                order.add("current");
                return delegate.current(token);
            }
        };
    }

    private static ArtifactRepository recordingRepository(
            ArtifactRepository delegate, List<String> order) {
        return new ArtifactRepository() {
            @Override public String put(byte[] encodedArtifact) {
                return delegate.put(encodedArtifact);
            }
            @Override public byte[] get(String artifactId) {
                order.add("artifact");
                return delegate.get(artifactId);
            }
            @Override public byte[] get(String artifactId,
                                        java.util.function.LongConsumer reservation) {
                order.add("artifact");
                return delegate.get(artifactId, reservation);
            }
        };
    }

    private static ArtifactRepository readOnlyRepository(
            Function<String, byte[]> reader) {
        return new ArtifactRepository() {
            @Override public String put(byte[] encodedArtifact) {
                throw new UnsupportedOperationException("read only");
            }
            @Override public byte[] get(String artifactId) {
                return reader.apply(artifactId);
            }
            @Override public byte[] get(String artifactId,
                                        java.util.function.LongConsumer reservation) {
                byte[] encoded = reader.apply(artifactId);
                if (encoded != null) reservation.accept(encoded.length);
                return encoded;
            }
        };
    }

    private static void assertSubsequence(List<String> actual, String... expected) {
        int position = 0;
        for (String value : actual) {
            if (position < expected.length && expected[position].equals(value)) position++;
        }
        assertEquals(expected.length, position, "order=" + actual);
    }
}
