package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryStageAssignmentAuthorityTest {

    @Test
    void publishHasOneWinnerExactReplayAndConflict() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-publish", 2);
        StageAssignmentManifest manifest = manifest(run, 101L, "worker-a", "inc-a");

        StageManifestPublication published = store.publish(manifest);
        assertEquals(StageManifestPublication.Code.PUBLISHED, published.code());
        assertEquals(1L, published.snapshot().acceptanceGeneration());
        assertTrue(published.snapshot().allNone());
        assertEquals(StageManifestPublication.Code.ALREADY_PUBLISHED,
                store.publish(manifest).code());

        StageAssignmentManifest conflict = manifest(run, 102L, "worker-a", "inc-a");
        assertEquals(StageManifestPublication.Code.CONFLICT, store.publish(conflict).code());
        StageAssignmentSnapshot loaded = store.assignmentSnapshot(manifest.fence());
        assertArrayEquals(StageAssignmentManifestCodec.encodeRoot(manifest),
                StageAssignmentManifestCodec.encodeRoot(loaded.manifest()));
    }

    @Test
    void acceptanceCommitsBeforeAckReplaysExactlyAndRejectsConflicts() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-accept", 2);
        StageAssignmentManifest manifest = manifest(run, 101L, "worker-a", "inc-a");
        StageAssignmentSnapshot snapshot = store.publish(manifest).snapshot();
        StageAssignmentToken token = snapshot.token(0);
        byte[] digest = digest(7);
        StageAssignmentAcceptance request = new StageAssignmentAcceptance(token, digest, 10L);

        StageAcceptanceDecision accepted = store.accept(request);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED, accepted.code());
        assertEquals(request, accepted.acceptance());
        StageAcceptanceDecision replay = store.accept(
                new StageAssignmentAcceptance(token, digest, 999L));
        assertEquals(StageAcceptanceDecision.Code.ALREADY_ACCEPTED, replay.code());
        assertEquals(10L, replay.acceptance().acceptedAtMillis());
        assertEquals(StageAcceptanceDecision.Code.ASSIGNMENT_CONFLICT,
                store.accept(new StageAssignmentAcceptance(token, digest(8), 11L)).code());

        StageAssignmentSnapshot loaded = store.assignmentSnapshot(manifest.fence());
        assertEquals(1, loaded.acceptedCount());
        assertFalse(loaded.allNone());
        assertEquals(request, loaded.acceptance(0));
    }

    @Test
    void recoveryClaimAndFirstAcceptanceHaveOneLinearizationPoint() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-race", 2);
        StageAssignmentManifest manifest = manifest(run, 101L, "worker-a", "inc-a");
        StageAssignmentAcceptance request = new StageAssignmentAcceptance(
                store.publish(manifest).snapshot().token(0), digest(7), 10L);
        RecoveryOwner owner = new RecoveryOwner("master-new", 99L);

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicReference<RecoveryClaim> claim = new AtomicReference<>();
        AtomicReference<StageAcceptanceDecision> acceptance = new AtomicReference<>();
        pool.submit(() -> {
            start.await();
            claim.set(store.claim(run.runId(), run.revision(), 0L, owner));
            return null;
        });
        pool.submit(() -> {
            start.await();
            acceptance.set(store.accept(request));
            return null;
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(RecoveryClaim.Code.ACQUIRED, claim.get().code());
        StageAssignmentSnapshot after = store.assignmentSnapshot(manifest.fence());
        assertEquals(2L, after.acceptanceGeneration());
        if (acceptance.get().code() == StageAcceptanceDecision.Code.ACCEPTED) {
            assertEquals(1, after.acceptedCount());
            assertEquals(StageAcceptanceDecision.Code.ALREADY_ACCEPTED,
                    store.accept(request).code());
        } else {
            assertEquals(StageAcceptanceDecision.Code.STALE_ASSIGNMENT_GENERATION,
                    acceptance.get().code());
            assertTrue(after.allNone());
        }

        StageAssignmentAcceptance currentGeneration = new StageAssignmentAcceptance(
                after.token(0), digest(9), 12L);
        assertEquals(after.acceptedCount() == 0
                        ? StageAcceptanceDecision.Code.RECOVERY_OWNED
                        : StageAcceptanceDecision.Code.ASSIGNMENT_CONFLICT,
                store.accept(currentGeneration).code());
    }

    @Test
    void claimPreservesCommittedReceiptFencesNoneAndOnlyAdvancesForNewOwner() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-claim", 2);
        RunAttemptFence fence = new RunAttemptFence(run.runId(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken());
        StageAssignmentRecord first = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, StageAssignmentRole.ROUTER,
                "worker-a", "localhost", 7001, -1, "inc-a");
        StageAssignmentRecord second = StageAssignmentRecord.create(
                fence, 1L, 1, 102L, 1, 0, StageAssignmentRole.SINK,
                "worker-b", "localhost", 7002, -1, "inc-b");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L, Arrays.asList(first, second));
        StageAssignmentSnapshot before = store.publish(manifest).snapshot();
        StageAssignmentAcceptance committed = new StageAssignmentAcceptance(
                before.token(0), digest(3), 10L);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED, store.accept(committed).code());

        RecoveryOwner firstOwner = new RecoveryOwner("master-first", 100L);
        RecoveryClaim firstClaim = store.claim(
                run.runId(), run.revision(), 0L, firstOwner);
        assertEquals(RecoveryClaim.Code.ACQUIRED, firstClaim.code());
        StageAssignmentSnapshot claimed = store.assignmentSnapshot(fence);
        assertEquals(2L, claimed.acceptanceGeneration());
        assertEquals(StageAcceptanceDecision.Code.ALREADY_ACCEPTED,
                store.accept(committed).code());
        assertEquals(StageAcceptanceDecision.Code.STALE_ASSIGNMENT_GENERATION,
                store.accept(new StageAssignmentAcceptance(before.token(1), digest(4), 11L)).code());
        assertEquals(StageAcceptanceDecision.Code.RECOVERY_OWNED,
                store.accept(new StageAssignmentAcceptance(claimed.token(1), digest(4), 11L)).code());

        RecoveryClaim replay = store.claim(
                run.runId(), run.revision(), 0L, firstOwner);
        assertEquals(RecoveryClaim.Code.REPLAYED, replay.code());
        assertEquals(2L, store.assignmentSnapshot(fence).acceptanceGeneration());
        RecoveryClaim replacement = store.claim(run.runId(), run.revision(),
                firstClaim.token().ownershipEpoch(), new RecoveryOwner("master-second", 200L));
        assertEquals(RecoveryClaim.Code.ACQUIRED, replacement.code());
        assertEquals(3L, store.assignmentSnapshot(fence).acceptanceGeneration());
    }

    @Test
    void terminalMutationRemovesManifestWhileGenericRetryIsRejected() {
        InMemoryRunStateStore terminalStore = new InMemoryRunStateStore();
        RunSnapshot terminal = attempt(terminalStore, "run-terminal", 1);
        StageAssignmentManifest terminalManifest = manifest(
                terminal, 101L, "worker-a", "inc-a");
        StageAssignmentSnapshot terminalAssignments = terminalStore.publish(terminalManifest).snapshot();
        terminalStore.accept(new StageAssignmentAcceptance(
                terminalAssignments.token(0), digest(1), 10L));
        RunDecision failed = terminalStore.apply(RunCommand.advanceAttempt(
                "fail-terminal", terminal.runId(), terminal.revision(),
                terminal.currentAttempt().attemptId(), terminal.currentAttempt().fenceToken(),
                AttemptStatus.FAILED, false, 3L, "FAILED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, failed.code());
        assertTrue(failed.snapshot().status().isTerminal());
        assertNull(terminalStore.assignmentSnapshot(terminalManifest.fence()));

        InMemoryRunStateStore retryStore = new InMemoryRunStateStore();
        RunSnapshot retry = attempt(retryStore, "run-retry", 2);
        StageAssignmentManifest retryManifest = manifest(retry, 201L, "worker-b", "inc-b");
        retryStore.publish(retryManifest);
        assertThrows(IllegalArgumentException.class,
                () -> RunCommand.advanceAttempt(
                        "fail-retry", retry.runId(), retry.revision(),
                        retry.currentAttempt().attemptId(),
                        retry.currentAttempt().fenceToken(),
                        AttemptStatus.FAILED, true, 3L,
                        "RETRY_WORKER_LOST", "retryable"));
        assertEquals(RunStatus.RUNNING, retryStore.get(retry.runId()).status());
        assertNotNull(retryStore.assignmentSnapshot(retryManifest.fence()));
    }

    @Test
    void publishAndAcceptFailClosedForStaleRunOrRecoveryOwner() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-fence", 2);
        StageAssignmentManifest manifest = manifest(run, 101L, "worker-a", "inc-a");
        assertEquals(RecoveryClaim.Code.ACQUIRED,
                store.claim(run.runId(), run.revision(), 0L,
                        new RecoveryOwner("master-new", 77L)).code());
        assertEquals(StageManifestPublication.Code.RECOVERY_OWNED,
                store.publish(manifest).code());

        RunAttemptFence wrong = new RunAttemptFence(run.runId(), "wrong-attempt", 1L);
        StageAssignmentRecord wrongRecord = StageAssignmentRecord.create(
                wrong, 1L, 0, 1L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "worker-a", "localhost", 7001, -1, "inc-a");
        assertEquals(StageManifestPublication.Code.STALE_RUN,
                store.publish(new StageAssignmentManifest(
                        wrong, run.artifactId(), 1L, Arrays.asList(wrongRecord))).code());
    }

    @Test
    void recoveredPublicationRequiresTheExactRecoveryToken() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-recovered-publish", 2);
        StageAssignmentManifest manifest = manifest(run, 301L, "worker-a", "inc-a");
        RecoveryClaim first = store.claim(run.runId(), run.revision(), 0L,
                new RecoveryOwner("master-first", 10L));
        assertEquals(RecoveryClaim.Code.ACQUIRED, first.code());
        Object authority = store;
        assertTrue(authority instanceof RecoveryFencedStageAssignmentAuthority);
        RecoveryFencedStageAssignmentAuthority recovered =
                (RecoveryFencedStageAssignmentAuthority) authority;

        assertEquals(StageManifestPublication.Code.RECOVERY_OWNED,
                store.publish(manifest).code());
        assertEquals(StageManifestPublication.Code.PUBLISHED,
                recovered.publishRecovered(first.token(), manifest).code());

        RecoveryClaim replacement = store.claim(run.runId(), run.revision(),
                first.token().ownershipEpoch(), new RecoveryOwner("master-second", 20L));
        assertEquals(RecoveryClaim.Code.ACQUIRED, replacement.code());
        assertEquals(StageManifestPublication.Code.RECOVERY_OWNED,
                recovered.publishRecovered(first.token(), manifest).code());
        assertEquals(StageManifestPublication.Code.ALREADY_PUBLISHED,
                recovered.publishRecovered(replacement.token(), manifest).code());
    }

    @Test
    void recoveredAcceptanceRequiresCurrentExactOwnerAndReplaysCommittedReceipt() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-recovered-accept", 2);
        StageAssignmentManifest manifest = manifest(run, 401L, "worker-a", "inc-a");
        RecoveryClaim first = store.claim(run.runId(), run.revision(), 0L,
                new RecoveryOwner("master-first", 10L));
        RecoveryFencedStageAssignmentAuthority recovered = store;
        assertEquals(StageManifestPublication.Code.PUBLISHED,
                recovered.publishRecovered(first.token(), manifest).code());

        RecoveryClaim replacement = store.claim(run.runId(), run.revision(),
                first.token().ownershipEpoch(), new RecoveryOwner("master-second", 20L));
        StageAssignmentAcceptance request = new StageAssignmentAcceptance(
                store.assignmentSnapshot(manifest.fence()).token(0), digest(6), 12L);
        assertEquals(StageAcceptanceDecision.Code.RECOVERY_OWNED,
                store.accept(request).code());
        assertEquals(StageAcceptanceDecision.Code.RECOVERY_OWNED,
                recovered.acceptRecovered(first.token(), request).code());
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                recovered.acceptRecovered(replacement.token(), request).code());
        assertEquals(StageAcceptanceDecision.Code.ALREADY_ACCEPTED,
                recovered.acceptRecovered(first.token(), request).code());
    }

    private static RunSnapshot attempt(InMemoryRunStateStore store, String runId, int maxAttempts) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", maxAttempts, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        RunDecision attempted = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.snapshot().revision(),
                "attempt-1", "publication-1", 2L));
        assertEquals(RunDecision.Code.ACCEPTED, attempted.code());
        return attempted.snapshot();
    }

    private static StageAssignmentManifest manifest(RunSnapshot run, long runtimeStageId,
                                                    String workerId, String incarnationId) {
        RunAttemptFence fence = new RunAttemptFence(run.runId(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, runtimeStageId, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                workerId, "localhost", 7001, -1, incarnationId);
        return new StageAssignmentManifest(
                fence, run.artifactId(), 1L, Arrays.asList(record));
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) value);
        return digest;
    }
}
