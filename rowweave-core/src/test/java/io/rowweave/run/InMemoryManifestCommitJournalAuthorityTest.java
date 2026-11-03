package io.rowweave.run;

import io.rowweave.commit.CommitManifest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryManifestCommitJournalAuthorityTest {

    @Test
    void sealedMembersReplayExactlyAndIntentFreezesCompleteClosure() {
        Fixture fixture = fixture("run-manifest-authority", 2, true);
        ManifestMemberReceipt first = fixture.member(0, "first");
        ManifestMemberReceipt second = fixture.member(1, "second");
        assertEquals(ManifestMemberDecision.Code.RECORDED,
                fixture.store.recordManifestMember(first).code());
        assertEquals(ManifestMemberDecision.Code.ALREADY_RECORDED,
                fixture.store.recordManifestMember(first).code());
        assertEquals(ManifestMemberDecision.Code.MEMBER_CONFLICT,
                fixture.store.recordManifestMember(fixture.member(0, "changed")).code());

        fixture.advanceToCommitting();
        ManifestCommitJournalSnapshot partial = fixture.store
                .manifestCommitJournal(fixture.manifest.fence());
        assertEquals(1, partial.memberCount());
        assertEquals(ManifestIntentDecision.Code.INCOMPLETE_MEMBERS,
                fixture.store.commitManifestIntent(
                        fixture.placeholderIntent(partial, 2)).code());

        assertEquals(ManifestMemberDecision.Code.RECORDED,
                fixture.store.recordManifestMember(second).code());
        ManifestCommitJournalSnapshot complete = fixture.store
                .manifestCommitJournal(fixture.manifest.fence());
        ManifestCommitIntent intended = complete.buildIntent();
        assertEquals(ManifestIntentDecision.Code.COMMITTED,
                fixture.store.commitManifestIntent(intended).code());
        assertEquals(ManifestIntentDecision.Code.ALREADY_COMMITTED,
                fixture.store.commitManifestIntent(intended).code());
        assertEquals(ManifestMemberDecision.Code.ALREADY_RECORDED,
                fixture.store.recordManifestMember(first).code());
        assertEquals(intended, fixture.store
                .manifestCommitJournal(fixture.manifest.fence()).intent());
    }

    @Test
    void intentRequiresCommittingAndExactRunPublicationIdentity() {
        Fixture fixture = fixture("run-manifest-status", 1, true);
        ManifestMemberReceipt member = fixture.member(0, "only");
        fixture.store.recordManifestMember(member);
        ManifestCommitIntent intent = fixture.store
                .manifestCommitJournal(fixture.manifest.fence()).buildIntent();
        assertEquals(ManifestIntentDecision.Code.ATTEMPT_NOT_COMMITTING,
                fixture.store.commitManifestIntent(intent).code());

        Fixture other = fixture("run-manifest-publication", 1, true);
        ManifestMemberReceipt wrongPublication = ManifestMemberReceipt.sealed(
                other.assignments.acceptance(0).token(),
                other.assignments.acceptance(0).commandDigest(),
                "domain-authority", 17L, "wrong-publication",
                descriptor("wrong", 17L, 1), 30L);
        assertEquals(ManifestMemberDecision.Code.COMMIT_IDENTITY_CONFLICT,
                other.store.recordManifestMember(wrongPublication).code());
    }

    @Test
    void memberRequiresSucceededTerminalOutputEvidence() {
        Fixture failed = fixture("run-manifest-failed", 1, false);
        assertEquals(ManifestMemberDecision.Code.TERMINAL_NOT_SUCCESSFUL,
                failed.store.recordManifestMember(failed.member(0, "failed")).code());
    }

    @Test
    void recoveryOwnerFencesFirstWritesButExactCommittedReplaysSurviveReplacement() {
        Fixture fixture = fixture("run-manifest-recovery", 2, true);
        RecoveryClaim firstOwner = fixture.store.claim(
                fixture.run.runId(), fixture.run.revision(), 0L,
                new RecoveryOwner("master-first", 20L));
        ManifestMemberReceipt first = fixture.member(0, "first");
        ManifestMemberReceipt second = fixture.member(1, "second");
        assertEquals(ManifestMemberDecision.Code.RECOVERY_OWNED,
                fixture.store.recordManifestMember(first).code());
        assertEquals(ManifestMemberDecision.Code.RECORDED,
                fixture.store.recordManifestMemberRecovered(firstOwner.token(), first).code());

        RecoveryClaim replacement = fixture.store.claim(
                fixture.run.runId(), fixture.run.revision(),
                firstOwner.token().ownershipEpoch(),
                new RecoveryOwner("master-second", 21L));
        assertEquals(ManifestMemberDecision.Code.RECOVERY_OWNED,
                fixture.store.recordManifestMemberRecovered(firstOwner.token(), second).code());
        assertEquals(ManifestMemberDecision.Code.RECORDED,
                fixture.store.recordManifestMemberRecovered(replacement.token(), second).code());
        assertEquals(ManifestMemberDecision.Code.ALREADY_RECORDED,
                fixture.store.recordManifestMemberRecovered(firstOwner.token(), first).code());

        fixture.advanceToCommitting(replacement.token());
        ManifestCommitIntent intent = fixture.store
                .manifestCommitJournal(fixture.manifest.fence()).buildIntent();
        assertEquals(ManifestIntentDecision.Code.RECOVERY_OWNED,
                fixture.store.commitManifestIntentRecovered(firstOwner.token(), intent).code());
        assertEquals(ManifestIntentDecision.Code.COMMITTED,
                fixture.store.commitManifestIntentRecovered(replacement.token(), intent).code());
        assertEquals(ManifestIntentDecision.Code.ALREADY_COMMITTED,
                fixture.store.commitManifestIntentRecovered(firstOwner.token(), intent).code());
    }

    @Test
    void terminalRunMutationAtomicallyRemovesManifestJournal() {
        Fixture fixture = fixture("run-manifest-cleanup", 1, true);
        fixture.store.recordManifestMember(fixture.member(0, "only"));
        RunSnapshot current = fixture.store.get(fixture.run.runId());
        RunDecision failed = fixture.store.apply(RunCommand.advanceAttempt(
                "terminal", current.runId(), current.revision(),
                current.currentAttempt().attemptId(), current.currentAttempt().fenceToken(),
                AttemptStatus.FAILED, false, 50L, "FAILED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, failed.code());
        assertNull(fixture.store.manifestCommitJournal(fixture.manifest.fence()));
    }

    @Test
    void abortDecisionIsDurableMutuallyExclusiveWithCommitAndSurvivesTerminalRun() {
        Fixture fixture = fixture("run-manifest-abort", 1, true);
        fixture.store.recordManifestMember(fixture.member(0, "only"));
        fixture.advanceToCommitting();
        BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                fixture.manifest.fence(), fixture.manifest.generation(),
                io.rowweave.BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1,
                "domain-authority", 17L, "publication-authority",
                digest(5), "PREPUBLICATION_FAILURE", 60L);

        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbort(decided).code());
        assertEquals(BatchSinkAbortDecision.Code.ALREADY_RECORDED,
                fixture.store.recordBatchSinkAbort(decided).code());
        assertEquals(ManifestIntentDecision.Code.INTENT_CONFLICT,
                fixture.store.commitManifestIntent(fixture.store
                        .manifestCommitJournal(fixture.manifest.fence())
                        .buildIntent()).code());

        BatchSinkAbortRecord completed = decided.completed(61L);
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbort(completed).code());
        assertEquals(completed,
                fixture.store.batchSinkAbort(fixture.manifest.fence()));

        RunSnapshot current = fixture.store.get(fixture.run.runId());
        RunDecision reconciling = fixture.store.apply(RunCommand.requestCancellation(
                "reconcile-abort", current.runId(), current.revision(),
                current.currentAttempt().attemptId(), current.currentAttempt().fenceToken(),
                CancellationCause.FENCE, 62L, "ABORT", "abort decided"));
        assertEquals(RunDecision.Code.ACCEPTED, reconciling.code());
        current = reconciling.snapshot();
        RunDecision terminal = fixture.store.apply(RunCommand.advanceAttempt(
                "terminal-abort", current.runId(), current.revision(),
                current.currentAttempt().attemptId(), current.currentAttempt().fenceToken(),
                AttemptStatus.FAILED, false, 63L, "FAILED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        assertNull(fixture.store.manifestCommitJournal(fixture.manifest.fence()));
        assertEquals(completed,
                fixture.store.batchSinkAbort(fixture.manifest.fence()));
    }

    @Test
    void committedIntentPermanentlyRejectsAbortDecision() {
        Fixture fixture = fixture("run-manifest-commit-no-abort", 1, true);
        fixture.store.recordManifestMember(fixture.member(0, "only"));
        fixture.advanceToCommitting();
        ManifestCommitIntent intent = fixture.store
                .manifestCommitJournal(fixture.manifest.fence()).buildIntent();
        fixture.store.commitManifestIntent(intent);
        BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                fixture.manifest.fence(), fixture.manifest.generation(),
                io.rowweave.BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1,
                "domain-authority", 17L, "publication-authority",
                digest(5), "TOO_LATE", 60L);
        assertEquals(BatchSinkAbortDecision.Code.COMMIT_ALREADY_DECIDED,
                fixture.store.recordBatchSinkAbort(abort).code());
    }

    @Test
    void timeoutCancelingAttemptMayDurablyDecideAbort() {
        Fixture fixture = fixture("run-manifest-timeout-abort", 1, true);
        RunSnapshot current = fixture.store.get(fixture.run.runId());
        for (AttemptStatus next : Arrays.asList(
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING)) {
            assertEquals(RunDecision.Code.ACCEPTED, fixture.store.apply(
                    RunCommand.advanceAttempt(
                            "advance-" + next + "-timeout-abort", current.runId(),
                            current.revision(), current.currentAttempt().attemptId(),
                            current.currentAttempt().fenceToken(), next,
                            false, 40L + next.ordinal(), next.name(), null)).code());
            current = fixture.store.get(fixture.run.runId());
        }
        assertEquals(RunDecision.Code.ACCEPTED, fixture.store.apply(
                RunCommand.requestCancellation(
                        "request-timeout-abort", current.runId(), current.revision(),
                        current.currentAttempt().attemptId(),
                        current.currentAttempt().fenceToken(), CancellationCause.TIMEOUT,
                        100L, "RUN_DEADLINE", "deadline reached")).code());
        BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                fixture.manifest.fence(), fixture.manifest.generation(),
                io.rowweave.BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1,
                "domain-authority", 17L, "publication-authority",
                digest(5), "RUN_DEADLINE", 100L);

        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbort(abort).code());
    }

    @Test
    void abortDecisionClosesTheMemberJournal() {
        Fixture fixture = fixture("run-manifest-abort-closes-members", 2, true);
        fixture.store.recordManifestMember(fixture.member(0, "first"));
        fixture.advanceToCommitting();
        BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                fixture.manifest.fence(), fixture.manifest.generation(),
                io.rowweave.BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1,
                "domain-authority", 17L, "publication-authority",
                digest(5), "PREPUBLICATION_FAILURE", 60L);
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbort(abort).code());

        assertEquals(ManifestMemberDecision.Code.MEMBER_CONFLICT,
                fixture.store.recordManifestMember(fixture.member(1, "late")).code());
    }

    @Test
    void abortLifecycleStillAllowsTerminalReconciliation() {
        Fixture fixture = fixture(
                "run-manifest-abort-terminal-boundary", 2, true, false);
        RecoveryClaim recovery = fixture.store.claim(
                fixture.run.runId(), fixture.store.get(fixture.run.runId()).revision(),
                0L, new RecoveryOwner("recovery-master", 71L));
        fixture.advanceToCommitting(recovery.token());
        BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                fixture.manifest.fence(), fixture.manifest.generation(),
                io.rowweave.BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1,
                "domain-authority", 17L, "publication-authority",
                digest(5), "RUN_DEADLINE", 60L);
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbortRecovered(
                        recovery.token(), decided).code());

        StageAssignmentAcceptance first = fixture.assignments.acceptance(0);
        StageTerminalReceipt firstLost = StageTerminalReceipt.lost(
                first.token(), first.commandDigest(),
                "WORKER_LOST", digest(7), 0L);
        assertEquals(StageTerminalDecision.Code.RECORDED,
                fixture.store.recordTerminalRecovered(
                        recovery.token(), firstLost).code());
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbortRecovered(
                        recovery.token(), decided.completed(62L)).code());
        assertEquals(StageTerminalDecision.Code.ALREADY_RECORDED,
                fixture.store.recordTerminalRecovered(
                        recovery.token(), firstLost).code());

        StageAssignmentAcceptance second = fixture.assignments.acceptance(1);
        StageTerminalReceipt secondLost = StageTerminalReceipt.lost(
                second.token(), second.commandDigest(),
                "WORKER_LOST", digest(8), 0L);
        assertEquals(StageTerminalDecision.Code.RECORDED,
                fixture.store.recordTerminalRecovered(
                        recovery.token(), secondLost).code());
        assertEquals(StageTerminalDecision.Code.ALREADY_RECORDED,
                fixture.store.recordTerminalRecovered(
                        recovery.token(), secondLost).code());
    }

    private static Fixture fixture(String runId, int count, boolean success) {
        return fixture(runId, count, success, true);
    }

    private static Fixture fixture(
            String runId, int count, boolean success, boolean recordTerminals) {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", 1, 0L, 1L))
                .snapshot();
        RunSnapshot run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.revision(),
                "attempt-1", "publication-authority", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(run.runId(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken());
        List<StageAssignmentRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, 100L + i, i, 0, StageAssignmentRole.SINK,
                    "worker-" + i, "127.0.0.1", 7001 + i, -1, "inc-" + i));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L, records);
        StageAssignmentSnapshot assignments = store.publish(manifest).snapshot();
        for (int i = 0; i < count; i++) {
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    assignments.token(i), digest(i + 1), 10L + i);
            store.accept(acceptance);
            if (recordTerminals) {
                StageTerminalReceipt terminal = success
                        ? StageTerminalReceipt.succeeded(
                        acceptance.token(), acceptance.commandDigest(), i, i, 20L + i)
                        : StageTerminalReceipt.failed(
                        acceptance.token(), acceptance.commandDigest(), i, 0L,
                        "FAILED", digest(9), 20L + i);
                store.recordTerminal(terminal);
            }
        }
        return new Fixture(store, run, manifest,
                store.assignmentSnapshot(fence));
    }

    private static CommitManifest.Member descriptor(String name, long generation, int value) {
        return new CommitManifest.Member(name,
                "data/" + generation + "/" + name + ".data",
                "schema-" + value, value, digest(value));
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[CommitManifest.SHA256_BYTES];
        Arrays.fill(digest, (byte) value);
        return digest;
    }

    private static final class Fixture {
        final InMemoryRunStateStore store;
        final RunSnapshot run;
        final StageAssignmentManifest manifest;
        final StageAssignmentSnapshot assignments;

        Fixture(InMemoryRunStateStore store, RunSnapshot run,
                StageAssignmentManifest manifest, StageAssignmentSnapshot assignments) {
            this.store = store;
            this.run = run;
            this.manifest = manifest;
            this.assignments = assignments;
        }

        ManifestMemberReceipt member(int ordinal, String name) {
            StageAssignmentAcceptance acceptance = assignments.acceptance(ordinal);
            return ManifestMemberReceipt.sealed(
                    acceptance.token(), acceptance.commandDigest(),
                    "domain-authority", 17L, "publication-authority",
                    descriptor(name, 17L, ordinal + 1), 30L + ordinal);
        }

        ManifestCommitIntent placeholderIntent(
                ManifestCommitJournalSnapshot partial, int expectedMembers) {
            List<byte[]> digests = new ArrayList<>();
            for (int i = 0; i < manifest.shardCount(); i++) digests.add(digest(7));
            return new ManifestCommitIntent(manifest.fence(), manifest.generation(),
                    "domain-authority", 17L, "publication-authority",
                    expectedMembers, digests, digest(8));
        }

        void advanceToCommitting() { advanceToCommitting(null); }

        void advanceToCommitting(RecoveryToken token) {
            for (AttemptStatus next : Arrays.asList(
                    AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                    AttemptStatus.COMMITTING)) {
                RunSnapshot current = store.get(run.runId());
                RunCommand command = RunCommand.advanceAttempt(
                        "advance-" + next + "-" + run.runId(), current.runId(),
                        current.revision(), current.currentAttempt().attemptId(),
                        current.currentAttempt().fenceToken(), next, false,
                        40L + next.ordinal(), next.name(), next.name());
                RunDecision decision = token == null
                        ? store.apply(command) : store.applyRecovered(token, command);
                assertEquals(RunDecision.Code.ACCEPTED, decision.code());
            }
            assertTrue(store.get(run.runId()).currentAttempt().status()
                    == AttemptStatus.COMMITTING);
        }
    }
}
