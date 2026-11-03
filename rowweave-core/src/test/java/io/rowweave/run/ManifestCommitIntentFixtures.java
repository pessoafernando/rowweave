package io.rowweave.run;

import io.rowweave.commit.CommitManifest;

import java.util.Arrays;
import java.util.Collections;

/** Test-only bridge for constructing the package-owned durable Manifest intent. */
public final class ManifestCommitIntentFixtures {
    private ManifestCommitIntentFixtures() {}

    public static ManifestCommitIntent ordinary(
            RunAttemptFence fence, String domainId, long generation,
            String publicationId, byte[] manifestHash) {
        return new ManifestCommitIntent(
                fence, 1L, domainId, generation, publicationId, 1,
                Collections.singletonList(new byte[32]), manifestHash);
    }

    /** Seeds the real in-memory Run authority through its durable journal protocol. */
    public static CommittedAuthority committedAuthority(
            RunAttemptFence expectedFence, CommitManifest manifest) {
        if (expectedFence == null || manifest == null || manifest.members().size() != 1) {
            throw new IllegalArgumentException("one-member committed authority fixture is required");
        }
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot created = accepted(store.apply(RunCommand.createRun(
                "create", expectedFence.runId(), "definition", "artifact", 1, 0L, 1L)));
        RunSnapshot run = accepted(store.apply(RunCommand.createAttempt(
                "attempt", created.runId(), created.revision(),
                expectedFence.attemptId(), manifest.attemptId(), 2L)));
        RunAttemptFence actualFence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        if (!expectedFence.equals(actualFence)) {
            throw new IllegalArgumentException("fixture fence is not the first Run attempt fence");
        }
        StageAssignmentRecord record = StageAssignmentRecord.create(
                actualFence, 1L, 0, 100L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 7001, -1, "worker-incarnation");
        StageAssignmentManifest assignments = new StageAssignmentManifest(
                actualFence, run.artifactId(), 1L, Collections.singletonList(record));
        StageAssignmentSnapshot snapshot = store.publish(assignments).snapshot();
        byte[] commandDigest = digest(3);
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                snapshot.token(0), commandDigest, 10L);
        require(store.accept(acceptance).code() == StageAcceptanceDecision.Code.ACCEPTED,
                "assignment acceptance failed");
        require(store.recordTerminal(StageTerminalReceipt.succeeded(
                acceptance.token(), acceptance.commandDigest(), 1L, 1L, 20L)).code()
                == StageTerminalDecision.Code.RECORDED, "terminal receipt failed");
        ManifestMemberReceipt member = ManifestMemberReceipt.sealed(
                acceptance.token(), acceptance.commandDigest(), manifest.domainId(),
                manifest.generation(), manifest.attemptId(), manifest.members().get(0), 30L);
        require(store.recordManifestMember(member).code()
                == ManifestMemberDecision.Code.RECORDED, "manifest member receipt failed");
        for (AttemptStatus status : Arrays.asList(
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING, AttemptStatus.COMMITTING)) {
            RunSnapshot current = store.get(run.runId());
            require(store.apply(RunCommand.advanceAttempt(
                    "advance-" + status, current.runId(), current.revision(),
                    current.currentAttempt().attemptId(),
                    current.currentAttempt().fenceToken(), status, false,
                    40L + status.ordinal(), status.name(), status.name())).code()
                    == RunDecision.Code.ACCEPTED, "Run advance failed: " + status);
        }
        ManifestCommitIntent intent = store.manifestCommitJournal(actualFence).buildIntent();
        require(store.commitManifestIntent(intent).code()
                == ManifestIntentDecision.Code.COMMITTED, "manifest intent commit failed");
        return new CommittedAuthority(store, intent);
    }

    private static RunSnapshot accepted(RunDecision decision) {
        require(decision.code() == RunDecision.Code.ACCEPTED, "Run command failed");
        return decision.snapshot();
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) value);
        return digest;
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    public static final class CommittedAuthority {
        private final DurableStageAssignmentAuthority authority;
        private final ManifestCommitIntent intent;

        private CommittedAuthority(
                DurableStageAssignmentAuthority authority, ManifestCommitIntent intent) {
            this.authority = authority;
            this.intent = intent;
        }

        public DurableStageAssignmentAuthority authority() { return authority; }
        public ManifestCommitIntent intent() { return intent; }
    }
}
