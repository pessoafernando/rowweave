package io.rowweave.incremental;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.run.ManifestCommitIntentFixtures;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** a visibility proof is scoped to its query ownership. */
class IncrementalJointProofOwnershipContractTest {

    @Test
    void proofBindsFeedIncarnationAndExactDualQueryOwnership(
            @TempDir Path root) throws Exception {
        ProofFixture fixture = fixture(root);
        RecoveryOwner owner = new RecoveryOwner("master", 11L);
        FrontierRecoveryToken frontier = frontier(
                "feed-a", 7L, owner, 1L, 0L, fixture.guard);
        RecoveryToken run = RecoveryToken.create(
                fixture.active.fence().runId(), owner, 1L, 4L);

        VerifiedIncrementalPublicationProof publication =
                IncrementalManifestStrictAdapter.verify(
                        fixture.decision, fixture.store, frontier, run);
        VerifiedIncrementalJointCommitProof joint =
                IncrementalJointCommitProofAssembler.join(
                        fixture.active, fixture.decision, publication);
        assertEquals("feed-a", publication.feedId());
        assertEquals(7L, publication.feedIncarnation());
        assertArrayEquals(frontier.tokenDigest(),
                publication.frontierRecoveryTokenDigest());
        assertArrayEquals(run.tokenDigest(),
                publication.runRecoveryTokenDigest());
        assertEquals(publication.feedId(), joint.feedId());
        assertEquals(publication.feedIncarnation(), joint.feedIncarnation());
        assertArrayEquals(publication.frontierRecoveryTokenDigest(),
                joint.frontierRecoveryTokenDigest());
        assertArrayEquals(publication.runRecoveryTokenDigest(),
                joint.runRecoveryTokenDigest());

        IncrementalAuthorityCommand command = command(joint);
        IncrementalAuthorityMutation mutation =
                IncrementalAuthorityMutation.jointTerminal(
                        command, frontier, run, joint);
        assertSame(run, mutation.runRecoveryToken());

        FrontierRecoveryToken refreshedFrontier = frontier(
                "feed-a", 7L, owner, 2L, 0L, fixture.guard);
        VerifiedIncrementalPublicationProof refreshedFrontierProof =
                IncrementalManifestStrictAdapter.verify(
                        fixture.decision, fixture.store, refreshedFrontier, run);
        assertNotEquals(hex(publication.proofDigest()),
                hex(refreshedFrontierProof.proofDigest()));

        RecoveryToken refreshedRun = RecoveryToken.create(
                fixture.active.fence().runId(), owner, 2L, 4L);
        VerifiedIncrementalPublicationProof refreshedRunProof =
                IncrementalManifestStrictAdapter.verify(
                        fixture.decision, fixture.store,
                        refreshedFrontier, refreshedRun);
        assertNotEquals(hex(refreshedFrontierProof.proofDigest()),
                hex(refreshedRunProof.proofDigest()));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.jointTerminal(
                        command, refreshedFrontier, run, joint));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.jointTerminal(
                        command, frontier, refreshedRun, joint));
        VerifiedIncrementalJointCommitProof refreshedJoint =
                IncrementalJointCommitProofAssembler.join(
                        fixture.active, fixture.decision, refreshedRunProof);
        assertSame(refreshedJoint, IncrementalAuthorityMutation.jointTerminal(
                command(refreshedJoint), refreshedFrontier, refreshedRun,
                refreshedJoint).jointCommitProof());

        FrontierRecoveryToken otherFeed = frontier(
                "feed-b", 7L, owner, 1L, 0L, fixture.guard);
        VerifiedIncrementalPublicationProof otherFeedProof =
                IncrementalManifestStrictAdapter.verify(
                        fixture.decision, fixture.store, otherFeed, run);
        assertNotEquals(hex(publication.proofDigest()),
                hex(otherFeedProof.proofDigest()));

        RecoveryToken otherOwnerRun = RecoveryToken.create(
                fixture.active.fence().runId(),
                new RecoveryOwner("other", 12L), 1L, 4L);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalManifestStrictAdapter.verify(
                        fixture.decision, fixture.store,
                        refreshedFrontier, otherOwnerRun));
        RecoveryToken wrongRun = RecoveryToken.create(
                "other-run", owner, 1L, 4L);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalManifestStrictAdapter.verify(
                        fixture.decision, fixture.store, frontier, wrongRun));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.jointTerminal(
                        command, refreshedFrontier, otherOwnerRun, joint));

        assertNoOverload(IncrementalManifestStrictAdapter.class, "verify", 2);
        assertNoOverload(IncrementalAuthorityMutation.class, "jointTerminal", 3);
    }

    private static ProofFixture fixture(Path root) throws Exception {
        ManifestFileStore store = new ManifestFileStore(root);
        Path staged = store.stagingPath("publication", "member");
        Files.write(staged, bytes("data"));
        store.forceStaged(staged);
        CommitManifest.Member member = store.installMember(
                9L, "member", "schema", staged);
        CommitManifest manifest = new CommitManifest(
                store.domainId(), 9L, "publication",
                Collections.singletonList(member));
        store.publish(manifest);

        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                7L, sha256("definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        IncrementalIntervalIdentity identity = IncrementalIntervalIdentity.from(capture);
        RunAttemptFence fence = new RunAttemptFence(
                identity.candidateRunId(), "attempt", 1L);
        IncrementalActiveInterval shell = new IncrementalActiveInterval(
                identity, capture, "artifact", 0L, fence, 1,
                IncrementalAttemptCommitPhase.CLAIMED, null);
        ManifestCommitIntentFixtures.CommittedAuthority committed =
                ManifestCommitIntentFixtures.committedAuthority(fence, manifest);
        byte[] decisionDigest = sha256(
                io.rowweave.run.ManifestCommitJournalCodec.encodeIntent(
                        committed.intent()));
        IncrementalActiveInterval active = new IncrementalActiveInterval(
                identity, capture, shell.artifactId(), 0L, fence, 1,
                IncrementalAttemptCommitPhase.COMMIT_DECIDED, decisionDigest);
        VerifiedIncrementalSinkCommitDecision decision =
                IncrementalSinkIntentStrictAdapter.verify(
                        active, committed.authority());
        return new ProofFixture(active, decision, store,
                new FeedGuardShardObservation(0, 1L, sha256("guard")));
    }

    private static IncrementalAuthorityCommand command(
            VerifiedIncrementalJointCommitProof proof) {
        return new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.COMMIT, "feed-a", "joint", 3L,
                7L, proof.intervalId(), proof.fence(), proof.proofDigest());
    }

    private static FrontierRecoveryToken frontier(
            String feedId, long feedIncarnation, RecoveryOwner owner,
            long ownershipEpoch, long claimedRevision,
            FeedGuardShardObservation guard) {
        return FrontierRecoveryToken.create(
                feedId, owner, ownershipEpoch, claimedRevision,
                feedIncarnation, guard, 1L);
    }

    private static byte[] sha256(String value) throws Exception {
        return sha256(bytes(value));
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static String hex(byte[] value) {
        StringBuilder out = new StringBuilder(value.length * 2);
        for (byte b : value) out.append(String.format("%02x", b & 0xff));
        return out.toString();
    }

    private static void assertNoOverload(
            Class<?> type, String methodName, int parameterCount) {
        for (Method method : type.getDeclaredMethods()) {
            if (method.getName().equals(methodName)
                    && method.getParameterCount() == parameterCount) {
                throw new AssertionError("stale overload remains: " + method);
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class ProofFixture {
        private final IncrementalActiveInterval active;
        private final VerifiedIncrementalSinkCommitDecision decision;
        private final ManifestFileStore store;
        private final FeedGuardShardObservation guard;

        private ProofFixture(
                IncrementalActiveInterval active,
                VerifiedIncrementalSinkCommitDecision decision,
                ManifestFileStore store,
                FeedGuardShardObservation guard) {
            this.active = active;
            this.decision = decision;
            this.store = store;
            this.guard = guard;
        }
    }
}
