package io.rowweave.incremental;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.run.DurableManifestCommitIntentObservation;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitIntentFixtures;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical commands and authority-only joint proofs. */
class IncrementalAuthorityProofContractTest {

    @Test
    void allSixCommandKindsHaveCanonicalExactIdentity() throws Exception {
        for (IncrementalAuthorityCommandKind kind : IncrementalAuthorityCommandKind.values()) {
            IncrementalAuthorityCommand command = command(kind, sha256("transition"));
            byte[] frame = IncrementalAuthorityCommandCodec.encode(command);
            assertTrue(frame.length <= IncrementalAuthorityCommandCodec.MAX_FRAME_BYTES);
            IncrementalAuthorityCommand decoded = IncrementalAuthorityCommandCodec.decode(frame);
            assertEquals(command, decoded);
            assertArrayEquals(frame, IncrementalAuthorityCommandCodec.encode(decoded));
            assertArrayEquals(IncrementalAuthorityCommandCodec.digest(command),
                    IncrementalAuthorityCommandCodec.digest(decoded));
        }
        assertEquals(6, IncrementalAuthorityCommandKind.values().length);

        IncrementalAuthorityCommand first = command(
                IncrementalAuthorityCommandKind.COMMIT, sha256("first"));
        IncrementalAuthorityCommand conflicting = command(
                IncrementalAuthorityCommandKind.COMMIT, sha256("conflicting"));
        assertEquals(first.commandId(), conflicting.commandId());
        assertNotEquals(hex(IncrementalAuthorityCommandCodec.digest(first)),
                hex(IncrementalAuthorityCommandCodec.digest(conflicting)));
    }

    @Test
    void commandShapeAndCodecFailClosed() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.INITIALIZE, "feed", "command", 0L, 1L,
                hex(sha256("unexpected")), fence(), sha256("transition")));
        assertThrows(IllegalArgumentException.class, () -> new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.COMMIT, "feed", "command", 0L, 1L,
                null, null, sha256("transition")));
        assertThrows(IllegalArgumentException.class, () -> new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.MARK_CONFLICT, "feed", "command", 0L, 1L,
                hex(sha256("half")), null, sha256("transition")));

        byte[] corrupt = IncrementalAuthorityCommandCodec.encode(command(
                IncrementalAuthorityCommandKind.CLAIM, sha256("transition")));
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityCommandCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityCommandCodec.decode(
                        new byte[IncrementalAuthorityCommandCodec.MAX_FRAME_BYTES + 1]));

        byte[] canonical = IncrementalAuthorityCommandCodec.encode(command(
                IncrementalAuthorityCommandKind.CLAIM, sha256("transition")));
        assertRejectedAfterResign(canonical, 4, new byte[]{2});
        assertRejectedAfterResign(canonical, 5, new byte[]{99});
        assertRejectedAfterResign(canonical, 6,
                new byte[]{0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff});
        assertRejectedAfterResign(canonical, 10,
                new byte[]{(byte) 0xc3, 0x28, 0x78, 0x78});

        byte[] nfc = IncrementalAuthorityCommandCodec.encode(new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, "abc", "command", 0L, 7L,
                hex(sha256("interval")), fence(), sha256("transition")));
        assertRejectedAfterResign(nfc, 10,
                new byte[]{0x65, (byte) 0xcc, (byte) 0x81});
        assertRejectedAfterResign(canonical, presenceOffset(canonical), new byte[]{2});
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityCommandCodec.decode(
                        appendBodyByteAndResign(canonical, (byte) 1)));
    }

    @Test
    void strictAdaptersMintProofOnlyForExactDurableIntentAndReopenedCommittedManifest(
            @TempDir Path domain) throws Exception {
        ProofFixture fixture = proofFixture(domain, claimedCapture());
        VerifiedIncrementalSinkCommitDecision sinkDecision =
                IncrementalSinkIntentStrictAdapter.verify(
                        fixture.active, fixture.sinkAuthority);
        VerifiedIncrementalPublicationProof publication =
                IncrementalManifestStrictAdapter.verify(
                        sinkDecision, fixture.manifestStore,
                        token(), runToken(sinkDecision.fence()));

        assertEquals(fixture.active.fence(), sinkDecision.fence());
        assertArrayEquals(fixture.active.commitDecisionDigest(),
                sinkDecision.commitDecisionDigest());
        assertEquals("publication", publication.publicationId());
        assertEquals(9L, publication.generation());
        assertArrayEquals(fixture.manifestHash, publication.manifestHash());
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalManifestStrictAdapter.verifyConflict(
                        fixture.active, sinkDecision, fixture.manifestStore,
                        token(), runToken(sinkDecision.fence())));

        ManifestFileStore notVisible = new ManifestFileStore(domain.resolve("not-visible"));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalManifestStrictAdapter.verify(
                        sinkDecision, notVisible,
                        token(), runToken(sinkDecision.fence())));
        ManifestFileStore wrongDomain = publishedStore(
                domain.resolve("wrong-domain"), "other-publication", 9L, "member");
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalManifestStrictAdapter.verify(
                        sinkDecision, wrongDomain,
                        token(), runToken(sinkDecision.fence())));
        ManifestFileStore wrongPublication = publishedStore(
                domain.resolve("wrong-publication"), "other-publication", 9L, "member");
        CommitManifest intendedPublication = new CommitManifest(
                wrongPublication.domainId(), 9L, "publication",
                wrongPublication.exact(9L).members());
        IncrementalActiveInterval shell = claimedActive();
        ManifestCommitIntentFixtures.CommittedAuthority intendedAuthority =
                ManifestCommitIntentFixtures.committedAuthority(
                        shell.fence(), intendedPublication);
        IncrementalActiveInterval intendedActive = active(
                shell.capturedTarget(), shell.fence(), sha256(
                        io.rowweave.run.ManifestCommitJournalCodec.encodeIntent(
                                intendedAuthority.intent())));
        VerifiedIncrementalSinkCommitDecision intendedDecision =
                IncrementalSinkIntentStrictAdapter.verify(
                        intendedActive, intendedAuthority.authority());
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalManifestStrictAdapter.verify(
                        intendedDecision, wrongPublication,
                        token(), runToken(intendedDecision.fence())));

        assertFalse(hasVerifyOverload(
                IncrementalSinkIntentStrictAdapter.class, ManifestCommitIntent.class));
        assertFalse(hasVerifyOverload(
                IncrementalManifestStrictAdapter.class, CommitManifest.class));
    }

    @Test
    void sinkIntentProofRejectsFenceOrCanonicalIntentDigestDrift(@TempDir Path domain)
            throws Exception {
        ProofFixture fixture = proofFixture(domain, claimedCapture());
        IncrementalActiveInterval wrongDecision = active(
                fixture.active.capturedTarget(), fixture.active.fence(), sha256("wrong-intent"));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSinkIntentStrictAdapter.verify(
                        wrongDecision, fixture.sinkAuthority));

        RunAttemptFence otherFence = new RunAttemptFence(
                fixture.active.fence().runId(), "other-attempt", 2L);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSinkIntentStrictAdapter.verify(
                        new IncrementalActiveInterval(
                                fixture.active.identity(), fixture.active.capturedTarget(),
                                fixture.active.artifactId(), fixture.active.claimedRevision(),
                                otherFence, fixture.active.attemptOrdinal(),
                                IncrementalAttemptCommitPhase.COMMIT_DECIDED,
                                fixture.active.commitDecisionDigest()),
                        fixture.sinkAuthority));
    }

    @Test
    void jointProofBindsActiveIntentAndPublicationByteExactly(@TempDir Path domain)
            throws Exception {
        ProofFixture fixture = proofFixture(domain, claimedCapture());
        VerifiedIncrementalSinkCommitDecision sinkDecision =
                IncrementalSinkIntentStrictAdapter.verify(
                        fixture.active, fixture.sinkAuthority);
        VerifiedIncrementalPublicationProof publication =
                IncrementalManifestStrictAdapter.verify(
                        sinkDecision, fixture.manifestStore,
                        token(), runToken(sinkDecision.fence()));
        VerifiedIncrementalJointCommitProof joint =
                IncrementalJointCommitProofAssembler.join(
                        fixture.active, sinkDecision, publication);

        assertEquals(fixture.active.identity().intervalId(), joint.intervalId());
        assertEquals(fixture.active.identity().capturedTargetReceiptDigest(),
                joint.capturedTargetReceiptDigest());
        assertArrayEquals(fixture.active.capturedTarget().definitionDigest(),
                joint.definitionFingerprint());
        assertEquals(IncrementalRunMode.SNAPSHOT_V1, joint.mode());
        assertEquals(fixture.active.fence(), joint.fence());
        assertEquals(fixture.active.artifactId(), joint.artifactId());
        assertArrayEquals(fixture.active.commitDecisionDigest(),
                joint.commitDecisionDigest());
        assertEquals("publication", joint.publicationId());
        assertEquals(9L, joint.generation());
        assertArrayEquals(fixture.manifestHash, joint.manifestHash());
        assertEquals(32, joint.proofDigest().length);

        IncrementalActiveInterval otherActive = active(
                fixture.active.capturedTarget(),
                new RunAttemptFence(fixture.active.fence().runId(), "other", 2L),
                fixture.active.commitDecisionDigest());
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalJointCommitProofAssembler.join(
                        otherActive, sinkDecision, publication));

        assertJointDigestDrifts(fixture, sinkDecision, publication, joint);
    }

    @Test
    void jointTerminalCommandMustMatchCapturedRunMode(@TempDir Path domain)
            throws Exception {
        ProofFixture normal = proofFixture(domain.resolve("normal"), claimedCapture());
        CapturedTargetReceipt backfillCapture = CapturedTargetReceipt.backfill(
                7L, sha256("definition"), sha256("source-contract"), 1L,
                IncrementalFrontier.int64Sequence(0L),
                IncrementalFrontier.int64Sequence(10L), "backfill", bytes("snapshot"));
        ProofFixture backfill = proofFixture(domain.resolve("backfill"), backfillCapture);
        VerifiedIncrementalJointCommitProof normalProof = jointProof(normal);
        VerifiedIncrementalJointCommitProof backfillProof = jointProof(backfill);
        assertEquals(IncrementalRunMode.SNAPSHOT_V1, normalProof.mode());
        assertEquals(IncrementalRunMode.BACKFILL_V1, backfillProof.mode());

        FrontierRecoveryToken token = token();
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.jointTerminal(
                        command(IncrementalAuthorityCommandKind.COMPLETE_BACKFILL,
                                normalProof.proofDigest(), normalProof.intervalId(),
                                normalProof.fence()), token,
                        runToken(normalProof.fence()), normalProof));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.jointTerminal(
                        command(IncrementalAuthorityCommandKind.COMMIT,
                                backfillProof.proofDigest(), backfillProof.intervalId(),
                                backfillProof.fence()), token,
                        runToken(backfillProof.fence()), backfillProof));
        assertEquals(normalProof, IncrementalAuthorityMutation.jointTerminal(
                command(IncrementalAuthorityCommandKind.COMMIT,
                        normalProof.proofDigest(), normalProof.intervalId(),
                        normalProof.fence()), token,
                runToken(normalProof.fence()), normalProof).jointCommitProof());
        assertEquals(backfillProof, IncrementalAuthorityMutation.jointTerminal(
                command(IncrementalAuthorityCommandKind.COMPLETE_BACKFILL,
                        backfillProof.proofDigest(), backfillProof.intervalId(),
                        backfillProof.fence()), token,
                runToken(backfillProof.fence()), backfillProof).jointCommitProof());
    }

    @Test
    void abortClosureProofHasNoStandaloneDigestAssembler() throws Exception {
        Constructor<?>[] constructors =
                VerifiedIncrementalAbortClosureProof.class.getDeclaredConstructors();
        assertEquals(1, constructors.length);
        assertTrue(Modifier.isPrivate(constructors[0].getModifiers()));
    }

    @Test
    void internalAuthorityHasOneJointMutationSurfaceAndProofsAreNotClientConstructible(
            @TempDir Path domain) throws Exception {
        assertFalse(Modifier.isPublic(IncrementalRunCommitAuthority.class.getModifiers()));
        assertFalse(Modifier.isPublic(IncrementalAuthorityMutation.class.getModifiers()));
        Method[] methods = IncrementalRunCommitAuthority.class.getDeclaredMethods();
        assertEquals(1, methods.length);
        assertEquals("apply", methods[0].getName());
        assertArrayEquals(new Class<?>[]{IncrementalAuthorityMutation.class},
                methods[0].getParameterTypes());

        assertNotPubliclyConstructible(VerifiedIncrementalSinkCommitDecision.class);
        assertNotPubliclyConstructible(VerifiedIncrementalPublicationProof.class);
        assertNotPubliclyConstructible(VerifiedIncrementalJointCommitProof.class);
        assertNotPubliclyConstructible(VerifiedIncrementalAbortClosureProof.class);
        assertNotPubliclyConstructible(VerifiedIncrementalConflictProof.class);
        assertNotPubliclyConstructible(DurableManifestCommitIntentObservation.class);

        ProofFixture fixture = proofFixture(domain, claimedCapture());
        VerifiedIncrementalSinkCommitDecision sinkDecision =
                IncrementalSinkIntentStrictAdapter.verify(
                        fixture.active, fixture.sinkAuthority);
        VerifiedIncrementalPublicationProof publication =
                IncrementalManifestStrictAdapter.verify(
                        sinkDecision, fixture.manifestStore,
                        token(), runToken(sinkDecision.fence()));
        VerifiedIncrementalJointCommitProof joint =
                IncrementalJointCommitProofAssembler.join(
                        fixture.active, sinkDecision, publication);
        FrontierRecoveryToken token = token();

        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.control(
                        command(IncrementalAuthorityCommandKind.COMMIT,
                                sha256("transition")), token));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.control(
                        command(IncrementalAuthorityCommandKind.MARK_CONFLICT,
                                sha256("transition")), token));
        IncrementalAuthorityMutation commit = IncrementalAuthorityMutation.jointTerminal(
                command(IncrementalAuthorityCommandKind.COMMIT, joint.proofDigest(),
                        joint.intervalId(), joint.fence()),
                token, runToken(joint.fence()), joint);
        assertEquals(joint, commit.jointCommitProof());
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.closeAborted(
                        command(IncrementalAuthorityCommandKind.COMMIT,
                                sha256("transition")), token, null, null));

        IncrementalAuthorityCommand laterRevision = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, "feed", "later", 5L, 7L,
                fixture.active.identity().intervalId(), fixture.active.fence(),
                sha256("later-transition"));
        assertEquals(laterRevision, IncrementalAuthorityMutation.control(
                laterRevision, token).command());
    }

    private static IncrementalAuthorityCommand command(
            IncrementalAuthorityCommandKind kind, byte[] transitionDigest) throws Exception {
        boolean initialize = kind == IncrementalAuthorityCommandKind.INITIALIZE;
        boolean markConflict = kind == IncrementalAuthorityCommandKind.MARK_CONFLICT;
        return new IncrementalAuthorityCommand(
                kind, "feed", "same-command", 0L, 7L,
                initialize || markConflict ? null : hex(sha256("interval")),
                initialize || markConflict ? null : fence(), transitionDigest);
    }

    private static IncrementalAuthorityCommand command(
            IncrementalAuthorityCommandKind kind, byte[] transitionDigest,
            String intervalId, RunAttemptFence commandFence) {
        return new IncrementalAuthorityCommand(
                kind, "feed", "same-command", 0L, 7L,
                intervalId, commandFence, transitionDigest);
    }

    private static ProofFixture proofFixture(Path domain, CapturedTargetReceipt capture)
            throws Exception {
        ManifestFileStore manifestStore = publishedStore(
                domain, "publication", 9L, "member");
        CommitManifest manifest = manifestStore.exact(9L);
        byte[] manifestHash = sha256(manifest.encode());
        IncrementalActiveInterval shell = claimedActive(capture);
        ManifestCommitIntentFixtures.CommittedAuthority committed =
                ManifestCommitIntentFixtures.committedAuthority(shell.fence(), manifest);
        ManifestCommitIntent intent = committed.intent();
        byte[] decisionDigest = sha256(
                io.rowweave.run.ManifestCommitJournalCodec.encodeIntent(intent));
        IncrementalActiveInterval active = active(
                shell.capturedTarget(), shell.fence(), decisionDigest);
        return new ProofFixture(active, committed.authority(), manifestStore,
                manifest, manifestHash);
    }

    private static VerifiedIncrementalJointCommitProof jointProof(ProofFixture fixture)
            throws Exception {
        VerifiedIncrementalSinkCommitDecision decision =
                IncrementalSinkIntentStrictAdapter.verify(
                        fixture.active, fixture.sinkAuthority);
        VerifiedIncrementalPublicationProof publication =
                IncrementalManifestStrictAdapter.verify(
                        decision, fixture.manifestStore,
                        token(), runToken(decision.fence()));
        return IncrementalJointCommitProofAssembler.join(
                fixture.active, decision, publication);
    }

    private static IncrementalActiveInterval claimedActive() throws Exception {
        return claimedActive(claimedCapture());
    }

    private static CapturedTargetReceipt claimedCapture() throws Exception {
        return CapturedTargetReceipt.normal(
                7L, sha256("definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.begin(), "trigger", IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
    }

    private static IncrementalActiveInterval claimedActive(CapturedTargetReceipt capture) {
        IncrementalIntervalIdentity identity = IncrementalIntervalIdentity.from(capture);
        return new IncrementalActiveInterval(
                identity, capture, "artifact", 0L,
                new RunAttemptFence(identity.candidateRunId(), "attempt", 1L),
                1, IncrementalAttemptCommitPhase.CLAIMED, null);
    }

    private static IncrementalActiveInterval active(
            CapturedTargetReceipt capture, RunAttemptFence fence, byte[] decisionDigest) {
        return active(capture, fence, decisionDigest, "artifact");
    }

    private static IncrementalActiveInterval active(
            CapturedTargetReceipt capture, RunAttemptFence fence,
            byte[] decisionDigest, String artifactId) {
        return new IncrementalActiveInterval(
                IncrementalIntervalIdentity.from(capture), capture, artifactId, 0L,
                fence, 1, IncrementalAttemptCommitPhase.COMMIT_DECIDED, decisionDigest);
    }

    private static ManifestFileStore publishedStore(
            Path domain, String publicationId, long generation, String member)
            throws Exception {
        ManifestFileStore store = new ManifestFileStore(domain);
        Path staged = store.stagingPath(publicationId, member);
        Files.write(staged, bytes("data-" + member));
        store.forceStaged(staged);
        CommitManifest.Member value = store.installMember(
                generation, member, "schema", staged);
        store.publish(new CommitManifest(store.domainId(), generation,
                publicationId, Collections.singletonList(value)));
        return store;
    }

    private static FrontierRecoveryToken token() throws Exception {
        return FrontierRecoveryToken.create(
                "feed", new RecoveryOwner("master", 1L), 1L, 0L, 7L,
                new FeedGuardShardObservation(0, 1L, sha256("guard")), 1L);
    }

    private static RecoveryToken runToken(RunAttemptFence fence) {
        return RecoveryToken.create(
                fence.runId(), new RecoveryOwner("master", 1L), 1L, 1L);
    }

    private static void assertNotPubliclyConstructible(Class<?> type) {
        for (Constructor<?> constructor : type.getDeclaredConstructors()) {
            assertFalse(Modifier.isPublic(constructor.getModifiers()),
                    "proof constructor must not be public: " + constructor);
        }
        for (Method method : type.getDeclaredMethods()) {
            if (method.getReturnType() == type) {
                assertFalse(Modifier.isPublic(method.getModifiers()),
                        "proof factory must not be public: " + method);
            }
        }
    }

    private static boolean hasVerifyOverload(Class<?> type, Class<?> forbiddenParameter) {
        for (Method method : type.getDeclaredMethods()) {
            if (!method.getName().equals("verify")) continue;
            for (Class<?> parameter : method.getParameterTypes()) {
                if (parameter == forbiddenParameter) return true;
            }
        }
        return false;
    }

    private static void assertJointDigestDrifts(
            ProofFixture fixture, VerifiedIncrementalSinkCommitDecision decision,
            VerifiedIncrementalPublicationProof publication,
            VerifiedIncrementalJointCommitProof baseline) throws Exception {
        CapturedTargetReceipt otherCapture = CapturedTargetReceipt.normal(
                7L, sha256("other-definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.begin(), "other-trigger", IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        RunAttemptFence otherFence = new RunAttemptFence(
                IncrementalIntervalIdentity.from(otherCapture).candidateRunId(),
                fixture.active.fence().attemptId(), fixture.active.fence().fenceToken());
        IncrementalActiveInterval other = active(
                otherCapture, otherFence, fixture.active.commitDecisionDigest());
        VerifiedIncrementalSinkCommitDecision otherDecision =
                new VerifiedIncrementalSinkCommitDecision(
                        decision.domainId(), otherFence, decision.publicationId(),
                        decision.generation(), decision.manifestHash(),
                        decision.commitDecisionDigest());
        VerifiedIncrementalPublicationProof otherPublication =
                new VerifiedIncrementalPublicationProof(
                        otherDecision, token(), runToken(otherFence));
        VerifiedIncrementalJointCommitProof captureAndDefinitionDrift =
                IncrementalJointCommitProofAssembler.join(
                        other, otherDecision, otherPublication);
        assertNotEquals(hex(baseline.proofDigest()),
                hex(captureAndDefinitionDrift.proofDigest()));

        IncrementalActiveInterval artifactDrift = active(
                fixture.active.capturedTarget(), fixture.active.fence(),
                fixture.active.commitDecisionDigest(), "other-artifact");
        VerifiedIncrementalJointCommitProof artifactProof =
                IncrementalJointCommitProofAssembler.join(
                        artifactDrift, decision, publication);
        assertNotEquals(hex(baseline.proofDigest()), hex(artifactProof.proofDigest()));

        CapturedTargetReceipt firstBackfill = CapturedTargetReceipt.backfill(
                7L, sha256("definition"), sha256("source-contract"), 1L,
                IncrementalFrontier.int64Sequence(0L),
                IncrementalFrontier.int64Sequence(10L),
                "stable-backfill", bytes("snapshot-token-a"));
        CapturedTargetReceipt recapturedBackfill = CapturedTargetReceipt.backfill(
                7L, sha256("definition"), sha256("source-contract"), 1L,
                IncrementalFrontier.int64Sequence(0L),
                IncrementalFrontier.int64Sequence(10L),
                "stable-backfill", bytes("snapshot-token-b"));
        IncrementalIntervalIdentity firstIdentity =
                IncrementalIntervalIdentity.from(firstBackfill);
        IncrementalIntervalIdentity recapturedIdentity =
                IncrementalIntervalIdentity.from(recapturedBackfill);
        assertEquals(firstIdentity.candidateRunId(), recapturedIdentity.candidateRunId());
        assertEquals(firstIdentity.intervalId(), recapturedIdentity.intervalId());
        assertNotEquals(firstIdentity.capturedTargetReceiptDigest(),
                recapturedIdentity.capturedTargetReceiptDigest());
        RunAttemptFence stableFence = new RunAttemptFence(
                firstIdentity.candidateRunId(), "attempt", 1L);
        VerifiedIncrementalSinkCommitDecision stableDecision =
                new VerifiedIncrementalSinkCommitDecision(
                        decision.domainId(), stableFence, decision.publicationId(),
                        decision.generation(), decision.manifestHash(),
                        decision.commitDecisionDigest());
        VerifiedIncrementalPublicationProof stablePublication =
                new VerifiedIncrementalPublicationProof(
                        stableDecision, token(), runToken(stableFence));
        VerifiedIncrementalJointCommitProof firstBackfillProof =
                IncrementalJointCommitProofAssembler.join(
                        active(firstBackfill, stableFence,
                                decision.commitDecisionDigest()),
                        stableDecision, stablePublication);
        VerifiedIncrementalJointCommitProof recapturedBackfillProof =
                IncrementalJointCommitProofAssembler.join(
                        active(recapturedBackfill, stableFence,
                                decision.commitDecisionDigest()),
                        stableDecision, stablePublication);
        assertEquals(firstBackfillProof.intervalId(), recapturedBackfillProof.intervalId());
        assertEquals(firstBackfillProof.fence(), recapturedBackfillProof.fence());
        assertArrayEquals(firstBackfillProof.definitionFingerprint(),
                recapturedBackfillProof.definitionFingerprint());
        assertEquals(firstBackfillProof.artifactId(), recapturedBackfillProof.artifactId());
        assertEquals(firstBackfillProof.publicationId(),
                recapturedBackfillProof.publicationId());
        assertNotEquals(hex(firstBackfillProof.proofDigest()),
                hex(recapturedBackfillProof.proofDigest()));
    }

    private static int presenceOffset(byte[] frame) {
        int offset = 6;
        int feedLength = readInt(frame, offset);
        offset += 4 + feedLength;
        int commandLength = readInt(frame, offset);
        return offset + 4 + commandLength + 8 + 8;
    }

    private static int readInt(byte[] value, int offset) {
        return ((value[offset] & 0xff) << 24)
                | ((value[offset + 1] & 0xff) << 16)
                | ((value[offset + 2] & 0xff) << 8)
                | (value[offset + 3] & 0xff);
    }

    private static void assertRejectedAfterResign(
            byte[] frame, int offset, byte[] replacement) throws Exception {
        byte[] malformed = frame.clone();
        System.arraycopy(replacement, 0, malformed, offset, replacement.length);
        resign(malformed);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityCommandCodec.decode(malformed));
    }

    private static byte[] appendBodyByteAndResign(byte[] frame, byte extra)
            throws Exception {
        int bodyLength = frame.length - 32;
        byte[] extended = new byte[frame.length + 1];
        System.arraycopy(frame, 0, extended, 0, bodyLength);
        extended[bodyLength] = extra;
        System.arraycopy(frame, bodyLength, extended, bodyLength + 1, 32);
        resign(extended);
        return extended;
    }

    private static void resign(byte[] frame) throws Exception {
        int bodyLength = frame.length - 32;
        byte[] digest = sha256(Arrays.copyOf(frame, bodyLength));
        System.arraycopy(digest, 0, frame, bodyLength, digest.length);
    }

    private static byte[] sha256(String value) throws Exception {
        return sha256(bytes(value));
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] value) {
        char[] output = new char[value.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < value.length; i++) {
            int unsigned = value[i] & 0xff;
            output[i * 2] = digits[unsigned >>> 4];
            output[i * 2 + 1] = digits[unsigned & 0xf];
        }
        return new String(output);
    }

    private static RunAttemptFence fence() throws Exception {
        return new RunAttemptFence(hex(sha256("run")), "attempt", 1L);
    }

    private static final class ProofFixture {
        private final IncrementalActiveInterval active;
        private final DurableStageAssignmentAuthority sinkAuthority;
        private final ManifestFileStore manifestStore;
        private final CommitManifest manifest;
        private final byte[] manifestHash;

        private ProofFixture(
                IncrementalActiveInterval active,
                DurableStageAssignmentAuthority sinkAuthority,
                ManifestFileStore manifestStore,
                CommitManifest manifest, byte[] manifestHash) {
            this.active = active;
            this.sinkAuthority = sinkAuthority;
            this.manifestStore = manifestStore;
            this.manifest = manifest;
            this.manifestHash = manifestHash;
        }
    }
}
