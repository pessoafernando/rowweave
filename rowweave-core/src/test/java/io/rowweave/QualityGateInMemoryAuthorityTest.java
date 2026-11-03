package io.rowweave;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalCodec;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkLocatorCodec;
import io.rowweave.run.QualityGateAssignmentBinding;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunResultSummary;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** one in-memory authority slot before irreversible intent. */
class QualityGateInMemoryAuthorityTest {

    private static final List<String> ROLES = Arrays.asList(
            TypedQualityManifest.VALID_SINK_UID,
            TypedQualityManifest.REJECTED_SINK_UID,
            TypedQualityManifest.SUMMARY_SINK_UID);

    @Test
    void passDecisionIsFirstWriteWinsFreezesMembersAndIsEmbeddedInQualityIntent() {
        Fixture fixture = fixture("run-quality-pass");
        fixture.sealAll();
        fixture.advanceToCommitting(null);
        QualityGateDecision decision = fixture.decision(QualityGatePolicy.allowAll(), 9L, 1L);

        assertEquals(QualityGateDecisionResult.Code.RECORDED,
                fixture.store.recordQualityGateDecision(decision).code());
        assertEquals(QualityGateDecisionResult.Code.ALREADY_RECORDED,
                fixture.store.recordQualityGateDecision(decision).code());
        assertEquals(QualityGateDecisionResult.Code.DECISION_CONFLICT,
                fixture.store.recordQualityGateDecision(fixture.decision(
                        QualityGatePolicy.allowAll(), 8L, 2L)).code());
        assertEquals(decision,
                fixture.store.qualityGateDecision(fixture.fence));
        assertEquals(ManifestMemberDecision.Code.MEMBER_CONFLICT,
                fixture.store.recordManifestMember(
                        fixture.changedMember(0, "changed-valid")).code());

        ManifestCommitJournalSnapshot decided = fixture.store
                .manifestCommitJournal(fixture.fence);
        assertEquals(decision, decided.qualityDecision());
        assertNull(decided.intent());
        ManifestCommitIntent intent = decided.buildIntent();
        assertTrue(intent.hasQualityDecision());
        assertEquals(decision, intent.qualityDecision());
        assertArrayEquals(decision.policyDigest(), intent.policyDigest());
        assertArrayEquals(QualityGateDecisionCodec.digest(decision),
                intent.qualityDecisionDigest());
        assertEquals(ManifestIntentDecision.Code.COMMITTED,
                fixture.store.commitManifestIntent(intent).code());
        assertEquals(ManifestIntentDecision.Code.ALREADY_COMMITTED,
                fixture.store.commitManifestIntent(intent).code());
        assertEquals(decision, fixture.store
                .manifestCommitJournal(fixture.fence).qualityDecision());
    }

    @Test
    void reservedQualityClosureCannotBypassDecisionWithLegacyIntent() {
        Fixture fixture = fixture("run-quality-no-legacy-bypass");
        fixture.sealAll();
        fixture.advanceToCommitting(null);
        ManifestCommitIntent legacy = fixture.store
                .manifestCommitJournal(fixture.fence).buildIntent();
        assertTrue(!legacy.hasQualityDecision());

        assertEquals(ManifestIntentDecision.Code.INTENT_CONFLICT,
                fixture.store.commitManifestIntent(legacy).code());
        assertNull(fixture.store
                .manifestCommitJournal(fixture.fence).intent());
    }

    @Test
    void trustedQualityAssignmentRejectsWrongPolicyAndRelabelledLocator() {
        Fixture wrongPolicy = fixture("run-quality-wrong-policy");
        List<byte[]> assignmentShards = new ArrayList<>();
        for (int i = 0; i < wrongPolicy.manifest.shardCount(); i++) {
            assignmentShards.add(StageAssignmentManifestCodec.encodeShard(
                    wrongPolicy.manifest, i));
        }
        StageAssignmentManifest decoded = StageAssignmentManifestCodec.decode(
                StageAssignmentManifestCodec.encodeRoot(wrongPolicy.manifest),
                assignmentShards);
        assertEquals(wrongPolicy.manifest.qualityGateBinding(),
                decoded.qualityGateBinding());
        wrongPolicy.sealAll();
        wrongPolicy.advanceToCommitting(null);
        QualityGateDecision differentPolicy = wrongPolicy.decision(
                QualityGatePolicy.builder().maxRejectedRows(0L).build(), 1L, 0L);
        assertEquals(QualityGateDecisionResult.Code.IDENTITY_CONFLICT,
                wrongPolicy.store.recordQualityGateDecision(differentPolicy).code());

        Fixture relabelled = fixture("run-quality-relabelled-locator");
        assertEquals(ManifestMemberDecision.Code.ASSIGNMENT_CONFLICT,
                relabelled.store.recordManifestMember(
                        relabelled.receiptWithRole(0, "ordinary.valid")).code());
        assertNull(relabelled.store.manifestCommitJournal(relabelled.fence).member(0));
    }

    @Test
    void qualityAssignmentRootUsesClosedCanonicalVersionedWire() {
        Fixture fixture = fixture("run-quality-root-wire");
        StageAssignmentManifest ordinary = new StageAssignmentManifest(
                fixture.fence, fixture.artifactId, fixture.manifest.generation(),
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                fixture.manifest.assignments());
        byte[] ordinaryRoot = StageAssignmentManifestCodec.encodeRoot(ordinary);
        byte[] qualityRoot = StageAssignmentManifestCodec.encodeRoot(fixture.manifest);
        byte[] shard = StageAssignmentManifestCodec.encodeShard(fixture.manifest, 0);
        byte[] acceptance = StageAssignmentManifestCodec.encodeAcceptanceShard(
                fixture.assignments, 0);
        byte[] terminal = StageAssignmentManifestCodec.encodeTerminalShard(
                fixture.assignments, 0);
        assertEquals(3, frameVersion(ordinaryRoot));
        assertEquals(5, frameVersion(qualityRoot));
        assertEquals(3, frameVersion(shard));
        assertEquals(5, frameVersion(terminal));

        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decode(
                        rewriteFrameVersion(qualityRoot, 4),
                        Collections.singletonList(shard)));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decode(
                        rewriteFrameVersion(qualityRoot, 6),
                        Collections.singletonList(shard)));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decode(
                        qualityRoot, Collections.singletonList(
                                rewriteFrameVersion(shard, 4))));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decodeSnapshot(
                        qualityRoot, Collections.singletonList(shard),
                        Collections.singletonList(acceptance),
                        Collections.singletonList(rewriteFrameVersion(terminal, 6))));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decode(
                        v5WithoutBinding(qualityRoot,
                                fixture.manifest.qualityGateBinding()),
                        Collections.singletonList(shard)));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decode(
                        reorderedQualityBindings(qualityRoot,
                                fixture.manifest.qualityGateBinding()),
                        Collections.singletonList(shard)));
    }

    @Test
    void decisionOnlyAndCommittedQualitySlotRoundTripWithoutLosingOriginalDecisionBytes() {
        Fixture fixture = fixture("run-quality-slot-codec");
        fixture.sealAll();
        fixture.advanceToCommitting(null);
        QualityGateDecision decision = fixture.decision(QualityGatePolicy.allowAll(), 1L, 0L);
        ManifestCommitJournalSnapshot complete = fixture.store
                .manifestCommitJournal(fixture.fence);
        byte[] decisionBytes = QualityGateDecisionCodec.encode(decision);
        byte[] decidedSlot = ManifestCommitJournalCodec.encodeQualityDecision(decision);
        ManifestCommitJournalSnapshot decided = ManifestCommitJournalCodec.decodeSnapshot(
                complete.assignments(), memberShards(complete), decidedSlot);
        assertEquals(decision, decided.qualityDecision());
        assertNull(decided.intent());

        ManifestCommitIntent qualityIntent = decided.buildIntent();
        byte[] committedSlot = ManifestCommitJournalCodec.encodeIntent(qualityIntent);
        assertTrue(committedSlot.length
                <= ManifestCommitJournalCodec.MAX_QUALITY_INTENT_FRAME_BYTES);
        assertTrue(indexOf(committedSlot, decisionBytes) >= 0);
        ManifestCommitJournalSnapshot committed = ManifestCommitJournalCodec.decodeSnapshot(
                complete.assignments(), memberShards(complete), committedSlot);
        assertEquals(decision, committed.qualityDecision());
        assertEquals(qualityIntent, committed.intent());
        assertArrayEquals(decisionBytes,
                committed.intent().qualityDecisionBytes());
    }

    @Test
    void qualitySlotRejectsUnknownInconsistentCorruptTruncatedTrailingAndOversizedFrames() {
        Fixture fixture = fixture("run-quality-slot-negative");
        fixture.sealAll();
        fixture.advanceToCommitting(null);
        QualityGateDecision decision = fixture.decision(
                QualityGatePolicy.allowAll(), 1L, 0L);
        ManifestCommitJournalSnapshot complete = fixture.store
                .manifestCommitJournal(fixture.fence);
        List<byte[]> shards = memberShards(complete);
        byte[] slot = ManifestCommitJournalCodec.encodeQualityDecision(decision);
        int decisionLength = ByteBuffer.wrap(slot, 13, 4).getInt();
        int policyDigestOffset = 12 + 1 + 4 + decisionLength;

        byte[] unknownVersion = slot.clone();
        unknownVersion[4] = 0x7f;
        unknownVersion[5] = 0x7f;
        assertSlotRejected(complete, shards, rechecksum(unknownVersion));

        byte[] unknownState = slot.clone();
        unknownState[12] = 99;
        assertSlotRejected(complete, shards, rechecksum(unknownState));

        byte[] outcomeMismatch = slot.clone();
        outcomeMismatch[12] = 2;
        assertSlotRejected(complete, shards, rechecksum(outcomeMismatch));

        byte[] policyDigestTamper = slot.clone();
        policyDigestTamper[policyDigestOffset] ^= 1;
        assertSlotRejected(complete, shards, rechecksum(policyDigestTamper));

        byte[] decisionDigestTamper = slot.clone();
        decisionDigestTamper[policyDigestOffset + 32] ^= 1;
        assertSlotRejected(complete, shards, rechecksum(decisionDigestTamper));

        assertSlotRejected(complete, shards, Arrays.copyOf(slot, slot.length - 1));
        assertSlotRejected(complete, shards, Arrays.copyOf(slot, slot.length + 1));
        assertSlotRejected(complete, shards, new byte[
                ManifestCommitJournalCodec.MAX_QUALITY_INTENT_FRAME_BYTES + 1]);
    }

    @Test
    void conflictingIdentityEvidenceDecisionAbortAndMissingClosureFailClosed() {
        Fixture fixture = fixture("run-quality-conflict");
        fixture.seal(0);
        fixture.advanceToCommitting(null);
        QualityGateDecision incomplete = fixture.decision(
                QualityGatePolicy.allowAll(), 1L, 0L);
        assertEquals(QualityGateDecisionResult.Code.INCOMPLETE_MEMBERS,
                fixture.store.recordQualityGateDecision(incomplete).code());

        fixture.seal(1);
        fixture.seal(2);
        QualityGateDecision wrongArtifact = QualityGateDecision.evaluate(
                "other-artifact", fixture.fence, fixture.publicationId,
                QualityGatePolicy.allowAll(), fixture.evidence(1L, 0L));
        assertEquals(QualityGateDecisionResult.Code.IDENTITY_CONFLICT,
                fixture.store.recordQualityGateDecision(wrongArtifact).code());

        QualityGateDecision wrongEvidence = QualityGateDecision.evaluate(
                fixture.artifactId, fixture.fence, fixture.publicationId,
                QualityGatePolicy.allowAll(), fixture.changedEvidence(1L, 0L));
        assertEquals(QualityGateDecisionResult.Code.EVIDENCE_CONFLICT,
                fixture.store.recordQualityGateDecision(wrongEvidence).code());

        BatchSinkAbortRecord abort = fixture.abort(
                digest(99), "PREPUBLICATION_FAILURE", 60L);
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbort(abort).code());
        assertEquals(QualityGateDecisionResult.Code.ABORT_ALREADY_DECIDED,
                fixture.store.recordQualityGateDecision(
                        fixture.decision(QualityGatePolicy.allowAll(), 1L, 0L)).code());
    }

    @Test
    void failDecisionCanOnlyBecomeMatchingQualityAbortAndCanNeverCommit() {
        QualityGatePolicy rejecting = QualityGatePolicy.builder()
                .maxRejectedRows(0L).build();
        Fixture fixture = fixture("run-quality-fail", rejecting);
        fixture.sealAll();
        fixture.advanceToCommitting(null);
        QualityGateDecision failed = fixture.decision(rejecting, 9L, 1L);
        assertEquals(QualityGateDecision.Outcome.FAIL, failed.outcome());
        assertEquals(QualityGateDecisionResult.Code.RECORDED,
                fixture.store.recordQualityGateDecision(failed).code());
        assertThrows(IllegalStateException.class, () -> fixture.store
                .manifestCommitJournal(fixture.fence).buildIntent());

        assertEquals(BatchSinkAbortDecision.Code.RECORD_CONFLICT,
                fixture.store.recordBatchSinkAbort(fixture.abort(
                        digest(88), "QUALITY_GATE_REJECTED", 60L)).code());
        assertEquals(BatchSinkAbortDecision.Code.RECORD_CONFLICT,
                fixture.store.recordBatchSinkAbort(fixture.abort(
                        QualityGateDecisionCodec.digest(failed),
                        "PREPUBLICATION_FAILURE", 60L)).code());
        BatchSinkAbortRecord exact = fixture.abort(
                QualityGateDecisionCodec.digest(failed),
                "QUALITY_GATE_REJECTED", 60L);
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbort(exact).code());
        assertEquals(BatchSinkAbortDecision.Code.ALREADY_RECORDED,
                fixture.store.recordBatchSinkAbort(exact).code());
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbort(exact.completed(80L)).code());

        RunSnapshot current = fixture.store.get(fixture.run.runId());
        RunDecision reconciling = fixture.store.apply(RunCommand.requestCancellation(
                "reconcile-quality-fail", current.runId(), current.revision(),
                current.currentAttempt().attemptId(),
                current.currentAttempt().fenceToken(),
                io.rowweave.run.CancellationCause.FENCE, 85L,
                "QUALITY_GATE_REJECTED", "quality rejected"));
        assertEquals(RunDecision.Code.ACCEPTED, reconciling.code());
        current = reconciling.snapshot();
        RunDecision terminal = fixture.store.apply(RunCommand.advanceAttempt(
                "terminal-quality-fail", current.runId(), current.revision(),
                current.currentAttempt().attemptId(),
                current.currentAttempt().fenceToken(), AttemptStatus.FAILED,
                false, 90L, "QUALITY_GATE_REJECTED", "quality rejected"));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        assertEquals(AttemptStatus.FAILED,
                terminal.snapshot().currentAttempt().status());
        assertEquals("QUALITY_GATE_REJECTED",
                terminal.snapshot().currentAttempt().reasonCode());
        assertEquals(failed, fixture.store.qualityGateDecision(fixture.fence));
    }

    @Test
    void passDecisionStillAllowsOrdinaryPrepublicationAbort() {
        Fixture fixture = fixture("run-quality-pass-abort");
        fixture.sealAll();
        fixture.advanceToCommitting(null);
        QualityGateDecision passed = fixture.decision(
                QualityGatePolicy.allowAll(), 1L, 0L);
        assertEquals(QualityGateDecisionResult.Code.RECORDED,
                fixture.store.recordQualityGateDecision(passed).code());
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbort(fixture.abort(
                        digest(77), "PREPUBLICATION_FAILURE", 60L)).code());
    }

    @Test
    void recoveryOwnerFencesDecisionWriteButExactReplayAndTerminalQuerySurvive() {
        Fixture fixture = fixture("run-quality-recovered");
        fixture.sealAll();
        RecoveryClaim owner = fixture.store.claim(
                fixture.run.runId(), fixture.run.revision(), 0L,
                new RecoveryOwner("master-quality", 20L));
        fixture.advanceToCommitting(owner.token());
        QualityGateDecision decision = fixture.decision(
                QualityGatePolicy.allowAll(), 2L, 0L);

        assertEquals(QualityGateDecisionResult.Code.RECOVERY_OWNED,
                fixture.store.recordQualityGateDecision(decision).code());
        assertEquals(QualityGateDecisionResult.Code.RECORDED,
                fixture.store.recordQualityGateDecisionRecovered(
                        owner.token(), decision).code());
        assertEquals(QualityGateDecisionResult.Code.ALREADY_RECORDED,
                fixture.store.recordQualityGateDecision(decision).code());

        RunSnapshot current = fixture.store.get(fixture.run.runId());
        RunDecision terminal = fixture.store.applyRecovered(owner.token(),
                RunCommand.advanceAttempt(
                        "terminal-quality", current.runId(), current.revision(),
                        current.currentAttempt().attemptId(),
                        current.currentAttempt().fenceToken(), AttemptStatus.SUCCEEDED,
                        false, 90L, "SUCCEEDED", "quality committed",
                        RunResultSummary.manifest(0L, 71L, digest(3))));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        assertEquals(decision, fixture.store.qualityGateDecision(fixture.fence));
    }

    private static List<byte[]> memberShards(ManifestCommitJournalSnapshot journal) {
        List<byte[]> shards = new ArrayList<>();
        for (int i = 0; i < journal.assignments().manifest().shardCount(); i++) {
            shards.add(ManifestCommitJournalCodec.encodeMemberShard(journal, i));
        }
        return shards;
    }

    private static void assertSlotRejected(
            ManifestCommitJournalSnapshot complete, List<byte[]> shards, byte[] slot) {
        assertThrows(IllegalArgumentException.class,
                () -> ManifestCommitJournalCodec.decodeSnapshot(
                        complete.assignments(), shards, slot));
    }

    private static byte[] rechecksum(byte[] frame) {
        byte[] digest = sha256(Arrays.copyOf(frame, frame.length - 32));
        System.arraycopy(digest, 0, frame, frame.length - 32, digest.length);
        return frame;
    }

    private static int frameVersion(byte[] frame) {
        return ((frame[4] & 0xff) << 8) | (frame[5] & 0xff);
    }

    private static byte[] rewriteFrameVersion(byte[] frame, int version) {
        byte[] copy = frame.clone();
        copy[4] = (byte) (version >>> 8);
        copy[5] = (byte) version;
        return rechecksum(copy);
    }

    private static byte[] v5WithoutBinding(
            byte[] root, QualityGateAssignmentBinding binding) {
        int policyStart = indexOf(root, binding.policyDigest());
        if (policyStart < 1) throw new AssertionError("policy digest missing from root");
        int bindingBytes = 32 + 4;
        for (QualityGateAssignmentBinding.MemberBinding member : binding.members()) {
            bindingBytes += 4 + 4
                    + member.role().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                    + 32;
        }
        int marker = policyStart - 1;
        int oldChecksum = root.length - 32;
        int sourceTail = policyStart + bindingBytes;
        byte[] stripped = new byte[root.length - bindingBytes];
        System.arraycopy(root, 0, stripped, 0, marker + 1);
        stripped[marker] = 0;
        System.arraycopy(root, sourceTail, stripped, marker + 1,
                oldChecksum - sourceTail);
        ByteBuffer.wrap(stripped).putInt(8,
                ByteBuffer.wrap(root).getInt(8) - bindingBytes);
        return rechecksum(stripped);
    }

    private static byte[] reorderedQualityBindings(
            byte[] root, QualityGateAssignmentBinding binding) {
        int policyStart = indexOf(root, binding.policyDigest());
        int first = policyStart + 32 + 4;
        int firstLength = bindingChunkLength(root, first);
        int second = first + firstLength;
        int secondLength = bindingChunkLength(root, second);
        byte[] reordered = root.clone();
        System.arraycopy(root, second, reordered, first, secondLength);
        System.arraycopy(root, first, reordered, first + secondLength, firstLength);
        return rechecksum(reordered);
    }

    private static int bindingChunkLength(byte[] root, int offset) {
        int roleLength = ByteBuffer.wrap(root, offset + 4, 4).getInt();
        return 4 + 4 + roleLength + 32;
    }

    private static Fixture fixture(String runId) {
        return fixture(runId, QualityGatePolicy.allowAll());
    }

    private static Fixture fixture(String runId, QualityGatePolicy expectedPolicy) {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact-quality",
                1, 0L, 1L)).snapshot();
        RunSnapshot run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.revision(),
                "attempt-1", "publication-quality", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(run.runId(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken());
        List<StageAssignmentRecord> records = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, 100L + i, i, 0, StageAssignmentRole.SINK,
                    "worker-" + i, "127.0.0.1", 7001 + i, -1, "inc-" + i));
        }
        List<QualityGateAssignmentBinding.MemberBinding> memberBindings =
                new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            memberBindings.add(new QualityGateAssignmentBinding.MemberBinding(
                    i, ROLES.get(i), BatchSinkDescriptorCodec.digest(
                    Fixture.sinkDescriptor(i, expectedPolicy))));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1, records,
                new QualityGateAssignmentBinding(
                        QualityGatePolicyCodec.digest(expectedPolicy), memberBindings));
        StageAssignmentSnapshot assignments = store.publish(manifest).snapshot();
        for (int i = 0; i < 3; i++) {
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    assignments.token(i), digest(i + 1), 10L + i);
            store.accept(acceptance);
            store.recordTerminal(StageTerminalReceipt.succeeded(
                    acceptance.token(), acceptance.commandDigest(), i, i, 20L + i));
        }
        return new Fixture(store, run, fence, manifest,
                store.assignmentSnapshot(fence), expectedPolicy);
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) value);
        return digest;
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) continue outer;
            }
            return i;
        }
        return -1;
    }

    private static final class Fixture {
        final InMemoryRunStateStore store;
        final RunSnapshot run;
        final RunAttemptFence fence;
        final StageAssignmentManifest manifest;
        final StageAssignmentSnapshot assignments;
        final String artifactId = "artifact-quality";
        final String publicationId = "publication-quality";
        final List<ManifestMemberReceipt> receipts = new ArrayList<>();
        final QualityGatePolicy expectedPolicy;

        Fixture(InMemoryRunStateStore store, RunSnapshot run,
                RunAttemptFence fence, StageAssignmentManifest manifest,
                StageAssignmentSnapshot assignments,
                QualityGatePolicy expectedPolicy) {
            this.store = store;
            this.run = run;
            this.fence = fence;
            this.manifest = manifest;
            this.assignments = assignments;
            this.expectedPolicy = expectedPolicy;
            for (int i = 0; i < 3; i++) receipts.add(receipt(i, "member-" + i));
        }

        void seal(int ordinal) {
            assertEquals(ManifestMemberDecision.Code.RECORDED,
                    store.recordManifestMember(receipts.get(ordinal)).code());
        }

        void sealAll() {
            for (int i = 0; i < 3; i++) seal(i);
        }

        ManifestMemberReceipt changedMember(int ordinal, String name) {
            return receipt(ordinal, name);
        }

        ManifestMemberReceipt receiptWithRole(int ordinal, String role) {
            return receipt(ordinal, "member-" + ordinal, role);
        }

        QualityPreparedEvidence evidence(long accepted, long rejected) {
            return new QualityPreparedEvidence(accepted, rejected,
                    Collections.emptyMap(), Collections.emptyList(), memberEvidence(receipts));
        }

        QualityPreparedEvidence changedEvidence(long accepted, long rejected) {
            List<ManifestMemberReceipt> changed = new ArrayList<>(receipts);
            changed.set(0, changedMember(0, "different"));
            return new QualityPreparedEvidence(accepted, rejected,
                    Collections.emptyMap(), Collections.emptyList(), memberEvidence(changed));
        }

        QualityGateDecision decision(
                QualityGatePolicy policy, long accepted, long rejected) {
            return QualityGateDecision.evaluate(artifactId, fence, publicationId,
                    policy, evidence(accepted, rejected));
        }

        BatchSinkAbortRecord abort(byte[] identity, String reason, long decidedAt) {
            return BatchSinkAbortRecord.decided(
                    fence, manifest.generation(),
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    "domain-quality", 71L, publicationId,
                    identity, reason, decidedAt);
        }

        void advanceToCommitting(RecoveryToken token) {
            for (AttemptStatus next : Arrays.asList(
                    AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                    AttemptStatus.COMMITTING)) {
                RunSnapshot current = store.get(run.runId());
                RunCommand command = RunCommand.advanceAttempt(
                        "advance-" + next + '-' + run.runId(), current.runId(),
                        current.revision(), current.currentAttempt().attemptId(),
                        current.currentAttempt().fenceToken(), next, false,
                        40L + next.ordinal(), next.name(), next.name());
                RunDecision decision = token == null
                        ? store.apply(command) : store.applyRecovered(token, command);
                assertEquals(RunDecision.Code.ACCEPTED, decision.code());
            }
        }

        private ManifestMemberReceipt receipt(int ordinal, String name) {
            return receipt(ordinal, name, ROLES.get(ordinal));
        }

        private ManifestMemberReceipt receipt(
                int ordinal, String name, String locatorRole) {
            CommitManifest.Member member = new CommitManifest.Member(
                    name, "data/71/" + name + ".data", "schema-" + ordinal,
                    10L + ordinal, digest(20 + ordinal));
            BatchSinkDescriptor sink = sinkDescriptor(ordinal, expectedPolicy);
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    manifest, manifest.assignment(ordinal), locatorRole, sink);
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    "domain-quality", ManifestCommitWire.encodeMember(member),
                    member.length(), member.sha256(), 30L + ordinal);
            return ManifestMemberReceipt.sealed(
                    assignments.acceptance(ordinal).token(),
                    assignments.acceptance(ordinal).commandDigest(),
                    "domain-quality", 71L, publicationId,
                    member, 30L + ordinal, locator);
        }

        private static List<QualityPreparedMemberEvidence> memberEvidence(
                List<ManifestMemberReceipt> receipts) {
            List<QualityPreparedMemberEvidence> members = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                ManifestMemberReceipt receipt = receipts.get(i);
                members.add(new QualityPreparedMemberEvidence(
                        ROLES.get(i), receipt.member().name(),
                        receipt.member().length(), receipt.member().sha256(),
                        sha256(PreparedSinkLocatorCodec.encode(
                                receipt.preparedLocator()))));
            }
            return members;
        }

        private static String repeat(char value, int count) {
            char[] chars = new char[count];
            Arrays.fill(chars, value);
            return new String(chars);
        }

        private static BatchSinkDescriptor sinkDescriptor(
                int ordinal, QualityGatePolicy policy) {
            return new BatchSinkDescriptor(
                    1, "rowweave.manifest-file", 1, repeat('0', 64),
                    "quality-member-" + ordinal, "domain-quality",
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    "quality-gate-v1:" + ROLES.get(ordinal) + ':'
                            + hex(QualityGatePolicyCodec.digest(policy)));
        }

        private static String hex(byte[] bytes) {
            StringBuilder out = new StringBuilder(bytes.length * 2);
            char[] alphabet = "0123456789abcdef".toCharArray();
            for (byte value : bytes) {
                out.append(alphabet[(value >>> 4) & 0xf])
                        .append(alphabet[value & 0xf]);
            }
            return out.toString();
        }
    }
}
