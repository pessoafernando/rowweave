package io.rowweave.incremental;

import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded O(1) frontier authority record and recovery fence. */
class IncrementalAuthorityRecordContractTest {

    @Test
    void readyActiveCommittedAndBackfillRecordsRoundTripCanonically() throws Exception {
        IncrementalAuthorityRecord ready = readyRecord();
        IncrementalActiveInterval normal = active(normalReceipt(), 1, null);
        IncrementalAuthorityRecord active = record(
                1L, IncrementalFrontierAuthorityState.ACTIVE, normal,
                IncrementalFrontier.begin(), null, 0L, null, null);
        IncrementalActiveInterval decided = active(
                normalReceipt(), 1, sha256("normal-decision"));
        IncrementalCommitReceipt normalCommit = IncrementalCommitReceipt.committed(
                decided, "publication-normal", 7L, sha256("manifest-normal"),
                "commit-normal", sha256("command-normal"));
        IncrementalAuthorityRecord committed = record(
                2L, IncrementalFrontierAuthorityState.READY, null,
                IncrementalFrontier.int64Sequence(10L), normalCommit, 0L, null, null);

        IncrementalActiveInterval backfill = active(
                backfillReceipt(), 2, sha256("backfill-decision"), 2L);
        IncrementalCommitReceipt backfillCommit = IncrementalCommitReceipt.committed(
                backfill, "publication-backfill", 8L, sha256("manifest-backfill"),
                "complete-backfill", sha256("command-backfill"));
        IncrementalAuthorityRecord afterBackfill = record(
                4L, IncrementalFrontierAuthorityState.READY, null,
                IncrementalFrontier.int64Sequence(10L), normalCommit,
                4L, backfillCommit, null);

        IncrementalAuthorityRecord[] records = {ready, active, committed, afterBackfill};
        for (IncrementalAuthorityRecord value : records) {
            byte[] frame = IncrementalAuthorityRecordCodec.encode(value);
            assertTrue(frame.length <= IncrementalAuthorityRecordCodec.MAX_FRAME_BYTES);
            IncrementalAuthorityRecord decoded = IncrementalAuthorityRecordCodec.decode(frame);
            assertEquals(value, decoded);
            assertArrayEquals(frame, IncrementalAuthorityRecordCodec.encode(decoded));
        }
        assertNull(ready.activeInterval());
        assertEquals(4L, afterBackfill.lastCompletedBackfillOrdinal());
        assertEquals(backfillCommit, afterBackfill.lastBackfillReceipt());
    }

    @Test
    void recordStateAndReceiptSlotsFailClosed() throws Exception {
        IncrementalActiveInterval active = active(normalReceipt(), 1, null);
        IncrementalCommitReceipt normal = IncrementalCommitReceipt.committed(
                active(normalReceipt(), 1, sha256("decision")),
                "publication", 1L, sha256("manifest"), "command", sha256("command"));
        IncrementalActiveInterval backfillActive = active(
                backfillReceipt(), 1, sha256("backfill-decision"));
        IncrementalCommitReceipt backfill = IncrementalCommitReceipt.committed(
                backfillActive, "backfill-publication", 2L, sha256("backfill-manifest"),
                "backfill-command", sha256("backfill-command"));

        assertThrows(IllegalArgumentException.class, () -> record(
                2L, IncrementalFrontierAuthorityState.READY, active,
                IncrementalFrontier.begin(), null, 0L, null, null));
        assertThrows(IllegalArgumentException.class, () -> record(
                2L, IncrementalFrontierAuthorityState.ACTIVE, null,
                IncrementalFrontier.begin(), null, 0L, null, null));
        assertThrows(IllegalArgumentException.class, () -> record(
                2L, IncrementalFrontierAuthorityState.CORRUPT_OR_CONFLICT, active,
                IncrementalFrontier.begin(), null, 0L, null, null));
        assertThrows(IllegalArgumentException.class, () -> record(
                2L, IncrementalFrontierAuthorityState.READY, null,
                IncrementalFrontier.begin(), backfill, 0L, null, null));
        assertThrows(IllegalArgumentException.class, () -> record(
                2L, IncrementalFrontierAuthorityState.READY, null,
                IncrementalFrontier.begin(), normal, 4L, normal, null));
        assertThrows(IllegalArgumentException.class, () -> record(
                2L, IncrementalFrontierAuthorityState.READY, null,
                IncrementalFrontier.begin(), normal, 0L, backfill, null));
        assertThrows(IllegalArgumentException.class, () -> record(
                2L, IncrementalFrontierAuthorityState.READY, null,
                IncrementalFrontier.begin(), null, 0L, null, sha256("conflict")));
        IncrementalAuthorityRecord frozen = record(
                2L, IncrementalFrontierAuthorityState.CORRUPT_OR_CONFLICT, active,
                IncrementalFrontier.begin(), null, 0L, null, sha256("conflict"));
        assertEquals(active, frozen.activeInterval(),
                "conflict must freeze rather than clear the active claim");
    }

    @Test
    void activeIntervalBindsFrozenCaptureArtifactRunAttemptAndDecision() throws Exception {
        CapturedTargetReceipt capture = normalReceipt();
        IncrementalActiveInterval claimed = active(capture, 1, null);

        assertEquals(IncrementalIntervalIdentity.from(capture), claimed.identity());
        assertEquals(capture, claimed.capturedTarget());
        assertEquals("artifact", claimed.artifactId());
        assertEquals(0L, claimed.claimedRevision());
        assertEquals(new RunAttemptFence(
                claimed.identity().candidateRunId(), "attempt-1", 1L), claimed.fence());
        assertEquals(IncrementalAttemptCommitPhase.CLAIMED, claimed.phase());
        assertNull(claimed.commitDecisionDigest());

        IncrementalActiveInterval decided = active(capture, 1, sha256("decision"));
        assertEquals(IncrementalAttemptCommitPhase.COMMIT_DECIDED, decided.phase());
        assertArrayEquals(sha256("decision"), decided.commitDecisionDigest());
        byte[] leaked = decided.commitDecisionDigest();
        leaked[0] ^= 1;
        assertArrayEquals(sha256("decision"), decided.commitDecisionDigest());

        IncrementalIntervalIdentity identity = IncrementalIntervalIdentity.from(capture);
        assertThrows(IllegalArgumentException.class, () -> new IncrementalActiveInterval(
                identity, capture, "artifact", 0L,
                new RunAttemptFence("wrong-run", "a", 1L),
                1, IncrementalAttemptCommitPhase.CLAIMED, null));
        assertThrows(IllegalArgumentException.class, () -> new IncrementalActiveInterval(
                identity, capture, "artifact", 0L, new RunAttemptFence(
                        identity.candidateRunId(), "a", 1L), 4,
                IncrementalAttemptCommitPhase.CLAIMED, null));
        assertThrows(IllegalArgumentException.class, () -> new IncrementalActiveInterval(
                identity, capture, "artifact", 0L, new RunAttemptFence(
                        identity.candidateRunId(), "a", 1L), 1,
                IncrementalAttemptCommitPhase.COMMIT_DECIDED, null));
        assertThrows(IllegalArgumentException.class, () -> new IncrementalActiveInterval(
                identity, capture, "artifact", 0L, new RunAttemptFence(
                        identity.candidateRunId(), "a", 1L), 1,
                IncrementalAttemptCommitPhase.CLAIMED, sha256("unexpected")));
        assertThrows(IllegalArgumentException.class, () -> new IncrementalActiveInterval(
                identity, capture, "artifact", 1L, new RunAttemptFence(
                        identity.candidateRunId(), "a", 1L), 1,
                IncrementalAttemptCommitPhase.CLAIMED, null));
    }

    @Test
    void commitReceiptBindsExactIntervalFenceVisibilityIntentAndCommand() throws Exception {
        IncrementalActiveInterval decided = active(
                normalReceipt(), 2, sha256("decision"));
        IncrementalCommitReceipt receipt = IncrementalCommitReceipt.committed(
                decided, "publication", 11L, sha256("manifest"),
                "command", sha256("command-body"));

        assertEquals(decided.identity().intervalId(), receipt.intervalId());
        assertEquals(0L, receipt.claimedRevision());
        assertEquals(decided.capturedTarget().from(), receipt.from());
        assertEquals(decided.capturedTarget().target(), receipt.to());
        assertArrayEquals(decided.capturedTarget().definitionDigest(),
                receipt.definitionFingerprint());
        assertEquals(decided.fence(), receipt.fence());
        assertEquals("publication", receipt.publicationId());
        assertEquals(11L, receipt.generation());
        assertArrayEquals(sha256("manifest"), receipt.manifestHash());
        assertArrayEquals(sha256("decision"), receipt.commitDecisionDigest());
        assertEquals("command", receipt.commandId());
        assertArrayEquals(sha256("command-body"), receipt.commandDigest());

        assertThrows(IllegalArgumentException.class,
                () -> IncrementalCommitReceipt.committed(
                        active(normalReceipt(), 1, null), "publication", 1L,
                        sha256("manifest"), "command", sha256("command")));
    }

    @Test
    void recoveryTokenCanonicallyBindsEveryAuthorityObservation() throws Exception {
        FeedGuardShardObservation guard = new FeedGuardShardObservation(
                17, 23L, sha256("guard-shard"));
        RecoveryOwner owner = new RecoveryOwner("master-a", 91L);
        FrontierRecoveryToken token = FrontierRecoveryToken.create(
                "feed", owner, 4L, 12L, 7L, guard, 101L);
        byte[] encoded = FrontierRecoveryTokenCodec.encode(token);

        assertEquals(token, FrontierRecoveryTokenCodec.decode(encoded));
        assertArrayEquals(encoded, FrontierRecoveryTokenCodec.encode(
                FrontierRecoveryTokenCodec.decode(encoded)));
        assertEquals(32, token.tokenDigest().length);
        assertNotEquals(token, FrontierRecoveryToken.create(
                "feed", owner, 5L, 12L, 7L, guard, 101L));
        assertNotEquals(token, FrontierRecoveryToken.create(
                "feed", owner, 4L, 13L, 7L, guard, 101L));
        assertNotEquals(token, FrontierRecoveryToken.create(
                "feed", owner, 4L, 12L, 8L, guard, 101L));
        assertNotEquals(token, FrontierRecoveryToken.create(
                "feed", owner, 4L, 12L, 7L,
                new FeedGuardShardObservation(17, 24L, sha256("guard-shard")), 101L));
        assertNotEquals(token, FrontierRecoveryToken.create(
                "feed", owner, 4L, 12L, 7L, guard, 102L));
    }

    @Test
    void recoveryTokenWireSupportsAuthorityBackendModulesWithoutExposingConstructors()
            throws Exception {
        assertTrue(Modifier.isPublic(FrontierRecoveryTokenCodec.class.getModifiers()));
        assertTrue(Modifier.isPublic(FrontierRecoveryTokenCodec.class
                .getDeclaredMethod("encode", FrontierRecoveryToken.class).getModifiers()));
        assertTrue(Modifier.isPublic(FrontierRecoveryTokenCodec.class
                .getDeclaredMethod("decode", byte[].class).getModifiers()));
        for (java.lang.reflect.Constructor<?> constructor
                : FrontierRecoveryToken.class.getDeclaredConstructors()) {
            assertFalse(Modifier.isPublic(constructor.getModifiers()));
        }
        Method decoded = FrontierRecoveryToken.class.getDeclaredMethod(
                "decoded", String.class, RecoveryOwner.class, long.class, long.class,
                long.class, FeedGuardShardObservation.class, long.class, byte[].class);
        assertFalse(Modifier.isPublic(decoded.getModifiers()));
    }

    @Test
    void recordKeepsSourceAndFrontierContractsDistinctAndBindsClaimRevision() throws Exception {
        IncrementalAuthorityRecord ready = readyRecord();
        assertArrayEquals(sha256("source-contract"), ready.sourceContractDigest());
        assertArrayEquals(sha256("frontier-contract"), ready.frontierContractDigest());

        IncrementalActiveInterval active = active(normalReceipt(), 1, null);
        assertEquals(0L, active.claimedRevision());
        assertThrows(IllegalArgumentException.class, () -> new IncrementalAuthorityRecord(
                1L, "feed", 7L, sha256("definition"), sha256("wrong-source"),
                sha256("frontier-contract"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                IncrementalFrontier.begin(), active, null, 0L, null,
                IncrementalFrontierAuthorityState.ACTIVE, null));
        assertThrows(IllegalArgumentException.class, () -> record(
                0L, IncrementalFrontierAuthorityState.ACTIVE, active,
                IncrementalFrontier.begin(), null, 0L, null, null));

        CapturedTargetReceipt laterNormal = CapturedTargetReceipt.normal(
                7L, sha256("definition"), sha256("source-contract"), 5L,
                IncrementalFrontier.int64Sequence(10L), "later-normal",
                IncrementalRunMode.INCREMENTAL_V1,
                IncrementalFrontier.int64Sequence(11L), bytes("snapshot"));
        IncrementalActiveInterval exactNormal = active(laterNormal, 1, null, 5L);
        assertEquals(exactNormal, record(
                6L, IncrementalFrontierAuthorityState.ACTIVE, exactNormal,
                IncrementalFrontier.int64Sequence(10L), null, 0L, null, null)
                .activeInterval());
        assertThrows(IllegalArgumentException.class, () -> record(
                7L, IncrementalFrontierAuthorityState.ACTIVE, exactNormal,
                IncrementalFrontier.int64Sequence(10L), null, 0L, null, null));

        IncrementalActiveInterval exactBackfill = active(
                backfillReceipt(1L), 1, null, 5L);
        assertEquals(exactBackfill, record(
                6L, IncrementalFrontierAuthorityState.ACTIVE, exactBackfill,
                IncrementalFrontier.begin(), null, 0L, null, null).activeInterval());
        assertThrows(IllegalArgumentException.class, () -> record(
                7L, IncrementalFrontierAuthorityState.ACTIVE, exactBackfill,
                IncrementalFrontier.begin(), null, 0L, null, null));

        CapturedTargetReceipt jumped = CapturedTargetReceipt.backfill(
                7L, sha256("definition"), sha256("source-contract"), 4L,
                IncrementalFrontier.int64Sequence(2L),
                IncrementalFrontier.int64Sequence(5L),
                "jumped-backfill", bytes("snapshot"));
        assertThrows(IllegalArgumentException.class, () -> record(
                1L, IncrementalFrontierAuthorityState.ACTIVE,
                active(jumped, 1, null), IncrementalFrontier.begin(),
                null, 0L, null, null));

        CapturedTargetReceipt noOp = CapturedTargetReceipt.normal(
                7L, sha256("definition"), sha256("source-contract"), 4L,
                IncrementalFrontier.int64Sequence(10L), "no-op",
                IncrementalRunMode.INCREMENTAL_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        assertThrows(IllegalArgumentException.class, () -> record(
                5L, IncrementalFrontierAuthorityState.ACTIVE,
                active(noOp, 1, null), IncrementalFrontier.int64Sequence(10L),
                null, 0L, null, null));
    }

    @Test
    void largestLegalConstantCardinalityRecordFitsAndNestedLengthsFailBeforeAllocation()
            throws Exception {
        String maxId = repeat('x', IncrementalAuthorityValue.MAX_ID_BYTES);
        byte[] maxSnapshot = new byte[CapturedTargetReceipt.MAX_SNAPSHOT_IDENTITY_BYTES];
        IncrementalFrontier committed = maximumTimestampFrontier(20L);
        IncrementalFrontier target = maximumTimestampFrontier(21L);
        assertEquals(IncrementalFrontierCodec.MAX_PAYLOAD_BYTES - 12,
                committed.orderedKey().canonicalOrderBytes().length);
        CapturedTargetReceipt currentCapture = CapturedTargetReceipt.normal(
                7L, sha256("definition"), sha256("source-contract"), 4L,
                committed, "current",
                IncrementalRunMode.INCREMENTAL_V1,
                target, maxSnapshot);
        IncrementalIntervalIdentity currentIdentity = IncrementalIntervalIdentity.from(currentCapture);
        IncrementalActiveInterval current = new IncrementalActiveInterval(
                currentIdentity, currentCapture, maxId, 4L,
                new RunAttemptFence(currentIdentity.candidateRunId(), maxId, 3L),
                3, IncrementalAttemptCommitPhase.CLAIMED, null);

        CapturedTargetReceipt previousCapture = CapturedTargetReceipt.normal(
                7L, sha256("definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.begin(), "previous",
                IncrementalRunMode.SNAPSHOT_V1, committed, maxSnapshot);
        IncrementalActiveInterval previous = active(
                previousCapture, 1, sha256("normal-decision"), 0L);
        IncrementalCommitReceipt normal = IncrementalCommitReceipt.committed(
                previous, maxId, 1L, sha256("normal-manifest"), maxId, sha256("normal-command"));
        CapturedTargetReceipt previousBackfillCapture = CapturedTargetReceipt.backfill(
                7L, sha256("definition"), sha256("source-contract"), 1L,
                maximumTimestampFrontier(1L), maximumTimestampFrontier(2L),
                "previous-backfill", maxSnapshot);
        IncrementalActiveInterval previousBackfill = active(
                previousBackfillCapture, 1, sha256("backfill-decision"), 2L);
        IncrementalCommitReceipt backfill = IncrementalCommitReceipt.committed(
                previousBackfill, maxId, 2L, sha256("backfill-manifest"),
                maxId, sha256("backfill-command"));
        IncrementalAuthorityRecord maximum = new IncrementalAuthorityRecord(
                5L, maxId, 7L, sha256("definition"), sha256("source-contract"),
                sha256("frontier-contract"), IncrementalFrontier.Kind.TIMESTAMP_KEY_V1,
                committed, current, normal, 1L, backfill,
                IncrementalFrontierAuthorityState.ACTIVE, null);

        byte[] encoded = IncrementalAuthorityRecordCodec.encode(maximum);
        assertTrue(encoded.length <= IncrementalAuthorityRecordCodec.MAX_FRAME_BYTES);
        assertEquals(maximum, IncrementalAuthorityRecordCodec.decode(encoded));

        byte[] invalidLength = IncrementalAuthorityRecordCodec.encode(readyRecord());
        ByteBuffer.wrap(invalidLength).putInt(18, IncrementalAuthorityValue.MAX_ID_BYTES + 1);
        resignAuthorityFrame(invalidLength);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityRecordCodec.decode(invalidLength));

        byte[] invalidUtf8 = IncrementalAuthorityRecordCodec.encode(readyRecord());
        invalidUtf8[22] = (byte) 0xc3;
        invalidUtf8[23] = 0x28;
        resignAuthorityFrame(invalidUtf8);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityRecordCodec.decode(invalidUtf8));

        byte[] nonNfc = IncrementalAuthorityRecordCodec.encode(readyRecord());
        nonNfc[22] = 'e';
        nonNfc[23] = (byte) 0xcc;
        nonNfc[24] = (byte) 0x81;
        nonNfc[25] = 'x';
        resignAuthorityFrame(nonNfc);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityRecordCodec.decode(nonNfc));
    }

    @Test
    void codecsRejectCorruptUnknownAndOversizedFrames() throws Exception {
        byte[] authority = IncrementalAuthorityRecordCodec.encode(readyRecord());
        authority[authority.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityRecordCodec.decode(authority));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityRecordCodec.decode(
                        new byte[IncrementalAuthorityRecordCodec.MAX_FRAME_BYTES + 1]));

        byte[] token = FrontierRecoveryTokenCodec.encode(FrontierRecoveryToken.create(
                "feed", new RecoveryOwner("master", 1L), 1L, 1L, 1L,
                new FeedGuardShardObservation(0, 1L, sha256("guard")), 1L));
        token[4] = 99;
        assertThrows(IllegalArgumentException.class,
                () -> FrontierRecoveryTokenCodec.decode(token));
    }

    @Test
    void authorityRecordHasOnlyConstantCardinalityHistorySlots() {
        int receiptSlots = 0;
        for (Field field : IncrementalAuthorityRecord.class.getDeclaredFields()) {
            assertFalse(Collection.class.isAssignableFrom(field.getType()),
                    "authority record must not accumulate collections: " + field.getName());
            if (field.getType() == IncrementalCommitReceipt.class) receiptSlots++;
        }
        assertEquals(2, receiptSlots, "only last normal and last backfill receipts are retained");
    }

    private static IncrementalAuthorityRecord readyRecord() throws Exception {
        return record(0L, IncrementalFrontierAuthorityState.READY, null,
                IncrementalFrontier.begin(), null, 0L, null, null);
    }

    private static IncrementalAuthorityRecord record(
            long revision, IncrementalFrontierAuthorityState state,
            IncrementalActiveInterval active, IncrementalFrontier committed,
            IncrementalCommitReceipt normal, long backfillOrdinal,
            IncrementalCommitReceipt backfill, byte[] conflictDigest) throws Exception {
        return new IncrementalAuthorityRecord(
                revision, "feed", 7L, sha256("definition"),
                sha256("source-contract"), sha256("frontier-contract"),
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1, committed, active,
                normal, backfillOrdinal, backfill, state, conflictDigest);
    }

    private static IncrementalActiveInterval active(
            CapturedTargetReceipt capture, int attempt, byte[] decision) {
        IncrementalIntervalIdentity identity = IncrementalIntervalIdentity.from(capture);
        long claimedRevision = capture.mode() == IncrementalRunMode.BACKFILL_V1
                ? 0L : capture.expectedSequence();
        return active(capture, attempt, decision, claimedRevision);
    }

    private static IncrementalActiveInterval active(
            CapturedTargetReceipt capture, int attempt, byte[] decision,
            long claimedRevision) {
        IncrementalIntervalIdentity identity = IncrementalIntervalIdentity.from(capture);
        return new IncrementalActiveInterval(
                identity, capture, "artifact", claimedRevision,
                new RunAttemptFence(identity.candidateRunId(), "attempt-" + attempt, attempt),
                attempt, decision == null
                ? IncrementalAttemptCommitPhase.CLAIMED
                : IncrementalAttemptCommitPhase.COMMIT_DECIDED, decision);
    }

    private static IncrementalFrontier maximumTimestampFrontier(long epochMicros) {
        byte[] component = new byte[1017];
        java.util.Arrays.fill(component, (byte) 1);
        OrderedFrontierKey key = OrderedFrontierKey.of(
                OrderedFrontierKey.bytes(component), OrderedFrontierKey.bytes(component),
                OrderedFrontierKey.bytes(component), OrderedFrontierKey.bytes(component));
        return IncrementalFrontier.timestampKey(epochMicros, key);
    }

    private static CapturedTargetReceipt normalReceipt() throws Exception {
        return CapturedTargetReceipt.normal(
                7L, sha256("definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.begin(), "trigger", IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot-normal"));
    }

    private static CapturedTargetReceipt backfillReceipt() throws Exception {
        return backfillReceipt(4L);
    }

    private static CapturedTargetReceipt backfillReceipt(long ordinal) throws Exception {
        return CapturedTargetReceipt.backfill(
                7L, sha256("definition"), sha256("source-contract"), ordinal,
                IncrementalFrontier.int64Sequence(2L),
                IncrementalFrontier.int64Sequence(5L),
                "backfill-request", bytes("snapshot-backfill"));
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void resignAuthorityFrame(byte[] frame) throws Exception {
        int bodyLength = frame.length - 32;
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(java.util.Arrays.copyOf(frame, bodyLength));
        System.arraycopy(digest, 0, frame, bodyLength, digest.length);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
