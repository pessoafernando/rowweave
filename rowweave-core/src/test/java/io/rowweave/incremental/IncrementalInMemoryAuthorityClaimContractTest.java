package io.rowweave.incremental;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded in-memory initialize and joint first claim. */
class IncrementalInMemoryAuthorityClaimContractTest {

    @Test
    void initializeFreezesFeedContractsAndExactReplayOnly() throws Exception {
        InMemoryIncrementalAuthorityStore store = new InMemoryIncrementalAuthorityStore();
        IncrementalInitializeRequest request = initializeRequest("feed", 1L);
        IncrementalAuthorityMutation mutation = IncrementalAuthorityMutation.initialize(
                initializeCommand(request, "initialize"), request);

        IncrementalAuthorityDecision accepted = store.apply(mutation);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED, accepted.code());
        assertReadyRecord(accepted.record(), request);
        assertEquals(1, store.guardEntryCount());

        IncrementalAuthorityDecision replay = store.apply(mutation);
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED, replay.code());
        assertEquals(accepted.record(), replay.record());
        assertEquals(1, store.guardEntryCount());

        IncrementalInitializeRequest drift = new IncrementalInitializeRequest(
                "feed", 1L, sha256("other-definition"),
                request.sourceContractDigest(), request.frontierContractDigest(),
                request.frontierKind());
        IncrementalAuthorityDecision conflict = store.apply(
                IncrementalAuthorityMutation.initialize(
                        initializeCommand(drift, "initialize-drift"), drift));
        assertEquals(IncrementalAuthorityDecision.Code.FEED_CONFLICT, conflict.code());
        assertEquals(accepted.record(), store.record("feed"));
        assertEquals(1, store.guardEntryCount());

        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.initialize(
                        initializeCommand(request, "bad-digest"), drift));
    }

    @Test
    void fixedGuardCatalogStopsAtSixteenEntriesPerShard() throws Exception {
        InMemoryIncrementalAuthorityStore store = new InMemoryIncrementalAuthorityStore();
        List<String> colliding = collidingFeedIds(17);
        int shard = InMemoryIncrementalAuthorityStore.guardShardIndex(colliding.get(0));
        for (int i = 0; i < 16; i++) {
            IncrementalInitializeRequest request = initializeRequest(colliding.get(i), 1L);
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    store.apply(IncrementalAuthorityMutation.initialize(
                            initializeCommand(request, "initialize-" + i), request)).code());
        }
        IncrementalInitializeRequest overflow = initializeRequest(colliding.get(16), 1L);
        assertEquals(IncrementalAuthorityDecision.Code.GUARD_SHARD_FULL,
                store.apply(IncrementalAuthorityMutation.initialize(
                        initializeCommand(overflow, "overflow"), overflow)).code());
        assertEquals(16, store.guardEntryCount());
        assertEquals(16, store.guardShardEntryCount(shard));
        assertTrue(store.guardShardFrameBytes(shard)
                <= InMemoryIncrementalAuthorityStore.MAX_GUARD_SHARD_FRAME_BYTES);
        assertNull(store.record(colliding.get(16)));
    }

    @Test
    void pendingRunIsExactBoundAndCannotBeRetargeted() throws Exception {
        InMemoryIncrementalAuthorityStore store = initializedStore("feed");
        FrontierRecoveryToken token = ownership(store, "feed", 0L, 0L, "master-a", 1L);
        CapturedTargetReceipt firstCapture = snapshotCapture(
                "trigger", bytes("snapshot-token-a"));
        IncrementalClaimRequest first = claimRequest("feed", firstCapture);

        assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                store.publishPending(token, first).code());
        assertEquals(IncrementalPendingRunDecision.Code.ALREADY_PUBLISHED,
                store.publishPending(token, first).code());
        RunSnapshot pending = store.runSnapshot(first.runId());
        assertEquals(RunStatus.READY, pending.status());
        assertEquals(0, pending.attemptCount());
        assertNull(pending.currentAttempt());
        assertEquals(first.artifactId(), pending.artifactId());
        assertEquals(hex(firstCapture.definitionDigest()), pending.definitionId());

        CapturedTargetReceipt recaptured = snapshotCapture(
                "trigger", bytes("snapshot-token-b"));
        IncrementalClaimRequest drift = claimRequest("feed", recaptured);
        assertEquals(first.runId(), drift.runId());
        assertNotEquals(first.intervalId(), drift.intervalId());
        assertEquals(IncrementalPendingRunDecision.Code.PENDING_CONFLICT,
                store.publishPending(token, drift).code());
        assertEquals(pending.revision(), store.runSnapshot(first.runId()).revision());

        IncrementalClaimRequest wrongDefinition = claimRequest("feed",
                CapturedTargetReceipt.normal(
                        1L, sha256("wrong-definition"), sha256("source-contract"), 0L,
                        IncrementalFrontier.begin(), "other-trigger",
                        IncrementalRunMode.SNAPSHOT_V1,
                        IncrementalFrontier.int64Sequence(11L), bytes("snapshot")));
        InMemoryIncrementalAuthorityStore contractStore = initializedStore("feed");
        FrontierRecoveryToken contractToken = ownership(
                contractStore, "feed", 0L, 0L, "master-contract", 1L);
        assertEquals(IncrementalPendingRunDecision.Code.CONTRACT_CONFLICT,
                contractStore.publishPending(contractToken, wrongDefinition).code());
        assertNull(contractStore.runSnapshot(wrongDefinition.runId()));
    }

    @Test
    void frontierContractDriftAndIncrementalNoOpCreateNoPendingRun() throws Exception {
        InMemoryIncrementalAuthorityStore store = initializedStore("feed");
        FrontierRecoveryToken token = ownership(store, "feed", 0L, 0L, "master", 1L);
        CapturedTargetReceipt capture = snapshotCapture("trigger", bytes("snapshot"));
        IncrementalClaimRequest wrongFrontierContract = new IncrementalClaimRequest(
                "feed", capture, sha256("other-frontier-contract"),
                "artifact", "attempt-1", "publication-1", 3, 0L, 10L);
        IncrementalAuthorityRecord before = store.record("feed");
        assertEquals(IncrementalPendingRunDecision.Code.CONTRACT_CONFLICT,
                store.publishPending(token, wrongFrontierContract).code());
        assertEquals(before, store.record("feed"));
        assertNull(store.pending("feed"));
        assertNull(store.runSnapshot(wrongFrontierContract.runId()));

        CapturedTargetReceipt noOpCapture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.int64Sequence(10L), "no-op",
                IncrementalRunMode.INCREMENTAL_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot-no-op"));
        IncrementalClaimRequest noOp = claimRequest("feed", noOpCapture);
        assertEquals(IncrementalPendingRunDecision.Code.CONTRACT_CONFLICT,
                store.publishPending(token, noOp).code());
        assertEquals(before, store.record("feed"));
        assertNull(store.pending("feed"));
        assertNull(store.runSnapshot(noOp.runId()));

        IncrementalAuthorityRecord committed = new IncrementalAuthorityRecord(
                5L, "feed", 1L, sha256("definition"),
                sha256("source-contract"), sha256("frontier-contract"),
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                IncrementalFrontier.int64Sequence(10L), null, null,
                0L, null, IncrementalFrontierAuthorityState.READY, null);
        CapturedTargetReceipt exactNoOpCapture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source-contract"), 5L,
                IncrementalFrontier.int64Sequence(10L), "exact-no-op",
                IncrementalRunMode.INCREMENTAL_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot-exact"));
        IncrementalClaimRequest exactNoOp = claimRequest("feed", exactNoOpCapture);
        assertEquals(IncrementalPendingRunDecision.Code.NO_OP,
                InMemoryIncrementalAuthorityStore.pendingAdmission(
                        committed, exactNoOp));

        CapturedTargetReceipt wrongRevisionCapture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source-contract"), 4L,
                IncrementalFrontier.int64Sequence(10L), "stale-no-op",
                IncrementalRunMode.INCREMENTAL_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot-stale"));
        assertEquals(IncrementalPendingRunDecision.Code.CONTRACT_CONFLICT,
                InMemoryIncrementalAuthorityStore.pendingAdmission(
                        committed, claimRequest("feed", wrongRevisionCapture)));
    }

    @Test
    void ownershipReplayAndGuardShardChangeFenceTheOldToken() throws Exception {
        InMemoryIncrementalAuthorityStore store = initializedStore("feed");
        RecoveryOwner owner = new RecoveryOwner("master-a", 1L);
        FrontierOwnershipClaim acquired = store.claimOwnership(
                "feed", 0L, 0L, owner);
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, acquired.code());
        assertEquals(acquired.token(), store.frontierOwnership("feed"));
        FrontierOwnershipClaim replay = store.claimOwnership(
                "feed", 0L, 0L, owner);
        assertEquals(FrontierOwnershipClaim.Code.REPLAYED, replay.code());
        assertEquals(acquired.token(), replay.token());

        int shard = InMemoryIncrementalAuthorityStore.guardShardIndex("feed");
        String colliding = collidingFeedId(shard, "guard-change");
        IncrementalInitializeRequest other = initializeRequest(colliding, 1L);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.initialize(
                        initializeCommand(other, "initialize-other"), other)).code());
        IncrementalClaimRequest request = claimRequest(
                "feed", snapshotCapture("trigger", bytes("snapshot")));
        assertEquals(IncrementalPendingRunDecision.Code.STALE_OWNERSHIP,
                store.publishPending(acquired.token(), request).code());

        FrontierOwnershipClaim refreshed = store.claimOwnership(
                "feed", 0L, 1L, owner);
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, refreshed.code());
        assertNotEquals(acquired.token(), refreshed.token());
        assertTrue(refreshed.token().ownershipEpoch() > acquired.token().ownershipEpoch());
        assertTrue(refreshed.token().guardShardObservation().shardRevision()
                > acquired.token().guardShardObservation().shardRevision());
        assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                store.publishPending(refreshed.token(), request).code());
        assertEquals(refreshed.token(), store.frontierOwnership("feed"));
        assertNull(store.frontierOwnership("missing-feed"));
    }

    @Test
    void claimAndFirstAttemptShareOneLinearizationPointAndReplayAfterAckLoss()
            throws Exception {
        InMemoryIncrementalAuthorityStore store = initializedStore("feed");
        FrontierRecoveryToken token = ownership(store, "feed", 0L, 0L, "master-a", 1L);
        IncrementalClaimRequest request = claimRequest(
                "feed", snapshotCapture("trigger", bytes("snapshot")));
        assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                store.publishPending(token, request).code());
        IncrementalAuthorityCommand command = claimCommand(request, "claim", 0L, 1L);
        IncrementalAuthorityMutation mutation =
                IncrementalAuthorityMutation.claim(command, token, request);

        IncrementalAuthorityDecision lostAck = store.apply(mutation);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED, lostAck.code());
        assertEquals(1L, lostAck.record().revision());
        assertEquals(IncrementalFrontierAuthorityState.ACTIVE,
                lostAck.record().state());
        assertEquals(request.intervalId(),
                lostAck.record().activeInterval().identity().intervalId());
        assertEquals(request.firstAttemptFence(),
                lostAck.record().activeInterval().fence());
        RunSnapshot running = lostAck.runSnapshot();
        assertEquals(RunStatus.RUNNING, running.status());
        assertEquals(1, running.attemptCount());
        assertEquals(AttemptStatus.CREATED, running.currentAttempt().status());
        assertEquals(request.attemptId(), running.currentAttempt().attemptId());
        assertEquals(request.publicationId(), running.currentAttempt().publicationId());
        assertEquals(request.firstAttemptFence().fenceToken(),
                running.currentAttempt().fenceToken());

        IncrementalAuthorityDecision replay = store.apply(mutation);
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED, replay.code());
        assertEquals(lostAck.record(), replay.record());
        assertEquals(running.revision(), replay.runSnapshot().revision());
        assertEquals(1, replay.runSnapshot().attemptCount());
        assertNull(store.pending("feed"));

        IncrementalAuthorityCommand conflictingCommand = claimCommand(
                request, "claim", 1L, 1L);
        IncrementalAuthorityDecision conflict = store.apply(
                IncrementalAuthorityMutation.claim(
                        conflictingCommand, token, request));
        assertEquals(IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                conflict.code());
        assertEquals(1, store.runSnapshot(request.runId()).attemptCount());

        FrontierRecoveryToken replacement = ownership(
                store, "feed", 1L, 1L, "master-b", 2L);
        assertNotEquals(token, replacement);
        assertEquals(IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                store.apply(mutation).code(),
                "an exact old command replay must still pass the current ownership fence");
        assertEquals(1, store.runSnapshot(request.runId()).attemptCount());
    }

    @Test
    void staleRevisionLeaderAndContractDriftHaveZeroRunOrFrontierMutation()
            throws Exception {
        InMemoryIncrementalAuthorityStore store = initializedStore("feed");
        FrontierRecoveryToken firstOwner = ownership(
                store, "feed", 0L, 0L, "master-a", 1L);
        FrontierRecoveryToken replacement = ownership(
                store, "feed", 0L, 1L, "master-b", 2L);
        IncrementalClaimRequest request = claimRequest(
                "feed", snapshotCapture("trigger", bytes("snapshot")));
        assertEquals(IncrementalPendingRunDecision.Code.STALE_OWNERSHIP,
                store.publishPending(firstOwner, request).code());
        assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                store.publishPending(replacement, request).code());
        RunSnapshot beforeRun = store.runSnapshot(request.runId());
        IncrementalAuthorityRecord beforeFrontier = store.record("feed");

        IncrementalAuthorityMutation staleRevision = IncrementalAuthorityMutation.claim(
                claimCommand(request, "stale-revision", 1L, 1L), replacement, request);
        assertEquals(IncrementalAuthorityDecision.Code.STALE_REVISION,
                store.apply(staleRevision).code());
        assertEquals(beforeFrontier, store.record("feed"));
        assertEquals(beforeRun.revision(), store.runSnapshot(request.runId()).revision());

        IncrementalAuthorityMutation staleOwner = IncrementalAuthorityMutation.claim(
                claimCommand(request, "stale-owner", 0L, 1L), firstOwner, request);
        assertEquals(IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                store.apply(staleOwner).code());
        assertEquals(beforeFrontier, store.record("feed"));
        assertEquals(beforeRun.revision(), store.runSnapshot(request.runId()).revision());

        IncrementalClaimRequest wrongFeed = claimRequest("other-feed",
                snapshotCapture("other-trigger", bytes("other")));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.claim(
                        claimCommand(wrongFeed, "wrong-feed", 0L, 1L),
                        replacement, wrongFeed));
    }

    @Test
    void oneActiveIntervalBlocksEveryOtherPendingRun() throws Exception {
        InMemoryIncrementalAuthorityStore store = initializedStore("feed");
        FrontierRecoveryToken token = ownership(store, "feed", 0L, 0L, "master", 1L);
        IncrementalClaimRequest first = claimRequest(
                "feed", snapshotCapture("trigger-a", bytes("snapshot-a")));
        store.publishPending(token, first);
        store.apply(IncrementalAuthorityMutation.claim(
                claimCommand(first, "claim-a", 0L, 1L), token, first));

        IncrementalClaimRequest second = claimRequest(
                "feed", snapshotCapture("trigger-b", bytes("snapshot-b")));
        assertEquals(IncrementalPendingRunDecision.Code.ACTIVE_OR_PENDING_EXISTS,
                store.publishPending(token, second).code());
        assertNull(store.runSnapshot(second.runId()));
        assertEquals(first.intervalId(), store.record("feed")
                .activeInterval().identity().intervalId());
    }

    private static InMemoryIncrementalAuthorityStore initializedStore(String feedId)
            throws Exception {
        InMemoryIncrementalAuthorityStore store = new InMemoryIncrementalAuthorityStore();
        IncrementalInitializeRequest request = initializeRequest(feedId, 1L);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.initialize(
                        initializeCommand(request, "initialize-" + feedId), request)).code());
        return store;
    }

    private static IncrementalInitializeRequest initializeRequest(
            String feedId, long incarnation) throws Exception {
        return new IncrementalInitializeRequest(
                feedId, incarnation, sha256("definition"), sha256("source-contract"),
                sha256("frontier-contract"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
    }

    private static IncrementalAuthorityCommand initializeCommand(
            IncrementalInitializeRequest request, String commandId) {
        return new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.INITIALIZE, request.feedId(), commandId,
                0L, request.feedIncarnation(), null, null, request.transitionDigest());
    }

    private static FrontierRecoveryToken ownership(
            InMemoryIncrementalAuthorityStore store, String feedId,
            long expectedRevision, long expectedOwnershipEpoch,
            String ownerId, long term) {
        FrontierOwnershipClaim claim = store.claimOwnership(
                feedId, expectedRevision, expectedOwnershipEpoch,
                new RecoveryOwner(ownerId, term));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, claim.code());
        return claim.token();
    }

    private static IncrementalClaimRequest claimRequest(
            String feedId, CapturedTargetReceipt capture) {
        return new IncrementalClaimRequest(
                feedId, capture, digest("frontier-contract"),
                "artifact", "attempt-1", "publication-1", 3, 0L, 10L);
    }

    private static IncrementalAuthorityCommand claimCommand(
            IncrementalClaimRequest request, String commandId,
            long expectedRevision, long incarnation) {
        return new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, request.feedId(), commandId,
                expectedRevision, incarnation, request.intervalId(),
                request.firstAttemptFence(), request.transitionDigest());
    }

    private static CapturedTargetReceipt snapshotCapture(
            String trigger, byte[] snapshot) throws Exception {
        return CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.begin(), trigger, IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), snapshot);
    }

    private static void assertReadyRecord(
            IncrementalAuthorityRecord record, IncrementalInitializeRequest request)
            throws Exception {
        assertEquals(0L, record.revision());
        assertEquals(request.feedId(), record.feedId());
        assertEquals(request.feedIncarnation(), record.feedIncarnation());
        assertArrayEquals(request.definitionDigest(), record.definitionDigest());
        assertArrayEquals(request.sourceContractDigest(), record.sourceContractDigest());
        assertArrayEquals(request.frontierContractDigest(), record.frontierContractDigest());
        assertEquals(request.frontierKind(), record.frontierKind());
        assertEquals(IncrementalFrontier.begin(), record.committedFrontier());
        assertEquals(IncrementalFrontierAuthorityState.READY, record.state());
        assertNull(record.activeInterval());
    }

    private static List<String> collidingFeedIds(int count) {
        List<String> matches = new ArrayList<>();
        int shard = InMemoryIncrementalAuthorityStore.guardShardIndex("capacity-seed");
        for (int i = 0; matches.size() < count; i++) {
            String candidate = "capacity-feed-" + i;
            if (InMemoryIncrementalAuthorityStore.guardShardIndex(candidate) == shard) {
                matches.add(candidate);
            }
        }
        return matches;
    }

    private static String collidingFeedId(int shard, String prefix) {
        for (int i = 0; ; i++) {
            String candidate = prefix + '-' + i;
            if (InMemoryIncrementalAuthorityStore.guardShardIndex(candidate) == shard) {
                return candidate;
            }
        }
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] digest(String value) {
        try {
            return sha256(value);
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
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
}
