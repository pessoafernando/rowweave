package io.rowweave.incremental;

import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageManifestPublication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** fresh reads never scan an unbounded authority history. */
class IncrementalFileAuthorityBoundedHistoryContractTest {
    private static final int HISTORY = 100_000;

    @Test
    void oneHundredThousandAcceptedTransitionsKeepFreshReplayBounded(
            @TempDir Path root) throws Exception {
        Path authorityRoot = root.resolve("authority");
        FileIncrementalAuthorityStore store =
                new FileIncrementalAuthorityStore(
                        authorityRoot, phase -> { },
                        System::currentTimeMillis, false);
        IncrementalInitializeRequest initialize = new IncrementalInitializeRequest(
                "feed", 1L, sha256("definition"), sha256("source"),
                sha256("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.initialize(
                        new IncrementalAuthorityCommand(
                                IncrementalAuthorityCommandKind.INITIALIZE,
                                "feed", "initialize", 0L, 1L,
                                null, null, initialize.transitionDigest()),
                        initialize)).code());

        long ownershipEpoch = 0L;
        RecoveryOwner lastOwner = null;
        FrontierRecoveryToken lastToken = null;
        for (int i = 0; i < HISTORY; i++) {
            lastOwner = new RecoveryOwner("master-" + (i & 1), i + 1L);
            FrontierOwnershipClaim claim = store.claimOwnership(
                    "feed", 0L, ownershipEpoch,
                    lastOwner);
            assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, claim.code());
            lastToken = claim.token();
            ownershipEpoch = lastToken.ownershipEpoch();
        }
        IncrementalAuthorityRecord expected = store.record("feed");
        assertTrue(store.walBytesForTest() <= 128L * 1024L,
                "current-state WAL must not grow with ownership history");

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(authorityRoot);
        assertEquals(expected, reopened.record("feed"));
        assertTrue(reopened.lastReplayFrameCountForTest() <= 64,
                "fresh process recovery must scan a fixed current-state bound");
        FrontierOwnershipClaim replayed = reopened.claimOwnership(
                "feed", 0L, ownershipEpoch - 1L, lastOwner);
        assertEquals(FrontierOwnershipClaim.Code.REPLAYED, replayed.code());
        assertEquals(lastToken, replayed.token());
    }

    @Test
    void maximumLegalAssignmentManifestSurvivesFreshReplay(
            @TempDir Path root) throws Exception {
        FileIncrementalAuthorityStore store =
                new FileIncrementalAuthorityStore(root);
        IncrementalInitializeRequest initialize = new IncrementalInitializeRequest(
                "feed", 1L, sha256("definition"), sha256("source"),
                sha256("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
        store.apply(IncrementalAuthorityMutation.initialize(
                new IncrementalAuthorityCommand(
                        IncrementalAuthorityCommandKind.INITIALIZE,
                        "feed", "initialize", 0L, 1L,
                        null, null, initialize.transitionDigest()), initialize));
        FrontierRecoveryToken token = store.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("master", 1L)).token();
        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source"), 0L,
                IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L),
                "snapshot".getBytes(StandardCharsets.UTF_8));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                "feed", capture, sha256("frontier"), "artifact",
                "attempt-1", "publication", 3, 0L, 10L);
        store.publishPending(token, request);
        store.apply(IncrementalAuthorityMutation.claim(
                new IncrementalAuthorityCommand(
                        IncrementalAuthorityCommandKind.CLAIM,
                        "feed", "claim", 0L, 1L,
                        request.intervalId(), request.firstAttemptFence(),
                        request.transitionDigest()), token, request));

        List<StageAssignmentRecord> records = new ArrayList<>(
                StageAssignmentManifest.MAX_ASSIGNMENTS);
        for (int i = 0; i < StageAssignmentManifest.MAX_ASSIGNMENTS; i++) {
            records.add(StageAssignmentRecord.create(
                    request.firstAttemptFence(), 1L, i, i + 1L,
                    0, i, StageAssignmentRole.SINK,
                    "worker", "localhost", 7001, -1, "incarnation"));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                request.firstAttemptFence(), request.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1, records);
        assertEquals(StageManifestPublication.Code.PUBLISHED,
                store.publish(manifest).code());

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(root);
        assertEquals(StageAssignmentManifest.MAX_ASSIGNMENTS,
                reopened.assignmentSnapshot(request.firstAttemptFence())
                        .manifest().assignmentCount());
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(StandardCharsets.UTF_8));
    }
}
