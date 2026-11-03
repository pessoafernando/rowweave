package io.rowweave.incremental;

import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** File authority recovery starts from bounded canonical state. */
class IncrementalFileAuthorityBootstrapContractTest {

    @Test
    void initializationGuardAndOwnershipSurviveFreshProcess(@TempDir Path root)
            throws Exception {
        FileIncrementalAuthorityStore first = new FileIncrementalAuthorityStore(root);
        IncrementalInitializeRequest initialize = initialize("feed");
        IncrementalAuthorityCommand command = initializeCommand(initialize, "initialize");
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                first.apply(IncrementalAuthorityMutation.initialize(
                        command, initialize)).code());
        FrontierOwnershipClaim original = first.claimOwnership(
                "feed", 0L, 0L, new RecoveryOwner("master-a", 1L));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, original.code());
        IncrementalAuthorityRecord expected = first.record("feed");

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(root);
        assertEquals(expected, reopened.record("feed"));
        assertEquals(original.token(), reopened.frontierOwnership("feed"));
        assertEquals(1, reopened.guardEntryCount());
        assertEquals(1, reopened.guardShardEntryCount(
                FileIncrementalAuthorityStore.guardShardIndex("feed")));
        assertEquals(FrontierOwnershipClaim.Code.REPLAYED,
                reopened.claimOwnership(
                        "feed", 0L, 0L,
                        new RecoveryOwner("master-a", 1L)).code());

        FrontierOwnershipClaim replacement = reopened.claimOwnership(
                "feed", 0L, original.token().ownershipEpoch(),
                new RecoveryOwner("master-b", 2L));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, replacement.code());
        FileIncrementalAuthorityStore secondReopen =
                new FileIncrementalAuthorityStore(root);
        assertEquals(replacement.token(),
                secondReopen.frontierOwnership("feed"));
        assertNull(secondReopen.frontierOwnership("missing-feed"));
        assertEquals(FrontierOwnershipClaim.Code.REPLAYED,
                secondReopen.claimOwnership(
                        "feed", 0L, original.token().ownershipEpoch(),
                        new RecoveryOwner("master-b", 2L)).code());
    }

    @Test
    void pendingRunAndJointFirstAttemptRecoverWithoutRecapture(@TempDir Path root)
            throws Exception {
        FileIncrementalAuthorityStore first = new FileIncrementalAuthorityStore(root);
        IncrementalInitializeRequest initialize = initialize("feed");
        first.apply(IncrementalAuthorityMutation.initialize(
                initializeCommand(initialize, "initialize"), initialize));
        FrontierRecoveryToken token = first.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("master", 1L)).token();
        IncrementalClaimRequest request = request("feed");
        assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                first.publishPending(token, request).code());

        FileIncrementalAuthorityStore pendingReopen =
                new FileIncrementalAuthorityStore(root);
        assertEquals(request, pendingReopen.pending("feed"));
        RunSnapshot pendingRun = pendingReopen.runSnapshot(request.runId());
        assertNotNull(pendingRun);
        assertEquals(RunStatus.READY, pendingRun.status());
        assertNull(pendingRun.currentAttempt());

        IncrementalAuthorityCommand claim = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, "feed", "claim",
                0L, 1L, request.intervalId(), request.firstAttemptFence(),
                request.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                pendingReopen.apply(IncrementalAuthorityMutation.claim(
                        claim, token, request)).code());
        IncrementalAuthorityRecord active = pendingReopen.record("feed");
        RunSnapshot running = pendingReopen.runSnapshot(request.runId());

        FileIncrementalAuthorityStore activeReopen =
                new FileIncrementalAuthorityStore(root);
        assertEquals(active, activeReopen.record("feed"));
        assertNull(activeReopen.pending("feed"));
        RunSnapshot recovered = activeReopen.runSnapshot(request.runId());
        assertEquals(running.revision(), recovered.revision());
        assertEquals(running.status(), recovered.status());
        assertEquals(running.currentAttempt().attemptId(),
                recovered.currentAttempt().attemptId());
        assertEquals(running.currentAttempt().fenceToken(),
                recovered.currentAttempt().fenceToken());
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                activeReopen.apply(IncrementalAuthorityMutation.claim(
                        claim, token, request)).code());
    }

    @Test
    void partialTailIsDiscardedButCommittedFrameCorruptionFailsClosed(
            @TempDir Path root) throws Exception {
        FileIncrementalAuthorityStore store = new FileIncrementalAuthorityStore(root);
        IncrementalInitializeRequest initialize = initialize("feed");
        store.apply(IncrementalAuthorityMutation.initialize(
                initializeCommand(initialize, "initialize"), initialize));
        IncrementalAuthorityRecord expected = store.record("feed");
        Path wal = root.resolve(
                "incremental-authority-v1/authority.wal");
        long committedLength = Files.size(wal);
        Files.write(wal, new byte[]{0x41, 0x58, 0x49},
                StandardOpenOption.APPEND);

        assertEquals(expected,
                new FileIncrementalAuthorityStore(root).record("feed"));
        assertEquals(committedLength, Files.size(wal));

        byte[] corrupt = Files.readAllBytes(wal);
        corrupt[corrupt.length - 1] ^= 1;
        Files.write(wal, corrupt);
        assertThrows(IllegalStateException.class,
                () -> new FileIncrementalAuthorityStore(root).record("feed"));
    }

    private static IncrementalInitializeRequest initialize(String feedId)
            throws Exception {
        return new IncrementalInitializeRequest(
                feedId, 1L, sha256("definition"), sha256("source"),
                sha256("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
    }

    private static IncrementalAuthorityCommand initializeCommand(
            IncrementalInitializeRequest request, String commandId) {
        return new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.INITIALIZE,
                request.feedId(), commandId, 0L, request.feedIncarnation(),
                null, null, request.transitionDigest());
    }

    private static IncrementalClaimRequest request(String feedId) throws Exception {
        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source"), 0L,
                IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L),
                "snapshot".getBytes(StandardCharsets.UTF_8));
        return new IncrementalClaimRequest(
                feedId, capture, sha256("frontier"), "artifact",
                "attempt-1", "publication-1", 3, 0L, 10L);
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(StandardCharsets.UTF_8));
    }
}
