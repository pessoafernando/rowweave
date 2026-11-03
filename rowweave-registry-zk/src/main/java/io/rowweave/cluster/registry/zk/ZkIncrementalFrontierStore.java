package io.rowweave.cluster.registry.zk;

import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.FeedGuardShardObservation;
import io.rowweave.incremental.FeedGuardShardObservation;
import io.rowweave.incremental.FrontierOwnershipClaim;
import io.rowweave.incremental.FrontierRecoveryToken;
import io.rowweave.incremental.FrontierRecoveryTokenCodec;
import io.rowweave.incremental.IncrementalAuthorityCommand;
import io.rowweave.incremental.IncrementalAuthorityCommandCodec;
import io.rowweave.incremental.IncrementalAuthorityCommandKind;
import io.rowweave.incremental.IncrementalAuthorityDecision;
import io.rowweave.incremental.IncrementalAuthorityGateway;
import io.rowweave.incremental.IncrementalAuthorityRecord;
import io.rowweave.incremental.IncrementalAuthorityRecordCodec;
import io.rowweave.incremental.IncrementalActiveInterval;
import io.rowweave.incremental.IncrementalAttemptCommitPhase;
import io.rowweave.incremental.IncrementalAttemptProgressCommand;
import io.rowweave.incremental.IncrementalClaimRequest;
import io.rowweave.incremental.IncrementalClaimRequestCodec;
import io.rowweave.incremental.IncrementalCommitReceipt;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierAuthorityState;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalIntervalIdentity;
import io.rowweave.incremental.IncrementalPendingRunDecision;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.VerifiedIncrementalJointCommitProof;
import io.rowweave.incremental.VerifiedIncrementalConflictProof;
import io.rowweave.incremental.VerifiedIncrementalAbortClosureProof;
import io.rowweave.incremental.ZkIncrementalAbortProofAuthority;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.RetryAttemptIdentity;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalCodec;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunCancellationAuthorityKernel;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunDeadlineAuthorityKernel;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunResultSummary;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunStateStoreKernel;
import io.rowweave.run.RunStateStoreUnavailableException;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.WholeAttemptRetryDecision;
import io.rowweave.run.WholeAttemptRetryAuthorityKernel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** frontier bootstrap/query/ownership authority. */
public final class ZkIncrementalFrontierStore
        implements IncrementalAuthorityGateway {
    private static final String ROOT = "/rowweave/incremental-frontier-v1";
    private static final String FEEDS = ROOT + "/feeds";
    private static final String GUARDS = ROOT + "/incarnation-shards";
    private static final int MAX_RETRIES = 16;
    private static final int MAX_GUARD_ENTRIES = 16;
    private static final int MAX_FEEDS =
            FeedGuardShardObservation.SHARD_COUNT * MAX_GUARD_ENTRIES;
    private static final int FEED_MAGIC = 0x52575a46; // RWZF
    private static final int GUARD_MAGIC = 0x52574947; // RWIG
    private static final int VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;

    private final CuratorFramework client;
    private final ZkRunStateStore runs;
    private final CommitHook afterInitializeCommit;
    private final CommitHook afterOwnershipCommit;
    private final CommitHook afterClaimCommit;
    private final CommitHook afterProgressCommit;

    ZkIncrementalFrontierStore(CuratorFramework client) {
        this(client, new ZkRunStateStore(client));
    }

    ZkIncrementalFrontierStore(
            CuratorFramework client, ZkRunStateStore runs) {
        this(client, runs, () -> { }, () -> { }, () -> { }, () -> { });
    }

    ZkIncrementalFrontierStore(
            CuratorFramework client, CommitHook afterInitializeCommit,
            CommitHook afterOwnershipCommit) {
        this(client, new ZkRunStateStore(client), afterInitializeCommit,
                afterOwnershipCommit, () -> { }, () -> { });
    }

    ZkIncrementalFrontierStore(
            CuratorFramework client, ZkRunStateStore runs,
            CommitHook afterInitializeCommit,
            CommitHook afterOwnershipCommit,
            CommitHook afterClaimCommit) {
        this(client, runs, afterInitializeCommit, afterOwnershipCommit,
                afterClaimCommit, () -> { });
    }

    ZkIncrementalFrontierStore(
            CuratorFramework client, ZkRunStateStore runs,
            CommitHook afterInitializeCommit,
            CommitHook afterOwnershipCommit,
            CommitHook afterClaimCommit,
            CommitHook afterProgressCommit) {
        if (client == null || runs == null || afterInitializeCommit == null
                || afterOwnershipCommit == null || afterClaimCommit == null
                || afterProgressCommit == null) {
            throw new IllegalArgumentException("client and commit hooks are required");
        }
        this.client = client;
        this.runs = runs;
        this.afterInitializeCommit = afterInitializeCommit;
        this.afterOwnershipCommit = afterOwnershipCommit;
        this.afterClaimCommit = afterClaimCommit;
        this.afterProgressCommit = afterProgressCommit;
        try {
            ensurePersistent(FEEDS);
            ensurePersistent(GUARDS);
        } catch (Exception failure) {
            throw unavailable("initialize ZK incremental frontier root", failure);
        }
    }

    @Override
    public IncrementalAuthorityDecision initialize(
            String commandId, IncrementalInitializeRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("initialize request is required");
        }
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.INITIALIZE,
                request.feedId(), commandId, 0L, request.feedIncarnation(),
                null, null, request.transitionDigest());
        byte[] commandFrame = IncrementalAuthorityCommandCodec.encode(command);
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                FeedFrame existing = readFeed(request.feedId(), null);
                if (existing != null) {
                    return replayInitialize(existing, command);
                }
                GuardVersion guard = readGuard(request.feedId());
                Long currentIncarnation = guard.entries.get(feedHash(request.feedId()));
                if (currentIncarnation != null
                        && request.feedIncarnation() != next(currentIncarnation)) {
                    return IncrementalAuthorityDecision.rejected(
                            IncrementalAuthorityDecision.Code.FEED_CONFLICT, null, null);
                }
                if (currentIncarnation == null
                        && request.feedIncarnation() != 1L) {
                    return IncrementalAuthorityDecision.rejected(
                            IncrementalAuthorityDecision.Code.FEED_CONFLICT, null, null);
                }
                if (currentIncarnation == null
                        && guard.entries.size() >= MAX_GUARD_ENTRIES) {
                    return IncrementalAuthorityDecision.rejected(
                            IncrementalAuthorityDecision.Code.GUARD_SHARD_FULL, null, null);
                }
                TreeMap<String, Long> nextEntries = new TreeMap<>(guard.entries);
                nextEntries.put(feedHash(request.feedId()), request.feedIncarnation());
                GuardFrame nextGuard = new GuardFrame(
                        guard.index, next(guard.revision), nextEntries);
                IncrementalAuthorityRecord record = readyRecord(request);
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check()
                        .withVersion(guard.version).forPath(guard.path));
                operations.add(client.transactionOp().setData()
                        .withVersion(guard.version).forPath(
                                guard.path, encodeGuard(nextGuard)));
                operations.add(client.transactionOp().create().forPath(
                        feedPath(request.feedId()), encodeFeed(
                                record, commandFrame, commandFrame, null)));
                client.transaction().forOperations(operations);
                afterInitializeCommit.run();
                return IncrementalAuthorityDecision.accepted(record, null);
            } catch (KeeperException.NodeExistsException
                    | KeeperException.BadVersionException race) {
                lastRace = race;
            } catch (KeeperException.ConnectionLossException unknown) {
                lastRace = unknown;
            } catch (Exception failure) {
                throw unavailable("initialize incremental feed", failure);
            }
        }
        try {
            FeedFrame observed = readFeed(request.feedId(), null);
            if (observed != null) return replayInitialize(observed, command);
        } catch (Exception failure) {
            throw unavailable("reconcile incremental feed initialization", failure);
        }
        throw unavailable("initialize incremental feed after retries", lastRace);
    }

    @Override
    public IncrementalAuthorityRecord record(String feedId) {
        requireFeedId(feedId);
        try {
            FeedFrame frame = readFeed(feedId, null);
            return frame == null ? null : frame.record;
        } catch (Exception failure) {
            throw unavailable("query incremental feed", failure);
        }
    }

    public IncrementalPendingRunDecision publishPending(
            FrontierRecoveryToken token, IncrementalClaimRequest request) {
        if (token == null || request == null) {
            throw new IllegalArgumentException(
                    "frontier token and pending request are required");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(request.feedId(), feedStat);
            if (feed == null) {
                return pending(IncrementalPendingRunDecision.Code.FEED_NOT_FOUND, null);
            }
            OwnershipView ownership = ownershipView(feed, feedStat, token);
            if (ownership == null) {
                return pending(IncrementalPendingRunDecision.Code.STALE_OWNERSHIP,
                        runs.get(request.runId()));
            }
            if (feed.record.activeInterval() != null) {
                return pending(
                        IncrementalPendingRunDecision.Code.ACTIVE_OR_PENDING_EXISTS,
                        runs.get(request.runId()));
            }
            if (feed.pending != null) {
                return pending(feed.pending.equals(request)
                                ? IncrementalPendingRunDecision.Code.ALREADY_PUBLISHED
                                : IncrementalPendingRunDecision.Code.PENDING_CONFLICT,
                        runs.get(feed.pending.runId()));
            }
            IncrementalPendingRunDecision.Code rejected =
                    pendingAdmission(feed.record, request);
            if (rejected != null) return pending(rejected, null);
            RunSnapshot existing = runs.get(request.runId());
            if (existing != null) {
                return pending(IncrementalPendingRunDecision.Code.RUN_CONFLICT,
                        existing);
            }
            byte[] nextFeed = encodeFeed(
                    feed.record,
                    IncrementalAuthorityCommandCodec.encode(feed.initializeCommand),
                    IncrementalAuthorityCommandCodec.encode(feed.lastCommand),
                    request);
            RunCommand create = RunCommand.createRun(
                    "incremental-pending-" + request.intervalId(),
                    request.runId(), hex(request.capturedTarget().definitionDigest()),
                    request.artifactId(), request.maxAttempts(),
                    request.deadlineEpochMillis(), request.eventTimeMillis());
            RunDecision decision = runs.applyAugmented(
                    create, null, false,
                    (ignored, operations) -> appendFrontierChecksAndSet(
                            request.feedId(), ownership, feedStat,
                            nextFeed, operations));
            if (decision.code() == RunDecision.Code.ACCEPTED) {
                return pending(IncrementalPendingRunDecision.Code.PUBLISHED,
                        decision.snapshot());
            }
            FeedFrame after = readFeed(request.feedId(), null);
            if (after != null && request.equals(after.pending)) {
                return pending(IncrementalPendingRunDecision.Code.ALREADY_PUBLISHED,
                        runs.get(request.runId()));
            }
            return pending(IncrementalPendingRunDecision.Code.RUN_CONFLICT,
                    decision.snapshot());
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("publish incremental pending Run", failure);
        }
    }

    public IncrementalAuthorityDecision claimInterval(
            String commandId, FrontierRecoveryToken token,
            IncrementalClaimRequest request) {
        if (token == null || request == null) {
            throw new IllegalArgumentException(
                    "frontier token and claim request are required");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(request.feedId(), feedStat);
            if (feed == null) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.FEED_NOT_FOUND,
                        null, null);
            }
            RunSnapshot run = runs.get(request.runId());
            if (claimApplied(feed, run, commandId, request)) {
                return IncrementalAuthorityDecision.replayed(feed.record, run);
            }
            OwnershipView ownership = ownershipView(feed, feedStat, token);
            if (ownership == null) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                        feed.record, run);
            }
            if (feed.record.activeInterval() != null) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.ACTIVE_EXISTS,
                        feed.record, run);
            }
            if (feed.pending == null) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.PENDING_NOT_FOUND,
                        feed.record, run);
            }
            if (!feed.pending.equals(request)) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.PENDING_CONFLICT,
                        feed.record, run);
            }
            if (!pendingRunMatches(run, request)) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.RUN_CONFLICT,
                        feed.record, run);
            }
            IncrementalAuthorityCommand command = claimCommand(
                    commandId, request, feed.record.revision());
            IncrementalActiveInterval active = new IncrementalActiveInterval(
                    IncrementalIntervalIdentity.from(request.capturedTarget()),
                    request.capturedTarget(), request.artifactId(),
                    feed.record.revision(), request.firstAttemptFence(), 1,
                    IncrementalAttemptCommitPhase.CLAIMED, null);
            IncrementalAuthorityRecord claimed = new IncrementalAuthorityRecord(
                    next(feed.record.revision()), feed.record.feedId(),
                    feed.record.feedIncarnation(), feed.record.definitionDigest(),
                    feed.record.sourceContractDigest(),
                    feed.record.frontierContractDigest(), feed.record.frontierKind(),
                    feed.record.committedFrontier(), active,
                    feed.record.lastCommitReceipt(),
                    feed.record.lastCompletedBackfillOrdinal(),
                    feed.record.lastBackfillReceipt(),
                    IncrementalFrontierAuthorityState.ACTIVE, null);
            byte[] nextFeed = encodeFeed(
                    claimed,
                    IncrementalAuthorityCommandCodec.encode(feed.initializeCommand),
                    IncrementalAuthorityCommandCodec.encode(command), null);
            RunCommand createAttempt = RunCommand.createAttempt(
                    command.commandId(), request.runId(), run.revision(),
                    request.attemptId(), request.publicationId(),
                    request.eventTimeMillis());
            RunDecision decision = runs.applyAugmented(
                    createAttempt, null, false,
                    (ignored, operations) -> appendFrontierChecksAndSet(
                            request.feedId(), ownership, feedStat,
                            nextFeed, operations));
            if (decision.code() == RunDecision.Code.ACCEPTED) {
                afterClaimCommit.run();
                return IncrementalAuthorityDecision.accepted(
                        claimed, decision.snapshot());
            }
            FeedFrame after = readFeed(request.feedId(), null);
            RunSnapshot afterRun = runs.get(request.runId());
            if (claimApplied(after, afterRun, commandId, request)) {
                return IncrementalAuthorityDecision.replayed(
                        after.record, afterRun);
            }
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.RUN_CONFLICT,
                    after == null ? null : after.record, afterRun);
        } catch (KeeperException.ConnectionLossException unknown) {
            try {
                FeedFrame after = readFeed(request.feedId(), null);
                RunSnapshot afterRun = runs.get(request.runId());
                if (claimApplied(after, afterRun, commandId, request)) {
                    return IncrementalAuthorityDecision.replayed(
                            after.record, afterRun);
                }
            } catch (Exception confirmation) {
                unknown.addSuppressed(confirmation);
            }
            throw unavailable("reconcile incremental claim", unknown);
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("claim incremental interval", failure);
        }
    }

    public RunDecision advanceProgress(
            FrontierRecoveryToken token,
            IncrementalAttemptProgressCommand command) {
        return advanceProgress(token, null, command, false);
    }

    public RunDecision advanceProgress(
            FrontierRecoveryToken token, RecoveryToken runToken,
            IncrementalAttemptProgressCommand command) {
        if (runToken == null || token == null
                || !token.owner().equals(runToken.owner())) {
            throw new IllegalArgumentException(
                    "matching frontier and Run recovery tokens are required");
        }
        return advanceProgress(token, runToken, command, true);
    }

    private RunDecision advanceProgress(
            FrontierRecoveryToken token, RecoveryToken runToken,
            IncrementalAttemptProgressCommand command, boolean recovered) {
        if (token == null || command == null) {
            throw new IllegalArgumentException(
                    "frontier token and progress command are required");
        }
        RunCommand runCommand = runProgress(command);
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(command.feedId(), feedStat);
            RunSnapshot current = runs.get(command.fence().runId());
            if (feed == null || ownershipView(feed, feedStat, token) == null) {
                return RunStateStoreKernel.staleRecoveryOwner(current);
            }
            if (current == null) {
                return RunStateStoreKernel.evaluate(null, runCommand);
            }
            if (command.expectedFrontierRevision() != feed.record.revision()) {
                return staleRunRevision(current, command);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (feed.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null
                    || !active.identity().intervalId().equals(command.intervalId())
                    || !active.fence().equals(command.fence())) {
                return staleRunFence(current, command);
            }
            if (runs.batchSinkAbort(command.fence()) != null) {
                return RunStateStoreKernel.illegalTransition(current);
            }
            OwnershipView ownership = ownershipView(feed, feedStat, token);
            if (ownership == null) {
                return RunStateStoreKernel.staleRecoveryOwner(current);
            }
            RunDecision decision = runs.applyAugmented(
                    runCommand, runToken, recovered,
                    (ignored, operations) -> appendFrontierChecks(
                            command.feedId(), ownership, feedStat, operations));
            if (decision.code() == RunDecision.Code.ACCEPTED) {
                afterProgressCommit.run();
            } else if (decision.code() == RunDecision.Code.ALREADY_APPLIED
                    && !currentlyOwned(command.feedId(), token)) {
                return RunStateStoreKernel.staleRecoveryOwner(
                        runs.get(command.fence().runId()));
            }
            return decision;
        } catch (KeeperException.ConnectionLossException unknown) {
            try {
                if (!currentlyOwned(command.feedId(), token)) {
                    return RunStateStoreKernel.staleRecoveryOwner(
                            runs.get(command.fence().runId()));
                }
                RunDecision replay = runs.applyAugmented(
                        runCommand, runToken, recovered,
                        ZkRunStateStore.TransactionAugment.NONE);
                if (replay.code() == RunDecision.Code.ALREADY_APPLIED) {
                    return replay;
                }
            } catch (Exception confirmation) {
                unknown.addSuppressed(confirmation);
            }
            throw unavailable("reconcile incremental progress", unknown);
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(command.feedId(), token)) {
                    return RunStateStoreKernel.staleRecoveryOwner(
                            runs.get(command.fence().runId()));
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("advance incremental progress", failure);
        }
    }

    public RecoveryClaim claimRunOwnership(
            FrontierRecoveryToken frontierToken, String runId,
            long expectedRevision, long expectedOwnershipEpoch,
            RecoveryOwner owner) {
        if (frontierToken == null || owner == null
                || !frontierToken.owner().equals(owner)) {
            throw new IllegalArgumentException(
                    "matching frontier and Run recovery owners are required");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            if (feed == null || ownershipView(
                    feed, feedStat, frontierToken) == null) {
                return RecoveryClaim.lostRace();
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (feed.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null
                    || !active.fence().runId().equals(runId)) {
                return RecoveryClaim.staleRun();
            }
            OwnershipView ownership = ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) return RecoveryClaim.lostRace();
            RecoveryClaim claim = runs.claimAugmented(
                    runId, expectedRevision, expectedOwnershipEpoch, owner,
                    (ignored, operations) -> appendFrontierChecks(
                            feed.record.feedId(), ownership,
                            feedStat, operations));
            if ((claim.code() == RecoveryClaim.Code.ACQUIRED
                    || claim.code() == RecoveryClaim.Code.REPLAYED)
                    && !currentlyOwned(feed.record.feedId(), frontierToken)) {
                return RecoveryClaim.lostRace();
            }
            return claim;
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return RecoveryClaim.lostRace();
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("claim incremental Run ownership", failure);
        }
    }

    public StageManifestPublication publishAssignments(
            FrontierRecoveryToken frontierToken,
            StageAssignmentManifest manifest) {
        return publishAssignments(frontierToken, null, manifest, false);
    }

    public StageManifestPublication publishAssignments(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentManifest manifest) {
        if (runToken == null || frontierToken == null
                || !frontierToken.owner().equals(runToken.owner())) {
            throw new IllegalArgumentException(
                    "matching frontier and Run recovery tokens are required");
        }
        return publishAssignments(frontierToken, runToken, manifest, true);
    }

    private StageManifestPublication publishAssignments(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentManifest manifest, boolean recovered) {
        if (frontierToken == null || manifest == null) {
            throw new IllegalArgumentException(
                    "frontier token and assignment manifest are required");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            if (feed == null || ownershipView(
                    feed, feedStat, frontierToken) == null) {
                return StageManifestPublication.rejected(
                        StageManifestPublication.Code.RECOVERY_OWNED);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (feed.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null
                    || !active.fence().equals(manifest.fence())
                    || !active.artifactId().equals(manifest.artifactId())) {
                return StageManifestPublication.rejected(
                        StageManifestPublication.Code.STALE_RUN);
            }
            OwnershipView ownership = ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) {
                return StageManifestPublication.rejected(
                        StageManifestPublication.Code.RECOVERY_OWNED);
            }
            StageManifestPublication publication = runs.publishAugmented(
                    manifest, runToken, recovered,
                    (ignored, operations) -> appendFrontierChecks(
                            feed.record.feedId(), ownership,
                            feedStat, operations));
            if ((publication.code() == StageManifestPublication.Code.PUBLISHED
                    || publication.code()
                    == StageManifestPublication.Code.ALREADY_PUBLISHED)
                    && !currentlyOwned(feed.record.feedId(), frontierToken)) {
                return StageManifestPublication.rejected(
                        StageManifestPublication.Code.RECOVERY_OWNED);
            }
            return publication;
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return StageManifestPublication.rejected(
                            StageManifestPublication.Code.RECOVERY_OWNED);
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("publish incremental assignments", failure);
        }
    }

    public StageAcceptanceDecision acceptAssignment(
            FrontierRecoveryToken frontierToken,
            StageAssignmentAcceptance acceptance) {
        return acceptAssignment(frontierToken, null, acceptance, false);
    }

    public StageAcceptanceDecision acceptAssignment(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentAcceptance acceptance) {
        if (runToken == null || frontierToken == null
                || !frontierToken.owner().equals(runToken.owner())) {
            throw new IllegalArgumentException(
                    "matching frontier and Run recovery tokens are required");
        }
        return acceptAssignment(frontierToken, runToken, acceptance, true);
    }

    private StageAcceptanceDecision acceptAssignment(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentAcceptance acceptance, boolean recovered) {
        if (frontierToken == null || acceptance == null) {
            throw new IllegalArgumentException(
                    "frontier token and assignment acceptance are required");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            if (feed == null || ownershipView(
                    feed, feedStat, frontierToken) == null) {
                return StageAcceptanceDecision.rejected(
                        StageAcceptanceDecision.Code.RECOVERY_OWNED);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (feed.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null
                    || !active.fence().equals(
                    acceptance.token().fence())) {
                return StageAcceptanceDecision.rejected(
                        StageAcceptanceDecision.Code.STALE_RUN);
            }
            OwnershipView ownership = ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) {
                return StageAcceptanceDecision.rejected(
                        StageAcceptanceDecision.Code.RECOVERY_OWNED);
            }
            StageAcceptanceDecision decision = runs.acceptAugmented(
                    acceptance, runToken, recovered,
                    (ignored, operations) -> appendFrontierChecks(
                            feed.record.feedId(), ownership,
                            feedStat, operations));
            if ((decision.code() == StageAcceptanceDecision.Code.ACCEPTED
                    || decision.code()
                    == StageAcceptanceDecision.Code.ALREADY_ACCEPTED)
                    && !currentlyOwned(feed.record.feedId(), frontierToken)) {
                return StageAcceptanceDecision.rejected(
                        StageAcceptanceDecision.Code.RECOVERY_OWNED);
            }
            return decision;
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return StageAcceptanceDecision.rejected(
                            StageAcceptanceDecision.Code.RECOVERY_OWNED);
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("accept incremental assignment", failure);
        }
    }

    public StageTerminalDecision recordStageTerminal(
            FrontierRecoveryToken frontierToken,
            StageTerminalReceipt receipt) {
        return recordStageTerminal(frontierToken, null, receipt, false);
    }

    public StageTerminalDecision recordStageTerminal(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageTerminalReceipt receipt) {
        if (runToken == null || frontierToken == null
                || !frontierToken.owner().equals(runToken.owner())) {
            throw new IllegalArgumentException(
                    "matching frontier and Run recovery tokens are required");
        }
        return recordStageTerminal(frontierToken, runToken, receipt, true);
    }

    private StageTerminalDecision recordStageTerminal(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageTerminalReceipt receipt, boolean recovered) {
        if (frontierToken == null || receipt == null) {
            throw new IllegalArgumentException(
                    "frontier token and terminal receipt are required");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            if (feed == null || ownershipView(
                    feed, feedStat, frontierToken) == null) {
                return StageTerminalDecision.rejected(
                        StageTerminalDecision.Code.RECOVERY_OWNED);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (feed.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null
                    || !active.fence().equals(receipt.token().fence())) {
                return StageTerminalDecision.rejected(
                        StageTerminalDecision.Code.STALE_RUN);
            }
            OwnershipView ownership = ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) {
                return StageTerminalDecision.rejected(
                        StageTerminalDecision.Code.RECOVERY_OWNED);
            }
            StageTerminalDecision decision = runs.recordTerminalAugmented(
                    receipt, runToken, recovered,
                    (ignored, operations) -> appendFrontierChecks(
                            feed.record.feedId(), ownership,
                            feedStat, operations));
            if ((decision.code() == StageTerminalDecision.Code.RECORDED
                    || decision.code()
                    == StageTerminalDecision.Code.ALREADY_RECORDED)
                    && !currentlyOwned(feed.record.feedId(), frontierToken)) {
                return StageTerminalDecision.rejected(
                        StageTerminalDecision.Code.RECOVERY_OWNED);
            }
            return decision;
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return StageTerminalDecision.rejected(
                            StageTerminalDecision.Code.RECOVERY_OWNED);
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("record incremental stage terminal", failure);
        }
    }

    public PreparedSinkDecision recordPreparedSink(
            FrontierRecoveryToken frontierToken,
            PreparedSinkReceipt receipt) {
        return recordPreparedSink(frontierToken, null, receipt, false);
    }

    public PreparedSinkDecision recordPreparedSink(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            PreparedSinkReceipt receipt) {
        if (runToken == null || frontierToken == null
                || !frontierToken.owner().equals(runToken.owner())) {
            throw new IllegalArgumentException(
                    "matching frontier and Run recovery tokens are required");
        }
        return recordPreparedSink(frontierToken, runToken, receipt, true);
    }

    private PreparedSinkDecision recordPreparedSink(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            PreparedSinkReceipt receipt, boolean recovered) {
        if (frontierToken == null || receipt == null) {
            throw new IllegalArgumentException(
                    "frontier token and prepared receipt are required");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            if (feed == null || ownershipView(
                    feed, feedStat, frontierToken) == null) {
                return PreparedSinkDecision.rejected(
                        PreparedSinkDecision.Code.RECOVERY_OWNED);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (feed.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null
                    || !active.fence().equals(receipt.token().fence())) {
                return PreparedSinkDecision.rejected(
                        PreparedSinkDecision.Code.STALE_RUN);
            }
            OwnershipView ownership = ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) {
                return PreparedSinkDecision.rejected(
                        PreparedSinkDecision.Code.RECOVERY_OWNED);
            }
            PreparedSinkDecision decision = runs.recordPreparedSinkAugmented(
                    receipt, runToken, recovered,
                    (ignored, operations) -> appendFrontierChecks(
                            feed.record.feedId(), ownership,
                            feedStat, operations));
            if ((decision.code() == PreparedSinkDecision.Code.RECORDED
                    || decision.code()
                    == PreparedSinkDecision.Code.ALREADY_RECORDED)
                    && !currentlyOwned(feed.record.feedId(), frontierToken)) {
                return PreparedSinkDecision.rejected(
                        PreparedSinkDecision.Code.RECOVERY_OWNED);
            }
            return decision;
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return PreparedSinkDecision.rejected(
                            PreparedSinkDecision.Code.RECOVERY_OWNED);
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("record incremental prepared Sink", failure);
        }
    }

    public ManifestMemberDecision recordManifestMember(
            FrontierRecoveryToken frontierToken,
            ManifestMemberReceipt receipt) {
        return recordManifestMember(frontierToken, null, receipt, false);
    }

    public ManifestMemberDecision recordManifestMember(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestMemberReceipt receipt) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        return recordManifestMember(frontierToken, runToken, receipt, true);
    }

    private ManifestMemberDecision recordManifestMember(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestMemberReceipt receipt, boolean recovered) {
        if (frontierToken == null || receipt == null) {
            throw new IllegalArgumentException(
                    "frontier token and manifest member are required");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) {
                return ManifestMemberDecision.rejected(
                        ManifestMemberDecision.Code.RECOVERY_OWNED);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (feed.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null
                    || !active.fence().equals(receipt.token().fence())) {
                return ManifestMemberDecision.rejected(
                        ManifestMemberDecision.Code.STALE_RUN);
            }
            ManifestMemberDecision decision = runs.recordManifestMemberAugmented(
                    receipt, runToken, recovered,
                    (ignored, operations, complete) -> {
                        if (complete && active.phase()
                                == IncrementalAttemptCommitPhase.CLAIMED) {
                            IncrementalAuthorityRecord prepared = withActivePhase(
                                    feed.record,
                                    IncrementalAttemptCommitPhase.PREPARED, null);
                            appendFrontierChecksAndSet(
                                    feed.record.feedId(), ownership, feedStat,
                                    encodeFeed(prepared,
                                            IncrementalAuthorityCommandCodec.encode(
                                                    feed.initializeCommand),
                                            IncrementalAuthorityCommandCodec.encode(
                                                    feed.lastCommand),
                                            feed.pending),
                                    operations);
                        } else {
                            appendFrontierChecks(feed.record.feedId(), ownership,
                                    feedStat, operations);
                        }
                    });
            if ((decision.code() == ManifestMemberDecision.Code.RECORDED
                    || decision.code()
                    == ManifestMemberDecision.Code.ALREADY_RECORDED)
                    && !currentlyOwned(feed.record.feedId(), frontierToken)) {
                return ManifestMemberDecision.rejected(
                        ManifestMemberDecision.Code.RECOVERY_OWNED);
            }
            return decision;
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return ManifestMemberDecision.rejected(
                            ManifestMemberDecision.Code.RECOVERY_OWNED);
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("record incremental manifest member", failure);
        }
    }

    public ManifestIntentDecision commitManifestIntent(
            FrontierRecoveryToken frontierToken,
            ManifestCommitIntent intent) {
        return commitManifestIntent(frontierToken, null, intent, false);
    }

    public ManifestIntentDecision commitManifestIntent(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestCommitIntent intent) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        return commitManifestIntent(frontierToken, runToken, intent, true);
    }

    private ManifestIntentDecision commitManifestIntent(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestCommitIntent intent, boolean recovered) {
        if (frontierToken == null || intent == null) {
            throw new IllegalArgumentException(
                    "frontier token and manifest intent are required");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) {
                return ManifestIntentDecision.rejected(
                        ManifestIntentDecision.Code.RECOVERY_OWNED);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (feed.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null
                    || !active.fence().equals(intent.fence())) {
                return ManifestIntentDecision.rejected(
                        ManifestIntentDecision.Code.STALE_RUN);
            }
            if (active.phase() != IncrementalAttemptCommitPhase.PREPARED
                    && active.phase()
                    != IncrementalAttemptCommitPhase.COMMIT_DECIDED) {
                return ManifestIntentDecision.rejected(
                        ManifestIntentDecision.Code.INCOMPLETE_MEMBERS);
            }
            IncrementalAuthorityRecord decided = active.phase()
                    == IncrementalAttemptCommitPhase.COMMIT_DECIDED
                    ? feed.record : withActivePhase(
                    feed.record, IncrementalAttemptCommitPhase.COMMIT_DECIDED,
                    sha256(ManifestCommitJournalCodec.encodeIntent(intent)));
            ManifestIntentDecision decision = runs.commitManifestIntentAugmented(
                    intent, runToken, recovered,
                    (ignored, operations) -> {
                        if (decided == feed.record) {
                            appendFrontierChecks(feed.record.feedId(), ownership,
                                    feedStat, operations);
                        } else {
                            appendFrontierChecksAndSet(
                                    feed.record.feedId(), ownership, feedStat,
                                    encodeFeed(decided,
                                            IncrementalAuthorityCommandCodec.encode(
                                                    feed.initializeCommand),
                                            IncrementalAuthorityCommandCodec.encode(
                                                    feed.lastCommand),
                                            feed.pending),
                                    operations);
                        }
                    });
            if ((decision.code() == ManifestIntentDecision.Code.COMMITTED
                    || decision.code()
                    == ManifestIntentDecision.Code.ALREADY_COMMITTED)
                    && !currentlyOwned(feed.record.feedId(), frontierToken)) {
                return ManifestIntentDecision.rejected(
                        ManifestIntentDecision.Code.RECOVERY_OWNED);
            }
            return decision;
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return ManifestIntentDecision.rejected(
                            ManifestIntentDecision.Code.RECOVERY_OWNED);
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("commit incremental manifest intent", failure);
        }
    }

    public BatchSinkAbortDecision recordBatchSinkAbort(
            FrontierRecoveryToken frontierToken,
            BatchSinkAbortRecord record) {
        return recordBatchSinkAbort(frontierToken, null, record, false);
    }

    public BatchSinkAbortDecision recordBatchSinkAbort(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            BatchSinkAbortRecord record) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        return recordBatchSinkAbort(frontierToken, runToken, record, true);
    }

    private BatchSinkAbortDecision recordBatchSinkAbort(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            BatchSinkAbortRecord record, boolean recovered) {
        if (frontierToken == null || record == null) {
            throw new IllegalArgumentException(
                    "frontier token and Sink abort record are required");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) {
                return BatchSinkAbortDecision.rejected(
                        BatchSinkAbortDecision.Code.RECOVERY_OWNED);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (feed.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null
                    || !active.fence().equals(record.fence())) {
                return BatchSinkAbortDecision.rejected(
                        BatchSinkAbortDecision.Code.STALE_RUN);
            }
            BatchSinkAbortDecision decision = runs.recordBatchSinkAbortAugmented(
                    record, runToken, recovered,
                    (ignored, operations) -> appendFrontierChecks(
                            feed.record.feedId(), ownership,
                            feedStat, operations));
            if ((decision.code() == BatchSinkAbortDecision.Code.RECORDED
                    || decision.code()
                    == BatchSinkAbortDecision.Code.ALREADY_RECORDED)
                    && !currentlyOwned(feed.record.feedId(), frontierToken)) {
                return BatchSinkAbortDecision.rejected(
                        BatchSinkAbortDecision.Code.RECOVERY_OWNED);
            }
            return decision;
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return BatchSinkAbortDecision.rejected(
                            BatchSinkAbortDecision.Code.RECOVERY_OWNED);
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("record incremental Sink abort", failure);
        }
    }

    public IncrementalAuthorityDecision jointSuccess(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAuthorityCommand command,
            VerifiedIncrementalJointCommitProof proof) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        if (command == null || proof == null
                || !frontierToken.feedId().equals(command.feedId())
                || frontierToken.feedIncarnation() != command.feedIncarnation()
                || !frontierToken.owner().equals(runToken.owner())
                || !runToken.runId().equals(proof.fence().runId())
                || !command.intervalId().equals(proof.intervalId())
                || !command.fence().equals(proof.fence())
                || !MessageDigest.isEqual(
                command.transitionDigest(), proof.proofDigest())
                || !MessageDigest.isEqual(
                frontierToken.tokenDigest(),
                proof.frontierRecoveryTokenDigest())
                || !MessageDigest.isEqual(
                runToken.tokenDigest(), proof.runRecoveryTokenDigest())) {
            throw new IllegalArgumentException(
                    "joint success requires its exact command, proof and ownership");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            RunSnapshot run = runs.get(runToken.runId());
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                        feed == null ? null : feed.record, run);
            }
            IncrementalAuthorityDecision replay = replayJointSuccess(
                    feed.record, run, command);
            if (replay != null) return replay;
            if (command.expectedRevision() != feed.record.revision()) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.STALE_REVISION,
                        feed.record, run);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (!jointSuccessIdentityMatches(feed.record, active, run,
                    command, proof)) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                        feed.record, run);
            }
            io.rowweave.run.ManifestCommitJournalSnapshot journal =
                    runs.manifestCommitJournal(active.fence());
            ManifestCommitIntent intent = journal == null ? null : journal.intent();
            byte[] decisionDigest = intent == null ? null : sha256(
                    ManifestCommitJournalCodec.encodeIntent(intent));
            if (!jointIntentMatches(intent, decisionDigest, proof)) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                        feed.record, run);
            }
            byte[] commandDigest = IncrementalAuthorityCommandCodec.digest(command);
            IncrementalCommitReceipt receipt = IncrementalCommitReceipt.committed(
                    active, proof.publicationId(), proof.generation(),
                    proof.manifestHash(), command.commandId(), commandDigest);
            IncrementalAuthorityRecord committed = committedRecord(
                    feed.record, active, receipt);
            RunResultSummary summary = RunResultSummary.manifestFrom(
                    journal.assignments(), proof.generation(), proof.manifestHash());
            long eventTime = Math.addExact(run.updatedAtMillis(), 1L);
            RunCommand terminal = RunCommand.advanceAttempt(
                    command.commandId(), run.runId(), run.revision(),
                    active.fence().attemptId(), active.fence().fenceToken(),
                    AttemptStatus.SUCCEEDED, false, eventTime,
                    "SUCCEEDED", null, summary);
            byte[] nextFeed = encodeFeed(
                    committed,
                    IncrementalAuthorityCommandCodec.encode(feed.initializeCommand),
                    IncrementalAuthorityCommandCodec.encode(command), null);
            RunDecision terminalDecision = runs.applyAugmented(
                    terminal, runToken, true,
                    (ignored, operations) -> appendFrontierChecksAndSet(
                            feed.record.feedId(), ownership, feedStat,
                            nextFeed, operations));
            if (terminalDecision.code() == RunDecision.Code.ACCEPTED) {
                return IncrementalAuthorityDecision.accepted(
                        committed, terminalDecision.snapshot());
            }
            if (terminalDecision.code() == RunDecision.Code.ALREADY_APPLIED) {
                FeedFrame after = readFeed(frontierToken.feedId(), new Stat());
                IncrementalAuthorityDecision confirmed = after == null ? null
                        : replayJointSuccess(
                        after.record, terminalDecision.snapshot(), command);
                if (confirmed != null) return confirmed;
                throw new IllegalStateException(
                        "joint success Run receipt exists without frontier receipt");
            }
            IncrementalAuthorityDecision.Code code = terminalDecision.code()
                    == RunDecision.Code.STALE_RECOVERY_OWNER
                    ? IncrementalAuthorityDecision.Code.STALE_OWNERSHIP
                    : IncrementalAuthorityDecision.Code.RUN_CONFLICT;
            return IncrementalAuthorityDecision.rejected(
                    code, feed.record, terminalDecision.snapshot());
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return IncrementalAuthorityDecision.rejected(
                            IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                            record(frontierToken.feedId()),
                            runs.get(runToken.runId()));
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (ArithmeticException invalidProjection) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    record(frontierToken.feedId()), runs.get(runToken.runId()));
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("commit incremental joint success", failure);
        }
    }

    private static IncrementalAuthorityDecision replayJointSuccess(
            IncrementalAuthorityRecord record, RunSnapshot run,
            IncrementalAuthorityCommand command) {
        if (record == null) return null;
        IncrementalCommitReceipt normal = record.lastCommitReceipt();
        IncrementalCommitReceipt backfill = record.lastBackfillReceipt();
        boolean normalId = normal != null
                && command.commandId().equals(normal.commandId());
        boolean backfillId = backfill != null
                && command.commandId().equals(backfill.commandId());
        if (!normalId && !backfillId) return null;
        IncrementalCommitReceipt selected = command.kind()
                == IncrementalAuthorityCommandKind.COMPLETE_BACKFILL
                ? backfill : normal;
        if (selected == null || normalId && backfillId
                || !MessageDigest.isEqual(
                selected.commandDigest(),
                IncrementalAuthorityCommandCodec.digest(command))) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                    record, run);
        }
        return IncrementalAuthorityDecision.replayed(record, run);
    }

    private static boolean jointSuccessIdentityMatches(
            IncrementalAuthorityRecord record, IncrementalActiveInterval active,
            RunSnapshot run, IncrementalAuthorityCommand command,
            VerifiedIncrementalJointCommitProof proof) {
        return record.state() == IncrementalFrontierAuthorityState.ACTIVE
                && active != null
                && active.phase() == IncrementalAttemptCommitPhase.COMMIT_DECIDED
                && command.feedIncarnation() == record.feedIncarnation()
                && command.intervalId().equals(active.identity().intervalId())
                && command.fence().equals(active.fence())
                && proof.feedId().equals(record.feedId())
                && proof.feedIncarnation() == record.feedIncarnation()
                && proof.intervalId().equals(active.identity().intervalId())
                && proof.capturedTargetReceiptDigest().equals(
                active.identity().capturedTargetReceiptDigest())
                && proof.mode() == active.capturedTarget().mode()
                && (proof.mode() == IncrementalRunMode.BACKFILL_V1)
                == (command.kind()
                == IncrementalAuthorityCommandKind.COMPLETE_BACKFILL)
                && MessageDigest.isEqual(
                proof.definitionFingerprint(),
                active.capturedTarget().definitionDigest())
                && proof.artifactId().equals(active.artifactId())
                && proof.fence().equals(active.fence())
                && run != null && run.currentAttempt() != null
                && run.runId().equals(active.fence().runId())
                && run.artifactId().equals(active.artifactId())
                && run.definitionId().equals(hex(record.definitionDigest()))
                && run.currentAttempt().attemptNumber() == active.attemptOrdinal()
                && run.currentAttempt().attemptId().equals(active.fence().attemptId())
                && run.currentAttempt().fenceToken() == active.fence().fenceToken()
                && run.currentAttempt().publicationId().equals(proof.publicationId())
                && (run.currentAttempt().status() == AttemptStatus.COMMITTING
                || run.currentAttempt().status() == AttemptStatus.RECONCILING);
    }

    private static boolean jointIntentMatches(
            ManifestCommitIntent intent, byte[] decisionDigest,
            VerifiedIncrementalJointCommitProof proof) {
        return intent != null && decisionDigest != null
                && intent.domainId().equals(proof.domainId())
                && intent.fence().equals(proof.fence())
                && intent.publicationId().equals(proof.publicationId())
                && intent.generation() == proof.generation()
                && MessageDigest.isEqual(
                intent.manifestHash(), proof.manifestHash())
                && MessageDigest.isEqual(
                decisionDigest, proof.commitDecisionDigest());
    }

    private static IncrementalAuthorityRecord committedRecord(
            IncrementalAuthorityRecord current, IncrementalActiveInterval active,
            IncrementalCommitReceipt receipt) {
        boolean backfill = active.capturedTarget().mode()
                == IncrementalRunMode.BACKFILL_V1;
        return new IncrementalAuthorityRecord(
                Math.addExact(current.revision(), 1L), current.feedId(),
                current.feedIncarnation(), current.definitionDigest(),
                current.sourceContractDigest(), current.frontierContractDigest(),
                current.frontierKind(), backfill
                ? current.committedFrontier() : active.capturedTarget().target(),
                null, backfill ? current.lastCommitReceipt() : receipt,
                backfill ? active.capturedTarget().expectedSequence()
                        : current.lastCompletedBackfillOrdinal(),
                backfill ? receipt : current.lastBackfillReceipt(),
                IncrementalFrontierAuthorityState.READY, null);
    }

    public IncrementalAuthorityDecision markConflict(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAuthorityCommand command,
            VerifiedIncrementalConflictProof proof) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        if (command == null || proof == null
                || command.kind() != IncrementalAuthorityCommandKind.MARK_CONFLICT
                || !frontierToken.feedId().equals(command.feedId())
                || frontierToken.feedIncarnation() != command.feedIncarnation()
                || !runToken.runId().equals(proof.fence().runId())
                || !command.intervalId().equals(proof.intervalId())
                || !command.fence().equals(proof.fence())
                || !MessageDigest.isEqual(
                command.transitionDigest(), proof.proofDigest())
                || !MessageDigest.isEqual(
                frontierToken.tokenDigest(),
                proof.frontierRecoveryTokenDigest())
                || !MessageDigest.isEqual(
                runToken.tokenDigest(), proof.runRecoveryTokenDigest())) {
            throw new IllegalArgumentException(
                    "conflict freeze requires its exact command, proof and ownership");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            RunSnapshot run = runs.get(runToken.runId());
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                        feed == null ? null : feed.record, run);
            }
            if (feed.lastCommand.commandId().equals(command.commandId())) {
                return feed.lastCommand.equals(command)
                        ? IncrementalAuthorityDecision.replayed(feed.record, run)
                        : IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                        feed.record, run);
            }
            if (retainedCommandIdMatches(feed.record, command.commandId())) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                        feed.record, run);
            }
            if (command.expectedRevision() != feed.record.revision()) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.STALE_REVISION,
                        feed.record, run);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (!conflictIdentityMatches(
                    feed.record, active, run, command, proof)) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                        feed.record, run);
            }
            io.rowweave.run.ManifestCommitJournalSnapshot journal =
                    runs.manifestCommitJournal(active.fence());
            ManifestCommitIntent intent = journal == null ? null : journal.intent();
            byte[] decisionDigest = intent == null ? null : sha256(
                    ManifestCommitJournalCodec.encodeIntent(intent));
            if (!conflictIntentMatches(intent, decisionDigest, proof)) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                        feed.record, run);
            }
            IncrementalAuthorityRecord frozen = conflictRecord(
                    feed.record, proof.proofDigest());
            byte[] nextFeed = encodeFeed(
                    frozen,
                    IncrementalAuthorityCommandCodec.encode(feed.initializeCommand),
                    IncrementalAuthorityCommandCodec.encode(command), null);
            if (run.currentAttempt().status() == AttemptStatus.RECONCILING) {
                ZkRunStateStore.AugmentedRecoveryCheck checked =
                        runs.checkRecoveredAugmented(
                                runToken, run,
                                (ignored, operations) ->
                                        appendFrontierChecksAndSet(
                                                feed.record.feedId(), ownership,
                                                feedStat, nextFeed, operations));
                if (checked == ZkRunStateStore.AugmentedRecoveryCheck.ACCEPTED) {
                    return IncrementalAuthorityDecision.accepted(frozen, run);
                }
                return IncrementalAuthorityDecision.rejected(
                        checked == ZkRunStateStore.AugmentedRecoveryCheck
                                .STALE_RECOVERY_OWNER
                                ? IncrementalAuthorityDecision.Code.STALE_OWNERSHIP
                                : IncrementalAuthorityDecision.Code.RUN_CONFLICT,
                        feed.record, runs.get(runToken.runId()));
            }
            long eventTime = Math.addExact(run.updatedAtMillis(), 1L);
            RunCommand reconciling = RunCommand.advanceAttempt(
                    command.commandId(), run.runId(), run.revision(),
                    active.fence().attemptId(), active.fence().fenceToken(),
                    AttemptStatus.RECONCILING, false, eventTime,
                    "CDC_AUTHORITY_CONFLICT", null);
            RunDecision runDecision = runs.applyAugmented(
                    reconciling, runToken, true,
                    (ignored, operations) -> appendFrontierChecksAndSet(
                            feed.record.feedId(), ownership, feedStat,
                            nextFeed, operations));
            if (runDecision.code() == RunDecision.Code.ACCEPTED) {
                return IncrementalAuthorityDecision.accepted(
                        frozen, runDecision.snapshot());
            }
            if (runDecision.code() == RunDecision.Code.ALREADY_APPLIED) {
                FeedFrame after = readFeed(frontierToken.feedId(), new Stat());
                if (after != null && after.lastCommand.equals(command)) {
                    return IncrementalAuthorityDecision.replayed(
                            after.record, runDecision.snapshot());
                }
                throw new IllegalStateException(
                        "conflict Run receipt exists without frontier freeze");
            }
            return IncrementalAuthorityDecision.rejected(
                    runDecision.code() == RunDecision.Code.STALE_RECOVERY_OWNER
                            ? IncrementalAuthorityDecision.Code.STALE_OWNERSHIP
                            : IncrementalAuthorityDecision.Code.RUN_CONFLICT,
                    feed.record, runDecision.snapshot());
        } catch (RunStateStoreUnavailableException raced) {
            try {
                FeedFrame after = readFeed(
                        frontierToken.feedId(), new Stat());
                if (after != null && after.lastCommand.equals(command)) {
                    return IncrementalAuthorityDecision.replayed(
                            after.record, runs.get(runToken.runId()));
                }
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return IncrementalAuthorityDecision.rejected(
                            IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                            record(frontierToken.feedId()),
                            runs.get(runToken.runId()));
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (ArithmeticException invalidProjection) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    record(frontierToken.feedId()), runs.get(runToken.runId()));
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("freeze incremental conflict", failure);
        }
    }

    private static boolean retainedCommandIdMatches(
            IncrementalAuthorityRecord record, String commandId) {
        IncrementalCommitReceipt normal = record.lastCommitReceipt();
        IncrementalCommitReceipt backfill = record.lastBackfillReceipt();
        return normal != null && commandId.equals(normal.commandId())
                || backfill != null && commandId.equals(backfill.commandId());
    }

    private static boolean conflictIdentityMatches(
            IncrementalAuthorityRecord record, IncrementalActiveInterval active,
            RunSnapshot run, IncrementalAuthorityCommand command,
            VerifiedIncrementalConflictProof proof) {
        return record.state() == IncrementalFrontierAuthorityState.ACTIVE
                && active != null
                && active.phase() == IncrementalAttemptCommitPhase.COMMIT_DECIDED
                && command.feedIncarnation() == record.feedIncarnation()
                && command.intervalId().equals(active.identity().intervalId())
                && command.fence().equals(active.fence())
                && proof.feedId().equals(record.feedId())
                && proof.feedIncarnation() == record.feedIncarnation()
                && proof.intervalId().equals(active.identity().intervalId())
                && proof.artifactId().equals(active.artifactId())
                && proof.fence().equals(active.fence())
                && run != null && run.currentAttempt() != null
                && run.runId().equals(active.fence().runId())
                && run.artifactId().equals(active.artifactId())
                && run.definitionId().equals(hex(record.definitionDigest()))
                && run.currentAttempt().attemptNumber() == active.attemptOrdinal()
                && run.currentAttempt().attemptId().equals(active.fence().attemptId())
                && run.currentAttempt().fenceToken() == active.fence().fenceToken()
                && (run.currentAttempt().status() == AttemptStatus.COMMITTING
                || run.currentAttempt().status() == AttemptStatus.RECONCILING);
    }

    private static boolean conflictIntentMatches(
            ManifestCommitIntent intent, byte[] decisionDigest,
            VerifiedIncrementalConflictProof proof) {
        return intent != null && decisionDigest != null
                && intent.domainId().equals(proof.domainId())
                && intent.fence().equals(proof.fence())
                && intent.publicationId().equals(proof.publicationId())
                && intent.generation() == proof.generation()
                && MessageDigest.isEqual(
                intent.manifestHash(), proof.manifestHash())
                && MessageDigest.isEqual(
                decisionDigest, proof.commitDecisionDigest());
    }

    private static IncrementalAuthorityRecord conflictRecord(
            IncrementalAuthorityRecord current, byte[] conflictDigest) {
        return new IncrementalAuthorityRecord(
                Math.addExact(current.revision(), 1L), current.feedId(),
                current.feedIncarnation(), current.definitionDigest(),
                current.sourceContractDigest(), current.frontierContractDigest(),
                current.frontierKind(), current.committedFrontier(),
                current.activeInterval(), current.lastCommitReceipt(),
                current.lastCompletedBackfillOrdinal(),
                current.lastBackfillReceipt(),
                IncrementalFrontierAuthorityState.CORRUPT_OR_CONFLICT,
                conflictDigest);
    }

    private static void requireMatchingRecoveryTokens(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken) {
        if (runToken == null || frontierToken == null
                || !frontierToken.owner().equals(runToken.owner())) {
            throw new IllegalArgumentException(
                    "matching frontier and Run recovery tokens are required");
        }
    }

    public RunSnapshot runSnapshot(String runId) {
        return runs.get(runId);
    }

    @Override
    public IncrementalAuthorityRecord activeRecordForRun(String runId) {
        if (runId == null || runId.isEmpty()) {
            throw new IllegalArgumentException("run id is required");
        }
        try {
            IncrementalAuthorityRecord match = null;
            List<String> children = client.getChildren().forPath(FEEDS);
            if (children.size() > MAX_FEEDS) {
                throw new IllegalStateException(
                        "incremental feed catalog exceeds bounded capacity");
            }
            for (String child : children) {
                final FeedFrame frame;
                try {
                    frame = decodeFeed(client.getData().forPath(
                            FEEDS + '/' + child));
                } catch (KeeperException.NoNodeException racedDelete) {
                    continue;
                }
                IncrementalActiveInterval active =
                        frame.record.activeInterval();
                if (active == null
                        || !runId.equals(active.fence().runId())) {
                    continue;
                }
                if (match != null
                        && !match.feedId().equals(frame.record.feedId())) {
                    throw new IllegalStateException(
                            "incremental Run is active in multiple feeds");
                }
                match = frame.record;
            }
            return match;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("find active incremental Run", failure);
        }
    }

    @Override
    public RecoveryToken runOwnership(String runId) {
        return runs.ownership(runId);
    }

    @Override
    public io.rowweave.run.StageAssignmentSnapshot assignmentSnapshot(
            io.rowweave.run.RunAttemptFence fence) {
        return runs.assignmentSnapshot(fence);
    }

    public WholeAttemptRetryDecision scheduleRetry(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            io.rowweave.run.RunAttemptFence fence) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        if (fence == null || !runToken.runId().equals(fence.runId())) {
            throw new IllegalArgumentException(
                    "retry fence must belong to the recovered Run");
        }
        RunSnapshot run = runs.get(runToken.runId());
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            IncrementalActiveInterval active = feed == null
                    ? null : feed.record.activeInterval();
            if (ownership == null
                    || feed.record.state()
                    != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null || !active.fence().equals(fence)) {
                return runs.staleRecoverySchedule(run);
            }
            if (active.phase()
                    == IncrementalAttemptCommitPhase.COMMIT_DECIDED) {
                return runs.tooLateSchedule(run);
            }
            final byte[] exhaustedClosure;
            if (run != null && run.attemptCount() >= run.maxAttempts()) {
                IncrementalAuthorityRecord closed = abortedRecord(feed.record);
                exhaustedClosure = encodeFeed(
                        closed,
                        IncrementalAuthorityCommandCodec.encode(
                                feed.initializeCommand),
                        IncrementalAuthorityCommandCodec.encode(
                                feed.initializeCommand), null);
            } else {
                exhaustedClosure = null;
            }
            WholeAttemptRetryDecision decision = runs.scheduleRetryAugmented(
                    runToken, fence, (ignored, operations) -> {
                        if (exhaustedClosure == null) {
                            appendFrontierChecks(
                                    feed.record.feedId(), ownership, feedStat,
                                    operations);
                        } else {
                            appendFrontierChecksAndSet(
                                    feed.record.feedId(), ownership, feedStat,
                                    exhaustedClosure, operations);
                        }
                    });
            if (decision.code() == WholeAttemptRetryDecision.Code.EXHAUSTED
                    && exhaustedClosure == null) {
                throw new IllegalStateException(
                        "retry exhausted without reserved frontier closure");
            }
            return decision;
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return runs.staleRecoverySchedule(
                            runs.get(runToken.runId()));
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (ArithmeticException exhausted) {
            return runs.unavailableSchedule(run);
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("schedule incremental whole-attempt retry", failure);
        }
    }

    public RetryAttemptClaim claimDueRetry(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        if (runId == null || !runToken.runId().equals(runId)) {
            throw new IllegalArgumentException(
                    "retry claim must belong to the recovered Run");
        }
        RunSnapshot run = runs.get(runId);
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            IncrementalActiveInterval active = feed == null
                    ? null : feed.record.activeInterval();
            if (ownership == null
                    || !runToken.equals(runs.ownership(runId))
                    || feed.record.state()
                    != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null) {
                return runs.staleRecoveryClaim(run);
            }
            if (feed.record.revision() == Long.MAX_VALUE
                    || run == null || run.revision() == Long.MAX_VALUE) {
                return runs.unavailableClaim(run);
            }
            if (run.currentAttempt() != null && run.attemptCount() > 1) {
                io.rowweave.run.RunAttemptFence currentFence =
                        new io.rowweave.run.RunAttemptFence(
                                runId, run.currentAttempt().attemptId(),
                                run.currentAttempt().fenceToken());
                if (active.fence().equals(currentFence)) {
                    return runs.claimDueRetryAugmented(
                            runToken, runId, (ignored, operations) ->
                                    appendFrontierChecks(
                                            feed.record.feedId(), ownership,
                                            feedStat, operations));
                }
            }
            RetryReadyEvaluation readiness = runs.evaluateRetryReady(runId);
            if (readiness.code() == RetryReadyEvaluation.Code.TOO_LATE) {
                evaluateDeadline(frontierToken, runToken, runId);
                return runs.tooLateClaim(runs.get(runId));
            }
            if (readiness.code() != RetryReadyEvaluation.Code.DUE
                    || readiness.identity() == null) {
                return runs.claimDueRetryAugmented(
                        runToken, runId, (ignored, operations) ->
                                appendFrontierChecks(
                                        feed.record.feedId(), ownership,
                                        feedStat, operations));
            }
            RetryAttemptIdentity identity = readiness.identity();
            io.rowweave.run.RunAttemptFence nextFence =
                    new io.rowweave.run.RunAttemptFence(
                            runId, identity.attemptId(), identity.fenceToken());
            IncrementalActiveInterval retried = new IncrementalActiveInterval(
                    active.identity(), active.capturedTarget(),
                    active.artifactId(), active.claimedRevision(), nextFence,
                    Math.addExact(active.attemptOrdinal(), 1),
                    IncrementalAttemptCommitPhase.CLAIMED, null);
            IncrementalAuthorityRecord projected = withActive(
                    feed.record, retried);
            byte[] nextFeed = encodeFeed(
                    projected,
                    IncrementalAuthorityCommandCodec.encode(
                            feed.initializeCommand),
                    IncrementalAuthorityCommandCodec.encode(
                            feed.initializeCommand), null);
            RetryAttemptClaim claim = runs.claimDueRetryAugmented(
                    runToken, runId,
                    (ignored, operations) -> appendFrontierChecksAndSet(
                            feed.record.feedId(), ownership, feedStat,
                            nextFeed, operations));
            if (claim.code() == RetryAttemptClaim.Code.ALREADY_CLAIMED) {
                FeedFrame after = readFeed(frontierToken.feedId(), new Stat());
                if (!retryProjectionMatches(after, claim)) {
                    throw new IllegalStateException(
                            "retry claim receipt exists without frontier projection");
                }
            }
            return claim;
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return runs.staleRecoveryClaim(runs.get(runId));
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (ArithmeticException | IllegalArgumentException exhausted) {
            return runs.unavailableClaim(run);
        } catch (IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("claim incremental retry attempt", failure);
        }
    }

    public RetryReadyEvaluation evaluateRetryReady(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        runId = WholeAttemptRetryAuthorityKernel.validateRunId(runId);
        if (!runToken.runId().equals(runId)) {
            throw new IllegalArgumentException(
                    "retry readiness must belong to the recovered Run");
        }
        RunSnapshot run = runs.get(runId);
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            IncrementalActiveInterval active = feed == null
                    ? null : feed.record.activeInterval();
            if (ownership == null
                    || !runToken.equals(runs.ownership(runId))
                    || feed.record.state()
                    != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null || !activeMatchesRun(active, run)) {
                return runs.staleRecoveryReady(run);
            }
            RetryReadyEvaluation result = runs.evaluateRetryReady(runId);
            if (!currentlyOwns(frontierToken, runToken)) {
                return runs.staleRecoveryReady(runs.get(runId));
            }
            return result;
        } catch (RunStateStoreUnavailableException unavailable) {
            throw unavailable;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("evaluate incremental retry readiness", failure);
        }
    }

    public RunCancellationResult requestUserCancellation(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId, String requestId) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        runId = RunCancellationAuthorityKernel.validateRunId(runId);
        requestId = RunCancellationAuthorityKernel.validateRequestId(requestId);
        if (!runToken.runId().equals(runId)) {
            throw new IllegalArgumentException(
                    "cancellation must belong to the recovered Run");
        }
        String commandId = RunCancellationAuthorityKernel.commandId(
                runId, requestId);
        RunSnapshot run = runs.get(runId);
        if (run == null) {
            return new RunCancellationResult(
                    runId, RunCancellationResult.Disposition.NOT_FOUND,
                    null, 0L);
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            IncrementalActiveInterval active = feed == null
                    ? null : feed.record.activeInterval();
            if (ownership == null
                    || !runToken.equals(runs.ownership(runId))) {
                return cancellationTooLate(runId, run);
            }
            RunCancellationAuthorityKernel.Plan plan =
                    RunCancellationAuthorityKernel.plan(
                            runId, commandId, run);
            if (feed.record.state()
                    != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null) {
                return plan.result() == null
                        ? cancellationTooLate(runId, run) : plan.result();
            }
            RunCommand command;
            if (active.phase()
                    == IncrementalAttemptCommitPhase.COMMIT_DECIDED
                    && run.currentAttempt() != null
                    && run.currentAttempt().status() == AttemptStatus.COMMITTING) {
                command = RunCommand.requestCancellation(
                        commandId, runId, run.revision(),
                        run.currentAttempt().attemptId(),
                        run.currentAttempt().fenceToken(),
                        CancellationCause.USER,
                        Math.max(run.updatedAtMillis(),
                                System.currentTimeMillis()),
                        RunCancellationAuthorityKernel.REASON_CODE, null);
            } else {
                if (plan.result() != null) return plan.result();
                command = plan.command();
            }
            if (!activeMatchesRun(active, run)) {
                return cancellationTooLate(runId, run);
            }
            boolean closeReadyRetry = run.status()
                    == io.rowweave.run.RunStatus.READY
                    && active.phase()
                    != IncrementalAttemptCommitPhase.COMMIT_DECIDED
                    && WholeAttemptRetryAuthorityKernel.retryWait(run) != null;
            final byte[] nextFeed;
            if (closeReadyRetry) {
                IncrementalAuthorityRecord closed = abortedRecord(feed.record);
                nextFeed = clearedCommandFeed(feed, closed);
            } else {
                nextFeed = null;
            }
            RunDecision decision = runs.applyAugmented(
                    command, runToken, true, (ignored, operations) -> {
                        if (nextFeed == null) {
                            appendFrontierChecks(
                                    feed.record.feedId(), ownership, feedStat,
                                    operations);
                        } else {
                            appendFrontierChecksAndSet(
                                    feed.record.feedId(), ownership, feedStat,
                                    nextFeed, operations);
                        }
                    });
            if (decision.code() == RunDecision.Code.ACCEPTED) {
                return RunCancellationAuthorityKernel.accepted(
                        decision.snapshot());
            }
            if (decision.code() == RunDecision.Code.ALREADY_APPLIED) {
                if (closeReadyRetry) {
                    FeedFrame after = readFeed(frontierToken.feedId(), new Stat());
                    if (after == null || after.record.state()
                            != IncrementalFrontierAuthorityState.READY
                            || after.record.activeInterval() != null) {
                        throw new IllegalStateException(
                                "cancellation receipt exists without frontier closure");
                    }
                }
                return new RunCancellationResult(
                        runId,
                        RunCancellationResult.Disposition.ALREADY_REQUESTED,
                        decision.snapshot().status(),
                        decision.snapshot().revision());
            }
            return cancellationTooLate(runId, decision.snapshot());
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return cancellationTooLate(runId, runs.get(runId));
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (ArithmeticException exhausted) {
            return cancellationTooLate(runId, runs.get(runId));
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("request incremental Run cancellation", failure);
        }
    }

    public RunDeadlineResult evaluateDeadline(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        runId = RunDeadlineAuthorityKernel.validateRunId(runId);
        if (!runToken.runId().equals(runId)) {
            throw new IllegalArgumentException(
                    "deadline evaluation must belong to the recovered Run");
        }
        RunSnapshot run = runs.get(runId);
        if (run == null) return RunDeadlineAuthorityKernel.missing(runId);
        if (run.deadlineEpochMillis() == 0L) {
            return RunDeadlineAuthorityKernel.plan(null, run, 0L).result();
        }
        long observed = runs.incrementalAuthorityTime();
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            IncrementalActiveInterval active = feed == null
                    ? null : feed.record.activeInterval();
            if (ownership == null
                    || !runToken.equals(runs.ownership(runId))) {
                return deadlineTooLate(runId, run, observed);
            }
            String commandId = RunDeadlineAuthorityKernel.commandId(run);
            RunDeadlineAuthorityKernel.Plan plan =
                    RunDeadlineAuthorityKernel.plan(
                            commandId, run, observed);
            if (feed.record.state()
                    != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null) {
                return plan.result() == null
                        ? deadlineTooLate(runId, run, observed) : plan.result();
            }
            RunCommand command;
            if (active.phase()
                    == IncrementalAttemptCommitPhase.COMMIT_DECIDED
                    && observed >= run.deadlineEpochMillis()
                    && run.currentAttempt() != null
                    && run.currentAttempt().status() == AttemptStatus.COMMITTING) {
                command = RunCommand.requestCancellation(
                        commandId, runId, run.revision(),
                        run.currentAttempt().attemptId(),
                        run.currentAttempt().fenceToken(),
                        CancellationCause.TIMEOUT,
                        Math.max(run.updatedAtMillis(), observed),
                        RunDeadlineAuthorityKernel.REASON_CODE, null);
            } else {
                if (plan.result() != null) return plan.result();
                command = plan.command();
            }
            if (!activeMatchesRun(active, run)) {
                return deadlineTooLate(runId, run, observed);
            }
            boolean closeReadyRetry = run.status()
                    == io.rowweave.run.RunStatus.READY
                    && active.phase()
                    != IncrementalAttemptCommitPhase.COMMIT_DECIDED
                    && WholeAttemptRetryAuthorityKernel.retryWait(run) != null;
            final byte[] nextFeed;
            if (closeReadyRetry) {
                IncrementalAuthorityRecord closed = abortedRecord(feed.record);
                nextFeed = clearedCommandFeed(feed, closed);
            } else {
                nextFeed = null;
            }
            RunDecision decision = runs.applyAugmented(
                    command, runToken, true, (ignored, operations) -> {
                        if (nextFeed == null) {
                            appendFrontierChecks(
                                    feed.record.feedId(), ownership, feedStat,
                                    operations);
                        } else {
                            appendFrontierChecksAndSet(
                                    feed.record.feedId(), ownership, feedStat,
                                    nextFeed, operations);
                        }
                    });
            if (decision.code() == RunDecision.Code.ACCEPTED) {
                return RunDeadlineAuthorityKernel.accepted(
                        decision.snapshot(), observed);
            }
            if (decision.code() == RunDecision.Code.ALREADY_APPLIED) {
                if (closeReadyRetry) {
                    FeedFrame after = readFeed(frontierToken.feedId(), new Stat());
                    if (after == null || after.record.state()
                            != IncrementalFrontierAuthorityState.READY
                            || after.record.activeInterval() != null) {
                        throw new IllegalStateException(
                                "deadline receipt exists without frontier closure");
                    }
                }
                return new RunDeadlineResult(
                        runId, RunDeadlineResult.Disposition.ALREADY_REQUESTED,
                        decision.snapshot().status(),
                        decision.snapshot().revision(),
                        decision.snapshot().deadlineEpochMillis(), observed);
            }
            return deadlineTooLate(runId,
                    decision.snapshot() == null ? run : decision.snapshot(),
                    observed);
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return deadlineTooLate(runId, runs.get(runId), observed);
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (ArithmeticException exhausted) {
            return deadlineTooLate(runId, runs.get(runId), observed);
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("evaluate incremental Run deadline", failure);
        }
    }

    public io.rowweave.run.ManifestCommitJournalSnapshot manifestCommitJournal(
            io.rowweave.run.RunAttemptFence fence) {
        return runs.manifestCommitJournal(fence);
    }

    public BatchSinkAbortRecord batchSinkAbort(
            io.rowweave.run.RunAttemptFence fence) {
        return runs.batchSinkAbort(fence);
    }

    public boolean currentlyOwns(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        try {
            RecoveryToken currentRun = runs.ownership(runToken.runId());
            return runToken.equals(currentRun)
                    && currentlyOwned(frontierToken.feedId(), frontierToken);
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("verify incremental dual ownership", failure);
        }
    }

    public VerifiedIncrementalAbortClosureProof verifyAbortClosure(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            io.rowweave.run.RunAttemptFence fence,
            AttemptStatus terminalStatus) {
        return ZkIncrementalAbortProofAuthority.verify(
                this, frontierToken, runToken, fence, terminalStatus);
    }

    public IncrementalAuthorityDecision closeAborted(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAuthorityCommand command,
            VerifiedIncrementalAbortClosureProof proof) {
        requireMatchingRecoveryTokens(frontierToken, runToken);
        if (command == null || proof == null
                || command.kind()
                != IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN
                || !frontierToken.feedId().equals(command.feedId())
                || frontierToken.feedIncarnation() != command.feedIncarnation()
                || !runToken.runId().equals(proof.fence().runId())
                || !command.intervalId().equals(proof.intervalId())
                || !command.fence().equals(proof.fence())
                || !MessageDigest.isEqual(
                command.transitionDigest(), proof.proofDigest())
                || !MessageDigest.isEqual(
                frontierToken.tokenDigest(),
                proof.frontierRecoveryTokenDigest())
                || !MessageDigest.isEqual(
                runToken.tokenDigest(), proof.runRecoveryTokenDigest())) {
            throw new IllegalArgumentException(
                    "failed close requires its exact command, proof and ownership");
        }
        try {
            Stat feedStat = new Stat();
            FeedFrame feed = readFeed(frontierToken.feedId(), feedStat);
            RunSnapshot run = runs.get(runToken.runId());
            OwnershipView ownership = feed == null ? null : ownershipView(
                    feed, feedStat, frontierToken);
            if (ownership == null) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                        feed == null ? null : feed.record, run);
            }
            if (feed.lastCommand.commandId().equals(command.commandId())) {
                return feed.lastCommand.equals(command)
                        ? IncrementalAuthorityDecision.replayed(feed.record, run)
                        : IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                        feed.record, run);
            }
            if (retainedCommandIdMatches(feed.record, command.commandId())) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                        feed.record, run);
            }
            if (command.expectedRevision() != feed.record.revision()) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.STALE_REVISION,
                        feed.record, run);
            }
            IncrementalActiveInterval active = feed.record.activeInterval();
            if (feed.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                    || active == null || !active.fence().equals(command.fence())
                    || run == null) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                        feed.record, run);
            }
            VerifiedIncrementalAbortClosureProof fresh = verifyAbortClosure(
                    frontierToken, runToken, command.fence(),
                    proof.terminalStatus());
            if (!MessageDigest.isEqual(
                    fresh.proofDigest(), proof.proofDigest())) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                        feed.record, run);
            }
            BatchSinkAbortRecord abort = runs.batchSinkAbort(command.fence());
            IncrementalAuthorityRecord closed = abortedRecord(feed.record);
            long eventTime = Math.addExact(run.updatedAtMillis(), 1L);
            RunCommand terminal = RunCommand.verifiedAbortClosureTerminal(
                    command.commandId(), run.runId(), run.revision(),
                    active.fence().attemptId(), active.fence().fenceToken(),
                    proof.terminalStatus(), eventTime,
                    abort.reasonCode());
            byte[] nextFeed = encodeFeed(
                    closed,
                    IncrementalAuthorityCommandCodec.encode(feed.initializeCommand),
                    IncrementalAuthorityCommandCodec.encode(command), null);
            RunDecision terminalDecision = runs.applyAugmented(
                    terminal, runToken, true,
                    (ignored, operations) -> appendFrontierChecksAndSet(
                            feed.record.feedId(), ownership, feedStat,
                            nextFeed, operations));
            if (terminalDecision.code() == RunDecision.Code.ACCEPTED) {
                return IncrementalAuthorityDecision.accepted(
                        closed, terminalDecision.snapshot());
            }
            if (terminalDecision.code() == RunDecision.Code.ALREADY_APPLIED) {
                FeedFrame after = readFeed(frontierToken.feedId(), new Stat());
                if (after != null && after.lastCommand.equals(command)) {
                    return IncrementalAuthorityDecision.replayed(
                            after.record, terminalDecision.snapshot());
                }
                throw new IllegalStateException(
                        "failed Run receipt exists without frontier closure");
            }
            return IncrementalAuthorityDecision.rejected(
                    terminalDecision.code() == RunDecision.Code.STALE_RECOVERY_OWNER
                            ? IncrementalAuthorityDecision.Code.STALE_OWNERSHIP
                            : IncrementalAuthorityDecision.Code.RUN_CONFLICT,
                    feed.record, terminalDecision.snapshot());
        } catch (RunStateStoreUnavailableException raced) {
            try {
                if (!currentlyOwned(frontierToken.feedId(), frontierToken)) {
                    return IncrementalAuthorityDecision.rejected(
                            IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                            record(frontierToken.feedId()),
                            runs.get(runToken.runId()));
                }
            } catch (Exception confirmation) {
                raced.addSuppressed(confirmation);
            }
            throw raced;
        } catch (ArithmeticException invalidProjection) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    record(frontierToken.feedId()), runs.get(runToken.runId()));
        } catch (IllegalArgumentException invalidEvidence) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    record(frontierToken.feedId()), runs.get(runToken.runId()));
        } catch (IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable("close incremental failed Run", failure);
        }
    }

    private static IncrementalAuthorityRecord abortedRecord(
            IncrementalAuthorityRecord current) {
        return new IncrementalAuthorityRecord(
                Math.addExact(current.revision(), 1L), current.feedId(),
                current.feedIncarnation(), current.definitionDigest(),
                current.sourceContractDigest(), current.frontierContractDigest(),
                current.frontierKind(), current.committedFrontier(), null,
                current.lastCommitReceipt(),
                current.lastCompletedBackfillOrdinal(),
                current.lastBackfillReceipt(),
                IncrementalFrontierAuthorityState.READY, null);
    }

    private static IncrementalAuthorityRecord withActive(
            IncrementalAuthorityRecord record,
            IncrementalActiveInterval active) {
        return new IncrementalAuthorityRecord(
                Math.addExact(record.revision(), 1L), record.feedId(),
                record.feedIncarnation(), record.definitionDigest(),
                record.sourceContractDigest(), record.frontierContractDigest(),
                record.frontierKind(), record.committedFrontier(), active,
                record.lastCommitReceipt(), record.lastCompletedBackfillOrdinal(),
                record.lastBackfillReceipt(),
                IncrementalFrontierAuthorityState.ACTIVE, null);
    }

    private static boolean activeMatchesRun(
            IncrementalActiveInterval active, RunSnapshot run) {
        if (active == null || run == null || run.currentAttempt() == null) {
            return false;
        }
        return active.fence().equals(new io.rowweave.run.RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken()));
    }

    private static RunCancellationResult cancellationTooLate(
            String runId, RunSnapshot run) {
        return run == null
                ? new RunCancellationResult(
                runId, RunCancellationResult.Disposition.NOT_FOUND, null, 0L)
                : new RunCancellationResult(
                runId, RunCancellationResult.Disposition.TOO_LATE,
                run.status(), run.revision());
    }

    private static RunDeadlineResult deadlineTooLate(
            String runId, RunSnapshot run, long observed) {
        return run == null
                ? RunDeadlineAuthorityKernel.missing(runId)
                : new RunDeadlineResult(
                run.runId(), RunDeadlineResult.Disposition.TOO_LATE,
                run.status(), run.revision(), run.deadlineEpochMillis(), observed);
    }

    private static byte[] clearedCommandFeed(
            FeedFrame feed, IncrementalAuthorityRecord record) {
        byte[] initialize = IncrementalAuthorityCommandCodec.encode(
                feed.initializeCommand);
        return encodeFeed(record, initialize, initialize, null);
    }

    private static boolean retryProjectionMatches(
            FeedFrame feed, RetryAttemptClaim claim) {
        if (feed == null || claim == null || claim.snapshot() == null
                || claim.snapshot().currentAttempt() == null
                || feed.record.activeInterval() == null) {
            return false;
        }
        io.rowweave.run.RunAttemptFence runFence =
                new io.rowweave.run.RunAttemptFence(
                        claim.snapshot().runId(),
                        claim.snapshot().currentAttempt().attemptId(),
                        claim.snapshot().currentAttempt().fenceToken());
        return feed.record.activeInterval().fence().equals(runFence)
                && feed.record.activeInterval().attemptOrdinal()
                == claim.snapshot().attemptCount();
    }

    private static IncrementalAuthorityRecord withActivePhase(
            IncrementalAuthorityRecord record,
            IncrementalAttemptCommitPhase phase, byte[] commitDecisionDigest) {
        IncrementalActiveInterval active = record.activeInterval();
        IncrementalActiveInterval projected = new IncrementalActiveInterval(
                active.identity(), active.capturedTarget(), active.artifactId(),
                active.claimedRevision(), active.fence(), active.attemptOrdinal(),
                phase, commitDecisionDigest);
        return new IncrementalAuthorityRecord(
                Math.addExact(record.revision(), 1L), record.feedId(),
                record.feedIncarnation(), record.definitionDigest(),
                record.sourceContractDigest(), record.frontierContractDigest(),
                record.frontierKind(), record.committedFrontier(), projected,
                record.lastCommitReceipt(), record.lastCompletedBackfillOrdinal(),
                record.lastBackfillReceipt(),
                IncrementalFrontierAuthorityState.ACTIVE, null);
    }

    @Override
    public FrontierOwnershipClaim claimOwnership(
            String feedId, long expectedRevision, long expectedOwnershipEpoch,
            RecoveryOwner owner) {
        requireFeedId(feedId);
        if (expectedRevision < 0L || expectedOwnershipEpoch < 0L || owner == null) {
            throw new IllegalArgumentException("ownership expectation and owner are required");
        }
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                Stat feedStat = new Stat();
                FeedFrame feed = readFeed(feedId, feedStat);
                if (feed == null) {
                    return FrontierOwnershipClaim.rejected(
                            FrontierOwnershipClaim.Code.FEED_NOT_FOUND);
                }
                if (feed.record.revision() != expectedRevision) {
                    return FrontierOwnershipClaim.rejected(
                            FrontierOwnershipClaim.Code.STALE_REVISION);
                }
                GuardVersion guard = readGuard(feedId);
                FeedGuardShardObservation observation = observation(feedId, guard);
                if (!Long.valueOf(feed.record.feedIncarnation()).equals(
                        guard.entries.get(feedHash(feedId)))) {
                    throw new IllegalStateException(
                            "feed incarnation guard does not match frontier record");
                }
                String ownerPath = feedPath(feedId) + "/recovery-owner";
                Stat ownerStat = new Stat();
                FrontierRecoveryToken current = readOwner(ownerPath, ownerStat);
                if (current != null && expectedOwnershipEpoch + 1L
                        == current.ownershipEpoch() && owner.equals(current.owner())
                        && expectedRevision == current.claimedRevision()
                        && feed.record.feedIncarnation() == current.feedIncarnation()
                        && feedStat.getCzxid() == current.feedRootCzxid()
                        && observation.equals(current.guardShardObservation())) {
                    return FrontierOwnershipClaim.replayed(current);
                }
                long currentEpoch = current == null ? 0L : current.ownershipEpoch();
                if (expectedOwnershipEpoch != currentEpoch) {
                    return FrontierOwnershipClaim.rejected(
                            FrontierOwnershipClaim.Code.LOST_RACE);
                }
                FrontierRecoveryToken token = FrontierRecoveryToken.create(
                        feedId, owner, next(currentEpoch), expectedRevision,
                        feed.record.feedIncarnation(), observation, feedStat.getCzxid());
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check()
                        .withVersion(feedStat.getVersion()).forPath(feedPath(feedId)));
                operations.add(client.transactionOp().check()
                        .withVersion(guard.version).forPath(guard.path));
                if (current == null) {
                    operations.add(client.transactionOp().create().forPath(
                            ownerPath, FrontierRecoveryTokenCodec.encode(token)));
                } else {
                    operations.add(client.transactionOp().setData()
                            .withVersion(ownerStat.getVersion()).forPath(
                                    ownerPath, FrontierRecoveryTokenCodec.encode(token)));
                }
                client.transaction().forOperations(operations);
                afterOwnershipCommit.run();
                return FrontierOwnershipClaim.acquired(token);
            } catch (KeeperException.NoNodeException missing) {
                lastRace = missing;
            } catch (KeeperException.NodeExistsException
                    | KeeperException.BadVersionException race) {
                lastRace = race;
            } catch (KeeperException.ConnectionLossException unknown) {
                lastRace = unknown;
            } catch (Exception failure) {
                throw unavailable("claim incremental frontier ownership", failure);
            }
        }
        throw unavailable("claim incremental frontier ownership after retries", lastRace);
    }

    @Override
    public FrontierRecoveryToken frontierOwnership(String feedId) {
        requireFeedId(feedId);
        try {
            return readOwner(feedPath(feedId) + "/recovery-owner", new Stat());
        } catch (Exception failure) {
            throw unavailable("read incremental frontier ownership", failure);
        }
    }

    static String feedPath(String feedId) {
        return FEEDS + "/" + feedHash(requireFeedId(feedId));
    }

    private OwnershipView ownershipView(
            FeedFrame feed, Stat feedStat, FrontierRecoveryToken token)
            throws Exception {
        if (token == null || !feed.record.feedId().equals(token.feedId())
                || feed.record.feedIncarnation() != token.feedIncarnation()
                || feedStat.getCzxid() != token.feedRootCzxid()) {
            return null;
        }
        GuardVersion guard = readGuard(feed.record.feedId());
        FeedGuardShardObservation currentObservation =
                observation(feed.record.feedId(), guard);
        if (!currentObservation.equals(token.guardShardObservation())) {
            return null;
        }
        Stat ownerStat = new Stat();
        FrontierRecoveryToken current = readOwner(
                feedPath(feed.record.feedId()) + "/recovery-owner", ownerStat);
        return token.equals(current)
                ? new OwnershipView(guard, ownerStat.getVersion()) : null;
    }

    private void appendFrontierChecksAndSet(
            String feedId, OwnershipView ownership, Stat feedStat, byte[] nextFeed,
            List<CuratorOp> operations) throws Exception {
        appendFrontierChecks(feedId, ownership, feedStat, operations);
        String path = feedPath(feedId);
        operations.add(client.transactionOp().setData()
                .withVersion(feedStat.getVersion()).forPath(
                        path, nextFeed));
    }

    private void appendFrontierChecks(
            String feedId, OwnershipView ownership, Stat feedStat,
            List<CuratorOp> operations) throws Exception {
        String path = feedPath(feedId);
        operations.add(client.transactionOp().check()
                .withVersion(feedStat.getVersion()).forPath(
                        path));
        operations.add(client.transactionOp().check()
                .withVersion(ownership.guard.version).forPath(
                        ownership.guard.path));
        operations.add(client.transactionOp().check()
                .withVersion(ownership.ownerVersion).forPath(
                        path + "/recovery-owner"));
    }

    private boolean currentlyOwned(
            String feedId, FrontierRecoveryToken token) throws Exception {
        Stat feedStat = new Stat();
        FeedFrame feed = readFeed(feedId, feedStat);
        return feed != null && ownershipView(feed, feedStat, token) != null;
    }

    private static IncrementalAuthorityCommand claimCommand(
            String commandId, IncrementalClaimRequest request,
            long expectedRevision) {
        return new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM,
                request.feedId(), commandId, expectedRevision,
                request.capturedTarget().feedIncarnation(), request.intervalId(),
                request.firstAttemptFence(), request.transitionDigest());
    }

    private static boolean claimApplied(
            FeedFrame feed, RunSnapshot run, String commandId,
            IncrementalClaimRequest request) {
        if (feed == null || !activeMatches(feed.record, request)
                || !runAttemptMatches(run, request)) {
            return false;
        }
        long claimedRevision = feed.record.activeInterval().claimedRevision();
        return feed.lastCommand.equals(
                claimCommand(commandId, request, claimedRevision));
    }

    private static RunCommand runProgress(
            IncrementalAttemptProgressCommand command) {
        return RunCommand.advanceAttempt(
                command.commandId(), command.fence().runId(),
                command.expectedRunRevision(), command.fence().attemptId(),
                command.fence().fenceToken(), command.targetStatus(), false,
                command.eventTimeMillis(), command.targetStatus().name(), null);
    }

    private static RunDecision staleRunRevision(
            RunSnapshot current, IncrementalAttemptProgressCommand command) {
        long stale = current.revision() == Long.MAX_VALUE
                ? current.revision() - 1L : current.revision() + 1L;
        return RunStateStoreKernel.evaluate(current, RunCommand.advanceAttempt(
                command.commandId(), current.runId(), stale,
                command.fence().attemptId(), command.fence().fenceToken(),
                command.targetStatus(), false, command.eventTimeMillis(),
                command.targetStatus().name(), null));
    }

    private static RunDecision staleRunFence(
            RunSnapshot current, IncrementalAttemptProgressCommand command) {
        long currentFence = current.currentAttempt() == null
                ? 1L : current.currentAttempt().fenceToken();
        long staleFence = currentFence == Long.MAX_VALUE
                ? currentFence - 1L : currentFence + 1L;
        return RunStateStoreKernel.evaluate(current, RunCommand.advanceAttempt(
                command.commandId(), current.runId(), current.revision(),
                "stale-incremental-progress", staleFence,
                command.targetStatus(), false, command.eventTimeMillis(),
                command.targetStatus().name(), null));
    }

    private static IncrementalPendingRunDecision pending(
            IncrementalPendingRunDecision.Code code, RunSnapshot run) {
        return new IncrementalPendingRunDecision(code, run);
    }

    private static IncrementalPendingRunDecision.Code pendingAdmission(
            IncrementalAuthorityRecord record, IncrementalClaimRequest request) {
        CapturedTargetReceipt capture = request.capturedTarget();
        if (!record.feedId().equals(request.feedId())
                || capture.feedIncarnation() != record.feedIncarnation()
                || !MessageDigest.isEqual(
                capture.definitionDigest(), record.definitionDigest())
                || !MessageDigest.isEqual(
                capture.sourceContractDigest(), record.sourceContractDigest())
                || !MessageDigest.isEqual(
                request.frontierContractDigest(), record.frontierContractDigest())
                || capture.target().kind() != record.frontierKind()) {
            return IncrementalPendingRunDecision.Code.CONTRACT_CONFLICT;
        }
        if (capture.mode() == IncrementalRunMode.BACKFILL_V1) {
            if (record.lastCompletedBackfillOrdinal() == Long.MAX_VALUE
                    || capture.expectedSequence()
                    != record.lastCompletedBackfillOrdinal() + 1L) {
                return IncrementalPendingRunDecision.Code.CONTRACT_CONFLICT;
            }
        } else if (capture.expectedSequence() != record.revision()
                || !capture.from().equals(record.committedFrontier())) {
            return IncrementalPendingRunDecision.Code.CONTRACT_CONFLICT;
        }
        return capture.mode() == IncrementalRunMode.INCREMENTAL_V1
                && capture.from().equals(capture.target())
                ? IncrementalPendingRunDecision.Code.NO_OP : null;
    }

    private static boolean pendingRunMatches(
            RunSnapshot run, IncrementalClaimRequest request) {
        return run != null
                && run.status() == io.rowweave.run.RunStatus.READY
                && run.attemptCount() == 0 && run.currentAttempt() == null
                && run.runId().equals(request.runId())
                && run.artifactId().equals(request.artifactId())
                && run.definitionId().equals(hex(
                request.capturedTarget().definitionDigest()));
    }

    private static boolean runAttemptMatches(
            RunSnapshot run, IncrementalClaimRequest request) {
        return run != null && run.currentAttempt() != null
                && run.runId().equals(request.runId())
                && run.currentAttempt().attemptId().equals(request.attemptId())
                && run.currentAttempt().publicationId().equals(
                request.publicationId())
                && run.currentAttempt().fenceToken()
                == request.firstAttemptFence().fenceToken();
    }

    private static boolean activeMatches(
            IncrementalAuthorityRecord record,
            IncrementalClaimRequest request) {
        return record.activeInterval() != null
                && record.activeInterval().identity().intervalId().equals(
                request.intervalId())
                && record.activeInterval().capturedTarget().equals(
                request.capturedTarget())
                && record.activeInterval().artifactId().equals(
                request.artifactId())
                && record.activeInterval().fence().equals(
                request.firstAttemptFence());
    }

    private IncrementalAuthorityDecision replayInitialize(
            FeedFrame existing, IncrementalAuthorityCommand command) {
        if (existing.initializeCommand.equals(command)) {
            return IncrementalAuthorityDecision.replayed(existing.record, null);
        }
        return IncrementalAuthorityDecision.rejected(
                IncrementalAuthorityDecision.Code.FEED_CONFLICT,
                existing.record, null);
    }

    private static IncrementalAuthorityRecord readyRecord(
            IncrementalInitializeRequest request) {
        return new IncrementalAuthorityRecord(
                0L, request.feedId(), request.feedIncarnation(),
                request.definitionDigest(), request.sourceContractDigest(),
                request.frontierContractDigest(), request.frontierKind(),
                IncrementalFrontier.begin(), null, null, 0L, null,
                IncrementalFrontierAuthorityState.READY, null);
    }

    private FeedFrame readFeed(String feedId, Stat stat) throws Exception {
        try {
            byte[] bytes = stat == null
                    ? client.getData().forPath(feedPath(feedId))
                    : client.getData().storingStatIn(stat).forPath(feedPath(feedId));
            return decodeFeed(bytes);
        } catch (KeeperException.NoNodeException absent) {
            return null;
        }
    }

    private FrontierRecoveryToken readOwner(String path, Stat stat) throws Exception {
        try {
            return FrontierRecoveryTokenCodec.decode(
                    client.getData().storingStatIn(stat).forPath(path));
        } catch (KeeperException.NoNodeException absent) {
            return null;
        }
    }

    private GuardVersion readGuard(String feedId) throws Exception {
        int index = guardShardIndex(feedId);
        String path = guardPath(index);
        ensureGuard(path, index);
        Stat stat = new Stat();
        GuardFrame frame = decodeGuard(
                client.getData().storingStatIn(stat).forPath(path));
        if (frame.index != index) {
            throw new IllegalArgumentException("incremental guard shard index mismatch");
        }
        return new GuardVersion(path, index, stat.getVersion(),
                frame.revision, frame.entries, encodeGuard(frame));
    }

    private void ensureGuard(String path, int index) throws Exception {
        try {
            client.create().forPath(path, encodeGuard(
                    new GuardFrame(index, 0L, new TreeMap<>())));
        } catch (KeeperException.NodeExistsException exists) {
            // Concurrent construction converges on the same canonical empty shard.
        }
    }

    private void ensurePersistent(String path) throws Exception {
        try {
            client.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        } catch (KeeperException.NodeExistsException exists) {
            // Existing persistent namespace is authoritative.
        }
    }

    private static FeedGuardShardObservation observation(
            String feedId, GuardVersion guard) {
        if (!guard.entries.containsKey(feedHash(feedId)) || guard.revision <= 0L) {
            throw new IllegalStateException("feed incarnation guard entry is absent");
        }
        return new FeedGuardShardObservation(
                guard.index, guard.revision, sha256(guard.encoded));
    }

    private static byte[] encodeFeed(
            IncrementalAuthorityRecord record, byte[] initializeCommandFrame,
            byte[] lastCommandFrame, IncrementalClaimRequest pending) {
        try {
            byte[] recordFrame = IncrementalAuthorityRecordCodec.encode(record);
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bodyBytes);
            out.writeInt(FEED_MAGIC);
            out.writeByte(VERSION);
            writeBytes(out, initializeCommandFrame);
            writeBytes(out, lastCommandFrame);
            writeBytes(out, recordFrame);
            out.writeBoolean(pending != null);
            if (pending != null) {
                writeBytes(out, IncrementalClaimRequestCodec.encode(pending));
            }
            out.flush();
            return checkedFrame(bodyBytes.toByteArray(),
                    IncrementalAuthorityRecordCodec.MAX_FRAME_BYTES
                            + 2 * IncrementalAuthorityCommandCodec.MAX_FRAME_BYTES
                            + IncrementalClaimRequestCodec.MAX_FRAME_BYTES + 256);
        } catch (IOException impossible) {
            throw new IllegalStateException("encode incremental ZK feed failed", impossible);
        }
    }

    private static FeedFrame decodeFeed(byte[] frame) {
        byte[] body = checkedBody(frame,
                IncrementalAuthorityRecordCodec.MAX_FRAME_BYTES
                        + 2 * IncrementalAuthorityCommandCodec.MAX_FRAME_BYTES
                        + IncrementalClaimRequestCodec.MAX_FRAME_BYTES + 256);
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
            if (in.readInt() != FEED_MAGIC || in.readUnsignedByte() != VERSION) {
                throw new IllegalArgumentException("incremental ZK feed header is invalid");
            }
            IncrementalAuthorityCommand initialize = IncrementalAuthorityCommandCodec.decode(
                    readBytes(in, IncrementalAuthorityCommandCodec.MAX_FRAME_BYTES));
            IncrementalAuthorityCommand last = IncrementalAuthorityCommandCodec.decode(
                    readBytes(in, IncrementalAuthorityCommandCodec.MAX_FRAME_BYTES));
            IncrementalAuthorityRecord record = IncrementalAuthorityRecordCodec.decode(
                    readBytes(in, IncrementalAuthorityRecordCodec.MAX_FRAME_BYTES));
            IncrementalClaimRequest pending = in.readBoolean()
                    ? IncrementalClaimRequestCodec.decode(readBytes(
                    in, IncrementalClaimRequestCodec.MAX_FRAME_BYTES)) : null;
            IncrementalInitializeRequest request = new IncrementalInitializeRequest(
                    record.feedId(), record.feedIncarnation(), record.definitionDigest(),
                    record.sourceContractDigest(), record.frontierContractDigest(),
                    record.frontierKind());
            if (in.read() != -1 || initialize.kind()
                    != IncrementalAuthorityCommandKind.INITIALIZE
                    || !initialize.feedId().equals(record.feedId())
                    || initialize.feedIncarnation() != record.feedIncarnation()
                    || !MessageDigest.isEqual(
                    initialize.transitionDigest(), request.transitionDigest())
                    || !last.feedId().equals(record.feedId())
                    || last.feedIncarnation() != record.feedIncarnation()
                    || pending != null && (!pending.feedId().equals(record.feedId())
                    || pending.capturedTarget().feedIncarnation()
                    != record.feedIncarnation())) {
                throw new IllegalArgumentException("incremental ZK feed binding is invalid");
            }
            return new FeedFrame(record, initialize, last, pending);
        } catch (IOException malformed) {
            throw new IllegalArgumentException("incremental ZK feed is malformed", malformed);
        }
    }

    private static byte[] encodeGuard(GuardFrame guard) {
        if (guard.index < 0 || guard.index >= FeedGuardShardObservation.SHARD_COUNT
                || guard.revision < 0L || guard.entries.size() > MAX_GUARD_ENTRIES) {
            throw new IllegalArgumentException("incremental guard shard is invalid");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bodyBytes);
            out.writeInt(GUARD_MAGIC);
            out.writeByte(VERSION);
            out.writeShort(guard.index);
            out.writeLong(guard.revision);
            out.writeByte(guard.entries.size());
            for (Map.Entry<String, Long> entry : guard.entries.entrySet()) {
                byte[] hash = hexBytes(entry.getKey());
                if (hash.length != 32 || entry.getValue() <= 0L) {
                    throw new IllegalArgumentException("incremental guard entry is invalid");
                }
                out.write(hash);
                out.writeLong(entry.getValue());
            }
            out.flush();
            return checkedFrame(bodyBytes.toByteArray(), 1024);
        } catch (IOException impossible) {
            throw new IllegalStateException("encode incremental guard failed", impossible);
        }
    }

    private static GuardFrame decodeGuard(byte[] frame) {
        byte[] body = checkedBody(frame, 1024);
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
            if (in.readInt() != GUARD_MAGIC || in.readUnsignedByte() != VERSION) {
                throw new IllegalArgumentException("incremental guard header is invalid");
            }
            int index = in.readUnsignedShort();
            long revision = in.readLong();
            int count = in.readUnsignedByte();
            if (index >= FeedGuardShardObservation.SHARD_COUNT || revision < 0L
                    || count > MAX_GUARD_ENTRIES) {
                throw new IllegalArgumentException("incremental guard shape is invalid");
            }
            TreeMap<String, Long> entries = new TreeMap<>();
            String previous = null;
            for (int i = 0; i < count; i++) {
                byte[] hash = new byte[32];
                in.readFully(hash);
                String key = hex(hash);
                long incarnation = in.readLong();
                if (incarnation <= 0L || previous != null
                        && previous.compareTo(key) >= 0) {
                    throw new IllegalArgumentException("incremental guard order is invalid");
                }
                entries.put(key, incarnation);
                previous = key;
            }
            if (in.read() != -1) {
                throw new IllegalArgumentException("incremental guard trailing bytes");
            }
            GuardFrame decoded = new GuardFrame(index, revision, entries);
            if (!Arrays.equals(frame, encodeGuard(decoded))) {
                throw new IllegalArgumentException("incremental guard is not canonical");
            }
            return decoded;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("incremental guard is malformed", malformed);
        }
    }

    private static byte[] checkedFrame(byte[] body, int maxBytes) {
        if (body.length + CHECKSUM_BYTES > maxBytes) {
            throw new IllegalArgumentException("incremental ZK frame exceeds limit");
        }
        byte[] frame = Arrays.copyOf(body, body.length + CHECKSUM_BYTES);
        System.arraycopy(sha256(body), 0, frame, body.length, CHECKSUM_BYTES);
        return frame;
    }

    private static byte[] checkedBody(byte[] frame, int maxBytes) {
        if (frame == null || frame.length <= CHECKSUM_BYTES || frame.length > maxBytes) {
            throw new IllegalArgumentException("incremental ZK frame length is invalid");
        }
        byte[] body = Arrays.copyOf(frame, frame.length - CHECKSUM_BYTES);
        byte[] checksum = Arrays.copyOfRange(frame, body.length, frame.length);
        if (!MessageDigest.isEqual(sha256(body), checksum)) {
            throw new IllegalArgumentException("incremental ZK frame checksum mismatch");
        }
        return body;
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in, int maxBytes) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maxBytes) {
            throw new IllegalArgumentException("incremental ZK nested frame is invalid");
        }
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    static int guardShardIndex(String feedId) {
        return sha256(requireFeedId(feedId).getBytes(StandardCharsets.UTF_8))[0] & 0xff;
    }

    static String guardPathForFeed(String feedId) {
        return guardPath(guardShardIndex(feedId));
    }

    private static String guardPath(int index) {
        return GUARDS + "/" + String.format(Locale.ROOT, "%02x", index);
    }

    private static String feedHash(String feedId) {
        return hex(sha256(requireFeedId(feedId).getBytes(StandardCharsets.UTF_8)));
    }

    private static String requireFeedId(String feedId) {
        if (feedId == null || feedId.isEmpty()
                || feedId.getBytes(StandardCharsets.UTF_8).length > 1024) {
            throw new IllegalArgumentException("feed id is invalid");
        }
        return feedId;
    }

    private static long next(long value) {
        try {
            return Math.addExact(value, 1L);
        } catch (ArithmeticException exhausted) {
            throw new IllegalStateException("incremental authority counter exhausted", exhausted);
        }
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] value = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int next = bytes[i] & 0xff;
            value[i * 2] = digits[next >>> 4];
            value[i * 2 + 1] = digits[next & 0x0f];
        }
        return new String(value);
    }

    private static byte[] hexBytes(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            int high = Character.digit(value.charAt(i * 2), 16);
            int low = Character.digit(value.charAt(i * 2 + 1), 16);
            bytes[i] = (byte) ((high << 4) | low);
        }
        return bytes;
    }

    private static IllegalStateException unavailable(String action, Throwable cause) {
        return new IllegalStateException(action + " unavailable", cause);
    }

    private static final class FeedFrame {
        private final IncrementalAuthorityRecord record;
        private final IncrementalAuthorityCommand initializeCommand;
        private final IncrementalAuthorityCommand lastCommand;
        private final IncrementalClaimRequest pending;

        private FeedFrame(
                IncrementalAuthorityRecord record,
                IncrementalAuthorityCommand initializeCommand,
                IncrementalAuthorityCommand lastCommand,
                IncrementalClaimRequest pending) {
            this.record = record;
            this.initializeCommand = initializeCommand;
            this.lastCommand = lastCommand;
            this.pending = pending;
        }
    }

    private static final class GuardFrame {
        private final int index;
        private final long revision;
        private final TreeMap<String, Long> entries;

        private GuardFrame(int index, long revision, TreeMap<String, Long> entries) {
            this.index = index;
            this.revision = revision;
            this.entries = entries;
        }
    }

    private static final class GuardVersion {
        private final String path;
        private final int index;
        private final int version;
        private final long revision;
        private final TreeMap<String, Long> entries;
        private final byte[] encoded;

        private GuardVersion(
                String path, int index, int version, long revision,
                TreeMap<String, Long> entries, byte[] encoded) {
            this.path = path;
            this.index = index;
            this.version = version;
            this.revision = revision;
            this.entries = entries;
            this.encoded = encoded;
        }
    }

    private static final class OwnershipView {
        private final GuardVersion guard;
        private final int ownerVersion;

        private OwnershipView(GuardVersion guard, int ownerVersion) {
            this.guard = guard;
            this.ownerVersion = ownerVersion;
        }
    }

    interface CommitHook {
        void run() throws Exception;
    }
}
