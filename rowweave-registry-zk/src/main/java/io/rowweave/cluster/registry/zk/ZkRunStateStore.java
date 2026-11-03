package io.rowweave.cluster.registry.zk;

import io.rowweave.artifact.ArtifactBackfillStatus;
import io.rowweave.artifact.ArtifactLifecycleCursor;
import io.rowweave.artifact.ArtifactLifecycleCodec;
import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.artifact.ArtifactReferenceRef;
import io.rowweave.artifact.ArtifactReferenceView;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.RecoverableRunCatalog;
import io.rowweave.run.RecoverableRunPage;
import io.rowweave.run.RecoverableRunRef;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunHistoryRetiringException;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateCodec;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreKernel;
import io.rowweave.run.RunStateStoreUnavailableException;
import io.rowweave.run.RunRecoveryCatalogKernel;
import io.rowweave.run.RunLifecycleCatalog;
import io.rowweave.run.RunLifecycleCatalogKernel;
import io.rowweave.run.RunLifecyclePage;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunLifecycleState;
import io.rowweave.run.RunRetentionCodec;
import io.rowweave.run.RunRetentionAuthority;
import io.rowweave.run.RunRetentionAuthorityKernel;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunPurgeProgress;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RetentionToken;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryOwnershipCodec;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RecoveryFencedRunStateStore;
import io.rowweave.run.RecoveryFencedStageAssignmentAuthority;
import io.rowweave.run.RunRecoveryOwnership;
import io.rowweave.run.RunCancellationAuthority;
import io.rowweave.run.RunCancellationAuthorityKernel;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunDeadlineAuthority;
import io.rowweave.run.RunDeadlineAuthorityKernel;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.WholeAttemptRetryAuthority;
import io.rowweave.run.WholeAttemptRetryAuthorityKernel;
import io.rowweave.run.WholeAttemptRetryClass;
import io.rowweave.run.WholeAttemptRetryDecision;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.RetryAttemptIdentity;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.DurableExecutionEvidenceAuthority;
import io.rowweave.run.DurableExecutionEvidenceView;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalOutcome;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalCodec;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkLocatorCodec;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.QualityGateAssignmentBinding;
import io.rowweave.run.QualityGateCommitJournalSnapshot;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.BatchSinkAbortRecordCodec;
import io.rowweave.run.AttemptSnapshot;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionCodec;
import io.rowweave.QualityGateDecisionResult;
import io.rowweave.QualityPreparedMemberEvidence;
import io.rowweave.commit.CommitManifest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiOperationRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.zip.CRC32;

/** The shared authority for a durable run, arbitrated by the ZK node version inside a multi request. */
final class ZkRunStateStore implements RunStateStore, RecoverableRunCatalog,
        RunRecoveryOwnership, RecoveryFencedRunStateStore,
        RecoveryFencedStageAssignmentAuthority, RunLifecycleCatalog,
        RunRetentionAuthority, ArtifactReferenceAuthority, RunCancellationAuthority,
        RunDeadlineAuthority, WholeAttemptRetryAuthority,
        DurableExecutionEvidenceAuthority {
    private static final String ROOT = "/rowweave/run-state-v1/runs";
    private static final String CATALOG_ROOT = "/rowweave/run-recovery-v1";
    private static final String CATALOG_SHARDS = CATALOG_ROOT + "/shards";
    private static final byte[] CATALOG_SCHEMA = new byte[] { 1 };
    private static final String LIFECYCLE_ROOT = "/rowweave/run-lifecycle-v1";
    private static final String LIFECYCLE_SHARDS = LIFECYCLE_ROOT + "/shards";
    private static final byte[] LIFECYCLE_SCHEMA = new byte[] { 1 };
    private static final String DEADLINE_ROOT = "/rowweave/run-deadline-v1";
    private static final String DEADLINE_TIME_ORACLE = DEADLINE_ROOT + "/time";
    private static final byte[] DEADLINE_SCHEMA = new byte[] { 1 };
    private static final String REFERENCE_ROOT = "/rowweave/artifact-references-v1";
    private static final String REFERENCE_ARTIFACTS = REFERENCE_ROOT + "/artifacts";
    private static final byte REFERENCE_SCHEMA_VERSION = 1;
    private static final byte REFERENCE_BACKFILLING = 0;
    private static final byte REFERENCE_READY = 1;
    private static final int REFERENCE_STATE_MAGIC = 0x52575242; // RWRB
    private static final int MAX_REFERENCE_CURSOR_BYTES = 128;
    private static final int REFERENCE_ROOT_MAGIC = 0x52575252; // RWRR
    private static final int REFERENCE_SHARD_MAGIC = 0x52575253; // RWRS
    private static final int REFERENCE_COUNT_BYTES = 4 + 1 + 32 + 8 + 32;
    private static final int REFERENCE_SHARD_COUNT_BYTES =
            4 + 1 + 32 + 1 + 4 + 32;
    private static final int MAX_RETRIES = 16;
    private static final int MAX_EVENT_NODES = 10_001;

    private final CuratorFramework client;
    private final CommitHook afterCommit;
    private final CommitHook afterOwnershipCommit;
    private final CommitHook afterAssignmentPublish;
    private final CommitHook afterAcceptanceCommit;
    private final CommitHook afterTerminalCommit;
    private final CommitHook afterManifestMemberCommit;
    private final CommitHook afterManifestIntentCommit;
    private final CommitHook afterRetentionCommit;
    private final AssignmentWriteObserver assignmentWriteObserver;
    private CommitHook afterPurgeCommit = () -> { };
    private PurgeWriteObserver purgeWriteObserver;
    private RetryWriteObserver retryWriteObserver;
    private EvidenceReadObserver evidenceReadObserver;
    private GuardedMutationObserver guardedMutationObserver;
    private TransactionWriteObserver transactionWriteObserver;
    private QualityJournalReadObserver qualityJournalReadObserver;
    private final int maxEventNodes;
    private final int maxCatalogEntries;
    private final int maxLifecycleEntries;
    private int maxArtifactReferencesPerShard = 4096;

    ZkRunStateStore(CuratorFramework client) {
        this(client, () -> { }, () -> { }, () -> { }, () -> { }, () -> { },
                () -> { }, () -> { }, MAX_EVENT_NODES,
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD, null);
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterCommit) {
        this(client, afterCommit, () -> { }, () -> { }, () -> { }, () -> { },
                () -> { }, () -> { }, MAX_EVENT_NODES,
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD, null);
    }

    ZkRunStateStore(CuratorFramework client, int maxArtifactReferencesPerShard) {
        this(client);
        if (maxArtifactReferencesPerShard < 1
                || maxArtifactReferencesPerShard > 4096) {
            throw new IllegalArgumentException(
                    "maxArtifactReferencesPerShard out of bounds");
        }
        this.maxArtifactReferencesPerShard = maxArtifactReferencesPerShard;
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterPurgeCommit,
                    PurgeWriteObserver purgeWriteObserver) {
        this(client);
        if (afterPurgeCommit == null || purgeWriteObserver == null) {
            throw new IllegalArgumentException("purge hooks are required");
        }
        this.afterPurgeCommit = afterPurgeCommit;
        this.purgeWriteObserver = purgeWriteObserver;
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterAssignmentPublish,
                    CommitHook afterAcceptanceCommit) {
        this(client, () -> { }, () -> { }, afterAssignmentPublish,
                afterAcceptanceCommit, () -> { }, () -> { }, () -> { }, MAX_EVENT_NODES,
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD, null);
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterAssignmentPublish,
                    CommitHook afterAcceptanceCommit,
                    CommitHook afterTerminalCommit) {
        this(client, () -> { }, () -> { }, afterAssignmentPublish,
                afterAcceptanceCommit, afterTerminalCommit,
                () -> { }, () -> { }, MAX_EVENT_NODES,
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD, null);
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterAssignmentPublish,
                    CommitHook afterAcceptanceCommit,
                    CommitHook afterTerminalCommit,
                    CommitHook afterManifestMemberCommit,
                    CommitHook afterManifestIntentCommit) {
        this(client, () -> { }, () -> { }, afterAssignmentPublish,
                afterAcceptanceCommit, afterTerminalCommit,
                afterManifestMemberCommit, afterManifestIntentCommit,
                MAX_EVENT_NODES, RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD, null);
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterAssignmentPublish,
                    CommitHook afterAcceptanceCommit,
                    AssignmentWriteObserver assignmentWriteObserver) {
        this(client, () -> { }, () -> { }, afterAssignmentPublish,
                afterAcceptanceCommit, () -> { }, () -> { }, () -> { }, MAX_EVENT_NODES,
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                assignmentWriteObserver);
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterCommit, int maxEventNodes) {
        this(client, afterCommit, () -> { }, () -> { }, () -> { }, () -> { },
                () -> { }, () -> { }, maxEventNodes,
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD, null);
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterCommit,
                    int maxEventNodes, int maxCatalogEntries) {
        this(client, afterCommit, () -> { }, () -> { }, () -> { }, () -> { },
                () -> { }, () -> { },
                maxEventNodes, maxCatalogEntries, null);
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterCommit,
                    int maxEventNodes, int maxCatalogEntries,
                    int maxLifecycleEntries) {
        this(client, afterCommit, () -> { }, () -> { }, () -> { }, () -> { },
                () -> { }, () -> { }, maxEventNodes, maxCatalogEntries, null,
                maxLifecycleEntries);
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterCommit,
                    int maxEventNodes, int maxCatalogEntries,
                    int maxLifecycleEntries, CommitHook afterRetentionCommit) {
        this(client, afterCommit, () -> { }, () -> { }, () -> { }, () -> { },
                () -> { }, () -> { }, maxEventNodes, maxCatalogEntries, null,
                maxLifecycleEntries, afterRetentionCommit);
    }

    ZkRunStateStore(CuratorFramework client, CommitHook afterCommit,
                    CommitHook afterOwnershipCommit,
                    int maxEventNodes, int maxCatalogEntries) {
        this(client, afterCommit, afterOwnershipCommit, () -> { }, () -> { }, () -> { },
                () -> { }, () -> { },
                maxEventNodes, maxCatalogEntries, null);
    }

    private ZkRunStateStore(CuratorFramework client, CommitHook afterCommit,
                    CommitHook afterOwnershipCommit, CommitHook afterAssignmentPublish,
                    CommitHook afterAcceptanceCommit,
                    CommitHook afterTerminalCommit,
                    CommitHook afterManifestMemberCommit,
                    CommitHook afterManifestIntentCommit,
                    int maxEventNodes, int maxCatalogEntries,
                    AssignmentWriteObserver assignmentWriteObserver) {
        this(client, afterCommit, afterOwnershipCommit, afterAssignmentPublish,
                afterAcceptanceCommit, afterTerminalCommit, afterManifestMemberCommit,
                afterManifestIntentCommit, maxEventNodes, maxCatalogEntries,
                assignmentWriteObserver,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD);
    }

    private ZkRunStateStore(CuratorFramework client, CommitHook afterCommit,
                    CommitHook afterOwnershipCommit, CommitHook afterAssignmentPublish,
                    CommitHook afterAcceptanceCommit,
                    CommitHook afterTerminalCommit,
                    CommitHook afterManifestMemberCommit,
                    CommitHook afterManifestIntentCommit,
                    int maxEventNodes, int maxCatalogEntries,
                    AssignmentWriteObserver assignmentWriteObserver,
                    int maxLifecycleEntries) {
        this(client, afterCommit, afterOwnershipCommit, afterAssignmentPublish,
                afterAcceptanceCommit, afterTerminalCommit, afterManifestMemberCommit,
                afterManifestIntentCommit, maxEventNodes, maxCatalogEntries,
                assignmentWriteObserver, maxLifecycleEntries, () -> { });
    }

    private ZkRunStateStore(CuratorFramework client, CommitHook afterCommit,
                    CommitHook afterOwnershipCommit, CommitHook afterAssignmentPublish,
                    CommitHook afterAcceptanceCommit,
                    CommitHook afterTerminalCommit,
                    CommitHook afterManifestMemberCommit,
                    CommitHook afterManifestIntentCommit,
                    int maxEventNodes, int maxCatalogEntries,
                    AssignmentWriteObserver assignmentWriteObserver,
                    int maxLifecycleEntries, CommitHook afterRetentionCommit) {
        if (client == null) throw new IllegalArgumentException("client is required");
        if (afterCommit == null) throw new IllegalArgumentException("afterCommit is required");
        if (afterOwnershipCommit == null) {
            throw new IllegalArgumentException("afterOwnershipCommit is required");
        }
        if (afterAssignmentPublish == null || afterAcceptanceCommit == null
                || afterTerminalCommit == null
                || afterManifestMemberCommit == null
                || afterManifestIntentCommit == null
                || afterRetentionCommit == null) {
            throw new IllegalArgumentException("assignment commit hooks are required");
        }
        if (maxEventNodes < 1 || maxEventNodes > MAX_EVENT_NODES) {
            throw new IllegalArgumentException("maxEventNodes out of bounds");
        }
        if (maxCatalogEntries < 1
                || maxCatalogEntries > RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD) {
            throw new IllegalArgumentException("maxCatalogEntries out of bounds");
        }
        if (maxLifecycleEntries < 1
                || maxLifecycleEntries > RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD) {
            throw new IllegalArgumentException("maxLifecycleEntries out of bounds");
        }
        this.client = client;
        this.afterCommit = afterCommit;
        this.afterOwnershipCommit = afterOwnershipCommit;
        this.afterAssignmentPublish = afterAssignmentPublish;
        this.afterAcceptanceCommit = afterAcceptanceCommit;
        this.afterTerminalCommit = afterTerminalCommit;
        this.afterManifestMemberCommit = afterManifestMemberCommit;
        this.afterManifestIntentCommit = afterManifestIntentCommit;
        this.afterRetentionCommit = afterRetentionCommit;
        this.assignmentWriteObserver = assignmentWriteObserver;
        this.maxEventNodes = maxEventNodes;
        this.maxCatalogEntries = maxCatalogEntries;
        this.maxLifecycleEntries = maxLifecycleEntries;
        try {
            ensurePersistent(ROOT, new byte[0]);
            ensureCatalog();
            ensureLifecycleCatalog();
            ensureReferenceIndex();
            ensureDeadlineTimeOracle();
        } catch (Exception failure) {
            throw unavailable(null, "initialize ZK run state root", failure);
        }
    }

    @Override
    public RunDecision apply(RunCommand command) {
        return apply(command, null, false);
    }

    @Override
    public RunDecision applyRecovered(RecoveryToken token, RunCommand command) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (command == null) throw new IllegalArgumentException("command is required");
        if (!token.runId().equals(command.runId())) {
            throw new IllegalArgumentException("recovery token and command runId mismatch");
        }
        return apply(command, token, true);
    }

    @Override
    public RunCancellationResult requestUserCancellation(
            String runId, String requestId) {
        runId = RunCancellationAuthorityKernel.validateRunId(runId);
        requestId = RunCancellationAuthorityKernel.validateRequestId(requestId);
        final String commandId =
                RunCancellationAuthorityKernel.commandId(runId, requestId);
        Throwable lastRace = null;
        int consecutiveTransportFailures = 0;
        String stableMismatch = null;
        int stableMismatchCount = 0;
        long observedRootCzxid = -1L;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, EvidenceReadPhase.CANCEL_AFTER_INITIAL_SNAPSHOT,
                        observedRootCzxid);
                if (authority.rootCzxid < 0L
                        || authority.snapshot == null) {
                    return new RunCancellationResult(runId,
                            RunCancellationResult.Disposition.NOT_FOUND, null, 0L);
                }
                if (observedRootCzxid < 0L) {
                    observedRootCzxid = authority.rootCzxid;
                }
                Layout layout = authority.layout;
                SnapshotVersion current = authority.snapshot;
                LifecycleEntry retentionView = authority.lifecycle;
                stableMismatch = null;
                stableMismatchCount = 0;
                if (retentionView != null
                        && retentionView.ref.state() == RunLifecycleState.DELETING) {
                    verifyExistingLifecycleOrRace(layout, current, retentionView);
                    confirmResultAuthority(authority);
                    return RunCancellationAuthorityKernel.retiring(runId);
                }
                if (current == null) {
                    return new RunCancellationResult(runId,
                            RunCancellationResult.Disposition.NOT_FOUND, null, 0L);
                }
                Receipt receipt = readReceipt(layout, commandId);
                if (receipt != null) {
                    verifyCatalogStable(layout, current);
                    verifyLifecycleStable(layout, current);
                    verifyReferenceStable(current);
                    confirmResultAuthority(authority);
                    return RunCancellationAuthorityKernel.replayed(
                            runId, current.snapshot, receipt.event, commandId);
                }

                RunCancellationAuthorityKernel.Plan plan =
                        RunCancellationAuthorityKernel.plan(
                                runId, commandId, current.snapshot);
                if (plan.result() != null) {
                    verifyCatalogStable(layout, current);
                    verifyLifecycleStable(layout, current);
                    verifyReferenceStable(current);
                    confirmResultAuthority(authority);
                    return plan.result();
                }
                RunCommand command = plan.command();
                RunDecision decision = RunStateStoreKernel.evaluate(
                        current.snapshot, command);
                if (!decision.accepted()) {
                    throw new IllegalStateException(
                            "atomic public cancellation rejected: " + decision.code());
                }
                CatalogMutation catalog = prepareCatalogMutation(
                        layout, current, decision.snapshot());
                LifecycleMutation lifecycle = prepareLifecycleMutation(
                        layout, current, decision.snapshot());
                ReferenceMutation reference = prepareReferenceMutation(
                        current, decision.snapshot());
                if (catalog.capacityExceeded || lifecycle.capacityExceeded
                        || reference.capacityExceeded) {
                    throw new IllegalStateException(
                            "existing Run cancellation exceeded authority capacity");
                }
                byte[] snapshot = RunStateCodec.encodeSnapshot(decision.snapshot());
                byte[] event = RunStateCodec.encodeEvent(decision.event());
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check()
                        .withVersion(current.version).forPath(layout.snapshot));
                operations.add(client.transactionOp().setData()
                        .withVersion(current.version).forPath(layout.snapshot, snapshot));
                operations.add(client.transactionOp().create().forPath(
                        eventPath(layout, decision.event().sequence()), event));
                operations.add(client.transactionOp().create().forPath(
                        receiptPath(layout, commandId), event));
                catalog.appendTo(client, operations);
                lifecycle.appendTo(client, operations);
                reference.appendTo(client, operations);
                appendGuardCheck(authority, operations);
                if (decision.snapshot().status() == io.rowweave.run.RunStatus.READY) {
                    appendAssignmentCleanup(layout, operations);
                }
                observeGuardedMutation(GuardedMutationKind.CANCEL, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.CANCEL, -1, operations);
                observeGuardedMutation(GuardedMutationKind.CANCEL, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                afterCommit.run();
                return RunCancellationAuthorityKernel.accepted(decision.snapshot());
            } catch (KeeperException.BadVersionException
                     | KeeperException.NodeExistsException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
                consecutiveTransportFailures = 0;
                stableMismatch = null;
                stableMismatchCount = 0;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                stableMismatch = null;
                stableMismatchCount = 0;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return new RunCancellationResult(runId,
                                RunCancellationResult.Disposition.NOT_FOUND,
                                null, 0L);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(commandId,
                            "confirm unknown ZK public cancellation outcome",
                            confirmationFailure);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
                if (++consecutiveTransportFailures >= 2) {
                    throw unavailable(commandId,
                            "reconcile unknown ZK public cancellation outcome", unknown);
                }
            } catch (RunStateStoreUnavailableException unavailable) {
                lastRace = unavailable;
                stableMismatch = null;
                stableMismatchCount = 0;
            } catch (CatalogRaceException raced) {
                lastRace = raced;
                stableMismatch = null;
                stableMismatchCount = 0;
            } catch (RunIncarnationChangedException changed) {
                return new RunCancellationResult(runId,
                        RunCancellationResult.Disposition.NOT_FOUND, null, 0L);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(commandId,
                        "apply ZK public cancellation", failure);
            }
        }
        throw unavailable(commandId,
                "ZK public cancellation did not converge", lastRace);
    }

    @Override
    public RunDeadlineResult evaluateDeadline(String runId) {
        return evaluateDeadline(runId, -1L);
    }

    private RunDeadlineResult evaluateDeadline(
            String runId, long expectedRootCzxid) {
        runId = RunDeadlineAuthorityKernel.validateRunId(runId);
        Throwable lastRace = null;
        int consecutiveTransportFailures = 0;
        long observedRootCzxid = expectedRootCzxid;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            String commandId = null;
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId,
                        EvidenceReadPhase.DEADLINE_AFTER_INITIAL_SNAPSHOT,
                        observedRootCzxid);
                if (authority.rootCzxid < 0L
                        || authority.snapshot == null) {
                    return RunDeadlineAuthorityKernel.missing(runId);
                }
                if (observedRootCzxid < 0L) {
                    observedRootCzxid = authority.rootCzxid;
                }
                Layout layout = authority.layout;
                SnapshotVersion current = authority.snapshot;
                LifecycleEntry retentionView = authority.lifecycle;
                if (retentionView != null
                        && retentionView.ref.state() == RunLifecycleState.DELETING) {
                    confirmResultAuthority(authority);
                    return RunDeadlineAuthorityKernel.retiring(runId);
                }
                if (current.snapshot.deadlineEpochMillis() == 0L) {
                    verifyCatalogStable(layout, current);
                    verifyLifecycleStable(layout, current);
                    verifyReferenceStable(current);
                    confirmResultAuthority(authority);
                    return RunDeadlineAuthorityKernel.plan(
                            null, current.snapshot, 0L).result();
                }

                long observed = deadlineAuthorityTime();
                commandId = RunDeadlineAuthorityKernel.commandId(current.snapshot);
                Receipt receipt = readReceipt(layout, commandId);
                if (receipt != null) {
                    verifyCatalogStable(layout, current);
                    verifyLifecycleStable(layout, current);
                    verifyReferenceStable(current);
                    confirmResultAuthority(authority);
                    return RunDeadlineAuthorityKernel.replayed(
                            current.snapshot, receipt.event, commandId, observed);
                }

                RunDeadlineAuthorityKernel.Plan plan =
                        RunDeadlineAuthorityKernel.plan(
                                commandId, current.snapshot, observed);
                if (plan.result() != null) {
                    verifyCatalogStable(layout, current);
                    verifyLifecycleStable(layout, current);
                    verifyReferenceStable(current);
                    confirmResultAuthority(authority);
                    return plan.result();
                }
                RunDecision decision = RunStateStoreKernel.evaluate(
                        current.snapshot, plan.command());
                if (!decision.accepted()) {
                    throw new IllegalStateException(
                            "atomic deadline mutation rejected: " + decision.code());
                }
                CatalogMutation catalog = prepareCatalogMutation(
                        layout, current, decision.snapshot());
                LifecycleMutation lifecycle = prepareLifecycleMutation(
                        layout, current, decision.snapshot());
                ReferenceMutation reference = prepareReferenceMutation(
                        current, decision.snapshot());
                if (catalog.capacityExceeded || lifecycle.capacityExceeded
                        || reference.capacityExceeded) {
                    throw new IllegalStateException(
                            "existing Run deadline exceeded authority capacity");
                }
                byte[] snapshot = RunStateCodec.encodeSnapshot(decision.snapshot());
                byte[] event = RunStateCodec.encodeEvent(decision.event());
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check()
                        .withVersion(current.version).forPath(layout.snapshot));
                operations.add(client.transactionOp().setData()
                        .withVersion(current.version).forPath(layout.snapshot, snapshot));
                operations.add(client.transactionOp().create().forPath(
                        eventPath(layout, decision.event().sequence()), event));
                operations.add(client.transactionOp().create().forPath(
                        receiptPath(layout, commandId), event));
                catalog.appendTo(client, operations);
                lifecycle.appendTo(client, operations);
                reference.appendTo(client, operations);
                appendGuardCheck(authority, operations);
                if (decision.snapshot().status() == io.rowweave.run.RunStatus.READY) {
                    appendAssignmentCleanup(layout, operations);
                }
                observeGuardedMutation(GuardedMutationKind.DEADLINE, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.DEADLINE, -1, operations);
                observeGuardedMutation(GuardedMutationKind.DEADLINE, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                afterCommit.run();
                return RunDeadlineAuthorityKernel.accepted(
                        decision.snapshot(), observed);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NodeExistsException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
                consecutiveTransportFailures = 0;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return RunDeadlineAuthorityKernel.missing(runId);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(commandId,
                            "confirm unknown ZK deadline outcome",
                            confirmationFailure);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
                if (++consecutiveTransportFailures >= 2) {
                    throw unavailable(commandId,
                            "reconcile unknown ZK deadline outcome", unknown);
                }
            } catch (RunStateStoreUnavailableException unavailable) {
                lastRace = unavailable;
            } catch (CatalogRaceException raced) {
                lastRace = raced;
            } catch (RunIncarnationChangedException changed) {
                return RunDeadlineAuthorityKernel.missing(runId);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(commandId, "apply ZK deadline", failure);
            }
        }
        throw unavailable(null, "ZK deadline did not converge", lastRace);
    }

    @Override
    public WholeAttemptRetryDecision scheduleRetry(RunAttemptFence fence) {
        return scheduleRetry(fence, null, false, TransactionAugment.NONE);
    }

    @Override
    public WholeAttemptRetryDecision scheduleRetryRecovered(
            RecoveryToken token, RunAttemptFence fence) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (fence == null) throw new IllegalArgumentException("fence is required");
        if (!token.runId().equals(fence.runId())) {
            throw new IllegalArgumentException(
                    "recovery token and retry fence runId mismatch");
        }
        return scheduleRetry(fence, token, true, TransactionAugment.NONE);
    }

    WholeAttemptRetryDecision scheduleRetryAugmented(
            RecoveryToken token, RunAttemptFence fence,
            TransactionAugment augment) {
        if (token == null || fence == null || augment == null) {
            throw new IllegalArgumentException(
                    "token, fence and transaction augment are required");
        }
        if (!token.runId().equals(fence.runId())) {
            throw new IllegalArgumentException(
                    "recovery token and retry fence runId mismatch");
        }
        return scheduleRetry(fence, token, true, augment);
    }

    private WholeAttemptRetryDecision scheduleRetry(
            RunAttemptFence fence, RecoveryToken token, boolean recovered,
            TransactionAugment augment) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        Throwable lastRace = null;
        int consecutiveTransportFailures = 0;
        long observedRootCzxid = -1L;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            String commandId = null;
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        fence.runId(), EvidenceReadPhase
                                .RETRY_SCHEDULE_AFTER_INITIAL_SNAPSHOT,
                        observedRootCzxid);
                if (authority.rootCzxid < 0L
                        || authority.snapshot == null) {
                    return retryDecision(WholeAttemptRetryDecision.Code.NOT_FOUND,
                            null, null, 0L);
                }
                if (observedRootCzxid < 0L) {
                    observedRootCzxid = authority.rootCzxid;
                }
                Layout layout = authority.layout;
                SnapshotVersion current = authority.snapshot;
                LifecycleEntry lifecycle = authority.lifecycle;
                verifyCatalogStable(layout, current);
                verifyReferenceStable(current);
                if (lifecycle.ref.state() == RunLifecycleState.DELETING) {
                    confirmResultAuthority(authority);
                    return retryDecision(WholeAttemptRetryDecision.Code.RETIRING,
                            current.snapshot, null, 0L);
                }
                OwnerVersion owner = readOwner(layout);
                if ((!recovered && owner != null)
                        || (recovered && (owner == null
                        || !token.equals(owner.token)))) {
                    confirmResultAuthority(authority, owner);
                    return retryDecision(
                            WholeAttemptRetryDecision.Code.STALE_RECOVERY_OWNER,
                            current.snapshot, null, 0L);
                }

                WholeAttemptRetryDecision replay = replayedRetrySchedule(
                        layout, current, fence);
                if (replay != null) {
                    checkRetryView(authority, owner);
                    return replay;
                }
                if (!matchesCurrentAttempt(current.snapshot, fence)) {
                    confirmResultAuthority(authority, owner);
                    return retryDecision(current.snapshot.status().isTerminal()
                                    ? WholeAttemptRetryDecision.Code.TOO_LATE
                                    : WholeAttemptRetryDecision.Code.STALE_RUN,
                            current.snapshot, null, 0L);
                }
                AttemptStatus status = current.snapshot.currentAttempt().status();
                if (status == AttemptStatus.COMMITTING
                        || status == AttemptStatus.RECONCILING
                        || status == AttemptStatus.CANCELING
                        || status.isTerminal()) {
                    confirmResultAuthority(authority, owner);
                    return retryDecision(WholeAttemptRetryDecision.Code.TOO_LATE,
                            current.snapshot, null, 0L);
                }

                long observed = deadlineAuthorityTime();
                if (deadlineDue(current.snapshot, observed)) {
                    observeEvidenceRead(EvidenceReadPhase
                            .RETRY_SCHEDULE_BEFORE_DEADLINE_EVALUATION);
                    evaluateDeadline(fence.runId(), observedRootCzxid);
                    observeEvidenceRead(EvidenceReadPhase
                            .RETRY_SCHEDULE_AFTER_DEADLINE_EVALUATION);
                    AuthorityObservation after = observeCoreAuthority(
                            fence.runId(), null, observedRootCzxid);
                    if (after.rootCzxid < 0L || after.snapshot == null) {
                        return retryDecision(
                                WholeAttemptRetryDecision.Code.NOT_FOUND,
                                null, null, 0L);
                    }
                    confirmResultAuthority(after);
                    return retryDecision(WholeAttemptRetryDecision.Code.TOO_LATE,
                            after.snapshot.snapshot, null, observed);
                }

                ManifestJournalView journal = readManifestJournalView(layout);
                AssignmentView assignments = journal == null
                        ? readAssignmentView(layout) : journal.assignments;
                AbortVersion abort = readAbortVersion(layout);
                if (abort == null) {
                    final boolean repaired;
                    try {
                        repaired = ensureAbortGuard(
                                fence.runId(), observedRootCzxid,
                                GuardedMutationKind.SINK_ABORT_GUARD);
                    } catch (RunStateStoreUnavailableException failure) {
                        throw new RetryAbortGuardRepairFailure(
                                fence.runId(), failure);
                    }
                    if (!repaired) {
                        return retryDecision(
                                WholeAttemptRetryDecision.Code.NOT_FOUND,
                                null, null, 0L);
                    }
                    continue;
                }
                WholeAttemptRetryAuthorityKernel.Closure closure =
                        WholeAttemptRetryAuthorityKernel.classify(
                                current.snapshot,
                                assignments == null ? null : assignments.snapshot,
                                journal == null ? null : journal.snapshot,
                                abort == null ? null : abort.record);
                if (closure.code() != null) {
                    checkRetryEvidenceView(authority, owner,
                            assignments, journal, abort);
                    return retryDecision(closure.code(), current.snapshot,
                            null, observed);
                }

                boolean exhausted = current.snapshot.attemptCount()
                        >= current.snapshot.maxAttempts();
                commandId = WholeAttemptRetryAuthorityKernel.scheduleCommandId(
                        fence, closure.retryClass(),
                        assignments == null ? null : assignments.snapshot,
                        abort == null ? null : abort.record);
                RunDecision decision = WholeAttemptRetryAuthorityKernel.evaluateSchedule(
                        commandId, current.snapshot, fence, observed,
                        closure.retryClass(), exhausted);
                if (!decision.accepted()) {
                    throw new IllegalStateException(
                            "atomic retry schedule rejected: " + decision.code());
                }
                commitRetryMutation(authority, layout, current, lifecycle,
                        owner, recovered,
                        commandId, decision, assignments, journal, abort,
                        GuardedMutationKind.RETRY_SCHEDULE, augment);
                afterCommit.run();
                return retryDecision(exhausted
                                ? WholeAttemptRetryDecision.Code.EXHAUSTED
                                : WholeAttemptRetryDecision.Code.SCHEDULED,
                        decision.snapshot(), closure.retryClass(), observed);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NodeExistsException
                     | KeeperException.NoNodeException
                     | CatalogRaceException raced) {
                lastRace = raced;
                consecutiveTransportFailures = 0;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(
                            fence.runId(), observedRootCzxid)) {
                        return retryDecision(
                                WholeAttemptRetryDecision.Code.NOT_FOUND,
                                null, null, 0L);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(commandId,
                            "confirm unknown ZK retry schedule outcome",
                            confirmationFailure);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
                if (++consecutiveTransportFailures >= 2) {
                    throw unavailable(commandId,
                            "reconcile unknown ZK retry schedule outcome", unknown);
                }
            } catch (RunStateStoreUnavailableException unavailable) {
                lastRace = unavailable;
            } catch (RunIncarnationChangedException changed) {
                return retryDecision(WholeAttemptRetryDecision.Code.NOT_FOUND,
                        null, null, 0L);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(commandId, "schedule ZK whole-attempt retry", failure);
            }
        }
        throw unavailable(null, "ZK retry schedule did not converge", lastRace);
    }

    @Override
    public RetryReadyEvaluation evaluateRetryReady(String runId) {
        runId = WholeAttemptRetryAuthorityKernel.validateRunId(runId);
        Throwable lastRace = null;
        long observedRootCzxid = -1L;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId,
                        EvidenceReadPhase.RETRY_READY_AFTER_INITIAL_SNAPSHOT,
                        observedRootCzxid);
                if (authority.rootCzxid < 0L
                        || authority.snapshot == null) {
                    return retryReady(RetryReadyEvaluation.Code.NOT_FOUND,
                            null, null, 0L, 0L, 0);
                }
                if (observedRootCzxid < 0L) {
                    observedRootCzxid = authority.rootCzxid;
                }
                Layout layout = authority.layout;
                SnapshotVersion current = authority.snapshot;
                LifecycleEntry lifecycle = authority.lifecycle;
                verifyCatalogStable(layout, current);
                verifyReferenceStable(current);
                if (lifecycle.ref.state() == RunLifecycleState.DELETING) {
                    confirmResultAuthority(authority);
                    return retryReady(RetryReadyEvaluation.Code.RETIRING,
                            current.snapshot, null, 0L, 0L, 0);
                }
                if (current.snapshot.status().isTerminal()) {
                    confirmResultAuthority(authority);
                    return retryReady(RetryReadyEvaluation.Code.TOO_LATE,
                            current.snapshot, null, 0L, 0L, 0);
                }
                WholeAttemptRetryAuthorityKernel.Wait wait =
                        WholeAttemptRetryAuthorityKernel.retryWait(current.snapshot);
                if (wait == null) {
                    confirmResultAuthority(authority);
                    return retryReady(RetryReadyEvaluation.Code.NOT_RETRY_WAIT,
                            current.snapshot, null, 0L, 0L, 0);
                }
                long observed = deadlineAuthorityTime();
                checkRetryView(authority, null);
                if (deadlineDue(current.snapshot, observed)) {
                    return retryReady(RetryReadyEvaluation.Code.TOO_LATE,
                            current.snapshot, wait.identity(), observed,
                            wait.notBeforeMillis(), wait.remainingAttempts());
                }
                return retryReady(observed >= wait.notBeforeMillis()
                                ? RetryReadyEvaluation.Code.DUE
                                : RetryReadyEvaluation.Code.NOT_DUE,
                        current.snapshot, wait.identity(), observed,
                        wait.notBeforeMillis(), wait.remainingAttempts());
            } catch (KeeperException.BadVersionException
                     | KeeperException.NoNodeException
                     | CatalogRaceException raced) {
                lastRace = raced;
            } catch (RunIncarnationChangedException changed) {
                return retryReady(RetryReadyEvaluation.Code.NOT_FOUND,
                        null, null, 0L, 0L, 0);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(null, "evaluate ZK retry readiness", failure);
            }
        }
        throw unavailable(null, "ZK retry readiness did not converge", lastRace);
    }

    @Override
    public RetryAttemptClaim claimDueRetry(String runId) {
        return claimDueRetry(null, runId, false, TransactionAugment.NONE);
    }

    @Override
    public RetryAttemptClaim claimDueRetryRecovered(
            RecoveryToken token, String runId) {
        if (token == null) throw new IllegalArgumentException("token is required");
        runId = WholeAttemptRetryAuthorityKernel.validateRunId(runId);
        if (!token.runId().equals(runId)) {
            throw new IllegalArgumentException(
                    "recovery token and retry claim runId mismatch");
        }
        return claimDueRetry(token, runId, true, TransactionAugment.NONE);
    }

    RetryAttemptClaim claimDueRetryAugmented(
            RecoveryToken token, String runId, TransactionAugment augment) {
        if (token == null || augment == null) {
            throw new IllegalArgumentException(
                    "token and transaction augment are required");
        }
        runId = WholeAttemptRetryAuthorityKernel.validateRunId(runId);
        if (!token.runId().equals(runId)) {
            throw new IllegalArgumentException(
                    "recovery token and retry claim runId mismatch");
        }
        return claimDueRetry(token, runId, true, augment);
    }

    private RetryAttemptClaim claimDueRetry(
            RecoveryToken token, String runId, boolean recovered,
            TransactionAugment augment) {
        runId = WholeAttemptRetryAuthorityKernel.validateRunId(runId);
        Throwable lastRace = null;
        int consecutiveTransportFailures = 0;
        long observedRootCzxid = -1L;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            String commandId = null;
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId,
                        EvidenceReadPhase.RETRY_CLAIM_AFTER_INITIAL_SNAPSHOT,
                        observedRootCzxid);
                if (authority.rootCzxid < 0L
                        || authority.snapshot == null) {
                    return retryClaim(RetryAttemptClaim.Code.NOT_FOUND,
                            null, null, 0L, 0L);
                }
                if (observedRootCzxid < 0L) {
                    observedRootCzxid = authority.rootCzxid;
                }
                Layout layout = authority.layout;
                SnapshotVersion current = authority.snapshot;
                LifecycleEntry lifecycle = authority.lifecycle;
                verifyCatalogStable(layout, current);
                verifyReferenceStable(current);
                if (lifecycle.ref.state() == RunLifecycleState.DELETING) {
                    confirmResultAuthority(authority);
                    return retryClaim(RetryAttemptClaim.Code.RETIRING,
                            current.snapshot, null, 0L, 0L);
                }
                OwnerVersion owner = readOwner(layout);
                if ((!recovered && owner != null)
                        || (recovered && (owner == null
                        || !token.equals(owner.token)))) {
                    confirmResultAuthority(authority, owner);
                    return retryClaim(RetryAttemptClaim.Code.STALE_RECOVERY_OWNER,
                            current.snapshot, null, 0L, 0L);
                }
                if (current.snapshot.status().isTerminal()) {
                    confirmResultAuthority(authority, owner);
                    return retryClaim(RetryAttemptClaim.Code.TOO_LATE,
                            current.snapshot, null, 0L, 0L);
                }

                RetryAttemptIdentity existing = claimedRetryIdentity(
                        layout, current.snapshot);
                if (existing != null) {
                    checkRetryView(authority, owner);
                    return retryClaim(RetryAttemptClaim.Code.ALREADY_CLAIMED,
                            current.snapshot, existing, 0L, 0L);
                }
                WholeAttemptRetryAuthorityKernel.Wait wait =
                        WholeAttemptRetryAuthorityKernel.retryWait(current.snapshot);
                if (wait == null) {
                    confirmResultAuthority(authority, owner);
                    return retryClaim(RetryAttemptClaim.Code.NOT_RETRY_WAIT,
                            current.snapshot, null, 0L, 0L);
                }
                long observed = deadlineAuthorityTime();
                if (deadlineDue(current.snapshot, observed)) {
                    observeEvidenceRead(EvidenceReadPhase
                            .RETRY_CLAIM_BEFORE_DEADLINE_EVALUATION);
                    evaluateDeadline(runId, observedRootCzxid);
                    observeEvidenceRead(EvidenceReadPhase
                            .RETRY_CLAIM_AFTER_DEADLINE_EVALUATION);
                    AuthorityObservation after = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (after.rootCzxid < 0L || after.snapshot == null) {
                        return retryClaim(RetryAttemptClaim.Code.NOT_FOUND,
                                null, null, 0L, 0L);
                    }
                    confirmResultAuthority(after);
                    return retryClaim(RetryAttemptClaim.Code.TOO_LATE,
                            after.snapshot.snapshot, wait.identity(), observed,
                            wait.notBeforeMillis());
                }
                if (observed < wait.notBeforeMillis()) {
                    checkRetryView(authority, owner);
                    return retryClaim(RetryAttemptClaim.Code.NOT_DUE,
                            current.snapshot, wait.identity(), observed,
                            wait.notBeforeMillis());
                }

                commandId = wait.identity().commandId("claim");
                RunDecision decision = WholeAttemptRetryAuthorityKernel.evaluateClaim(
                        current.snapshot, wait.identity(), observed);
                if (!decision.accepted()) {
                    throw new IllegalStateException(
                            "atomic retry claim rejected: " + decision.code());
                }
                commitRetryMutation(authority, layout, current, lifecycle,
                        owner, recovered,
                        commandId, decision, null, null, null,
                        GuardedMutationKind.RETRY_CLAIM, augment);
                afterCommit.run();
                return retryClaim(RetryAttemptClaim.Code.CLAIMED,
                        decision.snapshot(), wait.identity(), observed,
                        wait.notBeforeMillis());
            } catch (KeeperException.BadVersionException
                     | KeeperException.NodeExistsException
                     | KeeperException.NoNodeException
                     | CatalogRaceException raced) {
                lastRace = raced;
                consecutiveTransportFailures = 0;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return retryClaim(RetryAttemptClaim.Code.NOT_FOUND,
                                null, null, 0L, 0L);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(commandId,
                            "confirm unknown ZK retry claim outcome",
                            confirmationFailure);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
                if (++consecutiveTransportFailures >= 2) {
                    throw unavailable(commandId,
                            "reconcile unknown ZK retry claim outcome", unknown);
                }
            } catch (RunStateStoreUnavailableException unavailable) {
                lastRace = unavailable;
            } catch (RunIncarnationChangedException changed) {
                return retryClaim(RetryAttemptClaim.Code.NOT_FOUND,
                        null, null, 0L, 0L);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(commandId, "claim ZK whole-attempt retry", failure);
            }
        }
        throw unavailable(null, "ZK retry claim did not converge", lastRace);
    }

    private WholeAttemptRetryDecision replayedRetrySchedule(
            Layout layout, SnapshotVersion current, RunAttemptFence fence)
            throws Exception {
        if (current.snapshot.currentAttempt() == null
                || !fence.runId().equals(current.snapshot.runId())
                || !fence.attemptId().equals(
                current.snapshot.currentAttempt().attemptId())
                || fence.fenceToken()
                != current.snapshot.currentAttempt().fenceToken()) {
            return null;
        }
        WholeAttemptRetryClass retryClass = WholeAttemptRetryClass.fromReasonCode(
                current.snapshot.currentAttempt().reasonCode());
        WholeAttemptRetryDecision.Code committedCode = retryClass == null
                ? WholeAttemptRetryDecision.Code.EXHAUSTED
                : WholeAttemptRetryDecision.Code.SCHEDULED;
        if (!WholeAttemptRetryAuthorityKernel.scheduleStillCurrent(
                committedCode, retryClass, current.snapshot)) {
            return null;
        }
        ExecutionEvent event = decodeEvent(
                layout, sequenceName(current.snapshot.revision()));
        if (event.type() != ExecutionEvent.Type.ATTEMPT_ADVANCED
                || !fence.attemptId().equals(event.attemptId())
                || fence.fenceToken() != event.fenceToken()
                || event.attemptTo() != AttemptStatus.FAILED
                || !java.util.Objects.equals(
                current.snapshot.currentAttempt().reasonCode(),
                event.reasonCode())) {
            throw corruption(layout.runId,
                    "retry schedule snapshot/latest event mismatch");
        }
        Receipt receipt = readReceipt(layout, event.commandId());
        if (receipt == null) {
            throw corruption(layout.runId,
                    "retry schedule command receipt is missing");
        }
        return retryDecision(committedCode
                        == WholeAttemptRetryDecision.Code.SCHEDULED
                        ? WholeAttemptRetryDecision.Code.ALREADY_SCHEDULED
                        : committedCode,
                current.snapshot, retryClass, event.eventTimeMillis());
    }

    private RetryAttemptIdentity claimedRetryIdentity(
            Layout layout, RunSnapshot snapshot) throws Exception {
        if (snapshot.status() != io.rowweave.run.RunStatus.RUNNING
                || snapshot.currentAttempt() == null
                || snapshot.attemptCount() < 2) {
            return null;
        }
        RetryAttemptIdentity identity = RetryAttemptIdentity.derive(
                snapshot.runId(), snapshot.artifactId(), snapshot.attemptCount());
        Receipt receipt = readReceipt(layout, identity.commandId("claim"));
        return WholeAttemptRetryAuthorityKernel.claimedRetryIdentity(
                snapshot, receipt != null
                && receipt.event.type() == ExecutionEvent.Type.ATTEMPT_CREATED);
    }

    private void commitRetryMutation(
            AuthorityObservation authority, Layout layout,
            SnapshotVersion current, LifecycleEntry lifecycle,
            OwnerVersion owner, boolean recovered, String commandId,
            RunDecision decision, AssignmentView assignments,
            ManifestJournalView journal, AbortVersion abort,
            GuardedMutationKind mutationKind,
            TransactionAugment augment) throws Exception {
        CatalogMutation catalog = prepareCatalogMutation(
                layout, current, decision.snapshot());
        LifecycleMutation lifecycleMutation = prepareLifecycleMutation(
                layout, current, decision.snapshot());
        ReferenceMutation reference = prepareReferenceMutation(
                current, decision.snapshot());
        if (catalog.capacityExceeded || lifecycleMutation.capacityExceeded
                || reference.capacityExceeded) {
            throw new IllegalStateException(
                    "existing Run retry exceeded authority capacity");
        }
        List<CuratorOp> operations = new ArrayList<>();
        if (recovered) {
            operations.add(client.transactionOp().check()
                    .withVersion(owner.version).forPath(layout.recoveryOwner));
        }
        operations.add(client.transactionOp().check()
                .withVersion(current.version).forPath(layout.snapshot));
        operations.add(client.transactionOp().check()
                .withVersion(lifecycle.version).forPath(lifecycle.path));
        operations.add(client.transactionOp().setData()
                .withVersion(current.version).forPath(layout.snapshot,
                        RunStateCodec.encodeSnapshot(decision.snapshot())));
        byte[] event = RunStateCodec.encodeEvent(decision.event());
        operations.add(client.transactionOp().create().forPath(
                eventPath(layout, decision.event().sequence()), event));
        operations.add(client.transactionOp().create().forPath(
                receiptPath(layout, commandId), event));
        catalog.appendTo(client, operations);
        lifecycleMutation.appendTo(client, operations);
        reference.appendTo(client, operations);
        appendGuardCheck(authority, operations);
        appendRetryEvidenceCleanup(
                layout, operations, assignments, journal, abort);
        augment.append(client, operations);
        if (retryWriteObserver != null) {
            retryWriteObserver.beforeCommit(operations.size());
        }
        observeGuardedMutation(mutationKind, -1,
                GuardedMutationMoment.BEFORE_COMMIT);
        client.transaction().forOperations(operations);
        observeTransactionWrite(mutationKind, -1, operations);
        observeGuardedMutation(mutationKind, -1,
                GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
    }

    private void appendRetryEvidenceCleanup(
            Layout layout, List<CuratorOp> operations,
            AssignmentView assignments, ManifestJournalView journal,
            AbortVersion abort) throws Exception {
        if (assignments == null) {
            if (journal != null) {
                throw corruption(layout.runId,
                        "manifest journal exists without assignments");
            }
            if (abort != null) {
                operations.add(client.transactionOp().delete()
                        .withVersion(abort.version).forPath(layout.sinkAbort));
            }
            return;
        }
        if (journal == null || journal.assignments != assignments
                || journal.memberVersions.length
                != assignments.assignmentVersions.length) {
            throw corruption(layout.runId,
                    "retry assignment/journal evidence mismatch");
        }
        for (int i = 0; i < assignments.assignmentVersions.length; i++) {
            operations.add(client.transactionOp().delete()
                    .withVersion(journal.memberVersions[i])
                    .forPath(manifestMemberShardPath(layout, i)));
            operations.add(client.transactionOp().delete()
                    .withVersion(assignments.terminalVersions[i])
                    .forPath(terminalShardPath(layout, i)));
            operations.add(client.transactionOp().delete()
                    .withVersion(assignments.acceptanceVersions[i])
                    .forPath(acceptanceShardPath(layout, i)));
            operations.add(client.transactionOp().delete()
                    .withVersion(assignments.assignmentVersions[i])
                    .forPath(assignmentShardPath(layout, i)));
        }
        operations.add(client.transactionOp().delete()
                .withVersion(journal.intentVersion)
                .forPath(layout.manifestIntent));
        operations.add(client.transactionOp().delete()
                .withVersion(assignments.rootVersion)
                .forPath(layout.assignmentRoot));
        if (abort != null) {
            operations.add(client.transactionOp().delete()
                    .withVersion(abort.version).forPath(layout.sinkAbort));
        }
    }

    private void checkRetryEvidenceView(
            AuthorityObservation authority, OwnerVersion owner,
            AssignmentView assignments,
            ManifestJournalView journal, AbortVersion abort) throws Exception {
        Layout layout = authority.layout;
        ManifestJournalView confirmedJournal = readManifestJournalView(layout);
        AssignmentView confirmedAssignments = confirmedJournal == null
                ? readAssignmentView(layout) : confirmedJournal.assignments;
        AbortVersion confirmedAbort = readAbortVersion(layout);
        if (!sameRetryEvidence(assignments, journal, abort,
                confirmedAssignments, confirmedJournal, confirmedAbort)) {
            throw new KeeperException.BadVersionException();
        }
        List<CuratorOp> operations = retryViewChecks(authority, owner);
        if (assignments != null) {
            operations.add(client.transactionOp().check()
                    .withVersion(assignments.rootVersion)
                    .forPath(layout.assignmentRoot));
            for (int i = 0; i < assignments.assignmentVersions.length; i++) {
                operations.add(client.transactionOp().check()
                        .withVersion(assignments.assignmentVersions[i])
                        .forPath(assignmentShardPath(layout, i)));
                operations.add(client.transactionOp().check()
                        .withVersion(assignments.acceptanceVersions[i])
                        .forPath(acceptanceShardPath(layout, i)));
                operations.add(client.transactionOp().check()
                        .withVersion(assignments.terminalVersions[i])
                        .forPath(terminalShardPath(layout, i)));
            }
        }
        if (journal != null) {
            for (int i = 0; i < journal.memberVersions.length; i++) {
                operations.add(client.transactionOp().check()
                        .withVersion(journal.memberVersions[i])
                        .forPath(manifestMemberShardPath(layout, i)));
            }
            operations.add(client.transactionOp().check()
                    .withVersion(journal.intentVersion)
                    .forPath(layout.manifestIntent));
        }
        if (abort != null) {
            operations.add(client.transactionOp().check()
                    .withVersion(abort.version).forPath(layout.sinkAbort));
        }
        confirmResultChecks(layout.runId, operations);
    }

    private static boolean sameRetryEvidence(
            AssignmentView firstAssignments, ManifestJournalView firstJournal,
            AbortVersion firstAbort, AssignmentView confirmedAssignments,
            ManifestJournalView confirmedJournal, AbortVersion confirmedAbort) {
        return sameAssignmentVersions(firstAssignments, confirmedAssignments)
                && sameJournalVersions(firstJournal, confirmedJournal)
                && (firstAbort == null ? confirmedAbort == null
                        : confirmedAbort != null
                        && firstAbort.version == confirmedAbort.version);
    }

    private static boolean sameAssignmentVersions(
            AssignmentView first, AssignmentView confirmed) {
        return first == null ? confirmed == null
                : confirmed != null
                && first.rootVersion == confirmed.rootVersion
                && Arrays.equals(first.assignmentVersions,
                        confirmed.assignmentVersions)
                && Arrays.equals(first.acceptanceVersions,
                        confirmed.acceptanceVersions)
                && Arrays.equals(first.terminalVersions,
                        confirmed.terminalVersions);
    }

    private static boolean sameJournalVersions(
            ManifestJournalView first, ManifestJournalView confirmed) {
        return first == null ? confirmed == null
                : confirmed != null
                && first.intentVersion == confirmed.intentVersion
                && Arrays.equals(first.memberVersions,
                        confirmed.memberVersions);
    }

    private void checkRetryView(
            AuthorityObservation authority, OwnerVersion owner) throws Exception {
        confirmResultChecks(authority.layout.runId,
                retryViewChecks(authority, owner));
    }

    private List<CuratorOp> retryViewChecks(
            AuthorityObservation authority, OwnerVersion owner) throws Exception {
        Layout layout = authority.layout;
        SnapshotVersion current = authority.snapshot;
        LifecycleEntry lifecycle = authority.lifecycle;
        verifyCatalogStable(layout, current);
        verifyReferenceStable(current);
        List<CuratorOp> operations = new ArrayList<>();
        operations.add(client.transactionOp().check()
                .withVersion(current.version).forPath(layout.snapshot));
        operations.add(client.transactionOp().check()
                .withVersion(lifecycle.version).forPath(lifecycle.path));
        appendGuardCheck(authority, operations);
        if (owner != null) {
            operations.add(client.transactionOp().check()
                    .withVersion(owner.version).forPath(layout.recoveryOwner));
        }
        return operations;
    }

    private static boolean deadlineDue(RunSnapshot snapshot, long observed) {
        return snapshot.deadlineEpochMillis() > 0L
                && observed >= snapshot.deadlineEpochMillis();
    }

    private static WholeAttemptRetryDecision retryDecision(
            WholeAttemptRetryDecision.Code code, RunSnapshot snapshot,
            WholeAttemptRetryClass retryClass, long observed) {
        return WholeAttemptRetryAuthorityKernel.decision(
                code, snapshot, retryClass, observed);
    }

    private static RetryReadyEvaluation retryReady(
            RetryReadyEvaluation.Code code, RunSnapshot snapshot,
            RetryAttemptIdentity identity, long observed, long notBefore,
            int remainingAttempts) {
        return WholeAttemptRetryAuthorityKernel.ready(code, snapshot, identity,
                observed, notBefore, remainingAttempts);
    }

    private static RetryAttemptClaim retryClaim(
            RetryAttemptClaim.Code code, RunSnapshot snapshot,
            RetryAttemptIdentity identity, long observed, long notBefore) {
        return WholeAttemptRetryAuthorityKernel.claim(
                code, snapshot, identity, observed, notBefore);
    }

    WholeAttemptRetryDecision staleRecoverySchedule(RunSnapshot snapshot) {
        return WholeAttemptRetryAuthorityKernel.staleRecoverySchedule(snapshot);
    }

    WholeAttemptRetryDecision tooLateSchedule(RunSnapshot snapshot) {
        return WholeAttemptRetryAuthorityKernel.tooLateSchedule(snapshot);
    }

    WholeAttemptRetryDecision unavailableSchedule(RunSnapshot snapshot) {
        return WholeAttemptRetryAuthorityKernel.unavailableSchedule(snapshot);
    }

    RetryAttemptClaim staleRecoveryClaim(RunSnapshot snapshot) {
        return WholeAttemptRetryAuthorityKernel.staleRecoveryClaim(snapshot);
    }

    RetryAttemptClaim tooLateClaim(RunSnapshot snapshot) {
        return WholeAttemptRetryAuthorityKernel.tooLateClaim(snapshot);
    }

    RetryAttemptClaim unavailableClaim(RunSnapshot snapshot) {
        return WholeAttemptRetryAuthorityKernel.unavailableClaim(snapshot);
    }

    RetryReadyEvaluation staleRecoveryReady(RunSnapshot snapshot) {
        return WholeAttemptRetryAuthorityKernel.staleRecoveryReady(snapshot);
    }

    private RunDecision apply(RunCommand command, RecoveryToken token, boolean recovered) {
        return apply(command, token, recovered, TransactionAugment.NONE);
    }

    RunDecision applyAugmented(
            RunCommand command, RecoveryToken token, boolean recovered,
            TransactionAugment augment) {
        if (augment == null) {
            throw new IllegalArgumentException("transaction augment is required");
        }
        return apply(command, token, recovered, augment);
    }

    AugmentedRecoveryCheck checkRecoveredAugmented(
            RecoveryToken token, RunSnapshot expected,
            TransactionAugment augment) {
        if (token == null || expected == null || augment == null
                || !token.runId().equals(expected.runId())) {
            throw new IllegalArgumentException(
                    "recovered augmented check fields are invalid");
        }
        Throwable lastRace = null;
        int consecutiveTransportFailures = 0;
        long observedRootCzxid = -1L;
        byte[] expectedBytes = RunStateCodec.encodeSnapshot(expected);
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        expected.runId(),
                        EvidenceReadPhase.APPLY_AFTER_INITIAL_SNAPSHOT,
                        observedRootCzxid);
                if (authority.rootCzxid < 0L
                        || authority.snapshot == null
                        || authority.lifecycle == null) {
                    return AugmentedRecoveryCheck.RUN_CONFLICT;
                }
                if (observedRootCzxid < 0L) {
                    observedRootCzxid = authority.rootCzxid;
                }
                OwnerVersion owner = readOwner(authority.layout);
                if (owner == null || !token.equals(owner.token)) {
                    confirmResultAuthority(authority, owner);
                    return AugmentedRecoveryCheck.STALE_RECOVERY_OWNER;
                }
                if (!MessageDigest.isEqual(
                        expectedBytes, RunStateCodec.encodeSnapshot(
                                authority.snapshot.snapshot))) {
                    confirmResultAuthority(authority, owner);
                    return AugmentedRecoveryCheck.RUN_CONFLICT;
                }
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check()
                        .withVersion(authority.snapshot.version)
                        .forPath(authority.layout.snapshot));
                operations.add(client.transactionOp().check()
                        .withVersion(authority.lifecycle.version)
                        .forPath(authority.lifecycle.path));
                appendGuardCheck(authority, operations);
                operations.add(client.transactionOp().check()
                        .withVersion(owner.version)
                        .forPath(authority.layout.recoveryOwner));
                augment.append(client, operations);
                observeGuardedMutation(
                        GuardedMutationKind.APPLY_EXISTING, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.APPLY_EXISTING, -1, operations);
                observeGuardedMutation(
                        GuardedMutationKind.APPLY_EXISTING, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                afterCommit.run();
                return AugmentedRecoveryCheck.ACCEPTED;
            } catch (KeeperException.BadVersionException
                     | KeeperException.NodeExistsException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
                consecutiveTransportFailures = 0;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(
                            expected.runId(), observedRootCzxid)) {
                        return AugmentedRecoveryCheck.STALE_RECOVERY_OWNER;
                    }
                } catch (Exception confirmationFailure) {
                    unknown.addSuppressed(confirmationFailure);
                }
                if (++consecutiveTransportFailures >= 2) {
                    throw unavailable(expected.runId(),
                            "reconcile augmented recovery check", unknown);
                }
            } catch (RunIncarnationChangedException changed) {
                return AugmentedRecoveryCheck.STALE_RECOVERY_OWNER;
            } catch (RunStateStoreUnavailableException unavailable) {
                lastRace = unavailable;
            } catch (IllegalArgumentException | IllegalStateException failure) {
                throw failure;
            } catch (Exception failure) {
                throw unavailable(expected.runId(),
                        "apply augmented recovery check", failure);
            }
        }
        throw unavailable(expected.runId(),
                "augmented recovery check did not converge", lastRace);
    }

    private RunDecision apply(
            RunCommand command, RecoveryToken token, boolean recovered,
            TransactionAugment augment) {
        if (command == null) throw new IllegalArgumentException("command is required");
        boolean createCommand = RunStateStoreKernel.evaluate(null, command)
                .accepted();
        Throwable lastRace = null;
        int consecutiveTransportFailures = 0;
        String stableMismatch = null;
        int stableMismatchCount = 0;
        long observedRootCzxid = -1L;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = null;
                Layout layout;
                SnapshotVersion current;
                LifecycleEntry retentionView;
                if (createCommand) {
                    layout = ensureLayout(command.runId());
                    current = readSnapshot(layout, false);
                    observeEvidenceRead(
                            EvidenceReadPhase.APPLY_AFTER_INITIAL_SNAPSHOT);
                    retentionView = readLifecycleEntry(command.runId());
                    observeEvidenceRead(
                            EvidenceReadPhase.APPLY_AFTER_INITIAL_LIFECYCLE);
                } else {
                    authority = observeCoreAuthority(
                            command.runId(), EvidenceReadPhase
                                    .APPLY_AFTER_INITIAL_SNAPSHOT,
                            observedRootCzxid);
                    if (authority.rootCzxid < 0L
                            || authority.snapshot == null) {
                        return recovered
                                ? RunStateStoreKernel.staleRecoveryOwner(null)
                                : RunStateStoreKernel.evaluate(null, command);
                    }
                    if (observedRootCzxid < 0L) {
                        observedRootCzxid = authority.rootCzxid;
                    }
                    observeEvidenceRead(
                            EvidenceReadPhase.APPLY_AFTER_INITIAL_LIFECYCLE);
                    AuthorityObservation confirmed = observeCoreAuthority(
                            command.runId(), null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    authority = confirmed;
                    layout = authority.layout;
                    current = authority.snapshot;
                    retentionView = authority.lifecycle;
                }
                if ((current == null) != (retentionView == null)) {
                    observeEvidenceRead(
                            EvidenceReadPhase.CORE_MISMATCH_BEFORE_CONFIRMATION);
                    SnapshotVersion confirmed = readSnapshot(layout, false);
                    LifecycleEntry confirmedLifecycle =
                            readLifecycleEntry(command.runId());
                    if (!sameCoreView(current, retentionView,
                            confirmed, confirmedLifecycle)) {
                        stableMismatch = null;
                        stableMismatchCount = 0;
                        lastRace = new CatalogRaceException();
                        continue;
                    }
                    String mismatch = current == null
                            ? "lifecycle exists without snapshot"
                            : "snapshot lacks lifecycle entry";
                    if (mismatch.equals(stableMismatch)) {
                        stableMismatchCount++;
                    } else {
                        stableMismatch = mismatch;
                        stableMismatchCount = 1;
                    }
                    if (stableMismatchCount >= MAX_RETRIES) {
                        throw corruption(command.runId(), mismatch);
                    }
                    continue;
                }
                if (current != null) {
                    stableMismatch = null;
                    stableMismatchCount = 0;
                }
                if (retentionView != null
                        && retentionView.ref.state() == RunLifecycleState.DELETING) {
                    verifyExistingLifecycleOrRace(layout, current, retentionView);
                    if (authority != null) confirmResultAuthority(authority);
                    return RunStateStoreKernel.retentionInProgress(current.snapshot);
                }
                OwnerVersion currentOwner = readOwner(layout);
                if (current == null && currentOwner != null) {
                    if (readSnapshot(layout, false) != null) {
                        stableMismatch = null;
                        stableMismatchCount = 0;
                        lastRace = new CatalogRaceException();
                        continue;
                    }
                    String mismatch = "recovery owner exists without snapshot";
                    if (mismatch.equals(stableMismatch)) stableMismatchCount++;
                    else { stableMismatch = mismatch; stableMismatchCount = 1; }
                    if (stableMismatchCount >= MAX_RETRIES) {
                        throw corruption(command.runId(), mismatch);
                    }
                    continue;
                }
                if (!recovered && currentOwner != null) {
                    if (authority != null) {
                        confirmResultAuthority(authority, currentOwner);
                    }
                    return RunStateStoreKernel.recoveryOwned(current.snapshot);
                }
                if (recovered && (currentOwner == null
                        || !token.equals(currentOwner.token))) {
                    if (authority != null) {
                        confirmResultAuthority(authority, currentOwner);
                    }
                    return RunStateStoreKernel.staleRecoveryOwner(
                            current == null ? null : current.snapshot);
                }
                Receipt receipt = readReceipt(layout, command.commandId());
                observeEvidenceRead(EvidenceReadPhase.APPLY_AFTER_RECEIPT);
                if (receipt != null) {
                    if (current == null) {
                        if (readSnapshot(layout, false) != null) {
                            stableMismatch = null;
                            stableMismatchCount = 0;
                            lastRace = new CatalogRaceException();
                            continue;
                        }
                        String mismatch = "receipt exists without snapshot";
                        if (mismatch.equals(stableMismatch)) stableMismatchCount++;
                        else { stableMismatch = mismatch; stableMismatchCount = 1; }
                        if (stableMismatchCount >= MAX_RETRIES) {
                            throw corruption(command.runId(), mismatch);
                        }
                        continue;
                    }
                    verifyCatalogStable(layout, current);
                    verifyLifecycleStable(layout, current);
                    verifyReferenceStable(current);
                    if (authority != null) {
                        if (recovered) {
                            checkRecoveryMutationView(
                                    authority, currentOwner);
                        } else {
                            confirmResultAuthority(authority);
                        }
                    }
                    return MessageDigest.isEqual(receipt.event.commandDigest(), command.commandDigest())
                            ? RunStateStoreKernel.replayed(current.snapshot, receipt.event)
                            : RunStateStoreKernel.commandIdConflict(current.snapshot);
                }

                RunDecision decision = RunStateStoreKernel.evaluate(
                        current == null ? null : current.snapshot, command);
                if (!decision.accepted()) {
                    if (authority != null) {
                        confirmResultAuthority(authority, currentOwner);
                    }
                    return decision;
                }
                if (current == null
                        && hasCreateDependentAuthority(layout, decision.snapshot())) {
                    if (readSnapshot(layout, false) != null) {
                        stableMismatch = null;
                        stableMismatchCount = 0;
                        lastRace = new CatalogRaceException();
                        continue;
                    }
                    String mismatch = "dependent authority exists without snapshot";
                    if (mismatch.equals(stableMismatch)) stableMismatchCount++;
                    else { stableMismatch = mismatch; stableMismatchCount = 1; }
                    if (stableMismatchCount >= MAX_RETRIES) {
                        throw corruption(command.runId(), mismatch);
                    }
                    continue;
                }
                CatalogMutation catalog;
                LifecycleMutation lifecycle;
                ReferenceMutation reference;
                try {
                    catalog = prepareCatalogMutation(
                            layout, current, decision.snapshot());
                    lifecycle = prepareLifecycleMutation(
                            layout, current, decision.snapshot());
                    reference = prepareReferenceMutation(
                            current, decision.snapshot());
                } catch (IllegalStateException dependentFailure) {
                    if (current != null) throw dependentFailure;
                    if (readSnapshot(layout, false) != null) {
                        stableMismatch = null;
                        stableMismatchCount = 0;
                        lastRace = new CatalogRaceException();
                        continue;
                    }
                    String mismatch = dependentFailure.getMessage();
                    if (mismatch == null) {
                        mismatch = "dependent authority is corrupt without snapshot";
                    }
                    if (mismatch.equals(stableMismatch)) stableMismatchCount++;
                    else { stableMismatch = mismatch; stableMismatchCount = 1; }
                    if (stableMismatchCount >= MAX_RETRIES) throw dependentFailure;
                    continue;
                }
                stableMismatch = null;
                stableMismatchCount = 0;
                if (catalog.capacityExceeded || lifecycle.capacityExceeded
                        || reference.capacityExceeded) {
                    if (authority != null) {
                        confirmResultAuthority(authority, currentOwner);
                    }
                    return RunStateStoreKernel.limitExceeded(
                            current == null ? null : current.snapshot);
                }
                byte[] snapshot = RunStateCodec.encodeSnapshot(decision.snapshot());
                byte[] event = RunStateCodec.encodeEvent(decision.event());
                List<CuratorOp> operations = new ArrayList<>();
                if (current == null) {
                    operations.add(client.transactionOp().create()
                            .forPath(layout.snapshot, snapshot));
                } else {
                    operations.add(client.transactionOp().check()
                            .withVersion(current.version).forPath(layout.snapshot));
                    operations.add(client.transactionOp().setData()
                            .withVersion(current.version).forPath(layout.snapshot, snapshot));
                }
                if (recovered) {
                    operations.add(0, client.transactionOp().check()
                            .withVersion(currentOwner.version).forPath(layout.recoveryOwner));
                }
                operations.add(client.transactionOp().create().forPath(
                        eventPath(layout, decision.event().sequence()), event));
                operations.add(client.transactionOp().create().forPath(
                        receiptPath(layout, command.commandId()), event));
                catalog.appendTo(client, operations);
                lifecycle.appendTo(client, operations);
                reference.appendTo(client, operations);
                if (current != null) appendGuardCheck(authority, operations);
                if (decision.snapshot().status() == io.rowweave.run.RunStatus.READY) {
                    appendAssignmentCleanup(layout, operations);
                }
                augment.append(client, operations);
                if (current != null) {
                    observeGuardedMutation(GuardedMutationKind.APPLY_EXISTING, -1,
                            GuardedMutationMoment.BEFORE_COMMIT);
                }
                client.transaction().forOperations(operations);
                if (current != null) {
                    observeTransactionWrite(
                            GuardedMutationKind.APPLY_EXISTING, -1, operations);
                    observeGuardedMutation(GuardedMutationKind.APPLY_EXISTING, -1,
                            GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                }
                afterCommit.run();
                return decision;
            } catch (KeeperException.BadVersionException
                     | KeeperException.NodeExistsException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
                consecutiveTransportFailures = 0;
                stableMismatch = null;
                stableMismatchCount = 0;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                stableMismatch = null;
                stableMismatchCount = 0;
                try {
                    if (!confirmMutationAuthority(
                            command.runId(), observedRootCzxid)) {
                        return recovered
                                ? RunStateStoreKernel.staleRecoveryOwner(null)
                                : RunStateStoreKernel.evaluate(null, command);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(command.commandId(),
                            "confirm unknown ZK run mutation outcome",
                            confirmationFailure);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
                if (++consecutiveTransportFailures >= 2) {
                    throw unavailable(command.commandId(),
                            "reconcile unknown ZK run mutation outcome", unknown);
                }
                // One further iteration performs receipt reconciliation; never spin through all CAS retries offline.
            } catch (RunStateStoreUnavailableException unavailable) {
                lastRace = unavailable;
                stableMismatch = null;
                stableMismatchCount = 0;
            } catch (CatalogRaceException raced) {
                lastRace = raced;
                stableMismatch = null;
                stableMismatchCount = 0;
            } catch (RunIncarnationChangedException changed) {
                return recovered
                        ? RunStateStoreKernel.staleRecoveryOwner(null)
                        : RunStateStoreKernel.evaluate(null, command);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(command.commandId(), "apply ZK run mutation", failure);
            }
        }
        throw unavailable(command.commandId(), "ZK run mutation did not converge", lastRace);
    }

    private boolean hasCreateDependentAuthority(
            Layout layout, RunSnapshot intended) throws Exception {
        if (readCatalogEntry(layout.runId) != null
                || readLifecycleEntry(layout.runId) != null) {
            return true;
        }
        if (!ArtifactReferenceRef.isContentAddressed(intended.artifactId())) {
            return false;
        }
        ReferenceCountVersion root = readReferenceRoot(intended.artifactId());
        if (root == null) return false;
        ReferenceShard shard = referenceShard(
                intended.artifactId(), intended.runId());
        ReferenceShardCountVersion shardCount = readReferenceShard(shard);
        return shardCount != null
                && readReferenceEntry(shard, intended.runId()) != null;
    }

    private static boolean sameCoreView(
            SnapshotVersion first, LifecycleEntry firstLifecycle,
            SnapshotVersion confirmed, LifecycleEntry confirmedLifecycle) {
        boolean sameSnapshot = first == null
                ? confirmed == null
                : confirmed != null && first.version == confirmed.version
                && first.czxid == confirmed.czxid;
        boolean sameLifecycle = firstLifecycle == null
                ? confirmedLifecycle == null
                : confirmedLifecycle != null
                && firstLifecycle.version == confirmedLifecycle.version
                && firstLifecycle.czxid == confirmedLifecycle.czxid;
        return sameSnapshot && sameLifecycle;
    }

    private static boolean sameAuthorityView(
            AuthorityObservation first, AuthorityObservation confirmed) {
        return first.rootCzxid == confirmed.rootCzxid
                && first.guard.equals(confirmed.guard)
                && sameCoreView(first.snapshot, first.lifecycle,
                        confirmed.snapshot, confirmed.lifecycle);
    }

    private static CoreMismatch coreMismatch(AuthorityObservation authority) {
        if (authority.snapshot != null && authority.lifecycle == null) {
            return CoreMismatch.SNAPSHOT_ONLY;
        }
        if (authority.snapshot == null && authority.lifecycle != null) {
            return CoreMismatch.LIFECYCLE_ONLY;
        }
        if (authority.rootCzxid >= 0L && authority.snapshot == null) {
            return CoreMismatch.LAYOUT_ONLY;
        }
        return null;
    }

    private static String authorityMismatchKey(
            AuthorityObservation authority, CoreMismatch mismatch) {
        return mismatch.name() + ":" + authority.rootCzxid + ":"
                + authority.guard.present + ":" + authority.guard.version + ":"
                + authority.guard.generation + ":"
                + (authority.snapshot == null ? "-"
                        : authority.snapshot.version + "/" + authority.snapshot.czxid)
                + ":" + (authority.lifecycle == null ? "-"
                        : authority.lifecycle.version + "/" + authority.lifecycle.czxid);
    }

    private enum CoreMismatch {
        SNAPSHOT_ONLY("snapshot lacks lifecycle entry"),
        LIFECYCLE_ONLY("lifecycle exists without snapshot"),
        LAYOUT_ONLY("run layout exists without snapshot and lifecycle");

        private final String detail;

        CoreMismatch(String detail) {
            this.detail = detail;
        }
    }

    @Override
    public RunSnapshot get(String runId) {
        requireIdentity(runId, "runId");
        long validatedRootCzxid = -1L;
        String stableMismatch = null;
        CoreMismatch stableMismatchKind = null;
        int stableMismatchCount = 0;
        try {
            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                AuthorityObservation authority = observeAuthority(runId);
                observeEvidenceRead(
                        EvidenceReadPhase.EVIDENCE_AFTER_INITIAL_SNAPSHOT);
                CoreMismatch mismatch = coreMismatch(authority);
                if (mismatch != null) {
                    if (mismatch == CoreMismatch.LAYOUT_ONLY
                            && validatedRootCzxid != authority.rootCzxid) {
                        return null;
                    }
                    observeEvidenceRead(
                            EvidenceReadPhase.CORE_MISMATCH_BEFORE_CONFIRMATION);
                    String key = authorityMismatchKey(authority, mismatch);
                    if (key.equals(stableMismatch)) stableMismatchCount++;
                    else { stableMismatch = key; stableMismatchCount = 1; }
                    stableMismatchKind = mismatch;
                    if (stableMismatchCount >= MAX_RETRIES) {
                        throw corruption(runId, mismatch.detail);
                    }
                    continue;
                }
                if (authority.rootCzxid < 0L) return null;
                try {
                    SnapshotVersion current = authority.snapshot;
                    LifecycleEntry lifecycle = authority.lifecycle;
                    verifyExistingLifecycleOrRace(
                            authority.layout, current, lifecycle);
                    validatedRootCzxid = authority.rootCzxid;
                    stableMismatch = null;
                    stableMismatchKind = null;
                    stableMismatchCount = 0;
                    observeEvidenceRead(
                            EvidenceReadPhase.CORE_AFTER_VALIDATED_PAIR);
                    if (lifecycle.ref.state() != RunLifecycleState.DELETING) {
                        verifyLatestEventAndReceipt(
                                authority.layout, current.snapshot);
                        verifyCatalogStable(authority.layout, current);
                        verifyReferenceStable(current);
                    }
                    AuthorityObservation confirmed = observeAuthority(runId);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return confirmed.snapshot.snapshot;
                } catch (CatalogRaceException raced) {
                    // Snapshot changed between its read and catalog verification; retry a coherent view.
                } catch (IllegalStateException possibleRace) {
                    AuthorityObservation confirmed = observeAuthority(runId);
                    if (sameAuthorityView(authority, confirmed)) {
                        throw possibleRace;
                    }
                }
            }
            if (stableMismatchKind != null) {
                throw corruption(runId, stableMismatchKind.detail);
            }
            throw unavailable(null, "read coherent ZK run/catalog snapshot", null);
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            throw corruption;
        } catch (Exception failure) {
            throw unavailable(null, "read ZK run snapshot", failure);
        }
    }

    @Override
    public DurableExecutionEvidenceView executionEvidence(String runId) {
        requireIdentity(runId, "runId");
        long validatedRootCzxid = -1L;
        String stableMismatch = null;
        CoreMismatch stableMismatchKind = null;
        int stableMismatchCount = 0;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            final AuthorityObservation authority;
            try {
                observeEvidenceRead(EvidenceReadPhase.BEFORE_INITIAL_SNAPSHOT);
                authority = observeAuthority(runId);
                observeEvidenceRead(EvidenceReadPhase.EVIDENCE_AFTER_INITIAL_SNAPSHOT);
                CoreMismatch mismatch = coreMismatch(authority);
                if (mismatch != null) {
                    if (mismatch == CoreMismatch.LAYOUT_ONLY
                            && validatedRootCzxid != authority.rootCzxid) {
                        AuthorityObservation confirmed = observeAuthority(runId);
                        if (sameAuthorityView(authority, confirmed)) {
                            return DurableExecutionEvidenceView.missing();
                        }
                        continue;
                    }
                    observeEvidenceRead(
                            EvidenceReadPhase.CORE_MISMATCH_BEFORE_CONFIRMATION);
                    observeEvidenceRead(EvidenceReadPhase
                            .EVIDENCE_BEFORE_INITIAL_MISMATCH_CONFIRMATION);
                    AuthorityObservation confirmed = observeAuthority(runId);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    String key = authorityMismatchKey(authority, mismatch);
                    if (key.equals(stableMismatch)) stableMismatchCount++;
                    else { stableMismatch = key; stableMismatchCount = 1; }
                    stableMismatchKind = mismatch;
                    if (stableMismatchCount >= MAX_RETRIES) {
                        throw corruption(runId, mismatch.detail);
                    }
                    continue;
                }
                if (authority.rootCzxid < 0L) {
                    observeEvidenceRead(
                            EvidenceReadPhase.FINAL_ABSENCE_BEFORE_CONFIRMATION);
                    AuthorityObservation confirmed = observeAuthority(runId);
                    if (sameAuthorityView(authority, confirmed)) {
                        return DurableExecutionEvidenceView.missing();
                    }
                    continue;
                }
                stableMismatch = null;
                stableMismatchKind = null;
                stableMismatchCount = 0;
                verifyExistingLifecycleOrRace(authority.layout,
                        authority.snapshot, authority.lifecycle);
                if (authority.lifecycle.ref.state()
                        != RunLifecycleState.DELETING) {
                    verifyLatestEventAndReceipt(
                            authority.layout, authority.snapshot.snapshot);
                    verifyCatalogStable(authority.layout, authority.snapshot);
                    verifyReferenceStable(authority.snapshot);
                }
                validatedRootCzxid = authority.rootCzxid;
            } catch (KeeperException.NoNodeException
                     | KeeperException.BadVersionException
                     | CatalogRaceException raced) {
                continue;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException transportFailure) {
                throw unavailable(null,
                        "read initial ZK execution evidence snapshot",
                        transportFailure);
            } catch (IllegalArgumentException | IllegalStateException corruption) {
                throw corruption;
            } catch (Exception failure) {
                throw unavailable(null,
                        "read initial ZK execution evidence snapshot", failure);
            }

            AssignmentView assignments = null;
            boolean assignmentRaced = false;
            try {
                observeEvidenceRead(EvidenceReadPhase.BEFORE_ASSIGNMENT);
                assignments = readAssignmentView(authority.layout);
            } catch (KeeperException.NoNodeException
                     | KeeperException.BadVersionException raced) {
                assignmentRaced = true;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException transportFailure) {
                return confirmedSnapshotOnly(authority, transportFailure);
            } catch (IllegalArgumentException | IllegalStateException
                     | LinkageError evidenceFailure) {
                return confirmedSnapshotOnly(authority, evidenceFailure);
            } catch (Exception evidenceFailure) {
                return confirmedSnapshotOnly(authority, evidenceFailure);
            }
            try {
                observeEvidenceRead(EvidenceReadPhase.BEFORE_CONFIRMATION);
                AuthorityObservation confirmed = observeAuthority(runId);
                observeEvidenceRead(EvidenceReadPhase.EVIDENCE_AFTER_CONFIRMATION_SNAPSHOT);
                AuthorityObservation finalAuthority = observeAuthority(runId);
                if (!sameAuthorityView(confirmed, finalAuthority)) continue;
                if (finalAuthority.rootCzxid < 0L) {
                    observeEvidenceRead(
                            EvidenceReadPhase.FINAL_ABSENCE_BEFORE_CONFIRMATION);
                    AuthorityObservation absent = observeAuthority(runId);
                    if (sameAuthorityView(finalAuthority, absent)) {
                        return DurableExecutionEvidenceView.missing();
                    }
                    continue;
                }
                if (!sameAuthorityView(authority, finalAuthority)) continue;
                if (assignmentRaced || assignments == null) {
                    return DurableExecutionEvidenceView.snapshotOnly(
                            authority.snapshot.snapshot);
                }
                return DurableExecutionEvidenceView.present(
                        authority.snapshot.snapshot, assignments.snapshot);
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException transportFailure) {
                throw unavailable(null,
                        "confirm coherent ZK execution evidence",
                        transportFailure);
            } catch (KeeperException.NoNodeException
                     | KeeperException.BadVersionException
                     | CatalogRaceException raced) {
                // Bounded retry: final purge is an atomic snapshot+lifecycle+evidence delete.
            } catch (IllegalArgumentException | IllegalStateException corruption) {
                throw corruption;
            } catch (Exception evidenceFailure) {
                throw unavailable(null,
                        "confirm coherent ZK execution evidence",
                        evidenceFailure);
            }
        }
        if (stableMismatchKind != null) {
            throw corruption(runId, stableMismatchKind.detail);
        }
        throw unavailable(null, "read coherent ZK execution evidence", null);
    }

    private DurableExecutionEvidenceView confirmedSnapshotOnly(
            AuthorityObservation authority, Throwable evidenceFailure) {
        try {
            AuthorityObservation confirmed = observeAuthority(
                    authority.layout.runId);
            if (sameAuthorityView(authority, confirmed)) {
                return DurableExecutionEvidenceView.snapshotOnly(
                        authority.snapshot.snapshot);
            }
            throw unavailable(null,
                    "discard stale ZK execution evidence fallback",
                    evidenceFailure);
        } catch (RunStateStoreUnavailableException unavailable) {
            throw unavailable;
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            throw corruption;
        } catch (Exception confirmationFailure) {
            throw unavailable(null,
                    "confirm ZK execution evidence fallback",
                    confirmationFailure);
        }
    }

    private void observeEvidenceRead(EvidenceReadPhase phase) throws Exception {
        if (evidenceReadObserver != null) evidenceReadObserver.at(phase);
    }

    /**
     * Exposes a same-call, fully validated core pair only to an installed test
     * observer. Production instances install no observer and therefore perform
     * neither the additional lifecycle read nor any callback.
     */
    private void observeVerifiedCoreForTest(
            Layout layout, SnapshotVersion snapshot) throws Exception {
        if (evidenceReadObserver == null || snapshot == null) return;
        LifecycleEntry lifecycle = readLifecycleEntry(layout.runId);
        if (lifecycle == null) return;
        verifyExistingLifecycleOrRace(layout, snapshot, lifecycle);
        observeEvidenceRead(EvidenceReadPhase.CORE_AFTER_VALIDATED_PAIR);
    }

    private void observeVerifiedCoreForTest(Layout layout) throws Exception {
        if (evidenceReadObserver == null) return;
        observeVerifiedCoreForTest(layout, readSnapshot(layout, false));
    }

    private void observeGuardedMutation(
            GuardedMutationKind kind, int ordinal,
            GuardedMutationMoment moment) throws Exception {
        if (guardedMutationObserver != null) {
            guardedMutationObserver.at(kind, ordinal, moment);
        }
    }

    /** Test-only exact accounting of the real ZooKeeper multi request. */
    private void observeTransactionWrite(
            GuardedMutationKind kind, int ordinal,
            List<CuratorOp> operations) throws Exception {
        if (transactionWriteObserver == null) return;
        transactionWriteObserver.onWrite(kind, ordinal, operations.size(),
                serializedMultiRequestBytes(operations));
    }

    @Override
    public List<ExecutionEvent> events(String runId, long afterSequence, int limit) {
        if (afterSequence < 0L) throw new IllegalArgumentException("afterSequence must be >= 0");
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("limit must be in [1,1000]");
        requireIdentity(runId, "runId");
        long validatedRootCzxid = -1L;
        String stableMismatch = null;
        String stableMismatchDetail = null;
        int stableMismatchCount = 0;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeAuthority(runId);
                observeEvidenceRead(EvidenceReadPhase.EVENTS_AFTER_INITIAL_SNAPSHOT);
                observeEvidenceRead(EvidenceReadPhase.EVENTS_AFTER_INITIAL_LIFECYCLE);
                CoreMismatch coreMismatch = coreMismatch(authority);
                if (coreMismatch != null
                        && coreMismatch != CoreMismatch.LAYOUT_ONLY) {
                    observeEvidenceRead(
                            EvidenceReadPhase.CORE_MISMATCH_BEFORE_CONFIRMATION);
                    AuthorityObservation confirmed = observeAuthority(runId);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    String key = authorityMismatchKey(authority, coreMismatch);
                    if (key.equals(stableMismatch)) stableMismatchCount++;
                    else { stableMismatch = key; stableMismatchCount = 1; }
                    stableMismatchDetail = coreMismatch.detail;
                    if (stableMismatchCount >= MAX_RETRIES) {
                        throw corruption(runId, stableMismatchDetail);
                    }
                    continue;
                }
                if (authority.rootCzxid < 0L) {
                    observeEvidenceRead(
                            EvidenceReadPhase.FINAL_ABSENCE_BEFORE_CONFIRMATION);
                    AuthorityObservation confirmed = observeAuthority(runId);
                    if (sameAuthorityView(authority, confirmed)) {
                        return Collections.emptyList();
                    }
                    continue;
                }

                if (coreMismatch == null) {
                    verifyExistingLifecycleOrRace(authority.layout,
                            authority.snapshot, authority.lifecycle);
                    validatedRootCzxid = authority.rootCzxid;
                    stableMismatch = null;
                    stableMismatchDetail = null;
                    stableMismatchCount = 0;
                }
                List<String> children = client.getChildren()
                        .forPath(authority.layout.events);
                observeEvidenceRead(EvidenceReadPhase.EVENTS_AFTER_CHILDREN);
                children.sort(Comparator.naturalOrder());
                String structuralFailure = children.size() > maxEventNodes
                        ? "event child limit exceeded" : null;
                List<String> pageChildren = new ArrayList<>(
                        Math.min(limit, children.size()));
                List<byte[]> pagePayloads = new ArrayList<>(
                        Math.min(limit, children.size()));
                if (structuralFailure == null) {
                    for (int i = 0; i < children.size(); i++) {
                        String child = children.get(i);
                        String expected = sequenceName(i + 1L);
                        if (!expected.equals(child)) {
                            structuralFailure =
                                    "event sequence gap or invalid child: " + child;
                            break;
                        }
                        long sequence = i + 1L;
                        if (sequence <= afterSequence
                                || pageChildren.size() == limit) {
                            continue;
                        }
                        pageChildren.add(child);
                        pagePayloads.add(client.getData().forPath(
                                authority.layout.events + "/" + child));
                    }
                }

                AuthorityObservation confirmed = observeAuthority(runId);
                if (!sameAuthorityView(authority, confirmed)) continue;
                if (coreMismatch == CoreMismatch.LAYOUT_ONLY) {
                    if (children.isEmpty()) {
                        if (validatedRootCzxid == authority.rootCzxid) {
                            String key = authorityMismatchKey(
                                    authority, coreMismatch);
                            if (key.equals(stableMismatch)) stableMismatchCount++;
                            else { stableMismatch = key; stableMismatchCount = 1; }
                            stableMismatchDetail = coreMismatch.detail;
                            if (stableMismatchCount >= MAX_RETRIES) {
                                throw corruption(runId, stableMismatchDetail);
                            }
                            continue;
                        }
                        observeEvidenceRead(
                                EvidenceReadPhase.FINAL_ABSENCE_BEFORE_CONFIRMATION);
                        return Collections.emptyList();
                    }
                    String mismatch = "events exist without snapshot and lifecycle";
                    String key = authorityMismatchKey(authority, coreMismatch)
                            + ":events:" + children.size();
                    if (key.equals(stableMismatch)) stableMismatchCount++;
                    else { stableMismatch = key; stableMismatchCount = 1; }
                    stableMismatchDetail = mismatch;
                    if (stableMismatchCount >= MAX_RETRIES) {
                        throw corruption(runId, mismatch);
                    }
                    continue;
                }
                stableMismatch = null;
                stableMismatchDetail = null;
                stableMismatchCount = 0;
                if (confirmed.lifecycle.ref.state()
                        == RunLifecycleState.DELETING) {
                    throw new RunHistoryRetiringException(runId);
                }
                if (structuralFailure != null) {
                    throw corruption(runId, structuralFailure);
                }
                if (confirmed.snapshot.snapshot.revision() != children.size()) {
                    throw corruption(runId, "snapshot/event revision mismatch");
                }
                List<ExecutionEvent> page = new ArrayList<>(pageChildren.size());
                for (int i = 0; i < pageChildren.size(); i++) {
                    String child = pageChildren.get(i);
                    long sequence = Long.parseLong(child);
                    ExecutionEvent event = RunStateCodec.decodeEvent(
                            pagePayloads.get(i));
                    if (!runId.equals(event.runId())) {
                        throw corruption(runId, "event runId mismatch");
                    }
                    if (event.sequence() != sequence) {
                        throw corruption(runId, "event path mismatch");
                    }
                    page.add(event);
                }
                return Collections.unmodifiableList(page);
            } catch (KeeperException.NoNodeException
                     | KeeperException.BadVersionException
                     | CatalogRaceException raced) {
                stableMismatch = null;
                stableMismatchCount = 0;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException transportFailure) {
                throw unavailable(null, "read ZK run events", transportFailure);
            } catch (IllegalArgumentException | IllegalStateException corruption) {
                throw corruption;
            } catch (Exception failure) {
                throw unavailable(null, "read ZK run events", failure);
            }
        }
        if (stableMismatch != null) {
            throw corruption(runId, stableMismatchDetail);
        }
        throw unavailable(null, "read coherent ZK run events", null);
    }

    @Override
    public RunLifecycleRef lifecycle(String runId) {
        requireIdentity(runId, "runId");
        try {
            LifecycleEntry entry = readLifecycleEntry(runId);
            return entry == null ? null : entry.ref;
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            throw corruption;
        } catch (Exception failure) {
            throw unavailable(null, "read ZK run lifecycle", failure);
        }
    }

    @Override
    public RunLifecyclePage scanLifecycle(String cursor, int limit) {
        RunLifecycleCatalogKernel.validateLimit(limit);
        RunLifecycleCatalogKernel.Cursor position =
                RunLifecycleCatalogKernel.decodeCursor(cursor);
        LifecycleShard shard = lifecycleShard(position.shard());
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                LifecycleShardView view = readStableLifecycleShard(shard, false);
                if (view.guardVersion < 0) {
                    return RunLifecycleCatalogKernel.page(
                            cursor, limit, Collections.emptyList());
                }
                List<RunLifecycleRef> terminal = new ArrayList<>();
                for (String child : view.children) {
                    validateLifecycleChild(shard.number, child);
                    Stat stat = new Stat();
                    RunLifecycleRef stored = RunRetentionCodec.decodeRef(
                            client.getData().storingStatIn(stat)
                                    .forPath(shard.entries + "/" + child));
                    RunLifecycleRef ref = lifecycleWithAuthorityTime(stored, stat.getMtime());
                    if (!child.equals(RunLifecycleCatalogKernel.digestHex(ref.runId()))) {
                        throw corruption(ref.runId(), "lifecycle entry identity/hash mismatch");
                    }
                    if (ref.state() != RunLifecycleState.ACTIVE) terminal.add(ref);
                }
                Stat after = new Stat();
                byte[] guard = client.getData().storingStatIn(after).forPath(shard.guard);
                verifyLifecycleGuard(guard, shard.number);
                if (after.getVersion() != view.guardVersion) continue;
                return RunLifecycleCatalogKernel.page(cursor, limit, terminal);
            } catch (KeeperException.NoNodeException
                     | KeeperException.BadVersionException raced) {
                lastRace = raced;
            } catch (IllegalArgumentException | IllegalStateException corruption) {
                throw corruption;
            } catch (Exception failure) {
                throw unavailable(null, "scan ZK run lifecycle", failure);
            }
        }
        throw unavailable(null, "ZK run lifecycle scan did not converge", lastRace);
    }

    @Override
    public RunRetentionClaim claim(
            String runId, long expectedTerminalRevision, long cutoffMillis,
            RetentionOwner owner, long observedOwnershipEpoch) {
        requireIdentity(runId, "runId");
        RunRetentionAuthorityKernel.evaluate(
                runId, null, null, false, expectedTerminalRevision,
                cutoffMillis, owner, observedOwnershipEpoch);
        Throwable lastRace = null;
        long observedRootCzxid = -1L;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                if (authority.rootCzxid < 0L
                        || authority.snapshot == null) {
                    return RunRetentionClaim.staleRun();
                }
                if (observedRootCzxid < 0L) {
                    observedRootCzxid = authority.rootCzxid;
                }
                Layout layout = authority.layout;
                SnapshotVersion snapshot = authority.snapshot;
                LifecycleEntry lifecycle = authority.lifecycle;
                CatalogEntry recovery = readCatalogEntry(runId);
                AuthorityObservation confirmed = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                if (!sameAuthorityView(authority, confirmed)) {
                    lastRace = new CatalogRaceException();
                    continue;
                }
                authority = confirmed;
                snapshot = authority.snapshot;
                lifecycle = authority.lifecycle;
                verifyObservedCatalogStable(layout, snapshot, recovery);
                RunRetentionAuthorityKernel.Transition transition =
                        RunRetentionAuthorityKernel.evaluate(
                                runId, snapshot.snapshot, lifecycle.ref,
                                recovery != null, expectedTerminalRevision,
                                cutoffMillis, owner, observedOwnershipEpoch);
                if (transition.nextLifecycle() == null) {
                    return transition.claim();
                }
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check()
                        .withVersion(snapshot.version).forPath(layout.snapshot));
                operations.add(client.transactionOp().setData()
                        .withVersion(lifecycle.version).forPath(lifecycle.path,
                                RunRetentionCodec.encodeRef(
                                        transition.nextLifecycle())));
                appendGuardCheck(authority, operations);
                observeGuardedMutation(GuardedMutationKind.RETENTION_CLAIM, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.RETENTION_CLAIM, -1, operations);
                observeGuardedMutation(GuardedMutationKind.RETENTION_CLAIM, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                afterRetentionCommit.run();
                return transition.claim();
            } catch (KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(
                            runId, observedRootCzxid)) {
                        return RunRetentionClaim.staleRun();
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK retention claim outcome",
                            confirmationFailure);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
            } catch (CatalogRaceException raced) {
                lastRace = raced;
            } catch (RunIncarnationChangedException changed) {
                return RunRetentionClaim.staleRun();
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(null, "claim ZK retention ownership", failure);
            }
        }
        throw unavailable(null, "ZK retention claim did not converge", lastRace);
    }

    @Override
    public boolean current(RetentionToken token) {
        if (token == null) throw new IllegalArgumentException("token is required");
        return RunRetentionAuthorityKernel.current(lifecycle(token.runId()), token);
    }

    @Override
    public ArtifactReferenceView inspect(String artifactId) {
        String id = ArtifactReferenceRef.requireContentAddressed(artifactId);
        try {
            ReferenceCountVersion root = readReferenceRoot(id);
            if (root != null) {
                if (root.count <= 0L) {
                    throw referenceCorruption(id,
                            "artifact root has non-positive reference count");
                }
                return ArtifactReferenceView.referenced(root.count);
            }
            ArtifactBackfillStatus status = backfillStatus();
            if (status.code() != ArtifactBackfillStatus.Code.READY) {
                throw new IllegalStateException(
                        "Artifact reference authority is not READY: "
                                + status.code());
            }
            return ArtifactReferenceView.unreferenced();
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            throw corruption;
        } catch (Exception failure) {
            throw unavailable(null, "inspect ZK Artifact references", failure);
        }
    }

    @Override
    public ArtifactBackfillStatus backfillStatus() {
        try {
            ReferenceBackfillState state = readReferenceBackfillState();
            if (state.status == REFERENCE_READY) {
                return ArtifactBackfillStatus.ready();
            }
            return ArtifactBackfillStatus.backfilling(
                    state.cursor.encode(), state.processedEntries);
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            return ArtifactBackfillStatus.corrupt(corruption.getMessage());
        } catch (Exception failure) {
            throw unavailable(null,
                    "read ZK Artifact reference backfill state", failure);
        }
    }

    @Override
    public ArtifactBackfillStatus advanceBackfill(int maxEntries) {
        if (maxEntries < 1 || maxEntries > 1000) {
            throw new IllegalArgumentException("maxEntries must be in [1,1000]");
        }
        try {
            return advanceReferenceBackfill(maxEntries);
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            return ArtifactBackfillStatus.corrupt(corruption.getMessage());
        } catch (Exception failure) {
            throw unavailable(null, "advance ZK Artifact reference backfill", failure);
        }
    }

    @Override
    public RunPurgeProgress purge(RetentionToken token, int maxDeletes) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (maxDeletes < 1 || maxDeletes > 1000) {
            throw new IllegalArgumentException("maxDeletes must be in [1,1000]");
        }
        Throwable lastRace = null;
        long observedRootCzxid = -1L;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        token.runId(), null, observedRootCzxid);
                if (authority.rootCzxid < 0L) {
                    return classifyPurgedIncarnation(
                            token.runId(), -1L, null);
                }
                if (observedRootCzxid < 0L) {
                    observedRootCzxid = authority.rootCzxid;
                }
                observeEvidenceRead(EvidenceReadPhase.PURGE_AFTER_LAYOUT_LOOKUP);
                AuthorityObservation confirmed = observeCoreAuthority(
                        token.runId(), null, observedRootCzxid);
                if (!sameAuthorityView(authority, confirmed)) {
                    lastRace = new CatalogRaceException();
                    continue;
                }
                authority = confirmed;
                Layout layout = authority.layout;
                SnapshotVersion snapshot = authority.snapshot;
                LifecycleEntry lifecycle = authority.lifecycle;
                if (!RunRetentionAuthorityKernel.current(lifecycle.ref, token)) {
                    return RunPurgeProgress.staleOwner();
                }
                if (!snapshot.snapshot.status().isTerminal()
                        || snapshot.snapshot.revision() != token.terminalRevision()) {
                    return RunPurgeProgress.corrupt();
                }
                verifyCatalogStable(layout, snapshot);
                PurgeCursorVersion cursor = readPurgeCursor(
                        layout, snapshot.snapshot.revision());
                if (cursor.cursor.nextSequence <= snapshot.snapshot.revision()) {
                    return deletePurgeBatch(
                            authority, cursor, maxDeletes);
                }
                return finalizePurge(authority, cursor);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NodeExistsException
                     | KeeperException.NoNodeException
                     | CatalogRaceException raced) {
                lastRace = raced;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                throw unavailable(null, "reconcile unknown ZK purge outcome", unknown);
            } catch (RunIncarnationChangedException changed) {
                return classifyPurgedIncarnation(
                        token.runId(), observedRootCzxid, changed);
            } catch (IllegalArgumentException | IllegalStateException corruption) {
                return RunPurgeProgress.corrupt();
            } catch (Exception failure) {
                throw unavailable(null, "purge ZK run history", failure);
            }
        }
        throw unavailable(null, "ZK run purge did not converge", lastRace);
    }

    private RunPurgeProgress classifyPurgedIncarnation(
            String runId, long expectedRootCzxid, Throwable unresolved) {
        try {
            observeEvidenceRead(
                    EvidenceReadPhase.FINAL_ABSENCE_BEFORE_CONFIRMATION);
            AuthorityObservation current = observeCoreAuthority(
                    runId, null, -1L);
            if (current.rootCzxid < 0L) {
                return RunPurgeProgress.purged();
            }
            if (expectedRootCzxid < 0L
                    || current.rootCzxid != expectedRootCzxid) {
                return RunPurgeProgress.staleOwner();
            }
            throw unavailable(null,
                    "reconcile unresolved ZK purge outcome", unresolved);
        } catch (RunStateStoreUnavailableException unavailable) {
            throw unavailable;
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            return RunPurgeProgress.corrupt();
        } catch (Exception failure) {
            throw unavailable(null,
                    "confirm final ZK purge authority", failure);
        }
    }

    private RunPurgeProgress deletePurgeBatch(
            AuthorityObservation authority, PurgeCursorVersion current,
            int maxDeletes) throws Exception {
        Layout layout = authority.layout;
        SnapshotVersion snapshot = authority.snapshot;
        LifecycleEntry lifecycle = authority.lifecycle;
        PurgeCursor next = current.cursor;
        List<CuratorOp> operations = new ArrayList<>();
        operations.add(client.transactionOp().check().withVersion(lifecycle.version)
                .forPath(lifecycle.path));
        operations.add(client.transactionOp().check().withVersion(snapshot.version)
                .forPath(layout.snapshot));
        int deleted = 0;
        boolean receiptDeleteQueued = false;
        while (deleted < maxDeletes
                && next.nextSequence <= snapshot.snapshot.revision()) {
            String eventPath = eventPath(layout, next.nextSequence);
            Stat eventStat = new Stat();
            byte[] eventBytes = client.getData().storingStatIn(eventStat)
                    .forPath(eventPath);
            ExecutionEvent event = RunStateCodec.decodeEvent(eventBytes);
            if (!layout.runId.equals(event.runId())
                    || event.sequence() != next.nextSequence) {
                throw corruption(layout.runId, "purge event identity mismatch");
            }
            String receiptPath = receiptPath(layout, event.commandId());
            if (next.receiptPhase) {
                Stat receiptStat = new Stat();
                byte[] receipt = client.getData().storingStatIn(receiptStat)
                        .forPath(receiptPath);
                if (!MessageDigest.isEqual(eventBytes, receipt)) {
                    throw corruption(layout.runId,
                            "purge command receipt/event mismatch");
                }
                operations.add(client.transactionOp().delete()
                        .withVersion(receiptStat.getVersion()).forPath(receiptPath));
                next = new PurgeCursor(next.nextSequence, false);
                receiptDeleteQueued = true;
            } else {
                if (!receiptDeleteQueued
                        && client.checkExists().forPath(receiptPath) != null) {
                    throw corruption(layout.runId,
                            "receipt remains after purge cursor advanced");
                }
                operations.add(client.transactionOp().delete()
                        .withVersion(eventStat.getVersion()).forPath(eventPath));
                next = new PurgeCursor(next.nextSequence + 1L, true);
                receiptDeleteQueued = false;
            }
            deleted++;
        }
        byte[] intended = encodePurgeCursor(next);
        if (current.version < 0) {
            operations.add(client.transactionOp().create()
                    .forPath(layout.retentionPurge, intended));
        } else {
            operations.add(client.transactionOp().setData()
                    .withVersion(current.version)
                    .forPath(layout.retentionPurge, intended));
        }
        appendGuardCheck(authority, operations);
        int requestPayloadBytes = serializedPurgeRequestBytes(operations);
        try {
            observeGuardedMutation(GuardedMutationKind.PURGE_BATCH, -1,
                    GuardedMutationMoment.BEFORE_COMMIT);
            client.transaction().forOperations(operations);
            observeTransactionWrite(
                    GuardedMutationKind.PURGE_BATCH, -1, operations);
            observeGuardedMutation(GuardedMutationKind.PURGE_BATCH, -1,
                    GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
            recordPurgeWrite(operations.size(), requestPayloadBytes);
            afterPurgeCommit.run();
            return RunPurgeProgress.inProgress(deleted);
        } catch (KeeperException.ConnectionLossException
                 | KeeperException.OperationTimeoutException unknown) {
            if (!confirmMutationAuthority(
                    layout.runId, authority.rootCzxid)) {
                return classifyPurgedIncarnation(
                        layout.runId, authority.rootCzxid, unknown);
            }
            AuthorityObservation confirmed = observeCoreAuthority(
                    layout.runId, null, authority.rootCzxid);
            PurgeCursorVersion reconciled = readPurgeCursor(
                    layout, snapshot.snapshot.revision());
            AuthorityObservation finalAuthority = observeCoreAuthority(
                    layout.runId, null, authority.rootCzxid);
            if (!sameAuthorityView(confirmed, finalAuthority)) {
                throw unknown;
            }
            if (next.equals(reconciled.cursor)) {
                return RunPurgeProgress.inProgress(deleted);
            }
            throw unknown;
        }
    }

    private RunPurgeProgress finalizePurge(
            AuthorityObservation authority, PurgeCursorVersion cursor)
            throws Exception {
        Layout layout = authority.layout;
        List<CuratorOp> operations;
        try {
            operations = finalPurgeOperations(authority, cursor);
        } catch (IllegalArgumentException
                 | IllegalStateException candidateCorruption) {
            return confirmFinalPurgeCorruption(authority);
        }
        int requestPayloadBytes = serializedPurgeRequestBytes(operations);
        boolean commitAttempted = false;
        try {
            observeGuardedMutation(GuardedMutationKind.PURGE_FINAL, -1,
                    GuardedMutationMoment.BEFORE_COMMIT);
            commitAttempted = true;
            client.transaction().forOperations(operations);
            observeTransactionWrite(
                    GuardedMutationKind.PURGE_FINAL, -1, operations);
            observeGuardedMutation(GuardedMutationKind.PURGE_FINAL, -1,
                    GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
            recordPurgeWrite(operations.size(), requestPayloadBytes);
            afterPurgeCommit.run();
            return RunPurgeProgress.purged();
        } catch (KeeperException.ConnectionLossException
                 | KeeperException.OperationTimeoutException unknown) {
            if (!commitAttempted) throw unknown;
            return classifyPurgedIncarnation(
                    layout.runId, authority.rootCzxid, unknown);
        }
    }

    private List<CuratorOp> finalPurgeOperations(
            AuthorityObservation authority, PurgeCursorVersion cursor)
            throws Exception {
        Layout layout = authority.layout;
        SnapshotVersion snapshot = authority.snapshot;
        LifecycleEntry lifecycle = authority.lifecycle;
        observeEvidenceRead(
                EvidenceReadPhase.PURGE_FINAL_BEFORE_GUARD_MUTATION);
        Stat events = requireEmpty(layout.events, "event parent");
        Stat commands = requireEmpty(layout.commands, "command parent");
        if (readCatalogEntry(layout.runId) != null) {
            throw corruption(layout.runId,
                    "recoverable catalog remains before final purge");
        }
        ReferenceDeletion reference = prepareReferenceDeletion(
                snapshot.snapshot);
        List<CuratorOp> operations = new ArrayList<>();
        reference.appendTo(client, operations);
        appendFinalAssignmentDeletes(layout, operations);
        OwnerVersion owner = readOwner(layout);
        if (owner != null) {
            operations.add(client.transactionOp().delete()
                    .withVersion(owner.version)
                    .forPath(layout.recoveryOwner));
        }
        Stat abort = client.checkExists().forPath(layout.sinkAbort);
        if (abort != null) {
            operations.add(client.transactionOp().delete()
                    .withVersion(abort.getVersion())
                    .forPath(layout.sinkAbort));
        }
        Stat identity = new Stat();
        byte[] identityBytes = client.getData().storingStatIn(identity)
                .forPath(layout.identity);
        if (!MessageDigest.isEqual(identityBytes,
                layout.runId.getBytes(StandardCharsets.UTF_8))) {
            throw corruption(layout.runId,
                    "run identity mismatch before final purge");
        }
        Stat run = client.checkExists().forPath(layout.run);
        if (run == null || !validFinalRunChildren(layout)) {
            throw corruption(layout.runId,
                    "invalid run layout before final purge");
        }
        LifecycleShard shard = lifecycleShardForRun(layout.runId);
        LifecycleGuardVersion guard = authority.guard;
        if (!guard.present || guard.generation == Long.MAX_VALUE) {
            throw corruption(layout.runId,
                    "invalid lifecycle guard before final purge");
        }
        operations.add(client.transactionOp().delete()
                .withVersion(identity.getVersion()).forPath(layout.identity));
        operations.add(client.transactionOp().delete()
                .withVersion(snapshot.version).forPath(layout.snapshot));
        operations.add(client.transactionOp().delete()
                .withVersion(events.getVersion()).forPath(layout.events));
        operations.add(client.transactionOp().delete()
                .withVersion(commands.getVersion()).forPath(layout.commands));
        operations.add(client.transactionOp().delete()
                .withVersion(cursor.version).forPath(layout.retentionPurge));
        operations.add(client.transactionOp().delete()
                .withVersion(run.getVersion()).forPath(layout.run));
        operations.add(client.transactionOp().setData()
                .withVersion(guard.version)
                .forPath(shard.guard, longBytes(guard.generation + 1L)));
        operations.add(client.transactionOp().delete()
                .withVersion(lifecycle.version).forPath(lifecycle.path));
        return operations;
    }

    private RunPurgeProgress confirmFinalPurgeCorruption(
            AuthorityObservation candidateAuthority) throws Exception {
        try {
            observeEvidenceRead(
                    EvidenceReadPhase.FINAL_ABSENCE_BEFORE_CONFIRMATION);
            AuthorityObservation confirmed = observeCoreAuthority(
                    candidateAuthority.layout.runId, null, -1L);
            if (confirmed.rootCzxid < 0L) {
                return RunPurgeProgress.purged();
            }
            if (confirmed.rootCzxid != candidateAuthority.rootCzxid) {
                return RunPurgeProgress.staleOwner();
            }
            if (!sameAuthorityView(candidateAuthority, confirmed)) {
                throw new KeeperException.BadVersionException();
            }
            return RunPurgeProgress.corrupt();
        } catch (KeeperException.ConnectionLossException
                 | KeeperException.OperationTimeoutException
                         confirmationFailure) {
            throw unavailable(null,
                    "confirm ZK final purge corruption authority",
                    confirmationFailure);
        }
    }

    private Stat requireEmpty(String path, String kind) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        if (stat == null || stat.getNumChildren() != 0) {
            throw corruption(path, kind + " is missing or non-empty");
        }
        return stat;
    }

    private void appendFinalAssignmentDeletes(
            Layout layout, List<CuratorOp> operations) throws Exception {
        Stat stage = client.checkExists().forPath(layout.stageAssignment);
        if (stage == null) return;
        boolean retained = client.checkExists().forPath(layout.assignmentRoot) != null
                || client.checkExists().forPath(layout.assignmentStaging) != null;
        if (retained) appendAssignmentCleanup(layout, operations);
        else requireNoOrphanAssignmentShards(layout);
        String[] parents = new String[] {
                layout.manifestMemberShards, layout.terminalShards,
                layout.acceptanceShards, layout.assignmentShards
        };
        for (String parent : parents) {
            Stat stat = client.checkExists().forPath(parent);
            if (stat == null || (!retained && stat.getNumChildren() != 0)) {
                throw corruption(layout.runId,
                        "assignment shard parent is missing or inconsistent");
            }
            operations.add(client.transactionOp().delete().withVersion(stat.getVersion())
                    .forPath(parent));
        }
        Stat intent = client.checkExists().forPath(layout.manifestIntent);
        if (intent != null && !retained) {
            if (intent.getDataLength() != 0) {
                throw corruption(layout.runId,
                        "terminal run retains manifest intent");
            }
            operations.add(client.transactionOp().delete().withVersion(intent.getVersion())
                    .forPath(layout.manifestIntent));
        }
        int expectedChildren = parents.length + (intent == null ? 0 : 1)
                + (retained ? 1 : 0);
        if (stage.getNumChildren() != expectedChildren) {
            throw corruption(layout.runId,
                    "stage assignment contains unknown final children");
        }
        operations.add(client.transactionOp().delete().withVersion(stage.getVersion())
                .forPath(layout.stageAssignment));
    }

    private boolean validFinalRunChildren(Layout layout) throws Exception {
        Stat stat = client.checkExists().forPath(layout.run);
        if (stat == null || stat.getNumChildren() > 8) return false;
        List<String> children = client.getChildren().forPath(layout.run);
        for (String child : children) {
            if (!child.equals("identity") && !child.equals("snapshot")
                    && !child.equals("events") && !child.equals("commands")
                    && !child.equals("retention-purge")
                    && !child.equals("recovery-owner")
                    && !child.equals("stage-assignment")
                    && !child.equals("sink-abort")) {
                return false;
            }
        }
        return children.contains("identity") && children.contains("snapshot")
                && children.contains("events") && children.contains("commands")
                && children.contains("retention-purge");
    }

    private PurgeCursorVersion readPurgeCursor(Layout layout, long revision)
            throws Exception {
        try {
            Stat stat = new Stat();
            PurgeCursor cursor = decodePurgeCursor(
                    client.getData().storingStatIn(stat)
                            .forPath(layout.retentionPurge));
            if (cursor.nextSequence < 1L
                    || cursor.nextSequence > revision + 1L
                    || (!cursor.receiptPhase && cursor.nextSequence > revision)) {
                throw corruption(layout.runId, "purge cursor is out of bounds");
            }
            return new PurgeCursorVersion(cursor, stat.getVersion());
        } catch (KeeperException.NoNodeException absent) {
            return new PurgeCursorVersion(new PurgeCursor(1L, true), -1);
        }
    }

    private static byte[] encodePurgeCursor(PurgeCursor cursor) {
        ByteBuffer buffer = ByteBuffer.allocate(18);
        buffer.putInt(0x52575250).put((byte) 1)
                .putLong(cursor.nextSequence)
                .put((byte) (cursor.receiptPhase ? 1 : 0));
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, 14);
        buffer.putInt((int) crc.getValue());
        return buffer.array();
    }

    private static PurgeCursor decodePurgeCursor(byte[] encoded) {
        if (encoded == null || encoded.length != 18) {
            throw new IllegalArgumentException("invalid purge cursor length");
        }
        CRC32 crc = new CRC32();
        crc.update(encoded, 0, 14);
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        if (buffer.getInt() != 0x52575250 || buffer.get() != 1) {
            throw new IllegalArgumentException("invalid purge cursor header");
        }
        long sequence = buffer.getLong();
        byte phase = buffer.get();
        if ((int) crc.getValue() != buffer.getInt()
                || (phase != 0 && phase != 1)) {
            throw new IllegalArgumentException("invalid purge cursor checksum or phase");
        }
        return new PurgeCursor(sequence, phase == 1);
    }

    private void recordPurgeWrite(int operationCount, int requestPayloadBytes) {
        if (purgeWriteObserver != null) {
            purgeWriteObserver.onWrite(operationCount, requestPayloadBytes);
        }
    }

    private int serializedPurgeRequestBytes(List<CuratorOp> operations)
            throws Exception {
        if (purgeWriteObserver == null) return 0;
        return serializedMultiRequestBytes(operations);
    }

    private static int serializedMultiRequestBytes(List<CuratorOp> operations)
            throws Exception {
        List<Op> zkOperations = new ArrayList<>(operations.size());
        for (CuratorOp operation : operations) zkOperations.add(operation.get());
        MultiOperationRecord request = new MultiOperationRecord(zkOperations);
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        request.serialize(BinaryOutputArchive.getArchive(encoded), "request");
        return encoded.size();
    }

    @Override
    public RecoverableRunPage scan(String cursor, int limit) {
        RunRecoveryCatalogKernel.validateLimit(limit);
        RunRecoveryCatalogKernel.Cursor position = RunRecoveryCatalogKernel.decodeCursor(cursor);
        CatalogShard shard = catalogShard(position.shard());
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                ShardView view = readStableShard(shard, false);
                if (view.guardVersion < 0) {
                    return RunRecoveryCatalogKernel.page(cursor, limit, Collections.emptyList());
                }
                List<RecoverableRunRef> refs = new ArrayList<>(view.children.size());
                for (String child : view.children) {
                    validateCatalogChild(shard.number, child);
                    byte[] encoded = client.getData().forPath(shard.entries + "/" + child);
                    RecoverableRunRef ref = RunRecoveryCatalogKernel.decodeRef(encoded);
                    if (!child.equals(RunRecoveryCatalogKernel.digestHex(ref.runId()))) {
                        throw corruption(ref.runId(), "catalog entry identity/hash mismatch");
                    }
                    refs.add(ref);
                }
                Stat after = new Stat();
                byte[] guard = client.getData().storingStatIn(after).forPath(shard.guard);
                verifyGuard(guard, shard.number);
                if (after.getVersion() != view.guardVersion) continue;
                return RunRecoveryCatalogKernel.page(cursor, limit, refs);
            } catch (KeeperException.NoNodeException | KeeperException.BadVersionException raced) {
                lastRace = raced;
            } catch (IllegalArgumentException | IllegalStateException corruption) {
                throw corruption;
            } catch (Exception failure) {
                throw unavailable(null, "scan ZK recoverable run catalog", failure);
            }
        }
        throw unavailable(null, "ZK recoverable run scan did not converge", lastRace);
    }

    @Override
    public RecoveryToken ownership(String runId) {
        Layout layout = existingLayout(runId);
        if (layout == null) return null;
        try {
            OwnerVersion owner = readOwner(layout);
            return owner == null ? null : owner.token;
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            throw corruption;
        } catch (Exception failure) {
            throw unavailable(null, "read ZK recovery ownership", failure);
        }
    }

    @Override
    public RecoveryClaim claim(String runId, long expectedRevision,
                               long expectedOwnershipEpoch, RecoveryOwner owner) {
        return claim(runId, expectedRevision, expectedOwnershipEpoch, owner,
                TransactionAugment.NONE);
    }

    RecoveryClaim claimAugmented(
            String runId, long expectedRevision,
            long expectedOwnershipEpoch, RecoveryOwner owner,
            TransactionAugment augment) {
        if (augment == null) {
            throw new IllegalArgumentException("transaction augment is required");
        }
        return claim(runId, expectedRevision, expectedOwnershipEpoch, owner,
                augment);
    }

    private RecoveryClaim claim(
            String runId, long expectedRevision,
            long expectedOwnershipEpoch, RecoveryOwner owner,
            TransactionAugment augment) {
        requireIdentity(runId, "runId");
        if (expectedRevision <= 0L) {
            throw new IllegalArgumentException("expectedRevision must be > 0");
        }
        if (expectedOwnershipEpoch < 0L) {
            throw new IllegalArgumentException("expectedOwnershipEpoch must be >= 0");
        }
        if (owner == null) throw new IllegalArgumentException("owner is required");
        long observedRootCzxid = -1L;
        try {
            AuthorityObservation authority = observeCoreAuthority(
                    runId, null, observedRootCzxid);
            if (authority.rootCzxid < 0L || authority.snapshot == null) {
                return RecoveryClaim.staleRun();
            }
            observedRootCzxid = authority.rootCzxid;
            Layout layout = authority.layout;
            SnapshotVersion snapshot = authority.snapshot;
            if (snapshot.snapshot.status().isTerminal()
                    || snapshot.snapshot.revision() != expectedRevision) {
                return RecoveryClaim.staleRun();
            }
            CatalogEntry catalog = readCatalogEntry(runId);
            AssignmentView assignments = readAssignmentView(layout);
            StagingVersion staging = readStaging(layout);
            observeEvidenceRead(EvidenceReadPhase
                    .RECOVERY_CLAIM_BEFORE_OWNER_CLASSIFICATION);
            OwnerVersion current = readOwner(layout);
            observeEvidenceRead(EvidenceReadPhase
                    .RECOVERY_CLAIM_BEFORE_OWNER_CONFIRMATION);
            AuthorityObservation confirmed = observeCoreAuthority(
                    runId, null, observedRootCzxid);
            if (!sameAuthorityView(authority, confirmed)) {
                return confirmed.snapshot == null
                        || confirmed.snapshot.snapshot.status().isTerminal()
                        || confirmed.snapshot.snapshot.revision()
                                != expectedRevision
                        ? RecoveryClaim.staleRun()
                        : RecoveryClaim.lostRace();
            }
            authority = confirmed;
            layout = authority.layout;
            snapshot = authority.snapshot;
            verifyObservedCatalogStable(layout, snapshot, catalog);
            if (assignments != null && staging != null) {
                throw corruption(runId, "published assignment retains staging marker");
            }
            if (current != null && current.token.owner().equals(owner)) {
                checkClaimView(authority, catalog, current);
                return RecoveryClaim.replayed(current.token);
            }

            final RecoveryToken next;
            if (current == null) {
                if (expectedOwnershipEpoch != 0L) return RecoveryClaim.lostRace();
                next = RecoveryToken.create(runId, owner, 1L, expectedRevision);
            } else {
                if (current.token.ownershipEpoch() != expectedOwnershipEpoch) {
                    return RecoveryClaim.lostRace();
                }
                if (current.token.ownershipEpoch() == Long.MAX_VALUE) {
                    throw new IllegalStateException("recovery ownership epoch exhausted");
                }
                next = RecoveryToken.create(runId, owner,
                        current.token.ownershipEpoch() + 1L, expectedRevision);
            }

            List<CuratorOp> operations = claimFenceMutations(authority, catalog);
            appendClaimAssignmentFence(layout, assignments, staging, operations);
            if (current == null) {
                operations.add(client.transactionOp().create()
                        .forPath(layout.recoveryOwner,
                                RecoveryOwnershipCodec.encodeToken(next)));
            } else {
                operations.add(client.transactionOp().setData().withVersion(current.version)
                        .forPath(layout.recoveryOwner,
                                RecoveryOwnershipCodec.encodeToken(next)));
            }
            augment.append(client, operations);
            try {
                observeGuardedMutation(GuardedMutationKind.RECOVERY_CLAIM, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.RECOVERY_CLAIM, -1, operations);
                observeGuardedMutation(GuardedMutationKind.RECOVERY_CLAIM, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                afterOwnershipCommit.run();
                return RecoveryClaim.acquired(next);
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return RecoveryClaim.staleRun();
                    }
                    AuthorityObservation reconciledAuthority = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    observeEvidenceRead(EvidenceReadPhase
                            .RECOVERY_CLAIM_RECONCILE_AFTER_AUTHORITY);
                    CatalogEntry confirmedCatalog = readCatalogEntry(runId);
                    OwnerVersion reconciled = readOwner(
                            reconciledAuthority.layout);
                    observeEvidenceRead(EvidenceReadPhase
                            .RECOVERY_CLAIM_RECONCILE_BEFORE_CONFIRMATION);
                    AuthorityObservation finalAuthority = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(
                            reconciledAuthority, finalAuthority)) {
                        return finalAuthority.snapshot == null
                                || finalAuthority.snapshot.snapshot.status()
                                        .isTerminal()
                                || finalAuthority.snapshot.snapshot.revision()
                                        != expectedRevision
                                ? RecoveryClaim.staleRun()
                                : RecoveryClaim.lostRace();
                    }
                    verifyObservedCatalogStable(
                            finalAuthority.layout,
                            finalAuthority.snapshot,
                            confirmedCatalog);
                    if (reconciled != null && next.equals(reconciled.token)) {
                        checkClaimView(
                                finalAuthority,
                                confirmedCatalog, reconciled);
                        return RecoveryClaim.replayed(reconciled.token);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK recovery ownership outcome",
                            confirmationFailure);
                } catch (RunIncarnationChangedException changed) {
                    return RecoveryClaim.staleRun();
                }
                throw unavailable(null,
                        "reconcile unknown ZK recovery ownership outcome", unknown);
            }
        } catch (KeeperException.BadVersionException
                 | KeeperException.NodeExistsException
                 | KeeperException.NoNodeException
                 | CatalogRaceException raced) {
            try {
                if (observedRootCzxid >= 0L
                        && !confirmMutationAuthority(
                                runId, observedRootCzxid)) {
                    return RecoveryClaim.staleRun();
                }
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException
                             confirmationFailure) {
                throw unavailable(null,
                        "confirm raced ZK recovery ownership outcome",
                        confirmationFailure);
            } catch (IllegalArgumentException
                     | IllegalStateException corruption) {
                throw corruption;
            } catch (Exception confirmationRace) {
                throw unavailable(null,
                        "confirm raced ZK recovery ownership outcome",
                        confirmationRace);
            }
            return RecoveryClaim.lostRace();
        } catch (RunIncarnationChangedException changed) {
            return RecoveryClaim.staleRun();
        } catch (IllegalArgumentException | IllegalStateException contractFailure) {
            throw contractFailure;
        } catch (RunStateStoreUnavailableException unavailable) {
            throw unavailable;
        } catch (Exception failure) {
            throw unavailable(null, "claim ZK recovery ownership", failure);
        }
    }

    @Override
    public boolean current(RecoveryToken token) {
        if (token == null) throw new IllegalArgumentException("token is required");
        RecoveryToken stored = ownership(token.runId());
        return token.equals(stored);
    }

    @Override
    public StageManifestPublication publish(StageAssignmentManifest manifest) {
        return publish(manifest, null, false, TransactionAugment.NONE);
    }

    @Override
    public StageManifestPublication publishRecovered(
            RecoveryToken token, StageAssignmentManifest manifest) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        if (!token.runId().equals(manifest.fence().runId())) {
            throw new IllegalArgumentException("recovery token and manifest runId mismatch");
        }
        return publish(manifest, token, true, TransactionAugment.NONE);
    }

    StageManifestPublication publishAugmented(
            StageAssignmentManifest manifest, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (augment == null) {
            throw new IllegalArgumentException("transaction augment is required");
        }
        if (recovered && recoveryToken == null) {
            throw new IllegalArgumentException("recovery token is required");
        }
        if (recovered && manifest != null
                && !recoveryToken.runId().equals(manifest.fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and manifest runId mismatch");
        }
        return publish(manifest, recoveryToken, recovered, augment);
    }

    private StageManifestPublication publish(
            StageAssignmentManifest manifest, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        String runId = manifest.fence().runId();
        long observedRootCzxid;
        try {
            AuthorityObservation initial = observeCoreAuthority(runId, null, -1L);
            if (initial.rootCzxid < 0L || initial.snapshot == null) {
                return StageManifestPublication.rejected(
                        StageManifestPublication.Code.STALE_RUN);
            }
            observedRootCzxid = initial.rootCzxid;
            observeEvidenceRead(EvidenceReadPhase
                    .PUBLISH_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER);
        } catch (RunIncarnationChangedException changed) {
            return StageManifestPublication.rejected(
                    StageManifestPublication.Code.STALE_RUN);
        } catch (IllegalArgumentException | IllegalStateException contractFailure) {
            throw contractFailure;
        } catch (Exception failure) {
            throw unavailable(null, "observe ZK assignment publication", failure);
        }
        Throwable lastRace = null;
        int unresolvedAttempts = 0;
        while (unresolvedAttempts < MAX_RETRIES) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                if (authority.snapshot == null) {
                    return StageManifestPublication.rejected(
                            StageManifestPublication.Code.STALE_RUN);
                }
                Layout layout = authority.layout;
                ensureAssignmentLayout(authority);
                authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                SnapshotVersion run = authority.snapshot;
                if (run.snapshot.status().isTerminal()) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) {
                        unresolvedAttempts++;
                        continue;
                    }
                    return StageManifestPublication.rejected(
                            StageManifestPublication.Code.STALE_RUN);
                }
                CatalogEntry catalog = readCatalogEntry(layout.runId);
                verifyExistingCatalogOrRace(layout, run, catalog);
                if (!matchesPublishableAttempt(run.snapshot, manifest)) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) {
                        unresolvedAttempts++;
                        continue;
                    }
                    return StageManifestPublication.rejected(StageManifestPublication.Code.STALE_RUN);
                }
                OwnerVersion owner = readOwner(layout);
                if ((!recovered && owner != null)
                        || (recovered && (owner == null
                        || !recoveryToken.equals(owner.token)))) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) {
                        unresolvedAttempts++;
                        continue;
                    }
                    return StageManifestPublication.rejected(
                            StageManifestPublication.Code.RECOVERY_OWNED);
                }
                AssignmentView published = readAssignmentView(layout);
                if (published != null) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) {
                        unresolvedAttempts++;
                        continue;
                    }
                    return sameManifest(published.snapshot.manifest(), manifest)
                            ? StageManifestPublication.replayed(published.snapshot)
                            : StageManifestPublication.rejected(
                            StageManifestPublication.Code.CONFLICT);
                }

                byte[] intendedRoot = StageAssignmentManifestCodec.encodeRoot(manifest, 1L);
                StagingVersion staging = readStaging(layout);
                if (staging == null) {
                    byte[] snapshotBytes = RunStateCodec.encodeSnapshot(run.snapshot);
                    byte[] catalogBytes = RunRecoveryCatalogKernel.encodeRef(catalog.ref);
                    List<CuratorOp> createStaging = new ArrayList<>();
                    createStaging.add(client.transactionOp().setData().withVersion(run.version)
                            .forPath(layout.snapshot, snapshotBytes));
                    appendLifecycleAndGuardChecks(authority, createStaging);
                    createStaging.add(client.transactionOp().setData().withVersion(catalog.version)
                            .forPath(catalog.path, catalogBytes));
                    if (recovered) {
                        createStaging.add(client.transactionOp().check()
                                .withVersion(owner.version).forPath(layout.recoveryOwner));
                    }
                    createStaging.add(client.transactionOp().create()
                            .forPath(layout.assignmentStaging, intendedRoot));
                    augment.append(client, createStaging);
                    observeGuardedMutation(
                            GuardedMutationKind.PUBLISH_STAGING_MARKER, -1,
                            GuardedMutationMoment.BEFORE_COMMIT);
                    client.transaction().forOperations(createStaging);
                    observeTransactionWrite(
                            GuardedMutationKind.PUBLISH_STAGING_MARKER,
                            -1, createStaging);
                    observeGuardedMutation(
                            GuardedMutationKind.PUBLISH_STAGING_MARKER, -1,
                            GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                    recordAssignmentWrite(createStaging);
                    continue;
                }
                if (!MessageDigest.isEqual(staging.data, intendedRoot)) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) {
                        unresolvedAttempts++;
                        continue;
                    }
                    return StageManifestPublication.rejected(StageManifestPublication.Code.CONFLICT);
                }
                writeStagingShards(
                        runId, observedRootCzxid, manifest, staging);

                observeEvidenceRead(EvidenceReadPhase
                        .PUBLISH_AFTER_STAGING_BEFORE_FINAL_AUTHORITY);
                authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                layout = authority.layout;
                run = authority.snapshot;
                if (run == null) {
                    return StageManifestPublication.rejected(
                            StageManifestPublication.Code.STALE_RUN);
                }
                catalog = readCatalogEntry(layout.runId);
                verifyExistingCatalogOrRace(layout, run, catalog);
                if (!matchesPublishableAttempt(run.snapshot, manifest)) {
                    confirmResultAuthority(authority);
                    return StageManifestPublication.rejected(StageManifestPublication.Code.STALE_RUN);
                }
                owner = readOwner(layout);
                if ((!recovered && owner != null)
                        || (recovered && (owner == null
                        || !recoveryToken.equals(owner.token)))) {
                    confirmResultAuthority(authority, owner);
                    return StageManifestPublication.rejected(
                            StageManifestPublication.Code.RECOVERY_OWNED);
                }
                staging = readStaging(layout);
                if (staging == null || !MessageDigest.isEqual(staging.data, intendedRoot)) {
                    unresolvedAttempts++;
                    continue;
                }
                byte[] snapshotBytes = RunStateCodec.encodeSnapshot(run.snapshot);
                byte[] catalogBytes = RunRecoveryCatalogKernel.encodeRef(catalog.ref);
                List<CuratorOp> publishRoot = new ArrayList<>();
                publishRoot.add(client.transactionOp().setData().withVersion(run.version)
                        .forPath(layout.snapshot, snapshotBytes));
                appendLifecycleAndGuardChecks(authority, publishRoot);
                publishRoot.add(client.transactionOp().setData().withVersion(catalog.version)
                        .forPath(catalog.path, catalogBytes));
                if (recovered) {
                    publishRoot.add(client.transactionOp().check()
                            .withVersion(owner.version).forPath(layout.recoveryOwner));
                }
                publishRoot.add(client.transactionOp().create()
                        .forPath(layout.assignmentRoot, intendedRoot));
                publishRoot.add(client.transactionOp().delete().withVersion(staging.version)
                        .forPath(layout.assignmentStaging));
                augment.append(client, publishRoot);
                observeGuardedMutation(GuardedMutationKind.PUBLISH_ROOT, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(publishRoot);
                observeTransactionWrite(
                        GuardedMutationKind.PUBLISH_ROOT, -1, publishRoot);
                observeGuardedMutation(GuardedMutationKind.PUBLISH_ROOT, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                recordAssignmentWrite(publishRoot);
                afterAssignmentPublish.run();
                authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                layout = authority.layout;
                final AssignmentView committed;
                try {
                    committed = readAssignmentView(layout);
                } catch (IllegalArgumentException
                         | IllegalStateException candidateCorruption) {
                    throw confirmedPublishCorruption(
                            authority, candidateCorruption);
                }
                if (committed == null || !sameManifest(committed.snapshot.manifest(), manifest)) {
                    throw confirmedPublishCorruption(authority,
                            corruption(layout.runId,
                                    "published assignment root is not readable"));
                }
                confirmResultAuthority(authority, owner);
                return StageManifestPublication.published(committed.snapshot);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NodeExistsException
                     | KeeperException.NoNodeException
                     | CatalogRaceException raced) {
                lastRace = raced;
                unresolvedAttempts++;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return StageManifestPublication.rejected(
                                StageManifestPublication.Code.STALE_RUN);
                    }
                    AuthorityObservation authority = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    AssignmentView reconciled =
                            readAssignmentViewUnchecked(authority.layout);
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) {
                        unresolvedAttempts++;
                        continue;
                    }
                    if (reconciled != null && sameManifest(
                            reconciled.snapshot.manifest(), manifest)) {
                        return StageManifestPublication.replayed(
                                reconciled.snapshot);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK assignment publication outcome",
                            confirmationFailure);
                } catch (RunIncarnationChangedException changed) {
                    return StageManifestPublication.rejected(
                            StageManifestPublication.Code.STALE_RUN);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
                unresolvedAttempts++;
            } catch (RunIncarnationChangedException changed) {
                return StageManifestPublication.rejected(
                        StageManifestPublication.Code.STALE_RUN);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(null, "publish ZK stage assignment manifest", failure);
            }
        }
        throw unavailable(null, "ZK stage assignment publication did not converge", lastRace);
    }

    @Override
    public StageAssignmentSnapshot assignmentSnapshot(RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        long validatedRootCzxid = -1L;
        String stableMismatch = null;
        CoreMismatch stableMismatchKind = null;
        int stableMismatchCount = 0;
        try {
            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                try {
                    AuthorityObservation authority = observeAuthority(fence.runId());
                    CoreMismatch mismatch = coreMismatch(authority);
                    if (mismatch != null) {
                        if (mismatch == CoreMismatch.LAYOUT_ONLY
                                && validatedRootCzxid != authority.rootCzxid) {
                            return null;
                        }
                        observeEvidenceRead(
                                EvidenceReadPhase.CORE_MISMATCH_BEFORE_CONFIRMATION);
                        String key = authorityMismatchKey(authority, mismatch);
                        if (key.equals(stableMismatch)) stableMismatchCount++;
                        else { stableMismatch = key; stableMismatchCount = 1; }
                        stableMismatchKind = mismatch;
                        if (stableMismatchCount >= MAX_RETRIES) {
                            throw corruption(fence.runId(), mismatch.detail);
                        }
                        continue;
                    }
                    if (authority.rootCzxid < 0L) return null;
                    verifyExistingLifecycleOrRace(authority.layout,
                            authority.snapshot, authority.lifecycle);
                    validatedRootCzxid = authority.rootCzxid;
                    stableMismatch = null;
                    stableMismatchKind = null;
                    stableMismatchCount = 0;
                    observeEvidenceRead(
                            EvidenceReadPhase.CORE_AFTER_VALIDATED_PAIR);
                    observeEvidenceRead(
                            EvidenceReadPhase.EVIDENCE_AFTER_INITIAL_SNAPSHOT);
                    AssignmentView view = authority.snapshot.snapshot
                            .status().isTerminal()
                            ? null : readAssignmentView(authority.layout);
                    AuthorityObservation confirmed = observeAuthority(
                            fence.runId());
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return view != null
                            && view.snapshot.manifest().fence().equals(fence)
                            ? view.snapshot : null;
                } catch (KeeperException.NoNodeException
                         | KeeperException.BadVersionException
                         | CatalogRaceException raced) {
                    stableMismatch = null;
                    stableMismatchKind = null;
                    stableMismatchCount = 0;
                }
            }
            if (stableMismatchKind != null) {
                throw corruption(fence.runId(), stableMismatchKind.detail);
            }
            throw unavailable(null,
                    "read coherent ZK stage assignment snapshot", null);
        } catch (IllegalArgumentException malformed) {
            throw corruption(fence.runId(), "invalid assignment authority encoding");
        } catch (IllegalStateException corruption) {
            throw corruption;
        } catch (Exception failure) {
            throw unavailable(null, "read ZK stage assignment snapshot", failure);
        }
    }

    @Override
    public ManifestCommitJournalSnapshot manifestCommitJournal(RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        long validatedRootCzxid = -1L;
        String stableMismatch = null;
        CoreMismatch stableMismatchKind = null;
        int stableMismatchCount = 0;
        try {
            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                try {
                    AuthorityObservation authority = observeAuthority(fence.runId());
                    CoreMismatch mismatch = coreMismatch(authority);
                    if (mismatch != null) {
                        if (mismatch == CoreMismatch.LAYOUT_ONLY
                                && validatedRootCzxid != authority.rootCzxid) {
                            return null;
                        }
                        observeEvidenceRead(
                                EvidenceReadPhase.CORE_MISMATCH_BEFORE_CONFIRMATION);
                        String key = authorityMismatchKey(authority, mismatch);
                        if (key.equals(stableMismatch)) stableMismatchCount++;
                        else { stableMismatch = key; stableMismatchCount = 1; }
                        stableMismatchKind = mismatch;
                        if (stableMismatchCount >= MAX_RETRIES) {
                            throw corruption(fence.runId(), mismatch.detail);
                        }
                        continue;
                    }
                    if (authority.rootCzxid < 0L) return null;
                    verifyExistingLifecycleOrRace(authority.layout,
                            authority.snapshot, authority.lifecycle);
                    validatedRootCzxid = authority.rootCzxid;
                    stableMismatch = null;
                    stableMismatchKind = null;
                    stableMismatchCount = 0;
                    observeEvidenceRead(
                            EvidenceReadPhase.CORE_AFTER_VALIDATED_PAIR);
                    observeEvidenceRead(
                            EvidenceReadPhase.EVIDENCE_AFTER_INITIAL_SNAPSHOT);
                    ManifestJournalView view = authority.snapshot.snapshot
                            .status().isTerminal()
                            ? null : readManifestJournalView(authority.layout);
                    AuthorityObservation confirmed = observeAuthority(
                            fence.runId());
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return view != null && view.snapshot.assignments()
                            .manifest().fence().equals(fence)
                            ? view.snapshot : null;
                } catch (KeeperException.NoNodeException
                         | KeeperException.BadVersionException
                         | CatalogRaceException raced) {
                    stableMismatch = null;
                    stableMismatchKind = null;
                    stableMismatchCount = 0;
                }
            }
            if (stableMismatchKind != null) {
                throw corruption(fence.runId(), stableMismatchKind.detail);
            }
            throw unavailable(null,
                    "read coherent ZK manifest commit journal", null);
        } catch (IllegalArgumentException malformed) {
            throw corruption(fence.runId(), "invalid manifest journal encoding");
        } catch (IllegalStateException corruption) {
            throw corruption;
        } catch (Exception failure) {
            throw unavailable(null, "read ZK manifest commit journal", failure);
        }
    }

    @Override
    public QualityGateDecision qualityGateDecision(RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        Throwable lastRace = null;
        long observedRootCzxid = -1L;
        try {
            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                try {
                    AuthorityObservation authority = observeCoreAuthority(
                            fence.runId(), null, observedRootCzxid);
                    if (authority.rootCzxid < 0L
                            || authority.snapshot == null) return null;
                    if (observedRootCzxid < 0L) {
                        observedRootCzxid = authority.rootCzxid;
                    }
                    QualityJournalView view =
                            readQualityJournalView(authority.layout, true);
                    AuthorityObservation confirmed = observeCoreAuthority(
                            fence.runId(), null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    if (!sameAttemptIdentity(authority.snapshot.snapshot, fence)
                            || view == null || !view.root.fence().equals(fence)) {
                        return null;
                    }
                    return qualityCommitJournalSnapshot(view).decision();
                } catch (KeeperException.NoNodeException
                         | KeeperException.BadVersionException
                         | CatalogRaceException raced) {
                    lastRace = raced;
                }
            }
            throw unavailable(null,
                    "read coherent ZK quality decision", lastRace);
        } catch (RunIncarnationChangedException changed) {
            return null;
        } catch (IllegalArgumentException malformed) {
            throw corruption(fence.runId(), "invalid quality decision encoding");
        } catch (IllegalStateException corruption) {
            throw corruption;
        } catch (Exception failure) {
            throw unavailable(null, "read ZK quality decision", failure);
        }
    }

    @Override
    public QualityGateCommitJournalSnapshot qualityGateCommitJournal(
            RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        Throwable lastRace = null;
        long observedRootCzxid = -1L;
        try {
            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                try {
                    AuthorityObservation authority = observeCoreAuthority(
                            fence.runId(), null, observedRootCzxid);
                    if (authority.rootCzxid < 0L
                            || authority.snapshot == null) return null;
                    if (observedRootCzxid < 0L) {
                        observedRootCzxid = authority.rootCzxid;
                    }
                    QualityJournalView view =
                            readQualityJournalView(authority.layout, true);
                    AuthorityObservation confirmed = observeCoreAuthority(
                            fence.runId(), null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    if (!sameAttemptIdentity(authority.snapshot.snapshot, fence)
                            || view == null || !view.root.fence().equals(fence)) {
                        return null;
                    }
                    QualityGateDecision decision = view.slot.decision();
                    if (decision != null
                            && (!qualityDecisionMatchesRoot(view.root, decision)
                            || !qualityEvidenceMatches(view, decision))) {
                        throw corruption(fence.runId(),
                                "quality journal decision evidence mismatch");
                    }
                    ManifestCommitIntent intent = view.slot.intent();
                    if (intent != null && !qualityIntentMatches(view, intent)) {
                        throw corruption(fence.runId(),
                                "quality journal intent evidence mismatch");
                    }
                    return qualityCommitJournalSnapshot(view);
                } catch (KeeperException.NoNodeException
                         | KeeperException.BadVersionException
                         | CatalogRaceException raced) {
                    lastRace = raced;
                }
            }
            throw unavailable(null,
                    "read coherent ZK quality commit journal", lastRace);
        } catch (RunIncarnationChangedException changed) {
            return null;
        } catch (IllegalArgumentException malformed) {
            throw corruption(fence.runId(),
                    "invalid quality commit journal encoding");
        } catch (IllegalStateException corruption) {
            throw corruption;
        } catch (Exception failure) {
            throw unavailable(null, "read ZK quality commit journal", failure);
        }
    }

    @Override
    public QualityGateDecisionResult recordQualityGateDecision(
            QualityGateDecision decision) {
        return recordQualityGateDecision(decision, null, false);
    }

    @Override
    public QualityGateDecisionResult recordQualityGateDecisionRecovered(
            RecoveryToken recoveryToken, QualityGateDecision decision) {
        if (recoveryToken == null) {
            throw new IllegalArgumentException("token is required");
        }
        if (decision == null) {
            throw new IllegalArgumentException("decision is required");
        }
        if (!recoveryToken.runId().equals(decision.fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and quality decision runId mismatch");
        }
        return recordQualityGateDecision(decision, recoveryToken, true);
    }

    private QualityGateDecisionResult recordQualityGateDecision(
            QualityGateDecision requested, RecoveryToken recoveryToken,
            boolean recovered) {
        if (requested == null) {
            throw new IllegalArgumentException("decision is required");
        }
        String runId = requested.fence().runId();
        long observedRootCzxid;
        try {
            AuthorityObservation initial = observeCoreAuthority(runId, null, -1L);
            if (initial.rootCzxid < 0L || initial.snapshot == null) {
                return QualityGateDecisionResult.rejected(
                        QualityGateDecisionResult.Code.STALE_RUN);
            }
            observedRootCzxid = initial.rootCzxid;
            observeEvidenceRead(EvidenceReadPhase
                    .QUALITY_DECISION_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER);
        } catch (RunIncarnationChangedException changed) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.STALE_RUN);
        } catch (IllegalArgumentException | IllegalStateException contractFailure) {
            throw contractFailure;
        } catch (Exception failure) {
            throw unavailable(null, "observe ZK quality decision", failure);
        }
        if (!ensureAbortGuard(runId, observedRootCzxid,
                GuardedMutationKind.QUALITY_DECISION_ABORT_GUARD)) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.STALE_RUN);
        }
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                if (authority.snapshot == null
                        || authority.snapshot.snapshot.status().isTerminal()) {
                    return QualityGateDecisionResult.rejected(
                            QualityGateDecisionResult.Code.STALE_RUN);
                }
                Layout layout = authority.layout;
                SnapshotVersion run = authority.snapshot;
                CatalogEntry catalog = readCatalogEntry(layout.runId);
                verifyExistingCatalogOrRace(layout, run, catalog);
                QualityJournalView journal = readQualityJournalView(layout, true);
                AbortVersion abort = readAbortVersion(layout);
                if (abort == null) throw new KeeperException.NoNodeException();
                OwnerVersion owner = readOwner(layout);
                QualityGateDecisionResult decision = evaluateQualityDecision(
                        run.snapshot, owner, journal, abort, requested,
                        recoveryToken, recovered);
                if (decision.code()
                        != QualityGateDecisionResult.Code.RECORDED) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return decision;
                }
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check()
                        .withVersion(run.version).forPath(layout.snapshot));
                appendLifecycleAndGuardChecks(authority, operations);
                operations.add(client.transactionOp().check()
                        .withVersion(catalog.version).forPath(catalog.path));
                operations.add(client.transactionOp().check()
                        .withVersion(journal.rootVersion)
                        .forPath(layout.assignmentRoot));
                operations.add(client.transactionOp().check()
                        .withVersion(journal.intentVersion)
                        .forPath(layout.manifestIntent));
                operations.add(client.transactionOp().check()
                        .withVersion(abort.version).forPath(layout.sinkAbort));
                for (int i = 0; i < journal.memberShardIndices.length; i++) {
                    operations.add(client.transactionOp().check()
                            .withVersion(journal.memberVersions[i])
                            .forPath(manifestMemberShardPath(
                                    layout, journal.memberShardIndices[i])));
                }
                if (recovered) {
                    operations.add(client.transactionOp().check()
                            .withVersion(owner.version)
                            .forPath(layout.recoveryOwner));
                }
                operations.add(client.transactionOp().setData()
                        .withVersion(journal.intentVersion)
                        .forPath(layout.manifestIntent,
                                ManifestCommitJournalCodec
                                        .encodeQualityDecision(requested)));
                observeGuardedMutation(GuardedMutationKind.QUALITY_DECISION, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.QUALITY_DECISION, -1, operations);
                observeGuardedMutation(GuardedMutationKind.QUALITY_DECISION, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                recordAssignmentWrite(operations);
                if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                    return QualityGateDecisionResult.rejected(
                            QualityGateDecisionResult.Code.STALE_RUN);
                }
                return QualityGateDecisionResult.recorded(requested);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return QualityGateDecisionResult.rejected(
                                QualityGateDecisionResult.Code.STALE_RUN);
                    }
                    AuthorityObservation authority = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    QualityJournalView reconciled =
                            readQualityJournalView(authority.layout, true);
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    QualityGateDecision existing = reconciled == null ? null
                            : qualityCommitJournalSnapshot(reconciled).decision();
                    if (requested.equals(existing)) {
                        return QualityGateDecisionResult
                                .alreadyRecorded(existing);
                    }
                    if (existing != null) {
                        return QualityGateDecisionResult.rejected(
                                QualityGateDecisionResult.Code.DECISION_CONFLICT);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK quality decision outcome",
                            confirmationFailure);
                } catch (RunIncarnationChangedException changed) {
                    return QualityGateDecisionResult.rejected(
                            QualityGateDecisionResult.Code.STALE_RUN);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
            } catch (RunIncarnationChangedException changed) {
                return QualityGateDecisionResult.rejected(
                        QualityGateDecisionResult.Code.STALE_RUN);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(null, "record ZK quality decision", failure);
            }
        }
        throw unavailable(null, "ZK quality decision did not converge", lastRace);
    }

    @Override
    public StageAcceptanceDecision accept(StageAssignmentAcceptance request) {
        return accept(request, null, false, TransactionAugment.NONE);
    }

    @Override
    public StageAcceptanceDecision acceptRecovered(
            RecoveryToken recoveryToken, StageAssignmentAcceptance request) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (request == null) throw new IllegalArgumentException("acceptance is required");
        if (!recoveryToken.runId().equals(request.token().fence().runId())) {
            throw new IllegalArgumentException("recovery token and acceptance runId mismatch");
        }
        return accept(request, recoveryToken, true, TransactionAugment.NONE);
    }

    StageAcceptanceDecision acceptAugmented(
            StageAssignmentAcceptance request, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (augment == null) {
            throw new IllegalArgumentException("transaction augment is required");
        }
        if (recovered && recoveryToken == null) {
            throw new IllegalArgumentException("recovery token is required");
        }
        if (recovered && request != null
                && !recoveryToken.runId().equals(
                request.token().fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and acceptance runId mismatch");
        }
        return accept(request, recoveryToken, recovered, augment);
    }

    private StageAcceptanceDecision accept(
            StageAssignmentAcceptance request, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (request == null) throw new IllegalArgumentException("acceptance is required");
        StageAssignmentToken token = request.token();
        String runId = token.fence().runId();
        long observedRootCzxid;
        try {
            AuthorityObservation initial = observeCoreAuthority(runId, null, -1L);
            if (initial.rootCzxid < 0L || initial.snapshot == null) {
                return StageAcceptanceDecision.rejected(
                        StageAcceptanceDecision.Code.STALE_RUN);
            }
            observedRootCzxid = initial.rootCzxid;
            observeEvidenceRead(EvidenceReadPhase
                    .ACCEPT_AFTER_LAYOUT_BEFORE_REQUIRED_SNAPSHOT);
        } catch (RunIncarnationChangedException changed) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.STALE_RUN);
        } catch (IllegalArgumentException | IllegalStateException contractFailure) {
            throw contractFailure;
        } catch (Exception failure) {
            throw unavailable(null, "observe ZK assignment acceptance", failure);
        }
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                if (authority.snapshot == null) {
                    return StageAcceptanceDecision.rejected(
                            StageAcceptanceDecision.Code.STALE_RUN);
                }
                Layout layout = authority.layout;
                SnapshotVersion run = authority.snapshot;
                if (run.snapshot.status().isTerminal()) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return StageAcceptanceDecision.rejected(
                            StageAcceptanceDecision.Code.STALE_RUN);
                }
                CatalogEntry catalog = readCatalogEntry(layout.runId);
                verifyExistingCatalogOrRace(layout, run, catalog);
                AssignmentShardView view = readAssignmentShardView(layout, token);
                OwnerVersion owner = readOwner(layout);
                StageAcceptanceDecision decision = evaluateAcceptance(
                        run.snapshot, owner, view, request, recoveryToken, recovered);
                if (decision.code() != StageAcceptanceDecision.Code.ACCEPTED) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return decision;
                }
                int shard = token.physicalOrdinal()
                        / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check().withVersion(run.version)
                        .forPath(layout.snapshot));
                appendLifecycleAndGuardChecks(authority, operations);
                operations.add(client.transactionOp().check().withVersion(catalog.version)
                        .forPath(catalog.path));
                operations.add(client.transactionOp().check().withVersion(view.rootVersion)
                        .forPath(layout.assignmentRoot));
                if (recovered) {
                    operations.add(client.transactionOp().check().withVersion(owner.version)
                            .forPath(layout.recoveryOwner));
                }
                byte[] acceptanceBytes = StageAssignmentManifestCodec
                        .encodeAcceptanceShard(view.snapshot, request);
                operations.add(client.transactionOp().setData()
                        .withVersion(view.acceptanceVersion)
                        .forPath(acceptanceShardPath(layout, shard), acceptanceBytes));
                augment.append(client, operations);
                observeGuardedMutation(GuardedMutationKind.ACCEPT, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.ACCEPT, -1, operations);
                observeGuardedMutation(GuardedMutationKind.ACCEPT, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                recordAssignmentWrite(operations);
                afterAcceptanceCommit.run();
                return StageAcceptanceDecision.accepted(request);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return StageAcceptanceDecision.rejected(
                                StageAcceptanceDecision.Code.STALE_RUN);
                    }
                    AuthorityObservation authority = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    AssignmentShardView reconciled =
                            readAssignmentShardViewUnchecked(authority.layout, token);
                    StageAcceptanceDecision replay = evaluateAcceptanceUnchecked(
                            authority.layout, reconciled, request);
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    if (replay.code()
                            == StageAcceptanceDecision.Code.ALREADY_ACCEPTED) {
                        return replay;
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK assignment acceptance outcome",
                            confirmationFailure);
                } catch (RunIncarnationChangedException changed) {
                    return StageAcceptanceDecision.rejected(
                            StageAcceptanceDecision.Code.STALE_RUN);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
            } catch (RunIncarnationChangedException changed) {
                return StageAcceptanceDecision.rejected(
                        StageAcceptanceDecision.Code.STALE_RUN);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(null, "accept ZK stage assignment", failure);
            }
        }
        throw unavailable(null, "ZK stage assignment acceptance did not converge", lastRace);
    }

    @Override
    public StageTerminalDecision recordTerminal(StageTerminalReceipt receipt) {
        return recordTerminal(receipt, null, false, TransactionAugment.NONE);
    }

    @Override
    public StageTerminalDecision recordTerminalRecovered(
            RecoveryToken recoveryToken, StageTerminalReceipt receipt) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        if (!recoveryToken.runId().equals(receipt.token().fence().runId())) {
            throw new IllegalArgumentException("recovery token and receipt runId mismatch");
        }
        return recordTerminal(receipt, recoveryToken, true,
                TransactionAugment.NONE);
    }

    StageTerminalDecision recordTerminalAugmented(
            StageTerminalReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (augment == null) {
            throw new IllegalArgumentException("transaction augment is required");
        }
        if (recovered && recoveryToken == null) {
            throw new IllegalArgumentException("recovery token is required");
        }
        if (recovered && receipt != null
                && !recoveryToken.runId().equals(
                receipt.token().fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and receipt runId mismatch");
        }
        return recordTerminal(receipt, recoveryToken, recovered, augment);
    }

    private StageTerminalDecision recordTerminal(
            StageTerminalReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        StageAssignmentToken token = receipt.token();
        String runId = token.fence().runId();
        long observedRootCzxid;
        try {
            AuthorityObservation initial = observeCoreAuthority(runId, null, -1L);
            if (initial.rootCzxid < 0L || initial.snapshot == null) {
                return StageTerminalDecision.rejected(
                        StageTerminalDecision.Code.STALE_RUN);
            }
            observedRootCzxid = initial.rootCzxid;
            observeEvidenceRead(EvidenceReadPhase
                    .TERMINAL_AFTER_LAYOUT_BEFORE_REQUIRED_SNAPSHOT);
        } catch (RunIncarnationChangedException changed) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.STALE_RUN);
        } catch (IllegalArgumentException | IllegalStateException contractFailure) {
            throw contractFailure;
        } catch (Exception failure) {
            throw unavailable(null, "observe ZK assignment terminal receipt", failure);
        }
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                if (authority.snapshot == null) {
                    return StageTerminalDecision.rejected(
                            StageTerminalDecision.Code.STALE_RUN);
                }
                Layout layout = authority.layout;
                SnapshotVersion run = authority.snapshot;
                if (run.snapshot.status().isTerminal()) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return StageTerminalDecision.rejected(
                            StageTerminalDecision.Code.STALE_RUN);
                }
                CatalogEntry catalog = readCatalogEntry(layout.runId);
                verifyExistingCatalogOrRace(layout, run, catalog);
                AssignmentShardView view = readAssignmentShardView(layout, token);
                OwnerVersion owner = readOwner(layout);
                StageTerminalDecision decision = evaluateTerminal(
                        run.snapshot, owner, view, receipt, recoveryToken, recovered);
                if (decision.code() != StageTerminalDecision.Code.RECORDED) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return decision;
                }
                int shard = token.physicalOrdinal()
                        / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check().withVersion(run.version)
                        .forPath(layout.snapshot));
                appendLifecycleAndGuardChecks(authority, operations);
                operations.add(client.transactionOp().check().withVersion(catalog.version)
                        .forPath(catalog.path));
                operations.add(client.transactionOp().check().withVersion(view.rootVersion)
                        .forPath(layout.assignmentRoot));
                operations.add(client.transactionOp().check()
                        .withVersion(view.acceptanceVersion)
                        .forPath(acceptanceShardPath(layout, shard)));
                if (recovered) {
                    operations.add(client.transactionOp().check().withVersion(owner.version)
                            .forPath(layout.recoveryOwner));
                }
                byte[] terminalBytes = StageAssignmentManifestCodec
                        .encodeTerminalShard(view.snapshot, receipt);
                operations.add(client.transactionOp().setData()
                        .withVersion(view.terminalVersion)
                        .forPath(terminalShardPath(layout, shard), terminalBytes));
                augment.append(client, operations);
                observeGuardedMutation(GuardedMutationKind.TERMINAL, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.TERMINAL, -1, operations);
                observeGuardedMutation(GuardedMutationKind.TERMINAL, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                recordAssignmentWrite(operations);
                afterTerminalCommit.run();
                return StageTerminalDecision.recorded(receipt);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return StageTerminalDecision.rejected(
                                StageTerminalDecision.Code.STALE_RUN);
                    }
                    AuthorityObservation authority = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    AssignmentShardView reconciled =
                            readAssignmentShardViewUnchecked(authority.layout, token);
                    StageTerminalDecision replay = evaluateTerminalUnchecked(
                            reconciled, receipt);
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    if (replay.code()
                            == StageTerminalDecision.Code.ALREADY_RECORDED) {
                        return replay;
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK stage terminal outcome",
                            confirmationFailure);
                } catch (RunIncarnationChangedException changed) {
                    return StageTerminalDecision.rejected(
                            StageTerminalDecision.Code.STALE_RUN);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
            } catch (RunIncarnationChangedException changed) {
                return StageTerminalDecision.rejected(
                        StageTerminalDecision.Code.STALE_RUN);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(null, "record ZK stage terminal", failure);
            }
        }
        throw unavailable(null, "ZK stage terminal did not converge", lastRace);
    }

    @Override
    public PreparedSinkDecision recordPreparedSink(PreparedSinkReceipt receipt) {
        return recordPreparedSink(
                receipt, null, false, TransactionAugment.NONE);
    }

    @Override
    public PreparedSinkDecision recordPreparedSinkRecovered(
            RecoveryToken recoveryToken, PreparedSinkReceipt receipt) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        if (!recoveryToken.runId().equals(receipt.token().fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and prepared receipt runId mismatch");
        }
        return recordPreparedSink(
                receipt, recoveryToken, true, TransactionAugment.NONE);
    }

    PreparedSinkDecision recordPreparedSinkAugmented(
            PreparedSinkReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (augment == null) {
            throw new IllegalArgumentException("transaction augment is required");
        }
        if (recovered && recoveryToken == null) {
            throw new IllegalArgumentException("recovery token is required");
        }
        if (recovered && receipt != null
                && !recoveryToken.runId().equals(
                receipt.token().fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and prepared receipt runId mismatch");
        }
        return recordPreparedSink(
                receipt, recoveryToken, recovered, augment);
    }

    private PreparedSinkDecision recordPreparedSink(
            PreparedSinkReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        StageAssignmentToken token = receipt.token();
        String runId = token.fence().runId();
        long observedRootCzxid;
        try {
            AuthorityObservation initial = observeCoreAuthority(runId, null, -1L);
            if (initial.rootCzxid < 0L || initial.snapshot == null) {
                return PreparedSinkDecision.rejected(
                        PreparedSinkDecision.Code.STALE_RUN);
            }
            observedRootCzxid = initial.rootCzxid;
        } catch (RunIncarnationChangedException changed) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.STALE_RUN);
        } catch (IllegalArgumentException | IllegalStateException contractFailure) {
            throw contractFailure;
        } catch (Exception failure) {
            throw unavailable(null, "observe ZK prepared Sink receipt", failure);
        }
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                if (authority.snapshot == null) {
                    return PreparedSinkDecision.rejected(
                            PreparedSinkDecision.Code.STALE_RUN);
                }
                Layout layout = authority.layout;
                SnapshotVersion run = authority.snapshot;
                if (run.snapshot.status().isTerminal()) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return PreparedSinkDecision.rejected(
                            PreparedSinkDecision.Code.STALE_RUN);
                }
                CatalogEntry catalog = readCatalogEntry(layout.runId);
                verifyExistingCatalogOrRace(layout, run, catalog);
                AssignmentShardView view = readAssignmentShardView(layout, token);
                OwnerVersion owner = readOwner(layout);
                PreparedSinkDecision decision = evaluatePreparedSink(
                        run.snapshot, owner, view, receipt,
                        recoveryToken, recovered);
                if (decision.code() != PreparedSinkDecision.Code.RECORDED) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return decision;
                }
                int shard = token.physicalOrdinal()
                        / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check().withVersion(run.version)
                        .forPath(layout.snapshot));
                appendLifecycleAndGuardChecks(authority, operations);
                operations.add(client.transactionOp().check().withVersion(catalog.version)
                        .forPath(catalog.path));
                operations.add(client.transactionOp().check().withVersion(view.rootVersion)
                        .forPath(layout.assignmentRoot));
                operations.add(client.transactionOp().check()
                        .withVersion(view.acceptanceVersion)
                        .forPath(acceptanceShardPath(layout, shard)));
                if (recovered) {
                    operations.add(client.transactionOp().check().withVersion(owner.version)
                            .forPath(layout.recoveryOwner));
                }
                byte[] terminalBytes = StageAssignmentManifestCodec
                        .encodePreparedSinkShard(view.snapshot, receipt);
                operations.add(client.transactionOp().setData()
                        .withVersion(view.terminalVersion)
                        .forPath(terminalShardPath(layout, shard), terminalBytes));
                augment.append(client, operations);
                observeGuardedMutation(GuardedMutationKind.TERMINAL, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.TERMINAL, -1, operations);
                observeGuardedMutation(GuardedMutationKind.TERMINAL, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                recordAssignmentWrite(operations);
                return PreparedSinkDecision.recorded(receipt);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return PreparedSinkDecision.rejected(
                                PreparedSinkDecision.Code.STALE_RUN);
                    }
                    AuthorityObservation authority = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    AssignmentShardView reconciled =
                            readAssignmentShardViewUnchecked(authority.layout, token);
                    PreparedSinkReceipt existing = reconciled == null ? null
                            : reconciled.snapshot.preparedSink(
                            token.physicalOrdinal());
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    if (existing != null && existing.equals(receipt)) {
                        return PreparedSinkDecision.alreadyRecorded(existing);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK prepared Sink outcome",
                            confirmationFailure);
                } catch (RunIncarnationChangedException changed) {
                    return PreparedSinkDecision.rejected(
                            PreparedSinkDecision.Code.STALE_RUN);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
            } catch (RunIncarnationChangedException changed) {
                return PreparedSinkDecision.rejected(
                        PreparedSinkDecision.Code.STALE_RUN);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(null, "record ZK prepared Sink receipt", failure);
            }
        }
        throw unavailable(null, "ZK prepared Sink receipt did not converge", lastRace);
    }

    @Override
    public ManifestMemberDecision recordManifestMember(ManifestMemberReceipt receipt) {
        return recordManifestMember(
                receipt, null, false,
                (ignoredClient, ignoredOperations, ignoredComplete) -> { });
    }

    @Override
    public ManifestMemberDecision recordManifestMemberRecovered(
            RecoveryToken recoveryToken, ManifestMemberReceipt receipt) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        if (!recoveryToken.runId().equals(receipt.token().fence().runId())) {
            throw new IllegalArgumentException("recovery token and member runId mismatch");
        }
        return recordManifestMember(
                receipt, recoveryToken, true,
                (ignoredClient, ignoredOperations, ignoredComplete) -> { });
    }

    ManifestMemberDecision recordManifestMemberAugmented(
            ManifestMemberReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered, ManifestMemberTransactionAugment augment) {
        if (augment == null) {
            throw new IllegalArgumentException("transaction augment is required");
        }
        if (recovered && recoveryToken == null) {
            throw new IllegalArgumentException("recovery token is required");
        }
        if (recovered && receipt != null
                && !recoveryToken.runId().equals(
                receipt.token().fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and member runId mismatch");
        }
        return recordManifestMember(
                receipt, recoveryToken, recovered, augment);
    }

    private ManifestMemberDecision recordManifestMember(
            ManifestMemberReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered, ManifestMemberTransactionAugment augment) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        StageAssignmentToken token = receipt.token();
        String runId = token.fence().runId();
        long observedRootCzxid;
        try {
            AuthorityObservation initial = observeCoreAuthority(runId, null, -1L);
            if (initial.rootCzxid < 0L || initial.snapshot == null) {
                return ManifestMemberDecision.rejected(
                        ManifestMemberDecision.Code.STALE_RUN);
            }
            observedRootCzxid = initial.rootCzxid;
            observeEvidenceRead(EvidenceReadPhase
                    .MANIFEST_MEMBER_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER);
        } catch (RunIncarnationChangedException changed) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.STALE_RUN);
        } catch (IllegalArgumentException | IllegalStateException contractFailure) {
            throw contractFailure;
        } catch (Exception failure) {
            throw unavailable(null, "observe ZK manifest member", failure);
        }
        if (!ensureAbortGuard(runId, observedRootCzxid,
                GuardedMutationKind.MANIFEST_MEMBER_ABORT_GUARD)) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.STALE_RUN);
        }
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                if (authority.snapshot == null) {
                    return ManifestMemberDecision.rejected(
                            ManifestMemberDecision.Code.STALE_RUN);
                }
                Layout layout = authority.layout;
                SnapshotVersion run = authority.snapshot;
                if (run.snapshot.status().isTerminal()) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return ManifestMemberDecision.rejected(
                            ManifestMemberDecision.Code.STALE_RUN);
                }
                CatalogEntry catalog = readCatalogEntry(layout.runId);
                verifyExistingCatalogOrRace(layout, run, catalog);
                ManifestMemberShardView view =
                        readManifestMemberShardView(layout, token);
                AbortVersion abort = readAbortVersion(layout);
                if (abort == null) throw new KeeperException.NoNodeException();
                if (abort.record != null) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return ManifestMemberDecision.rejected(
                            ManifestMemberDecision.Code.MEMBER_CONFLICT);
                }
                OwnerVersion owner = readOwner(layout);
                ManifestMemberDecision decision = evaluateManifestMember(
                        run.snapshot, owner, view, receipt,
                        recoveryToken, recovered);
                if (decision.code() != ManifestMemberDecision.Code.RECORDED) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return decision;
                }
                int shard = token.physicalOrdinal()
                        / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check().withVersion(run.version)
                        .forPath(layout.snapshot));
                appendLifecycleAndGuardChecks(authority, operations);
                operations.add(client.transactionOp().check().withVersion(catalog.version)
                        .forPath(catalog.path));
                operations.add(client.transactionOp().check()
                        .withVersion(view.assignments.rootVersion)
                        .forPath(layout.assignmentRoot));
                operations.add(client.transactionOp().check()
                        .withVersion(view.assignments.acceptanceVersion)
                        .forPath(acceptanceShardPath(layout, shard)));
                operations.add(client.transactionOp().check()
                        .withVersion(view.assignments.terminalVersion)
                        .forPath(terminalShardPath(layout, shard)));
                operations.add(client.transactionOp().check()
                        .withVersion(view.intent.version)
                        .forPath(layout.manifestIntent));
                operations.add(client.transactionOp().check()
                        .withVersion(abort.version).forPath(layout.sinkAbort));
                if (recovered) {
                    operations.add(client.transactionOp().check().withVersion(owner.version)
                            .forPath(layout.recoveryOwner));
                }
                byte[] memberBytes = ManifestCommitJournalCodec.encodeMemberShard(
                        view.members, receipt);
                operations.add(client.transactionOp().setData()
                        .withVersion(view.memberVersion)
                        .forPath(manifestMemberShardPath(layout, shard), memberBytes));
                ManifestCommitJournalSnapshot journal =
                        readManifestJournalView(layout).snapshot;
                ManifestMemberReceipt existing = journal.member(
                        token.physicalOrdinal());
                boolean completeAfterWrite = existing == null
                        ? completePreparedClosure(journal.withMember(receipt))
                        : completePreparedClosure(journal);
                augment.append(client, operations, completeAfterWrite);
                observeGuardedMutation(GuardedMutationKind.MANIFEST_MEMBER, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.MANIFEST_MEMBER, -1, operations);
                observeGuardedMutation(GuardedMutationKind.MANIFEST_MEMBER, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                recordAssignmentWrite(operations);
                afterManifestMemberCommit.run();
                return ManifestMemberDecision.recorded(receipt);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return ManifestMemberDecision.rejected(
                                ManifestMemberDecision.Code.STALE_RUN);
                    }
                    AuthorityObservation authority = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    ManifestMemberShardView reconciled =
                            readManifestMemberShardViewUnchecked(
                                    authority.layout, token);
                    ManifestMemberDecision replay = evaluateManifestMemberUnchecked(
                            reconciled, receipt);
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    if (replay.code()
                            == ManifestMemberDecision.Code.ALREADY_RECORDED) {
                        return replay;
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK manifest member outcome",
                            confirmationFailure);
                } catch (RunIncarnationChangedException changed) {
                    return ManifestMemberDecision.rejected(
                            ManifestMemberDecision.Code.STALE_RUN);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
            } catch (RunIncarnationChangedException changed) {
                return ManifestMemberDecision.rejected(
                        ManifestMemberDecision.Code.STALE_RUN);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(null, "record ZK manifest member", failure);
            }
        }
        throw unavailable(null, "ZK manifest member did not converge", lastRace);
    }

    @Override
    public ManifestIntentDecision commitManifestIntent(ManifestCommitIntent intent) {
        return commitManifestIntent(
                intent, null, false, TransactionAugment.NONE);
    }

    @Override
    public ManifestIntentDecision commitManifestIntentRecovered(
            RecoveryToken recoveryToken, ManifestCommitIntent intent) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (intent == null) throw new IllegalArgumentException("intent is required");
        if (!recoveryToken.runId().equals(intent.fence().runId())) {
            throw new IllegalArgumentException("recovery token and intent runId mismatch");
        }
        return commitManifestIntent(
                intent, recoveryToken, true, TransactionAugment.NONE);
    }

    ManifestIntentDecision commitManifestIntentAugmented(
            ManifestCommitIntent intent, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (augment == null) {
            throw new IllegalArgumentException("transaction augment is required");
        }
        if (recovered && recoveryToken == null) {
            throw new IllegalArgumentException("recovery token is required");
        }
        if (recovered && intent != null
                && !recoveryToken.runId().equals(intent.fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and intent runId mismatch");
        }
        return commitManifestIntent(
                intent, recoveryToken, recovered, augment);
    }

    private ManifestIntentDecision commitManifestIntent(
            ManifestCommitIntent intent, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (intent == null) throw new IllegalArgumentException("intent is required");
        String runId = intent.fence().runId();
        long observedRootCzxid;
        try {
            AuthorityObservation initial = observeCoreAuthority(runId, null, -1L);
            if (initial.rootCzxid < 0L || initial.snapshot == null) {
                return ManifestIntentDecision.rejected(
                        ManifestIntentDecision.Code.STALE_RUN);
            }
            observedRootCzxid = initial.rootCzxid;
            observeEvidenceRead(EvidenceReadPhase
                    .MANIFEST_INTENT_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER);
        } catch (RunIncarnationChangedException changed) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.STALE_RUN);
        } catch (IllegalArgumentException | IllegalStateException contractFailure) {
            throw contractFailure;
        } catch (Exception failure) {
            throw unavailable(null, "observe ZK manifest intent", failure);
        }
        if (!ensureAbortGuard(runId, observedRootCzxid,
                GuardedMutationKind.MANIFEST_INTENT_ABORT_GUARD)) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.STALE_RUN);
        }
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                if (authority.snapshot == null) {
                    return ManifestIntentDecision.rejected(
                            ManifestIntentDecision.Code.STALE_RUN);
                }
                Layout layout = authority.layout;
                SnapshotVersion run = authority.snapshot;
                if (run.snapshot.status().isTerminal()) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return ManifestIntentDecision.rejected(
                            ManifestIntentDecision.Code.STALE_RUN);
                }
                CatalogEntry catalog = readCatalogEntry(layout.runId);
                verifyExistingCatalogOrRace(layout, run, catalog);
                ManifestJournalView view = intent.hasQualityDecision()
                        ? null : readManifestJournalView(layout);
                QualityJournalView qualityView = intent.hasQualityDecision()
                        ? readQualityJournalView(layout, true) : null;
                AbortVersion abort = readAbortVersion(layout);
                OwnerVersion owner = readOwner(layout);
                if (abort == null) throw new KeeperException.NoNodeException();
                if (abort.record != null) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return ManifestIntentDecision.rejected(
                            ManifestIntentDecision.Code.INTENT_CONFLICT);
                }
                ManifestIntentDecision decision = intent.hasQualityDecision()
                        ? evaluateQualityManifestIntent(
                        run.snapshot, owner, qualityView, intent,
                        recoveryToken, recovered)
                        : evaluateManifestIntent(
                        run.snapshot, owner, view, intent,
                        recoveryToken, recovered);
                if (decision.code() != ManifestIntentDecision.Code.COMMITTED) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return decision;
                }
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check().withVersion(run.version)
                        .forPath(layout.snapshot));
                appendLifecycleAndGuardChecks(authority, operations);
                operations.add(client.transactionOp().check().withVersion(catalog.version)
                        .forPath(catalog.path));
                operations.add(client.transactionOp().check()
                        .withVersion(intent.hasQualityDecision()
                                ? qualityView.rootVersion
                                : view.assignments.rootVersion)
                        .forPath(layout.assignmentRoot));
                operations.add(client.transactionOp().check()
                        .withVersion(abort.version).forPath(layout.sinkAbort));
                int[] memberVersions = intent.hasQualityDecision()
                        ? qualityView.memberVersions : view.memberVersions;
                int[] memberShardIndices = intent.hasQualityDecision()
                        ? qualityView.memberShardIndices : null;
                for (int i = 0; i < memberVersions.length; i++) {
                    operations.add(client.transactionOp().check()
                            .withVersion(memberVersions[i])
                            .forPath(manifestMemberShardPath(layout,
                                    memberShardIndices == null
                                            ? i : memberShardIndices[i])));
                }
                if (recovered) {
                    operations.add(client.transactionOp().check().withVersion(owner.version)
                            .forPath(layout.recoveryOwner));
                }
                byte[] intentBytes = ManifestCommitJournalCodec.encodeIntent(intent);
                operations.add(client.transactionOp().setData()
                        .withVersion(intent.hasQualityDecision()
                                ? qualityView.intentVersion
                                : view.intentVersion)
                        .forPath(layout.manifestIntent, intentBytes));
                augment.append(client, operations);
                observeGuardedMutation(GuardedMutationKind.MANIFEST_INTENT, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.MANIFEST_INTENT, -1, operations);
                observeGuardedMutation(GuardedMutationKind.MANIFEST_INTENT, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                recordAssignmentWrite(operations);
                afterManifestIntentCommit.run();
                return ManifestIntentDecision.committed(intent);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return ManifestIntentDecision.rejected(
                                ManifestIntentDecision.Code.STALE_RUN);
                    }
                    AuthorityObservation authority = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    ManifestJournalView reconciled = intent.hasQualityDecision()
                            ? null : readManifestJournalViewUnchecked(authority.layout);
                    QualityJournalView reconciledQuality = intent.hasQualityDecision()
                            ? readQualityJournalView(authority.layout, true) : null;
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    if ((reconciled != null
                            && intent.equals(reconciled.snapshot.intent()))
                            || qualityIntentMatches(
                            reconciledQuality, intent)) {
                        return ManifestIntentDecision.alreadyCommitted(intent);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK manifest intent outcome",
                            confirmationFailure);
                } catch (RunIncarnationChangedException changed) {
                    return ManifestIntentDecision.rejected(
                            ManifestIntentDecision.Code.STALE_RUN);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
            } catch (RunIncarnationChangedException changed) {
                return ManifestIntentDecision.rejected(
                        ManifestIntentDecision.Code.STALE_RUN);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(null, "commit ZK manifest intent", failure);
            }
        }
        throw unavailable(null, "ZK manifest intent did not converge", lastRace);
    }

    @Override
    public BatchSinkAbortRecord batchSinkAbort(RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        Layout layout = existingLayout(fence.runId());
        if (layout == null) return null;
        try {
            AbortVersion current = readAbortVersion(layout);
            if (current == null || current.record == null) return null;
            if (!current.record.fence().equals(fence)) {
                throw corruption(fence.runId(), "durable Sink abort fence mismatch");
            }
            return current.record;
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable(null, "read ZK durable Sink abort", failure);
        }
    }

    @Override
    public BatchSinkAbortDecision recordBatchSinkAbort(BatchSinkAbortRecord record) {
        return recordBatchSinkAbort(
                record, null, false, TransactionAugment.NONE);
    }

    @Override
    public BatchSinkAbortDecision recordBatchSinkAbortRecovered(
            RecoveryToken token, BatchSinkAbortRecord record) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (record == null) throw new IllegalArgumentException("abort record is required");
        if (!token.runId().equals(record.fence().runId())) {
            throw new IllegalArgumentException("recovery token and abort runId mismatch");
        }
        return recordBatchSinkAbort(
                record, token, true, TransactionAugment.NONE);
    }

    BatchSinkAbortDecision recordBatchSinkAbortAugmented(
            BatchSinkAbortRecord record, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (augment == null) {
            throw new IllegalArgumentException("transaction augment is required");
        }
        if (recovered && recoveryToken == null) {
            throw new IllegalArgumentException("recovery token is required");
        }
        if (recovered && record != null
                && !recoveryToken.runId().equals(record.fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and abort runId mismatch");
        }
        return recordBatchSinkAbort(
                record, recoveryToken, recovered, augment);
    }

    private BatchSinkAbortDecision recordBatchSinkAbort(
            BatchSinkAbortRecord record, RecoveryToken recoveryToken,
            boolean recovered, TransactionAugment augment) {
        if (record == null) throw new IllegalArgumentException("abort record is required");
        String runId = record.fence().runId();
        long observedRootCzxid;
        try {
            AuthorityObservation initial = observeCoreAuthority(runId, null, -1L);
            if (initial.rootCzxid < 0L || initial.snapshot == null) {
                return BatchSinkAbortDecision.rejected(
                        BatchSinkAbortDecision.Code.STALE_RUN);
            }
            observedRootCzxid = initial.rootCzxid;
            observeEvidenceRead(EvidenceReadPhase
                    .SINK_ABORT_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER);
        } catch (RunIncarnationChangedException changed) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.STALE_RUN);
        } catch (IllegalArgumentException | IllegalStateException contractFailure) {
            throw contractFailure;
        } catch (Exception failure) {
            throw unavailable(null, "observe ZK durable Sink abort", failure);
        }
        if (!ensureAbortGuard(runId, observedRootCzxid,
                GuardedMutationKind.SINK_ABORT_GUARD)) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.STALE_RUN);
        }
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, null, observedRootCzxid);
                if (authority.snapshot == null) {
                    return BatchSinkAbortDecision.rejected(
                            BatchSinkAbortDecision.Code.STALE_RUN);
                }
                Layout layout = authority.layout;
                SnapshotVersion run = authority.snapshot;
                if (run.snapshot.status().isTerminal()) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return BatchSinkAbortDecision.rejected(
                            BatchSinkAbortDecision.Code.STALE_RUN);
                }
                CatalogEntry catalog = readCatalogEntry(layout.runId);
                verifyExistingCatalogOrRace(layout, run, catalog);
                boolean qualityBounded = StageAssignmentManifestCodec
                        .hasQualityGateBinding(client.getData()
                                .forPath(layout.assignmentRoot));
                ManifestJournalView journal = qualityBounded ? null
                        : readManifestJournalView(layout);
                QualityJournalView qualityJournal = qualityBounded
                        ? readQualityJournalView(layout, true) : null;
                QualityGateCommitJournalSnapshot qualitySnapshot = qualityBounded
                        ? qualityCommitJournalSnapshot(qualityJournal) : null;
                AbortVersion abort = readAbortVersion(layout);
                if (abort == null) throw new KeeperException.NoNodeException();
                OwnerVersion owner = readOwner(layout);
                BatchSinkAbortDecision decision = qualityBounded
                        ? evaluateQualityBatchSinkAbort(
                        run.snapshot, owner, qualitySnapshot, abort, record,
                        recoveryToken, recovered)
                        : evaluateBatchSinkAbort(
                        run.snapshot, owner, journal, abort, record,
                        recoveryToken, recovered);
                if (decision.code() != BatchSinkAbortDecision.Code.RECORDED) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    return decision;
                }
                byte[] encoded = BatchSinkAbortRecordCodec.encode(record);
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check().withVersion(run.version)
                        .forPath(layout.snapshot));
                appendLifecycleAndGuardChecks(authority, operations);
                operations.add(client.transactionOp().check().withVersion(catalog.version)
                        .forPath(catalog.path));
                operations.add(client.transactionOp().check()
                        .withVersion(qualityBounded
                                ? qualityJournal.rootVersion
                                : journal.assignments.rootVersion)
                        .forPath(layout.assignmentRoot));
                operations.add(client.transactionOp().check()
                        .withVersion(qualityBounded
                                ? qualityJournal.intentVersion
                                : journal.intentVersion)
                        .forPath(layout.manifestIntent));
                if (qualityBounded) {
                    for (int i = 0; i < qualityJournal.memberVersions.length; i++) {
                        operations.add(client.transactionOp().check()
                                .withVersion(qualityJournal.memberVersions[i])
                                .forPath(manifestMemberShardPath(layout,
                                        qualityJournal.memberShardIndices[i])));
                    }
                }
                operations.add(client.transactionOp().setData()
                        .withVersion(abort.version).forPath(layout.sinkAbort, encoded));
                if (recovered) {
                    operations.add(client.transactionOp().check().withVersion(owner.version)
                            .forPath(layout.recoveryOwner));
                }
                augment.append(client, operations);
                observeGuardedMutation(GuardedMutationKind.SINK_ABORT, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.SINK_ABORT, -1, operations);
                observeGuardedMutation(GuardedMutationKind.SINK_ABORT, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                recordAssignmentWrite(operations);
                return BatchSinkAbortDecision.recorded(record);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, observedRootCzxid)) {
                        return BatchSinkAbortDecision.rejected(
                                BatchSinkAbortDecision.Code.STALE_RUN);
                    }
                    AuthorityObservation authority = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    AbortVersion reconciled =
                            readAbortVersionUnchecked(authority.layout);
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, observedRootCzxid);
                    if (!sameAuthorityView(authority, confirmed)) continue;
                    if (reconciled != null && record.equals(reconciled.record)) {
                        return BatchSinkAbortDecision.alreadyRecorded(record);
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK durable Sink abort outcome",
                            confirmationFailure);
                } catch (RunIncarnationChangedException changed) {
                    return BatchSinkAbortDecision.rejected(
                            BatchSinkAbortDecision.Code.STALE_RUN);
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
            } catch (RunIncarnationChangedException changed) {
                return BatchSinkAbortDecision.rejected(
                        BatchSinkAbortDecision.Code.STALE_RUN);
            } catch (IllegalArgumentException | IllegalStateException contractFailure) {
                throw contractFailure;
            } catch (Exception failure) {
                throw unavailable(null, "record ZK durable Sink abort", failure);
            }
        }
        throw unavailable(null, "ZK durable Sink abort did not converge", lastRace);
    }

    private BatchSinkAbortDecision evaluateBatchSinkAbort(
            RunSnapshot run, OwnerVersion owner, ManifestJournalView journal,
            AbortVersion abort, BatchSinkAbortRecord requested,
            RecoveryToken recoveryToken, boolean recovered) {
        if (journal == null || !matchesCurrentAttempt(run, requested.fence())) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.STALE_RUN);
        }
        if (journal.snapshot.intent() != null) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.COMMIT_ALREADY_DECIDED);
        }
        QualityGateDecision qualityDecision =
                journal.snapshot.qualityDecision();
        if (qualityDecision != null
                && qualityDecision.outcome() == QualityGateDecision.Outcome.FAIL
                && (!"QUALITY_GATE_REJECTED".equals(requested.reasonCode())
                || !MessageDigest.isEqual(requested.identityDigest(),
                QualityGateDecisionCodec.digest(qualityDecision)))) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        AttemptStatus status = run.currentAttempt().status();
        boolean abortableCanceling = status == AttemptStatus.CANCELING
                && (run.currentAttempt().cancellationCause()
                == io.rowweave.run.CancellationCause.USER
                || run.currentAttempt().cancellationCause()
                == io.rowweave.run.CancellationCause.TIMEOUT);
        if (status != AttemptStatus.RUNNING && status != AttemptStatus.COMMITTING
                && status != AttemptStatus.RECONCILING && !abortableCanceling) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.ATTEMPT_NOT_ABORTABLE);
        }
        if (requested.assignmentGeneration()
                != journal.snapshot.assignments().manifest().generation()
                || !requested.publicationId().equals(
                run.currentAttempt().publicationId())) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        for (int i = 0; i < journal.snapshot.assignments()
                .manifest().assignmentCount(); i++) {
            ManifestMemberReceipt member = journal.snapshot.member(i);
            if (member != null
                    && (!requested.commitDomainId().equals(member.domainId())
                    || requested.generation() != member.generation()
                    || !requested.publicationId().equals(member.publicationId()))) {
                return BatchSinkAbortDecision.rejected(
                        BatchSinkAbortDecision.Code.RECORD_CONFLICT);
            }
        }
        BatchSinkAbortRecord existing = abort.record;
        if (existing != null) {
            if (existing.equals(requested)) {
                return BatchSinkAbortDecision.alreadyRecorded(existing);
            }
            if (!(existing.state() == BatchSinkAbortRecord.State.ABORT_DECIDED
                    && requested.state() == BatchSinkAbortRecord.State.ABORTED
                    && existing.sameDecision(requested))) {
                return BatchSinkAbortDecision.rejected(
                        BatchSinkAbortDecision.Code.RECORD_CONFLICT);
            }
        } else if (requested.state() != BatchSinkAbortRecord.State.ABORT_DECIDED) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        if ((!recovered && owner != null)
                || (recovered && (owner == null
                || !recoveryToken.equals(owner.token)))) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECOVERY_OWNED);
        }
        return BatchSinkAbortDecision.recorded(requested);
    }

    private BatchSinkAbortDecision evaluateQualityBatchSinkAbort(
            RunSnapshot run, OwnerVersion owner,
            QualityGateCommitJournalSnapshot journal,
            AbortVersion abort, BatchSinkAbortRecord requested,
            RecoveryToken recoveryToken, boolean recovered) {
        if (journal == null || !matchesCurrentAttempt(run, requested.fence())) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.STALE_RUN);
        }
        if (journal.intent() != null) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.COMMIT_ALREADY_DECIDED);
        }
        QualityGateDecision qualityDecision = journal.decision();
        if (qualityDecision != null
                && qualityDecision.outcome() == QualityGateDecision.Outcome.FAIL
                && (!"QUALITY_GATE_REJECTED".equals(requested.reasonCode())
                || !MessageDigest.isEqual(requested.identityDigest(),
                QualityGateDecisionCodec.digest(qualityDecision)))) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        AttemptStatus status = run.currentAttempt().status();
        boolean abortableCanceling = status == AttemptStatus.CANCELING
                && (run.currentAttempt().cancellationCause()
                == io.rowweave.run.CancellationCause.USER
                || run.currentAttempt().cancellationCause()
                == io.rowweave.run.CancellationCause.TIMEOUT);
        if (status != AttemptStatus.RUNNING && status != AttemptStatus.COMMITTING
                && status != AttemptStatus.RECONCILING && !abortableCanceling) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.ATTEMPT_NOT_ABORTABLE);
        }
        if (requested.assignmentGeneration() != journal.assignmentGeneration()
                || !requested.publicationId().equals(
                run.currentAttempt().publicationId())) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        for (ManifestMemberReceipt member : journal.members()) {
            if (member != null
                    && (!requested.commitDomainId().equals(member.domainId())
                    || requested.generation() != member.generation()
                    || !requested.publicationId().equals(member.publicationId()))) {
                return BatchSinkAbortDecision.rejected(
                        BatchSinkAbortDecision.Code.RECORD_CONFLICT);
            }
        }
        BatchSinkAbortRecord existing = abort.record;
        if (existing != null) {
            if (existing.equals(requested)) {
                return BatchSinkAbortDecision.alreadyRecorded(existing);
            }
            if (!(existing.state() == BatchSinkAbortRecord.State.ABORT_DECIDED
                    && requested.state() == BatchSinkAbortRecord.State.ABORTED
                    && existing.sameDecision(requested))) {
                return BatchSinkAbortDecision.rejected(
                        BatchSinkAbortDecision.Code.RECORD_CONFLICT);
            }
        } else if (requested.state() != BatchSinkAbortRecord.State.ABORT_DECIDED) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        if ((!recovered && owner != null)
                || (recovered && (owner == null
                || !recoveryToken.equals(owner.token)))) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECOVERY_OWNED);
        }
        return BatchSinkAbortDecision.recorded(requested);
    }

    private void ensureAssignmentLayout(AuthorityObservation authority)
            throws Exception {
        Layout layout = authority.layout;
        try {
            List<CuratorOp> operations = new ArrayList<>();
            operations.add(client.transactionOp().check()
                    .withVersion(authority.snapshot.version)
                    .forPath(layout.snapshot));
            appendLifecycleAndGuardChecks(authority, operations);
            operations.add(client.transactionOp().create()
                    .forPath(layout.stageAssignment, new byte[0]));
            operations.add(client.transactionOp().create()
                    .forPath(layout.assignmentShards, new byte[0]));
            operations.add(client.transactionOp().create()
                    .forPath(layout.acceptanceShards, new byte[0]));
            operations.add(client.transactionOp().create()
                    .forPath(layout.terminalShards, new byte[0]));
            operations.add(client.transactionOp().create().forPath(
                    layout.manifestMemberShards, new byte[0]));
            operations.add(client.transactionOp().create()
                    .forPath(layout.manifestIntent, new byte[0]));
            observeGuardedMutation(GuardedMutationKind.PUBLISH_LAYOUT, -1,
                    GuardedMutationMoment.BEFORE_COMMIT);
            client.transaction().forOperations(operations);
            observeTransactionWrite(
                    GuardedMutationKind.PUBLISH_LAYOUT, -1, operations);
            observeGuardedMutation(GuardedMutationKind.PUBLISH_LAYOUT, -1,
                    GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
            recordAssignmentWrite(operations);
        } catch (KeeperException.NodeExistsException racedOrExisting) {
            // Verify the complete fixed parent set below.
        }
        authority = observeCoreAuthority(
                layout.runId, null, authority.rootCzxid);
        layout = authority.layout;
        if (client.checkExists().forPath(layout.stageAssignment) == null
                || client.checkExists().forPath(layout.assignmentShards) == null
                || client.checkExists().forPath(layout.acceptanceShards) == null
                || client.checkExists().forPath(layout.terminalShards) == null
                || client.checkExists().forPath(layout.manifestMemberShards) == null) {
            throw confirmedPublishCorruption(authority,
                    corruption(layout.runId,
                            "incomplete stage assignment layout"));
        }
        if (client.checkExists().forPath(layout.manifestIntent) == null) {
            AssignmentView published = readAssignmentView(layout);
            if (published != null && published.snapshot.manifest()
                    .hasQualityGateBinding()) {
                throw confirmedPublishCorruption(authority,
                        corruption(layout.runId,
                                "quality decision slot is missing"));
            }
            try {
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check()
                        .withVersion(authority.snapshot.version)
                        .forPath(layout.snapshot));
                appendLifecycleAndGuardChecks(authority, operations);
                if (published != null) {
                    operations.add(client.transactionOp().check()
                            .withVersion(published.rootVersion)
                            .forPath(layout.assignmentRoot));
                }
                operations.add(client.transactionOp().create()
                        .forPath(layout.manifestIntent, new byte[0]));
                observeGuardedMutation(
                        GuardedMutationKind.PUBLISH_INTENT_REPAIR, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(
                        GuardedMutationKind.PUBLISH_INTENT_REPAIR,
                        -1, operations);
                observeGuardedMutation(
                        GuardedMutationKind.PUBLISH_INTENT_REPAIR, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                recordAssignmentWrite(operations);
            } catch (KeeperException.NodeExistsException raced) {
                // The exact empty guard is verified below.
            }
            authority = observeCoreAuthority(
                    layout.runId, null, authority.rootCzxid);
            layout = authority.layout;
        }
        if (client.getData().forPath(layout.manifestIntent).length != 0
                && client.checkExists().forPath(layout.assignmentRoot) == null) {
            throw confirmedPublishCorruption(authority,
                    corruption(layout.runId,
                            "stale manifest intent without assignment"));
        }
    }

    private void writeStagingShards(
            String runId, long expectedRootCzxid,
            StageAssignmentManifest manifest, StagingVersion initial)
            throws Exception {
        StageAssignmentSnapshot empty = StageAssignmentSnapshot.initial(manifest);
        StagingVersion staging = initial;
        Layout layout = observeCoreAuthority(
                runId, null, expectedRootCzxid).layout;
        for (int shard = 0; shard < manifest.shardCount(); shard++) {
            AuthorityObservation authority = observeCoreAuthority(
                    runId, null, expectedRootCzxid);
            layout = authority.layout;
            String assignmentPath = assignmentShardPath(layout, shard);
            String acceptancePath = acceptanceShardPath(layout, shard);
            String terminalPath = terminalShardPath(layout, shard);
            String memberPath = manifestMemberShardPath(layout, shard);
            byte[] assignmentBytes = StageAssignmentManifestCodec.encodeShard(manifest, shard);
            byte[] acceptanceBytes = StageAssignmentManifestCodec.encodeAcceptanceShard(empty, shard);
            byte[] terminalBytes = StageAssignmentManifestCodec.encodeTerminalShard(empty, shard);
            byte[] memberBytes = ManifestCommitJournalCodec.encodeMemberShard(
                    ManifestCommitJournalSnapshot.initial(empty), shard);
            Stat assignmentStat = client.checkExists().forPath(assignmentPath);
            Stat acceptanceStat = client.checkExists().forPath(acceptancePath);
            Stat terminalStat = client.checkExists().forPath(terminalPath);
            Stat memberStat = client.checkExists().forPath(memberPath);
            if ((assignmentStat == null) != (acceptanceStat == null)
                    || (assignmentStat == null) != (terminalStat == null)
                    || (assignmentStat == null) != (memberStat == null)) {
                throw confirmedPublishCorruption(authority,
                        corruption(layout.runId,
                                "incomplete staged assignment shard " + shard));
            }
            if (assignmentStat != null) {
                if (!MessageDigest.isEqual(assignmentBytes,
                        client.getData().forPath(assignmentPath))
                        || !MessageDigest.isEqual(acceptanceBytes,
                        client.getData().forPath(acceptancePath))
                        || !MessageDigest.isEqual(terminalBytes,
                        client.getData().forPath(terminalPath))) {
                    throw confirmedPublishCorruption(authority,
                            corruption(layout.runId,
                                    "staged assignment shard conflict " + shard));
                }
                if (!MessageDigest.isEqual(memberBytes,
                        client.getData().forPath(memberPath))) {
                    throw confirmedPublishCorruption(authority,
                            corruption(layout.runId,
                                    "staged manifest member shard conflict " + shard));
                }
                AuthorityObservation confirmed = observeCoreAuthority(
                        runId, null, expectedRootCzxid);
                if (!sameAuthorityView(authority, confirmed)) {
                    throw new KeeperException.BadVersionException();
                }
                continue;
            }
            List<CuratorOp> operations = new ArrayList<>();
            operations.add(client.transactionOp().check()
                    .withVersion(authority.snapshot.version)
                    .forPath(layout.snapshot));
            appendLifecycleAndGuardChecks(authority, operations);
            operations.add(client.transactionOp().setData()
                    .withVersion(staging.version)
                    .forPath(layout.assignmentStaging, staging.data));
            operations.add(client.transactionOp().create()
                    .forPath(assignmentPath, assignmentBytes));
            operations.add(client.transactionOp().create()
                    .forPath(acceptancePath, acceptanceBytes));
            operations.add(client.transactionOp().create()
                    .forPath(terminalPath, terminalBytes));
            operations.add(client.transactionOp().create()
                    .forPath(memberPath, memberBytes));
            observeGuardedMutation(
                    GuardedMutationKind.PUBLISH_STAGING_SHARD, shard,
                    GuardedMutationMoment.BEFORE_COMMIT);
            client.transaction().forOperations(operations);
            observeTransactionWrite(
                    GuardedMutationKind.PUBLISH_STAGING_SHARD,
                    shard, operations);
            observeGuardedMutation(
                    GuardedMutationKind.PUBLISH_STAGING_SHARD, shard,
                    GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
            recordAssignmentWrite(operations);
            observeCoreAuthority(runId, null, expectedRootCzxid);
            staging = readStaging(layout);
            if (staging == null || !MessageDigest.isEqual(initial.data, staging.data)) {
                throw new KeeperException.BadVersionException();
            }
        }
        AuthorityObservation confirmed = observeCoreAuthority(
                runId, null, expectedRootCzxid);
        layout = confirmed.layout;
        try {
            verifyShardNames(
                    layout.assignmentShards, manifest.shardCount(), layout.runId);
            verifyShardNames(
                    layout.acceptanceShards, manifest.shardCount(), layout.runId);
            verifyShardNames(
                    layout.terminalShards, manifest.shardCount(), layout.runId);
            verifyShardNames(layout.manifestMemberShards,
                    manifest.shardCount(), layout.runId);
        } catch (IllegalArgumentException
                 | IllegalStateException candidateCorruption) {
            throw confirmedPublishCorruption(
                    confirmed, candidateCorruption);
        }
    }

    /** Test-only wire accounting at successful assignment multi boundaries. */
    private void recordAssignmentWrite(List<CuratorOp> operations)
            throws Exception {
        if (assignmentWriteObserver == null) return;
        assignmentWriteObserver.onWrite(
                operations.size(), serializedMultiRequestBytes(operations));
    }

    private AssignmentView readAssignmentViewUnchecked(Layout layout) {
        try {
            return readAssignmentView(layout);
        } catch (Exception failure) {
            throw unavailable(null, "reconcile ZK stage assignment", failure);
        }
    }

    private AssignmentShardView readAssignmentShardViewUnchecked(
            Layout layout, StageAssignmentToken token) {
        try {
            return readAssignmentShardView(layout, token);
        } catch (Exception failure) {
            throw unavailable(null, "reconcile ZK stage assignment shard", failure);
        }
    }

    private ManifestMemberShardView readManifestMemberShardViewUnchecked(
            Layout layout, StageAssignmentToken token) {
        try {
            return readManifestMemberShardView(layout, token);
        } catch (Exception failure) {
            throw unavailable(null, "reconcile ZK manifest member shard", failure);
        }
    }

    private ManifestJournalView readManifestJournalViewUnchecked(Layout layout) {
        try {
            return readManifestJournalView(layout);
        } catch (Exception failure) {
            throw unavailable(null, "reconcile ZK manifest journal", failure);
        }
    }

    private QualityJournalView readQualityJournalView(
            Layout layout, boolean includeMembers) throws Exception {
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            Stat rootStat = new Stat();
            final byte[] rootBytes;
            try {
                rootBytes = client.getData().storingStatIn(rootStat)
                        .forPath(layout.assignmentRoot);
            } catch (KeeperException.NoNodeException absent) {
                return null;
            }
            if (readStaging(layout) != null) {
                throw corruption(layout.runId,
                        "published assignment retains staging marker");
            }
            StageAssignmentManifestCodec.QualityRootSnapshot root =
                    StageAssignmentManifestCodec.decodeQualityRoot(rootBytes);
            if (!layout.runId.equals(root.fence().runId())) {
                throw corruption(layout.runId,
                        "quality assignment root runId mismatch");
            }
            int[] shardIndices = includeMembers
                    ? qualityMemberShardIndices(root.qualityGateBinding())
                    : new int[0];
            ManifestCommitJournalCodec.MemberShardSnapshot[] memberShards =
                    new ManifestCommitJournalCodec.MemberShardSnapshot[
                            shardIndices.length];
            List<byte[]> memberShardDigests = new ArrayList<>(
                    shardIndices.length);
            int[] memberVersions = new int[shardIndices.length];
            int[] acceptanceVersions = new int[shardIndices.length];
            int[] terminalVersions = new int[shardIndices.length];
            for (int i = 0; i < shardIndices.length; i++) {
                int shard = shardIndices[i];
                byte[] assignmentBytes = client.getData()
                        .forPath(assignmentShardPath(layout, shard));
                Stat acceptanceStat = new Stat();
                byte[] acceptanceBytes = client.getData()
                        .storingStatIn(acceptanceStat)
                        .forPath(acceptanceShardPath(layout, shard));
                Stat terminalStat = new Stat();
                byte[] terminalBytes = client.getData()
                        .storingStatIn(terminalStat)
                        .forPath(terminalShardPath(layout, shard));
                StageAssignmentManifestCodec.ShardSnapshot assignment =
                        StageAssignmentManifestCodec.decodeShardSnapshot(
                                rootBytes, assignmentBytes, acceptanceBytes,
                                terminalBytes, shard);
                Stat memberStat = new Stat();
                byte[] memberBytes = client.getData()
                        .storingStatIn(memberStat)
                        .forPath(manifestMemberShardPath(layout, shard));
                memberShardDigests.add(sha256(memberBytes));
                memberShards[i] = ManifestCommitJournalCodec.decodeMemberShard(
                        assignment, memberBytes, shard);
                memberVersions[i] = memberStat.getVersion();
                acceptanceVersions[i] = acceptanceStat.getVersion();
                terminalVersions[i] = terminalStat.getVersion();
            }
            IntentVersion intent = readManifestIntent(layout);
            if (intent == null) {
                throw corruption(layout.runId,
                        "manifest intent guard is missing");
            }
            ManifestCommitJournalCodec.QualitySlotSnapshot slot =
                    ManifestCommitJournalCodec.decodeQualitySlot(intent.data);
            Stat rootAfter = client.checkExists()
                    .forPath(layout.assignmentRoot);
            if (rootAfter == null
                    || rootAfter.getVersion() != rootStat.getVersion()) {
                continue;
            }
            boolean changed = false;
            for (int i = 0; i < shardIndices.length; i++) {
                int shard = shardIndices[i];
                Stat acceptanceAfter = client.checkExists()
                        .forPath(acceptanceShardPath(layout, shard));
                Stat terminalAfter = client.checkExists()
                        .forPath(terminalShardPath(layout, shard));
                Stat memberAfter = client.checkExists()
                        .forPath(manifestMemberShardPath(layout, shard));
                if (acceptanceAfter == null || terminalAfter == null
                        || memberAfter == null
                        || acceptanceAfter.getVersion()
                        != acceptanceVersions[i]
                        || terminalAfter.getVersion() != terminalVersions[i]
                        || memberAfter.getVersion() != memberVersions[i]) {
                    changed = true;
                    break;
                }
            }
            Stat intentAfter = client.checkExists()
                    .forPath(layout.manifestIntent);
            if (changed || intentAfter == null
                    || intentAfter.getVersion() != intent.version) {
                continue;
            }
            List<ManifestMemberReceipt> members = new ArrayList<>(3);
            List<StageAssignmentAcceptance> acceptances = new ArrayList<>(3);
            List<StageTerminalReceipt> terminals = new ArrayList<>(3);
            if (includeMembers) {
                for (QualityGateAssignmentBinding.MemberBinding binding
                        : root.qualityGateBinding().members()) {
                    int shard = binding.physicalOrdinal()
                            / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
                    int selected = indexOf(shardIndices, shard);
                    if (selected < 0) {
                        throw corruption(layout.runId,
                                "quality member shard selection is incomplete");
                    }
                    ManifestCommitJournalCodec.MemberShardSnapshot memberShard =
                            memberShards[selected];
                    members.add(memberShard.member(binding.physicalOrdinal()));
                    acceptances.add(memberShard.assignments()
                            .acceptance(binding.physicalOrdinal()));
                    terminals.add(memberShard.assignments()
                            .terminal(binding.physicalOrdinal()));
                }
            }
            if (qualityJournalReadObserver != null) {
                qualityJournalReadObserver.onRead(shardIndices.length);
            }
            long acceptanceGeneration = memberShards.length == 0 ? 1L
                    : memberShards[0].assignments().acceptanceGeneration();
            return new QualityJournalView(root, acceptanceGeneration,
                    acceptances, terminals, members,
                    rootStat.getVersion(), shardIndices, memberVersions,
                    memberShardDigests, intent.version, slot);
        }
        throw new KeeperException.BadVersionException();
    }

    private static QualityGateCommitJournalSnapshot qualityCommitJournalSnapshot(
            QualityJournalView view) {
        if (view == null) return null;
        return new QualityGateCommitJournalSnapshot(
                view.root.fence(), view.root.artifactId(),
                view.root.generation(), view.acceptanceGeneration,
                view.root.qualityGateBinding(), view.acceptances,
                view.terminals, view.members, view.memberShardDigests,
                view.slot.decision(), view.slot.intent());
    }

    private static int[] qualityMemberShardIndices(
            QualityGateAssignmentBinding binding) {
        int[] selected = new int[QualityGateAssignmentBinding.MEMBER_COUNT];
        int count = 0;
        for (QualityGateAssignmentBinding.MemberBinding member
                : binding.members()) {
            int shard = member.physicalOrdinal()
                    / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
            if (indexOf(selected, count, shard) < 0) selected[count++] = shard;
        }
        Arrays.sort(selected, 0, count);
        return Arrays.copyOf(selected, count);
    }

    private static int indexOf(int[] values, int target) {
        return indexOf(values, values.length, target);
    }

    private static int indexOf(int[] values, int length, int target) {
        for (int i = 0; i < length; i++) {
            if (values[i] == target) return i;
        }
        return -1;
    }

    private ManifestMemberShardView readManifestMemberShardView(
            Layout layout, StageAssignmentToken token) throws Exception {
        AssignmentShardView assignment = readAssignmentShardView(layout, token);
        if (assignment == null) return null;
        int shard = token.physicalOrdinal()
                / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        Stat memberStat = new Stat();
        byte[] memberBytes = client.getData().storingStatIn(memberStat)
                .forPath(manifestMemberShardPath(layout, shard));
        ManifestCommitJournalCodec.MemberShardSnapshot members =
                ManifestCommitJournalCodec.decodeMemberShard(
                        assignment.snapshot, memberBytes, shard);
        IntentVersion intent = readManifestIntent(layout);
        if (intent == null) {
            throw corruption(layout.runId, "manifest intent guard is missing");
        }
        return new ManifestMemberShardView(
                assignment, members, memberStat.getVersion(), intent);
    }

    private ManifestJournalView readManifestJournalView(Layout layout) throws Exception {
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            AssignmentView assignments = readAssignmentView(layout);
            if (assignments == null) return null;
            List<String> names = sortedChildren(layout.manifestMemberShards, layout.runId);
            if (names.size() != assignments.snapshot.manifest().shardCount()) {
                throw corruption(layout.runId, "manifest member shard count mismatch");
            }
            List<byte[]> memberBytes = new ArrayList<>(names.size());
            int[] memberVersions = new int[names.size()];
            for (int i = 0; i < names.size(); i++) {
                if (!shardName(i).equals(names.get(i))) {
                    throw corruption(layout.runId,
                            "non-canonical manifest member shard name");
                }
                Stat stat = new Stat();
                memberBytes.add(client.getData().storingStatIn(stat)
                        .forPath(manifestMemberShardPath(layout, i)));
                memberVersions[i] = stat.getVersion();
            }
            IntentVersion intent = readManifestIntent(layout);
            if (intent == null) {
                throw corruption(layout.runId, "manifest intent guard is missing");
            }
            ManifestCommitJournalSnapshot snapshot =
                    ManifestCommitJournalCodec.decodeSnapshot(
                            assignments.snapshot, memberBytes,
                            intent.data.length == 0 ? null : intent.data);
            if (!assignmentViewCurrent(layout, assignments)) continue;
            boolean changed = false;
            for (int i = 0; i < memberVersions.length; i++) {
                Stat after = client.checkExists()
                        .forPath(manifestMemberShardPath(layout, i));
                if (after == null || after.getVersion() != memberVersions[i]) {
                    changed = true;
                    break;
                }
            }
            Stat intentAfter = client.checkExists().forPath(layout.manifestIntent);
            if (changed || intentAfter == null
                    || intentAfter.getVersion() != intent.version) continue;
            return new ManifestJournalView(snapshot, assignments,
                    memberVersions, intent.version);
        }
        throw new KeeperException.BadVersionException();
    }

    private boolean assignmentViewCurrent(
            Layout layout, AssignmentView view) throws Exception {
        Stat root = client.checkExists().forPath(layout.assignmentRoot);
        if (root == null || root.getVersion() != view.rootVersion) return false;
        for (int i = 0; i < view.acceptanceVersions.length; i++) {
            Stat acceptance = client.checkExists()
                    .forPath(acceptanceShardPath(layout, i));
            Stat terminal = client.checkExists()
                    .forPath(terminalShardPath(layout, i));
            if (acceptance == null
                    || acceptance.getVersion() != view.acceptanceVersions[i]
                    || terminal == null
                    || terminal.getVersion() != view.terminalVersions[i]) return false;
        }
        return true;
    }

    private IntentVersion readManifestIntent(Layout layout) throws Exception {
        try {
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat)
                    .forPath(layout.manifestIntent);
            return new IntentVersion(data, stat.getVersion());
        } catch (KeeperException.NoNodeException absent) {
            return null;
        }
    }

    private AbortVersion readAbortVersion(Layout layout) throws Exception {
        try {
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat)
                    .forPath(layout.sinkAbort);
            return new AbortVersion(data.length == 0
                    ? null : BatchSinkAbortRecordCodec.decode(data), stat.getVersion());
        } catch (KeeperException.NoNodeException absent) {
            return null;
        }
    }

    private AbortVersion readAbortVersionUnchecked(Layout layout) {
        try {
            return readAbortVersion(layout);
        } catch (Exception failure) {
            throw unavailable(null, "reconcile ZK durable Sink abort", failure);
        }
    }

    private boolean ensureAbortGuard(
            String runId, long expectedRootCzxid,
            GuardedMutationKind mutationKind) {
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                AuthorityObservation authority = observeCoreAuthority(
                        runId, null, expectedRootCzxid);
                if (authority.rootCzxid < 0L || authority.snapshot == null) {
                    return false;
                }
                Layout layout = authority.layout;
                if (client.checkExists().forPath(layout.sinkAbort) != null) {
                    AuthorityObservation confirmed = observeCoreAuthority(
                            runId, null, expectedRootCzxid);
                    if (sameAuthorityView(authority, confirmed)) return true;
                    continue;
                }
                List<CuratorOp> operations = new ArrayList<>();
                operations.add(client.transactionOp().check()
                        .withVersion(authority.snapshot.version)
                        .forPath(layout.snapshot));
                appendLifecycleAndGuardChecks(authority, operations);
                operations.add(client.transactionOp().create()
                        .forPath(layout.sinkAbort, new byte[0]));
                observeGuardedMutation(mutationKind, -1,
                        GuardedMutationMoment.BEFORE_COMMIT);
                client.transaction().forOperations(operations);
                observeTransactionWrite(mutationKind, -1, operations);
                observeGuardedMutation(mutationKind, -1,
                        GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                return true;
            } catch (KeeperException.BadVersionException
                     | KeeperException.NodeExistsException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
            } catch (KeeperException.ConnectionLossException
                     | KeeperException.OperationTimeoutException unknown) {
                lastRace = unknown;
                try {
                    if (!confirmMutationAuthority(runId, expectedRootCzxid)) {
                        return false;
                    }
                    AuthorityObservation authority = observeCoreAuthority(
                            runId, null, expectedRootCzxid);
                    if (client.checkExists()
                            .forPath(authority.layout.sinkAbort) != null) {
                        AuthorityObservation confirmed = observeCoreAuthority(
                                runId, null, expectedRootCzxid);
                        if (sameAuthorityView(authority, confirmed)) return true;
                    }
                } catch (KeeperException.ConnectionLossException
                         | KeeperException.OperationTimeoutException
                                 confirmationFailure) {
                    throw unavailable(null,
                            "confirm unknown ZK durable Sink abort guard outcome",
                            confirmationFailure);
                } catch (RunIncarnationChangedException changed) {
                    return false;
                } catch (IllegalArgumentException
                         | IllegalStateException corruption) {
                    throw corruption;
                } catch (Exception confirmationRace) {
                    lastRace = confirmationRace;
                }
            } catch (RunIncarnationChangedException changed) {
                return false;
            } catch (IllegalArgumentException | IllegalStateException failure) {
                throw failure;
            } catch (Exception failure) {
                throw unavailable(null,
                        "ensure ZK durable Sink abort guard", failure);
            }
        }
        throw unavailable(null,
                "ZK durable Sink abort guard did not converge", lastRace);
    }

    /** Receipt hot path: authenticate root plus the one immutable/mutable shard it changes. */
    private AssignmentShardView readAssignmentShardView(
            Layout layout, StageAssignmentToken token) throws Exception {
        int ordinal = token.physicalOrdinal();
        if (ordinal < 0 || ordinal >= StageAssignmentManifest.MAX_ASSIGNMENTS) {
            return null;
        }
        int shard = ordinal / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        Stat rootStat = new Stat();
        final byte[] root;
        try {
            root = client.getData().storingStatIn(rootStat)
                    .forPath(layout.assignmentRoot);
        } catch (KeeperException.NoNodeException absent) {
            return null;
        }
        if (readStaging(layout) != null) {
            throw corruption(layout.runId,
                    "published assignment retains staging marker");
        }
        byte[] assignment = client.getData()
                .forPath(assignmentShardPath(layout, shard));
        Stat acceptanceStat = new Stat();
        byte[] acceptance = client.getData().storingStatIn(acceptanceStat)
                .forPath(acceptanceShardPath(layout, shard));
        Stat terminalStat = new Stat();
        byte[] terminal = client.getData().storingStatIn(terminalStat)
                .forPath(terminalShardPath(layout, shard));
        StageAssignmentManifestCodec.ShardSnapshot snapshot =
                StageAssignmentManifestCodec.decodeShardSnapshot(
                        root, assignment, acceptance, terminal, shard);
        if (!layout.runId.equals(snapshot.fence().runId())) {
            throw corruption(layout.runId, "assignment manifest runId mismatch");
        }
        return new AssignmentShardView(snapshot, rootStat.getVersion(),
                acceptanceStat.getVersion(), terminalStat.getVersion());
    }

    private AssignmentView readAssignmentView(Layout layout) throws Exception {
        if (client.checkExists().forPath(layout.stageAssignment) == null) return null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            Stat rootStat = new Stat();
            final byte[] root;
            try {
                root = client.getData().storingStatIn(rootStat).forPath(layout.assignmentRoot);
            } catch (KeeperException.NoNodeException absent) {
                if (readStaging(layout) == null) requireNoOrphanAssignmentShards(layout);
                return null;
            }
            if (readStaging(layout) != null) {
                throw corruption(layout.runId, "published assignment retains staging marker");
            }
            List<String> assignmentNames = sortedChildren(layout.assignmentShards, layout.runId);
            List<String> acceptanceNames = sortedChildren(layout.acceptanceShards, layout.runId);
            List<String> terminalNames = sortedChildren(layout.terminalShards, layout.runId);
            if (assignmentNames.size() != acceptanceNames.size()
                    || assignmentNames.size() != terminalNames.size()
                    || assignmentNames.size() > StageAssignmentManifest.MAX_SHARDS) {
                throw corruption(layout.runId,
                        "assignment/acceptance/terminal shard count mismatch");
            }
            List<byte[]> assignmentBytes = new ArrayList<>(assignmentNames.size());
            List<byte[]> acceptanceBytes = new ArrayList<>(acceptanceNames.size());
            List<byte[]> terminalBytes = new ArrayList<>(terminalNames.size());
            int[] assignmentVersions = new int[assignmentNames.size()];
            int[] acceptanceVersions = new int[acceptanceNames.size()];
            int[] terminalVersions = new int[terminalNames.size()];
            for (int i = 0; i < assignmentNames.size(); i++) {
                String expected = shardName(i);
                if (!expected.equals(assignmentNames.get(i))
                        || !expected.equals(acceptanceNames.get(i))
                        || !expected.equals(terminalNames.get(i))) {
                    throw corruption(layout.runId, "non-canonical stage assignment shard name");
                }
                Stat assignmentStat = new Stat();
                assignmentBytes.add(client.getData().storingStatIn(assignmentStat)
                        .forPath(assignmentShardPath(layout, i)));
                assignmentVersions[i] = assignmentStat.getVersion();
                Stat acceptanceStat = new Stat();
                acceptanceBytes.add(client.getData().storingStatIn(acceptanceStat)
                        .forPath(acceptanceShardPath(layout, i)));
                acceptanceVersions[i] = acceptanceStat.getVersion();
                Stat terminalStat = new Stat();
                terminalBytes.add(client.getData().storingStatIn(terminalStat)
                        .forPath(terminalShardPath(layout, i)));
                terminalVersions[i] = terminalStat.getVersion();
            }
            StageAssignmentSnapshot snapshot = StageAssignmentManifestCodec.decodeSnapshot(
                    root, assignmentBytes, acceptanceBytes, terminalBytes);
            if (!layout.runId.equals(snapshot.manifest().fence().runId())) {
                throw corruption(layout.runId, "assignment manifest runId mismatch");
            }
            Stat rootAfter = client.checkExists().forPath(layout.assignmentRoot);
            if (rootAfter == null || rootAfter.getVersion() != rootStat.getVersion()) continue;
            boolean changed = false;
            for (int i = 0; i < acceptanceVersions.length; i++) {
                Stat after = client.checkExists().forPath(acceptanceShardPath(layout, i));
                if (after == null || after.getVersion() != acceptanceVersions[i]) {
                    changed = true;
                    break;
                }
                after = client.checkExists().forPath(terminalShardPath(layout, i));
                if (after == null || after.getVersion() != terminalVersions[i]) {
                    changed = true;
                    break;
                }
            }
            if (!changed) {
                return new AssignmentView(snapshot, rootStat.getVersion(),
                        assignmentVersions, acceptanceVersions, terminalVersions);
            }
        }
        throw new KeeperException.BadVersionException();
    }

    private StageAcceptanceDecision evaluateAcceptance(
            RunSnapshot run, OwnerVersion owner, AssignmentShardView view,
            StageAssignmentAcceptance request, RecoveryToken recoveryToken,
            boolean recovered) {
        StageAssignmentToken token = request.token();
        if (view == null || !matchesCurrentAttempt(run, token.fence())) {
            return StageAcceptanceDecision.rejected(StageAcceptanceDecision.Code.STALE_RUN);
        }
        StageAssignmentManifestCodec.ShardSnapshot manifest = view.snapshot;
        int ordinal = token.physicalOrdinal();
        if (!manifest.fence().equals(token.fence())
                || token.manifestGeneration() != manifest.generation()
                || ordinal < 0 || ordinal >= manifest.assignmentCount()) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.ASSIGNMENT_CONFLICT);
        }
        final StageAssignmentRecord record;
        try {
            record = manifest.record(ordinal);
        } catch (IllegalArgumentException wrongShard) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (!record.stageExecutionId().equals(token.stageExecutionId())
                || record.runtimeStageId() != token.runtimeStageId()
                || !record.workerId().equals(token.workerId())
                || !record.workerIncarnationId().equals(token.workerIncarnationId())) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.ASSIGNMENT_CONFLICT);
        }
        StageAssignmentAcceptance existing = manifest.acceptance(ordinal);
        if (existing != null) {
            return existing.token().equals(token)
                    && MessageDigest.isEqual(existing.commandDigest(), request.commandDigest())
                    ? StageAcceptanceDecision.alreadyAccepted(existing)
                    : StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (token.acceptanceGeneration() != manifest.acceptanceGeneration()) {
            return StageAcceptanceDecision.staleGeneration();
        }
        if ((!recovered && owner != null)
                || (recovered && (owner == null
                || !recoveryToken.equals(owner.token)))) {
            return StageAcceptanceDecision.rejected(StageAcceptanceDecision.Code.RECOVERY_OWNED);
        }
        return StageAcceptanceDecision.accepted(request);
    }

    private StageAcceptanceDecision evaluateAcceptanceUnchecked(
            Layout layout, AssignmentShardView view, StageAssignmentAcceptance request) {
        if (view != null) {
            int ordinal = request.token().physicalOrdinal();
            if (ordinal >= 0 && ordinal < view.snapshot.assignmentCount()) {
                StageAssignmentAcceptance existing;
                try {
                    existing = view.snapshot.acceptance(ordinal);
                } catch (IllegalArgumentException wrongShard) {
                    existing = null;
                }
                if (existing != null && existing.token().equals(request.token())
                        && MessageDigest.isEqual(existing.commandDigest(), request.commandDigest())) {
                    return StageAcceptanceDecision.alreadyAccepted(existing);
                }
            }
        }
        return StageAcceptanceDecision.rejected(StageAcceptanceDecision.Code.UNAVAILABLE);
    }

    private StageTerminalDecision evaluateTerminal(
            RunSnapshot run, OwnerVersion owner, AssignmentShardView view,
            StageTerminalReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered) {
        StageAssignmentToken token = receipt.token();
        if (view == null || !matchesCurrentAttempt(run, token.fence())) {
            return StageTerminalDecision.rejected(StageTerminalDecision.Code.STALE_RUN);
        }
        StageAssignmentManifestCodec.ShardSnapshot manifest = view.snapshot;
        int ordinal = token.physicalOrdinal();
        if (!manifest.fence().equals(token.fence())
                || token.manifestGeneration() != manifest.generation()
                || ordinal < 0 || ordinal >= manifest.assignmentCount()) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.ASSIGNMENT_CONFLICT);
        }
        final StageAssignmentRecord record;
        final StageAssignmentAcceptance acceptance;
        final StageTerminalReceipt existing;
        try {
            record = manifest.record(ordinal);
            acceptance = manifest.acceptance(ordinal);
            existing = manifest.terminal(ordinal);
        } catch (IllegalArgumentException wrongShard) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (!record.stageExecutionId().equals(token.stageExecutionId())
                || record.runtimeStageId() != token.runtimeStageId()
                || !record.workerId().equals(token.workerId())
                || !record.workerIncarnationId().equals(token.workerIncarnationId())) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (acceptance == null) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.ACCEPTANCE_REQUIRED);
        }
        if (!acceptance.token().equals(token)
                || !MessageDigest.isEqual(
                acceptance.commandDigest(), receipt.commandDigest())) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (existing != null) {
            return existing.hasSameTerminalBase(receipt)
                    ? StageTerminalDecision.alreadyRecorded(existing)
                    : StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.TERMINAL_CONFLICT);
        }
        if ((!recovered && owner != null)
                || (recovered && (owner == null
                || !recoveryToken.equals(owner.token)))) {
            return StageTerminalDecision.rejected(StageTerminalDecision.Code.RECOVERY_OWNED);
        }
        if (receipt.outcome() == StageTerminalOutcome.LOST && !recovered) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.LOST_REQUIRES_RECOVERY);
        }
        return StageTerminalDecision.recorded(receipt);
    }

    private StageTerminalDecision evaluateTerminalUnchecked(
            AssignmentShardView view, StageTerminalReceipt receipt) {
        if (view != null) {
            int ordinal = receipt.token().physicalOrdinal();
            try {
                StageTerminalReceipt existing = view.snapshot.terminal(ordinal);
                if (existing != null && existing.hasSameTerminalBase(receipt)) {
                    return StageTerminalDecision.alreadyRecorded(existing);
                }
            } catch (IllegalArgumentException wrongShard) {
                // The unknown write cannot be proven from this shard view.
            }
        }
        return StageTerminalDecision.rejected(StageTerminalDecision.Code.UNAVAILABLE);
    }

    private PreparedSinkDecision evaluatePreparedSink(
            RunSnapshot run, OwnerVersion owner, AssignmentShardView view,
            PreparedSinkReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered) {
        StageAssignmentToken token = receipt.token();
        if (view == null || !matchesCurrentAttempt(run, token.fence())) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.STALE_RUN);
        }
        StageAssignmentManifestCodec.ShardSnapshot shard = view.snapshot;
        int ordinal = token.physicalOrdinal();
        final StageAssignmentRecord record;
        final StageAssignmentAcceptance acceptance;
        final StageTerminalReceipt terminal;
        final PreparedSinkReceipt existing;
        try {
            record = shard.record(ordinal);
            acceptance = shard.acceptance(ordinal);
            terminal = shard.terminal(ordinal);
            existing = shard.preparedSink(ordinal);
        } catch (IllegalArgumentException wrongShard) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (token.manifestGeneration() != shard.generation()
                || !record.stageExecutionId().equals(token.stageExecutionId())
                || record.runtimeStageId() != token.runtimeStageId()
                || !record.workerId().equals(token.workerId())
                || !record.workerIncarnationId().equals(
                token.workerIncarnationId())) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (terminal == null) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.TERMINAL_REQUIRED);
        }
        if (!terminal.succeeded() || acceptance == null
                || !receipt.token().equals(terminal.token())
                || !receipt.token().equals(acceptance.token())
                || !MessageDigest.isEqual(
                receipt.commandDigest(), terminal.commandDigest())
                || !MessageDigest.isEqual(
                receipt.commandDigest(), acceptance.commandDigest())) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (existing != null) {
            return existing.equals(receipt)
                    ? PreparedSinkDecision.alreadyRecorded(existing)
                    : PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.LOCATOR_CONFLICT);
        }
        if ((!recovered && owner != null)
                || recovered && (owner == null
                || !recoveryToken.equals(owner.token))) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.RECOVERY_OWNED);
        }
        return PreparedSinkDecision.recorded(receipt);
    }

    private ManifestMemberDecision evaluateManifestMember(
            RunSnapshot run, OwnerVersion owner, ManifestMemberShardView view,
            ManifestMemberReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered) {
        StageAssignmentToken token = receipt.token();
        if (view == null || !matchesCurrentAttempt(run, token.fence())) {
            return ManifestMemberDecision.rejected(ManifestMemberDecision.Code.STALE_RUN);
        }
        StageAssignmentManifestCodec.ShardSnapshot assignments =
                view.assignments.snapshot;
        int ordinal = token.physicalOrdinal();
        final StageAssignmentRecord record;
        final StageAssignmentAcceptance acceptance;
        final StageTerminalReceipt terminal;
        final ManifestMemberReceipt existing;
        try {
            record = assignments.record(ordinal);
            acceptance = assignments.acceptance(ordinal);
            terminal = assignments.terminal(ordinal);
            existing = view.members.member(ordinal);
        } catch (IllegalArgumentException wrongShard) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (!assignments.fence().equals(token.fence())
                || token.manifestGeneration() != assignments.generation()
                || !record.stageExecutionId().equals(token.stageExecutionId())
                || record.runtimeStageId() != token.runtimeStageId()
                || !record.workerId().equals(token.workerId())
                || !record.workerIncarnationId().equals(token.workerIncarnationId())
                || !(record.role() == io.rowweave.run.StageAssignmentRole.SINK
                || record.role() == io.rowweave.run.StageAssignmentRole.WHOLE_GRAPH
                || record.role()
                == io.rowweave.run.StageAssignmentRole.SYSTEM_CONTROL)) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (acceptance == null || terminal == null) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.TERMINAL_REQUIRED);
        }
        if (!acceptance.token().equals(token)
                || !terminal.token().equals(token)
                || !MessageDigest.isEqual(
                acceptance.commandDigest(), receipt.commandDigest())
                || !MessageDigest.isEqual(
                terminal.commandDigest(), receipt.commandDigest())) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (!terminal.succeeded()) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.TERMINAL_NOT_SUCCESSFUL);
        }
        if (!run.currentAttempt().publicationId().equals(receipt.publicationId())) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.COMMIT_IDENTITY_CONFLICT);
        }
        if (existing != null) {
            return existing.equals(receipt)
                    ? ManifestMemberDecision.alreadyRecorded(existing)
                    : ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.MEMBER_CONFLICT);
        }
        if (view.intent.data.length != 0) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.INTENT_ALREADY_COMMITTED);
        }
        int first = assignments.firstOrdinal();
        int count = Math.min(StageAssignmentManifest.MAX_RECORDS_PER_SHARD,
                assignments.assignmentCount() - first);
        for (int i = 0; i < count; i++) {
            ManifestMemberReceipt member = view.members.member(first + i);
            if (member == null) continue;
            if (!member.domainId().equals(receipt.domainId())
                    || member.generation() != receipt.generation()
                    || !member.publicationId().equals(receipt.publicationId())) {
                return ManifestMemberDecision.rejected(
                        ManifestMemberDecision.Code.COMMIT_IDENTITY_CONFLICT);
            }
            if (member.member().name().equals(receipt.member().name())) {
                return ManifestMemberDecision.rejected(
                        ManifestMemberDecision.Code.MEMBER_CONFLICT);
            }
        }
        if ((!recovered && owner != null)
                || (recovered && (owner == null
                || !recoveryToken.equals(owner.token)))) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.RECOVERY_OWNED);
        }
        return ManifestMemberDecision.recorded(receipt);
    }

    private static boolean completePreparedClosure(
            ManifestCommitJournalSnapshot journal) {
        if (journal == null || !journal.allExpectedMembersSealed()) return false;
        StageAssignmentSnapshot assignments = journal.assignments();
        for (int i = 0; i < assignments.manifest().assignmentCount(); i++) {
            io.rowweave.run.StageAssignmentRole role =
                    assignments.manifest().assignment(i).role();
            if (role != io.rowweave.run.StageAssignmentRole.SINK
                    && role != io.rowweave.run.StageAssignmentRole.WHOLE_GRAPH
                    && role != io.rowweave.run.StageAssignmentRole.SYSTEM_CONTROL) {
                continue;
            }
            PreparedSinkReceipt prepared = assignments.preparedSink(i);
            ManifestMemberReceipt member = journal.member(i);
            if (prepared == null || member == null || !member.hasPreparedLocator()
                    || !prepared.locator().equals(member.preparedLocator())) {
                return false;
            }
        }
        return true;
    }

    private ManifestMemberDecision evaluateManifestMemberUnchecked(
            ManifestMemberShardView view, ManifestMemberReceipt receipt) {
        if (view != null) {
            try {
                ManifestMemberReceipt existing = view.members.member(
                        receipt.token().physicalOrdinal());
                if (existing != null && existing.equals(receipt)) {
                    return ManifestMemberDecision.alreadyRecorded(existing);
                }
            } catch (IllegalArgumentException wrongShard) {
                // The unknown write cannot be proven from this shard.
            }
        }
        return ManifestMemberDecision.rejected(ManifestMemberDecision.Code.UNAVAILABLE);
    }

    private ManifestIntentDecision evaluateManifestIntent(
            RunSnapshot run, OwnerVersion owner, ManifestJournalView view,
            ManifestCommitIntent intent, RecoveryToken recoveryToken,
            boolean recovered) {
        if (view == null || !matchesCurrentAttempt(run, intent.fence())) {
            return ManifestIntentDecision.rejected(ManifestIntentDecision.Code.STALE_RUN);
        }
        ManifestCommitIntent existing = view.snapshot.intent();
        if (existing != null) {
            return existing.equals(intent)
                    ? ManifestIntentDecision.alreadyCommitted(existing)
                    : ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if (run.currentAttempt().status() != AttemptStatus.COMMITTING) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.ATTEMPT_NOT_COMMITTING);
        }
        if (!intent.fence().equals(
                view.snapshot.assignments().manifest().fence())
                || intent.assignmentGeneration() !=
                view.snapshot.assignments().manifest().generation()
                || !intent.publicationId().equals(
                run.currentAttempt().publicationId())) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if (!view.snapshot.allExpectedMembersSealed()) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INCOMPLETE_MEMBERS);
        }
        if (!view.snapshot.buildIntent().equals(intent)) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if ((!recovered && owner != null)
                || (recovered && (owner == null
                || !recoveryToken.equals(owner.token)))) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.RECOVERY_OWNED);
        }
        return ManifestIntentDecision.committed(intent);
    }

    private ManifestIntentDecision evaluateQualityManifestIntent(
            RunSnapshot run, OwnerVersion owner, QualityJournalView journal,
            ManifestCommitIntent intent, RecoveryToken recoveryToken,
            boolean recovered) {
        if (journal == null || !matchesCurrentAttempt(run, intent.fence())) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.STALE_RUN);
        }
        try {
            qualityCommitJournalSnapshot(journal);
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.CORRUPT);
        }
        ManifestCommitIntent existing = journal.slot.intent();
        if (existing != null) {
            if (!qualityIntentMatches(journal, existing)) {
                return ManifestIntentDecision.rejected(
                        ManifestIntentDecision.Code.CORRUPT);
            }
            return existing.equals(intent)
                    ? ManifestIntentDecision.alreadyCommitted(existing)
                    : ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if (run.currentAttempt().status() != AttemptStatus.COMMITTING) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.ATTEMPT_NOT_COMMITTING);
        }
        if (!intent.hasQualityDecision()
                || !intent.fence().equals(journal.root.fence())
                || intent.assignmentGeneration() != journal.root.generation()
                || !intent.publicationId().equals(
                run.currentAttempt().publicationId())) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        QualityGateDecision decision = journal.slot.decision();
        if (decision == null
                || decision.outcome() != QualityGateDecision.Outcome.PASS
                || !decision.equals(intent.qualityDecision())) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if (journal.members.size() != QualityGateAssignmentBinding.MEMBER_COUNT
                || journal.members.contains(null)) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INCOMPLETE_MEMBERS);
        }
        if (!qualityEvidenceMatches(journal, decision)
                || !qualityIntentFrom(journal, decision).equals(intent)) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if ((!recovered && owner != null)
                || (recovered && (owner == null
                || !recoveryToken.equals(owner.token)))) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.RECOVERY_OWNED);
        }
        return ManifestIntentDecision.committed(intent);
    }

    private static boolean qualityIntentMatches(
            QualityJournalView journal, ManifestCommitIntent intent) {
        if (journal == null || intent == null || !intent.hasQualityDecision()) {
            return false;
        }
        QualityGateCommitJournalSnapshot snapshot =
                qualityCommitJournalSnapshot(journal);
        return intent.equals(snapshot.intent());
    }

    private static ManifestCommitIntent qualityIntentFrom(
            QualityJournalView journal, QualityGateDecision decision) {
        List<Integer> order = new ArrayList<>(
                QualityGateAssignmentBinding.MEMBER_COUNT);
        for (int i = 0; i < QualityGateAssignmentBinding.MEMBER_COUNT; i++) {
            order.add(i);
        }
        List<QualityGateAssignmentBinding.MemberBinding> bindings =
                journal.root.qualityGateBinding().members();
        order.sort(Comparator.comparingInt(
                index -> bindings.get(index).physicalOrdinal()));
        ManifestMemberReceipt identity = journal.members.get(order.get(0));
        List<CommitManifest.Member> descriptors = new ArrayList<>(
                QualityGateAssignmentBinding.MEMBER_COUNT);
        for (Integer index : order) {
            ManifestMemberReceipt member = journal.members.get(index);
            if (!identity.domainId().equals(member.domainId())
                    || identity.generation() != member.generation()
                    || !identity.publicationId().equals(member.publicationId())) {
                throw new IllegalStateException(
                        "quality manifest member commit identity mismatch");
            }
            descriptors.add(member.member());
        }
        CommitManifest manifest = new CommitManifest(
                identity.domainId(), identity.generation(),
                identity.publicationId(), descriptors);
        return ManifestCommitIntent.quality(
                journal.root.fence(), journal.root.generation(),
                identity.domainId(), identity.generation(),
                identity.publicationId(), descriptors.size(),
                journal.memberShardDigests, sha256(manifest.encode()),
                QualityGateDecisionCodec.encode(decision));
    }

    private QualityGateDecisionResult evaluateQualityDecision(
            RunSnapshot run, OwnerVersion owner, QualityJournalView journal,
            AbortVersion abort, QualityGateDecision requested,
            RecoveryToken recoveryToken, boolean recovered) {
        if (journal == null
                || !matchesCurrentAttempt(run, requested.fence())) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.STALE_RUN);
        }
        QualityGateCommitJournalSnapshot validated;
        try {
            validated = qualityCommitJournalSnapshot(journal);
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.CORRUPT);
        }
        QualityGateDecision existing = validated.decision();
        if (existing != null) {
            return existing.equals(requested)
                    ? QualityGateDecisionResult.alreadyRecorded(existing)
                    : QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.DECISION_CONFLICT);
        }
        if (abort.record != null) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.ABORT_ALREADY_DECIDED);
        }
        if (journal.slot.intent() != null) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.DECISION_CONFLICT);
        }
        if (run.currentAttempt().status() != AttemptStatus.COMMITTING) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.ATTEMPT_NOT_COMMITTING);
        }
        if (!requested.fence().equals(journal.root.fence())
                || !requested.artifactId().equals(journal.root.artifactId())
                || !requested.publicationId().equals(
                run.currentAttempt().publicationId())
                || !MessageDigest.isEqual(
                journal.root.qualityGateBinding().policyDigest(),
                requested.policyDigest())) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.IDENTITY_CONFLICT);
        }
        if (journal.members.size() != 3
                || journal.members.contains(null)) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.INCOMPLETE_MEMBERS);
        }
        if (!qualityEvidenceMatches(journal, requested)) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.EVIDENCE_CONFLICT);
        }
        if ((!recovered && owner != null)
                || (recovered && (owner == null
                || !recoveryToken.equals(owner.token)))) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.RECOVERY_OWNED);
        }
        return QualityGateDecisionResult.recorded(requested);
    }

    private static boolean qualityDecisionMatchesRoot(
            StageAssignmentManifestCodec.QualityRootSnapshot root,
            QualityGateDecision decision) {
        return root.fence().equals(decision.fence())
                && root.artifactId().equals(decision.artifactId())
                && MessageDigest.isEqual(
                root.qualityGateBinding().policyDigest(),
                decision.policyDigest());
    }

    private static boolean qualityEvidenceMatches(
            QualityJournalView journal, QualityGateDecision decision) {
        if (!qualityDecisionMatchesRoot(journal.root, decision)
                || decision.members().size()
                != QualityGateAssignmentBinding.MEMBER_COUNT) {
            return false;
        }
        List<QualityGateAssignmentBinding.MemberBinding> bindings =
                journal.root.qualityGateBinding().members();
        for (int i = 0; i < bindings.size(); i++) {
            QualityGateAssignmentBinding.MemberBinding binding = bindings.get(i);
            ManifestMemberReceipt receipt = journal.members.get(i);
            QualityPreparedMemberEvidence evidence = decision.members().get(i);
            if (receipt == null || !receipt.hasPreparedLocator()
                    || !journal.root.qualityGateBinding().matches(
                    binding.physicalOrdinal(),
                    receipt.preparedLocator().attemptIdentity())
                    || !binding.role().equals(evidence.role())
                    || !receipt.member().name().equals(evidence.memberName())
                    || receipt.member().length() != evidence.dataLength()
                    || !MessageDigest.isEqual(
                    receipt.member().sha256(), evidence.dataHash())
                    || !MessageDigest.isEqual(
                    sha256(PreparedSinkLocatorCodec.encode(
                            receipt.preparedLocator())),
                    evidence.preparedLocatorDigest())) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesPublishableAttempt(RunSnapshot run,
                                                     StageAssignmentManifest manifest) {
        return run != null && run.artifactId().equals(manifest.artifactId())
                && matchesCurrentAttempt(run, manifest.fence())
                && (run.currentAttempt().status() == AttemptStatus.CREATED
                || run.currentAttempt().status() == AttemptStatus.DISPATCHING);
    }

    private static boolean matchesCurrentAttempt(RunSnapshot run, RunAttemptFence fence) {
        if (run == null || run.status().isTerminal()) return false;
        return sameAttemptIdentity(run, fence);
    }

    private static boolean sameAttemptIdentity(
            RunSnapshot run, RunAttemptFence fence) {
        if (run == null) return false;
        AttemptSnapshot current = run.currentAttempt();
        return current != null && run.runId().equals(fence.runId())
                && current.attemptId().equals(fence.attemptId())
                && current.fenceToken() == fence.fenceToken();
    }

    private static boolean sameManifest(StageAssignmentManifest left,
                                        StageAssignmentManifest right) {
        if (!MessageDigest.isEqual(StageAssignmentManifestCodec.encodeRoot(left),
                StageAssignmentManifestCodec.encodeRoot(right))) return false;
        if (left.shardCount() != right.shardCount()) return false;
        for (int i = 0; i < left.shardCount(); i++) {
            if (!MessageDigest.isEqual(StageAssignmentManifestCodec.encodeShard(left, i),
                    StageAssignmentManifestCodec.encodeShard(right, i))) return false;
        }
        return true;
    }

    private StagingVersion readStaging(Layout layout) throws Exception {
        try {
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat)
                    .forPath(layout.assignmentStaging);
            return new StagingVersion(data, stat.getVersion());
        } catch (KeeperException.NoNodeException absent) {
            return null;
        }
    }

    private void appendAssignmentCleanup(Layout layout,
                                         List<CuratorOp> operations) throws Exception {
        if (client.checkExists().forPath(layout.stageAssignment) == null) return;
        AssignmentView assignments = readAssignmentView(layout);
        StagingVersion staging = readStaging(layout);
        if (assignments != null && staging != null) {
            throw corruption(layout.runId, "published assignment retains staging marker");
        }
        if (assignments == null && staging == null) {
            requireNoOrphanAssignmentShards(layout);
            return;
        }
        appendAssignmentShardDeletes(layout, operations);
        Stat intent = client.checkExists().forPath(layout.manifestIntent);
        if (intent == null) throw new KeeperException.NoNodeException();
        operations.add(client.transactionOp().delete().withVersion(intent.getVersion())
                .forPath(layout.manifestIntent));
        if (assignments != null) {
            operations.add(client.transactionOp().delete()
                    .withVersion(assignments.rootVersion)
                    .forPath(layout.assignmentRoot));
        } else {
            operations.add(client.transactionOp().delete().withVersion(staging.version)
                    .forPath(layout.assignmentStaging));
        }
    }

    private void appendAssignmentShardDeletes(Layout layout,
                                              List<CuratorOp> operations) throws Exception {
        List<String> assignments = sortedChildren(layout.assignmentShards, layout.runId);
        List<String> acceptances = sortedChildren(layout.acceptanceShards, layout.runId);
        List<String> terminals = sortedChildren(layout.terminalShards, layout.runId);
        List<String> members = sortedChildren(layout.manifestMemberShards, layout.runId);
        if (assignments.size() != acceptances.size()
                || assignments.size() != terminals.size()
                || assignments.size() != members.size()
                || assignments.size() > StageAssignmentManifest.MAX_SHARDS) {
            throw corruption(layout.runId, "assignment cleanup shard count mismatch");
        }
        for (int i = 0; i < assignments.size(); i++) {
            String expected = shardName(i);
            if (!expected.equals(assignments.get(i))
                    || !expected.equals(acceptances.get(i))
                    || !expected.equals(terminals.get(i))
                    || !expected.equals(members.get(i))) {
                throw corruption(layout.runId, "assignment cleanup found non-canonical shard");
            }
            String acceptancePath = acceptanceShardPath(layout, i);
            String assignmentPath = assignmentShardPath(layout, i);
            String terminalPath = terminalShardPath(layout, i);
            String memberPath = manifestMemberShardPath(layout, i);
            Stat acceptance = client.checkExists().forPath(acceptancePath);
            Stat assignment = client.checkExists().forPath(assignmentPath);
            Stat terminal = client.checkExists().forPath(terminalPath);
            Stat member = client.checkExists().forPath(memberPath);
            if (acceptance == null || assignment == null
                    || terminal == null || member == null) {
                throw new KeeperException.NoNodeException();
            }
            operations.add(client.transactionOp().delete().withVersion(member.getVersion())
                    .forPath(memberPath));
            operations.add(client.transactionOp().delete().withVersion(terminal.getVersion())
                    .forPath(terminalPath));
            operations.add(client.transactionOp().delete().withVersion(acceptance.getVersion())
                    .forPath(acceptancePath));
            operations.add(client.transactionOp().delete().withVersion(assignment.getVersion())
                    .forPath(assignmentPath));
        }
    }

    private void requireNoOrphanAssignmentShards(Layout layout) throws Exception {
        if (client.checkExists().forPath(layout.stageAssignment) == null) return;
        if (!sortedChildren(layout.assignmentShards, layout.runId).isEmpty()
                || !sortedChildren(layout.acceptanceShards, layout.runId).isEmpty()
                || !sortedChildren(layout.terminalShards, layout.runId).isEmpty()
                || !sortedChildren(layout.manifestMemberShards, layout.runId).isEmpty()) {
            throw corruption(layout.runId, "orphan stage assignment shards without root/staging");
        }
    }

    private List<String> sortedChildren(String path, String runId) throws Exception {
        try {
            List<String> children = client.getChildren().forPath(path);
            children.sort(Comparator.naturalOrder());
            return children;
        } catch (KeeperException.NoNodeException missing) {
            throw corruption(runId, "stage assignment shard parent is missing");
        }
    }

    private void verifyShardNames(String path, int count, String runId) throws Exception {
        List<String> names = sortedChildren(path, runId);
        if (names.size() != count) {
            throw corruption(runId, "staged shard count mismatch");
        }
        for (int i = 0; i < count; i++) {
            if (!shardName(i).equals(names.get(i))) {
                throw corruption(runId, "non-canonical staged shard name");
            }
        }
    }

    private static String assignmentShardPath(Layout layout, int shard) {
        return layout.assignmentShards + "/" + shardName(shard);
    }

    private static String acceptanceShardPath(Layout layout, int shard) {
        return layout.acceptanceShards + "/" + shardName(shard);
    }

    private static String terminalShardPath(Layout layout, int shard) {
        return layout.terminalShards + "/" + shardName(shard);
    }

    private static String manifestMemberShardPath(Layout layout, int shard) {
        return layout.manifestMemberShards + "/" + shardName(shard);
    }

    private static String shardName(int shard) {
        if (shard < 0 || shard >= StageAssignmentManifest.MAX_SHARDS) {
            throw new IllegalArgumentException("assignment shard out of bounds");
        }
        return String.format(Locale.ROOT, "%03d", shard);
    }

    private void checkClaimView(AuthorityObservation authority,
                                CatalogEntry catalog, OwnerVersion owner) throws Exception {
        List<CuratorOp> checks = claimChecks(
                authority.layout, authority.snapshot, catalog);
        checks.add(client.transactionOp().check()
                .withVersion(authority.lifecycle.version)
                .forPath(authority.lifecycle.path));
        appendGuardCheck(authority, checks);
        checks.add(client.transactionOp().check().withVersion(owner.version)
                .forPath(authority.layout.recoveryOwner));
        client.transaction().forOperations(checks);
    }

    private void checkRecoveryMutationView(
            AuthorityObservation authority, OwnerVersion owner)
            throws Exception {
        Layout layout = authority.layout;
        SnapshotVersion snapshot = authority.snapshot;
        List<CuratorOp> checks = new ArrayList<>();
        checks.add(client.transactionOp().check().withVersion(snapshot.version)
                .forPath(layout.snapshot));
        if (!snapshot.snapshot.status().isTerminal()) {
            CatalogEntry catalog = readCatalogEntry(layout.runId);
            verifyExistingCatalogOrRace(layout, snapshot, catalog);
            checks.add(client.transactionOp().check().withVersion(catalog.version)
                    .forPath(catalog.path));
        }
        checks.add(client.transactionOp().check()
                .withVersion(authority.lifecycle.version)
                .forPath(authority.lifecycle.path));
        appendGuardCheck(authority, checks);
        checks.add(client.transactionOp().check().withVersion(owner.version)
                .forPath(layout.recoveryOwner));
        confirmResultChecks(layout.runId, checks);
    }

    private List<CuratorOp> claimChecks(Layout layout, SnapshotVersion snapshot,
                                        CatalogEntry catalog) throws Exception {
        if (catalog == null) throw corruption(layout.runId, "non-terminal snapshot lacks catalog entry");
        List<CuratorOp> operations = new ArrayList<>();
        operations.add(client.transactionOp().check().withVersion(snapshot.version)
                .forPath(layout.snapshot));
        operations.add(client.transactionOp().check().withVersion(catalog.version)
                .forPath(catalog.path));
        return operations;
    }

    private List<CuratorOp> claimFenceMutations(
            AuthorityObservation authority, CatalogEntry catalog) throws Exception {
        Layout layout = authority.layout;
        SnapshotVersion snapshot = authority.snapshot;
        if (catalog == null) {
            throw corruption(layout.runId, "non-terminal snapshot lacks catalog entry");
        }
        List<CuratorOp> operations = new ArrayList<>();
        operations.add(client.transactionOp().setData().withVersion(snapshot.version)
                .forPath(layout.snapshot,
                        RunStateCodec.encodeSnapshot(snapshot.snapshot)));
        operations.add(client.transactionOp().setData().withVersion(catalog.version)
                .forPath(catalog.path,
                        RunRecoveryCatalogKernel.encodeRef(catalog.ref)));
        operations.add(client.transactionOp().check()
                .withVersion(authority.lifecycle.version)
                .forPath(authority.lifecycle.path));
        appendGuardCheck(authority, operations);
        return operations;
    }

    private void appendClaimAssignmentFence(Layout layout, AssignmentView assignments,
                                            StagingVersion staging,
                                            List<CuratorOp> operations) throws Exception {
        if (assignments != null) {
            long generation = assignments.snapshot.acceptanceGeneration();
            if (generation == Long.MAX_VALUE) {
                throw new IllegalStateException("assignment acceptance generation exhausted");
            }
            StageAssignmentSnapshot fenced = assignments.snapshot
                    .withAcceptanceGeneration(generation + 1L);
            operations.add(client.transactionOp().setData()
                    .withVersion(assignments.rootVersion)
                    .forPath(layout.assignmentRoot,
                            StageAssignmentManifestCodec.encodeRoot(
                                    fenced.manifest(), fenced.acceptanceGeneration())));
            return;
        }
        if (staging != null) {
            appendAssignmentShardDeletes(layout, operations);
            operations.add(client.transactionOp().delete().withVersion(staging.version)
                    .forPath(layout.assignmentStaging));
            return;
        }
        requireNoOrphanAssignmentShards(layout);
    }

    private OwnerVersion readOwner(Layout layout) throws Exception {
        try {
            Stat stat = new Stat();
            RecoveryToken token = RecoveryOwnershipCodec.decodeToken(
                    client.getData().storingStatIn(stat).forPath(layout.recoveryOwner));
            if (!layout.runId.equals(token.runId())) {
                throw corruption(layout.runId, "recovery owner runId mismatch");
            }
            return new OwnerVersion(token, stat.getVersion());
        } catch (KeeperException.NoNodeException missing) {
            return null;
        }
    }

    private void ensureCatalog() throws Exception {
        if (client.checkExists().forPath(CATALOG_ROOT) == null) {
            Stat runRoot = client.checkExists().forPath(ROOT);
            if (runRoot != null && runRoot.getNumChildren() > 0) {
                throw new IllegalStateException(
                        "existing Run authority requires explicit recovery catalog migration");
            }
        }
        ensurePersistent(CATALOG_ROOT, CATALOG_SCHEMA);
        byte[] schema = client.getData().forPath(CATALOG_ROOT);
        if (!MessageDigest.isEqual(CATALOG_SCHEMA, schema)) {
            throw new IllegalStateException("unsupported ZK recovery catalog schema");
        }
        ensurePersistent(CATALOG_SHARDS, new byte[0]);
    }

    private void ensureLifecycleCatalog() throws Exception {
        if (client.checkExists().forPath(LIFECYCLE_ROOT) == null) {
            Stat runRoot = client.checkExists().forPath(ROOT);
            if (runRoot != null && runRoot.getNumChildren() > 0) {
                throw new IllegalStateException(
                        "existing Run authority requires explicit lifecycle catalog migration");
            }
        }
        ensurePersistent(LIFECYCLE_ROOT, LIFECYCLE_SCHEMA);
        byte[] schema = client.getData().forPath(LIFECYCLE_ROOT);
        if (!MessageDigest.isEqual(LIFECYCLE_SCHEMA, schema)) {
            throw new IllegalStateException("unsupported ZK lifecycle catalog schema");
        }
        ensurePersistent(LIFECYCLE_SHARDS, new byte[0]);
    }

    private void ensureReferenceIndex() throws Exception {
        if (client.checkExists().forPath(REFERENCE_ROOT) == null) {
            Stat runRoot = client.checkExists().forPath(ROOT);
            byte state = runRoot != null && runRoot.getNumChildren() == 0
                    ? REFERENCE_READY : REFERENCE_BACKFILLING;
            try {
                client.transaction().forOperations(Arrays.asList(
                        client.transactionOp().create().forPath(
                                REFERENCE_ROOT, encodeReferenceBackfillState(
                                        ReferenceBackfillState.initial(state))),
                        client.transactionOp().create().forPath(
                                REFERENCE_ARTIFACTS, new byte[0])));
            } catch (KeeperException.NodeExistsException raced) {
                // Another new writer installed the complete schema pair.
            }
        }
        readReferenceBackfillState();
        if (client.checkExists().forPath(REFERENCE_ARTIFACTS) == null) {
            throw new IllegalStateException(
                    "incomplete ZK Artifact reference index");
        }
    }

    private ArtifactBackfillStatus advanceReferenceBackfill(int maxEntries)
            throws Exception {
        int remaining = maxEntries;
        Throwable lastRace = null;
        int consecutiveRaces = 0;
        while (true) {
            ReferenceBackfillState state = readReferenceBackfillState();
            if (state.status == REFERENCE_READY) {
                return ArtifactBackfillStatus.ready();
            }
            ArtifactLifecycleCursor cursor = state.cursor;
            LifecycleShard shard = lifecycleShard(cursor.shard());
            LifecycleShardView view = readStableLifecycleShard(shard, false);
            List<String> children = new ArrayList<>(view.children);
            children.sort(Comparator.naturalOrder());
            String nextChild = null;
            for (String child : children) {
                validateLifecycleChild(shard.number, child);
                if (cursor.afterDigest() != null
                        && child.compareTo(cursor.afterDigest()) <= 0) {
                    continue;
                }
                if (nextChild == null) nextChild = child;
                if (nextChild != null) break;
            }
            try {
                if (nextChild != null && remaining > 0) {
                    BackfillLifecycleEntry lifecycle =
                            readBackfillLifecycleEntry(shard, nextChild);
                    observeEvidenceRead(EvidenceReadPhase
                            .BACKFILL_AFTER_LIFECYCLE_SELECTION);
                    long selectedRootCzxid =
                            bindBackfillSelection(lifecycle);
                    AuthorityObservation authority =
                            observeBackfillAuthority(
                                    lifecycle, selectedRootCzxid);
                    if (authority.rootCzxid < 0L) {
                        throw new CatalogRaceException();
                    }
                    ReferenceMutation mutation;
                    try {
                        mutation = prepareBackfillReference(lifecycle.ref);
                    } catch (IllegalArgumentException
                             | IllegalStateException candidateCorruption) {
                        AuthorityObservation confirmed = observeCoreAuthority(
                                lifecycle.ref.runId(), null,
                                authority.rootCzxid);
                        if (!sameAuthorityView(authority, confirmed)) {
                            throw new CatalogRaceException();
                        }
                        throw candidateCorruption;
                    }
                    if (mutation.capacityExceeded) {
                        AuthorityObservation confirmed = observeCoreAuthority(
                                lifecycle.ref.runId(), null,
                                authority.rootCzxid);
                        if (!sameAuthorityView(authority, confirmed)) {
                            throw new CatalogRaceException();
                        }
                        throw referenceCorruption(
                                lifecycle.ref.artifactId(),
                                "reference shard capacity exhausted during "
                                        + "backfill");
                    }
                    ArtifactLifecycleCursor nextCursor = ArtifactLifecycleCursor.at(
                            ArtifactLifecycleCursor.Kind.REFERENCE_BACKFILL,
                            cursor.cycle(), cursor.shard(), nextChild);
                    if (state.processedEntries == Long.MAX_VALUE) {
                        throw new IllegalStateException(
                                "reference backfill progress overflow");
                    }
                    ReferenceBackfillState next = state.advance(
                            nextCursor, state.processedEntries + 1L);
                    List<CuratorOp> operations = new ArrayList<>();
                    operations.add(client.transactionOp().check()
                            .withVersion(lifecycle.version)
                            .forPath(lifecycle.path));
                    mutation.appendTo(client, operations);
                    operations.add(client.transactionOp().setData()
                            .withVersion(state.version).forPath(
                                    REFERENCE_ROOT,
                                    encodeReferenceBackfillState(next)));
                    appendGuardCheck(authority, operations);
                    observeVerifiedCoreForTest(
                            authority.layout, authority.snapshot);
                    boolean commitAttempted = false;
                    try {
                        observeGuardedMutation(
                                GuardedMutationKind.REFERENCE_BACKFILL_ENTRY,
                                -1, GuardedMutationMoment.BEFORE_COMMIT);
                        AuthorityObservation confirmed = observeCoreAuthority(
                                lifecycle.ref.runId(), null,
                                authority.rootCzxid);
                        if (!sameAuthorityView(authority, confirmed)) {
                            throw new CatalogRaceException();
                        }
                        commitAttempted = true;
                        client.transaction().forOperations(operations);
                        observeTransactionWrite(
                                GuardedMutationKind.REFERENCE_BACKFILL_ENTRY,
                                -1, operations);
                        observeGuardedMutation(
                                GuardedMutationKind.REFERENCE_BACKFILL_ENTRY,
                                -1,
                                GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
                    } catch (KeeperException.ConnectionLossException
                             | KeeperException.OperationTimeoutException
                                     unknown) {
                        if (!commitAttempted) throw unknown;
                        try {
                            confirmMutationAuthority(
                                    lifecycle.ref.runId(),
                                    authority.rootCzxid);
                        } catch (KeeperException.ConnectionLossException
                                 | KeeperException.OperationTimeoutException
                                         confirmationFailure) {
                            throw unavailable(null,
                                    "confirm unknown ZK Artifact reference "
                                            + "backfill outcome",
                                    confirmationFailure);
                        }
                        throw unavailable(null,
                                "reconcile unknown ZK Artifact reference "
                                        + "backfill outcome", unknown);
                    }
                    consecutiveRaces = 0;
                    remaining--;
                    if (remaining == 0) {
                        return ArtifactBackfillStatus.backfilling(
                                next.cursor.encode(), next.processedEntries);
                    }
                    continue;
                }
                if (nextChild != null) {
                    return ArtifactBackfillStatus.backfilling(
                            cursor.encode(), state.processedEntries);
                }
                boolean complete = cursor.shard() == 255;
                ReferenceBackfillState next = complete
                        ? state.ready()
                        : state.advance(cursor.nextShard(), state.processedEntries);
                List<CuratorOp> operations = new ArrayList<>();
                if (view.guardVersion >= 0) {
                    operations.add(client.transactionOp().check()
                            .withVersion(view.guardVersion).forPath(shard.guard));
                }
                operations.add(client.transactionOp().setData()
                        .withVersion(state.version).forPath(
                                REFERENCE_ROOT,
                                encodeReferenceBackfillState(next)));
                client.transaction().forOperations(operations);
                return complete
                        ? ArtifactBackfillStatus.ready()
                        : ArtifactBackfillStatus.backfilling(
                        next.cursor.encode(), next.processedEntries);
            } catch (KeeperException.BadVersionException
                     | KeeperException.NodeExistsException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
                if (++consecutiveRaces >= MAX_RETRIES * 4) break;
            } catch (CatalogRaceException raced) {
                lastRace = raced;
                if (++consecutiveRaces >= MAX_RETRIES * 4) break;
            } catch (RunIncarnationChangedException raced) {
                lastRace = raced;
                if (++consecutiveRaces >= MAX_RETRIES * 4) break;
            }
        }
        throw unavailable(null,
                "ZK Artifact reference backfill did not converge", lastRace);
    }

    private BackfillLifecycleEntry readBackfillLifecycleEntry(
            LifecycleShard shard, String child) throws Exception {
        String path = shard.entries + "/" + child;
        Stat stat = new Stat();
        RunLifecycleRef stored = RunRetentionCodec.decodeRef(
                client.getData().storingStatIn(stat).forPath(path));
        RunLifecycleRef ref = lifecycleWithAuthorityTime(stored, stat.getMtime());
        if (!child.equals(RunLifecycleCatalogKernel.digestHex(ref.runId()))
                || RunLifecycleCatalogKernel.shard(child) != shard.number) {
            throw corruption(ref.runId(),
                    "backfill lifecycle entry identity/hash mismatch");
        }
        return new BackfillLifecycleEntry(
                path, ref, stat.getVersion(), stat.getCzxid());
    }

    private AuthorityObservation observeBackfillAuthority(
            BackfillLifecycleEntry selected, long expectedRootCzxid)
            throws Exception {
        String runId = selected.ref.runId();
        String stableMismatch = null;
        CoreMismatch stableMismatchKind = null;
        int stableMismatchCount = 0;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            AuthorityObservation authority;
            try {
                authority = observeAuthority(runId);
            } catch (CatalogRaceException
                     | KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                stableMismatch = null;
                stableMismatchKind = null;
                stableMismatchCount = 0;
                continue;
            }
            if (expectedRootCzxid >= 0L
                    && authority.rootCzxid != expectedRootCzxid) {
                throw new RunIncarnationChangedException();
            }
            CoreMismatch mismatch = coreMismatch(authority);
            if (mismatch != null) {
                String key = authorityMismatchKey(authority, mismatch);
                if (key.equals(stableMismatch)) stableMismatchCount++;
                else { stableMismatch = key; stableMismatchCount = 1; }
                stableMismatchKind = mismatch;
                if (stableMismatchCount >= MAX_RETRIES) {
                    throw corruption(runId, mismatch.detail);
                }
                continue;
            }
            if (authority.rootCzxid < 0L) return authority;
            verifyExistingLifecycleOrRace(authority.layout,
                    authority.snapshot, authority.lifecycle);
            if (!selected.sameEntry(authority.lifecycle)) {
                throw new CatalogRaceException();
            }
            AuthorityObservation confirmed = observeAuthority(runId);
            if (expectedRootCzxid >= 0L
                    && confirmed.rootCzxid != expectedRootCzxid) {
                throw new RunIncarnationChangedException();
            }
            if (sameAuthorityView(authority, confirmed)) return authority;
        }
        if (stableMismatchKind != null) {
            throw corruption(runId, stableMismatchKind.detail);
        }
        throw new CatalogRaceException();
    }

    private long bindBackfillSelection(BackfillLifecycleEntry selected)
            throws Exception {
        Layout layout = layout(selected.ref.runId());
        Stat root = client.checkExists().forPath(layout.run);
        LifecycleEntry confirmed = readLifecycleEntry(selected.ref.runId());
        if (root == null || !selected.sameEntry(confirmed)) {
            throw new CatalogRaceException();
        }
        return root.getCzxid();
    }

    private ReferenceMutation prepareBackfillReference(RunLifecycleRef lifecycle)
            throws Exception {
        if (!ArtifactReferenceRef.isContentAddressed(lifecycle.artifactId())) {
            return ReferenceMutation.none();
        }
        ArtifactReferenceRef intended = new ArtifactReferenceRef(
                lifecycle.runId(), lifecycle.artifactId(), lifecycle.revision());
        ReferenceCountVersion root = readReferenceRoot(lifecycle.artifactId());
        ReferenceShard shard = referenceShard(
                lifecycle.artifactId(), lifecycle.runId());
        ReferenceShardCountVersion shardCount = root == null
                ? null : readReferenceShard(shard);
        ReferenceEntry existing = shardCount == null
                ? null : readReferenceEntry(shard, lifecycle.runId());
        if (existing != null) {
            if (!intended.equals(existing.ref)) {
                throw referenceCorruption(lifecycle.artifactId(),
                        "backfill found a conflicting exact reference");
            }
            return ReferenceMutation.none();
        }
        return prepareReferenceInsert(root, shardCount, shard, intended, 4096);
    }

    private ReferenceMutation prepareReferenceMutation(
            SnapshotVersion current, RunSnapshot next) throws Exception {
        if (!ArtifactReferenceRef.isContentAddressed(next.artifactId())) {
            return ReferenceMutation.none();
        }
        ReferenceCountVersion root = readReferenceRoot(next.artifactId());
        ReferenceShard shard = referenceShard(next.artifactId(), next.runId());
        ReferenceShardCountVersion shardCount = root == null
                ? null : readReferenceShard(shard);
        ReferenceEntry existing = shardCount == null
                ? null : readReferenceEntry(shard, next.runId());
        if (current != null) {
            if (existing == null) {
                ReferenceBackfillState state = readReferenceBackfillState();
                if (state.status != REFERENCE_BACKFILLING) {
                    throw referenceCorruption(next.artifactId(),
                            "Run snapshot lacks exact Artifact reference");
                }
                return prepareReferenceInsert(root, shardCount, shard,
                        reference(next), 4096);
            }
            try {
                requireExactReference(existing, current.snapshot);
            } catch (IllegalStateException inconsistent) {
                if (snapshotVersionChanged(
                        layout(current.snapshot.runId()), current.version)) {
                    throw new CatalogRaceException();
                }
                throw inconsistent;
            }
            return ReferenceMutation.update(existing.path, existing.version,
                    ArtifactLifecycleCodec.encodeReference(
                            reference(next)));
        }
        if (existing != null) {
            throw referenceCorruption(next.artifactId(),
                    "reference exists without Run snapshot");
        }
        return prepareReferenceInsert(root, shardCount, shard,
                reference(next), maxArtifactReferencesPerShard);
    }

    private ReferenceMutation prepareReferenceInsert(
            ReferenceCountVersion root,
            ReferenceShardCountVersion shardCount,
            ReferenceShard shard, ArtifactReferenceRef intended,
            int capacity) throws Exception {
        if (root == null) {
            return ReferenceMutation.createArtifact(
                    intended.artifactId(), shard, intended);
        }
        if (root.count == Long.MAX_VALUE) {
            throw referenceCorruption(intended.artifactId(),
                    "reference count overflow");
        }
        if (shardCount == null) {
            return ReferenceMutation.createShard(
                    root, shard, intended);
        }
        verifyReferenceShardChildren(shard, shardCount);
        if (shardCount.count >= capacity) {
            return ReferenceMutation.capacityExceeded();
        }
        return ReferenceMutation.createEntry(
                root, shardCount, shard, intended);
    }

    private void verifyReferenceStable(SnapshotVersion current) throws Exception {
        RunSnapshot snapshot = current.snapshot;
        if (!ArtifactReferenceRef.isContentAddressed(snapshot.artifactId())) return;
        ReferenceShard shard = referenceShard(
                snapshot.artifactId(), snapshot.runId());
        ReferenceCountVersion root = readReferenceRoot(snapshot.artifactId());
        ReferenceShardCountVersion shardCount = root == null
                ? null : readReferenceShard(shard);
        ReferenceEntry entry = shardCount == null
                ? null : readReferenceEntry(shard, snapshot.runId());
        try {
            requireExactReference(entry, snapshot);
        } catch (IllegalStateException inconsistent) {
            Layout layout = layout(snapshot.runId());
            if (snapshotVersionChanged(layout, current.version)) {
                throw new CatalogRaceException();
            }
            throw inconsistent;
        }
    }

    private ReferenceDeletion prepareReferenceDeletion(RunSnapshot snapshot)
            throws Exception {
        if (!ArtifactReferenceRef.isContentAddressed(snapshot.artifactId())) {
            return ReferenceDeletion.none();
        }
        ReferenceCountVersion root = readReferenceRoot(snapshot.artifactId());
        ReferenceShard shard = referenceShard(
                snapshot.artifactId(), snapshot.runId());
        ReferenceShardCountVersion shardCount = root == null
                ? null : readReferenceShard(shard);
        ReferenceEntry entry = shardCount == null
                ? null : readReferenceEntry(shard, snapshot.runId());
        requireExactReference(entry, snapshot);
        verifyReferenceShardChildren(shard, shardCount);
        if (root.count < shardCount.count || root.count <= 0L) {
            throw referenceCorruption(snapshot.artifactId(),
                    "root/shard reference counts disagree");
        }
        return ReferenceDeletion.of(root, shardCount, shard, entry);
    }

    private ReferenceCountVersion readReferenceRoot(String artifactId)
            throws Exception {
        String path = referenceArtifactPath(artifactId);
        try {
            Stat stat = new Stat();
            long count = decodeReferenceCount(
                    client.getData().storingStatIn(stat).forPath(path), artifactId);
            Stat shards = client.checkExists().forPath(path + "/shards");
            if (shards == null || shards.getNumChildren() < 1
                    || shards.getNumChildren() > 256) {
                Stat latest = client.checkExists().forPath(path);
                if (latest == null || latest.getVersion() != stat.getVersion()) {
                    throw new CatalogRaceException();
                }
                throw referenceCorruption(artifactId,
                        "reference shard parent is missing or invalid");
            }
            return new ReferenceCountVersion(path, count, stat.getVersion(),
                    shards.getNumChildren(), shards.getVersion());
        } catch (KeeperException.NoNodeException missing) {
            return null;
        }
    }

    private ReferenceShardCountVersion readReferenceShard(ReferenceShard shard)
            throws Exception {
        try {
            Stat stat = new Stat();
            int count = decodeReferenceShardCount(
                    client.getData().storingStatIn(stat).forPath(shard.path),
                    shard.artifactId, shard.number);
            Stat entries = client.checkExists().forPath(shard.entries);
            if (entries == null || entries.getNumChildren() != count
                    || entries.getNumChildren() > 4096) {
                Stat latest = client.checkExists().forPath(shard.path);
                if (latest == null || latest.getVersion() != stat.getVersion()) {
                    throw new CatalogRaceException();
                }
                throw referenceCorruption(shard.artifactId,
                        "reference shard child count mismatch");
            }
            return new ReferenceShardCountVersion(
                    shard.path, count, stat.getVersion(), entries.getVersion());
        } catch (KeeperException.NoNodeException missing) {
            return null;
        }
    }

    private ReferenceEntry readReferenceEntry(ReferenceShard shard, String runId)
            throws Exception {
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        String path = shard.entries + "/" + digest;
        try {
            Stat stat = new Stat();
            ArtifactReferenceRef ref = ArtifactLifecycleCodec.decodeReference(
                    client.getData().storingStatIn(stat).forPath(path));
            if (!runId.equals(ref.runId())
                    || !shard.artifactId.equals(ref.artifactId())
                    || !digest.equals(
                    RunLifecycleCatalogKernel.digestHex(ref.runId()))) {
                throw referenceCorruption(shard.artifactId,
                        "reference entry identity/hash mismatch");
            }
            return new ReferenceEntry(path, ref, stat.getVersion());
        } catch (KeeperException.NoNodeException missing) {
            return null;
        }
    }

    private void verifyReferenceShardChildren(
            ReferenceShard shard, ReferenceShardCountVersion count)
            throws Exception {
        Stat entries = client.checkExists().forPath(shard.entries);
        if (entries == null || entries.getNumChildren() != count.count
                || entries.getNumChildren() > 4096) {
            Stat latest = client.checkExists().forPath(shard.path);
            if (latest == null || latest.getVersion() != count.version) {
                throw new CatalogRaceException();
            }
            throw referenceCorruption(shard.artifactId,
                    "reference shard child count mismatch");
        }
    }

    private static void requireExactReference(
            ReferenceEntry entry, RunSnapshot snapshot) {
        if (entry == null
                || !snapshot.runId().equals(entry.ref.runId())
                || !snapshot.artifactId().equals(entry.ref.artifactId())
                || snapshot.revision() != entry.ref.lifecycleRevision()) {
            throw referenceCorruption(snapshot.artifactId(),
                    "Run snapshot lacks exact Artifact reference");
        }
    }

    private void ensureLifecycleShard(LifecycleShard shard) throws Exception {
        if (!lifecycleShardExists(shard)) {
            try {
                client.transaction().forOperations(Arrays.asList(
                        client.transactionOp().create().forPath(
                                shard.guard, longBytes(0L)),
                        client.transactionOp().create().forPath(
                                shard.entries, new byte[0])));
            } catch (KeeperException.NodeExistsException raced) {
                // Another session initialized the complete guard/entries pair.
            }
        }
        if (!lifecycleShardExists(shard)) {
            throw new IllegalStateException("incomplete lifecycle shard: " + shard.number);
        }
        verifyLifecycleGuard(client.getData().forPath(shard.guard), shard.number);
    }

    private LifecycleMutation prepareLifecycleMutation(
            Layout layout, SnapshotVersion current, RunSnapshot next) throws Exception {
        LifecycleEntry existing = readLifecycleEntry(layout.runId);
        if (current == null) {
            if (existing != null) {
                throw corruption(layout.runId, "lifecycle entry exists without snapshot");
            }
            LifecycleShard shard = lifecycleShardForRun(layout.runId);
            ensureLifecycleShard(shard);
            LifecycleShardView view = readStableLifecycleShard(shard, true);
            if (view.children.size() >= maxLifecycleEntries) {
                return LifecycleMutation.capacityExceeded();
            }
            return LifecycleMutation.create(shard, view,
                    RunLifecycleCatalogKernel.digestHex(layout.runId),
                    RunRetentionCodec.encodeRef(storedLifecycle(next)));
        }
        verifyExistingLifecycleOrRace(layout, current, existing);
        return LifecycleMutation.update(existing.path, existing.version,
                RunRetentionCodec.encodeRef(storedLifecycle(next)));
    }

    private void verifyLifecycleStable(Layout layout, SnapshotVersion current)
            throws Exception {
        LifecycleEntry existing = readLifecycleEntry(layout.runId);
        verifyExistingLifecycleOrRace(layout, current, existing);
    }

    private void verifyExistingLifecycleOrRace(
            Layout layout, SnapshotVersion current, LifecycleEntry existing)
            throws Exception {
        RunSnapshot snapshot = current.snapshot;
        boolean expectedState = snapshot.status().isTerminal()
                ? existing != null && (existing.ref.state() == RunLifecycleState.TERMINAL
                || existing.ref.state() == RunLifecycleState.DELETING)
                : existing != null && existing.ref.state() == RunLifecycleState.ACTIVE;
        if (existing != null
                && existing.ref.runId().equals(layout.runId)
                && existing.ref.revision() == snapshot.revision()
                && existing.ref.status() == snapshot.status()
                && existing.ref.artifactId().equals(snapshot.artifactId())
                && expectedState) {
            return;
        }
        if (snapshotVersionChanged(layout, current.version)) {
            throw new CatalogRaceException();
        }
        if (existing == null) {
            throw corruption(layout.runId, "snapshot lacks lifecycle entry");
        }
        throw corruption(layout.runId, "snapshot/lifecycle revision or status mismatch");
    }

    private LifecycleEntry readLifecycleEntry(String runId) throws Exception {
        LifecycleShard shard = lifecycleShardForRun(runId);
        if (!lifecycleShardExists(shard)) return null;
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        String path = shard.entries + "/" + digest;
        try {
            Stat stat = new Stat();
            RunLifecycleRef stored = RunRetentionCodec.decodeRef(
                    client.getData().storingStatIn(stat).forPath(path));
            RunLifecycleRef ref = lifecycleWithAuthorityTime(stored, stat.getMtime());
            if (!runId.equals(ref.runId())
                    || !digest.equals(RunLifecycleCatalogKernel.digestHex(ref.runId()))) {
                throw corruption(runId, "lifecycle entry identity/hash mismatch");
            }
            return new LifecycleEntry(
                    path, ref, stat.getVersion(), stat.getCzxid());
        } catch (KeeperException.NoNodeException missing) {
            return null;
        }
    }

    private LifecycleShardView readStableLifecycleShard(
            LifecycleShard shard, boolean required) throws Exception {
        if (!lifecycleShardExists(shard)) {
            if (required) throw new IllegalStateException("lifecycle shard is missing");
            return new LifecycleShardView(-1, 0L, Collections.emptyList());
        }
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            Stat before = new Stat();
            byte[] guardBefore = client.getData().storingStatIn(before)
                    .forPath(shard.guard);
            long generation = verifyLifecycleGuard(guardBefore, shard.number);
            List<String> children = client.getChildren().forPath(shard.entries);
            if (children.size() > maxLifecycleEntries) {
                throw new IllegalStateException(
                        "lifecycle shard exceeds hard capacity: " + shard.number);
            }
            Stat after = new Stat();
            byte[] guardAfter = client.getData().storingStatIn(after)
                    .forPath(shard.guard);
            long afterGeneration = verifyLifecycleGuard(guardAfter, shard.number);
            if (before.getVersion() == after.getVersion()
                    && generation == afterGeneration) {
                return new LifecycleShardView(before.getVersion(), generation, children);
            }
        }
        throw new CatalogRaceException();
    }

    private boolean lifecycleShardExists(LifecycleShard shard) throws Exception {
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            boolean guard = client.checkExists().forPath(shard.guard) != null;
            boolean entries = client.checkExists().forPath(shard.entries) != null;
            if (guard == entries) return guard;
        }
        throw new IllegalStateException("incomplete lifecycle shard: " + shard.number);
    }

    private static RunLifecycleRef storedLifecycle(RunSnapshot snapshot) {
        if (snapshot.status().isTerminal()) {
            // ZK server mtime of this setData is the authority time; 1 is an on-wire sentinel only.
            return RunLifecycleRef.terminal(snapshot.runId(), snapshot.revision(),
                    snapshot.status(), snapshot.artifactId(), 1L);
        }
        return RunLifecycleRef.active(snapshot.runId(), snapshot.revision(),
                snapshot.status(), snapshot.artifactId());
    }

    private static RunLifecycleRef lifecycleWithAuthorityTime(
            RunLifecycleRef stored, long serverMtime) {
        if (stored.state() == RunLifecycleState.ACTIVE) return stored;
        if (stored.state() == RunLifecycleState.TERMINAL) {
            if (stored.terminalAtMillis() != 1L || serverMtime <= 0L) {
                throw new IllegalStateException("invalid ZK lifecycle authority time");
            }
            return RunLifecycleRef.terminal(stored.runId(), stored.revision(),
                    stored.status(), stored.artifactId(), serverMtime);
        }
        return stored;
    }

    private static LifecycleShard lifecycleShardForRun(String runId) {
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        return lifecycleShard(RunLifecycleCatalogKernel.shard(digest));
    }

    private static LifecycleShard lifecycleShard(int number) {
        if (number < 0 || number >= RunLifecycleCatalogKernel.SHARD_COUNT) {
            throw new IllegalArgumentException("lifecycle shard out of bounds");
        }
        String name = String.format(Locale.ROOT, "%02x", number);
        String guard = LIFECYCLE_SHARDS + "/" + name;
        return new LifecycleShard(number, guard, guard + "/entries");
    }

    private static void validateLifecycleChild(int shard, String child) {
        try {
            if (RunLifecycleCatalogKernel.shard(child) != shard) {
                throw new IllegalStateException("lifecycle child in wrong shard");
            }
        } catch (IllegalArgumentException malformed) {
            throw new IllegalStateException("invalid lifecycle child name", malformed);
        }
    }

    private static long verifyLifecycleGuard(byte[] encoded, int shard) {
        if (encoded == null || encoded.length != 8) {
            throw new IllegalStateException("invalid lifecycle shard guard: " + shard);
        }
        long generation = ByteBuffer.wrap(encoded).getLong();
        if (generation < 0L) {
            throw new IllegalStateException("lifecycle shard guard overflow: " + shard);
        }
        return generation;
    }

    /** O(1) lifecycle-incarnation fence read; never enumerates shard entries. */
    private LifecycleGuardVersion readLifecycleGuard(LifecycleShard shard)
            throws Exception {
        Stat guardPresence = client.checkExists().forPath(shard.guard);
        Stat entriesPresence = client.checkExists().forPath(shard.entries);
        if ((guardPresence == null) != (entriesPresence == null)) {
            throw new LifecycleGuardShapeException(
                    guardPresence != null, entriesPresence != null);
        }
        if (guardPresence == null) return LifecycleGuardVersion.absent();
        Stat stat = new Stat();
        byte[] encoded;
        try {
            encoded = client.getData().storingStatIn(stat).forPath(shard.guard);
        } catch (KeeperException.NoNodeException raced) {
            throw new CatalogRaceException();
        }
        if (client.checkExists().forPath(shard.entries) == null) {
            throw new LifecycleGuardShapeException(true, false);
        }
        return LifecycleGuardVersion.present(
                stat.getVersion(), verifyLifecycleGuard(encoded, shard.number));
    }

    private void ensureCatalogShard(CatalogShard shard) throws Exception {
        if (!catalogShardExists(shard)) {
            try {
                client.transaction().forOperations(Arrays.asList(
                        client.transactionOp().create().forPath(shard.guard, longBytes(0L)),
                        client.transactionOp().create().forPath(shard.entries, new byte[0])));
            } catch (KeeperException.NodeExistsException raced) {
                // Another session initialized this logical shard; verify the complete pair below.
            }
        }
        if (!catalogShardExists(shard)) {
            throw new IllegalStateException("incomplete recoverable run shard: " + shard.number);
        }
        verifyGuard(client.getData().forPath(shard.guard), shard.number);
    }

    private CatalogMutation prepareCatalogMutation(Layout layout, SnapshotVersion current,
                                                   RunSnapshot next) throws Exception {
        CatalogEntry existing = readCatalogEntry(layout.runId);
        if (current == null) {
            if (existing != null) {
                throw corruption(layout.runId, "catalog entry exists without snapshot");
            }
            CatalogShard shard = catalogShardForRun(layout.runId);
            ensureCatalogShard(shard);
            ShardView view = readStableShard(shard, true);
            if (view.children.size() >= maxCatalogEntries) {
                return CatalogMutation.capacityExceeded();
            }
            return CatalogMutation.create(shard, view,
                    RunRecoveryCatalogKernel.digestHex(layout.runId),
                    RunRecoveryCatalogKernel.encodeRef(ref(next)));
        }
        verifyExistingCatalogOrRace(layout, current, existing);
        if (next.status().isTerminal()) {
            CatalogShard shard = catalogShardForRun(layout.runId);
            ShardView view = readStableShard(shard, true);
            String digest = RunRecoveryCatalogKernel.digestHex(layout.runId);
            if (!view.children.contains(digest)) {
                if (snapshotVersionChanged(layout, current.version)) throw new CatalogRaceException();
                throw corruption(layout.runId, "catalog entry is absent from shard");
            }
            return CatalogMutation.delete(shard, view, digest, existing.version);
        }
        return CatalogMutation.update(existing.path, existing.version,
                RunRecoveryCatalogKernel.encodeRef(ref(next)));
    }

    private void verifyCatalogStable(Layout layout, SnapshotVersion current) throws Exception {
        CatalogEntry existing = readCatalogEntry(layout.runId);
        if (current.snapshot.status().isTerminal()) {
            if (existing == null) return;
            if (snapshotVersionChanged(layout, current.version)) throw new CatalogRaceException();
            throw corruption(layout.runId, "terminal snapshot retains catalog entry");
        }
        verifyExistingCatalogOrRace(layout, current, existing);
    }

    private void verifyObservedCatalogStable(
            Layout layout, SnapshotVersion current, CatalogEntry existing) {
        if (current.snapshot.status().isTerminal()) {
            if (existing == null) return;
            throw corruption(layout.runId,
                    "terminal snapshot retains catalog entry");
        }
        if (existing != null
                && existing.ref.runId().equals(layout.runId)
                && existing.ref.revision() == current.snapshot.revision()
                && existing.ref.status() == current.snapshot.status()) {
            return;
        }
        if (existing == null) {
            throw corruption(layout.runId,
                    "non-terminal snapshot lacks catalog entry");
        }
        throw corruption(layout.runId,
                "snapshot/catalog revision or status mismatch");
    }

    private void verifyExistingCatalogOrRace(Layout layout, SnapshotVersion current,
                                             CatalogEntry existing) throws Exception {
        if (existing != null
                && existing.ref.runId().equals(layout.runId)
                && existing.ref.revision() == current.snapshot.revision()
                && existing.ref.status() == current.snapshot.status()) {
            return;
        }
        if (snapshotVersionChanged(layout, current.version)) throw new CatalogRaceException();
        if (existing == null) throw corruption(layout.runId, "non-terminal snapshot lacks catalog entry");
        throw corruption(layout.runId, "snapshot/catalog revision or status mismatch");
    }

    private boolean snapshotVersionChanged(Layout layout, int expectedVersion) throws Exception {
        Stat stat = client.checkExists().forPath(layout.snapshot);
        return stat == null || stat.getVersion() != expectedVersion;
    }

    private CatalogEntry readCatalogEntry(String runId) throws Exception {
        CatalogShard shard = catalogShardForRun(runId);
        if (!catalogShardExists(shard)) return null;
        String digest = RunRecoveryCatalogKernel.digestHex(runId);
        String path = shard.entries + "/" + digest;
        try {
            Stat stat = new Stat();
            RecoverableRunRef ref = RunRecoveryCatalogKernel.decodeRef(
                    client.getData().storingStatIn(stat).forPath(path));
            if (!runId.equals(ref.runId())
                    || !digest.equals(RunRecoveryCatalogKernel.digestHex(ref.runId()))) {
                throw corruption(runId, "catalog entry identity/hash mismatch");
            }
            return new CatalogEntry(path, ref, stat.getVersion());
        } catch (KeeperException.NoNodeException missing) {
            return null;
        }
    }

    private ShardView readStableShard(CatalogShard shard, boolean required) throws Exception {
        if (!catalogShardExists(shard)) {
            if (required) throw new IllegalStateException("recoverable run shard is missing");
            return new ShardView(-1, 0L, Collections.emptyList());
        }
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            Stat before = new Stat();
            byte[] guardBefore = client.getData().storingStatIn(before).forPath(shard.guard);
            long generation = verifyGuard(guardBefore, shard.number);
            List<String> children = client.getChildren().forPath(shard.entries);
            if (children.size() > maxCatalogEntries) {
                throw new IllegalStateException("recoverable run shard exceeds hard capacity: "
                        + shard.number);
            }
            Stat after = new Stat();
            byte[] guardAfter = client.getData().storingStatIn(after).forPath(shard.guard);
            long afterGeneration = verifyGuard(guardAfter, shard.number);
            if (before.getVersion() == after.getVersion() && generation == afterGeneration) {
                return new ShardView(before.getVersion(), generation, children);
            }
        }
        throw new CatalogRaceException();
    }

    private boolean catalogShardExists(CatalogShard shard) throws Exception {
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            boolean guard = client.checkExists().forPath(shard.guard) != null;
            boolean entries = client.checkExists().forPath(shard.entries) != null;
            if (guard == entries) return guard;
            // Two reads can straddle the atomic lazy-init multi; retry before declaring corruption.
        }
        throw new IllegalStateException("incomplete recoverable run shard: " + shard.number);
    }

    private static RecoverableRunRef ref(RunSnapshot snapshot) {
        return new RecoverableRunRef(snapshot.runId(), snapshot.revision(), snapshot.status());
    }

    private static CatalogShard catalogShardForRun(String runId) {
        String digest = RunRecoveryCatalogKernel.digestHex(runId);
        return catalogShard(RunRecoveryCatalogKernel.shard(digest));
    }

    private static CatalogShard catalogShard(int number) {
        if (number < 0 || number >= RunRecoveryCatalogKernel.SHARD_COUNT) {
            throw new IllegalArgumentException("catalog shard out of bounds");
        }
        String name = String.format(Locale.ROOT, "%02x", number);
        String guard = CATALOG_SHARDS + "/" + name;
        return new CatalogShard(number, guard, guard + "/entries");
    }

    private static void validateCatalogChild(int shard, String child) {
        try {
            if (RunRecoveryCatalogKernel.shard(child) != shard) {
                throw new IllegalStateException("recoverable run child in wrong shard");
            }
        } catch (IllegalArgumentException malformed) {
            throw new IllegalStateException("invalid recoverable run child name", malformed);
        }
    }

    private static long verifyGuard(byte[] encoded, int shard) {
        if (encoded == null || encoded.length != 8) {
            throw new IllegalStateException("invalid recoverable run shard guard: " + shard);
        }
        long generation = ByteBuffer.wrap(encoded).getLong();
        if (generation < 0L) {
            throw new IllegalStateException("recoverable run shard guard overflow: " + shard);
        }
        return generation;
    }

    private ReferenceBackfillState readReferenceBackfillState() throws Exception {
        Stat stat = new Stat();
        ReferenceBackfillState decoded = decodeReferenceBackfillState(
                client.getData().storingStatIn(stat).forPath(REFERENCE_ROOT));
        return decoded.withVersion(stat.getVersion());
    }

    private static byte[] encodeReferenceBackfillState(
            ReferenceBackfillState state) {
        if (state == null) {
            throw new IllegalArgumentException("reference backfill state is required");
        }
        byte[] cursor = state.cursor == null ? new byte[0]
                : state.cursor.encode().getBytes(StandardCharsets.US_ASCII);
        if (cursor.length > MAX_REFERENCE_CURSOR_BYTES) {
            throw new IllegalStateException("reference backfill cursor is too large");
        }
        ByteBuffer body = ByteBuffer.allocate(4 + 1 + 1 + 8 + 2 + cursor.length);
        body.putInt(REFERENCE_STATE_MAGIC);
        body.put(REFERENCE_SCHEMA_VERSION);
        body.put(state.status);
        body.putLong(state.processedEntries);
        body.putShort((short) cursor.length);
        body.put(cursor);
        return appendDigest(body.array());
    }

    private static ReferenceBackfillState decodeReferenceBackfillState(
            byte[] encoded) {
        int minimum = 4 + 1 + 1 + 8 + 2 + 32;
        int maximum = minimum + MAX_REFERENCE_CURSOR_BYTES;
        if (encoded == null || encoded.length < minimum
                || encoded.length > maximum) {
            throw new IllegalStateException(
                    "invalid ZK Artifact reference backfill envelope");
        }
        int bodyLength = encoded.length - 32;
        byte[] body = Arrays.copyOf(encoded, bodyLength);
        byte[] checksum = Arrays.copyOfRange(encoded, bodyLength, encoded.length);
        if (!MessageDigest.isEqual(checksum, sha256(body))) {
            throw new IllegalStateException(
                    "ZK Artifact reference backfill checksum mismatch");
        }
        ByteBuffer in = ByteBuffer.wrap(body);
        if (in.getInt() != REFERENCE_STATE_MAGIC
                || in.get() != REFERENCE_SCHEMA_VERSION) {
            throw new IllegalStateException(
                    "unsupported ZK Artifact reference schema");
        }
        byte status = in.get();
        long processed = in.getLong();
        int cursorLength = in.getShort() & 0xffff;
        if ((status != REFERENCE_READY && status != REFERENCE_BACKFILLING)
                || processed < 0L || cursorLength > MAX_REFERENCE_CURSOR_BYTES
                || in.remaining() != cursorLength) {
            throw new IllegalStateException(
                    "invalid ZK Artifact reference backfill fields");
        }
        ArtifactLifecycleCursor cursor = null;
        if (cursorLength > 0) {
            byte[] cursorBytes = new byte[cursorLength];
            in.get(cursorBytes);
            cursor = ArtifactLifecycleCursor.decode(
                    ArtifactLifecycleCursor.Kind.REFERENCE_BACKFILL,
                    new String(cursorBytes, StandardCharsets.US_ASCII));
        }
        if ((status == REFERENCE_READY) != (cursor == null)) {
            throw new IllegalStateException(
                    "reference READY/cursor state mismatch");
        }
        return new ReferenceBackfillState(
                status, cursor, processed, -1);
    }

    private static ArtifactReferenceRef reference(RunSnapshot snapshot) {
        return new ArtifactReferenceRef(snapshot.runId(), snapshot.artifactId(),
                snapshot.revision());
    }

    private static String referenceArtifactPath(String artifactId) {
        return REFERENCE_ARTIFACTS + "/"
                + ArtifactReferenceRef.requireContentAddressed(artifactId);
    }

    private static ReferenceShard referenceShard(String artifactId, String runId) {
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        int number = RunLifecycleCatalogKernel.shard(digest);
        String artifact = referenceArtifactPath(artifactId);
        String path = artifact + "/shards/"
                + String.format(Locale.ROOT, "%02x", number);
        return new ReferenceShard(artifactId, number, path, path + "/entries");
    }

    private static byte[] encodeReferenceCount(String artifactId, long count) {
        if (count <= 0L || count > 256L * 4096L) {
            throw referenceCorruption(artifactId,
                    "reference count out of bounds");
        }
        ByteBuffer body = ByteBuffer.allocate(REFERENCE_COUNT_BYTES - 32);
        body.putInt(REFERENCE_ROOT_MAGIC);
        body.put(REFERENCE_SCHEMA_VERSION);
        body.put(hexBytes(artifactId));
        body.putLong(count);
        return appendDigest(body.array());
    }

    private static long decodeReferenceCount(byte[] encoded, String artifactId) {
        byte[] body = verifiedReferenceRecord(
                encoded, REFERENCE_COUNT_BYTES, artifactId, "root count");
        ByteBuffer in = ByteBuffer.wrap(body);
        if (in.getInt() != REFERENCE_ROOT_MAGIC
                || in.get() != REFERENCE_SCHEMA_VERSION) {
            throw referenceCorruption(artifactId,
                    "unsupported reference root count schema");
        }
        byte[] identity = new byte[32];
        in.get(identity);
        if (!MessageDigest.isEqual(identity, hexBytes(artifactId))) {
            throw referenceCorruption(artifactId,
                    "reference root identity mismatch");
        }
        long count = in.getLong();
        if (count <= 0L || count > 256L * 4096L || in.hasRemaining()) {
            throw referenceCorruption(artifactId,
                    "reference root count out of bounds");
        }
        return count;
    }

    private static byte[] encodeReferenceShardCount(
            String artifactId, int shard, int count) {
        if (shard < 0 || shard >= 256 || count < 1 || count > 4096) {
            throw referenceCorruption(artifactId,
                    "reference shard count out of bounds");
        }
        ByteBuffer body = ByteBuffer.allocate(REFERENCE_SHARD_COUNT_BYTES - 32);
        body.putInt(REFERENCE_SHARD_MAGIC);
        body.put(REFERENCE_SCHEMA_VERSION);
        body.put(hexBytes(artifactId));
        body.put((byte) shard);
        body.putInt(count);
        return appendDigest(body.array());
    }

    private static int decodeReferenceShardCount(
            byte[] encoded, String artifactId, int shard) {
        byte[] body = verifiedReferenceRecord(
                encoded, REFERENCE_SHARD_COUNT_BYTES, artifactId, "shard count");
        ByteBuffer in = ByteBuffer.wrap(body);
        if (in.getInt() != REFERENCE_SHARD_MAGIC
                || in.get() != REFERENCE_SCHEMA_VERSION) {
            throw referenceCorruption(artifactId,
                    "unsupported reference shard count schema");
        }
        byte[] identity = new byte[32];
        in.get(identity);
        if (!MessageDigest.isEqual(identity, hexBytes(artifactId))
                || (in.get() & 0xff) != shard) {
            throw referenceCorruption(artifactId,
                    "reference shard identity mismatch");
        }
        int count = in.getInt();
        if (count < 1 || count > 4096 || in.hasRemaining()) {
            throw referenceCorruption(artifactId,
                    "reference shard count out of bounds");
        }
        return count;
    }

    private static byte[] verifiedReferenceRecord(
            byte[] encoded, int expectedBytes, String artifactId, String kind) {
        if (encoded == null || encoded.length != expectedBytes) {
            throw referenceCorruption(artifactId,
                    "invalid reference " + kind + " envelope");
        }
        int bodyLength = encoded.length - 32;
        byte[] body = Arrays.copyOf(encoded, bodyLength);
        byte[] checksum = Arrays.copyOfRange(encoded, bodyLength, encoded.length);
        if (!MessageDigest.isEqual(checksum, sha256(body))) {
            throw referenceCorruption(artifactId,
                    "invalid reference " + kind + " checksum");
        }
        return body;
    }

    private static byte[] appendDigest(byte[] body) {
        byte[] encoded = Arrays.copyOf(body, body.length + 32);
        byte[] digest = sha256(body);
        System.arraycopy(digest, 0, encoded, body.length, digest.length);
        return encoded;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static byte[] hexBytes(String artifactId) {
        String id = ArtifactReferenceRef.requireContentAddressed(artifactId);
        byte[] bytes = new byte[32];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ((Character.digit(id.charAt(i * 2), 16) << 4)
                    | Character.digit(id.charAt(i * 2 + 1), 16));
        }
        return bytes;
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private Layout ensureLayout(String runId) throws Exception {
        Layout layout = layout(runId);
        Stat existing = client.checkExists().forPath(layout.run);
        if (existing == null) {
            List<CuratorOp> operations = Arrays.asList(
                    client.transactionOp().create().forPath(layout.run, new byte[0]),
                    client.transactionOp().create().forPath(layout.identity,
                            runId.getBytes(StandardCharsets.UTF_8)),
                    client.transactionOp().create().forPath(layout.events, new byte[0]),
                    client.transactionOp().create().forPath(layout.commands, new byte[0]),
                    client.transactionOp().create().forPath(layout.sinkAbort, new byte[0]));
            try {
                client.transaction().forOperations(operations);
            } catch (KeeperException.NodeExistsException raced) {
                // Another session initialized the same hash path; verify its complete identity below.
            }
        }
        AuthorityObservation authority = observeAuthority(runId);
        if (authority.rootCzxid < 0L) throw new CatalogRaceException();
        return authority.layout;
    }

    private Layout existingLayout(String runId) {
        requireIdentity(runId, "runId");
        try {
            AuthorityObservation authority = observeAuthority(runId);
            return authority.rootCzxid < 0L ? null : authority.layout;
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            throw corruption;
        } catch (Exception failure) {
            throw unavailable(null, "lookup ZK run layout", failure);
        }
    }

    private AuthorityObservation observeAuthority(String runId)
            throws Exception {
        return observeAuthority(runId, null);
    }

    private AuthorityObservation observeAuthority(
            String runId, EvidenceReadPhase afterSnapshotPhase)
            throws Exception {
        requireIdentity(runId, "runId");
        Layout layout = layout(runId);
        LifecycleShard shard = lifecycleShardForRun(runId);
        Throwable lastRace = null;
        String stableGuardShape = null;
        int stableGuardShapeCount = 0;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                LifecycleGuardVersion before = readLifecycleGuard(shard);
                Stat root = client.checkExists().forPath(layout.run);
                if (root != null) {
                    observeEvidenceRead(EvidenceReadPhase.LAYOUT_AFTER_ROOT_STAT);
                    try {
                        verifyLayout(layout, runId);
                    } catch (KeeperException.NoNodeException raced) {
                        lastRace = raced;
                        if (!authorityBoundaryUnchanged(
                                layout, root.getCzxid(), before)) {
                            continue;
                        }
                        throw corruption(runId, "incomplete run layout");
                    }
                }
                SnapshotVersion snapshot = root == null
                        ? null : readSnapshot(layout, false);
                observeEvidenceRead(
                        EvidenceReadPhase.AUTHORITY_AFTER_SNAPSHOT);
                if (root != null && afterSnapshotPhase != null) {
                    observeEvidenceRead(afterSnapshotPhase);
                }
                LifecycleEntry lifecycle = readLifecycleEntry(runId);
                LifecycleGuardVersion after = readLifecycleGuard(shard);
                Stat confirmedRoot = client.checkExists().forPath(layout.run);
                if (!before.equals(after)
                        || !sameRootIncarnation(root, confirmedRoot)) {
                    if (afterSnapshotPhase != null && root != null
                            && !sameRootIncarnation(root, confirmedRoot)) {
                        throw new RunIncarnationChangedException();
                    }
                    lastRace = new CatalogRaceException();
                    continue;
                }
                if (!before.present
                        && (snapshot != null || lifecycle != null)) {
                    throw corruption(runId,
                            "committed authority exists without lifecycle guard");
                }
                return new AuthorityObservation(
                        layout, root == null ? -1L : root.getCzxid(),
                        before, snapshot, lifecycle);
            } catch (LifecycleGuardShapeException incomplete) {
                lastRace = incomplete;
                String shape = incomplete.guardPresent + ":"
                        + incomplete.entriesPresent;
                if (shape.equals(stableGuardShape)) stableGuardShapeCount++;
                else { stableGuardShape = shape; stableGuardShapeCount = 1; }
                if (stableGuardShapeCount >= MAX_RETRIES) {
                    throw corruption(runId, "incomplete lifecycle shard");
                }
            } catch (CatalogRaceException
                     | KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                lastRace = raced;
                stableGuardShape = null;
                stableGuardShapeCount = 0;
            }
        }
        throw new CatalogRaceException();
    }

    private AuthorityObservation observeCoreAuthority(
            String runId, EvidenceReadPhase afterSnapshotPhase,
            long expectedRootCzxid) throws Exception {
        long validatedRootCzxid = expectedRootCzxid;
        String stableMismatch = null;
        CoreMismatch stableMismatchKind = null;
        int stableMismatchCount = 0;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            AuthorityObservation authority;
            try {
                authority = observeAuthority(runId, afterSnapshotPhase);
            } catch (CatalogRaceException
                     | KeeperException.BadVersionException
                     | KeeperException.NoNodeException raced) {
                stableMismatch = null;
                stableMismatchKind = null;
                stableMismatchCount = 0;
                continue;
            }
            if (validatedRootCzxid >= 0L
                    && authority.rootCzxid != validatedRootCzxid) {
                throw new RunIncarnationChangedException();
            }
            CoreMismatch mismatch = coreMismatch(authority);
            if (mismatch != null) {
                if (mismatch == CoreMismatch.LAYOUT_ONLY
                        && validatedRootCzxid < 0L) {
                    return authority;
                }
                String key = authorityMismatchKey(authority, mismatch);
                if (key.equals(stableMismatch)) stableMismatchCount++;
                else { stableMismatch = key; stableMismatchCount = 1; }
                stableMismatchKind = mismatch;
                if (stableMismatchCount >= MAX_RETRIES) {
                    throw corruption(runId, mismatch.detail);
                }
                continue;
            }
            if (authority.rootCzxid < 0L) return authority;
            verifyExistingLifecycleOrRace(authority.layout,
                    authority.snapshot, authority.lifecycle);
            if (validatedRootCzxid < 0L) {
                validatedRootCzxid = authority.rootCzxid;
            }
            stableMismatch = null;
            stableMismatchKind = null;
            stableMismatchCount = 0;
            observeEvidenceRead(EvidenceReadPhase.CORE_AFTER_VALIDATED_PAIR);
            AuthorityObservation confirmed = observeAuthority(runId);
            if (confirmed.rootCzxid != validatedRootCzxid) {
                throw new RunIncarnationChangedException();
            }
            if (sameAuthorityView(authority, confirmed)) return authority;
        }
        if (stableMismatchKind != null) {
            throw corruption(runId, stableMismatchKind.detail);
        }
        throw new CatalogRaceException();
    }

    private boolean confirmMutationAuthority(
            String runId, long expectedRootCzxid) throws Exception {
        observeEvidenceRead(
                EvidenceReadPhase.CORE_MISMATCH_BEFORE_CONFIRMATION);
        try {
            observeCoreAuthority(runId, null, expectedRootCzxid);
            return true;
        } catch (RunIncarnationChangedException changed) {
            return false;
        }
    }

    private boolean authorityBoundaryUnchanged(
            Layout layout, long rootCzxid, LifecycleGuardVersion guard)
            throws Exception {
        LifecycleGuardVersion confirmedGuard = readLifecycleGuard(
                lifecycleShardForRun(layout.runId));
        Stat confirmedRoot = client.checkExists().forPath(layout.run);
        return guard.equals(confirmedGuard)
                && confirmedRoot != null
                && confirmedRoot.getCzxid() == rootCzxid;
    }

    private boolean authorityCurrent(AuthorityObservation authority)
            throws Exception {
        LifecycleGuardVersion guard = readLifecycleGuard(
                lifecycleShardForRun(authority.layout.runId));
        Stat root = client.checkExists().forPath(authority.layout.run);
        return authority.guard.equals(guard)
                && (authority.rootCzxid < 0L
                        ? root == null
                        : root != null
                        && root.getCzxid() == authority.rootCzxid);
    }

    private static boolean sameRootIncarnation(Stat first, Stat second) {
        return first == null ? second == null
                : second != null && first.getCzxid() == second.getCzxid();
    }

    private void appendGuardCheck(
            AuthorityObservation authority, List<CuratorOp> operations)
            throws Exception {
        if (!authority.guard.present) {
            throw corruption(authority.layout.runId,
                    "mutation authority lacks lifecycle guard");
        }
        operations.add(client.transactionOp().check()
                .withVersion(authority.guard.version)
                .forPath(lifecycleShardForRun(
                        authority.layout.runId).guard));
    }

    private void appendLifecycleAndGuardChecks(
            AuthorityObservation authority, List<CuratorOp> operations)
            throws Exception {
        if (authority.lifecycle == null) {
            throw corruption(authority.layout.runId,
                    "mutation authority lacks lifecycle entry");
        }
        operations.add(client.transactionOp().check()
                .withVersion(authority.lifecycle.version)
                .forPath(authority.lifecycle.path));
        appendGuardCheck(authority, operations);
    }

    private void confirmResultAuthority(AuthorityObservation authority)
            throws Exception {
        confirmResultAuthority(authority, null);
    }

    private void confirmResultAuthority(
            AuthorityObservation authority, OwnerVersion owner)
            throws Exception {
        if (authority == null || authority.snapshot == null
                || authority.lifecycle == null) {
            throw new KeeperException.BadVersionException();
        }
        List<CuratorOp> checks = new ArrayList<>();
        checks.add(client.transactionOp().check()
                .withVersion(authority.snapshot.version)
                .forPath(authority.layout.snapshot));
        checks.add(client.transactionOp().check()
                .withVersion(authority.lifecycle.version)
                .forPath(authority.lifecycle.path));
        appendGuardCheck(authority, checks);
        if (owner != null) {
            checks.add(client.transactionOp().check()
                    .withVersion(owner.version)
                    .forPath(authority.layout.recoveryOwner));
        }
        confirmResultChecks(authority.layout.runId, checks);
    }

    private void confirmResultChecks(
            String runId, List<CuratorOp> checks) throws Exception {
        try {
            observeEvidenceRead(
                    EvidenceReadPhase.RESULT_BEFORE_AUTHORITY_CONFIRMATION);
            client.transaction().forOperations(checks);
        } catch (KeeperException.ConnectionLossException
                 | KeeperException.OperationTimeoutException
                         confirmationFailure) {
            throw new ResultAuthorityConfirmationFailure(
                    runId, confirmationFailure);
        }
    }

    private RuntimeException confirmedPublishCorruption(
            AuthorityObservation candidateAuthority,
            RuntimeException candidateCorruption) throws Exception {
        AuthorityObservation confirmed;
        try {
            confirmed = observeCoreAuthority(
                    candidateAuthority.layout.runId, null,
                    candidateAuthority.rootCzxid);
        } catch (KeeperException.ConnectionLossException
                 | KeeperException.OperationTimeoutException
                         confirmationFailure) {
            throw unavailable(null,
                    "confirm ZK assignment corruption authority",
                    confirmationFailure);
        }
        if (!sameAuthorityView(candidateAuthority, confirmed)) {
            throw new KeeperException.BadVersionException();
        }
        return candidateCorruption;
    }

    private void verifyLayout(Layout layout, String runId) throws Exception {
        byte[] identity = client.getData().forPath(layout.identity);
        if (!MessageDigest.isEqual(identity, runId.getBytes(StandardCharsets.UTF_8))) {
            throw corruption(runId, "run identity hash conflict");
        }
        if (client.checkExists().forPath(layout.events) == null
                || client.checkExists().forPath(layout.commands) == null) {
            throw corruption(runId, "incomplete run layout");
        }
    }

    private SnapshotVersion readSnapshot(Layout layout, boolean required) throws Exception {
        try {
            Stat stat = new Stat();
            RunSnapshot snapshot = RunStateCodec.decodeSnapshot(
                    client.getData().storingStatIn(stat).forPath(layout.snapshot));
            if (!layout.runId.equals(snapshot.runId())) throw corruption(layout.runId, "snapshot runId mismatch");
            return new SnapshotVersion(
                    snapshot, stat.getVersion(), stat.getCzxid());
        } catch (KeeperException.NoNodeException missing) {
            if (required) throw corruption(layout.runId, "snapshot is missing");
            return null;
        }
    }

    private Receipt readReceipt(Layout layout, String commandId) throws Exception {
        try {
            byte[] encoded = client.getData().forPath(receiptPath(layout, commandId));
            ExecutionEvent event = RunStateCodec.decodeEvent(encoded);
            if (!layout.runId.equals(event.runId()) || !commandId.equals(event.commandId())) {
                throw corruption(layout.runId, "command receipt identity mismatch");
            }
            byte[] authoritative = client.getData().forPath(eventPath(layout, event.sequence()));
            if (!MessageDigest.isEqual(encoded, authoritative)) {
                throw corruption(layout.runId, "command receipt/event mismatch");
            }
            return new Receipt(event);
        } catch (KeeperException.NoNodeException missing) {
            return null;
        }
    }

    private void verifyLatestEventAndReceipt(Layout layout, RunSnapshot snapshot) throws Exception {
        try {
            ExecutionEvent event = decodeEvent(layout, sequenceName(snapshot.revision()));
            if (event.sequence() != snapshot.revision()) throw corruption(layout.runId, "latest event mismatch");
            byte[] receipt = client.getData().forPath(receiptPath(layout, event.commandId()));
            byte[] encoded = RunStateCodec.encodeEvent(event);
            if (!MessageDigest.isEqual(receipt, encoded)) {
                throw corruption(layout.runId, "latest command receipt mismatch");
            }
        } catch (KeeperException.NoNodeException missing) {
            throw corruption(layout.runId, "latest event or command receipt is missing");
        }
    }

    private ExecutionEvent decodeEvent(Layout layout, String child) throws Exception {
        ExecutionEvent event = RunStateCodec.decodeEvent(
                client.getData().forPath(layout.events + "/" + child));
        if (!layout.runId.equals(event.runId())) throw corruption(layout.runId, "event runId mismatch");
        return event;
    }

    private void ensurePersistent(String path, byte[] data) throws Exception {
        try {
            client.create().creatingParentsIfNeeded().forPath(path, data);
        } catch (KeeperException.NodeExistsException ignore) {
            // Idempotent global initialization.
        }
    }

    private void ensureDeadlineTimeOracle() throws Exception {
        ensurePersistent(DEADLINE_ROOT, DEADLINE_SCHEMA);
        if (!MessageDigest.isEqual(
                DEADLINE_SCHEMA, client.getData().forPath(DEADLINE_ROOT))) {
            throw new IllegalStateException("unsupported ZK deadline authority schema");
        }
        ensurePersistent(DEADLINE_TIME_ORACLE, DEADLINE_SCHEMA);
        if (!MessageDigest.isEqual(
                DEADLINE_SCHEMA, client.getData().forPath(DEADLINE_TIME_ORACLE))) {
            throw new IllegalStateException("corrupt ZK deadline time oracle");
        }
    }

    private long deadlineAuthorityTime() throws Exception {
        byte[] schema = client.getData().forPath(DEADLINE_TIME_ORACLE);
        if (!MessageDigest.isEqual(DEADLINE_SCHEMA, schema)) {
            throw new IllegalStateException("corrupt ZK deadline time oracle");
        }
        // The oracle is a clock sample, not a lock. Run snapshot CAS decides the winner;
        // version-fencing this shared node would create avoidable cross-Run contention.
        Stat touched = client.setData()
                .forPath(DEADLINE_TIME_ORACLE, DEADLINE_SCHEMA);
        if (touched.getMtime() <= 0L) {
            throw new IllegalStateException(
                    "invalid ZK deadline authority time");
        }
        return touched.getMtime();
    }

    long incrementalAuthorityTime() {
        try {
            return deadlineAuthorityTime();
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable(null,
                    "read incremental ZK authority time", failure);
        }
    }

    private static Layout layout(String runId) {
        requireIdentity(runId, "runId");
        String run = ROOT + "/" + sha256Hex(runId);
        return new Layout(runId, run);
    }

    private static String eventPath(Layout layout, long sequence) {
        return layout.events + "/" + sequenceName(sequence);
    }

    private static String receiptPath(Layout layout, String commandId) {
        requireIdentity(commandId, "commandId");
        return layout.commands + "/" + sha256Hex(commandId);
    }

    private static String sequenceName(long sequence) {
        if (sequence <= 0L) throw new IllegalArgumentException("sequence must be > 0");
        return String.format(Locale.ROOT, "%020d", sequence);
    }

    private static void requireIdentity(String value, String path) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || value.getBytes(StandardCharsets.UTF_8).length > 256) {
            throw new IllegalArgumentException("invalid " + path);
        }
    }

    private static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            char[] out = new char[digest.length * 2];
            char[] alphabet = "0123456789abcdef".toCharArray();
            for (int i = 0; i < digest.length; i++) {
                out[i * 2] = alphabet[(digest[i] >>> 4) & 0xf];
                out[i * 2 + 1] = alphabet[digest[i] & 0xf];
            }
            return new String(out);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static IllegalStateException corruption(String runId, String detail) {
        return new IllegalStateException("corrupt ZK run authority run=" + runId + ": " + detail);
    }

    private static IllegalStateException referenceCorruption(
            String artifactId, String detail) {
        return new IllegalStateException(
                "corrupt ZK Artifact reference authority artifact="
                        + artifactId + ": " + detail);
    }

    private static RunStateStoreUnavailableException unavailable(
            String commandId, String operation, Throwable cause) {
        return new RunStateStoreUnavailableException(commandId, operation + " failed", cause);
    }

    interface CommitHook { void run() throws Exception; }

    /** Package-private performance probe; production constructors install no observer. */
    interface AssignmentWriteObserver {
        void onWrite(int operationCount, int requestPayloadBytes);
    }

    /** Test-only accounting at successful retention purge transaction boundaries. */
    interface PurgeWriteObserver {
        void onWrite(int operationCount, int requestPayloadBytes);
    }

    /** Test-only retry transaction race/operation-count probe. */
    interface RetryWriteObserver {
        void beforeCommit(int operationCount) throws Exception;
    }

    enum EvidenceReadPhase {
        LAYOUT_AFTER_ROOT_STAT,
        AUTHORITY_AFTER_SNAPSHOT,
        BEFORE_INITIAL_SNAPSHOT,
        BEFORE_ASSIGNMENT,
        BEFORE_CONFIRMATION,
        APPLY_AFTER_INITIAL_SNAPSHOT,
        APPLY_AFTER_INITIAL_LIFECYCLE,
        APPLY_AFTER_RECEIPT,
        CANCEL_AFTER_INITIAL_SNAPSHOT,
        EVENTS_AFTER_INITIAL_SNAPSHOT,
        EVENTS_AFTER_INITIAL_LIFECYCLE,
        EVENTS_AFTER_CHILDREN,
        EVIDENCE_AFTER_INITIAL_SNAPSHOT,
        EVIDENCE_BEFORE_INITIAL_MISMATCH_CONFIRMATION,
        EVIDENCE_AFTER_CONFIRMATION_SNAPSHOT,
        DEADLINE_AFTER_INITIAL_SNAPSHOT,
        RETRY_SCHEDULE_AFTER_INITIAL_SNAPSHOT,
        RETRY_READY_AFTER_INITIAL_SNAPSHOT,
        RETRY_CLAIM_AFTER_INITIAL_SNAPSHOT,
        RETRY_SCHEDULE_BEFORE_DEADLINE_EVALUATION,
        RETRY_CLAIM_BEFORE_DEADLINE_EVALUATION,
        RETRY_SCHEDULE_AFTER_DEADLINE_EVALUATION,
        RETRY_CLAIM_AFTER_DEADLINE_EVALUATION,
        RECOVERY_CLAIM_BEFORE_OWNER_CLASSIFICATION,
        RECOVERY_CLAIM_BEFORE_OWNER_CONFIRMATION,
        RECOVERY_CLAIM_RECONCILE_AFTER_AUTHORITY,
        RECOVERY_CLAIM_RECONCILE_BEFORE_CONFIRMATION,
        PURGE_AFTER_LAYOUT_LOOKUP,
        PURGE_FINAL_BEFORE_GUARD_MUTATION,
        PUBLISH_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER,
        PUBLISH_AFTER_STAGING_BEFORE_FINAL_AUTHORITY,
        ACCEPT_AFTER_LAYOUT_BEFORE_REQUIRED_SNAPSHOT,
        TERMINAL_AFTER_LAYOUT_BEFORE_REQUIRED_SNAPSHOT,
        MANIFEST_MEMBER_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER,
        MANIFEST_INTENT_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER,
        QUALITY_DECISION_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER,
        SINK_ABORT_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER,
        BACKFILL_AFTER_LIFECYCLE_SELECTION,
        RESULT_BEFORE_AUTHORITY_CONFIRMATION,
        CORE_AFTER_VALIDATED_PAIR,
        CORE_MISMATCH_BEFORE_CONFIRMATION,
        FINAL_ABSENCE_BEFORE_CONFIRMATION
    }

    /** Test-only snapshot-first ordering and failure-injection probe. */
    interface EvidenceReadObserver {
        void at(EvidenceReadPhase phase) throws Exception;
    }

    enum GuardedMutationKind {
        APPLY_EXISTING,
        CANCEL,
        DEADLINE,
        RETRY_SCHEDULE,
        RETRY_CLAIM,
        RETENTION_CLAIM,
        RECOVERY_CLAIM,
        REFERENCE_BACKFILL_ENTRY,
        PURGE_BATCH,
        PURGE_FINAL,
        PUBLISH_LAYOUT,
        PUBLISH_INTENT_REPAIR,
        PUBLISH_STAGING_MARKER,
        PUBLISH_STAGING_SHARD,
        PUBLISH_ROOT,
        MANIFEST_MEMBER_ABORT_GUARD,
        MANIFEST_INTENT_ABORT_GUARD,
        QUALITY_DECISION_ABORT_GUARD,
        SINK_ABORT_GUARD,
        ACCEPT,
        TERMINAL,
        MANIFEST_MEMBER,
        MANIFEST_INTENT,
        QUALITY_DECISION,
        SINK_ABORT
    }

    enum GuardedMutationMoment {
        BEFORE_COMMIT,
        AFTER_COMMIT_BEFORE_ACK
    }

    /** Test-only exact transaction interleaving probe; production installs none. */
    interface GuardedMutationObserver {
        void at(GuardedMutationKind kind, int ordinal,
                GuardedMutationMoment moment) throws Exception;
    }

    /** Test-only exact operation and serialized multi-request accounting. */
    interface TransactionWriteObserver {
        void onWrite(GuardedMutationKind kind, int ordinal,
                int operationCount, int requestBytes);
    }

    /** Test-only accounting for quality-specific bounded shard reads. */
    interface QualityJournalReadObserver {
        void onRead(int memberShardCount);
    }

    void retryWriteObserverForTest(RetryWriteObserver observer) {
        if (observer == null) throw new IllegalArgumentException("observer is required");
        this.retryWriteObserver = observer;
    }

    void evidenceReadObserverForTest(EvidenceReadObserver observer) {
        if (observer == null) throw new IllegalArgumentException("observer is required");
        this.evidenceReadObserver = observer;
    }

    void guardedMutationObserverForTest(GuardedMutationObserver observer) {
        if (observer == null) throw new IllegalArgumentException("observer is required");
        this.guardedMutationObserver = observer;
    }

    void transactionWriteObserverForTest(TransactionWriteObserver observer) {
        if (observer == null) throw new IllegalArgumentException("observer is required");
        this.transactionWriteObserver = observer;
    }

    void qualityJournalReadObserverForTest(QualityJournalReadObserver observer) {
        if (observer == null) throw new IllegalArgumentException("observer is required");
        this.qualityJournalReadObserver = observer;
    }

    interface TransactionAugment {
        TransactionAugment NONE = (ignoredClient, ignoredOperations) -> { };

        void append(CuratorFramework client, List<CuratorOp> operations)
                throws Exception;
    }

    enum AugmentedRecoveryCheck {
        ACCEPTED,
        STALE_RECOVERY_OWNER,
        RUN_CONFLICT
    }

    interface ManifestMemberTransactionAugment {
        void append(CuratorFramework client, List<CuratorOp> operations,
                    boolean completePreparedClosure) throws Exception;
    }

    private static final class Layout {
        final String runId;
        final String run;
        final String identity;
        final String snapshot;
        final String events;
        final String commands;
        final String retentionPurge;
        final String recoveryOwner;
        final String stageAssignment;
        final String assignmentRoot;
        final String assignmentStaging;
        final String assignmentShards;
        final String acceptanceShards;
        final String terminalShards;
        final String manifestMemberShards;
        final String manifestIntent;
        final String sinkAbort;

        Layout(String runId, String run) {
            this.runId = runId;
            this.run = run;
            this.identity = run + "/identity";
            this.snapshot = run + "/snapshot";
            this.events = run + "/events";
            this.commands = run + "/commands";
            this.retentionPurge = run + "/retention-purge";
            this.recoveryOwner = run + "/recovery-owner";
            this.stageAssignment = run + "/stage-assignment";
            this.assignmentRoot = stageAssignment + "/root";
            this.assignmentStaging = stageAssignment + "/staging";
            this.assignmentShards = stageAssignment + "/assignment-shards";
            this.acceptanceShards = stageAssignment + "/acceptance-shards";
            this.terminalShards = stageAssignment + "/terminal-shards";
            this.manifestMemberShards = stageAssignment + "/manifest-member-shards";
            this.manifestIntent = stageAssignment + "/manifest-intent";
            this.sinkAbort = run + "/sink-abort";
        }
    }

    private static final class SnapshotVersion {
        final RunSnapshot snapshot;
        final int version;
        final long czxid;
        SnapshotVersion(RunSnapshot snapshot, int version, long czxid) {
            this.snapshot = snapshot;
            this.version = version;
            this.czxid = czxid;
        }
    }

    private static final class LifecycleGuardVersion {
        final boolean present;
        final int version;
        final long generation;

        private LifecycleGuardVersion(
                boolean present, int version, long generation) {
            this.present = present;
            this.version = version;
            this.generation = generation;
        }

        static LifecycleGuardVersion absent() {
            return new LifecycleGuardVersion(false, -1, 0L);
        }

        static LifecycleGuardVersion present(int version, long generation) {
            return new LifecycleGuardVersion(true, version, generation);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof LifecycleGuardVersion)) return false;
            LifecycleGuardVersion that = (LifecycleGuardVersion) other;
            return present == that.present && version == that.version
                    && generation == that.generation;
        }

        @Override
        public int hashCode() {
            int result = Boolean.hashCode(present);
            result = 31 * result + version;
            return 31 * result + Long.hashCode(generation);
        }
    }

    private static final class AuthorityObservation {
        final Layout layout;
        final long rootCzxid;
        final LifecycleGuardVersion guard;
        final SnapshotVersion snapshot;
        final LifecycleEntry lifecycle;

        AuthorityObservation(
                Layout layout, long rootCzxid,
                LifecycleGuardVersion guard, SnapshotVersion snapshot,
                LifecycleEntry lifecycle) {
            this.layout = layout;
            this.rootCzxid = rootCzxid;
            this.guard = guard;
            this.snapshot = snapshot;
            this.lifecycle = lifecycle;
        }
    }

    private static final class PurgeCursor {
        final long nextSequence;
        final boolean receiptPhase;

        PurgeCursor(long nextSequence, boolean receiptPhase) {
            this.nextSequence = nextSequence;
            this.receiptPhase = receiptPhase;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof PurgeCursor)) return false;
            PurgeCursor that = (PurgeCursor) other;
            return nextSequence == that.nextSequence
                    && receiptPhase == that.receiptPhase;
        }

        @Override
        public int hashCode() {
            return 31 * Long.hashCode(nextSequence)
                    + Boolean.hashCode(receiptPhase);
        }
    }

    private static final class PurgeCursorVersion {
        final PurgeCursor cursor;
        final int version;

        PurgeCursorVersion(PurgeCursor cursor, int version) {
            this.cursor = cursor;
            this.version = version;
        }
    }

    private static final class ResultAuthorityConfirmationFailure
            extends Exception {
        ResultAuthorityConfirmationFailure(String runId, Throwable cause) {
            super("business result authority confirmation failed run=" + runId,
                    cause);
        }
    }

    private static final class RetryAbortGuardRepairFailure
            extends Exception {
        RetryAbortGuardRepairFailure(String runId, Throwable cause) {
            super("retry abort guard repair failed run=" + runId, cause);
        }
    }

    private static final class Receipt {
        final ExecutionEvent event;
        Receipt(ExecutionEvent event) { this.event = event; }
    }

    private static final class OwnerVersion {
        final RecoveryToken token;
        final int version;

        OwnerVersion(RecoveryToken token, int version) {
            this.token = token;
            this.version = version;
        }
    }

    private static final class StagingVersion {
        final byte[] data;
        final int version;

        StagingVersion(byte[] data, int version) {
            this.data = data.clone();
            this.version = version;
        }
    }

    private static final class AssignmentView {
        final StageAssignmentSnapshot snapshot;
        final int rootVersion;
        final int[] assignmentVersions;
        final int[] acceptanceVersions;
        final int[] terminalVersions;

        AssignmentView(StageAssignmentSnapshot snapshot, int rootVersion,
                       int[] assignmentVersions, int[] acceptanceVersions,
                       int[] terminalVersions) {
            this.snapshot = snapshot;
            this.rootVersion = rootVersion;
            this.assignmentVersions = assignmentVersions.clone();
            this.acceptanceVersions = acceptanceVersions.clone();
            this.terminalVersions = terminalVersions.clone();
        }
    }

    private static final class AssignmentShardView {
        final StageAssignmentManifestCodec.ShardSnapshot snapshot;
        final int rootVersion;
        final int acceptanceVersion;
        final int terminalVersion;

        AssignmentShardView(StageAssignmentManifestCodec.ShardSnapshot snapshot,
                            int rootVersion, int acceptanceVersion,
                            int terminalVersion) {
            this.snapshot = snapshot;
            this.rootVersion = rootVersion;
            this.acceptanceVersion = acceptanceVersion;
            this.terminalVersion = terminalVersion;
        }
    }

    private static final class IntentVersion {
        final byte[] data;
        final int version;

        IntentVersion(byte[] data, int version) {
            this.data = data.clone();
            this.version = version;
        }
    }

    private static final class AbortVersion {
        final BatchSinkAbortRecord record;
        final int version;

        AbortVersion(BatchSinkAbortRecord record, int version) {
            this.record = record;
            this.version = version;
        }
    }

    private static final class ManifestMemberShardView {
        final AssignmentShardView assignments;
        final ManifestCommitJournalCodec.MemberShardSnapshot members;
        final int memberVersion;
        final IntentVersion intent;

        ManifestMemberShardView(
                AssignmentShardView assignments,
                ManifestCommitJournalCodec.MemberShardSnapshot members,
                int memberVersion, IntentVersion intent) {
            this.assignments = assignments;
            this.members = members;
            this.memberVersion = memberVersion;
            this.intent = intent;
        }
    }

    private static final class ManifestJournalView {
        final ManifestCommitJournalSnapshot snapshot;
        final AssignmentView assignments;
        final int[] memberVersions;
        final int intentVersion;

        ManifestJournalView(
                ManifestCommitJournalSnapshot snapshot,
                AssignmentView assignments, int[] memberVersions,
                int intentVersion) {
            this.snapshot = snapshot;
            this.assignments = assignments;
            this.memberVersions = memberVersions.clone();
            this.intentVersion = intentVersion;
        }
    }

    private static final class QualityJournalView {
        final StageAssignmentManifestCodec.QualityRootSnapshot root;
        final long acceptanceGeneration;
        final List<StageAssignmentAcceptance> acceptances;
        final List<StageTerminalReceipt> terminals;
        final List<ManifestMemberReceipt> members;
        final int rootVersion;
        final int[] memberShardIndices;
        final int[] memberVersions;
        final List<byte[]> memberShardDigests;
        final int intentVersion;
        final ManifestCommitJournalCodec.QualitySlotSnapshot slot;

        QualityJournalView(
                StageAssignmentManifestCodec.QualityRootSnapshot root,
                long acceptanceGeneration,
                List<StageAssignmentAcceptance> acceptances,
                List<StageTerminalReceipt> terminals,
                List<ManifestMemberReceipt> members, int rootVersion,
                int[] memberShardIndices, int[] memberVersions,
                List<byte[]> memberShardDigests,
                int intentVersion,
                ManifestCommitJournalCodec.QualitySlotSnapshot slot) {
            this.root = root;
            this.acceptanceGeneration = acceptanceGeneration;
            this.acceptances = new ArrayList<>(acceptances);
            this.terminals = new ArrayList<>(terminals);
            this.members = new ArrayList<>(members);
            this.rootVersion = rootVersion;
            this.memberShardIndices = memberShardIndices.clone();
            this.memberVersions = memberVersions.clone();
            this.memberShardDigests = new ArrayList<>(
                    memberShardDigests.size());
            for (byte[] digest : memberShardDigests) {
                this.memberShardDigests.add(digest.clone());
            }
            this.intentVersion = intentVersion;
            this.slot = slot;
        }
    }

    private static final class CatalogShard {
        final int number;
        final String guard;
        final String entries;

        CatalogShard(int number, String guard, String entries) {
            this.number = number;
            this.guard = guard;
            this.entries = entries;
        }
    }

    private static final class ShardView {
        final int guardVersion;
        final long generation;
        final List<String> children;

        ShardView(int guardVersion, long generation, List<String> children) {
            this.guardVersion = guardVersion;
            this.generation = generation;
            this.children = new ArrayList<>(children);
        }
    }

    private static final class LifecycleShard {
        final int number;
        final String guard;
        final String entries;

        LifecycleShard(int number, String guard, String entries) {
            this.number = number;
            this.guard = guard;
            this.entries = entries;
        }
    }

    private static final class LifecycleShardView {
        final int guardVersion;
        final long generation;
        final List<String> children;

        LifecycleShardView(int guardVersion, long generation, List<String> children) {
            this.guardVersion = guardVersion;
            this.generation = generation;
            this.children = new ArrayList<>(children);
        }
    }

    private static final class LifecycleEntry {
        final String path;
        final RunLifecycleRef ref;
        final int version;
        final long czxid;

        LifecycleEntry(
                String path, RunLifecycleRef ref, int version, long czxid) {
            this.path = path;
            this.ref = ref;
            this.version = version;
            this.czxid = czxid;
        }
    }

    private static final class ReferenceBackfillState {
        final byte status;
        final ArtifactLifecycleCursor cursor;
        final long processedEntries;
        final int version;

        ReferenceBackfillState(byte status, ArtifactLifecycleCursor cursor,
                               long processedEntries, int version) {
            this.status = status;
            this.cursor = cursor;
            this.processedEntries = processedEntries;
            this.version = version;
        }

        static ReferenceBackfillState initial(byte status) {
            if (status == REFERENCE_READY) {
                return new ReferenceBackfillState(
                        status, null, 0L, -1);
            }
            if (status == REFERENCE_BACKFILLING) {
                return new ReferenceBackfillState(status,
                        ArtifactLifecycleCursor.start(
                                ArtifactLifecycleCursor.Kind.REFERENCE_BACKFILL),
                        0L, -1);
            }
            throw new IllegalArgumentException(
                    "invalid initial reference backfill state");
        }

        ReferenceBackfillState withVersion(int nextVersion) {
            return new ReferenceBackfillState(
                    status, cursor, processedEntries, nextVersion);
        }

        ReferenceBackfillState advance(
                ArtifactLifecycleCursor nextCursor, long nextProcessed) {
            if (status != REFERENCE_BACKFILLING || nextCursor == null
                    || nextProcessed < processedEntries) {
                throw new IllegalStateException(
                        "invalid reference backfill advancement");
            }
            return new ReferenceBackfillState(
                    status, nextCursor, nextProcessed, version);
        }

        ReferenceBackfillState ready() {
            if (status != REFERENCE_BACKFILLING) {
                throw new IllegalStateException(
                        "reference backfill is already READY");
            }
            return new ReferenceBackfillState(
                    REFERENCE_READY, null, processedEntries, version);
        }
    }

    private static final class BackfillLifecycleEntry {
        final String path;
        final RunLifecycleRef ref;
        final int version;
        final long czxid;

        BackfillLifecycleEntry(
                String path, RunLifecycleRef ref, int version, long czxid) {
            this.path = path;
            this.ref = ref;
            this.version = version;
            this.czxid = czxid;
        }

        boolean sameEntry(LifecycleEntry current) {
            return current != null
                    && path.equals(current.path)
                    && version == current.version
                    && czxid == current.czxid
                    && ref.runId().equals(current.ref.runId())
                    && ref.revision() == current.ref.revision()
                    && ref.status() == current.ref.status()
                    && ref.artifactId().equals(current.ref.artifactId())
                    && ref.state() == current.ref.state()
                    && ref.terminalAtMillis()
                            == current.ref.terminalAtMillis()
                    && java.util.Objects.equals(
                            ref.retentionToken(),
                            current.ref.retentionToken());
        }
    }

    private static final class ReferenceCountVersion {
        final String path;
        final long count;
        final int version;
        final int shardChildren;
        final int shardsVersion;

        ReferenceCountVersion(String path, long count, int version,
                              int shardChildren, int shardsVersion) {
            this.path = path;
            this.count = count;
            this.version = version;
            this.shardChildren = shardChildren;
            this.shardsVersion = shardsVersion;
        }
    }

    private static final class ReferenceShard {
        final String artifactId;
        final int number;
        final String path;
        final String entries;

        ReferenceShard(String artifactId, int number,
                       String path, String entries) {
            this.artifactId = artifactId;
            this.number = number;
            this.path = path;
            this.entries = entries;
        }
    }

    private static final class ReferenceShardCountVersion {
        final String path;
        final int count;
        final int version;
        final int entriesVersion;

        ReferenceShardCountVersion(String path, int count,
                                   int version, int entriesVersion) {
            this.path = path;
            this.count = count;
            this.version = version;
            this.entriesVersion = entriesVersion;
        }
    }

    private static final class ReferenceEntry {
        final String path;
        final ArtifactReferenceRef ref;
        final int version;

        ReferenceEntry(String path, ArtifactReferenceRef ref, int version) {
            this.path = path;
            this.ref = ref;
            this.version = version;
        }
    }

    private static final class ReferenceMutation {
        enum Kind {
            NONE, CREATE_ARTIFACT, CREATE_SHARD, CREATE_ENTRY,
            UPDATE, CAPACITY_EXCEEDED
        }

        final Kind kind;
        final ReferenceCountVersion root;
        final ReferenceShardCountVersion shardCount;
        final ReferenceShard shard;
        final ArtifactReferenceRef ref;
        final String path;
        final int version;
        final byte[] data;
        final boolean capacityExceeded;

        private ReferenceMutation(
                Kind kind, ReferenceCountVersion root,
                ReferenceShardCountVersion shardCount,
                ReferenceShard shard, ArtifactReferenceRef ref,
                String path, int version, byte[] data) {
            this.kind = kind;
            this.root = root;
            this.shardCount = shardCount;
            this.shard = shard;
            this.ref = ref;
            this.path = path;
            this.version = version;
            this.data = data == null ? null : data.clone();
            this.capacityExceeded = kind == Kind.CAPACITY_EXCEEDED;
        }

        static ReferenceMutation none() {
            return new ReferenceMutation(
                    Kind.NONE, null, null, null, null, null, -1, null);
        }

        static ReferenceMutation capacityExceeded() {
            return new ReferenceMutation(Kind.CAPACITY_EXCEEDED,
                    null, null, null, null, null, -1, null);
        }

        static ReferenceMutation createArtifact(
                String artifactId, ReferenceShard shard,
                ArtifactReferenceRef ref) {
            return new ReferenceMutation(Kind.CREATE_ARTIFACT,
                    null, null, shard, ref,
                    referenceArtifactPath(artifactId), -1, null);
        }

        static ReferenceMutation createShard(
                ReferenceCountVersion root, ReferenceShard shard,
                ArtifactReferenceRef ref) {
            return new ReferenceMutation(Kind.CREATE_SHARD,
                    root, null, shard, ref, null, -1, null);
        }

        static ReferenceMutation createEntry(
                ReferenceCountVersion root,
                ReferenceShardCountVersion shardCount,
                ReferenceShard shard, ArtifactReferenceRef ref) {
            return new ReferenceMutation(Kind.CREATE_ENTRY,
                    root, shardCount, shard, ref, null, -1, null);
        }

        static ReferenceMutation update(String path, int version, byte[] data) {
            return new ReferenceMutation(Kind.UPDATE,
                    null, null, null, null, path, version, data);
        }

        void appendTo(CuratorFramework client, List<CuratorOp> operations)
                throws Exception {
            if (kind == Kind.NONE || kind == Kind.CAPACITY_EXCEEDED) return;
            if (kind == Kind.UPDATE) {
                operations.add(client.transactionOp().setData()
                        .withVersion(version).forPath(path, data));
                return;
            }
            String child = shard.entries + "/"
                    + RunLifecycleCatalogKernel.digestHex(ref.runId());
            byte[] encodedRef = ArtifactLifecycleCodec.encodeReference(ref);
            if (kind == Kind.CREATE_ARTIFACT) {
                operations.add(client.transactionOp().create().forPath(
                        path, encodeReferenceCount(ref.artifactId(), 1L)));
                operations.add(client.transactionOp().create().forPath(
                        path + "/shards", new byte[0]));
                operations.add(client.transactionOp().create().forPath(
                        shard.path, encodeReferenceShardCount(
                                ref.artifactId(), shard.number, 1)));
                operations.add(client.transactionOp().create().forPath(
                        shard.entries, new byte[0]));
                operations.add(client.transactionOp().create().forPath(
                        child, encodedRef));
                return;
            }
            operations.add(client.transactionOp().setData()
                    .withVersion(root.version).forPath(root.path,
                            encodeReferenceCount(
                                    ref.artifactId(), root.count + 1L)));
            if (kind == Kind.CREATE_SHARD) {
                operations.add(client.transactionOp().create().forPath(
                        shard.path, encodeReferenceShardCount(
                                ref.artifactId(), shard.number, 1)));
                operations.add(client.transactionOp().create().forPath(
                        shard.entries, new byte[0]));
            } else {
                operations.add(client.transactionOp().setData()
                        .withVersion(shardCount.version).forPath(shard.path,
                                encodeReferenceShardCount(ref.artifactId(),
                                        shard.number, shardCount.count + 1)));
            }
            operations.add(client.transactionOp().create().forPath(
                    child, encodedRef));
        }
    }

    private static final class ReferenceDeletion {
        final ReferenceCountVersion root;
        final ReferenceShardCountVersion shardCount;
        final ReferenceShard shard;
        final ReferenceEntry entry;

        private ReferenceDeletion(ReferenceCountVersion root,
                                  ReferenceShardCountVersion shardCount,
                                  ReferenceShard shard, ReferenceEntry entry) {
            this.root = root;
            this.shardCount = shardCount;
            this.shard = shard;
            this.entry = entry;
        }

        static ReferenceDeletion none() {
            return new ReferenceDeletion(null, null, null, null);
        }

        static ReferenceDeletion of(
                ReferenceCountVersion root,
                ReferenceShardCountVersion shardCount,
                ReferenceShard shard, ReferenceEntry entry) {
            if (root == null || shardCount == null || entry == null) {
                throw referenceCorruption(shard == null ? "unknown" : shard.artifactId,
                        "reference deletion state is incomplete");
            }
            if (root.count == 1L
                    && (shardCount.count != 1 || root.shardChildren != 1)) {
                throw referenceCorruption(shard.artifactId,
                        "last reference has non-empty sibling structure");
            }
            if (root.shardChildren > 1 && root.count <= shardCount.count) {
                throw referenceCorruption(shard.artifactId,
                        "root count omits sibling reference shards");
            }
            return new ReferenceDeletion(root, shardCount, shard, entry);
        }

        void appendTo(CuratorFramework client, List<CuratorOp> operations)
                throws Exception {
            if (entry == null) return;
            operations.add(client.transactionOp().delete()
                    .withVersion(entry.version).forPath(entry.path));
            if (root.count == 1L) {
                operations.add(client.transactionOp().delete()
                        .withVersion(shardCount.entriesVersion)
                        .forPath(shard.entries));
                operations.add(client.transactionOp().delete()
                        .withVersion(shardCount.version).forPath(shard.path));
                operations.add(client.transactionOp().delete()
                        .withVersion(root.shardsVersion)
                        .forPath(root.path + "/shards"));
                operations.add(client.transactionOp().delete()
                        .withVersion(root.version).forPath(root.path));
                return;
            }
            operations.add(client.transactionOp().setData()
                    .withVersion(root.version).forPath(root.path,
                            encodeReferenceCount(
                                    shard.artifactId, root.count - 1L)));
            if (shardCount.count == 1) {
                operations.add(client.transactionOp().delete()
                        .withVersion(shardCount.entriesVersion)
                        .forPath(shard.entries));
                operations.add(client.transactionOp().delete()
                        .withVersion(shardCount.version).forPath(shard.path));
            } else {
                operations.add(client.transactionOp().setData()
                        .withVersion(shardCount.version).forPath(shard.path,
                                encodeReferenceShardCount(shard.artifactId,
                                        shard.number, shardCount.count - 1)));
            }
        }
    }

    private static final class LifecycleMutation {
        enum Kind { CREATE, UPDATE, CAPACITY_EXCEEDED }

        final Kind kind;
        final LifecycleShard shard;
        final LifecycleShardView view;
        final String path;
        final int entryVersion;
        final byte[] data;
        final boolean capacityExceeded;

        private LifecycleMutation(Kind kind, LifecycleShard shard,
                                  LifecycleShardView view, String path,
                                  int entryVersion, byte[] data) {
            this.kind = kind;
            this.shard = shard;
            this.view = view;
            this.path = path;
            this.entryVersion = entryVersion;
            this.data = data == null ? null : data.clone();
            this.capacityExceeded = kind == Kind.CAPACITY_EXCEEDED;
        }

        static LifecycleMutation create(LifecycleShard shard,
                                        LifecycleShardView view,
                                        String digest, byte[] data) {
            return new LifecycleMutation(Kind.CREATE, shard, view,
                    shard.entries + "/" + digest, -1, data);
        }

        static LifecycleMutation update(String path, int entryVersion, byte[] data) {
            return new LifecycleMutation(
                    Kind.UPDATE, null, null, path, entryVersion, data);
        }

        static LifecycleMutation capacityExceeded() {
            return new LifecycleMutation(
                    Kind.CAPACITY_EXCEEDED, null, null, null, -1, null);
        }

        void appendTo(CuratorFramework client, List<CuratorOp> operations)
                throws Exception {
            if (kind == Kind.CAPACITY_EXCEEDED) return;
            if (kind == Kind.UPDATE) {
                operations.add(client.transactionOp().setData()
                        .withVersion(entryVersion).forPath(path, data));
                return;
            }
            if (view.generation == Long.MAX_VALUE) {
                throw new IllegalStateException("lifecycle shard generation overflow");
            }
            operations.add(client.transactionOp().setData()
                    .withVersion(view.guardVersion)
                    .forPath(shard.guard, longBytes(view.generation + 1L)));
            operations.add(client.transactionOp().create().forPath(path, data));
        }
    }

    private static final class CatalogEntry {
        final String path;
        final RecoverableRunRef ref;
        final int version;

        CatalogEntry(String path, RecoverableRunRef ref, int version) {
            this.path = path;
            this.ref = ref;
            this.version = version;
        }
    }

    private static final class CatalogMutation {
        enum Kind { CREATE, UPDATE, DELETE, CAPACITY_EXCEEDED }

        final Kind kind;
        final CatalogShard shard;
        final ShardView view;
        final String path;
        final int entryVersion;
        final byte[] data;
        final boolean capacityExceeded;

        private CatalogMutation(Kind kind, CatalogShard shard, ShardView view,
                                String path, int entryVersion, byte[] data) {
            this.kind = kind;
            this.shard = shard;
            this.view = view;
            this.path = path;
            this.entryVersion = entryVersion;
            this.data = data == null ? null : data.clone();
            this.capacityExceeded = kind == Kind.CAPACITY_EXCEEDED;
        }

        static CatalogMutation create(CatalogShard shard, ShardView view,
                                      String digest, byte[] data) {
            return new CatalogMutation(Kind.CREATE, shard, view,
                    shard.entries + "/" + digest, -1, data);
        }

        static CatalogMutation update(String path, int entryVersion, byte[] data) {
            return new CatalogMutation(Kind.UPDATE, null, null, path, entryVersion, data);
        }

        static CatalogMutation delete(CatalogShard shard, ShardView view,
                                      String digest, int entryVersion) {
            return new CatalogMutation(Kind.DELETE, shard, view,
                    shard.entries + "/" + digest, entryVersion, null);
        }

        static CatalogMutation capacityExceeded() {
            return new CatalogMutation(Kind.CAPACITY_EXCEEDED,
                    null, null, null, -1, null);
        }

        void appendTo(CuratorFramework client, List<CuratorOp> operations) throws Exception {
            if (kind == Kind.CAPACITY_EXCEEDED) return;
            if (kind == Kind.UPDATE) {
                operations.add(client.transactionOp().setData().withVersion(entryVersion)
                        .forPath(path, data));
                return;
            }
            if (view.generation == Long.MAX_VALUE) {
                throw new IllegalStateException("recoverable run shard generation overflow");
            }
            operations.add(client.transactionOp().setData().withVersion(view.guardVersion)
                    .forPath(shard.guard, longBytes(view.generation + 1L)));
            if (kind == Kind.CREATE) {
                operations.add(client.transactionOp().create().forPath(path, data));
            } else {
                operations.add(client.transactionOp().delete().withVersion(entryVersion).forPath(path));
            }
        }
    }

    private static final class CatalogRaceException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    private static final class RunIncarnationChangedException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    private static final class LifecycleGuardShapeException extends Exception {
        private static final long serialVersionUID = 1L;
        final boolean guardPresent;
        final boolean entriesPresent;

        LifecycleGuardShapeException(
                boolean guardPresent, boolean entriesPresent) {
            this.guardPresent = guardPresent;
            this.entriesPresent = entriesPresent;
        }
    }
}
