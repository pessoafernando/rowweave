package io.rowweave.incremental;

import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwnershipCodec;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateCodec;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.BatchSinkAbortRecordCodec;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalCodec;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkLocatorCodec;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.PreparedSinkReceiptCodec;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.WholeAttemptRetryDecision;
import io.rowweave.commit.ManifestCommitWire;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Local durable incremental authority. Accepted bootstrap and joint-claim
 * transitions are forced as canonical WAL frames under one file-domain lock.
 */
public final class FileIncrementalAuthorityStore
        implements IncrementalLocalAuthority, IncrementalFrontierStore {
    private static final int MAGIC = 0x52574946; // RWIF
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 4 + 1 + 1 + 4;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_PAYLOAD_BYTES =
            StageAssignmentManifestCodec.MAX_ROOT_FRAME_BYTES + Integer.BYTES
            + StageAssignmentManifest.MAX_SHARDS
            * (Integer.BYTES
            + StageAssignmentManifestCodec.MAX_SHARD_FRAME_BYTES)
            + FrontierRecoveryTokenCodec.MAX_FRAME_BYTES
            + RecoveryOwnershipCodec.MAX_FRAME_BYTES
            + 2 * Integer.BYTES;
    private static final long MAX_WAL_BYTES = 128L * 1024L * 1024L;
    private static final int LOCK_STRIPES = 256;
    private static final ReentrantLock[] JVM_LOCKS =
            new ReentrantLock[LOCK_STRIPES];

    private static final int INITIALIZE = 1;
    private static final int CLAIM_OWNERSHIP = 2;
    private static final int PUBLISH_PENDING = 3;
    private static final int CLAIM_INTERVAL = 4;
    private static final int ADVANCE_PROGRESS = 5;
    private static final int PUBLISH_ASSIGNMENTS = 6;
    private static final int ACCEPT_ASSIGNMENT = 7;
    private static final int RECORD_TERMINAL = 8;
    private static final int RECORD_PREPARED = 9;
    private static final int RECORD_MEMBER = 10;
    private static final int COMMIT_INTENT = 11;
    private static final int CLAIM_RUN = 12;
    private static final int JOINT_SUCCESS = 13;
    private static final int JOINT_CONFLICT = 14;
    private static final int RECORD_ABORT = 15;
    private static final int JOINT_ABORT = 16;
    private static final int SCHEDULE_RETRY = 17;
    private static final int CLAIM_RETRY = 18;
    private static final int RECORD_TERMINAL_RECOVERED = 19;
    private static final int REQUEST_CANCELLATION = 20;
    private static final int EVALUATE_DEADLINE = 21;
    private static final int BOOTSTRAP_CHECKPOINT = 22;
    private static final int READY_CHECKPOINT = 23;
    private static final int ADVANCE_PROGRESS_RECOVERED = 24;
    private static final int PUBLISH_ASSIGNMENTS_RECOVERED = 25;
    private static final int ACCEPT_ASSIGNMENT_RECOVERED = 26;
    private static final int RECORD_PREPARED_RECOVERED = 27;
    private static final int RECORD_MEMBER_RECOVERED = 28;
    private static final int COMMIT_INTENT_RECOVERED = 29;
    private static final int CHECKPOINT_INTERVAL = 64;

    static {
        for (int i = 0; i < JVM_LOCKS.length; i++) {
            JVM_LOCKS[i] = new ReentrantLock();
        }
    }

    private final Path root;
    private final Path wal;
    private final Path lockFile;
    private final Path runHistoryRoot;
    private final ReentrantLock jvmLock;
    private final IncrementalJointSuccessObserver transactionObserver;
    private final LongSupplier authorityClock;
    private final boolean forceDurability;
    private InMemoryIncrementalAuthorityStore delegate;
    private long loadedAuthorityTime;
    private AtomicLong delegateClock = new AtomicLong(1L);
    private Object loadedFileKey;
    private long loadedWalSize = -1L;
    private int lastReplayFrameCount;
    private boolean loaded;

    public FileIncrementalAuthorityStore(Path baseDirectory) {
        this(baseDirectory, phase -> { }, System::currentTimeMillis, true);
    }

    FileIncrementalAuthorityStore(
            Path baseDirectory,
            IncrementalJointSuccessObserver transactionObserver) {
        this(baseDirectory, transactionObserver, System::currentTimeMillis, true);
    }

    FileIncrementalAuthorityStore(
            Path baseDirectory,
            IncrementalJointSuccessObserver transactionObserver,
            LongSupplier authorityClock) {
        this(baseDirectory, transactionObserver, authorityClock, true);
    }

    FileIncrementalAuthorityStore(
            Path baseDirectory,
            IncrementalJointSuccessObserver transactionObserver,
            LongSupplier authorityClock, boolean forceDurability) {
        if (baseDirectory == null) {
            throw new IllegalArgumentException("base directory is required");
        }
        if (transactionObserver == null) {
            throw new IllegalArgumentException("transaction observer is required");
        }
        if (authorityClock == null) {
            throw new IllegalArgumentException("authority clock is required");
        }
        this.root = baseDirectory.toAbsolutePath().normalize()
                .resolve("incremental-authority-v1");
        this.wal = root.resolve("authority.wal");
        this.lockFile = root.resolve("authority.lock");
        this.runHistoryRoot = root.resolve("run-history-v1");
        this.jvmLock = JVM_LOCKS[(root.toString().hashCode() & 0x7fffffff)
                % JVM_LOCKS.length];
        this.transactionObserver = transactionObserver;
        this.authorityClock = authorityClock;
        this.forceDurability = forceDurability;
        try {
            boolean existed = Files.exists(root);
            Files.createDirectories(root);
            if (!Files.isDirectory(root)) {
                throw new IllegalArgumentException(
                        "incremental authority root is not a directory");
            }
            if (!existed) forceDirectory(root.getParent());
            boolean filesExisted = Files.exists(wal) && Files.exists(lockFile);
            boolean historyExisted = Files.exists(runHistoryRoot);
            Files.createDirectories(runHistoryRoot);
            if (!Files.isDirectory(runHistoryRoot)) {
                throw new IllegalArgumentException(
                        "incremental Run history root is not a directory");
            }
            try (FileChannel ignored = FileChannel.open(
                    wal, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
                 FileChannel ignoredLock = FileChannel.open(
                         lockFile, StandardOpenOption.CREATE,
                         StandardOpenOption.WRITE)) {
                // Creation is made durable by the directory force below.
            }
            if (!filesExisted || !historyExisted) forceDirectory(root);
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "failed to initialize incremental authority", failure);
        }
        this.delegate = new InMemoryIncrementalAuthorityStore(delegateClock::get);
    }

    @Override
    public IncrementalAuthorityDecision apply(
            IncrementalAuthorityMutation mutation) {
        if (mutation == null) {
            throw new IllegalArgumentException("authority mutation is required");
        }
        if (mutation.command().kind()
                != IncrementalAuthorityCommandKind.INITIALIZE
                && mutation.command().kind()
                != IncrementalAuthorityCommandKind.CLAIM
                && mutation.command().kind()
                != IncrementalAuthorityCommandKind.COMMIT
                && mutation.command().kind()
                != IncrementalAuthorityCommandKind.COMPLETE_BACKFILL
                && mutation.command().kind()
                != IncrementalAuthorityCommandKind.MARK_CONFLICT
                && mutation.command().kind()
                != IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN) {
            throw new UnsupportedOperationException(
                    "File authority terminal transaction is not yet enabled");
        }
        return locked(channel -> {
            reload(channel);
            boolean terminal = mutation.command().kind()
                    == IncrementalAuthorityCommandKind.COMMIT
                    || mutation.command().kind()
                    == IncrementalAuthorityCommandKind.COMPLETE_BACKFILL
                    || mutation.command().kind()
                    == IncrementalAuthorityCommandKind.MARK_CONFLICT
                    || mutation.command().kind()
                    == IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN;
            if (terminal) {
                transactionObserver.observe(
                        IncrementalJointSuccessPhase.BEFORE_COMMIT);
            }
            IncrementalAuthorityDecision decision = delegate.apply(mutation);
            if (decision.code() == IncrementalAuthorityDecision.Code.ACCEPTED) {
                int kind = mutation.command().kind()
                        == IncrementalAuthorityCommandKind.INITIALIZE
                        ? INITIALIZE
                        : mutation.command().kind()
                        == IncrementalAuthorityCommandKind.CLAIM
                        ? CLAIM_INTERVAL
                        : mutation.command().kind()
                        == IncrementalAuthorityCommandKind.MARK_CONFLICT
                        ? JOINT_CONFLICT
                        : mutation.command().kind()
                        == IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN
                        ? JOINT_ABORT : JOINT_SUCCESS;
                append(channel, frame(kind, encodeMutation(mutation)));
                if (terminal) {
                    transactionObserver.observe(
                            IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
                }
            }
            return decision;
        });
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
        return apply(IncrementalAuthorityMutation.initialize(command, request));
    }

    public FrontierOwnershipClaim claimOwnership(
            String feedId, long expectedRevision, long expectedOwnershipEpoch,
            RecoveryOwner owner) {
        return locked(channel -> {
            reload(channel);
            FrontierOwnershipClaim result = delegate.claimOwnership(
                    feedId, expectedRevision, expectedOwnershipEpoch, owner);
            if (result.code() == FrontierOwnershipClaim.Code.ACQUIRED) {
                append(channel, frame(CLAIM_OWNERSHIP,
                        encodeOwnership(feedId, expectedRevision,
                                expectedOwnershipEpoch, owner)));
            }
            return result;
        });
    }

    @Override
    public FrontierRecoveryToken frontierOwnership(String feedId) {
        return locked(channel -> {
            reload(channel);
            return delegate.frontierOwnership(feedId);
        });
    }

    public IncrementalPendingRunDecision publishPending(
            FrontierRecoveryToken token, IncrementalClaimRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("claim request is required");
        }
        return locked(channel -> {
            reload(channel);
            IncrementalPendingRunDecision result =
                    delegate.publishPending(
                            token, request, readRunHistory(request.runId()));
            if (result.code() == IncrementalPendingRunDecision.Code.PUBLISHED) {
                append(channel, frame(PUBLISH_PENDING,
                        encodePending(token, request)));
            }
            return result;
        });
    }

    @Override
    public RunDecision advanceProgress(
            FrontierRecoveryToken token,
            IncrementalAttemptProgressCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("progress command is required");
        }
        return locked(channel -> {
            reload(channel);
            RunDecision result = delegate.advanceProgress(token, command);
            if (result.code() == RunDecision.Code.ACCEPTED) {
                append(channel, frame(ADVANCE_PROGRESS,
                        encodeProgress(token, command)));
            }
            return result;
        });
    }

    @Override
    public RunDecision advanceProgress(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAttemptProgressCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("progress command is required");
        }
        return locked(channel -> {
            reload(channel);
            RunDecision result = delegate.advanceProgress(
                    frontierToken, runToken, command);
            if (result.code() == RunDecision.Code.ACCEPTED) {
                append(channel, frame(ADVANCE_PROGRESS_RECOVERED,
                        encodeRecovered(frontierToken, runToken,
                                encodeProgress(frontierToken, command))));
            }
            return result;
        });
    }

    @Override
    public StageManifestPublication publish(StageAssignmentManifest manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException("assignment manifest is required");
        }
        return locked(channel -> {
            reload(channel);
            StageManifestPublication result = delegate.publish(manifest);
            if (result.code() == StageManifestPublication.Code.PUBLISHED) {
                append(channel, frame(PUBLISH_ASSIGNMENTS,
                        encodeAssignments(manifest)));
            }
            return result;
        });
    }

    @Override
    public StageManifestPublication publishRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentManifest manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException(
                    "assignment manifest is required");
        }
        return locked(channel -> {
            reload(channel);
            StageManifestPublication result = delegate.publishRecovered(
                    frontierToken, runToken, manifest);
            if (result.code() == StageManifestPublication.Code.PUBLISHED) {
                append(channel, frame(PUBLISH_ASSIGNMENTS_RECOVERED,
                        encodeRecovered(frontierToken, runToken,
                                encodeAssignments(manifest))));
            }
            return result;
        });
    }

    @Override
    public StageAssignmentSnapshot assignmentSnapshot(RunAttemptFence fence) {
        return locked(channel -> {
            reload(channel);
            return delegate.assignmentSnapshot(fence);
        });
    }

    @Override
    public StageAcceptanceDecision accept(StageAssignmentAcceptance acceptance) {
        if (acceptance == null) {
            throw new IllegalArgumentException("assignment acceptance is required");
        }
        return locked(channel -> {
            reload(channel);
            StageAcceptanceDecision result = delegate.accept(acceptance);
            if (result.code() == StageAcceptanceDecision.Code.ACCEPTED) {
                append(channel, frame(ACCEPT_ASSIGNMENT,
                        encodeAcceptance(acceptance)));
            }
            return result;
        });
    }

    @Override
    public StageAcceptanceDecision acceptRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentAcceptance acceptance) {
        if (acceptance == null) {
            throw new IllegalArgumentException(
                    "assignment acceptance is required");
        }
        return locked(channel -> {
            reload(channel);
            StageAcceptanceDecision result = delegate.acceptRecovered(
                    frontierToken, runToken, acceptance);
            if (result.code() == StageAcceptanceDecision.Code.ACCEPTED) {
                append(channel, frame(ACCEPT_ASSIGNMENT_RECOVERED,
                        encodeRecovered(frontierToken, runToken,
                                encodeAcceptance(acceptance))));
            }
            return result;
        });
    }

    @Override
    public StageTerminalDecision recordTerminal(StageTerminalReceipt receipt) {
        if (receipt == null) {
            throw new IllegalArgumentException("terminal receipt is required");
        }
        return locked(channel -> {
            reload(channel);
            StageTerminalDecision result = delegate.recordTerminal(receipt);
            if (result.code() == StageTerminalDecision.Code.RECORDED) {
                append(channel, frame(RECORD_TERMINAL,
                        StageAssignmentManifestCodec.encodeTerminalReceipt(
                                receipt)));
            }
            return result;
        });
    }

    @Override
    public StageTerminalDecision recordTerminalRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageTerminalReceipt receipt) {
        if (receipt == null) {
            throw new IllegalArgumentException("terminal receipt is required");
        }
        return locked(channel -> {
            reload(channel);
            StageTerminalDecision result = delegate.recordTerminalRecovered(
                    frontierToken, runToken, receipt);
            if (result.code() == StageTerminalDecision.Code.RECORDED) {
                append(channel, frame(RECORD_TERMINAL_RECOVERED,
                        encodeRecoveredTerminal(
                                frontierToken, runToken, receipt)));
            }
            return result;
        });
    }

    @Override
    public PreparedSinkDecision recordPreparedSink(
            PreparedSinkReceipt receipt) {
        if (receipt == null) {
            throw new IllegalArgumentException("prepared Sink receipt is required");
        }
        return locked(channel -> {
            reload(channel);
            PreparedSinkDecision result = delegate.recordPreparedSink(receipt);
            if (result.code() == PreparedSinkDecision.Code.RECORDED) {
                append(channel, frame(RECORD_PREPARED,
                        PreparedSinkReceiptCodec.encode(receipt)));
            }
            return result;
        });
    }

    @Override
    public PreparedSinkDecision recordPreparedSinkRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            PreparedSinkReceipt receipt) {
        if (receipt == null) {
            throw new IllegalArgumentException(
                    "prepared Sink receipt is required");
        }
        return locked(channel -> {
            reload(channel);
            PreparedSinkDecision result =
                    delegate.recordPreparedSinkRecovered(
                            frontierToken, runToken, receipt);
            if (result.code() == PreparedSinkDecision.Code.RECORDED) {
                append(channel, frame(RECORD_PREPARED_RECOVERED,
                        encodeRecovered(frontierToken, runToken,
                                PreparedSinkReceiptCodec.encode(receipt))));
            }
            return result;
        });
    }

    @Override
    public ManifestMemberDecision recordManifestMember(
            ManifestMemberReceipt receipt) {
        if (receipt == null) {
            throw new IllegalArgumentException("Manifest member receipt is required");
        }
        return locked(channel -> {
            reload(channel);
            ManifestMemberDecision result =
                    delegate.recordManifestMember(receipt);
            if (result.code() == ManifestMemberDecision.Code.RECORDED) {
                append(channel, frame(RECORD_MEMBER,
                        encodeManifestMember(receipt)));
            }
            return result;
        });
    }

    @Override
    public ManifestMemberDecision recordManifestMemberRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestMemberReceipt receipt) {
        if (receipt == null) {
            throw new IllegalArgumentException(
                    "Manifest member receipt is required");
        }
        return locked(channel -> {
            reload(channel);
            ManifestMemberDecision result =
                    delegate.recordManifestMemberRecovered(
                            frontierToken, runToken, receipt);
            if (result.code() == ManifestMemberDecision.Code.RECORDED) {
                append(channel, frame(RECORD_MEMBER_RECOVERED,
                        encodeRecovered(frontierToken, runToken,
                                encodeManifestMember(receipt))));
            }
            return result;
        });
    }

    @Override
    public ManifestIntentDecision commitManifestIntent(
            ManifestCommitIntent intent) {
        if (intent == null) {
            throw new IllegalArgumentException("Manifest intent is required");
        }
        return locked(channel -> {
            reload(channel);
            ManifestIntentDecision result =
                    delegate.commitManifestIntent(intent);
            if (result.code() == ManifestIntentDecision.Code.COMMITTED) {
                append(channel, frame(COMMIT_INTENT,
                        ManifestCommitJournalCodec.encodeIntent(intent)));
            }
            return result;
        });
    }

    @Override
    public ManifestIntentDecision commitManifestIntentRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestCommitIntent intent) {
        if (intent == null) {
            throw new IllegalArgumentException(
                    "Manifest intent is required");
        }
        return locked(channel -> {
            reload(channel);
            ManifestIntentDecision result =
                    delegate.commitManifestIntentRecovered(
                            frontierToken, runToken, intent);
            if (result.code() == ManifestIntentDecision.Code.COMMITTED) {
                append(channel, frame(COMMIT_INTENT_RECOVERED,
                        encodeRecovered(frontierToken, runToken,
                                ManifestCommitJournalCodec.encodeIntent(
                                        intent))));
            }
            return result;
        });
    }

    @Override
    public ManifestCommitJournalSnapshot manifestCommitJournal(
            RunAttemptFence fence) {
        return locked(channel -> {
            reload(channel);
            return delegate.manifestCommitJournal(fence);
        });
    }

    @Override
    public BatchSinkAbortDecision recordBatchSinkAbortRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            BatchSinkAbortRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("abort record is required");
        }
        return locked(channel -> {
            reload(channel);
            BatchSinkAbortDecision result =
                    delegate.recordBatchSinkAbortRecovered(
                            frontierToken, runToken, record);
            if (result.code() == BatchSinkAbortDecision.Code.RECORDED) {
                append(channel, frame(RECORD_ABORT,
                        encodeAbort(frontierToken, runToken, record)));
            }
            return result;
        });
    }

    public BatchSinkAbortRecord batchSinkAbort(RunAttemptFence fence) {
        return locked(channel -> {
            reload(channel);
            return delegate.batchSinkAbort(fence);
        });
    }

    @Override
    public WholeAttemptRetryDecision scheduleRetry(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            RunAttemptFence fence) {
        return locked(channel -> {
            reload(channel);
            WholeAttemptRetryDecision result = delegate.scheduleRetry(
                    frontierToken, runToken, fence);
            if (result.code() == WholeAttemptRetryDecision.Code.SCHEDULED
                    || result.code()
                    == WholeAttemptRetryDecision.Code.EXHAUSTED) {
                append(channel, frame(SCHEDULE_RETRY,
                        encodeRetrySchedule(
                                frontierToken, runToken, fence, result)));
            }
            return result;
        });
    }

    @Override
    public RetryReadyEvaluation evaluateRetryReady(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        return locked(channel -> {
            reload(channel);
            return delegate.evaluateRetryReady(
                    frontierToken, runToken, runId);
        });
    }

    @Override
    public RetryAttemptClaim claimDueRetry(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        return locked(channel -> {
            reload(channel);
            RetryReadyEvaluation readiness = delegate.evaluateRetryReady(
                    frontierToken, runToken, runId);
            boolean jointMutation = readiness.code()
                    == RetryReadyEvaluation.Code.DUE;
            if (jointMutation) {
                transactionObserver.observe(
                        IncrementalJointSuccessPhase.BEFORE_COMMIT);
            }
            RetryAttemptClaim result = delegate.claimDueRetry(
                    frontierToken, runToken, runId);
            if (result.code() == RetryAttemptClaim.Code.CLAIMED) {
                append(channel, frame(CLAIM_RETRY,
                        encodeRetryClaim(
                                frontierToken, runToken, runId, result)));
                transactionObserver.observe(
                        IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
            }
            return result;
        });
    }

    @Override
    public RunCancellationResult requestUserCancellation(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId, String requestId) {
        return locked(channel -> {
            reload(channel);
            RunSnapshot before = delegate.runSnapshot(runId);
            RetryReadyEvaluation readiness = delegate.evaluateRetryReady(
                    frontierToken, runToken, runId);
            boolean jointMutation = before != null
                    && before.status() == io.rowweave.run.RunStatus.READY
                    && (readiness.code() == RetryReadyEvaluation.Code.DUE
                    || readiness.code() == RetryReadyEvaluation.Code.NOT_DUE);
            if (jointMutation) {
                transactionObserver.observe(
                        IncrementalJointSuccessPhase.BEFORE_COMMIT);
            }
            RunCancellationResult result = delegate.requestUserCancellation(
                    frontierToken, runToken, runId, requestId);
            if (result.disposition()
                    == RunCancellationResult.Disposition.ACCEPTED) {
                append(channel, frame(REQUEST_CANCELLATION,
                        encodeCancellation(
                                frontierToken, runToken, runId, requestId,
                                loadedAuthorityTime)));
                if (jointMutation) {
                    transactionObserver.observe(
                            IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
                }
            }
            return result;
        });
    }

    @Override
    public RunDeadlineResult evaluateDeadline(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        return locked(channel -> {
            reload(channel);
            RunSnapshot before = delegate.runSnapshot(runId);
            RetryReadyEvaluation readiness = delegate.evaluateRetryReady(
                    frontierToken, runToken, runId);
            boolean jointMutation = before != null
                    && before.status() == io.rowweave.run.RunStatus.READY
                    && (readiness.code() == RetryReadyEvaluation.Code.DUE
                    || readiness.code() == RetryReadyEvaluation.Code.NOT_DUE
                    || readiness.code() == RetryReadyEvaluation.Code.TOO_LATE);
            if (jointMutation) {
                transactionObserver.observe(
                        IncrementalJointSuccessPhase.BEFORE_COMMIT);
            }
            RunDeadlineResult result = delegate.evaluateDeadline(
                    frontierToken, runToken, runId);
            if (result.disposition()
                    == RunDeadlineResult.Disposition.ACCEPTED) {
                append(channel, frame(EVALUATE_DEADLINE,
                        encodeDeadline(
                                frontierToken, runToken, runId,
                                result.observedAuthorityTimeMillis())));
                if (jointMutation) {
                    transactionObserver.observe(
                            IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
                }
            }
            return result;
        });
    }

    @Override
    public VerifiedIncrementalAbortClosureProof verifyAbortClosure(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            RunAttemptFence fence, AttemptStatus terminalStatus) {
        return locked(channel -> {
            reload(channel);
            return delegate.verifyAbortClosure(
                    frontierToken, runToken, fence, terminalStatus);
        });
    }

    @Override
    public RecoveryClaim claim(
            String runId, long expectedRevision,
            long expectedOwnershipEpoch, RecoveryOwner owner) {
        return locked(channel -> {
            reload(channel);
            RecoveryClaim result = delegate.claim(
                    runId, expectedRevision, expectedOwnershipEpoch, owner);
            if (result.code() == RecoveryClaim.Code.ACQUIRED) {
                append(channel, frame(CLAIM_RUN,
                        encodeRunClaim(runId, expectedRevision,
                                expectedOwnershipEpoch, owner)));
            }
            return result;
        });
    }

    @Override
    public RecoveryToken ownership(String runId) {
        return locked(channel -> {
            reload(channel);
            return delegate.ownership(runId);
        });
    }

    @Override
    public boolean current(RecoveryToken token) {
        return locked(channel -> {
            reload(channel);
            return delegate.current(token);
        });
    }

    public IncrementalAuthorityRecord record(String feedId) {
        return locked(channel -> {
            reload(channel);
            return delegate.record(feedId);
        });
    }

    public IncrementalClaimRequest pending(String feedId) {
        return locked(channel -> {
            reload(channel);
            return delegate.pending(feedId);
        });
    }

    public RunSnapshot runSnapshot(String runId) {
        return locked(channel -> {
            reload(channel);
            RunSnapshot current = delegate.runSnapshot(runId);
            return current == null ? readRunHistory(runId) : current;
        });
    }

    public int guardEntryCount() {
        return locked(channel -> {
            reload(channel);
            return delegate.guardEntryCount();
        });
    }

    public int guardShardEntryCount(int shard) {
        return locked(channel -> {
            reload(channel);
            return delegate.guardShardEntryCount(shard);
        });
    }

    public int guardShardFrameBytes(int shard) {
        return locked(channel -> {
            reload(channel);
            return delegate.guardShardFrameBytes(shard);
        });
    }

    public static int guardShardIndex(String feedId) {
        return InMemoryIncrementalAuthorityStore.guardShardIndex(feedId);
    }

    long walBytesForTest() {
        return locked(FileChannel::size);
    }

    int lastReplayFrameCountForTest() {
        return lastReplayFrameCount;
    }

    private void reload(FileChannel channel) throws IOException {
        long size = channel.size();
        BasicFileAttributes attributes = Files.readAttributes(
                wal, BasicFileAttributes.class);
        if (loaded && size == loadedWalSize
                && Objects.equals(attributes.fileKey(), loadedFileKey)) {
            loadedAuthorityTime = authorityClock.getAsLong();
            delegateClock.set(loadedAuthorityTime);
            return;
        }
        if (size > MAX_WAL_BYTES || size > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "incremental authority WAL exceeds its bounded size");
        }
        ByteBuffer bytes = ByteBuffer.allocate((int) size);
        channel.position(0L);
        while (bytes.hasRemaining() && channel.read(bytes) >= 0) {
            // Keep reading until the exact, already-bounded file is consumed.
        }
        bytes.flip();
        AtomicLong replayClock = new AtomicLong(1L);
        InMemoryIncrementalAuthorityStore recovered =
                new InMemoryIncrementalAuthorityStore(replayClock::get);
        int validBytes = 0;
        int replayedFrames = 0;
        while (bytes.hasRemaining()) {
            int frameStart = bytes.position();
            if (bytes.remaining() < HEADER_BYTES) break;
            int magic = bytes.getInt();
            int version = bytes.get() & 0xff;
            int kind = bytes.get() & 0xff;
            int length = bytes.getInt();
            if (magic != MAGIC || version != VERSION
                    || length < 0 || length > MAX_PAYLOAD_BYTES) {
                throw new IllegalStateException(
                        "incremental authority WAL frame header is corrupt");
            }
            if (bytes.remaining() < length + CHECKSUM_BYTES) break;
            byte[] payload = new byte[length];
            bytes.get(payload);
            byte[] checksum = new byte[CHECKSUM_BYTES];
            bytes.get(checksum);
            int bodyLength = HEADER_BYTES + length;
            byte[] body = new byte[bodyLength];
            ByteBuffer duplicate = bytes.duplicate();
            duplicate.position(frameStart).limit(frameStart + bodyLength);
            duplicate.get(body);
            if (!MessageDigest.isEqual(
                    checksum, IncrementalAuthorityValue.sha256(body))) {
                throw new IllegalStateException(
                        "incremental authority WAL checksum mismatch");
            }
            replay(recovered, replayClock, kind, payload);
            validBytes = bytes.position();
            replayedFrames++;
        }
        if (validBytes != size) {
            channel.truncate(validBytes);
            force(channel);
        }
        loadedAuthorityTime = authorityClock.getAsLong();
        replayClock.set(loadedAuthorityTime);
        delegateClock = replayClock;
        delegate = recovered;
        BasicFileAttributes loadedAttributes = Files.readAttributes(
                wal, BasicFileAttributes.class);
        loadedFileKey = loadedAttributes.fileKey();
        loadedWalSize = validBytes;
        lastReplayFrameCount = replayedFrames;
        loaded = true;
    }

    private static void replay(
            InMemoryIncrementalAuthorityStore target,
            AtomicLong replayClock, int kind,
            byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(
                    new ByteArrayInputStream(payload));
            switch (kind) {
                case INITIALIZE:
                    replayInitialize(target, in);
                    break;
                case CLAIM_OWNERSHIP:
                    replayOwnership(target, in);
                    break;
                case PUBLISH_PENDING:
                    replayPending(target, in);
                    break;
                case CLAIM_INTERVAL:
                    replayClaim(target, in);
                    break;
                case ADVANCE_PROGRESS:
                    replayProgress(target, in);
                    break;
                case PUBLISH_ASSIGNMENTS:
                    replayAssignments(target, in);
                    break;
                case ACCEPT_ASSIGNMENT:
                    replayAcceptance(target, in);
                    break;
                case RECORD_TERMINAL:
                    replayTerminal(target, in);
                    break;
                case RECORD_PREPARED:
                    replayPrepared(target, in);
                    break;
                case RECORD_MEMBER:
                    replayManifestMember(target, in);
                    break;
                case COMMIT_INTENT:
                    replayManifestIntent(target, in);
                    break;
                case CLAIM_RUN:
                    replayRunClaim(target, in);
                    break;
                case JOINT_SUCCESS:
                    replayJointSuccess(target, in);
                    break;
                case JOINT_CONFLICT:
                    replayJointConflict(target, in);
                    break;
                case RECORD_ABORT:
                    replayAbort(target, in);
                    break;
                case JOINT_ABORT:
                    replayJointAbort(target, in);
                    break;
                case SCHEDULE_RETRY:
                    replayRetrySchedule(target, replayClock, in);
                    break;
                case CLAIM_RETRY:
                    replayRetryClaim(target, replayClock, in);
                    break;
                case RECORD_TERMINAL_RECOVERED:
                    replayRecoveredTerminal(target, in);
                    break;
                case REQUEST_CANCELLATION:
                    replayCancellation(target, replayClock, in);
                    break;
                case EVALUATE_DEADLINE:
                    replayDeadline(target, replayClock, in);
                    break;
                case BOOTSTRAP_CHECKPOINT:
                    replayBootstrapCheckpoint(target, in);
                    break;
                case READY_CHECKPOINT:
                    replayReadyCheckpoint(target, in);
                    break;
                case ADVANCE_PROGRESS_RECOVERED:
                    replayRecoveredProgress(target, in);
                    break;
                case PUBLISH_ASSIGNMENTS_RECOVERED:
                    replayRecoveredAssignments(target, in);
                    break;
                case ACCEPT_ASSIGNMENT_RECOVERED:
                    replayRecoveredAcceptance(target, in);
                    break;
                case RECORD_PREPARED_RECOVERED:
                    replayRecoveredPrepared(target, in);
                    break;
                case RECORD_MEMBER_RECOVERED:
                    replayRecoveredManifestMember(target, in);
                    break;
                case COMMIT_INTENT_RECOVERED:
                    replayRecoveredManifestIntent(target, in);
                    break;
                default:
                    throw new IllegalStateException(
                            "unknown incremental authority WAL operation");
            }
            if (in.read() != -1) {
                throw new IllegalStateException(
                        "incremental authority WAL payload has trailing bytes");
            }
        } catch (IOException malformed) {
            throw new IllegalStateException(
                    "incremental authority WAL payload is malformed", malformed);
        }
    }

    private static void replayInitialize(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        IncrementalAuthorityCommand command = IncrementalAuthorityCommandCodec.decode(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, IncrementalAuthorityCommandCodec.MAX_FRAME_BYTES,
                        "initialize command"));
        IncrementalInitializeRequest request = decodeInitialize(in);
        if (target.apply(IncrementalAuthorityMutation.initialize(
                command, request)).code()
                != IncrementalAuthorityDecision.Code.ACCEPTED) {
            throw new IllegalStateException("durable initialize replay diverged");
        }
    }

    private static void replayOwnership(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        String feedId = IncrementalAuthorityCodecSupport.readText(
                in, "ownership feed id");
        long expectedRevision = in.readLong();
        long expectedEpoch = in.readLong();
        RecoveryOwner owner = RecoveryOwnershipCodec.decodeOwner(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, RecoveryOwnershipCodec.MAX_FRAME_BYTES,
                        "recovery owner"));
        if (target.claimOwnership(feedId, expectedRevision,
                expectedEpoch, owner).code()
                != FrontierOwnershipClaim.Code.ACQUIRED) {
            throw new IllegalStateException("durable ownership replay diverged");
        }
    }

    private static void replayPending(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        FrontierRecoveryToken token = FrontierRecoveryTokenCodec.decode(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, FrontierRecoveryTokenCodec.MAX_FRAME_BYTES,
                        "frontier recovery token"));
        IncrementalClaimRequest request = decodeClaimRequest(in);
        if (target.publishPending(token, request).code()
                != IncrementalPendingRunDecision.Code.PUBLISHED) {
            throw new IllegalStateException("durable pending replay diverged");
        }
    }

    private static void replayClaim(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        IncrementalAuthorityCommand command = IncrementalAuthorityCommandCodec.decode(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, IncrementalAuthorityCommandCodec.MAX_FRAME_BYTES,
                        "claim command"));
        FrontierRecoveryToken token = FrontierRecoveryTokenCodec.decode(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, FrontierRecoveryTokenCodec.MAX_FRAME_BYTES,
                        "frontier recovery token"));
        IncrementalClaimRequest request = decodeClaimRequest(in);
        if (target.apply(IncrementalAuthorityMutation.claim(
                command, token, request)).code()
                != IncrementalAuthorityDecision.Code.ACCEPTED) {
            throw new IllegalStateException("durable joint claim replay diverged");
        }
    }

    private static void replayProgress(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        FrontierRecoveryToken token = readFrontierToken(in);
        IncrementalAttemptProgressCommand command = decodeProgress(in);
        if (target.advanceProgress(token, command).code()
                != RunDecision.Code.ACCEPTED) {
            throw new IllegalStateException("durable progress replay diverged");
        }
    }

    private static void replayAssignments(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        if (target.publish(decodeAssignments(in)).code()
                != StageManifestPublication.Code.PUBLISHED) {
            throw new IllegalStateException(
                    "durable assignment publication replay diverged");
        }
    }

    private static void replayAcceptance(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        if (target.accept(decodeAcceptance(in)).code()
                != StageAcceptanceDecision.Code.ACCEPTED) {
            throw new IllegalStateException(
                    "durable assignment acceptance replay diverged");
        }
    }

    private static void replayTerminal(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        byte[] frame = readRemaining(
                in, StageAssignmentManifestCodec.MAX_TERMINAL_RECEIPT_FRAME_BYTES,
                "terminal receipt");
        if (target.recordTerminal(
                StageAssignmentManifestCodec.decodeTerminalReceipt(frame)).code()
                != StageTerminalDecision.Code.RECORDED) {
            throw new IllegalStateException("durable terminal replay diverged");
        }
    }

    private static void replayPrepared(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        byte[] frame = readRemaining(
                in, PreparedSinkReceiptCodec.MAX_FRAME_BYTES,
                "prepared Sink receipt");
        if (target.recordPreparedSink(
                PreparedSinkReceiptCodec.decode(frame)).code()
                != PreparedSinkDecision.Code.RECORDED) {
            throw new IllegalStateException(
                    "durable prepared Sink replay diverged");
        }
    }

    private static void replayManifestMember(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        if (target.recordManifestMember(decodeManifestMember(in)).code()
                != ManifestMemberDecision.Code.RECORDED) {
            throw new IllegalStateException(
                    "durable Manifest member replay diverged");
        }
    }

    private static void replayManifestIntent(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        byte[] frame = readRemaining(
                in, ManifestCommitJournalCodec.MAX_QUALITY_INTENT_FRAME_BYTES,
                "Manifest intent");
        if (target.commitManifestIntent(
                ManifestCommitJournalCodec.decodeIntent(frame)).code()
                != ManifestIntentDecision.Code.COMMITTED) {
            throw new IllegalStateException(
                    "durable Manifest intent replay diverged");
        }
    }

    private static void replayRunClaim(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        String runId = IncrementalAuthorityCodecSupport.readText(
                in, "Run claim id");
        long expectedRevision = in.readLong();
        long expectedEpoch = in.readLong();
        RecoveryOwner owner = readRecoveryOwner(in);
        if (target.claim(runId, expectedRevision, expectedEpoch, owner).code()
                != RecoveryClaim.Code.ACQUIRED) {
            throw new IllegalStateException("durable Run claim replay diverged");
        }
    }

    private static void replayJointSuccess(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        IncrementalAuthorityCommand command = readCommand(in);
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        IncrementalActiveInterval active = target.record(
                command.feedId()).activeInterval();
        VerifiedIncrementalSinkCommitDecision decision =
                IncrementalSinkIntentStrictAdapter.verify(active, target);
        VerifiedIncrementalPublicationProof publication =
                new VerifiedIncrementalPublicationProof(
                        decision, frontierToken, runToken);
        VerifiedIncrementalJointCommitProof proof =
                IncrementalJointCommitProofAssembler.join(
                        active, decision, publication);
        IncrementalAuthorityDecision applied = target.apply(
                IncrementalAuthorityMutation.jointTerminal(
                        command, frontierToken, runToken, proof));
        if (applied.code() != IncrementalAuthorityDecision.Code.ACCEPTED) {
            throw new IllegalStateException(
                    "durable joint success replay diverged: " + applied.code());
        }
    }

    private static void replayJointConflict(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        IncrementalAuthorityCommand command = readCommand(in);
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        IncrementalActiveInterval active = target.record(
                command.feedId()).activeInterval();
        VerifiedIncrementalSinkCommitDecision decision =
                IncrementalSinkIntentStrictAdapter.verify(active, target);
        VerifiedIncrementalConflictProof proof =
                new VerifiedIncrementalConflictProof(
                        active, decision, frontierToken, runToken);
        IncrementalAuthorityDecision applied = target.apply(
                IncrementalAuthorityMutation.markConflict(
                        command, frontierToken, runToken, proof));
        if (applied.code() != IncrementalAuthorityDecision.Code.ACCEPTED) {
            throw new IllegalStateException(
                    "durable joint conflict replay diverged: " + applied.code());
        }
    }

    private static void replayAbort(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        BatchSinkAbortRecord record = BatchSinkAbortRecordCodec.decode(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, BatchSinkAbortRecordCodec.MAX_FRAME_BYTES,
                        "batch Sink abort record"));
        if (target.recordBatchSinkAbortRecovered(
                frontierToken, runToken, record).code()
                != BatchSinkAbortDecision.Code.RECORDED) {
            throw new IllegalStateException("durable abort replay diverged");
        }
    }

    private static void replayJointAbort(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        IncrementalAuthorityCommand command = readCommand(in);
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        AttemptStatus terminalStatus = AttemptStatus.valueOf(
                IncrementalAuthorityCodecSupport.readText(
                        in, "abort terminal status"));
        VerifiedIncrementalAbortClosureProof proof =
                target.verifyAbortClosure(
                        frontierToken, runToken, command.fence(),
                        terminalStatus);
        IncrementalAuthorityDecision applied = target.apply(
                IncrementalAuthorityMutation.closeAborted(
                        command, frontierToken, runToken, proof));
        if (applied.code() != IncrementalAuthorityDecision.Code.ACCEPTED) {
            throw new IllegalStateException(
                    "durable joint abort replay diverged: " + applied.code());
        }
    }

    private static void replayRetrySchedule(
            InMemoryIncrementalAuthorityStore target,
            AtomicLong replayClock, DataInputStream in) throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        RunAttemptFence fence = readFence(in);
        WholeAttemptRetryDecision.Code expected =
                WholeAttemptRetryDecision.Code.valueOf(
                        IncrementalAuthorityCodecSupport.readText(
                                in, "retry schedule result"));
        replayClock.set(in.readLong());
        WholeAttemptRetryDecision result = target.scheduleRetry(
                frontierToken, runToken, fence);
        if (result.code() != expected) {
            throw new IllegalStateException(
                    "durable retry schedule replay diverged: "
                            + result.code());
        }
    }

    private static void replayRetryClaim(
            InMemoryIncrementalAuthorityStore target,
            AtomicLong replayClock, DataInputStream in) throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        String runId = IncrementalAuthorityCodecSupport.readText(
                in, "retry claim Run id");
        replayClock.set(in.readLong());
        RetryAttemptClaim result = target.claimDueRetry(
                frontierToken, runToken, runId);
        if (result.code() != RetryAttemptClaim.Code.CLAIMED) {
            throw new IllegalStateException(
                    "durable retry claim replay diverged: " + result.code());
        }
    }

    private static void replayRecoveredTerminal(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        byte[] receipt = readRemaining(
                in, StageAssignmentManifestCodec.MAX_TERMINAL_RECEIPT_FRAME_BYTES,
                "recovered terminal receipt");
        if (target.recordTerminalRecovered(
                frontierToken, runToken,
                StageAssignmentManifestCodec.decodeTerminalReceipt(receipt)).code()
                != StageTerminalDecision.Code.RECORDED) {
            throw new IllegalStateException(
                    "durable recovered terminal replay diverged");
        }
    }

    private static void replayRecoveredProgress(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        DataInputStream body = recoveredBody(in, "recovered progress");
        FrontierRecoveryToken encodedToken = readFrontierToken(body);
        if (!frontierToken.equals(encodedToken)
                || target.advanceProgress(
                frontierToken, runToken, decodeProgress(body)).code()
                != RunDecision.Code.ACCEPTED
                || body.read() != -1) {
            throw new IllegalStateException(
                    "durable recovered progress replay diverged");
        }
    }

    private static void replayRecoveredAssignments(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        DataInputStream body = recoveredBody(in, "recovered assignments");
        StageManifestPublication result = target.publishRecovered(
                frontierToken, runToken, decodeAssignments(body));
        if (result.code() != StageManifestPublication.Code.PUBLISHED
                || body.read() != -1) {
            throw new IllegalStateException(
                    "durable recovered assignment replay diverged");
        }
    }

    private static void replayRecoveredAcceptance(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        DataInputStream body = recoveredBody(in, "recovered acceptance");
        StageAcceptanceDecision result = target.acceptRecovered(
                frontierToken, runToken, decodeAcceptance(body));
        if (result.code() != StageAcceptanceDecision.Code.ACCEPTED
                || body.read() != -1) {
            throw new IllegalStateException(
                    "durable recovered acceptance replay diverged");
        }
    }

    private static void replayRecoveredPrepared(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        byte[] receipt = readRemaining(
                in, PreparedSinkReceiptCodec.MAX_FRAME_BYTES,
                "recovered prepared Sink receipt");
        if (target.recordPreparedSinkRecovered(
                frontierToken, runToken,
                PreparedSinkReceiptCodec.decode(receipt)).code()
                != PreparedSinkDecision.Code.RECORDED) {
            throw new IllegalStateException(
                    "durable recovered prepared Sink replay diverged");
        }
    }

    private static void replayRecoveredManifestMember(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        DataInputStream body = recoveredBody(in, "recovered Manifest member");
        ManifestMemberDecision result = target.recordManifestMemberRecovered(
                frontierToken, runToken, decodeManifestMember(body));
        if (result.code() != ManifestMemberDecision.Code.RECORDED
                || body.read() != -1) {
            throw new IllegalStateException(
                    "durable recovered Manifest member replay diverged");
        }
    }

    private static void replayRecoveredManifestIntent(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        byte[] intent = readRemaining(
                in, ManifestCommitJournalCodec.MAX_QUALITY_INTENT_FRAME_BYTES,
                "recovered Manifest intent");
        if (target.commitManifestIntentRecovered(
                frontierToken, runToken,
                ManifestCommitJournalCodec.decodeIntent(intent)).code()
                != ManifestIntentDecision.Code.COMMITTED) {
            throw new IllegalStateException(
                    "durable recovered Manifest intent replay diverged");
        }
    }

    private static DataInputStream recoveredBody(
            DataInputStream in, String field) throws IOException {
        return new DataInputStream(new ByteArrayInputStream(
                readRemaining(in, MAX_PAYLOAD_BYTES, field)));
    }

    private static void replayCancellation(
            InMemoryIncrementalAuthorityStore target,
            AtomicLong replayClock, DataInputStream in) throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        String runId = IncrementalAuthorityCodecSupport.readText(
                in, "cancellation Run id");
        String requestId = IncrementalAuthorityCodecSupport.readText(
                in, "cancellation request id");
        replayClock.set(in.readLong());
        RunCancellationResult result = target.requestUserCancellation(
                frontierToken, runToken, runId, requestId);
        if (result.disposition()
                != RunCancellationResult.Disposition.ACCEPTED) {
            throw new IllegalStateException(
                    "durable cancellation replay diverged: "
                            + result.disposition());
        }
    }

    private static void replayDeadline(
            InMemoryIncrementalAuthorityStore target,
            AtomicLong replayClock, DataInputStream in) throws IOException {
        FrontierRecoveryToken frontierToken = readFrontierToken(in);
        RecoveryToken runToken = readRunToken(in);
        String runId = IncrementalAuthorityCodecSupport.readText(
                in, "deadline Run id");
        replayClock.set(in.readLong());
        RunDeadlineResult result = target.evaluateDeadline(
                frontierToken, runToken, runId);
        if (result.disposition() != RunDeadlineResult.Disposition.ACCEPTED) {
            throw new IllegalStateException(
                    "durable deadline replay diverged: "
                            + result.disposition());
        }
    }

    private static void replayBootstrapCheckpoint(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        long nextRootIdentity = in.readLong();
        int count = in.readInt();
        if (count < 0 || count > FeedGuardShardObservation.SHARD_COUNT
                * FeedGuardCatalog.MAX_ENTRIES_PER_SHARD) {
            throw new IllegalArgumentException(
                    "bootstrap checkpoint feed count is invalid");
        }
        List<InMemoryIncrementalAuthorityStore.FileBootstrapFeed> feeds =
                new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            IncrementalAuthorityRecord record =
                    IncrementalAuthorityRecordCodec.decode(
                            IncrementalAuthorityCodecSupport.readBytes(
                                    in, IncrementalAuthorityRecordCodec.MAX_FRAME_BYTES,
                                    "checkpoint authority record"));
            long rootIdentity = in.readLong();
            long ownershipEpoch = in.readLong();
            FrontierRecoveryToken token = in.readBoolean()
                    ? FrontierRecoveryTokenCodec.decode(
                    IncrementalAuthorityCodecSupport.readBytes(
                            in, FrontierRecoveryTokenCodec.MAX_FRAME_BYTES,
                            "checkpoint frontier token")) : null;
            String lastCommandId = null;
            byte[] lastCommandDigest = null;
            if (in.readBoolean()) {
                lastCommandId = IncrementalAuthorityCodecSupport.readText(
                        in, "checkpoint command id");
                lastCommandDigest = IncrementalAuthorityCodecSupport.readDigest(
                        in, "checkpoint command digest");
            }
            feeds.add(new InMemoryIncrementalAuthorityStore.FileBootstrapFeed(
                    record, rootIdentity, ownershipEpoch, token,
                    lastCommandId, lastCommandDigest));
        }
        target.restoreFileBootstrapCheckpoint(
                new InMemoryIncrementalAuthorityStore.FileBootstrapCheckpoint(
                        nextRootIdentity, feeds));
    }

    private static byte[] encodeBootstrapCheckpoint(
            InMemoryIncrementalAuthorityStore.FileBootstrapCheckpoint checkpoint) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeLong(checkpoint.nextRootIdentity);
            out.writeInt(checkpoint.feeds.size());
            for (InMemoryIncrementalAuthorityStore.FileBootstrapFeed feed
                    : checkpoint.feeds) {
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, IncrementalAuthorityRecordCodec.encode(feed.record));
                out.writeLong(feed.rootIdentity);
                out.writeLong(feed.ownershipEpoch);
                out.writeBoolean(feed.token != null);
                if (feed.token != null) {
                    IncrementalAuthorityCodecSupport.writeBytes(
                            out, FrontierRecoveryTokenCodec.encode(feed.token));
                }
                out.writeBoolean(feed.lastCommandId != null);
                if (feed.lastCommandId != null) {
                    IncrementalAuthorityCodecSupport.writeText(
                            out, feed.lastCommandId);
                    out.write(feed.lastCommandDigest);
                }
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory bootstrap checkpoint encoding failed", impossible);
        }
    }

    private static void replayReadyCheckpoint(
            InMemoryIncrementalAuthorityStore target, DataInputStream in)
            throws IOException {
        long nextRootIdentity = in.readLong();
        int count = in.readInt();
        if (count < 0 || count > FeedGuardShardObservation.SHARD_COUNT
                * FeedGuardCatalog.MAX_ENTRIES_PER_SHARD) {
            throw new IllegalArgumentException(
                    "ready checkpoint feed count is invalid");
        }
        List<InMemoryIncrementalAuthorityStore.FileBootstrapFeed> feeds =
                readCheckpointFeeds(in, count);
        int runCount = in.readInt();
        if (runCount < 0 || runCount > count * 2) {
            throw new IllegalArgumentException(
                    "ready checkpoint Run count is invalid");
        }
        List<InMemoryIncrementalAuthorityStore.FileCheckpointRun> runs =
                new ArrayList<>(runCount);
        for (int i = 0; i < runCount; i++) {
            RunSnapshot snapshot = RunStateCodec.decodeSnapshot(
                    IncrementalAuthorityCodecSupport.readBytes(
                            in, RunStateCodec.MAX_FRAME_BYTES,
                            "checkpoint Run snapshot"));
            RecoveryToken token = RecoveryOwnershipCodec.decodeToken(
                    IncrementalAuthorityCodecSupport.readBytes(
                            in, RecoveryOwnershipCodec.MAX_FRAME_BYTES,
                            "checkpoint Run recovery token"));
            runs.add(new InMemoryIncrementalAuthorityStore.FileCheckpointRun(
                    snapshot, token));
        }
        target.restoreFileReadyCheckpoint(
                new InMemoryIncrementalAuthorityStore.FileReadyCheckpoint(
                        nextRootIdentity, feeds, runs));
    }

    private static List<InMemoryIncrementalAuthorityStore.FileBootstrapFeed>
    readCheckpointFeeds(DataInputStream in, int count) throws IOException {
        List<InMemoryIncrementalAuthorityStore.FileBootstrapFeed> feeds =
                new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            IncrementalAuthorityRecord record =
                    IncrementalAuthorityRecordCodec.decode(
                            IncrementalAuthorityCodecSupport.readBytes(
                                    in, IncrementalAuthorityRecordCodec.MAX_FRAME_BYTES,
                                    "checkpoint authority record"));
            long rootIdentity = in.readLong();
            long ownershipEpoch = in.readLong();
            FrontierRecoveryToken token = in.readBoolean()
                    ? FrontierRecoveryTokenCodec.decode(
                    IncrementalAuthorityCodecSupport.readBytes(
                            in, FrontierRecoveryTokenCodec.MAX_FRAME_BYTES,
                            "checkpoint frontier token")) : null;
            String lastCommandId = null;
            byte[] lastCommandDigest = null;
            if (in.readBoolean()) {
                lastCommandId = IncrementalAuthorityCodecSupport.readText(
                        in, "checkpoint command id");
                lastCommandDigest = IncrementalAuthorityCodecSupport.readDigest(
                        in, "checkpoint command digest");
            }
            String lastTerminalRunId = in.readBoolean()
                    ? IncrementalAuthorityCodecSupport.readText(
                    in, "checkpoint terminal Run id") : null;
            feeds.add(new InMemoryIncrementalAuthorityStore.FileBootstrapFeed(
                    record, rootIdentity, ownershipEpoch, token,
                    lastCommandId, lastCommandDigest, lastTerminalRunId));
        }
        return feeds;
    }

    private static byte[] encodeReadyCheckpoint(
            InMemoryIncrementalAuthorityStore.FileReadyCheckpoint checkpoint) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeLong(checkpoint.nextRootIdentity);
            out.writeInt(checkpoint.feeds.size());
            writeCheckpointFeeds(out, checkpoint.feeds);
            out.writeInt(checkpoint.runs.size());
            for (InMemoryIncrementalAuthorityStore.FileCheckpointRun run
                    : checkpoint.runs) {
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, RunStateCodec.encodeSnapshot(run.snapshot));
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, RecoveryOwnershipCodec.encodeToken(run.token));
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory ready checkpoint encoding failed", impossible);
        }
    }

    private static void writeCheckpointFeeds(
            DataOutputStream out,
            List<InMemoryIncrementalAuthorityStore.FileBootstrapFeed> feeds)
            throws IOException {
        for (InMemoryIncrementalAuthorityStore.FileBootstrapFeed feed : feeds) {
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, IncrementalAuthorityRecordCodec.encode(feed.record));
            out.writeLong(feed.rootIdentity);
            out.writeLong(feed.ownershipEpoch);
            out.writeBoolean(feed.token != null);
            if (feed.token != null) {
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, FrontierRecoveryTokenCodec.encode(feed.token));
            }
            out.writeBoolean(feed.lastCommandId != null);
            if (feed.lastCommandId != null) {
                IncrementalAuthorityCodecSupport.writeText(
                        out, feed.lastCommandId);
                out.write(feed.lastCommandDigest);
            }
            out.writeBoolean(feed.lastTerminalRunId != null);
            if (feed.lastTerminalRunId != null) {
                IncrementalAuthorityCodecSupport.writeText(
                        out, feed.lastTerminalRunId);
            }
        }
    }

    private static byte[] encodeMutation(IncrementalAuthorityMutation mutation) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, IncrementalAuthorityCommandCodec.encode(
                            mutation.command()));
            if (mutation.command().kind()
                    == IncrementalAuthorityCommandKind.INITIALIZE) {
                encodeInitialize(out, mutation.initializeRequest());
            } else if (mutation.command().kind()
                    == IncrementalAuthorityCommandKind.CLAIM) {
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, FrontierRecoveryTokenCodec.encode(
                                mutation.recoveryToken()));
                encodeClaimRequest(out, mutation.claimRequest());
            } else if (mutation.command().kind()
                    == IncrementalAuthorityCommandKind.COMMIT
                    || mutation.command().kind()
                    == IncrementalAuthorityCommandKind.COMPLETE_BACKFILL
                    || mutation.command().kind()
                    == IncrementalAuthorityCommandKind.MARK_CONFLICT) {
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, FrontierRecoveryTokenCodec.encode(
                                mutation.recoveryToken()));
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, RecoveryOwnershipCodec.encodeToken(
                                mutation.runRecoveryToken()));
            } else if (mutation.command().kind()
                    == IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN) {
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, FrontierRecoveryTokenCodec.encode(
                                mutation.recoveryToken()));
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, RecoveryOwnershipCodec.encodeToken(
                                mutation.runRecoveryToken()));
                IncrementalAuthorityCodecSupport.writeText(
                        out, mutation.abortClosureProof()
                                .terminalStatus().name());
            } else {
                throw new IllegalArgumentException(
                        "File bootstrap WAL does not yet support this transition");
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory authority WAL encoding failed", impossible);
        }
    }

    private static byte[] encodeOwnership(
            String feedId, long expectedRevision, long expectedEpoch,
            RecoveryOwner owner) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeText(out, feedId);
            out.writeLong(expectedRevision);
            out.writeLong(expectedEpoch);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, RecoveryOwnershipCodec.encodeOwner(owner));
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory ownership WAL encoding failed", impossible);
        }
    }

    private static byte[] encodePending(
            FrontierRecoveryToken token, IncrementalClaimRequest request) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, FrontierRecoveryTokenCodec.encode(token));
            encodeClaimRequest(out, request);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory pending WAL encoding failed", impossible);
        }
    }

    private static byte[] encodeProgress(
            FrontierRecoveryToken token,
            IncrementalAttemptProgressCommand command) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, FrontierRecoveryTokenCodec.encode(token));
            IncrementalAuthorityCodecSupport.writeText(out, command.feedId());
            IncrementalAuthorityCodecSupport.writeText(out, command.intervalId());
            out.writeLong(command.expectedFrontierRevision());
            out.writeLong(command.expectedRunRevision());
            writeFence(out, command.fence());
            IncrementalAuthorityCodecSupport.writeText(
                    out, command.targetStatus().name());
            IncrementalAuthorityCodecSupport.writeText(out, command.commandId());
            out.writeLong(command.eventTimeMillis());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory progress WAL encoding failed", impossible);
        }
    }

    private static IncrementalAttemptProgressCommand decodeProgress(
            DataInputStream in) throws IOException {
        String feedId = IncrementalAuthorityCodecSupport.readText(
                in, "progress feed id");
        String intervalId = IncrementalAuthorityCodecSupport.readText(
                in, "progress interval id");
        long frontierRevision = in.readLong();
        long runRevision = in.readLong();
        RunAttemptFence fence = readFence(in);
        AttemptStatus target = AttemptStatus.valueOf(
                IncrementalAuthorityCodecSupport.readText(
                        in, "progress target"));
        String commandId = IncrementalAuthorityCodecSupport.readText(
                in, "progress command id");
        return new IncrementalAttemptProgressCommand(
                feedId, intervalId, frontierRevision, runRevision,
                fence, target, commandId, in.readLong());
    }

    private static byte[] encodeAssignments(StageAssignmentManifest manifest) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, StageAssignmentManifestCodec.encodeRoot(manifest));
            out.writeInt(manifest.shardCount());
            for (int i = 0; i < manifest.shardCount(); i++) {
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, StageAssignmentManifestCodec.encodeShard(
                                manifest, i));
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory assignment WAL encoding failed", impossible);
        }
    }

    private static StageAssignmentManifest decodeAssignments(
            DataInputStream in) throws IOException {
        byte[] root = IncrementalAuthorityCodecSupport.readBytes(
                in, StageAssignmentManifestCodec.MAX_ROOT_FRAME_BYTES,
                "assignment root");
        int count = in.readInt();
        if (count < 1 || count > StageAssignmentManifest.MAX_SHARDS) {
            throw new IllegalArgumentException(
                    "assignment shard count is invalid");
        }
        List<byte[]> shards = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            shards.add(IncrementalAuthorityCodecSupport.readBytes(
                    in, StageAssignmentManifestCodec.MAX_SHARD_FRAME_BYTES,
                    "assignment shard"));
        }
        return StageAssignmentManifestCodec.decode(root, shards);
    }

    private static byte[] encodeAcceptance(
            StageAssignmentAcceptance acceptance) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, StageAssignmentManifestCodec.encodeToken(
                            acceptance.token()));
            out.write(acceptance.commandDigest());
            out.writeLong(acceptance.acceptedAtMillis());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory acceptance WAL encoding failed", impossible);
        }
    }

    private static StageAssignmentAcceptance decodeAcceptance(
            DataInputStream in) throws IOException {
        return new StageAssignmentAcceptance(
                StageAssignmentManifestCodec.decodeToken(
                        IncrementalAuthorityCodecSupport.readBytes(
                                in, StageAssignmentManifestCodec.MAX_TOKEN_FRAME_BYTES,
                                "assignment token")),
                IncrementalAuthorityCodecSupport.readDigest(
                        in, "assignment command digest"),
                in.readLong());
    }

    private static byte[] encodeManifestMember(ManifestMemberReceipt receipt) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, StageAssignmentManifestCodec.encodeToken(
                            receipt.token()));
            out.write(receipt.commandDigest());
            IncrementalAuthorityCodecSupport.writeText(out, receipt.domainId());
            out.writeLong(receipt.generation());
            IncrementalAuthorityCodecSupport.writeText(
                    out, receipt.publicationId());
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, ManifestCommitWire.encodeMember(receipt.member()));
            out.writeLong(receipt.sealedAtMillis());
            out.writeBoolean(receipt.hasPreparedLocator());
            if (receipt.hasPreparedLocator()) {
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, PreparedSinkLocatorCodec.encode(
                                receipt.preparedLocator()));
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory Manifest member WAL encoding failed", impossible);
        }
    }

    private static ManifestMemberReceipt decodeManifestMember(
            DataInputStream in) throws IOException {
        io.rowweave.run.StageAssignmentToken token =
                StageAssignmentManifestCodec.decodeToken(
                        IncrementalAuthorityCodecSupport.readBytes(
                                in, StageAssignmentManifestCodec.MAX_TOKEN_FRAME_BYTES,
                                "member token"));
        byte[] commandDigest = IncrementalAuthorityCodecSupport.readDigest(
                in, "member command digest");
        String domainId = IncrementalAuthorityCodecSupport.readText(
                in, "member domain id");
        long generation = in.readLong();
        String publicationId = IncrementalAuthorityCodecSupport.readText(
                in, "member publication id");
        io.rowweave.commit.CommitManifest.Member member =
                ManifestCommitWire.decodeMember(
                        IncrementalAuthorityCodecSupport.readBytes(
                                in, PreparedSinkLocatorCodec.MAX_FRAME_BYTES,
                                "Manifest member"));
        long sealedAt = in.readLong();
        if (!in.readBoolean()) {
            return ManifestMemberReceipt.sealed(
                    token, commandDigest, domainId, generation,
                    publicationId, member, sealedAt);
        }
        PreparedSinkLocator locator = PreparedSinkLocatorCodec.decode(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, PreparedSinkLocatorCodec.MAX_FRAME_BYTES,
                        "prepared Sink locator"));
        return ManifestMemberReceipt.sealed(
                token, commandDigest, domainId, generation,
                publicationId, member, sealedAt, locator);
    }

    private static byte[] encodeRunClaim(
            String runId, long expectedRevision, long expectedEpoch,
            RecoveryOwner owner) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeText(out, runId);
            out.writeLong(expectedRevision);
            out.writeLong(expectedEpoch);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, RecoveryOwnershipCodec.encodeOwner(owner));
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory Run claim WAL encoding failed", impossible);
        }
    }

    private static byte[] encodeAbort(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            BatchSinkAbortRecord record) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, FrontierRecoveryTokenCodec.encode(frontierToken));
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, RecoveryOwnershipCodec.encodeToken(runToken));
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, BatchSinkAbortRecordCodec.encode(record));
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory abort WAL encoding failed", impossible);
        }
    }

    private static byte[] encodeRetrySchedule(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            RunAttemptFence fence, WholeAttemptRetryDecision result) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, FrontierRecoveryTokenCodec.encode(frontierToken));
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, RecoveryOwnershipCodec.encodeToken(runToken));
            writeFence(out, fence);
            IncrementalAuthorityCodecSupport.writeText(
                    out, result.code().name());
            out.writeLong(result.observedAtMillis());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory retry schedule WAL encoding failed", impossible);
        }
    }

    private static byte[] encodeRetryClaim(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId, RetryAttemptClaim result) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, FrontierRecoveryTokenCodec.encode(frontierToken));
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, RecoveryOwnershipCodec.encodeToken(runToken));
            IncrementalAuthorityCodecSupport.writeText(out, runId);
            out.writeLong(result.observedAtMillis());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory retry claim WAL encoding failed", impossible);
        }
    }

    private static byte[] encodeRecoveredTerminal(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageTerminalReceipt receipt) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, FrontierRecoveryTokenCodec.encode(frontierToken));
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, RecoveryOwnershipCodec.encodeToken(runToken));
            out.write(StageAssignmentManifestCodec.encodeTerminalReceipt(receipt));
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory recovered terminal WAL encoding failed", impossible);
        }
    }

    private static byte[] encodeRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            byte[] body) {
        if (frontierToken == null || runToken == null || body == null) {
            throw new IllegalArgumentException(
                    "recovered WAL fields are required");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, FrontierRecoveryTokenCodec.encode(frontierToken));
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, RecoveryOwnershipCodec.encodeToken(runToken));
            out.write(body);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory recovered WAL encoding failed", impossible);
        }
    }

    private static byte[] encodeCancellation(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId, String requestId, long observedAtMillis) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, FrontierRecoveryTokenCodec.encode(frontierToken));
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, RecoveryOwnershipCodec.encodeToken(runToken));
            IncrementalAuthorityCodecSupport.writeText(out, runId);
            IncrementalAuthorityCodecSupport.writeText(out, requestId);
            out.writeLong(observedAtMillis);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory cancellation WAL encoding failed", impossible);
        }
    }

    private static byte[] encodeDeadline(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId, long observedAtMillis) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, FrontierRecoveryTokenCodec.encode(frontierToken));
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, RecoveryOwnershipCodec.encodeToken(runToken));
            IncrementalAuthorityCodecSupport.writeText(out, runId);
            out.writeLong(observedAtMillis);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory deadline WAL encoding failed", impossible);
        }
    }

    private static IncrementalAuthorityCommand readCommand(
            DataInputStream in) throws IOException {
        return IncrementalAuthorityCommandCodec.decode(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, IncrementalAuthorityCommandCodec.MAX_FRAME_BYTES,
                        "authority command"));
    }

    private static FrontierRecoveryToken readFrontierToken(
            DataInputStream in) throws IOException {
        return FrontierRecoveryTokenCodec.decode(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, FrontierRecoveryTokenCodec.MAX_FRAME_BYTES,
                        "frontier recovery token"));
    }

    private static RecoveryToken readRunToken(DataInputStream in)
            throws IOException {
        return RecoveryOwnershipCodec.decodeToken(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, RecoveryOwnershipCodec.MAX_FRAME_BYTES,
                        "Run recovery token"));
    }

    private static RecoveryOwner readRecoveryOwner(DataInputStream in)
            throws IOException {
        return RecoveryOwnershipCodec.decodeOwner(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, RecoveryOwnershipCodec.MAX_FRAME_BYTES,
                        "recovery owner"));
    }

    private static void writeFence(
            DataOutputStream out, RunAttemptFence fence) throws IOException {
        IncrementalAuthorityCodecSupport.writeText(out, fence.runId());
        IncrementalAuthorityCodecSupport.writeText(out, fence.attemptId());
        out.writeLong(fence.fenceToken());
    }

    private static RunAttemptFence readFence(DataInputStream in)
            throws IOException {
        return new RunAttemptFence(
                IncrementalAuthorityCodecSupport.readText(in, "fence run id"),
                IncrementalAuthorityCodecSupport.readText(in, "fence attempt id"),
                in.readLong());
    }

    private static byte[] readRemaining(
            DataInputStream in, int maxBytes, String field) throws IOException {
        int remaining = in.available();
        if (remaining < 1 || remaining > maxBytes) {
            throw new IllegalArgumentException(field + " length is invalid");
        }
        byte[] bytes = new byte[remaining];
        in.readFully(bytes);
        return bytes;
    }

    private static void encodeInitialize(
            DataOutputStream out, IncrementalInitializeRequest request)
            throws IOException {
        IncrementalAuthorityCodecSupport.writeText(out, request.feedId());
        out.writeLong(request.feedIncarnation());
        out.write(request.definitionDigest());
        out.write(request.sourceContractDigest());
        out.write(request.frontierContractDigest());
        out.writeByte(request.frontierKind().wireId());
    }

    private static IncrementalInitializeRequest decodeInitialize(
            DataInputStream in) throws IOException {
        return new IncrementalInitializeRequest(
                IncrementalAuthorityCodecSupport.readText(in, "feed id"),
                in.readLong(),
                IncrementalAuthorityCodecSupport.readDigest(
                        in, "definition digest"),
                IncrementalAuthorityCodecSupport.readDigest(
                        in, "source contract digest"),
                IncrementalAuthorityCodecSupport.readDigest(
                        in, "frontier contract digest"),
                IncrementalFrontier.Kind.fromWire(in.readUnsignedByte()));
    }

    private static void encodeClaimRequest(
            DataOutputStream out, IncrementalClaimRequest request)
            throws IOException {
        IncrementalAuthorityCodecSupport.writeText(out, request.feedId());
        IncrementalAuthorityCodecSupport.writeBytes(
                out, CapturedTargetReceiptCodec.encode(
                        request.capturedTarget()));
        out.write(request.frontierContractDigest());
        IncrementalAuthorityCodecSupport.writeText(out, request.artifactId());
        IncrementalAuthorityCodecSupport.writeText(out, request.attemptId());
        IncrementalAuthorityCodecSupport.writeText(out, request.publicationId());
        out.writeInt(request.maxAttempts());
        out.writeLong(request.deadlineEpochMillis());
        out.writeLong(request.eventTimeMillis());
    }

    private static IncrementalClaimRequest decodeClaimRequest(
            DataInputStream in) throws IOException {
        String feedId = IncrementalAuthorityCodecSupport.readText(
                in, "claim feed id");
        CapturedTargetReceipt capture = CapturedTargetReceiptCodec.decode(
                IncrementalAuthorityCodecSupport.readBytes(
                        in, CapturedTargetReceiptCodec.MAX_FRAME_BYTES,
                        "captured target receipt"));
        byte[] contract = IncrementalAuthorityCodecSupport.readDigest(
                in, "frontier contract digest");
        String artifactId = IncrementalAuthorityCodecSupport.readText(
                in, "artifact id");
        String attemptId = IncrementalAuthorityCodecSupport.readText(
                in, "attempt id");
        String publicationId = IncrementalAuthorityCodecSupport.readText(
                in, "publication id");
        return new IncrementalClaimRequest(
                feedId, capture, contract, artifactId, attemptId,
                publicationId, in.readInt(), in.readLong(), in.readLong());
    }

    private static byte[] frame(int kind, byte[] payload) {
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "incremental authority WAL payload exceeds bound");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(
                    HEADER_BYTES + payload.length);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.writeInt(MAGIC);
            body.writeByte(VERSION);
            body.writeByte(kind);
            body.writeInt(payload.length);
            body.write(payload);
            body.flush();
            byte[] canonical = bodyBytes.toByteArray();
            ByteArrayOutputStream framed = new ByteArrayOutputStream(
                    canonical.length + CHECKSUM_BYTES);
            framed.write(canonical);
            framed.write(IncrementalAuthorityValue.sha256(canonical));
            return framed.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory authority WAL frame failed", impossible);
        }
    }

    private void append(FileChannel channel, byte[] frame)
            throws IOException {
        long size = channel.size();
        if (size > MAX_WAL_BYTES - frame.length) {
            throw new IllegalStateException(
                    "incremental authority WAL exceeds its bounded size");
        }
        channel.position(size);
        ByteBuffer bytes = ByteBuffer.wrap(frame);
        while (bytes.hasRemaining()) channel.write(bytes);
        force(channel);
        loadedWalSize = size + frame.length;
        loadedFileKey = Files.readAttributes(
                wal, BasicFileAttributes.class).fileKey();
        lastReplayFrameCount++;
        maybeCheckpoint(frame[5] & 0xff);
    }

    private void maybeCheckpoint(int appendedKind) throws IOException {
        boolean terminal = appendedKind == JOINT_SUCCESS
                || appendedKind == JOINT_ABORT
                || appendedKind == REQUEST_CANCELLATION
                || appendedKind == EVALUATE_DEADLINE
                || appendedKind == SCHEDULE_RETRY;
        if (lastReplayFrameCount < CHECKPOINT_INTERVAL && !terminal) return;
        InMemoryIncrementalAuthorityStore.FileReadyCheckpoint checkpoint =
                delegate.fileReadyCheckpoint();
        if (checkpoint == null) return;
        byte[] payload = encodeReadyCheckpoint(checkpoint);
        if (payload.length > MAX_PAYLOAD_BYTES) return;
        byte[] checkpointFrame = frame(READY_CHECKPOINT, payload);
        for (InMemoryIncrementalAuthorityStore.FileCheckpointRun run
                : checkpoint.runs) {
            persistRunHistory(run.snapshot);
        }
        Path temporary = root.resolve("authority.wal.checkpoint.tmp");
        try {
            try (FileChannel output = FileChannel.open(
                    temporary, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                ByteBuffer bytes = ByteBuffer.wrap(checkpointFrame);
                while (bytes.hasRemaining()) output.write(bytes);
                force(output);
            }
            try {
                Files.move(temporary, wal,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IllegalStateException(
                        "incremental authority checkpoint requires atomic replace",
                        unsupported);
            }
            if (forceDurability) forceDirectory(root);
            BasicFileAttributes attributes = Files.readAttributes(
                    wal, BasicFileAttributes.class);
            loadedFileKey = attributes.fileKey();
            loadedWalSize = checkpointFrame.length;
            lastReplayFrameCount = 1;
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private void force(FileChannel channel) throws IOException {
        if (forceDurability) channel.force(true);
    }

    private void persistRunHistory(RunSnapshot snapshot) throws IOException {
        byte[] canonical = RunStateCodec.encodeSnapshot(snapshot);
        Path target = runHistoryPath(snapshot.runId());
        if (Files.exists(target)) {
            byte[] existing = readBoundedRunHistory(target);
            if (!MessageDigest.isEqual(existing, canonical)) {
                throw new IllegalStateException(
                        "incremental Run history identity conflict");
            }
            return;
        }
        Path temporary = runHistoryRoot.resolve(
                target.getFileName().toString() + ".tmp");
        try {
            try (FileChannel output = FileChannel.open(
                    temporary, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                ByteBuffer bytes = ByteBuffer.wrap(canonical);
                while (bytes.hasRemaining()) output.write(bytes);
                force(output);
            }
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.FileAlreadyExistsException raced) {
                byte[] existing = readBoundedRunHistory(target);
                if (!MessageDigest.isEqual(existing, canonical)) {
                    throw new IllegalStateException(
                            "incremental Run history identity conflict", raced);
                }
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IllegalStateException(
                        "incremental Run history requires atomic install",
                        unsupported);
            }
            if (forceDurability) forceDirectory(runHistoryRoot);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private RunSnapshot readRunHistory(String runId) throws IOException {
        Path path = runHistoryPath(runId);
        if (!Files.exists(path)) return null;
        RunSnapshot snapshot = RunStateCodec.decodeSnapshot(
                readBoundedRunHistory(path));
        if (!runId.equals(snapshot.runId())) {
            throw new IllegalStateException(
                    "incremental Run history hash identity mismatch");
        }
        return snapshot;
    }

    private static byte[] readBoundedRunHistory(Path path) throws IOException {
        long size = Files.size(path);
        if (size < 1L || size > RunStateCodec.MAX_FRAME_BYTES) {
            throw new IllegalStateException(
                    "incremental Run history frame exceeds bound");
        }
        return Files.readAllBytes(path);
    }

    private Path runHistoryPath(String runId) {
        byte[] digest = IncrementalAuthorityValue.sha256(
                runId.getBytes(StandardCharsets.UTF_8));
        char[] name = new char[digest.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < digest.length; i++) {
            int value = digest[i] & 0xff;
            name[i * 2] = digits[value >>> 4];
            name[i * 2 + 1] = digits[value & 0xf];
        }
        return runHistoryRoot.resolve(new String(name) + ".run");
    }

    private <T> T locked(FileOperation<T> operation) {
        jvmLock.lock();
        try (FileChannel lockChannel = FileChannel.open(
                lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = lockChannel.lock();
             FileChannel walChannel = FileChannel.open(
                     wal, StandardOpenOption.CREATE, StandardOpenOption.READ,
                     StandardOpenOption.WRITE)) {
            return operation.run(walChannel);
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "incremental authority file operation failed", failure);
        } finally {
            jvmLock.unlock();
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        if (directory == null) return;
        try (FileChannel channel = FileChannel.open(
                directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private interface FileOperation<T> {
        T run(FileChannel channel) throws IOException;
    }
}
