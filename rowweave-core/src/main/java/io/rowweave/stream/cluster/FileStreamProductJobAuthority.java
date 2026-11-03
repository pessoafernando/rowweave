package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Checksummed file authority for product submission, lifecycle and cancel identity. */
public final class FileStreamProductJobAuthority {
    private static final Object JVM_FILE_LOCK = new Object();
    private static final byte[] MAGIC =
            "RWXXSPJ1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 8;
    private static final int EVIDENCE_VERSION = 7;
    private static final int EVIDENCE_PREVIOUS_VERSION = 6;
    private static final int RESCALE_VERSION = 5;
    private static final int RECOVERY_VERSION = 4;
    private static final int SAVEPOINT_VERSION = 3;
    private static final int PREVIOUS_VERSION = 2;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_TEXT_BYTES = 1_024;
    private static final long MAX_FRAME_BYTES =
            PlanArtifactCodec.MAX_FRAME_BYTES + 256L * 1_024L;

    private final Path root;
    private final StreamProductJobStateStore stateStore;

    public FileStreamProductJobAuthority(Path root) {
        if (root == null) {
            throw new IllegalArgumentException("Stream product authority root is required");
        }
        try {
            Files.createDirectories(root);
            if (!Files.isDirectory(root)) {
                throw new IOException("authority root is not a directory");
            }
            this.root = root.toRealPath();
            this.stateStore = null;
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Stream product authority root is unavailable", failure);
        }
    }

    /** Uses a shared linearizable store while preserving the same semantics. */
    public FileStreamProductJobAuthority(
            StreamProductJobStateStore stateStore) {
        if (stateStore == null) {
            throw new IllegalArgumentException(
                    "Stream product job state store is required");
        }
        this.root = null;
        this.stateStore = stateStore;
    }

    public StreamProductJobSnapshot publish(
            StreamSubmissionId submissionId, PlanArtifact artifact) {
        if (submissionId == null || artifact == null) {
            throw new IllegalArgumentException(
                    "Stream submission identity and Artifact are required");
        }
        PlanArtifactValidator.validateForStreamProductExecution(artifact);
        StreamJobId jobId = deriveJobId(submissionId);
        StreamProductJobSnapshot created = new StreamProductJobSnapshot(
                submissionId, jobId, artifact, StreamJobStatus.SUBMITTED,
                StreamJobProgress.empty(), null, null);
        if (stateStore != null) {
            StreamProductJobSnapshot existing =
                    stateStore.createIfAbsent(created);
            requirePublished(existing, submissionId, artifact, null);
            return existing;
        }
        return locked(jobId, () -> {
            Path path = statePath(jobId);
            if (Files.exists(path)) {
                StreamProductJobSnapshot existing = read(path);
                if (!existing.submissionId().equals(submissionId)
                        || !existing.artifactId().equals(artifact.artifactId())
                        || existing.recoveryOrNull() != null) {
                    throw failure(StreamProductControlException.Code.SUBMISSION_CONFLICT,
                            "Stream submission identity is bound to another Artifact");
                }
                return existing;
            }
            write(path, created);
            return created;
        });
    }

    public StreamProductJobSnapshot publishRecovery(
            StreamSubmissionId submissionId, PlanArtifact artifact,
            StreamProductRecovery recovery) {
        if (submissionId == null || artifact == null || recovery == null) {
            throw new IllegalArgumentException(
                    "Stream recovery submission is required");
        }
        PlanArtifactValidator.validateForStreamProductExecution(artifact);
        PortableStreamJob target =
                PortableStreamJobArtifact.decodeValidated(artifact);
        if (target.plan().parallelism() != recovery.targetParallelism()
                || target.plan().maxParallelism()
                != recovery.targetMaxParallelism()) {
            throw failure(StreamProductControlException.Code.INVALID_STATE,
                    "recovery parallelism differs from the target Artifact");
        }
        StreamProductJobSnapshot source = byJob(recovery.sourceJobId());
        requireCompletedRecoveryPoint(source, recovery);
        if (recovery.offlineRescale()
                && (recovery.recoveryPointId().kind()
                != io.rowweave.stream.api.StreamRecoveryPointId.Kind.SAVEPOINT
                || source.status()
                != StreamJobStatus.STOPPED_WITH_SAVEPOINT)) {
            throw failure(StreamProductControlException.Code.INVALID_STATE,
                    "offline rescale requires a stopped source savepoint");
        }
        if (!recovery.offlineRescale()
                && !source.artifactId().equals(artifact.artifactId())) {
            throw failure(StreamProductControlException.Code.INVALID_STATE,
                    "restore requires the exact source Artifact");
        }
        StreamJobId jobId = deriveJobId(submissionId);
        StreamProductJobSnapshot created = new StreamProductJobSnapshot(
                submissionId, jobId, artifact,
                StreamJobStatus.SUBMITTED, StreamJobProgress.empty(),
                null, null, java.util.Collections.emptyList(), null,
                recovery);
        if (stateStore != null) {
            StreamProductJobSnapshot existing =
                    stateStore.createIfAbsent(created);
            requirePublished(existing, submissionId, artifact, recovery);
            return existing;
        }
        return locked(jobId, () -> {
            Path path = statePath(jobId);
            if (Files.exists(path)) {
                StreamProductJobSnapshot existing = read(path);
                if (!existing.submissionId().equals(submissionId)
                        || !existing.artifactId().equals(artifact.artifactId())
                        || !recovery.equals(existing.recoveryOrNull())) {
                    throw failure(
                            StreamProductControlException.Code.SUBMISSION_CONFLICT,
                            "Stream recovery identity is bound to another request");
                }
                return existing;
            }
            write(path, created);
            return created;
        });
    }

    private static void requireCompletedRecoveryPoint(
            StreamProductJobSnapshot source,
            StreamProductRecovery recovery) {
        String value = recovery.recoveryPointId().value();
        if (recovery.recoveryPointId().kind()
                == io.rowweave.stream.api.StreamRecoveryPointId.Kind.CHECKPOINT) {
            for (StreamProductCheckpointOperation checkpoint
                    : source.checkpoints()) {
                if (checkpoint.checkpointId().equals(value)
                        && checkpoint.status()
                        == StreamOperationStatus.SUCCEEDED) return;
            }
        } else {
            StreamProductSavepointOperation savepoint =
                    source.savepointOrNull();
            if (savepoint != null && savepoint.savepointId().equals(value)
                    && savepoint.status()
                    == StreamOperationStatus.SUCCEEDED) return;
        }
        throw failure(StreamProductControlException.Code.INVALID_STATE,
                "Stream recovery point is not completed and authoritative");
    }

    public StreamProductJobSnapshot bySubmission(StreamSubmissionId submissionId) {
        if (submissionId == null) {
            throw new IllegalArgumentException("submissionId is required");
        }
        return byJob(deriveJobId(submissionId));
    }

    public StreamProductJobSnapshot byJob(StreamJobId jobId) {
        if (jobId == null) throw new IllegalArgumentException("jobId is required");
        if (stateStore != null) {
            StreamProductJobSnapshot snapshot = stateStore.current(jobId);
            if (snapshot == null) {
                throw failure(StreamProductControlException.Code.JOB_NOT_FOUND,
                        "Stream product job does not exist");
            }
            requireJobIdentity(jobId, snapshot);
            return snapshot;
        }
        return locked(jobId, () -> {
            Path path = statePath(jobId);
            if (!Files.exists(path)) {
                throw failure(StreamProductControlException.Code.JOB_NOT_FOUND,
                        "Stream product job does not exist");
            }
            StreamProductJobSnapshot snapshot = read(path);
            if (!snapshot.jobId().equals(jobId)) {
                throw failure(StreamProductControlException.Code.INVALID_FRAME,
                        "Stream product job identity differs from its authority path");
            }
            return snapshot;
        });
    }

    java.util.List<StreamProductJobSnapshot> all() {
        if (stateStore != null) {
            java.util.List<StreamProductJobSnapshot> snapshots =
                    new java.util.ArrayList<>(stateStore.all());
            for (StreamProductJobSnapshot snapshot : snapshots) {
                requireJobIdentity(snapshot.jobId(), snapshot);
            }
            snapshots.sort((left, right) -> left.jobId().value()
                    .compareTo(right.jobId().value()));
            return java.util.Collections.unmodifiableList(snapshots);
        }
        java.util.List<StreamJobId> jobIds = new java.util.ArrayList<>();
        try (java.nio.file.DirectoryStream<Path> entries =
                     Files.newDirectoryStream(root, "product-job-*.bin")) {
            for (Path path : entries) {
                StreamProductJobSnapshot snapshot = read(path);
                if (!path.equals(statePath(snapshot.jobId()))) {
                    throw failure(StreamProductControlException.Code.INVALID_FRAME,
                            "Stream product job authority path is noncanonical");
                }
                jobIds.add(snapshot.jobId());
            }
        } catch (StreamProductControlException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Stream product authority catalog is unavailable", failure);
        }
        jobIds.sort((left, right) -> left.value().compareTo(right.value()));
        java.util.List<StreamProductJobSnapshot> snapshots =
                new java.util.ArrayList<>(jobIds.size());
        for (StreamJobId jobId : jobIds) snapshots.add(byJob(jobId));
        return java.util.Collections.unmodifiableList(snapshots);
    }

    public StreamProductJobSnapshot beginRestart(StreamJobId jobId) {
        return mutate(jobId, current -> {
            if (current.status() != StreamJobStatus.RUNNING) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "only a running Stream job can restart");
            }
            return current.beginRestart();
        });
    }

    public StreamProductJobSnapshot transition(
            StreamJobId jobId, StreamJobStatus next) {
        if (next == null) throw new IllegalArgumentException("next status is required");
        return mutate(jobId, current -> {
            if (current.status().terminal() || current.status() == next) {
                return current;
            }
            if (!allowed(current.status(), next)) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "Illegal Stream lifecycle transition "
                                + current.status() + " -> " + next);
            }
            return current.withStatus(next);
        });
    }

    public StreamProductJobSnapshot updateCdcPhase(
            StreamJobId jobId, StreamCdcPhase phase) {
        if (phase == null) {
            throw new IllegalArgumentException("Stream CDC phase is required");
        }
        return mutate(jobId, current -> current.withCdcPhase(phase));
    }

    public StreamProductJobSnapshot updateEvidence(
            StreamJobId jobId, StreamJobEvidence evidence) {
        if (jobId == null || evidence == null
                || !jobId.equals(evidence.jobId())) {
            throw new IllegalArgumentException(
                    "Stream job evidence identity is invalid");
        }
        return mutate(jobId, current -> current.withEvidence(evidence));
    }

    public StreamProductJobSnapshot acceptCancel(
            StreamJobId jobId, StreamCommandId commandId) {
        if (commandId == null) {
            throw new IllegalArgumentException("cancel command ID is required");
        }
        return mutate(jobId, current -> {
            if (current.cancelCommandId() != null) {
                if (current.cancelCommandId().equals(commandId)) {
                    rejectReboundCommand(current, commandId,
                            CommandKind.CANCEL);
                    return current;
                }
                throw failure(StreamProductControlException.Code.COMMAND_CONFLICT,
                        "Stream job already has another cancel command");
            }
            rejectReboundCommand(current, commandId, CommandKind.CANCEL);
            requireNoRescaleInFlight(current, "cancellation");
            if (current.status().terminal()) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "terminal Stream job cannot accept cancellation");
            }
            if (!allowed(current.status(), StreamJobStatus.STOPPING)) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "Stream job cannot enter STOPPING from " + current.status());
            }
            return current.withCancel(commandId,
                    StreamOperationStatus.ACCEPTED, StreamJobStatus.STOPPING);
        });
    }

    public StreamProductJobSnapshot completeCancel(
            StreamJobId jobId, StreamCommandId commandId) {
        if (commandId == null) {
            throw new IllegalArgumentException("cancel command ID is required");
        }
        return mutate(jobId, current -> {
            if (!commandId.equals(current.cancelCommandId())) {
                throw failure(StreamProductControlException.Code.COMMAND_CONFLICT,
                        "Stream cancel command identity differs");
            }
            if (current.status() == StreamJobStatus.CANCELED
                    && current.cancelStatusOrNull()
                    == StreamOperationStatus.SUCCEEDED) {
                return current;
            }
            if (current.status().terminal()) return current;
            if (current.status() != StreamJobStatus.STOPPING) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "Stream cancel can complete only from STOPPING");
            }
            return current.withCancel(commandId,
                    StreamOperationStatus.SUCCEEDED, StreamJobStatus.CANCELED);
        });
    }

    public StreamProductJobSnapshot acceptCheckpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        if (commandId == null) {
            throw new IllegalArgumentException(
                    "checkpoint command ID is required");
        }
        return mutate(jobId, current -> {
            for (StreamProductCheckpointOperation operation
                    : current.checkpoints()) {
                if (operation.commandId().equals(commandId)) {
                    rejectReboundCommand(current, commandId,
                            CommandKind.CHECKPOINT);
                    return current;
                }
                if (operation.status() == StreamOperationStatus.ACCEPTED
                        || operation.status()
                        == StreamOperationStatus.RUNNING) {
                    throw failure(
                            StreamProductControlException.Code.INVALID_STATE,
                            "Stream job already has a checkpoint in flight");
                }
            }
            rejectReboundCommand(current, commandId,
                    CommandKind.CHECKPOINT);
            requireNoRescaleInFlight(current, "checkpoint");
            if (current.status() != StreamJobStatus.RUNNING) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "checkpoint requires a RUNNING Stream job");
            }
            StreamProductCheckpointOperation accepted =
                    new StreamProductCheckpointOperation(commandId,
                            checkpointId(jobId, commandId),
                            StreamOperationStatus.ACCEPTED,
                            System.currentTimeMillis());
            return current.withCheckpoint(accepted, false);
        });
    }

    public StreamProductJobSnapshot startCheckpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return checkpointTransition(jobId, commandId,
                StreamOperationStatus.ACCEPTED,
                StreamOperationStatus.RUNNING, false, null);
    }

    public StreamProductJobSnapshot completeCheckpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return completeCheckpoint(jobId, commandId, null);
    }

    public StreamProductJobSnapshot completeCheckpoint(
            StreamJobId jobId, StreamCommandId commandId,
            StreamCdcPhase cdcPhase) {
        return checkpointTransition(jobId, commandId,
                StreamOperationStatus.RUNNING,
                StreamOperationStatus.SUCCEEDED, true, cdcPhase);
    }

    public StreamProductJobSnapshot failCheckpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return mutate(jobId, current -> {
            StreamProductCheckpointOperation operation =
                    current.checkpoint(commandId);
            if (operation.status() == StreamOperationStatus.FAILED
                    || operation.status() == StreamOperationStatus.SUCCEEDED) {
                return current;
            }
            return retainConfigured(current.withCheckpoint(
                    operation.withStatus(StreamOperationStatus.FAILED), false));
        });
    }

    private StreamProductJobSnapshot checkpointTransition(
            StreamJobId jobId, StreamCommandId commandId,
            StreamOperationStatus expected, StreamOperationStatus next,
            boolean completed, StreamCdcPhase cdcPhase) {
        if (commandId == null) {
            throw new IllegalArgumentException(
                    "checkpoint command ID is required");
        }
        return mutate(jobId, current -> {
            StreamProductCheckpointOperation operation =
                    current.checkpoint(commandId);
            if (operation.status() == next
                    || (next == StreamOperationStatus.RUNNING
                    && operation.status() == StreamOperationStatus.SUCCEEDED)) {
                return current;
            }
            if (operation.status() != expected) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "checkpoint command transition is invalid");
            }
            StreamProductJobSnapshot transitioned = current.withCheckpoint(
                    operation.withStatus(next),
                    completed, cdcPhase == null
                            ? current.progress().cdcPhase() : cdcPhase);
            return completed ? retainConfigured(transitioned) : transitioned;
        });
    }

    private static StreamProductJobSnapshot retainConfigured(
            StreamProductJobSnapshot snapshot) {
        int retained = PortableStreamJobArtifact.decodeValidated(
                snapshot.artifact()).checkpointConfig().retainedCompleted();
        return snapshot.retainCheckpointHistory(retained);
    }

    public StreamProductJobSnapshot acceptSavepoint(
            StreamJobId jobId, StreamCommandId commandId,
            String requestDigest) {
        if (commandId == null || requestDigest == null
                || !requestDigest.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(
                    "savepoint command identity and request digest are required");
        }
        return mutate(jobId, current -> {
            StreamProductSavepointOperation existing =
                    current.savepointOrNull();
            if (existing != null) {
                if (existing.commandId().equals(commandId)
                        && existing.requestDigest().equals(requestDigest)) {
                    rejectReboundCommand(current, commandId,
                            CommandKind.SAVEPOINT);
                    return current;
                }
                throw failure(StreamProductControlException.Code.COMMAND_CONFLICT,
                        "Stream job already has another savepoint command");
            }
            rejectReboundCommand(current, commandId, CommandKind.SAVEPOINT);
            requireNoRescaleInFlight(current, "savepoint");
            if (current.status() != StreamJobStatus.RUNNING) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "savepoint requires a RUNNING Stream job");
            }
            for (StreamProductCheckpointOperation checkpoint
                    : current.checkpoints()) {
                if (checkpoint.status() == StreamOperationStatus.ACCEPTED
                        || checkpoint.status()
                        == StreamOperationStatus.RUNNING) {
                    throw failure(
                            StreamProductControlException.Code.INVALID_STATE,
                            "savepoint cannot overlap a checkpoint");
                }
            }
            return current.withSavepoint(new StreamProductSavepointOperation(
                    commandId, savepointId(jobId, commandId, requestDigest),
                    requestDigest, StreamOperationStatus.ACCEPTED),
                    StreamJobStatus.RUNNING);
        });
    }

    private static void rejectReboundCommand(
            StreamProductJobSnapshot snapshot, StreamCommandId commandId,
            CommandKind expectedKind) {
        if (expectedKind != CommandKind.CANCEL
                && commandId.equals(snapshot.cancelCommandId())) {
            commandConflict();
        }
        StreamProductSavepointOperation savepoint =
                snapshot.savepointOrNull();
        if (expectedKind != CommandKind.SAVEPOINT && savepoint != null
                && commandId.equals(savepoint.commandId())) {
            commandConflict();
        }
        if (expectedKind != CommandKind.CHECKPOINT) {
            for (StreamProductCheckpointOperation checkpoint
                    : snapshot.checkpoints()) {
                if (commandId.equals(checkpoint.commandId())) {
                    commandConflict();
                }
            }
        }
        if (expectedKind != CommandKind.RESCALE) {
            for (StreamProductRescaleOperation rescale
                    : snapshot.rescales()) {
                if (commandId.equals(rescale.commandId())) {
                    commandConflict();
                }
            }
        }
    }

    private static void commandConflict() {
        throw failure(StreamProductControlException.Code.COMMAND_CONFLICT,
                "Stream command identity is bound to another operation");
    }

    private enum CommandKind {
        CANCEL,
        CHECKPOINT,
        SAVEPOINT,
        RESCALE
    }

    public StreamProductJobSnapshot acceptRescale(
            StreamJobId jobId, StreamCommandId commandId,
            int targetParallelism, long sourceGeneration) {
        if (commandId == null || targetParallelism < 1
                || sourceGeneration < 1L) {
            throw new IllegalArgumentException(
                    "live rescale command is invalid");
        }
        return mutate(jobId, current -> {
            StreamProductRescaleOperation existing = current.rescaleOrNull();
            if (existing != null) {
                if (existing.status() == StreamOperationStatus.ACCEPTED
                        || existing.status() == StreamOperationStatus.RUNNING) {
                    if (existing.commandId().equals(commandId)
                            && existing.targetParallelism()
                            == targetParallelism
                            && existing.sourceGeneration()
                            == sourceGeneration) {
                        rejectReboundCommand(current, commandId,
                                CommandKind.RESCALE);
                        return current;
                    }
                    throw failure(
                            StreamProductControlException.Code.COMMAND_CONFLICT,
                            "Stream job already has another rescale command");
                }
                for (StreamProductRescaleOperation historical
                        : current.rescales()) {
                    if (!historical.commandId().equals(commandId)) continue;
                    if (historical.targetParallelism() == targetParallelism
                            && historical.sourceGeneration()
                            == sourceGeneration) return current;
                    throw failure(StreamProductControlException.Code
                                    .COMMAND_CONFLICT,
                            "Stream rescale command identity was rebound");
                }
            }
            rejectReboundCommand(current, commandId, CommandKind.RESCALE);
            if (current.status() != StreamJobStatus.RUNNING) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "live rescale requires a RUNNING Stream job");
            }
            for (StreamProductCheckpointOperation checkpoint
                    : current.checkpoints()) {
                if (checkpoint.status() == StreamOperationStatus.ACCEPTED
                        || checkpoint.status()
                        == StreamOperationStatus.RUNNING) {
                    throw failure(StreamProductControlException.Code.INVALID_STATE,
                            "live rescale cannot overlap a checkpoint");
                }
            }
            StreamProductSavepointOperation savepoint =
                    current.savepointOrNull();
            if (savepoint != null
                    && (savepoint.status() == StreamOperationStatus.ACCEPTED
                    || savepoint.status() == StreamOperationStatus.RUNNING)) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "live rescale cannot overlap a savepoint");
            }
            int maximum = PortableStreamJobArtifact.decodeValidated(
                    current.artifact()).plan().maxParallelism();
            if (targetParallelism > maximum) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "live rescale target exceeds maxParallelism");
            }
            StreamProductRescaleOperation operation =
                    new StreamProductRescaleOperation(commandId,
                            rescaleId(jobId, commandId, targetParallelism,
                                    sourceGeneration),
                            targetParallelism, sourceGeneration,
                            Math.addExact(sourceGeneration, 1L),
                            StreamOperationStatus.ACCEPTED,
                            StreamProductRescaleOperation.Phase.INTENT_DURABLE);
            return current.withRescale(operation);
        });
    }

    public StreamProductJobSnapshot startRescale(
            StreamJobId jobId, StreamCommandId commandId) {
        return rescaleTransition(jobId, commandId,
                StreamProductRescaleOperation.Phase.INTENT_DURABLE,
                StreamOperationStatus.RUNNING,
                StreamProductRescaleOperation.Phase.CHECKPOINTING);
    }

    public StreamProductJobSnapshot decideRescaleCutover(
            StreamJobId jobId, StreamCommandId commandId) {
        return rescaleTransition(jobId, commandId,
                StreamProductRescaleOperation.Phase.CHECKPOINTING,
                StreamOperationStatus.RUNNING,
                StreamProductRescaleOperation.Phase.CUTOVER_DURABLE);
    }

    public StreamProductJobSnapshot completeRescale(
            StreamJobId jobId, StreamCommandId commandId) {
        return rescaleTransition(jobId, commandId,
                StreamProductRescaleOperation.Phase.CUTOVER_DURABLE,
                StreamOperationStatus.SUCCEEDED,
                StreamProductRescaleOperation.Phase.CUTOVER_COMPLETE);
    }

    public StreamProductJobSnapshot failRescale(
            StreamJobId jobId, StreamCommandId commandId) {
        return mutate(jobId, current -> {
            StreamProductRescaleOperation operation =
                    current.rescale(commandId);
            if (operation.phase()
                    == StreamProductRescaleOperation.Phase.FAILED) {
                return current;
            }
            if (operation.phase()
                    == StreamProductRescaleOperation.Phase.CUTOVER_DURABLE
                    || operation.phase()
                    == StreamProductRescaleOperation.Phase.CUTOVER_COMPLETE) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "durable rescale cutover cannot roll back");
            }
            return current.withRescale(operation.with(
                    StreamOperationStatus.FAILED,
                    StreamProductRescaleOperation.Phase.FAILED));
        });
    }

    private StreamProductJobSnapshot rescaleTransition(
            StreamJobId jobId, StreamCommandId commandId,
            StreamProductRescaleOperation.Phase expected,
            StreamOperationStatus status,
            StreamProductRescaleOperation.Phase next) {
        return mutate(jobId, current -> {
            StreamProductRescaleOperation operation =
                    current.rescale(commandId);
            if (operation.phase() == next) return current;
            if (operation.phase() != expected) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "live rescale transition is invalid");
            }
            return current.withRescale(operation.with(status, next));
        });
    }

    private static void requireNoRescaleInFlight(
            StreamProductJobSnapshot snapshot, String operation) {
        StreamProductRescaleOperation rescale = snapshot.rescaleOrNull();
        if (rescale != null && (rescale.status()
                == StreamOperationStatus.ACCEPTED
                || rescale.status() == StreamOperationStatus.RUNNING)) {
            throw failure(StreamProductControlException.Code.INVALID_STATE,
                    operation + " cannot overlap live rescale");
        }
    }

    public StreamProductJobSnapshot startSavepoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return savepointTransition(jobId, commandId,
                StreamOperationStatus.ACCEPTED,
                StreamOperationStatus.RUNNING, StreamJobStatus.RUNNING);
    }

    public StreamProductJobSnapshot completeSavepoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return savepointTransition(jobId, commandId,
                StreamOperationStatus.RUNNING,
                StreamOperationStatus.SUCCEEDED,
                StreamJobStatus.STOPPED_WITH_SAVEPOINT);
    }

    public StreamProductJobSnapshot failSavepoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return mutate(jobId, current -> {
            StreamProductSavepointOperation operation =
                    current.savepoint(commandId);
            if (operation.status() == StreamOperationStatus.FAILED
                    || operation.status() == StreamOperationStatus.SUCCEEDED) {
                return current;
            }
            return current.withSavepoint(operation.withStatus(
                    StreamOperationStatus.FAILED), current.status());
        });
    }

    private StreamProductJobSnapshot savepointTransition(
            StreamJobId jobId, StreamCommandId commandId,
            StreamOperationStatus expected, StreamOperationStatus next,
            StreamJobStatus jobStatus) {
        if (commandId == null) {
            throw new IllegalArgumentException(
                    "savepoint command ID is required");
        }
        return mutate(jobId, current -> {
            StreamProductSavepointOperation operation =
                    current.savepoint(commandId);
            if (operation.status() == next
                    || (next == StreamOperationStatus.RUNNING
                    && operation.status() == StreamOperationStatus.SUCCEEDED)) {
                return current;
            }
            if (operation.status() != expected) {
                throw failure(StreamProductControlException.Code.INVALID_STATE,
                        "savepoint command transition is invalid");
            }
            return current.withSavepoint(operation.withStatus(next), jobStatus);
        });
    }

    private StreamProductJobSnapshot mutate(
            StreamJobId jobId, Mutation mutation) {
        if (jobId == null || mutation == null) {
            throw new IllegalArgumentException("jobId and mutation are required");
        }
        if (stateStore != null) {
            StreamProductJobSnapshot updated = stateStore.update(
                    jobId, mutation::apply);
            if (updated == null) {
                throw failure(StreamProductControlException.Code.JOB_NOT_FOUND,
                        "Stream product job does not exist");
            }
            requireJobIdentity(jobId, updated);
            return updated;
        }
        return locked(jobId, () -> {
            Path path = statePath(jobId);
            if (!Files.exists(path)) {
                throw failure(StreamProductControlException.Code.JOB_NOT_FOUND,
                        "Stream product job does not exist");
            }
            StreamProductJobSnapshot current = read(path);
            StreamProductJobSnapshot next = mutation.apply(current);
            if (!next.equals(current)) write(path, next);
            return next;
        });
    }

    private StreamProductJobSnapshot locked(StreamJobId jobId, Action action) {
        Path lockPath = root.resolve(fileStem(jobId) + ".lock");
        synchronized (JVM_FILE_LOCK) {
            try (FileChannel channel = FileChannel.open(lockPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 FileLock ignored = channel.lock()) {
                return action.run();
            } catch (StreamProductControlException failure) {
                throw failure;
            } catch (IOException failure) {
                throw new IllegalStateException(
                        "Stream product authority operation failed", failure);
            }
        }
    }

    private Path statePath(StreamJobId jobId) {
        return root.resolve(fileStem(jobId) + ".bin");
    }

    private static void requirePublished(
            StreamProductJobSnapshot existing,
            StreamSubmissionId submissionId, PlanArtifact artifact,
            StreamProductRecovery recovery) {
        if (!existing.submissionId().equals(submissionId)
                || !existing.artifactId().equals(artifact.artifactId())
                || !java.util.Objects.equals(
                recovery, existing.recoveryOrNull())) {
            throw failure(
                    StreamProductControlException.Code.SUBMISSION_CONFLICT,
                    recovery == null
                            ? "Stream submission identity is bound to another Artifact"
                            : "Stream recovery identity is bound to another request");
        }
    }

    private static void requireJobIdentity(StreamJobId jobId,
            StreamProductJobSnapshot snapshot) {
        if (!snapshot.jobId().equals(jobId)
                || !deriveJobId(snapshot.submissionId()).equals(jobId)) {
            throw failure(StreamProductControlException.Code.INVALID_FRAME,
                    "Stream product job identity differs from its authority key");
        }
    }

    private static String fileStem(StreamJobId jobId) {
        return "product-job-" + sha256Hex(
                jobId.value().getBytes(StandardCharsets.UTF_8));
    }

    private static StreamJobId deriveJobId(StreamSubmissionId submissionId) {
        return StreamJobId.of("stream-" + sha256Hex(
                submissionId.value().getBytes(StandardCharsets.UTF_8)));
    }

    private static String savepointId(
            StreamJobId jobId, StreamCommandId commandId,
            String requestDigest) {
        return "stream-savepoint:" + sha256Hex((jobId.value() + '\n'
                + commandId.value() + '\n' + requestDigest)
                .getBytes(StandardCharsets.UTF_8));
    }

    private static String rescaleId(
            StreamJobId jobId, StreamCommandId commandId,
            int targetParallelism, long sourceGeneration) {
        return "stream-rescale:" + sha256Hex((jobId.value() + '\n'
                + commandId.value() + '\n' + targetParallelism + '\n'
                + sourceGeneration).getBytes(StandardCharsets.UTF_8));
    }

    private static boolean allowed(
            StreamJobStatus current, StreamJobStatus next) {
        switch (current) {
            case SUBMITTED:
                return next == StreamJobStatus.STARTING
                        || next == StreamJobStatus.STOPPING
                        || next == StreamJobStatus.FAILED;
            case STARTING:
                return next == StreamJobStatus.RUNNING
                        || next == StreamJobStatus.STOPPING
                        || next == StreamJobStatus.FAILED;
            case RUNNING:
                return next == StreamJobStatus.RESTARTING
                        || next == StreamJobStatus.FINISHED
                        || next == StreamJobStatus.STOPPING
                        || next == StreamJobStatus.FAILED;
            case RESTARTING:
                return next == StreamJobStatus.RUNNING
                        || next == StreamJobStatus.STOPPING
                        || next == StreamJobStatus.FAILED;
            case STOPPING:
                return next == StreamJobStatus.CANCELED
                        || next == StreamJobStatus.FAILED
                        || next == StreamJobStatus.STOPPED_WITH_SAVEPOINT;
            default:
                return false;
        }
    }

    private static void write(Path target, StreamProductJobSnapshot snapshot)
            throws IOException {
        byte[] frame = encode(snapshot);
        Path temporary = Files.createTempFile(
                target.getParent(), ".stream-product-job-", ".tmp");
        boolean moved = false;
        try (FileChannel channel = FileChannel.open(temporary,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buffer = ByteBuffer.wrap(frame);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            moved = true;
            try (FileChannel directory = FileChannel.open(
                    target.getParent(), StandardOpenOption.READ)) {
                directory.force(true);
            }
        } finally {
            if (!moved) Files.deleteIfExists(temporary);
        }
    }

    private static StreamProductJobSnapshot read(Path path) throws IOException {
        long size = Files.size(path);
        if (size <= 0L || size > MAX_FRAME_BYTES) {
            throw failure(StreamProductControlException.Code.LIMIT_EXCEEDED,
                    "Stream product authority frame exceeds its bound");
        }
        return decode(Files.readAllBytes(path));
    }

    static byte[] encode(StreamProductJobSnapshot snapshot) {
        return encode(snapshot, VERSION);
    }

    static byte[] encode(StreamProductJobSnapshot snapshot, int version) {
        if (version != VERSION && version != EVIDENCE_VERSION) {
            throw new IllegalArgumentException(
                    "unsupported Stream product authority write version");
        }
        try {
            byte[] artifact = PlanArtifactCodec.encode(snapshot.artifact());
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                writeText(body, snapshot.submissionId().value());
                writeText(body, snapshot.jobId().value());
                body.writeInt(snapshot.status().wireId());
                writeProgress(body, snapshot.progress(), true);
                body.writeBoolean(snapshot.cancelCommandId() != null);
                if (snapshot.cancelCommandId() != null) {
                    writeText(body, snapshot.cancelCommandId().value());
                    body.writeInt(operationWireId(
                            snapshot.cancelStatusOrNull()));
                }
                body.writeInt(snapshot.checkpoints().size());
                for (StreamProductCheckpointOperation checkpoint
                        : snapshot.checkpoints()) {
                    writeText(body, checkpoint.commandId().value());
                    writeText(body, checkpoint.checkpointId());
                    body.writeInt(operationWireId(checkpoint.status()));
                    if (version >= VERSION) {
                        body.writeLong(checkpoint.acceptedAtEpochMillis());
                        body.writeLong(checkpoint.completedAtEpochMillis());
                    }
                }
                body.writeBoolean(snapshot.savepointOrNull() != null);
                if (snapshot.savepointOrNull() != null) {
                    StreamProductSavepointOperation savepoint =
                            snapshot.savepointOrNull();
                    writeText(body, savepoint.commandId().value());
                    writeText(body, savepoint.savepointId());
                    writeText(body, savepoint.requestDigest());
                    body.writeInt(operationWireId(savepoint.status()));
                }
                body.writeBoolean(snapshot.recoveryOrNull() != null);
                if (snapshot.recoveryOrNull() != null) {
                    StreamProductRecovery recovery =
                            snapshot.recoveryOrNull();
                    writeText(body, recovery.sourceJobId().value());
                    body.writeInt(recovery.recoveryPointId().kind().ordinal());
                    writeText(body, recovery.recoveryPointId().value());
                    body.writeInt(recovery.targetParallelism());
                    body.writeInt(recovery.targetMaxParallelism());
                    body.writeBoolean(recovery.offlineRescale());
                }
                body.writeInt(snapshot.rescales().size());
                for (StreamProductRescaleOperation rescale
                        : snapshot.rescales()) {
                    writeText(body, rescale.commandId().value());
                    writeText(body, rescale.rescaleId());
                    body.writeInt(rescale.targetParallelism());
                    body.writeLong(rescale.sourceGeneration());
                    body.writeLong(rescale.targetGeneration());
                    body.writeInt(operationWireId(rescale.status()));
                    body.writeInt(rescale.phase().wireId());
                }
                writeEvidence(body, snapshot.evidence());
                body.writeInt(artifact.length);
                body.write(artifact);
            }
            byte[] body = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.write(MAGIC);
                frame.writeInt(version);
                frame.writeInt(body.length);
                frame.write(body);
                frame.write(sha256(body));
            }
            byte[] frame = frameBytes.toByteArray();
            if (frame.length > MAX_FRAME_BYTES) {
                throw failure(StreamProductControlException.Code.LIMIT_EXCEEDED,
                        "Stream product authority frame exceeds its bound");
            }
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static StreamProductJobSnapshot decode(byte[] frame) {
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            int version = input.readInt();
            int bodyLength = input.readInt();
            if (!MessageDigest.isEqual(MAGIC, magic)
                    || (version != VERSION
                    && version != EVIDENCE_VERSION
                    && version != EVIDENCE_PREVIOUS_VERSION
                    && version != RESCALE_VERSION
                    && version != RECOVERY_VERSION
                    && version != SAVEPOINT_VERSION
                    && version != PREVIOUS_VERSION)
                    || bodyLength < 0
                    || MAGIC.length + 8L + bodyLength + DIGEST_BYTES
                    != frame.length) {
                throw failure(StreamProductControlException.Code.INVALID_FRAME,
                        "Stream product authority frame header is invalid");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            input.readFully(expected);
            if (!MessageDigest.isEqual(expected, sha256(body))) {
                throw failure(StreamProductControlException.Code.INVALID_FRAME,
                        "Stream product authority checksum differs");
            }
            try (DataInputStream value = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                StreamSubmissionId submissionId =
                        StreamSubmissionId.of(readText(value));
                StreamJobId jobId = StreamJobId.of(readText(value));
                StreamJobStatus status = jobStatus(value.readInt());
                StreamJobProgress progress = readProgress(value,
                        version >= EVIDENCE_VERSION);
                StreamCommandId cancelId = null;
                StreamOperationStatus cancelStatus = null;
                if (value.readBoolean()) {
                    cancelId = StreamCommandId.of(readText(value));
                    cancelStatus = operationStatus(value.readInt());
                }
                int checkpointCount = value.readInt();
                if (checkpointCount < 0 || checkpointCount > 257) {
                    throw failure(
                            StreamProductControlException.Code.LIMIT_EXCEEDED,
                            "checkpoint command count exceeds its bound");
                }
                java.util.List<StreamProductCheckpointOperation> checkpoints =
                        new java.util.ArrayList<>(checkpointCount);
                for (int index = 0; index < checkpointCount; index++) {
                    checkpoints.add(new StreamProductCheckpointOperation(
                            StreamCommandId.of(readText(value)),
                            readText(value),
                            operationStatus(value.readInt()),
                            version >= VERSION ? value.readLong() : 0L,
                            version >= VERSION ? value.readLong() : 0L));
                }
                StreamProductSavepointOperation savepoint = null;
                if (version >= SAVEPOINT_VERSION && value.readBoolean()) {
                    savepoint = new StreamProductSavepointOperation(
                            StreamCommandId.of(readText(value)),
                            readText(value), readText(value),
                            operationStatus(value.readInt()));
                }
                StreamProductRecovery recovery = null;
                if (version >= RECOVERY_VERSION && value.readBoolean()) {
                    StreamJobId sourceJobId = StreamJobId.of(readText(value));
                    int kind = value.readInt();
                    io.rowweave.stream.api.StreamRecoveryPointId point;
                    if (kind == 0) {
                        point = io.rowweave.stream.api.StreamRecoveryPointId
                                .checkpoint(readText(value));
                    } else if (kind == 1) {
                        point = io.rowweave.stream.api.StreamRecoveryPointId
                                .savepoint(readText(value));
                    } else {
                        throw failure(
                                StreamProductControlException.Code.INVALID_FRAME,
                                "unknown Stream recovery point kind");
                    }
                    recovery = new StreamProductRecovery(sourceJobId, point,
                            value.readInt(), value.readInt(),
                            value.readBoolean());
                }
                java.util.List<StreamProductRescaleOperation> rescales =
                        new java.util.ArrayList<>();
                if (version >= EVIDENCE_PREVIOUS_VERSION) {
                    int rescaleCount = value.readInt();
                    if (rescaleCount < 0 || rescaleCount > 129) {
                        throw failure(StreamProductControlException.Code
                                        .LIMIT_EXCEEDED,
                                "rescale command count exceeds its bound");
                    }
                    for (int index = 0; index < rescaleCount; index++) {
                        rescales.add(readRescale(value));
                    }
                } else if (version >= RESCALE_VERSION
                        && value.readBoolean()) {
                    rescales.add(readRescale(value));
                }
                StreamJobEvidence evidence = version >= EVIDENCE_VERSION
                        ? readEvidence(value, jobId, progress)
                        : StreamJobEvidence.unavailable(jobId, progress, 0L);
                int artifactLength = value.readInt();
                if (artifactLength <= 0
                        || artifactLength > PlanArtifactCodec.MAX_FRAME_BYTES
                        || artifactLength > value.available()) {
                    throw failure(StreamProductControlException.Code.LIMIT_EXCEEDED,
                            "Stream product Artifact length is invalid");
                }
                byte[] artifactBytes = new byte[artifactLength];
                value.readFully(artifactBytes);
                if (value.available() != 0) {
                    throw failure(StreamProductControlException.Code.INVALID_FRAME,
                            "Stream product authority frame has trailing bytes");
                }
                PlanArtifact artifact = PlanArtifactCodec.decode(artifactBytes);
                PlanArtifactValidator.validateForStreamProductExecution(artifact);
                if (!deriveJobId(submissionId).equals(jobId)) {
                    throw failure(StreamProductControlException.Code.INVALID_FRAME,
                            "Stream product job identity is noncanonical");
                }
                return new StreamProductJobSnapshot(
                        submissionId, jobId, artifact, status, progress,
                        cancelId, cancelStatus, checkpoints, savepoint,
                        recovery, rescales, evidence);
            }
        } catch (StreamProductControlException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw failure(StreamProductControlException.Code.INVALID_FRAME,
                    "Stream product authority frame is truncated");
        } catch (IOException | RuntimeException malformed) {
            if (malformed instanceof StreamProductControlException) {
                throw (StreamProductControlException) malformed;
            }
            throw new StreamProductControlException(
                    StreamProductControlException.Code.INVALID_FRAME,
                    "Stream product authority frame is malformed", malformed);
        }
    }

    private static StreamProductRescaleOperation readRescale(
            DataInputStream value) throws IOException {
        return new StreamProductRescaleOperation(
                StreamCommandId.of(readText(value)), readText(value),
                value.readInt(), value.readLong(), value.readLong(),
                operationStatus(value.readInt()),
                StreamProductRescaleOperation.Phase.fromWireId(
                        value.readInt()));
    }

    private static void writeProgress(
            DataOutputStream output, StreamJobProgress progress,
            boolean includeBacklog)
            throws IOException {
        output.writeInt(progress.attempt());
        output.writeLong(progress.completedCheckpoints());
        output.writeLong(progress.operatorWatermark());
        output.writeInt(progress.cdcPhase().wireId());
        output.writeInt(progress.restarts());
        output.writeLong(progress.recordsRead());
        output.writeLong(progress.recordsWritten());
        output.writeLong(progress.bytesRead());
        output.writeLong(progress.bytesWritten());
        if (includeBacklog) {
            output.writeLong(progress.backlogEvents());
            output.writeLong(progress.backlogBytes());
        }
        output.writeInt(progress.mailboxHighWaterEvents());
        output.writeLong(progress.channelHighWaterBytes());
        output.writeLong(progress.managedStateBytes());
        output.writeInt(progress.timerHighWaterCount());
        output.writeInt(progress.windowHighWaterCount());
        output.writeLong(progress.backpressuredMillis());
    }

    private static StreamJobProgress readProgress(
            DataInputStream input, boolean includesBacklog)
            throws IOException {
        int attempt = input.readInt();
        long completedCheckpoints = input.readLong();
        long operatorWatermark = input.readLong();
        StreamCdcPhase phase = cdcPhase(input.readInt());
        int restarts = input.readInt();
        long recordsRead = input.readLong();
        long recordsWritten = input.readLong();
        long bytesRead = input.readLong();
        long bytesWritten = input.readLong();
        long backlogEvents = includesBacklog ? input.readLong() : 0L;
        long backlogBytes = includesBacklog ? input.readLong() : 0L;
        return new StreamJobProgress(
                attempt, completedCheckpoints, operatorWatermark,
                phase, restarts, recordsRead, recordsWritten,
                bytesRead, bytesWritten, backlogEvents, backlogBytes,
                input.readInt(), input.readLong(), input.readLong(),
                input.readInt(), input.readInt(), input.readLong());
    }

    private static void writeEvidence(
            DataOutputStream output, StreamJobEvidence evidence)
            throws IOException {
        output.writeBoolean(evidence.available());
        output.writeLong(evidence.observedAtEpochMillis());
        if (!evidence.available()) return;
        writeText(output, evidence.runId());
        writeText(output, evidence.attemptId());
        output.writeInt(evidence.attemptOrdinal());
        output.writeLong(evidence.fenceToken());
        output.writeLong(evidence.coordinatorTerm());
        output.writeLong(evidence.assignmentGeneration());
        output.writeBoolean(evidence.latestCompletedCheckpointId() != null);
        if (evidence.latestCompletedCheckpointId() != null) {
            writeText(output, evidence.latestCompletedCheckpointId());
            output.writeLong(evidence.latestCompletedCheckpointSequence());
            output.writeLong(evidence.latestCheckpointDurationMillis());
            output.writeLong(evidence.latestCheckpointBytes());
        }
        writeProgress(output, evidence.attemptProgress(), true);
        writeProgress(output, evidence.lifetimeProgress(), true);
        output.writeInt(evidence.placementTaskCount());
        output.writeInt(evidence.reportedTaskCount());
    }

    private static StreamJobEvidence readEvidence(
            DataInputStream input, StreamJobId jobId,
            StreamJobProgress fallbackProgress) throws IOException {
        boolean available = input.readBoolean();
        long observedAtEpochMillis = input.readLong();
        if (!available) {
            return StreamJobEvidence.unavailable(jobId,
                    fallbackProgress, observedAtEpochMillis);
        }
        String runId = readText(input);
        String attemptId = readText(input);
        int attemptOrdinal = input.readInt();
        long fenceToken = input.readLong();
        long coordinatorTerm = input.readLong();
        long assignmentGeneration = input.readLong();
        String checkpointId = null;
        long checkpointSequence = 0L;
        long checkpointDurationMillis = 0L;
        long checkpointBytes = 0L;
        if (input.readBoolean()) {
            checkpointId = readText(input);
            checkpointSequence = input.readLong();
            checkpointDurationMillis = input.readLong();
            checkpointBytes = input.readLong();
        }
        StreamJobProgress attemptProgress = readProgress(input, true);
        StreamJobProgress lifetimeProgress = readProgress(input, true);
        return new StreamJobEvidence(jobId, runId, attemptId,
                attemptOrdinal, fenceToken, coordinatorTerm,
                assignmentGeneration, checkpointId, checkpointSequence,
                checkpointDurationMillis, checkpointBytes,
                attemptProgress, lifetimeProgress, input.readInt(),
                input.readInt(), observedAtEpochMillis);
    }

    private static StreamJobStatus jobStatus(int wireId) {
        for (StreamJobStatus status : StreamJobStatus.values()) {
            if (status.wireId() == wireId) return status;
        }
        throw failure(StreamProductControlException.Code.INVALID_FRAME,
                "unknown Stream job status wire ID");
    }

    private static StreamCdcPhase cdcPhase(int wireId) {
        for (StreamCdcPhase phase : StreamCdcPhase.values()) {
            if (phase.wireId() == wireId) return phase;
        }
        throw failure(StreamProductControlException.Code.INVALID_FRAME,
                "unknown Stream CDC phase wire ID");
    }

    private static int operationWireId(StreamOperationStatus status) {
        switch (status) {
            case ACCEPTED: return 1;
            case RUNNING: return 2;
            case SUCCEEDED: return 3;
            case FAILED: return 4;
            default: throw new AssertionError(status);
        }
    }

    private static StreamOperationStatus operationStatus(int wireId) {
        switch (wireId) {
            case 1: return StreamOperationStatus.ACCEPTED;
            case 2: return StreamOperationStatus.RUNNING;
            case 3: return StreamOperationStatus.SUCCEEDED;
            case 4: return StreamOperationStatus.FAILED;
            default:
                throw failure(StreamProductControlException.Code.INVALID_FRAME,
                        "unknown Stream operation status wire ID");
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= 0 || bytes.length > MAX_TEXT_BYTES) {
            throw failure(StreamProductControlException.Code.LIMIT_EXCEEDED,
                    "Stream product authority text exceeds its bound");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_TEXT_BYTES) {
            throw failure(StreamProductControlException.Code.LIMIT_EXCEEDED,
                    "Stream product authority text exceeds its bound");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
            if (!MessageDigest.isEqual(
                    bytes, value.getBytes(StandardCharsets.UTF_8))) {
                throw failure(StreamProductControlException.Code.INVALID_FRAME,
                        "Stream product authority text is noncanonical");
            }
            return value;
        } catch (java.nio.charset.CharacterCodingException malformed) {
            throw failure(StreamProductControlException.Code.INVALID_FRAME,
                    "Stream product authority text is invalid UTF-8");
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String sha256Hex(byte[] value) {
        byte[] digest = sha256(value);
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[digest.length * 2];
        for (int index = 0; index < digest.length; index++) {
            int current = digest[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }

    private static String checkpointId(
            StreamJobId jobId, StreamCommandId commandId) {
        byte[] bytes = (jobId.value() + '\0' + commandId.value())
                .getBytes(StandardCharsets.UTF_8);
        return "stream-checkpoint:" + sha256Hex(bytes);
    }

    private static StreamProductControlException failure(
            StreamProductControlException.Code code, String message) {
        return new StreamProductControlException(code, message);
    }

    private interface Action { StreamProductJobSnapshot run() throws IOException; }
    private interface Mutation {
        StreamProductJobSnapshot apply(StreamProductJobSnapshot current);
    }
}
