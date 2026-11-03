package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSubmissionId;

import java.util.Objects;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Immutable bounded authority projection for one product Stream job. */
public final class StreamProductJobSnapshot {
    private final StreamSubmissionId submissionId;
    private final StreamJobId jobId;
    private final PlanArtifact artifact;
    private final StreamJobStatus status;
    private final StreamJobProgress progress;
    private final StreamJobEvidence evidence;
    private final StreamCommandId cancelCommandId;
    private final StreamOperationStatus cancelStatus;
    private final List<StreamProductCheckpointOperation> checkpoints;
    private final StreamProductSavepointOperation savepoint;
    private final StreamProductRecovery recovery;
    private final List<StreamProductRescaleOperation> rescales;

    StreamProductJobSnapshot(
            StreamSubmissionId submissionId, StreamJobId jobId,
            PlanArtifact artifact, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus) {
        this(submissionId, jobId, artifact, status, progress,
                cancelCommandId, cancelStatus, Collections.emptyList(), null);
    }

    StreamProductJobSnapshot(
            StreamSubmissionId submissionId, StreamJobId jobId,
            PlanArtifact artifact, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus,
            List<StreamProductCheckpointOperation> checkpoints) {
        this(submissionId, jobId, artifact, status, progress,
                cancelCommandId, cancelStatus, checkpoints, null);
    }

    StreamProductJobSnapshot(
            StreamSubmissionId submissionId, StreamJobId jobId,
            PlanArtifact artifact, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus,
            List<StreamProductCheckpointOperation> checkpoints,
            StreamProductSavepointOperation savepoint) {
        this(submissionId, jobId, artifact, status, progress,
                cancelCommandId, cancelStatus, checkpoints, savepoint, null);
    }

    StreamProductJobSnapshot(
            StreamSubmissionId submissionId, StreamJobId jobId,
            PlanArtifact artifact, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus,
            List<StreamProductCheckpointOperation> checkpoints,
            StreamProductSavepointOperation savepoint,
            StreamProductRecovery recovery) {
        this(submissionId, jobId, artifact, status, progress,
                cancelCommandId, cancelStatus, checkpoints, savepoint,
                recovery, (StreamProductRescaleOperation) null);
    }

    StreamProductJobSnapshot(
            StreamSubmissionId submissionId, StreamJobId jobId,
            PlanArtifact artifact, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus,
            List<StreamProductCheckpointOperation> checkpoints,
            StreamProductSavepointOperation savepoint,
            StreamProductRecovery recovery,
            StreamProductRescaleOperation rescale) {
        this(submissionId, jobId, artifact, status, progress,
                cancelCommandId, cancelStatus, checkpoints, savepoint,
                recovery, rescale == null ? Collections.emptyList()
                        : Collections.singletonList(rescale));
    }

    StreamProductJobSnapshot(
            StreamSubmissionId submissionId, StreamJobId jobId,
            PlanArtifact artifact, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus,
            List<StreamProductCheckpointOperation> checkpoints,
            StreamProductSavepointOperation savepoint,
            StreamProductRecovery recovery,
            List<StreamProductRescaleOperation> rescales) {
        this(submissionId, jobId, artifact, status, progress,
                cancelCommandId, cancelStatus, checkpoints, savepoint,
                recovery, rescales, StreamJobEvidence.unavailable(
                        jobId, progress, 0L));
    }

    StreamProductJobSnapshot(
            StreamSubmissionId submissionId, StreamJobId jobId,
            PlanArtifact artifact, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus,
            List<StreamProductCheckpointOperation> checkpoints,
            StreamProductSavepointOperation savepoint,
            StreamProductRecovery recovery,
            List<StreamProductRescaleOperation> rescales,
            StreamJobEvidence evidence) {
        this.submissionId = Objects.requireNonNull(
                submissionId, "submissionId");
        this.jobId = Objects.requireNonNull(jobId, "jobId");
        this.artifact = Objects.requireNonNull(artifact, "artifact");
        this.status = Objects.requireNonNull(status, "status");
        this.progress = Objects.requireNonNull(progress, "progress");
        this.evidence = Objects.requireNonNull(evidence, "evidence");
        if (!jobId.equals(evidence.jobId())) {
            throw new IllegalArgumentException(
                    "Stream evidence belongs to another job");
        }
        if ((cancelCommandId == null) != (cancelStatus == null)) {
            throw new IllegalArgumentException(
                    "cancel identity and status must be present together");
        }
        this.cancelCommandId = cancelCommandId;
        this.cancelStatus = cancelStatus;
        if (checkpoints == null || checkpoints.size() > 257) {
            throw new IllegalArgumentException(
                    "checkpoint command history exceeds its bound");
        }
        ArrayList<StreamProductCheckpointOperation> copy = new ArrayList<>();
        Set<String> unique = new HashSet<>();
        for (StreamProductCheckpointOperation checkpoint : checkpoints) {
            if (checkpoint == null || !unique.add(
                    checkpoint.commandId().value())) {
                throw new IllegalArgumentException(
                        "checkpoint command history is noncanonical");
            }
            copy.add(checkpoint);
        }
        this.checkpoints = Collections.unmodifiableList(copy);
        this.savepoint = savepoint;
        this.recovery = recovery;
        if (rescales == null || rescales.size() > 129) {
            throw new IllegalArgumentException(
                    "rescale command history exceeds its bound");
        }
        ArrayList<StreamProductRescaleOperation> rescaleCopy =
                new ArrayList<>();
        Set<String> rescaleCommands = new HashSet<>();
        boolean inFlight = false;
        for (StreamProductRescaleOperation operation : rescales) {
            if (operation == null || !rescaleCommands.add(
                    operation.commandId().value())) {
                throw new IllegalArgumentException(
                        "rescale command history is noncanonical");
            }
            boolean active = operation.status()
                    == StreamOperationStatus.ACCEPTED
                    || operation.status() == StreamOperationStatus.RUNNING;
            if (active && inFlight) {
                throw new IllegalArgumentException(
                        "multiple live rescale commands are in flight");
            }
            inFlight |= active;
            rescaleCopy.add(operation);
        }
        this.rescales = Collections.unmodifiableList(rescaleCopy);
    }

    public StreamSubmissionId submissionId() { return submissionId; }
    public StreamJobId jobId() { return jobId; }
    public String artifactId() { return artifact.artifactId(); }
    public PlanArtifact artifact() { return artifact; }
    public StreamJobStatus status() { return status; }
    public StreamJobProgress progress() { return progress; }
    public StreamJobEvidence evidence() { return evidence; }
    public StreamCommandId cancelCommandId() { return cancelCommandId; }

    public StreamOperationStatus cancelStatus(StreamCommandId commandId) {
        if (commandId == null || !commandId.equals(cancelCommandId)) {
            throw new StreamProductControlException(
                    StreamProductControlException.Code.COMMAND_NOT_FOUND,
                    "Stream cancel command does not exist");
        }
        return cancelStatus;
    }

    StreamOperationStatus cancelStatusOrNull() { return cancelStatus; }

    List<StreamProductCheckpointOperation> checkpoints() {
        return checkpoints;
    }

    StreamProductSavepointOperation savepointOrNull() { return savepoint; }
    StreamProductRecovery recoveryOrNull() { return recovery; }
    List<StreamProductRescaleOperation> rescales() { return rescales; }
    StreamProductRescaleOperation rescaleOrNull() {
        return rescales.isEmpty() ? null : rescales.get(rescales.size() - 1);
    }

    StreamProductRescaleOperation rescale(StreamCommandId commandId) {
        if (commandId != null) {
            for (StreamProductRescaleOperation operation : rescales) {
                if (commandId.equals(operation.commandId())) return operation;
            }
        }
        throw new StreamProductControlException(
                StreamProductControlException.Code.COMMAND_NOT_FOUND,
                "Stream rescale command does not exist");
    }

    StreamProductSavepointOperation savepoint(StreamCommandId commandId) {
        if (savepoint == null || commandId == null
                || !commandId.equals(savepoint.commandId())) {
            throw new StreamProductControlException(
                    StreamProductControlException.Code.COMMAND_NOT_FOUND,
                    "Stream savepoint command does not exist");
        }
        return savepoint;
    }

    StreamProductCheckpointOperation checkpoint(StreamCommandId commandId) {
        for (StreamProductCheckpointOperation checkpoint : checkpoints) {
            if (checkpoint.commandId().equals(commandId)) return checkpoint;
        }
        throw new StreamProductControlException(
                StreamProductControlException.Code.COMMAND_NOT_FOUND,
                "Stream checkpoint command does not exist");
    }

    StreamProductJobSnapshot withStatus(StreamJobStatus next) {
        return new StreamProductJobSnapshot(
                submissionId, jobId, artifact, next, progress,
                cancelCommandId, cancelStatus, checkpoints, savepoint,
                recovery, rescales, evidence);
    }

    StreamProductJobSnapshot withCdcPhase(StreamCdcPhase phase) {
        if (phase == null) {
            throw new IllegalArgumentException("Stream CDC phase is required");
        }
        if (progress.cdcPhase() == phase) return this;
        StreamJobProgress next = new StreamJobProgress(
                progress.attempt(), progress.completedCheckpoints(),
                progress.operatorWatermark(), phase, progress.restarts(),
                progress.recordsRead(), progress.recordsWritten(),
                progress.bytesRead(), progress.bytesWritten(),
                progress.backlogEvents(), progress.backlogBytes(),
                progress.mailboxHighWaterEvents(),
                progress.channelHighWaterBytes(),
                progress.managedStateBytes(), progress.timerHighWaterCount(),
                progress.windowHighWaterCount(),
                progress.backpressuredMillis());
        return new StreamProductJobSnapshot(
                submissionId, jobId, artifact, status, next,
                cancelCommandId, cancelStatus, checkpoints, savepoint,
                recovery, rescales, evidenceForProgress(next));
    }

    StreamProductJobSnapshot beginRestart() {
        StreamJobProgress next = new StreamJobProgress(
                Math.addExact(progress.attempt(), 1),
                progress.completedCheckpoints(),
                progress.operatorWatermark(), progress.cdcPhase(),
                Math.addExact(progress.restarts(), 1),
                progress.recordsRead(), progress.recordsWritten(),
                progress.bytesRead(), progress.bytesWritten(),
                progress.backlogEvents(), progress.backlogBytes(),
                progress.mailboxHighWaterEvents(),
                progress.channelHighWaterBytes(),
                progress.managedStateBytes(),
                progress.timerHighWaterCount(),
                progress.windowHighWaterCount(),
                progress.backpressuredMillis());
        return new StreamProductJobSnapshot(
                submissionId, jobId, artifact, StreamJobStatus.RESTARTING,
                next, cancelCommandId, cancelStatus, checkpoints, savepoint,
                recovery, rescales, evidence);
    }

    StreamProductJobSnapshot withCancel(
            StreamCommandId commandId, StreamOperationStatus operationStatus,
            StreamJobStatus jobStatus) {
        return new StreamProductJobSnapshot(
                submissionId, jobId, artifact, jobStatus, progress,
                commandId, operationStatus, checkpoints, savepoint,
                recovery, rescales, evidence);
    }

    StreamProductJobSnapshot withCheckpoint(
            StreamProductCheckpointOperation checkpoint,
            boolean completed) {
        return withCheckpoint(checkpoint, completed, progress.cdcPhase());
    }

    StreamProductJobSnapshot withCheckpoint(
            StreamProductCheckpointOperation checkpoint,
            boolean completed, StreamCdcPhase cdcPhase) {
        ArrayList<StreamProductCheckpointOperation> next =
                new ArrayList<>(checkpoints);
        boolean replaced = false;
        for (int index = 0; index < next.size(); index++) {
            if (next.get(index).commandId().equals(checkpoint.commandId())) {
                next.set(index, checkpoint);
                replaced = true;
                break;
            }
        }
        if (!replaced) next.add(checkpoint);
        StreamJobProgress nextProgress = completed
                ? incrementCompleted(progress, cdcPhase) : progress;
        return new StreamProductJobSnapshot(
                submissionId, jobId, artifact, status, nextProgress,
                cancelCommandId, cancelStatus, next, savepoint, recovery,
                rescales, evidenceForProgress(nextProgress));
    }

    StreamProductJobSnapshot retainCheckpointHistory(int retainedTerminal) {
        if (retainedTerminal < 1 || retainedTerminal > 128) {
            throw new IllegalArgumentException(
                    "retained checkpoint history is invalid");
        }
        boolean[] keep = new boolean[checkpoints.size()];
        int successes = 0;
        int failures = 0;
        for (int index = checkpoints.size() - 1; index >= 0; index--) {
            StreamOperationStatus operation = checkpoints.get(index).status();
            if (operation == StreamOperationStatus.ACCEPTED
                    || operation == StreamOperationStatus.RUNNING) {
                keep[index] = true;
            } else if (operation == StreamOperationStatus.SUCCEEDED
                    && successes++ < retainedTerminal) {
                keep[index] = true;
            } else if (operation == StreamOperationStatus.FAILED
                    && failures++ < retainedTerminal) {
                keep[index] = true;
            }
        }
        ArrayList<StreamProductCheckpointOperation> retained =
                new ArrayList<>();
        for (int index = 0; index < checkpoints.size(); index++) {
            if (keep[index]) retained.add(checkpoints.get(index));
        }
        if (retained.size() == checkpoints.size()) return this;
        return new StreamProductJobSnapshot(
                submissionId, jobId, artifact, status, progress,
                cancelCommandId, cancelStatus, retained, savepoint, recovery,
                rescales, evidence);
    }

    StreamProductJobSnapshot withSavepoint(
            StreamProductSavepointOperation operation,
            StreamJobStatus jobStatus) {
        return new StreamProductJobSnapshot(
                submissionId, jobId, artifact, jobStatus, progress,
                cancelCommandId, cancelStatus, checkpoints, operation,
                recovery, rescales, evidence);
    }

    StreamProductJobSnapshot withRescale(
            StreamProductRescaleOperation operation) {
        ArrayList<StreamProductRescaleOperation> next =
                new ArrayList<>(rescales);
        boolean replaced = false;
        for (int index = 0; index < next.size(); index++) {
            if (next.get(index).commandId().equals(operation.commandId())) {
                next.set(index, operation);
                replaced = true;
                break;
            }
        }
        if (!replaced) next.add(operation);
        if (next.size() > 129) {
            next.remove(0);
        }
        return new StreamProductJobSnapshot(
                submissionId, jobId, artifact, status, progress,
                cancelCommandId, cancelStatus, checkpoints, savepoint,
                recovery, next, evidence);
    }

    StreamProductJobSnapshot withEvidence(StreamJobEvidence next) {
        if (next == null || !jobId.equals(next.jobId())) {
            throw new IllegalArgumentException(
                    "Stream evidence belongs to another job");
        }
        if (evidence.available() && evidencePrecedes(next, evidence)) {
            return this;
        }
        if (next.equals(evidence)) return this;
        return new StreamProductJobSnapshot(
                submissionId, jobId, artifact, status,
                next.lifetimeProgress(), cancelCommandId, cancelStatus,
                checkpoints, savepoint, recovery, rescales, next);
    }

    private static boolean evidencePrecedes(StreamJobEvidence candidate,
            StreamJobEvidence current) {
        if (!candidate.available()) return true;
        if (candidate.latestCompletedCheckpointSequence()
                < current.latestCompletedCheckpointSequence()
                || cumulativeRegressed(candidate.lifetimeProgress(),
                current.lifetimeProgress())) {
            return true;
        }
        int identity = Long.compare(candidate.coordinatorTerm(),
                current.coordinatorTerm());
        if (identity == 0) {
            identity = Long.compare(candidate.fenceToken(),
                    current.fenceToken());
        }
        if (identity == 0) {
            identity = Long.compare(candidate.assignmentGeneration(),
                    current.assignmentGeneration());
        }
        if (identity == 0) {
            identity = Integer.compare(candidate.attemptOrdinal(),
                    current.attemptOrdinal());
        }
        if (identity < 0) return true;
        if (identity > 0) return false;
        if (!candidate.runId().equals(current.runId())
                || !candidate.attemptId().equals(current.attemptId())
                || candidate.observedAtEpochMillis()
                < current.observedAtEpochMillis()
                || cumulativeRegressed(candidate.attemptProgress(),
                current.attemptProgress())) {
            return true;
        }
        if (candidate.latestCompletedCheckpointSequence()
                == current.latestCompletedCheckpointSequence()
                && current.latestCompletedCheckpointId() != null) {
            return !current.latestCompletedCheckpointId().equals(
                    candidate.latestCompletedCheckpointId())
                    || current.latestCheckpointDurationMillis()
                    != candidate.latestCheckpointDurationMillis()
                    || current.latestCheckpointBytes()
                    != candidate.latestCheckpointBytes();
        }
        return false;
    }

    private static boolean cumulativeRegressed(StreamJobProgress candidate,
            StreamJobProgress current) {
        return candidate.completedCheckpoints()
                < current.completedCheckpoints()
                || candidate.restarts() < current.restarts()
                || candidate.recordsRead() < current.recordsRead()
                || candidate.recordsWritten() < current.recordsWritten()
                || candidate.bytesRead() < current.bytesRead()
                || candidate.bytesWritten() < current.bytesWritten()
                || candidate.backpressuredMillis()
                < current.backpressuredMillis();
    }

    private StreamJobEvidence evidenceForProgress(StreamJobProgress next) {
        if (evidence.available()) return evidence;
        return StreamJobEvidence.unavailable(
                jobId, next, evidence.observedAtEpochMillis());
    }

    private static StreamJobProgress incrementCompleted(
            StreamJobProgress value, StreamCdcPhase cdcPhase) {
        return new StreamJobProgress(value.attempt(),
                Math.addExact(value.completedCheckpoints(), 1L),
                value.operatorWatermark(), cdcPhase, value.restarts(),
                value.recordsRead(), value.recordsWritten(),
                value.bytesRead(), value.bytesWritten(),
                value.backlogEvents(), value.backlogBytes(),
                value.mailboxHighWaterEvents(), value.channelHighWaterBytes(),
                value.managedStateBytes(), value.timerHighWaterCount(),
                value.windowHighWaterCount(), value.backpressuredMillis());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamProductJobSnapshot)) return false;
        StreamProductJobSnapshot that = (StreamProductJobSnapshot) other;
        return submissionId.equals(that.submissionId)
                && jobId.equals(that.jobId)
                && artifact.artifactId().equals(that.artifact.artifactId())
                && status == that.status
                && progress.equals(that.progress)
                && evidence.equals(that.evidence)
                && Objects.equals(cancelCommandId, that.cancelCommandId)
                && cancelStatus == that.cancelStatus
                && checkpoints.equals(that.checkpoints)
                && Objects.equals(savepoint, that.savepoint)
                && Objects.equals(recovery, that.recovery)
                && rescales.equals(that.rescales);
    }

    @Override
    public int hashCode() {
        return Objects.hash(submissionId, jobId, artifact.artifactId(),
                status, progress, evidence, cancelCommandId, cancelStatus, checkpoints,
                savepoint, recovery, rescales);
    }
}
