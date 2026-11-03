package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSubmissionId;

/** Artifact-free bounded control response shared by Master and reconnectable handles. */
final class StreamProductJobView {
    private final StreamSubmissionId submissionId;
    private final StreamJobId jobId;
    private final String artifactId;
    private final StreamJobStatus status;
    private final StreamJobProgress progress;
    private final StreamCommandId cancelCommandId;
    private final StreamOperationStatus cancelStatus;
    private final StreamCommandId checkpointCommandId;
    private final String checkpointId;
    private final StreamOperationStatus checkpointStatus;
    private final StreamCommandId savepointCommandId;
    private final String savepointId;
    private final StreamOperationStatus savepointStatus;
    private final StreamCommandId rescaleCommandId;
    private final String rescaleId;
    private final int targetParallelism;
    private final StreamOperationStatus rescaleStatus;
    private final StreamJobEvidence evidence;

    StreamProductJobView(
            StreamSubmissionId submissionId, StreamJobId jobId,
            String artifactId, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus) {
        this(submissionId, jobId, artifactId, status, progress,
                cancelCommandId, cancelStatus, null, null, null,
                null, null, null, null, null, 0, null);
    }

    StreamProductJobView(
            StreamSubmissionId submissionId, StreamJobId jobId,
            String artifactId, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus,
            StreamCommandId checkpointCommandId, String checkpointId,
            StreamOperationStatus checkpointStatus) {
        this(submissionId, jobId, artifactId, status, progress,
                cancelCommandId, cancelStatus, checkpointCommandId,
                checkpointId, checkpointStatus, null, null, null,
                null, null, 0, null);
    }

    StreamProductJobView(
            StreamSubmissionId submissionId, StreamJobId jobId,
            String artifactId, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus,
            StreamCommandId checkpointCommandId, String checkpointId,
            StreamOperationStatus checkpointStatus,
            StreamCommandId savepointCommandId, String savepointId,
            StreamOperationStatus savepointStatus) {
        this(submissionId, jobId, artifactId, status, progress,
                cancelCommandId, cancelStatus, checkpointCommandId,
                checkpointId, checkpointStatus, savepointCommandId,
                savepointId, savepointStatus, null, null, 0, null);
    }

    StreamProductJobView(
            StreamSubmissionId submissionId, StreamJobId jobId,
            String artifactId, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus,
            StreamCommandId checkpointCommandId, String checkpointId,
            StreamOperationStatus checkpointStatus,
            StreamCommandId savepointCommandId, String savepointId,
            StreamOperationStatus savepointStatus,
            StreamCommandId rescaleCommandId, String rescaleId,
            int targetParallelism,
            StreamOperationStatus rescaleStatus) {
        this(submissionId, jobId, artifactId, status, progress,
                cancelCommandId, cancelStatus, checkpointCommandId,
                checkpointId, checkpointStatus, savepointCommandId,
                savepointId, savepointStatus, rescaleCommandId, rescaleId,
                targetParallelism, rescaleStatus, null);
    }

    StreamProductJobView(
            StreamSubmissionId submissionId, StreamJobId jobId,
            String artifactId, StreamJobStatus status,
            StreamJobProgress progress, StreamCommandId cancelCommandId,
            StreamOperationStatus cancelStatus,
            StreamCommandId checkpointCommandId, String checkpointId,
            StreamOperationStatus checkpointStatus,
            StreamCommandId savepointCommandId, String savepointId,
            StreamOperationStatus savepointStatus,
            StreamCommandId rescaleCommandId, String rescaleId,
            int targetParallelism,
            StreamOperationStatus rescaleStatus,
            StreamJobEvidence evidence) {
        if (submissionId == null || jobId == null || artifactId == null
                || artifactId.length() != 64 || status == null
                || progress == null
                || (cancelCommandId == null) != (cancelStatus == null)
                || (checkpointCommandId == null) != (checkpointId == null)
                || (checkpointCommandId == null)
                != (checkpointStatus == null)
                || (checkpointId != null && !checkpointId.matches(
                "stream-checkpoint:[0-9a-f]{64}"))
                || (savepointCommandId == null) != (savepointId == null)
                || (savepointCommandId == null) != (savepointStatus == null)
                || (savepointId != null && !savepointId.matches(
                "stream-savepoint:[0-9a-f]{64}"))
                || (rescaleCommandId == null) != (rescaleId == null)
                || (rescaleCommandId == null) != (rescaleStatus == null)
                || (rescaleCommandId == null) != (targetParallelism == 0)
                || (rescaleId != null && !rescaleId.matches(
                "stream-rescale:[0-9a-f]{64}"))
                || (evidence != null && !jobId.equals(evidence.jobId()))) {
            throw new IllegalArgumentException("invalid Stream product job view");
        }
        this.submissionId = submissionId;
        this.jobId = jobId;
        this.artifactId = artifactId;
        this.status = status;
        this.progress = progress;
        this.cancelCommandId = cancelCommandId;
        this.cancelStatus = cancelStatus;
        this.checkpointCommandId = checkpointCommandId;
        this.checkpointId = checkpointId;
        this.checkpointStatus = checkpointStatus;
        this.savepointCommandId = savepointCommandId;
        this.savepointId = savepointId;
        this.savepointStatus = savepointStatus;
        this.rescaleCommandId = rescaleCommandId;
        this.rescaleId = rescaleId;
        this.targetParallelism = targetParallelism;
        this.rescaleStatus = rescaleStatus;
        this.evidence = evidence;
    }

    static StreamProductJobView from(StreamProductJobSnapshot snapshot) {
        return new StreamProductJobView(
                snapshot.submissionId(), snapshot.jobId(),
                snapshot.artifactId(), snapshot.status(), snapshot.progress(),
                snapshot.cancelCommandId(), snapshot.cancelStatusOrNull());
    }

    static StreamProductJobView from(StreamProductJobSnapshot snapshot,
            StreamProductCheckpointOperation checkpoint) {
        return new StreamProductJobView(
                snapshot.submissionId(), snapshot.jobId(),
                snapshot.artifactId(), snapshot.status(), snapshot.progress(),
                snapshot.cancelCommandId(), snapshot.cancelStatusOrNull(),
                checkpoint.commandId(), checkpoint.checkpointId(),
                checkpoint.status());
    }

    static StreamProductJobView from(StreamProductJobSnapshot snapshot,
            StreamProductSavepointOperation savepoint) {
        return new StreamProductJobView(
                snapshot.submissionId(), snapshot.jobId(),
                snapshot.artifactId(), snapshot.status(), snapshot.progress(),
                snapshot.cancelCommandId(), snapshot.cancelStatusOrNull(),
                null, null, null, savepoint.commandId(),
                savepoint.savepointId(), savepoint.status());
    }

    static StreamProductJobView from(StreamProductJobSnapshot snapshot,
            StreamProductRescaleOperation rescale) {
        return new StreamProductJobView(
                snapshot.submissionId(), snapshot.jobId(),
                snapshot.artifactId(), snapshot.status(), snapshot.progress(),
                snapshot.cancelCommandId(), snapshot.cancelStatusOrNull(),
                null, null, null, null, null, null,
                rescale.commandId(), rescale.rescaleId(),
                rescale.targetParallelism(), rescale.status());
    }

    static StreamProductJobView fromEvidence(
            StreamProductJobSnapshot snapshot) {
        return new StreamProductJobView(
                snapshot.submissionId(), snapshot.jobId(),
                snapshot.artifactId(), snapshot.status(), snapshot.progress(),
                snapshot.cancelCommandId(), snapshot.cancelStatusOrNull(),
                null, null, null, null, null, null,
                null, null, 0, null, snapshot.evidence());
    }

    StreamSubmissionId submissionId() { return submissionId; }
    StreamJobId jobId() { return jobId; }
    String artifactId() { return artifactId; }
    StreamJobStatus status() { return status; }
    StreamJobProgress progress() { return progress; }
    StreamCommandId cancelCommandId() { return cancelCommandId; }
    StreamOperationStatus cancelStatus() { return cancelStatus; }
    StreamCommandId checkpointCommandId() { return checkpointCommandId; }
    String checkpointId() { return checkpointId; }
    StreamOperationStatus checkpointStatus() { return checkpointStatus; }
    StreamCommandId savepointCommandId() { return savepointCommandId; }
    String savepointId() { return savepointId; }
    StreamOperationStatus savepointStatus() { return savepointStatus; }
    StreamCommandId rescaleCommandId() { return rescaleCommandId; }
    String rescaleId() { return rescaleId; }
    int targetParallelism() { return targetParallelism; }
    StreamOperationStatus rescaleStatus() { return rescaleStatus; }
    StreamJobEvidence evidence() { return evidence; }
}
