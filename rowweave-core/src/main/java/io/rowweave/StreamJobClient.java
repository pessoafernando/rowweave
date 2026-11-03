package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamSubmissionId;

/** Sole product submission/reattachment boundary for resident Stream jobs. */
public interface StreamJobClient extends AutoCloseable {
    StreamJobHandle submit(StreamSubmissionId submissionId,
                           PlanArtifact streamArtifact);
    StreamJobHandle attachSubmission(StreamSubmissionId submissionId);
    StreamJobHandle attach(StreamJobId jobId);
    StreamCommandHandle attachCancel(StreamJobId jobId,
                                     io.rowweave.stream.api.StreamCommandId commandId);
    StreamCheckpointHandle attachCheckpoint(StreamJobId jobId,
                                             io.rowweave.stream.api.StreamCommandId commandId);
    StreamSavepointHandle attachSavepoint(StreamJobId jobId,
                                           io.rowweave.stream.api.StreamCommandId commandId);
    default StreamRescaleHandle attachRescale(StreamJobId jobId,
            io.rowweave.stream.api.StreamCommandId commandId) {
        throw new UnsupportedOperationException(
                "live rescale control is unavailable");
    }
    StreamJobHandle restore(StreamSubmissionId submissionId,
                            PlanArtifact streamArtifact,
                            StreamRestoreRequest request);
    StreamJobHandle offlineRescale(StreamSubmissionId submissionId,
                                   PlanArtifact streamArtifact,
                                   StreamOfflineRescaleRequest request);

    @Override
    default void close() { }
}
