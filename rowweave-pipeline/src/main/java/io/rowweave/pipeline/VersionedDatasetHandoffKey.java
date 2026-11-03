package io.rowweave.pipeline;

import java.util.Objects;

/** Identity of one logical VERSIONED_DATASET winner slot. */
public final class VersionedDatasetHandoffKey {
    private final String pipelineId;
    private final String revisionId;
    private final String definitionDigest;
    private final String edgeId;
    private final String materializationId;
    private final String schemaFingerprint;
    private final String producerStepId;
    private final String producerSinkOperatorId;
    private final String producerArtifactId;
    private final String logicalRunId;

    public VersionedDatasetHandoffKey(
            String pipelineId, String revisionId, String definitionDigest,
            String edgeId, String materializationId, String schemaFingerprint,
            String producerStepId, String producerSinkOperatorId,
            String producerArtifactId, String logicalRunId) {
        this.pipelineId = VersionedDatasetHandoffSupport.id(pipelineId, "key.pipelineId");
        this.revisionId = VersionedDatasetHandoffSupport.id(revisionId, "key.revisionId");
        this.definitionDigest = VersionedDatasetHandoffSupport.digest(
                definitionDigest, "key.definitionDigest");
        this.edgeId = VersionedDatasetHandoffSupport.id(edgeId, "key.edgeId");
        this.materializationId = VersionedDatasetHandoffSupport.id(
                materializationId, "key.materializationId");
        this.schemaFingerprint = PipelineText.schema(
                schemaFingerprint, "key.schemaFingerprint");
        this.producerStepId = VersionedDatasetHandoffSupport.id(
                producerStepId, "key.producerStepId");
        this.producerSinkOperatorId = VersionedDatasetHandoffSupport.id(
                producerSinkOperatorId, "key.producerSinkOperatorId");
        this.producerArtifactId = VersionedDatasetHandoffSupport.id(
                producerArtifactId, "key.producerArtifactId");
        this.logicalRunId = VersionedDatasetHandoffSupport.id(
                logicalRunId, "key.logicalRunId");
    }

    public String pipelineId() { return pipelineId; }
    public String revisionId() { return revisionId; }
    public String definitionDigest() { return definitionDigest; }
    public String edgeId() { return edgeId; }
    public String materializationId() { return materializationId; }
    public String schemaFingerprint() { return schemaFingerprint; }
    public String producerStepId() { return producerStepId; }
    public String producerSinkOperatorId() { return producerSinkOperatorId; }
    public String producerArtifactId() { return producerArtifactId; }
    public String logicalRunId() { return logicalRunId; }
    public String keyDigest() {
        return VersionedDatasetHandoffCodec.sha256Hex(
                VersionedDatasetHandoffCodec.encodeKey(this));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof VersionedDatasetHandoffKey)) return false;
        VersionedDatasetHandoffKey that = (VersionedDatasetHandoffKey) other;
        return pipelineId.equals(that.pipelineId)
                && revisionId.equals(that.revisionId)
                && definitionDigest.equals(that.definitionDigest)
                && edgeId.equals(that.edgeId)
                && materializationId.equals(that.materializationId)
                && schemaFingerprint.equals(that.schemaFingerprint)
                && producerStepId.equals(that.producerStepId)
                && producerSinkOperatorId.equals(that.producerSinkOperatorId)
                && producerArtifactId.equals(that.producerArtifactId)
                && logicalRunId.equals(that.logicalRunId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipelineId, revisionId, definitionDigest, edgeId,
                materializationId, schemaFingerprint, producerStepId,
                producerSinkOperatorId, producerArtifactId, logicalRunId);
    }
}
