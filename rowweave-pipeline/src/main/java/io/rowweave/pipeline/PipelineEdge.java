package io.rowweave.pipeline;

import java.util.Objects;

/** Explicit materialized data boundary between complete execution units. */
public final class PipelineEdge {
    public enum BoundaryKind {
        VERSIONED_DATASET
    }

    private final String edgeId;
    private final String producerStepId;
    private final String producerSinkOperatorId;
    private final String consumerStepId;
    private final String consumerSourceOperatorId;
    private final String materializationId;
    private final String schemaFingerprint;
    private final BoundaryKind boundaryKind;

    public PipelineEdge(
            String edgeId, String producerStepId, String producerSinkOperatorId,
            String consumerStepId, String consumerSourceOperatorId,
            String materializationId, String schemaFingerprint,
            BoundaryKind boundaryKind) {
        this.edgeId = id(edgeId, "edge.edgeId");
        this.producerStepId = id(producerStepId, "edge.producerStepId");
        this.producerSinkOperatorId = id(
                producerSinkOperatorId, "edge.producerSinkOperatorId");
        this.consumerStepId = id(consumerStepId, "edge.consumerStepId");
        this.consumerSourceOperatorId = id(
                consumerSourceOperatorId, "edge.consumerSourceOperatorId");
        this.materializationId = id(materializationId, "edge.materializationId");
        this.schemaFingerprint = PipelineText.schema(
                schemaFingerprint, "edge.schemaFingerprint");
        if (boundaryKind == null) {
            throw PipelineText.failure("INVALID_BOUNDARY", "edge.boundaryKind",
                    "boundary kind is required");
        }
        this.boundaryKind = boundaryKind;
    }

    private static String id(String value, String path) {
        return PipelineText.required(value, path, PipelineLimits.MAX_ID_BYTES);
    }

    public String edgeId() { return edgeId; }
    public String producerStepId() { return producerStepId; }
    public String producerSinkOperatorId() { return producerSinkOperatorId; }
    public String consumerStepId() { return consumerStepId; }
    public String consumerSourceOperatorId() { return consumerSourceOperatorId; }
    public String materializationId() { return materializationId; }
    public String schemaFingerprint() { return schemaFingerprint; }
    public BoundaryKind boundaryKind() { return boundaryKind; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PipelineEdge)) return false;
        PipelineEdge that = (PipelineEdge) other;
        return edgeId.equals(that.edgeId)
                && producerStepId.equals(that.producerStepId)
                && producerSinkOperatorId.equals(that.producerSinkOperatorId)
                && consumerStepId.equals(that.consumerStepId)
                && consumerSourceOperatorId.equals(that.consumerSourceOperatorId)
                && materializationId.equals(that.materializationId)
                && schemaFingerprint.equals(that.schemaFingerprint)
                && boundaryKind == that.boundaryKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeId, producerStepId, producerSinkOperatorId,
                consumerStepId, consumerSourceOperatorId, materializationId,
                schemaFingerprint, boundaryKind);
    }
}
