package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamOperationStatus;

import java.util.Objects;

/** Immutable durable projection of one live rescale operation. */
final class StreamProductRescaleOperation {
    enum Phase {
        INTENT_DURABLE(1),
        CHECKPOINTING(2),
        CUTOVER_DURABLE(3),
        CUTOVER_COMPLETE(4),
        FAILED(5);

        private final int wireId;
        Phase(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static Phase fromWireId(int wireId) {
            for (Phase value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw new IllegalArgumentException("unknown rescale phase");
        }
    }

    private final StreamCommandId commandId;
    private final String rescaleId;
    private final int targetParallelism;
    private final long sourceGeneration;
    private final long targetGeneration;
    private final StreamOperationStatus status;
    private final Phase phase;

    StreamProductRescaleOperation(StreamCommandId commandId,
            String rescaleId, int targetParallelism,
            long sourceGeneration, long targetGeneration,
            StreamOperationStatus status, Phase phase) {
        if (commandId == null || rescaleId == null || rescaleId.isEmpty()
                || targetParallelism < 1 || sourceGeneration < 1L
                || targetGeneration != sourceGeneration + 1L
                || status == null || phase == null
                || !consistent(status, phase)) {
            throw new IllegalArgumentException(
                    "live rescale operation is invalid");
        }
        this.commandId = commandId;
        this.rescaleId = rescaleId;
        this.targetParallelism = targetParallelism;
        this.sourceGeneration = sourceGeneration;
        this.targetGeneration = targetGeneration;
        this.status = status;
        this.phase = phase;
    }

    StreamCommandId commandId() { return commandId; }
    String rescaleId() { return rescaleId; }
    int targetParallelism() { return targetParallelism; }
    long sourceGeneration() { return sourceGeneration; }
    long targetGeneration() { return targetGeneration; }
    StreamOperationStatus status() { return status; }
    Phase phase() { return phase; }

    StreamProductRescaleOperation with(
            StreamOperationStatus nextStatus, Phase nextPhase) {
        return new StreamProductRescaleOperation(commandId, rescaleId,
                targetParallelism, sourceGeneration, targetGeneration,
                nextStatus, nextPhase);
    }

    private static boolean consistent(
            StreamOperationStatus status, Phase phase) {
        switch (phase) {
            case INTENT_DURABLE:
                return status == StreamOperationStatus.ACCEPTED;
            case CHECKPOINTING:
            case CUTOVER_DURABLE:
                return status == StreamOperationStatus.RUNNING;
            case CUTOVER_COMPLETE:
                return status == StreamOperationStatus.SUCCEEDED;
            case FAILED:
                return status == StreamOperationStatus.FAILED;
            default:
                return false;
        }
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamProductRescaleOperation)) return false;
        StreamProductRescaleOperation that =
                (StreamProductRescaleOperation) other;
        return targetParallelism == that.targetParallelism
                && sourceGeneration == that.sourceGeneration
                && targetGeneration == that.targetGeneration
                && commandId.equals(that.commandId)
                && rescaleId.equals(that.rescaleId)
                && status == that.status && phase == that.phase;
    }

    @Override public int hashCode() {
        return Objects.hash(commandId, rescaleId, targetParallelism,
                sourceGeneration, targetGeneration, status, phase);
    }
}
