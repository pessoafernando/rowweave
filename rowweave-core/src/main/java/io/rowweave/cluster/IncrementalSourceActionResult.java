package io.rowweave.cluster;

import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.IncrementalMaterializationResponse;

import java.util.Objects;

/** Bounded Worker result containing either a capture or materialization value. */
final class IncrementalSourceActionResult {
    enum Kind { CAPTURED, MATERIALIZED }

    private final Kind kind;
    private final CapturedTargetReceipt capturedTarget;
    private final IncrementalMaterializationResponse materialization;

    private IncrementalSourceActionResult(
            Kind kind, CapturedTargetReceipt capturedTarget,
            IncrementalMaterializationResponse materialization) {
        if (kind == null
                || (kind == Kind.CAPTURED) != (capturedTarget != null)
                || (kind == Kind.MATERIALIZED) != (materialization != null)) {
            throw new IllegalArgumentException(
                    "incremental source action result fields are invalid");
        }
        this.kind = kind;
        this.capturedTarget = capturedTarget;
        this.materialization = materialization;
    }

    static IncrementalSourceActionResult captured(
            CapturedTargetReceipt capturedTarget) {
        return new IncrementalSourceActionResult(
                Kind.CAPTURED, capturedTarget, null);
    }

    static IncrementalSourceActionResult materialized(
            IncrementalMaterializationResponse materialization) {
        return new IncrementalSourceActionResult(
                Kind.MATERIALIZED, null, materialization);
    }

    Kind kind() { return kind; }
    CapturedTargetReceipt capturedTarget() { return capturedTarget; }
    IncrementalMaterializationResponse materialization() {
        return materialization;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalSourceActionResult)) return false;
        IncrementalSourceActionResult that =
                (IncrementalSourceActionResult) other;
        return kind == that.kind
                && Objects.equals(capturedTarget, that.capturedTarget)
                && Objects.equals(materialization, that.materialization);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, capturedTarget, materialization);
    }
}
