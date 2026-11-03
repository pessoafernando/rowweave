package io.rowweave.incremental;

import java.util.function.LongConsumer;

/** Bounded capture/materialization boundary implemented locally or by Worker RPC. */
public interface IncrementalSourceActions {
    CapturedTargetReceipt capture(
            IncrementalCaptureRequest request, long deadlineNanos);

    IncrementalMaterializationResponse materialize(
            CapturedTargetReceipt capturedTarget, long deadlineNanos,
            LongConsumer reservation);
}
