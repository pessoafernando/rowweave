package io.rowweave.artifact;

import io.rowweave.incremental.IncrementalClaimRequest;
import io.rowweave.incremental.IncrementalPendingRunDecision;

/** Authority callback that creates or replays one exact zero-attempt pending Run. */
@FunctionalInterface
public interface IncrementalPendingRunPublisher {
    IncrementalPendingRunDecision publish(IncrementalClaimRequest request);
}
