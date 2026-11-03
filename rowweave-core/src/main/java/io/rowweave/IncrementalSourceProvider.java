package io.rowweave;

import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.IncrementalCaptureRequest;
import io.rowweave.incremental.IncrementalCaptureResponse;
import io.rowweave.incremental.IncrementalMaterializationResponse;
import io.rowweave.incremental.IncrementalSourceConfig;
import io.rowweave.incremental.IncrementalSourceProviderContract;

import java.io.Serializable;

/** Stateless same-build extension for bounded incremental source control actions. */
public interface IncrementalSourceProvider extends Serializable {

    /** Runtime self-description; the Worker compares it with the signed contract. */
    IncrementalSourceProviderContract contract();

    /** Decode and re-encode the inert configuration into exact canonical bytes. */
    byte[] canonicalizeConfiguration(IncrementalSourceConfig configuration);

    /** Perform one read-only, bounded target capture. */
    IncrementalCaptureResponse captureTarget(
            IncrementalSourceConfig configuration,
            IncrementalCaptureRequest request);

    /** Materialize exact split metadata from the already frozen capture receipt. */
    IncrementalMaterializationResponse materializeInterval(
            IncrementalSourceConfig configuration,
            CapturedTargetReceipt capturedTarget);

    /**
     * Build one unopened reader for the already frozen split. The runtime calls
     * this only after exact pre-open verification; open/close remain supervised
     * while row reads execute directly on the bounded source stage thread.
     */
    default SourceExecutor<IncrementalSourceSplit> openSplit(
            IncrementalSourceSplit input) {
        throw new UnsupportedOperationException(
                "INCREMENTAL_SOURCE_OPEN_UNSUPPORTED");
    }
}
