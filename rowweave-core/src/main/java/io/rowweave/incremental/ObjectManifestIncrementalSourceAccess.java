package io.rowweave.incremental;

import io.rowweave.IncrementalSourceSplit;
import io.rowweave.SourceExecutor;
import io.rowweave.ObjectStorageReadRequest;

import java.util.List;

/** Worker-local object access; capture must read one immutable manifest version. */
public interface ObjectManifestIncrementalSourceAccess {
    boolean supports(ObjectManifestIncrementalSourceProfile profile);

    ObjectManifestIncrementalSourceProfile.Manifest capture(
            ObjectManifestIncrementalSourceProfile profile,
            IncrementalCaptureRequest request);

    List<ObjectManifestIncrementalSourceProfile.Entry> materialize(
            ObjectManifestIncrementalSourceProfile profile,
            CapturedTargetReceipt capturedTarget,
            ObjectManifestIncrementalSourceProfile.Manifest manifest);

    SourceExecutor<IncrementalSourceSplit> open(
            ObjectManifestIncrementalSourceProfile profile,
            IncrementalSourceSplit input,
            ObjectStorageReadRequest request);
}
