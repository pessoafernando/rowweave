package io.rowweave.incremental;

import io.rowweave.IncrementalSourceProvider;
import io.rowweave.IncrementalSourceSplit;
import io.rowweave.SourceExecutor;
import io.rowweave.ObjectStorageReadRequest;

import java.util.ServiceLoader;

/** Core adapter for immutable manifest capture and exact object-version reads. */
public final class ObjectManifestIncrementalSourceProvider
        implements IncrementalSourceProvider {
    private static final long serialVersionUID = 1L;
    private final transient ObjectManifestIncrementalSourceAccess directAccess;

    public ObjectManifestIncrementalSourceProvider() {
        this(null);
    }

    public ObjectManifestIncrementalSourceProvider(
            ObjectManifestIncrementalSourceAccess access) {
        this.directAccess = access;
    }

    @Override
    public IncrementalSourceProviderContract contract() {
        return ObjectManifestIncrementalSourceProfile.builtInContract();
    }

    @Override
    public byte[] canonicalizeConfiguration(
            IncrementalSourceConfig configuration) {
        return profile(configuration).canonicalizeConfiguration(configuration);
    }

    @Override
    public IncrementalCaptureResponse captureTarget(
            IncrementalSourceConfig configuration,
            IncrementalCaptureRequest request) {
        ObjectManifestIncrementalSourceProfile profile = profile(configuration);
        ObjectManifestIncrementalSourceProfile.Manifest manifest =
                access(profile).capture(profile, request);
        if (manifest == null) {
            throw new IllegalArgumentException(
                    "OBJECT_MANIFEST_CAPTURE_INVALID");
        }
        return profile.capture(request, manifest);
    }

    @Override
    public IncrementalMaterializationResponse materializeInterval(
            IncrementalSourceConfig configuration,
            CapturedTargetReceipt capturedTarget) {
        ObjectManifestIncrementalSourceProfile profile = profile(configuration);
        ObjectManifestIncrementalSourceProfile.Manifest manifest =
                ObjectManifestIncrementalSourceProfile.decodeManifestIdentity(
                        capturedTarget.snapshotIdentity());
        return profile.materialize(capturedTarget, manifest,
                access(profile).materialize(
                        profile, capturedTarget, manifest));
    }

    @Override
    public SourceExecutor<IncrementalSourceSplit> openSplit(
            IncrementalSourceSplit input) {
        ObjectManifestIncrementalSourceProfile profile =
                profile(input.configuration());
        ObjectStorageReadRequest request =
                ObjectManifestIncrementalSourceProfile.decodeOpenRequest(
                        input.openRequest());
        profile.verifyOpenRequest(input, request);
        SourceExecutor<IncrementalSourceSplit> opened =
                access(profile).open(profile, input, request);
        if (opened == null) {
            throw new IllegalArgumentException(
                    "OBJECT_MANIFEST_READER_MISSING");
        }
        return opened;
    }

    private static ObjectManifestIncrementalSourceProfile profile(
            IncrementalSourceConfig configuration) {
        return ObjectManifestIncrementalSourceProfile.bind(configuration);
    }

    private ObjectManifestIncrementalSourceAccess access(
            ObjectManifestIncrementalSourceProfile profile) {
        if (directAccess != null) {
            if (!directAccess.supports(profile)) {
                throw new IllegalArgumentException(
                        "OBJECT_MANIFEST_ACCESS_UNAVAILABLE");
            }
            return directAccess;
        }
        ObjectManifestIncrementalSourceAccess found = null;
        int candidates = 0;
        for (ObjectManifestIncrementalSourceAccess candidate
                : ServiceLoader.load(ObjectManifestIncrementalSourceAccess.class)) {
            if (++candidates > 32) {
                throw new IllegalArgumentException(
                        "OBJECT_MANIFEST_ACCESS_AMBIGUOUS");
            }
            if (!candidate.supports(profile)) continue;
            if (found != null) {
                throw new IllegalArgumentException(
                        "OBJECT_MANIFEST_ACCESS_AMBIGUOUS");
            }
            found = candidate;
        }
        if (found == null) {
            throw new IllegalArgumentException(
                    "OBJECT_MANIFEST_ACCESS_UNAVAILABLE");
        }
        return found;
    }
}
