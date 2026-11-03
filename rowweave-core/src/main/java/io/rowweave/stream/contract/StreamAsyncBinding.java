package io.rowweave.stream.contract;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;

import java.util.Objects;

/** Portable supervised async request, replay and capacity contract. */
public final class StreamAsyncBinding implements StreamNodeBinding {
    public enum Ordering {
        ORDERED(1), UNORDERED(2);
        private final int wireId;
        Ordering(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static Ordering fromWireId(int wireId) {
            for (Ordering value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_ASYNC_ORDERING", "unknown ordering");
        }
    }

    public enum ReplayMode {
        AT_LEAST_ONCE(1), IMMUTABLE_VERSION(2), IDENTITY_KEYED(3);
        private final int wireId;
        ReplayMode(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static ReplayMode fromWireId(int wireId) {
            for (ReplayMode value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_ASYNC_REPLAY_MODE", "unknown replay mode");
        }
    }

    private final FunctionDeploymentReference deploymentReference;
    private final Ordering ordering;
    private final ReplayMode replayMode;
    private final String replayAuthorityId;
    private final String replayAuthorityDigest;
    private final StreamAsyncResourceDescriptor resources;

    public StreamAsyncBinding(FunctionDeploymentReference deploymentReference,
            Ordering ordering, ReplayMode replayMode,
            String replayAuthorityId, String replayAuthorityDigest,
            StreamAsyncResourceDescriptor resources) {
        boolean replayStable = replayMode != ReplayMode.AT_LEAST_ONCE;
        if (deploymentReference == null || ordering == null
                || replayMode == null || resources == null
                || deploymentReference.functionAbi()
                != FunctionAbi.STREAM_ASYNC_V1
                || replayStable != (replayAuthorityId != null
                && replayAuthorityDigest != null)
                || !replayStable && (replayAuthorityId != null
                || replayAuthorityDigest != null)) {
            throw failure("INVALID_ASYNC_BINDING", "async binding is invalid");
        }
        this.deploymentReference = deploymentReference;
        this.ordering = ordering;
        this.replayMode = replayMode;
        this.replayAuthorityId = replayAuthorityId == null ? null
                : StreamContractSupport.text(replayAuthorityId,
                "nodes.async.replayAuthorityId", 256);
        this.replayAuthorityDigest = replayAuthorityDigest == null ? null
                : StreamContractSupport.digest(replayAuthorityDigest,
                "nodes.async.replayAuthorityDigest");
        this.resources = resources;
    }

    private static StreamContractException failure(String code,
            String message) {
        return StreamContractSupport.failure(code, "nodes.async", message);
    }

    @Override public StreamOperatorCode.BindingKind kind() {
        return StreamOperatorCode.BindingKind.ASYNC;
    }
    public FunctionDeploymentReference deploymentReference() {
        return deploymentReference;
    }
    public Ordering ordering() { return ordering; }
    public ReplayMode replayMode() { return replayMode; }
    public String replayAuthorityId() { return replayAuthorityId; }
    public String replayAuthorityDigest() { return replayAuthorityDigest; }
    public StreamAsyncResourceDescriptor resources() { return resources; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamAsyncBinding)) return false;
        StreamAsyncBinding that = (StreamAsyncBinding) other;
        return deploymentReference.equals(that.deploymentReference)
                && ordering == that.ordering && replayMode == that.replayMode
                && Objects.equals(replayAuthorityId, that.replayAuthorityId)
                && Objects.equals(replayAuthorityDigest,
                that.replayAuthorityDigest)
                && resources.equals(that.resources);
    }

    @Override public int hashCode() {
        return Objects.hash(deploymentReference, ordering, replayMode,
                replayAuthorityId, replayAuthorityDigest, resources);
    }
}
