package io.rowweave.incremental;

import java.util.Arrays;
import java.util.Objects;

/** Provider result containing no data rows or provider-authored authority fields. */
public final class IncrementalCaptureResponse {
    private final IncrementalFrontier target;
    private final byte[] snapshotIdentity;

    public IncrementalCaptureResponse(
            IncrementalFrontier target, byte[] snapshotIdentity) {
        if (target == null
                || target.kind() == IncrementalFrontier.Kind.BEGIN_V1
                || snapshotIdentity == null || snapshotIdentity.length == 0
                || snapshotIdentity.length
                > CapturedTargetReceipt.MAX_SNAPSHOT_IDENTITY_BYTES) {
            throw new IllegalArgumentException(
                    "incremental capture response fields are invalid");
        }
        this.target = target;
        this.snapshotIdentity = snapshotIdentity.clone();
    }

    public IncrementalFrontier target() { return target; }
    public byte[] snapshotIdentity() { return snapshotIdentity.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalCaptureResponse)) return false;
        IncrementalCaptureResponse that = (IncrementalCaptureResponse) other;
        return target.equals(that.target)
                && Arrays.equals(snapshotIdentity, that.snapshotIdentity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, Arrays.hashCode(snapshotIdentity));
    }
}
