package io.rowweave.stream.contract;

import java.util.Objects;

/** Immutable long-lived copy of one completed checkpoint manifest. */
public final class StreamSavepoint {
    private final String savepointId;
    private final CheckpointManifest checkpointManifest;

    public StreamSavepoint(String savepointId,
                           CheckpointManifest checkpointManifest) {
        this.savepointId = canonicalId(savepointId);
        if (checkpointManifest == null) {
            throw StreamContractSupport.failure("INVALID_SAVEPOINT",
                    "savepoint.manifest",
                    "checkpoint manifest is required");
        }
        this.checkpointManifest = checkpointManifest;
    }

    public String savepointId() {
        return savepointId;
    }

    public static String canonicalId(String savepointId) {
        return StreamContractSupport.text(savepointId, "savepoint.id", 256);
    }

    public CheckpointManifest checkpointManifest() {
        return checkpointManifest;
    }

    public String savepointDigest() {
        return StreamContractSupport.sha256Hex(
                StreamSavepointCodec.encode(this));
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamSavepoint)) return false;
        StreamSavepoint that = (StreamSavepoint) other;
        return savepointId.equals(that.savepointId)
                && checkpointManifest.equals(that.checkpointManifest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(savepointId, checkpointManifest);
    }
}
