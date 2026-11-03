package io.rowweave.stream.contract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** Canonical digest-only joint recovery manifest for one checkpoint cut. */
public final class CheckpointManifest {
    private final CheckpointIdentity checkpointIdentity;
    private final List<CheckpointTaskReceipt> taskReceipts;

    public CheckpointManifest(
            CheckpointIdentity checkpointIdentity,
            List<CheckpointTaskReceipt> taskReceipts) {
        if (checkpointIdentity == null || taskReceipts == null
                || taskReceipts.isEmpty()
                || taskReceipts.size()
                > CheckpointManifestCodec.MAX_TASK_RECEIPTS) {
            throw StreamContractSupport.failure("INVALID_CHECKPOINT_MANIFEST",
                    "checkpoint.manifest", "bounded non-empty task receipts are required");
        }
        TreeMap<String, CheckpointTaskReceipt> canonical = new TreeMap<>();
        for (CheckpointTaskReceipt receipt : taskReceipts) {
            if (receipt == null
                    || !checkpointIdentity.equals(receipt.checkpointIdentity())) {
                throw StreamContractSupport.failure("STALE_CHECKPOINT",
                        "checkpoint.manifest.receipts",
                        "every receipt must belong to the manifest checkpoint");
            }
            if (canonical.put(receipt.taskId(), receipt) != null) {
                throw StreamContractSupport.failure("DUPLICATE_TASK",
                        "checkpoint.manifest.receipts",
                        "task receipts must have unique identities");
            }
        }
        this.checkpointIdentity = checkpointIdentity;
        this.taskReceipts = Collections.unmodifiableList(
                new ArrayList<>(canonical.values()));
    }

    public CheckpointIdentity checkpointIdentity() { return checkpointIdentity; }
    public List<CheckpointTaskReceipt> taskReceipts() { return taskReceipts; }
    public String manifestDigest() { return CheckpointManifestCodec.digestOf(this); }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CheckpointManifest)) return false;
        CheckpointManifest that = (CheckpointManifest) other;
        return checkpointIdentity.equals(that.checkpointIdentity)
                && taskReceipts.equals(that.taskReceipts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpointIdentity, taskReceipts);
    }
}
