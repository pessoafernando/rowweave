package io.rowweave.upgrade;

import java.util.Objects;

/** Accepted command identity retained for replay-idempotence. */
public final class UpgradeCommandEvidence {
    private final String commandId;
    private final String commandDigest;

    public UpgradeCommandEvidence(String commandId, String commandDigest) {
        UpgradeCodecSupport.requireText(commandId, "command id");
        UpgradeCodecSupport.requireDigest(commandDigest, "command digest");
        this.commandId = commandId;
        this.commandDigest = commandDigest;
    }

    public String commandId() { return commandId; }
    public String commandDigest() { return commandDigest; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof UpgradeCommandEvidence)) return false;
        UpgradeCommandEvidence that = (UpgradeCommandEvidence) other;
        return commandId.equals(that.commandId)
                && commandDigest.equals(that.commandDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commandId, commandDigest);
    }
}
