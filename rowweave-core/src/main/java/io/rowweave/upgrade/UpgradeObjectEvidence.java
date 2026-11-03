package io.rowweave.upgrade;

import java.util.Objects;

/** Durable identity and byte digests for one object copied to a shadow store. */
public final class UpgradeObjectEvidence {
    public static final int VERSION_1 = 1;

    private final int version;
    private final long generation;
    private final String relativePath;
    private final String sourceDigest;
    private final String targetDigest;
    private final String commandId;
    private final String commandDigest;

    public UpgradeObjectEvidence(
            int version, long generation, String relativePath,
            String sourceDigest, String targetDigest,
            String commandId, String commandDigest) {
        if (version != VERSION_1 || generation <= 0L) {
            throw new IllegalArgumentException("object evidence version is invalid");
        }
        UpgradeCodecSupport.requireText(relativePath, "relative path");
        UpgradeCodecSupport.requireDigest(sourceDigest, "source digest");
        UpgradeCodecSupport.requireDigest(targetDigest, "target digest");
        UpgradeCodecSupport.requireText(commandId, "command id");
        UpgradeCodecSupport.requireDigest(commandDigest, "command digest");
        this.version = version;
        this.generation = generation;
        this.relativePath = relativePath;
        this.sourceDigest = sourceDigest;
        this.targetDigest = targetDigest;
        this.commandId = commandId;
        this.commandDigest = commandDigest;
    }

    public int version() { return version; }
    public long generation() { return generation; }
    public String relativePath() { return relativePath; }
    public String sourceDigest() { return sourceDigest; }
    public String targetDigest() { return targetDigest; }
    public String commandId() { return commandId; }
    public String commandDigest() { return commandDigest; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof UpgradeObjectEvidence)) return false;
        UpgradeObjectEvidence that = (UpgradeObjectEvidence) other;
        return version == that.version && generation == that.generation
                && relativePath.equals(that.relativePath)
                && sourceDigest.equals(that.sourceDigest)
                && targetDigest.equals(that.targetDigest)
                && commandId.equals(that.commandId)
                && commandDigest.equals(that.commandDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, generation, relativePath, sourceDigest,
                targetDigest, commandId, commandDigest);
    }
}
