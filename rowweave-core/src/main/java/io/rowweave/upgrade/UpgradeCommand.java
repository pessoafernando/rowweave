package io.rowweave.upgrade;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Fully bound, replay-safe command for an upgrade authority transition. */
public final class UpgradeCommand {
    public enum Kind {
        QUIESCE(1),
        BEGIN_COPY(2),
        VERIFY(3),
        CUTOVER(4),
        RESUME_WRITES(5),
        MARK_NEW_SEMANTICS(6),
        DATA_ROLLBACK(7),
        MARK_CLEANUP_ELIGIBLE(8),
        FREEZE_FAILURE(9),
        FREEZE_ADMISSION(10);

        private final int wireId;

        Kind(int wireId) { this.wireId = wireId; }
        public int wireId() { return wireId; }
    }

    private final Kind kind;
    private final String commandId;
    private final String releaseId;
    private final String storeId;
    private final long generation;
    private final String sourceNamespace;
    private final String sourceDigest;
    private final String targetNamespace;
    private final String targetDigest;
    private final UpgradeOwner owner;
    private final long expectedActivePointerVersion;
    private final long verifiedObjectCount;
    private final int verifiedShardCount;
    private final String verifiedSourceDigest;
    private final String verifiedTargetDigest;
    private final long rollbackDeadlineEpochMillis;
    private final String commandDigest;

    public UpgradeCommand(
            Kind kind, String commandId, String releaseId, String storeId,
            long generation, String sourceNamespace, String sourceDigest,
            String targetNamespace, String targetDigest, UpgradeOwner owner,
            long expectedActivePointerVersion, long verifiedObjectCount,
            int verifiedShardCount, String verifiedSourceDigest,
            String verifiedTargetDigest, long rollbackDeadlineEpochMillis) {
        if (kind == null || owner == null) {
            throw new IllegalArgumentException("command kind and owner are required");
        }
        UpgradeCodecSupport.requireText(commandId, "command id");
        UpgradeCodecSupport.requireText(releaseId, "release id");
        UpgradeCodecSupport.requireText(storeId, "store id");
        UpgradeCodecSupport.requireText(sourceNamespace, "source namespace");
        UpgradeCodecSupport.requireDigest(sourceDigest, "source digest");
        UpgradeCodecSupport.requireText(targetNamespace, "target namespace");
        UpgradeCodecSupport.requireDigest(targetDigest, "target digest");
        UpgradeCodecSupport.requireDigest(
                verifiedSourceDigest, "verified source digest");
        UpgradeCodecSupport.requireDigest(
                verifiedTargetDigest, "verified target digest");
        if (generation <= 0L || expectedActivePointerVersion < 0L
                || verifiedObjectCount < 0L || verifiedShardCount < 0
                || rollbackDeadlineEpochMillis < 0L) {
            throw new IllegalArgumentException("command counters are invalid");
        }
        this.kind = kind;
        this.commandId = commandId;
        this.releaseId = releaseId;
        this.storeId = storeId;
        this.generation = generation;
        this.sourceNamespace = sourceNamespace;
        this.sourceDigest = sourceDigest;
        this.targetNamespace = targetNamespace;
        this.targetDigest = targetDigest;
        this.owner = owner;
        this.expectedActivePointerVersion = expectedActivePointerVersion;
        this.verifiedObjectCount = verifiedObjectCount;
        this.verifiedShardCount = verifiedShardCount;
        this.verifiedSourceDigest = verifiedSourceDigest;
        this.verifiedTargetDigest = verifiedTargetDigest;
        this.rollbackDeadlineEpochMillis = rollbackDeadlineEpochMillis;
        this.commandDigest = digest();
    }

    public Kind kind() { return kind; }
    public String commandId() { return commandId; }
    public String releaseId() { return releaseId; }
    public String storeId() { return storeId; }
    public long generation() { return generation; }
    public String sourceNamespace() { return sourceNamespace; }
    public String sourceDigest() { return sourceDigest; }
    public String targetNamespace() { return targetNamespace; }
    public String targetDigest() { return targetDigest; }
    public UpgradeOwner owner() { return owner; }
    public long expectedActivePointerVersion() { return expectedActivePointerVersion; }
    public long verifiedObjectCount() { return verifiedObjectCount; }
    public int verifiedShardCount() { return verifiedShardCount; }
    public String verifiedSourceDigest() { return verifiedSourceDigest; }
    public String verifiedTargetDigest() { return verifiedTargetDigest; }
    public long rollbackDeadlineEpochMillis() { return rollbackDeadlineEpochMillis; }
    public String commandDigest() { return commandDigest; }

    private String digest() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            UpgradeCodecSupport.writeString(out, "rowweave-upgrade-command-v1");
            out.writeInt(kind.wireId());
            UpgradeCodecSupport.writeString(out, commandId);
            UpgradeCodecSupport.writeString(out, releaseId);
            UpgradeCodecSupport.writeString(out, storeId);
            out.writeLong(generation);
            UpgradeCodecSupport.writeString(out, sourceNamespace);
            UpgradeCodecSupport.writeDigest(out, sourceDigest);
            UpgradeCodecSupport.writeString(out, targetNamespace);
            UpgradeCodecSupport.writeDigest(out, targetDigest);
            out.writeLong(owner.term());
            UpgradeCodecSupport.writeString(out, owner.masterId());
            UpgradeCodecSupport.writeString(out, owner.incarnationId());
            out.writeLong(owner.fence());
            out.writeLong(expectedActivePointerVersion);
            out.writeLong(verifiedObjectCount);
            out.writeInt(verifiedShardCount);
            UpgradeCodecSupport.writeDigest(out, verifiedSourceDigest);
            UpgradeCodecSupport.writeDigest(out, verifiedTargetDigest);
            out.writeLong(rollbackDeadlineEpochMillis);
            out.flush();
            return UpgradeCodecSupport.sha256Hex(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory command digest failed", impossible);
        }
    }
}
