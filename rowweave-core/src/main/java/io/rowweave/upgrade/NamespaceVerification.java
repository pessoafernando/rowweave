package io.rowweave.upgrade;

/** Bounded evidence produced by a complete source/shadow namespace scan. */
public final class NamespaceVerification {
    private final long objectCount;
    private final int shardCount;
    private final long evidenceCount;
    private final String sourceDigest;
    private final String targetDigest;

    public NamespaceVerification(long objectCount, int shardCount,
                                 String sourceDigest, String targetDigest) {
        this(objectCount, shardCount, objectCount, sourceDigest, targetDigest);
    }

    public NamespaceVerification(long objectCount, int shardCount,
                                 long evidenceCount,
                                 String sourceDigest, String targetDigest) {
        if (objectCount < 0L || shardCount < 0 || evidenceCount < 0L
                || evidenceCount != objectCount) {
            throw new IllegalArgumentException("namespace counts are invalid");
        }
        UpgradeCodecSupport.requireDigest(sourceDigest, "source digest");
        UpgradeCodecSupport.requireDigest(targetDigest, "target digest");
        this.objectCount = objectCount;
        this.shardCount = shardCount;
        this.evidenceCount = evidenceCount;
        this.sourceDigest = sourceDigest;
        this.targetDigest = targetDigest;
    }

    public long objectCount() { return objectCount; }
    public int shardCount() { return shardCount; }
    public long evidenceCount() { return evidenceCount; }
    public String sourceDigest() { return sourceDigest; }
    public String targetDigest() { return targetDigest; }
}
