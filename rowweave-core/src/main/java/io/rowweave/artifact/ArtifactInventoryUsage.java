package io.rowweave.artifact;

/** One inventory usage observation with a hard bound; it never takes part in a deletion decision. */
public final class ArtifactInventoryUsage {
    public enum Scope { UNKNOWN, GLOBAL, SHARD }

    private final Scope scope;
    private final int shard;
    private final int entries;
    private final long bytes;
    private final int entryCapacity;
    private final long byteCapacity;

    private ArtifactInventoryUsage(Scope scope, int shard, int entries,
                                   long bytes, int entryCapacity,
                                   long byteCapacity) {
        if (scope == null) throw new IllegalArgumentException("scope is required");
        if (((scope == Scope.UNKNOWN || scope == Scope.GLOBAL) && shard != -1)
                || (scope == Scope.SHARD && (shard < 0 || shard > 255))) {
            throw new IllegalArgumentException("inventory usage shard is invalid");
        }
        if (entryCapacity < 1 || byteCapacity < 1L
                || entries < 0 || entries > entryCapacity
                || bytes < 0L || bytes > byteCapacity) {
            throw new IllegalArgumentException("inventory usage is out of bounds");
        }
        this.scope = scope;
        this.shard = shard;
        this.entries = entries;
        this.bytes = bytes;
        this.entryCapacity = entryCapacity;
        this.byteCapacity = byteCapacity;
    }

    public static ArtifactInventoryUsage global(
            int entries, long bytes, int entryCapacity, long byteCapacity) {
        return new ArtifactInventoryUsage(Scope.GLOBAL, -1, entries, bytes,
                entryCapacity, byteCapacity);
    }

    public static ArtifactInventoryUsage unknown(
            int entryCapacity, long byteCapacity) {
        return new ArtifactInventoryUsage(Scope.UNKNOWN, -1, 0, 0L,
                entryCapacity, byteCapacity);
    }

    public static ArtifactInventoryUsage shard(
            int shard, int entries, long bytes,
            int entryCapacity, long byteCapacity) {
        return new ArtifactInventoryUsage(Scope.SHARD, shard, entries, bytes,
                entryCapacity, byteCapacity);
    }

    public Scope scope() { return scope; }
    public int shard() { return shard; }
    public int entries() { return entries; }
    public long bytes() { return bytes; }
    public int entryCapacity() { return entryCapacity; }
    public long byteCapacity() { return byteCapacity; }
}
