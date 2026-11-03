package io.rowweave.artifact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** A canonical, bounded inventory snapshot for one fixed Artifact hash stripe. */
public final class ArtifactInventorySnapshot {
    public static final int MAX_ENTRIES = 4096;

    private final int shard;
    private final long generation;
    private final List<ArtifactInventoryEntry> entries;
    private final long storedBytes;

    private ArtifactInventorySnapshot(int shard, long generation,
                                      List<ArtifactInventoryEntry> entries,
                                      long storedBytes) {
        this.shard = shard;
        this.generation = generation;
        this.entries = entries;
        this.storedBytes = storedBytes;
    }

    public static ArtifactInventorySnapshot of(int shard, long generation,
                                                List<ArtifactInventoryEntry> entries) {
        if (shard < 0 || shard > 255) {
            throw new IllegalArgumentException("inventory shard is out of bounds");
        }
        if (generation < 0L) {
            throw new IllegalArgumentException("inventory generation must be >= 0");
        }
        if (entries == null) throw new IllegalArgumentException("entries is required");
        if (entries.size() > MAX_ENTRIES) {
            throw new IllegalArgumentException("inventory entry capacity exceeded");
        }
        List<ArtifactInventoryEntry> canonical = new ArrayList<>(entries.size());
        Set<String> ids = new HashSet<>();
        long bytes = 0L;
        for (ArtifactInventoryEntry entry : entries) {
            if (entry == null) throw new IllegalArgumentException("null inventory entry");
            if (ArtifactLifecycleSupport.shard(entry.artifactId()) != shard) {
                throw new IllegalArgumentException("inventory entry is in wrong shard");
            }
            if (!ids.add(entry.artifactId())) {
                throw new IllegalArgumentException("duplicate inventory artifactId");
            }
            try {
                bytes = Math.addExact(bytes, entry.encodedBytes());
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("inventory byte count overflow", overflow);
            }
            canonical.add(entry);
        }
        canonical.sort(Comparator.comparing(ArtifactInventoryEntry::artifactId));
        return new ArtifactInventorySnapshot(shard, generation,
                Collections.unmodifiableList(canonical), bytes);
    }

    public int shard() { return shard; }
    public long generation() { return generation; }
    public List<ArtifactInventoryEntry> entries() { return entries; }
    public long storedBytes() { return storedBytes; }
}
