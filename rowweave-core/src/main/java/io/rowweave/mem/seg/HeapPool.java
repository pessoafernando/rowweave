package io.rowweave.mem.seg;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Heap implementation of MemorySegmentPool: free lists bucketed by size, so a freed segment is reclaimed and reused at once (deterministic return).
 * The outstanding count supports leak auditing and a double free throws. A segment is zeroed before reuse so an earlier payload cannot leak.
 */
public final class HeapPool implements MemorySegmentPool {

    private final Map<Integer, Deque<MemorySegment>> freeBySize = new HashMap<>();
    private final Set<MemorySegment> live = java.util.Collections.newSetFromMap(new IdentityHashMap<>());

    @Override
    public synchronized MemorySegment allocate(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
        Deque<MemorySegment> bucket = freeBySize.get(size);
        MemorySegment seg = (bucket != null && !bucket.isEmpty()) ? bucket.pop() : new HeapSegment(size);
        live.add(seg);
        return seg;
    }

    @Override
    public synchronized void free(MemorySegment segment) {
        if (!live.remove(segment)) {
            throw new IllegalStateException("segment not allocated by this pool or already freed");
        }
        segment.clear();
        freeBySize.computeIfAbsent(segment.size(), k -> new ArrayDeque<>()).push(segment);
    }

    @Override
    public synchronized int outstanding() {
        return live.size();
    }

    @Override
    public synchronized void trim() {
        freeBySize.clear();
    }

    /** Test probe: idle payload bytes strongly referenced by the pool. */
    public synchronized long cachedBytes() {
        long bytes = 0;
        for (Map.Entry<Integer, Deque<MemorySegment>> entry : freeBySize.entrySet()) {
            bytes = Math.addExact(bytes, Math.multiplyExact((long) entry.getKey(), entry.getValue().size()));
        }
        return bytes;
    }

    /** Test probe: number of non-empty free buckets. */
    public synchronized int freeBucketCount() {
        int count = 0;
        for (Deque<MemorySegment> bucket : freeBySize.values()) {
            if (!bucket.isEmpty()) {
                count++;
            }
        }
        return count;
    }
}
