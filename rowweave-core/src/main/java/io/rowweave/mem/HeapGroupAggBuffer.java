package io.rowweave.mem;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The on-heap GroupAggBuffer: a LinkedHashMap preserves first-seen order, and keys hash through a content
 * wrapper over byte[].
 * No spill — it serves only direct calls that bypass PlanGraph, plus tests, while the production path uses
 * {@link OpenAddressingGroupAggBuffer}, which does spill.
 *
 * <p>Safety valve: once the distinct key count passes {@code maxGroups} it fails loudly with an
 * {@link IllegalStateException} naming the spill implementations to inject, rather than letting the heap
 * drift into an OOM. Failing fast is preferred to running out of memory.
 */
public final class HeapGroupAggBuffer implements GroupAggBuffer {

    /** Default hard cap on distinct keys when no explicit limit is set (guards against runaway misuse; the production path does not use this implementation). */
    private static final int DEFAULT_MAX_GROUPS = 5_000_000;

    private final Supplier<Object[]> accumulatorFactory;
    private final int maxGroups;
    private final Map<KeyWrapper, Object[]> table = new LinkedHashMap<>();
    private boolean closed;

    public HeapGroupAggBuffer(Supplier<Object[]> accumulatorFactory) {
        this(accumulatorFactory, DEFAULT_MAX_GROUPS);
    }

    public HeapGroupAggBuffer(Supplier<Object[]> accumulatorFactory, int maxGroups) {
        this.accumulatorFactory = accumulatorFactory;
        this.maxGroups = maxGroups;
    }

    @Override
    public Object[] getOrCreate(byte[] keyBytes) {
        ensureOpen();
        KeyWrapper key = new KeyWrapper(keyBytes);
        Object[] accs = table.get(key);
        if (accs == null) {
            if (table.size() >= maxGroups) {
                throw new IllegalStateException(
                        "HeapGroupAggBuffer exceeded the distinct-key limit " + maxGroups + "; "
                                + " use PlanGraph, which injects spill-capable OpenAddressingGroupAggBuffer");
            }
            accs = accumulatorFactory.get();
            table.put(key, accs);
        }
        return accs;
    }

    @Override
    public long groupCount() {
        ensureOpen();
        return table.size();
    }

    @Override
    public Iterator<Group> iterator() {
        ensureOpen();
        Iterator<Map.Entry<KeyWrapper, Object[]>> it = table.entrySet().iterator();
        return new Iterator<Group>() {
            @Override
            public boolean hasNext() {
                ensureOpen();
                return it.hasNext();
            }

            @Override
            public Group next() {
                ensureOpen();
                Map.Entry<KeyWrapper, Object[]> e = it.next();
                return new HeapGroup(e.getKey().bytes, e.getValue());
            }
        };
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        table.clear();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("GroupAggBuffer is closed");
        }
    }

    private static final class HeapGroup implements Group {
        private final byte[] keyBytes;
        private final Object[] accumulators;

        HeapGroup(byte[] keyBytes, Object[] accumulators) {
            this.keyBytes = keyBytes;
            this.accumulators = accumulators;
        }

        @Override
        public byte[] keyBytes() {
            return keyBytes;
        }

        @Override
        public Object[] accumulators() {
            return accumulators;
        }
    }

    /** byte[] wrapper that hashes and compares by content (byte[] compares by identity, so it cannot be a map key directly). */
    private static final class KeyWrapper {
        private final byte[] bytes;
        private final int hash;

        KeyWrapper(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof KeyWrapper)) {
                return false;
            }
            return Arrays.equals(bytes, ((KeyWrapper) o).bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
