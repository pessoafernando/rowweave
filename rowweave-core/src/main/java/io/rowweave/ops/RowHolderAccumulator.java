package io.rowweave.ops;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.Accumulator;
import io.rowweave.mem.RowSerializer;

/**
 * Holds the **canonical representative row** for each distinct key, the one with the smallest serialized byte
 * sequence, **not** the first seen.
 *
 * <p>Why determinism requires this: when distinct dedupes on a subset of key columns, the surviving row's
 * **non-key columns are arbitrary**. Taking the first seen would make the representative depend on arrival
 * order, and the interleaving of a parallel or cluster fan-in from many producers is not deterministic, so the
 * same key could keep a different representative row and the output multiset of non-key columns would diverge
 * between cluster and single machine. Choosing the lexicographically smallest serialized bytes makes the
 * representative **independent of arrival order** and bit-identical across engines. For a full-column distinct
 * there are no non-key columns, and the two rules coincide.
 *
 * <p>It implements {@link Accumulator} to support spill: writeState serializes the representative row and
 * mergeState keeps the canonical minimum of the two sides, consistent with the same ordering rule.
 */
public final class RowHolderAccumulator implements Accumulator {

    private final RowSerializer serializer;
    private byte[] held; // serialized representative row; null = empty

    public RowHolderAccumulator(DataSchema dataSchema) {
        this.serializer = new RowSerializer(dataSchema);
    }

    public void setIfEmpty(DataRow row) {
        adopt(serializer.serialize(row));
    }

    public DataRow get() {
        return held == null ? null : serializer.deserialize(held);
    }

    @Override
    public byte[] writeState() {
        return held == null ? new byte[0] : held;
    }

    @Override
    public void mergeState(byte[] state) {
        if (state != null && state.length > 0) {
            adopt(state);
        }
    }

    /** Admits a candidate: hold it when empty, otherwise keep the canonical minimum (unsigned byte order), which makes the result independent of arrival order. */
    private void adopt(byte[] candidate) {
        if (held == null || compareBytes(candidate, held) < 0) {
            held = candidate;
        }
    }

    /** The total order of unsigned byte lexicographic comparison (platform-independent and deterministic). */
    private static int compareBytes(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int d = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (d != 0) {
                return d;
            }
        }
        return a.length - b.length;
    }
}
