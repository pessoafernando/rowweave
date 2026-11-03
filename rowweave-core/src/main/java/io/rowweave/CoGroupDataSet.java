package io.rowweave;

import io.rowweave.ops.join.CoGroup;

/**
 * Intermediate type for coGroup: {@code left.coGroup(right, fn)} returns a CoGroupDataSet,
 * and {@code .on(leftKeys, rightKeys)} lands it as a coGroup {@link DataSet}.
 *
 * <p>Graph-building sugar only: `.on(...)` delegates to {@link DataSet#coGroup(DataSet, CoGroup)} and lands on the existing describable
 * CoGroup - no new typeId, no executor change. Output meta = fn.getResultSchema().
 */
public final class CoGroupDataSet {

    private final DataSet left;
    private final DataSet right;
    private final CoGroupFunction fn;

    CoGroupDataSet(DataSet left, DataSet right, CoGroupFunction fn) {
        this.left = left;
        this.right = right;
        this.fn = fn;
    }

    /** on(leftKeys, rightKeys): coGroup by keys, landing on a CoGroup operator. */
    public DataSet on(String[] leftKeys, String[] rightKeys) {
        CoGroup cg = CoGroup.of(left.getSchema(), right.getSchema(), leftKeys, rightKeys, fn);
        return left.coGroup(right, cg);
    }

    /** on(key): single-key coGroup (the same key name on both sides). */
    public DataSet on(String key) {
        return on(new String[]{key}, new String[]{key});
    }
}
