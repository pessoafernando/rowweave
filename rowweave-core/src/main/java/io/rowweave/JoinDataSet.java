package io.rowweave;

import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;

/**
 * Intermediate type for join: {@code left.join(right)}/{@code leftJoin}/... returns a
 * JoinDataSet. {@code .on(keys)} lands the join {@link DataSet} ([left..., right...]), after which the DataSet's own
 * {@link DataSet#select(String...)} picks columns. {@code .withFunc(keys, fn)} = join by keys, then hand every row pair to a {@link JoinFunction} that produces a custom output.
 *
 * <p>Graph-building sugar only: `.on` delegates to {@link DataSet#join(DataSet, SortMergeJoin)} (the existing describable SMJ);
 * `.withFunc` appends one map onto the join result that splits the row back into its left and right halves and calls fn (a describable code-lambda).
 */
public final class JoinDataSet {

    private final DataSet left;
    private final DataSet right;
    private final JoinType joinType;

    JoinDataSet(DataSet left, DataSet right, JoinType joinType) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
    }

    /** on(leftKeys, rightKeys): join by keys, landing on SortMergeJoin, and return the join result DataSet([left..., right...]). */
    public DataSet on(String[] leftKeys, String[] rightKeys) {
        SortMergeJoin smj = SortMergeJoin.of(
                left.getSchema(), right.getSchema(), leftKeys, rightKeys, joinType);
        return left.join(right, smj);
    }

    /** on(key): single-key equality join (the same key name on both sides). */
    public DataSet on(String key) {
        return on(new String[]{key}, new String[]{key});
    }

    /** The join type. */
    public JoinType getJoinType() {
        return joinType;
    }

    /**
     * withFunc(leftKeys, rightKeys, fn): after joining by keys, hand each (leftRow, rightRow) pair to the user's {@link JoinFunction}
     * to produce a custom output row. The implementation = on(keys) to get [left..., right...], then a map that splits back into the left and right halves (an all-null half is passed as null, matching outer join semantics) and calls fn.
     * Output meta = fn.getResultSchema().
     */
    public DataSet withFunc(String[] leftKeys, String[] rightKeys, JoinFunction fn) {
        DataSchema outMeta = fn.getResultSchema();
        if (outMeta == null) {
            throw new IllegalArgumentException("JoinFunction.getResultSchema() must not be null");
        }
        final int leftWidth = left.getSchema().getFieldCount();
        final int rightWidth = right.getSchema().getFieldCount();
        DataSet joined = on(leftKeys, rightKeys); // [left..., right...]
        MapFunction split = concatRow -> {
            DataRow l = sliceOrNull(concatRow, 0, leftWidth);
            DataRow r = sliceOrNull(concatRow, leftWidth, rightWidth);
            return fn.join(l, r);
        };
        return joined.mapWithMeta(split, outMeta);
    }

    /** withFunc(key, fn): single-key equality join plus a custom output. */
    public DataSet withFunc(String key, JoinFunction fn) {
        return withFunc(new String[]{key}, new String[]{key}, fn);
    }

    /** Take the [from, from+width) half of a concat row; when that half is all null (the unmatched side of an outer join) return null, which matches the unmatched-side semantics of {@link JoinFunction}. */
    private static DataRow sliceOrNull(DataRow concat, int from, int width) {
        boolean allNull = true;
        DataRow half = new DataRow(width);
        for (int i = 0; i < width; i++) {
            Object v = concat.get(from + i);
            half.set(i, v);
            if (v != null) {
                allNull = false;
            }
        }
        return allNull ? null : half;
    }
}
