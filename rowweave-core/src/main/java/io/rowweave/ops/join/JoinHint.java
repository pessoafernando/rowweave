package io.rowweave.ops.join;

/**
  * Join algorithm hints. A hint changes only the algorithm chosen, never the meaning of the result.
 *
 * <ul>
  * <li>{@link #SORT_MERGE}: choose SortMergeJoin explicitly, supported both locally and on a cluster, and the safe choice for production.</li>
  * <li>{@link #HASH}: choose the managed HashJoin explicitly, locally and on a cluster alike; once over budget it switches deterministically to spill-safe SMJ.</li>
 * </ul>
  * Without a hint, both the local and the cluster runtime use AUTO.
 */
public enum JoinHint {
    SORT_MERGE,
    HASH
}
