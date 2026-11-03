package io.rowweave.ops.join;

/**
  * The join types, implemented for join by {@link SortMergeJoin} and reused by CoGroup. A top-level enum, never nested inside an operator.
 */
public enum JoinType {
    INNER, LEFT, RIGHT, FULL
}
