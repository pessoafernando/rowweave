package io.rowweave.ops.join;

/** Closed physical strategy carried from PlanGraph to every join consumer. */
public enum JoinPhysicalStrategy {
    HASH,
    SORT_MERGE,
    AUTO;

    public static JoinPhysicalStrategy fromPlanHint(String hint) {
        if (hint == null) return AUTO;
        if ("AUTO".equals(hint)) return AUTO;
        if ("HASH".equals(hint)) return HASH;
        if ("SORT_MERGE".equals(hint)) return SORT_MERGE;
        throw new IllegalArgumentException("unknown joinHint: " + hint);
    }
}
