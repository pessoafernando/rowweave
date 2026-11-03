package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;

/**
 * Intermediate type for grouping: {@code dataSet.groupBy(fields...)} returns a Grouper,
 * which then chains the aggregation you want (sum/max/min/count/reduceGroup/combineGroup/combineReduceGroup), and each one produces a new {@link DataSet}.
 *
 * <p>Graph-building sugar only: it delegates to {@link DataSet#groupBy(ParallelGroupByAgg)} / reduceGroup / combineGroup and lands on the existing
 * describable operators - no new typeId, no executor change. The default output column name follows the naming convention (sum0/count0/max0/min0);
 * the <b>alias form</b> appends {@link DataSet#select(String...)} after groupBy (`aggCol as alias`) to rename the last column (key columns pass through).
 */
public final class Grouper {

    private final DataSet parent;
    private final String[] keyFields;
    private final DataSchema dataSchema;

    Grouper(DataSet parent, String[] keyFields) {
        if (keyFields == null || keyFields.length == 0) {
            throw new IllegalArgumentException("groupBy requires at least one field");
        }
        this.parent = parent;
        this.keyFields = keyFields.clone();
        this.dataSchema = parent.getSchema();
    }

    /** The grouping key field names. */
    public String[] getFields() {
        return keyFields.clone();
    }

    /** sum(field): group sum (BigDecimal, scale 10 HALF_UP, null for an empty group). */
    public DataSet sum(String field) {
        return parent.groupBy(ParallelGroupByAgg.of(dataSchema, keyFields).sum(field));
    }

    /** sum(field, alias): same as sum, with the output aggregate column named alias. */
    public DataSet sum(String field, String alias) {
        return aliasAgg(parent.groupBy(ParallelGroupByAgg.of(dataSchema, keyFields).sum(field)), alias);
    }

    /** count(): rows per group (count(*), null measures included), as Long. */
    public DataSet count() {
        return parent.groupBy(ParallelGroupByAgg.of(dataSchema, keyFields).count());
    }

    /** count(field): rows per group where that field is not null, as Long. */
    public DataSet count(String field) {
        return parent.groupBy(ParallelGroupByAgg.of(dataSchema, keyFields).countField(field));
    }

    /** count(field, alias): same as count(field), with the output column named alias. */
    public DataSet count(String field, String alias) {
        return aliasAgg(parent.groupBy(ParallelGroupByAgg.of(dataSchema, keyFields).countField(field)), alias);
    }

    /** max(field): group maximum (nulls ignored, null for an empty group), keeping the source field type. */
    public DataSet max(String field) {
        return parent.groupBy(ParallelGroupByAgg.of(dataSchema, keyFields).max(field));
    }

    /** max(field, alias). */
    public DataSet max(String field, String alias) {
        return aliasAgg(parent.groupBy(ParallelGroupByAgg.of(dataSchema, keyFields).max(field)), alias);
    }

    /** min(field): group minimum (nulls ignored, null for an empty group), keeping the source field type. */
    public DataSet min(String field) {
        return parent.groupBy(ParallelGroupByAgg.of(dataSchema, keyFields).min(field));
    }

    /** min(field, alias). */
    public DataSet min(String field, String alias) {
        return aliasAgg(parent.groupBy(ParallelGroupByAgg.of(dataSchema, keyFields).min(field)), alias);
    }

    /** reduceGroup(func): cluster rows by grouping key and hand each group to a user function that emits 0..N rows (the output meta comes from func). */
    public DataSet reduceGroup(GroupReduceFunction func) {
        DataSchema resultMeta = func.getResultSchema();
        if (resultMeta == null) {
            throw new IllegalArgumentException("GroupReduceFunction.getResultSchema() must not be null");
        }
        return parent.reduceGroup(io.rowweave.ops.ReduceGroup.of(dataSchema, keyFields, func), resultMeta);
    }

    /**
     * combineGroup(func): map-side pre-aggregation before grouping. The semantics are: partition by grouping key first, then combine each
     * partition; the implementation uses DataSet.combineGroup (contiguous slices), so with grouping keys the user function aggregates by key internally. Delegates to DataSet.combineGroup.
     */
    public DataSet combineGroup(GroupCombineFunction func) {
        return parent.combineGroup(func);
    }

    /** combineReduceGroup(func): combine pre-aggregation plus the final reduce. Delegates to DataSet.combineReduceGroup. */
    public DataSet combineReduceGroup(GroupCombineReduceFunction func) {
        return parent.combineReduceGroup(func);
    }

    /** groupBy output = [key..., aggCol]; select passes the key names through and renames the trailing column with `aggCol as alias`. */
    private DataSet aliasAgg(DataSet grouped, String alias) {
        DataSchema gm = grouped.getSchema();
        int aggIdx = gm.getFieldCount() - 1;
        String aggName = gm.getField(aggIdx).getName();
        String[] exprs = new String[gm.getFieldCount()];
        for (int i = 0; i < aggIdx; i++) {
            exprs[i] = gm.getField(i).getName();          // key columns pass through (bare field name)
        }
        exprs[aggIdx] = aggName + " as " + alias;         // rename the trailing aggregate column
        return grouped.select(exprs);
    }
}
