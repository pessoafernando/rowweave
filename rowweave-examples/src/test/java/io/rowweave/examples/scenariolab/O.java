package io.rowweave.examples.scenariolab;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.SortField;
import io.rowweave.ops.join.JoinType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Oracle helpers: every expected value is computed by an **independent Java implementation** that shares
 * nothing with the engine, so an equivalence check means something. The semantics follow the value spec:
 * null forms its own group when grouping; null never matches null in a join; sum accumulates at full
 * precision with setScale(10, HALF_UP) applied once before output; in ordering a null number defaults to 0,
 * a null string sorts smallest, and NULLS_FIRST/LAST place nulls absolutely.
 */
public final class O {

    private O() {
    }

    // ---- groupBy oracle ----------------------------------------------------

    /** Grouped sum: output [keys..., BigDecimal(sum)]. */
    public static List<DataRow> groupSum(List<DataRow> in, int[] keyIdx, int valIdx) {
        Map<List<Object>, BigDecimal> acc = new LinkedHashMap<>();
        for (DataRow r : in) {
            List<Object> key = keyOf(r, keyIdx);
            BigDecimal v = r.getBigDecimal(valIdx);
            if (v != null) {
                acc.merge(key, v, BigDecimal::add);
            } else {
                acc.putIfAbsent(key, null);
                acc.computeIfPresent(key, (k, cur) -> cur);
            }
        }
        List<DataRow> out = new ArrayList<>();
        for (Map.Entry<List<Object>, BigDecimal> e : acc.entrySet()) {
            BigDecimal sum = e.getValue() == null
                    ? null : e.getValue().setScale(10, RoundingMode.HALF_UP);
            out.add(joinKeyAndVals(e.getKey(), sum));
        }
        return out;
    }

    /** Grouped count(*): output [keys..., Long]. */
    public static List<DataRow> groupCount(List<DataRow> in, int[] keyIdx) {
        Map<List<Object>, Long> acc = new LinkedHashMap<>();
        for (DataRow r : in) {
            acc.merge(keyOf(r, keyIdx), 1L, Long::sum);
        }
        List<DataRow> out = new ArrayList<>();
        for (Map.Entry<List<Object>, Long> e : acc.entrySet()) {
            out.add(joinKeyAndVals(e.getKey(), e.getValue()));
        }
        return out;
    }

    /** Grouped count(field) over non-null values: output [keys..., Long]. */
    public static List<DataRow> groupCountField(List<DataRow> in, int[] keyIdx, int valIdx) {
        Map<List<Object>, Long> acc = new LinkedHashMap<>();
        for (DataRow r : in) {
            long add = r.get(valIdx) == null ? 0L : 1L;
            acc.merge(keyOf(r, keyIdx), add, Long::sum);
        }
        List<DataRow> out = new ArrayList<>();
        for (Map.Entry<List<Object>, Long> e : acc.entrySet()) {
            out.add(joinKeyAndVals(e.getKey(), e.getValue()));
        }
        return out;
    }

    /** Grouped max and min: nulls are ignored, a group that is empty or all null yields null, and the type follows the source column. */
    public static List<DataRow> groupMinMax(List<DataRow> in, int[] keyIdx, int valIdx, boolean max) {
        Map<List<Object>, Object> acc = new LinkedHashMap<>();
        for (DataRow r : in) {
            List<Object> key = keyOf(r, keyIdx);
            Object v = r.get(valIdx);
            if (v == null) {
                acc.putIfAbsent(key, null);
                continue;
            }
            Object cur = acc.get(key);
            if (cur == null || (max ? compareValues(v, cur, false) > 0
                    : compareValues(v, cur, false) < 0)) {
                acc.put(key, v);
            }
        }
        List<DataRow> out = new ArrayList<>();
        for (Map.Entry<List<Object>, Object> e : acc.entrySet()) {
            out.add(joinKeyAndVals(e.getKey(), e.getValue()));
        }
        return out;
    }

    /** Several aggregations per group (keys, then one value column per aggregation in order). Each agg is [kind, valIdx], kind one of SUM/COUNT_ALL/COUNT_FIELD/MAX/MIN. */
    public static List<DataRow> groupMultiAgg(List<DataRow> in, int[] keyIdx, List<Agg> aggs) {
        Map<List<Object>, List<DataRow>> groups = new LinkedHashMap<>();
        for (DataRow r : in) {
            groups.computeIfAbsent(keyOf(r, keyIdx), k -> new ArrayList<>()).add(r);
        }
        List<DataRow> out = new ArrayList<>();
        for (Map.Entry<List<Object>, List<DataRow>> e : groups.entrySet()) {
            Object[] vals = new Object[keyIdx.length + aggs.size()];
            List<Object> key = e.getKey();
            for (int i = 0; i < key.size(); i++) {
                vals[i] = key.get(i);
            }
            for (int a = 0; a < aggs.size(); a++) {
                vals[keyIdx.length + a] = aggs.get(a).apply(e.getValue());
            }
            DataRow r = new DataRow(vals.length);
            for (int i = 0; i < vals.length; i++) {
                r.set(i, vals[i]);
            }
            out.add(r);
        }
        return out;
    }

    /** A single aggregation specification. */
    public static final class Agg {
        final String kind;
        final int valIdx;

        private Agg(String kind, int valIdx) {
            this.kind = kind;
            this.valIdx = valIdx;
        }

        public static Agg sum(int valIdx) {
            return new Agg("SUM", valIdx);
        }

        public static Agg countAll() {
            return new Agg("COUNT_ALL", -1);
        }

        public static Agg countField(int valIdx) {
            return new Agg("COUNT_FIELD", valIdx);
        }

        public static Agg max(int valIdx) {
            return new Agg("MAX", valIdx);
        }

        public static Agg min(int valIdx) {
            return new Agg("MIN", valIdx);
        }

        Object apply(List<DataRow> rows) {
            switch (kind) {
                case "SUM":
                    return bdSum(rows, valIdx);
                case "COUNT_ALL":
                    return (long) rows.size();
                case "COUNT_FIELD": {
                    long c = 0;
                    for (DataRow r : rows) {
                        if (r.get(valIdx) != null) {
                            c++;
                        }
                    }
                    return c;
                }
                case "MAX": {
                    Object best = null;
                    for (DataRow r : rows) {
                        Object v = r.get(valIdx);
                        if (v != null && (best == null || compareValues(v, best, false) > 0)) {
                            best = v;
                        }
                    }
                    return best;
                }
                case "MIN": {
                    Object best = null;
                    for (DataRow r : rows) {
                        Object v = r.get(valIdx);
                        if (v != null && (best == null || compareValues(v, best, false) < 0)) {
                            best = v;
                        }
                    }
                    return best;
                }
                default:
                    throw new IllegalStateException("unknown agg " + kind);
            }
        }
    }

    private static BigDecimal bdSum(List<DataRow> rows, int idx) {
        BigDecimal total = null;
        for (DataRow r : rows) {
            BigDecimal v = r.getBigDecimal(idx);
            if (v != null) {
                total = total == null ? BigDecimal.ZERO : total;
                total = total.add(v);
            }
        }
        return total == null ? null : total.setScale(10, RoundingMode.HALF_UP);
    }

    private static List<Object> keyOf(DataRow r, int[] keyIdx) {
        List<Object> key = new ArrayList<>(keyIdx.length);
        for (int i : keyIdx) {
            key.add(r.get(i));
        }
        return key;
    }

    private static DataRow joinKeyAndVals(List<Object> key, Object... vals) {
        Object[] all = new Object[key.size() + vals.length];
        for (int i = 0; i < key.size(); i++) {
            all[i] = key.get(i);
        }
        System.arraycopy(vals, 0, all, key.size(), vals.length);
        DataRow r = new DataRow(all.length);
        for (int i = 0; i < all.length; i++) {
            r.set(i, all[i]);
        }
        return r;
    }

    /** null-safe value comparison: numbers as BigDecimal, strings in natural order, Boolean with false before true. */
    private static int compareValues(Object a, Object b, boolean desc) {
        int cmp;
        if (a instanceof String || b instanceof String) {
            cmp = ((String) a).compareTo((String) b);
        } else if (a instanceof Boolean || b instanceof Boolean) {
            cmp = Boolean.compare((Boolean) a, (Boolean) b);
        } else {
            cmp = toBd(a).compareTo(toBd(b));
        }
        return desc ? -cmp : cmp;
    }

    private static BigDecimal toBd(Object v) {
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        if (v instanceof Number) {
            return new BigDecimal(v.toString());
        }
        throw new IllegalArgumentException("not numeric: " + v);
    }

    // ---- orderBy oracle ----------------------------------------------------

    /**
     * Ordering oracle: a stable sort that reproduces the null semantics exactly.
     * DEFAULT treats a null number as zero, so it moves with the direction, and a null of any
     * other type as the smallest value, first under ASC and last under DESC.
     * NULLS_FIRST and NULLS_LAST place nulls absolutely, regardless of direction.
     */
    public static List<DataRow> sortRows(List<DataRow> in, DataSchema meta, SortField... sortFields) {
        List<DataRow> rows = new ArrayList<>(in);
        Comparator<DataRow> cmp = (r1, r2) -> {
            for (SortField sf : sortFields) {
                int c = compareCell(r1.get(sf.getFieldIndex()), r2.get(sf.getFieldIndex()),
                        meta.getField(sf.getFieldIndex()).getDataType(),
                        sf.getDirection(), sf.getNullOrder());
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        };
        rows.sort(cmp);
        return rows;
    }

    private static int compareCell(Object v1, Object v2, DataType t,
                                   SortField.Direction dir, SortField.NullOrder no) {
        if (no == SortField.NullOrder.NULLS_FIRST || no == SortField.NullOrder.NULLS_LAST) {
            boolean n1 = v1 == null;
            boolean n2 = v2 == null;
            if (n1 || n2) {
                if (n1 && n2) {
                    return 0;
                }
                return no == SortField.NullOrder.NULLS_FIRST ? (n1 ? -1 : 1) : (n1 ? 1 : -1);
            }
        }
        boolean numeric = t == DataType.BigDecimalType || t == DataType.LongType
                || t == DataType.IntegerType;
        Object a1 = v1;
        Object a2 = v2;
        if (numeric) {
            if (no == SortField.NullOrder.DEFAULT) {
                a1 = v1 == null ? BigDecimal.ZERO : toBd(v1);
                a2 = v2 == null ? BigDecimal.ZERO : toBd(v2);
            }
            int c = toBd(a1).compareTo(toBd(a2));
            return dir == SortField.Direction.ASC ? c : -c;
        }
        if (no == SortField.NullOrder.DEFAULT) {
            // a null of a non-numeric type is the smallest value, first under ASC and last under DESC, and the direction does not flip it
            if (v1 == null || v2 == null) {
                if (v1 == null && v2 == null) {
                    return 0;
                }
                int c = v1 == null ? -1 : 1;
                return dir == SortField.Direction.ASC ? c : -c;
            }
        }
        int c;
        if (t == DataType.BooleanType) {
            c = Boolean.compare((Boolean) v1, (Boolean) v2);
        } else {
            c = ((String) v1).compareTo((String) v2);
        }
        return dir == SortField.Direction.ASC ? c : -c;
    }

    // ---- distinct / union oracle -------------------------------------------

    /** Dedup by a sub-key, keeping the first full row seen for each key; compared as a multiset, so output order is irrelevant. */
    public static List<DataRow> distinctBy(List<DataRow> in, int[] keyIdx) {
        LinkedHashMap<List<Object>, DataRow> seen = new LinkedHashMap<>();
        for (DataRow r : in) {
            seen.putIfAbsent(keyOf(r, keyIdx), r);
        }
        return new ArrayList<>(seen.values());
    }

    public static List<DataRow> distinctAll(List<DataRow> in) {
        LinkedHashMap<RowKey, DataRow> seen = new LinkedHashMap<>();
        for (DataRow r : in) {
            seen.putIfAbsent(RowKey.of(r), r);
        }
        return new ArrayList<>(seen.values());
    }

    // ---- join oracle --------------------------------------------------------

    /**
     * Equi-join oracle: the output row is the left columns followed by the right columns.
     * A null key never matches, equal keys produce the cartesian product, and in an outer join
     * the unmatched half is all null.
     */
    public static List<DataRow> joinRows(List<DataRow> left, List<DataRow> right,
                                      int[] lk, int[] rk, JoinType type,
                                      int leftWidth, int rightWidth) {
        List<DataRow> out = new ArrayList<>();
        boolean[] rightMatched = new boolean[right.size()];
        for (DataRow l : left) {
            List<Object> lkey = keyOf(l, lk);
            boolean matched = false;
            for (int j = 0; j < right.size(); j++) {
                DataRow r = right.get(j);
                List<Object> rkey = keyOf(r, rk);
                if (rkey.contains(null)) {
                    continue;  // a null key never matches
                }
                if (keyEquals(lkey, rkey)) {
                    matched = true;
                    rightMatched[j] = true;
                    out.add(concatHalves(l, r, leftWidth, rightWidth));
                }
            }
            if (!matched && (type == JoinType.LEFT || type == JoinType.FULL)) {
                out.add(concatHalves(l, null, leftWidth, rightWidth));
            }
        }
        if (type == JoinType.RIGHT || type == JoinType.FULL) {
            for (int j = 0; j < right.size(); j++) {
                if (!rightMatched[j]) {
                    out.add(concatHalves(null, right.get(j), leftWidth, rightWidth));
                }
            }
        }
        return out;
    }

    private static boolean keyEquals(List<Object> a, List<Object> b) {
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i) == null || b.get(i) == null) {
                return false;
            }
            if (!valueEquals(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean valueEquals(Object x, Object y) {
        if (x instanceof BigDecimal || y instanceof BigDecimal) {
            return toBd(x).compareTo(toBd(y)) == 0;
        }
        return Objects.equals(x, y);
    }

    public static DataRow concatHalves(DataRow l, DataRow r, int leftWidth, int rightWidth) {
        DataRow out = new DataRow(leftWidth + rightWidth);
        for (int i = 0; i < leftWidth; i++) {
            out.set(i, l == null ? null : l.get(i));
        }
        for (int i = 0; i < rightWidth; i++) {
            out.set(leftWidth + i, r == null ? null : r.get(i));
        }
        return out;
    }

    /** Take a sub-row: the projection oracle. */
    public static DataRow project(DataRow r, int[] idx) {
        DataRow out = new DataRow(idx.length);
        for (int i = 0; i < idx.length; i++) {
            out.set(i, r.get(idx[i]));
        }
        return out;
    }

    /** Convenience: build the output schema from the key names plus the aggregation fields. */
    public static DataSchema keysPlusAggMeta(String[] keyNames, Field agg) {
        Field[] all = new Field[keyNames.length + 1];
        for (int i = 0; i < keyNames.length; i++) {
            all[i] = new Field(keyNames[i], DataType.StringType);
        }
        all[keyNames.length] = agg;
        return new DataSchema(all);
    }

    /** Defensive copy of an array. */
    public static int[] idx(int... xs) {
        return xs;
    }

    /** Entry point for a builder with several aggregations, mirroring the engine ParallelGroupByAgg. */
    public static io.rowweave.ops.ParallelGroupByAgg pga(DataSchema meta, String... keys) {
        return io.rowweave.ops.ParallelGroupByAgg.of(meta, keys);
    }

    /** Convenience constructor for a sort field. */
    public static SortField sf(DataSchema meta, String field, boolean asc, String nullOrder) {
        return new SortField(meta.getFieldIndex(field),
                asc ? SortField.Direction.ASC : SortField.Direction.DESC,
                SortField.NullOrder.valueOf(nullOrder));
    }

    /** coGroup SumLeft oracle: group the union of left and right by key (null keys fall in one group) and emit [k, sum of the left v] per group. */
    public static List<DataRow> groupSumLeft(List<DataRow> left, List<DataRow> right) {
        java.util.LinkedHashSet<Object> keys = new java.util.LinkedHashSet<>();
        for (DataRow r : left) {
            keys.add(r.get(0));
        }
        for (DataRow r : right) {
            keys.add(r.get(0));
        }
        keys.remove(null);
        boolean hasNull = left.stream().anyMatch(r -> r.get(0) == null)
                || right.stream().anyMatch(r -> r.get(0) == null);
        List<Object> all = new ArrayList<>(keys);
        if (hasNull) {
            all.add(null);
        }
        List<DataRow> out = new ArrayList<>();
        for (Object k : all) {
            BigDecimal sum = BigDecimal.ZERO;
            boolean any = false;
            for (DataRow r : left) {
                if (r.get(0) == null ? k == null : r.get(0).equals(k)) {
                    if (r.get(1) != null) {
                        sum = sum.add(r.getBigDecimal(1));
                        any = true;
                    }
                }
            }
            DataRow o = new DataRow(2);
            o.set(0, k);
            o.set(1, any ? sum.setScale(10, RoundingMode.HALF_UP) : null);
            out.add(o);
        }
        return out;
    }

    /** Convenience constructor for the engine Top operator. */
    public static io.rowweave.ops.Top top(io.rowweave.DataSchema meta, int n, SortField... fields) {
        return io.rowweave.ops.Top.of(meta, n, fields);
    }

    /** Convenience constructor for the engine OrderBy operator. */
    public static io.rowweave.ops.OrderBy orderBy(io.rowweave.DataSchema meta, SortField... fields) {
        return io.rowweave.ops.OrderBy.of(meta, fields);
    }

    @SafeVarargs
    public static <T> List<T> listOf(T... xs) {
        return new ArrayList<>(Arrays.asList(xs));
    }
}
