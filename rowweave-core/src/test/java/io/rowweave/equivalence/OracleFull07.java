package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleFull07 {

    private static final int ID = 0;
    private static final int DEPT = 1;
    private static final int REGION = 2;
    private static final int GRADE = 3;
    private static final int AMOUNT = 4;
    private static final int QTY = 5;

    private static final int DIM_DEPT = 0;
    private static final int DIM_BONUS = 1;

    private OracleFull07() { }

    /** Returns a list of five entries, the outputs o0 through o4, in that order. */
    public static List<List<Object[]>> compute(List<Object[]> ordersA,
                                               List<Object[]> ordersB,
                                               List<Object[]> supp,
                                               List<Object[]> dim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(ordersA));
        out.add(o1(ordersA, ordersB, dim));
        out.add(o2(ordersA, supp));
        out.add(o3(ordersA, ordersB));
        out.add(o4(ordersA, ordersB));
        return out;
    }

    // ---------------------------------------------------------------------
    // ---------------------------------------------------------------------
    private static List<Object[]> o0(List<Object[]> a) {
        // distinct over all 6 columns: one representative kept for each unique whole row.
        Map<KeyN, Object[]> distinct = new LinkedHashMap<>();
        for (Object[] r : a) {
            KeyN k = new KeyN(new Object[] { r[ID], r[DEPT], r[REGION], r[GRADE], r[AMOUNT], r[QTY] });
            if (!distinct.containsKey(k)) {
                distinct.put(k, r);
            }
        }

        // group by region sum(amount), computed on the deduplicated rows.
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] r : distinct.values()) {
            BigDecimal amt = (BigDecimal) r[AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;  // filter amount>=0 always holds here
            }
            Key k = new Key(r[REGION]);
            BigDecimal acc = sums.get(k);
            if (amt == null) {
                if (acc == null) {
                    sums.put(k, BigDecimal.ZERO);
                }
            } else {
                sums.put(k, acc == null ? amt : acc.add(amt));
            }
        }

        List<Object[]> grouped = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : sums.entrySet()) {
            grouped.add(new Object[] { e.getKey().value, scale(e.getValue()) });
        }

        grouped.sort((x, y) -> {
            int c = ((BigDecimal) y[1]).compareTo((BigDecimal) x[1]); // sum desc
            if (c != 0) {
                return c;
            }
            return compareNumNullsLast(x[0], y[0]); // region asc NULLS_LAST
        });

        List<Object[]> res = new ArrayList<>();
        int limit = Math.min(5, grouped.size());
        for (int i = 0; i < limit; i++) {
            res.add(grouped.get(i));
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // o1: u = union(union(leftJoin(A,dim) on dept, rightJoin(B,dim) on dept ), fullJoin(A,dim) on dept )
    // group by dept(idx1): max(amount idx4), min(amount), count -> [dept,max,min,count]
    // ---------------------------------------------------------------------
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        Map<Object, List<Object[]>> dIndex = indexByCol(dim, DIM_DEPT);

        List<Object[]> u = new ArrayList<>();  // 8 columns = [order6, dim2]

        // leftJoin(A, dim): every A row is kept.
        for (Object[] ra : a) {
            Object dept = ra[DEPT];
            List<Object[]> matches = dept == null ? null : dIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(join8(ra, null));
            } else {
                for (Object[] d : matches) {
                    u.add(join8(ra, d));
                }
            }
        }

        // rightJoin(B, dim): every dim row is kept and paired with all its B rows, and an unmatched dim row nulls the B side.
        Map<Object, List<Object[]>> bIndex = indexByCol(b, DEPT);
        for (Object[] d : dim) {
            Object dept = d[DIM_DEPT];
            List<Object[]> matches = dept == null ? null : bIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(join8(null, d));
            } else {
                for (Object[] rb : matches) {
                    u.add(join8(rb, d));
                }
            }
        }

        // fullJoin(A, dim): both sides are kept in full.
        for (Object[] ra : a) {
            Object dept = ra[DEPT];
            List<Object[]> matches = dept == null ? null : dIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(join8(ra, null));
            } else {
                for (Object[] d : matches) {
                    u.add(join8(ra, d));
                }
            }
        }
        Map<Object, List<Object[]>> aIndex = indexByCol(a, DEPT);
        for (Object[] d : dim) {
            Object dept = d[DIM_DEPT];
            List<Object[]> matches = dept == null ? null : aIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(join8(null, d));  // a dim row with no A match leaves the A side entirely null
            }
        }

        return groupMaxMinCountByDept(u);
    }

    // ---------------------------------------------------------------------
    // -> reduceGroup sum(lc) by dept -> [dept, lcSum]
    // ---------------------------------------------------------------------
    private static List<Object[]> o2(List<Object[]> a, List<Object[]> s) {
        Map<Key, int[]> counts = coGroupCounts(a, s);
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, int[]> e : counts.entrySet()) {
            int rc = e.getValue()[1];
            if (rc < 0) {
                continue;  // filter rc>=0 always holds here
            }
            res.add(new Object[] { e.getKey().value, e.getValue()[0] });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // ---------------------------------------------------------------------
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b) {
        Map<Object, List<Object[]>> bIdIndex = indexByCol(b, ID);

        Map<Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] ra : a) {
            Object idA = ra[ID];
            if (idA == null) {
                continue;
            }
            List<Object[]> matches = bIdIndex.get(idA);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            Integer gradeA = (Integer) ra[GRADE];
            int copies = (gradeA == null ? 0 : gradeA) + 1;
            if (copies <= 0) {
                continue;  // no copies are emitted
            }
            Key2 k = new Key2(ra[ID], ra[DEPT]);
            if (!seen.containsKey(k)) {
                // the representative row comes from the A side of the joined row, where idx0, idx1 and idx4 match ra
                seen.put(k, ra);
            }
        }

        List<Object[]> res = new ArrayList<>();
        for (Object[] ra : seen.values()) {
            BigDecimal amt = (BigDecimal) ra[AMOUNT];
            res.add(new Object[] { ra[ID], ra[DEPT], amt == null ? null : scale(amt) });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // o4: cg = group A by dept sum(amount); cr = group B by dept sum(amount);
    // fullJoin on (dept,dept) -> [dept, sum0, dept, total]
    // ---------------------------------------------------------------------
    private static List<Object[]> o4(List<Object[]> a, List<Object[]> b) {
        Map<Key, BigDecimal> cg = sumByDept(a);
        Map<Key, BigDecimal> cr = sumByDept(b);
        return fullJoinSums(cg, cr);
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------
    private static Map<Key, BigDecimal> sumByDept(List<Object[]> rows) {
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] r : rows) {
            BigDecimal amt = (BigDecimal) r[AMOUNT];
            Key k = new Key(r[DEPT]);
            BigDecimal acc = sums.get(k);
            if (amt == null) {
                if (acc == null) {
                    sums.put(k, BigDecimal.ZERO);
                }
            } else {
                sums.put(k, acc == null ? amt : acc.add(amt));
            }
        }
        return sums;
    }

    private static List<Object[]> fullJoinSums(Map<Key, BigDecimal> cg, Map<Key, BigDecimal> cr) {
        Map<Key, Object[]> merged = new LinkedHashMap<>();
        for (Map.Entry<Key, BigDecimal> e : cg.entrySet()) {
            merged.put(e.getKey(), new Object[] { e.getKey().value, scale(e.getValue()), null, null });
        }
        for (Map.Entry<Key, BigDecimal> e : cr.entrySet()) {
            Object[] row = merged.get(e.getKey());
            if (row == null) {
                merged.put(e.getKey(), new Object[] { null, null, e.getKey().value, scale(e.getValue()) });
            } else {
                row[2] = e.getKey().value;
                row[3] = scale(e.getValue());
            }
        }
        return new ArrayList<>(merged.values());
    }

    private static Map<Key, int[]> coGroupCounts(List<Object[]> a, List<Object[]> s) {
        Map<Key, int[]> counts = new LinkedHashMap<>();
        for (Object[] r : a) {
            Key k = new Key(r[DEPT]);
            int[] c = counts.get(k);
            if (c == null) {
                c = new int[2];
                counts.put(k, c);
            }
            c[0]++;
        }
        for (Object[] r : s) {
            Key k = new Key(r[DEPT]);
            int[] c = counts.get(k);
            if (c == null) {
                c = new int[2];
                counts.put(k, c);
            }
            c[1]++;
        }
        return counts;
    }

    private static Map<Object, List<Object[]>> indexByCol(List<Object[]> rows, int col) {
        Map<Object, List<Object[]>> idx = new LinkedHashMap<>();
        for (Object[] r : rows) {
            Object v = r[col];
            if (v == null) {
                continue;
            }
            idx.computeIfAbsent(v, kk -> new ArrayList<>()).add(r);
        }
        return idx;
    }

    private static List<Object[]> groupMaxMinCountByDept(Iterable<Object[]> rows) {
        Map<Key, Agg> groups = new LinkedHashMap<>();
        for (Object[] row : rows) {
            Key k = new Key(row[DEPT]);
            Agg agg = groups.get(k);
            if (agg == null) {
                agg = new Agg();
                groups.put(k, agg);
            }
            agg.count++;
            BigDecimal amt = (BigDecimal) row[AMOUNT];
            if (amt != null) {
                if (agg.max == null || amt.compareTo(agg.max) > 0) {
                    agg.max = amt;
                }
                if (agg.min == null || amt.compareTo(agg.min) < 0) {
                    agg.min = amt;
                }
            }
        }
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, Agg> e : groups.entrySet()) {
            Agg agg = e.getValue();
            BigDecimal max = agg.max == null ? null : scale(agg.max);
            BigDecimal min = agg.min == null ? null : scale(agg.min);
            res.add(new Object[] { e.getKey().value, max, min, (long) agg.count });
        }
        return res;
    }

    private static Object[] join8(Object[] side6, Object[] d) {
        Object[] row = new Object[8];
        if (side6 != null) {
            System.arraycopy(side6, 0, row, 0, 6);
        }
        if (d != null) {
            row[6] = d[DIM_DEPT];
            row[7] = d[DIM_BONUS];
        }
        return row;
    }

    private static BigDecimal scale(BigDecimal v) {
        return v.setScale(10, RoundingMode.HALF_UP);
    }

    /** Ascending numeric order, NULLS_LAST. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareNumNullsLast(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;
        }
        if (b == null) {
            return -1;
        }
        return ((Comparable) a).compareTo(b);
    }

    /** A single-value grouping key; null forms its own group. */
    private static final class Key {
        final Object value;

        Key(Object value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key)) {
                return false;
            }
            Object v = ((Key) o).value;
            return value == null ? v == null : value.equals(v);
        }

        @Override
        public int hashCode() {
            return value == null ? 0 : value.hashCode();
        }
    }

    /** A two-value grouping key; null forms its own group. */
    private static final class Key2 {
        final Object a;
        final Object b;

        Key2(Object a, Object b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key2)) {
                return false;
            }
            Key2 k = (Key2) o;
            return eq(a, k.a) && eq(b, k.b);
        }

        private static boolean eq(Object x, Object y) {
            return x == null ? y == null : x.equals(y);
        }

        @Override
        public int hashCode() {
            int h = a == null ? 0 : a.hashCode();
            h = 31 * h + (b == null ? 0 : b.hashCode());
            return h;
        }
    }

    /** An n-value grouping key for whole-row distinct; null forms its own group. */
    private static final class KeyN {
        final Object[] vals;

        KeyN(Object[] vals) {
            this.vals = vals;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof KeyN)) {
                return false;
            }
            Object[] other = ((KeyN) o).vals;
            if (vals.length != other.length) {
                return false;
            }
            for (int i = 0; i < vals.length; i++) {
                Object x = vals[i];
                Object y = other[i];
                if (x == null ? y != null : !x.equals(y)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int h = 1;
            for (Object v : vals) {
                h = 31 * h + (v == null ? 0 : v.hashCode());
            }
            return h;
        }
    }

    private static final class Agg {
        BigDecimal max;
        BigDecimal min;
        int count;
    }
}

