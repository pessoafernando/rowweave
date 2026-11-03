package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleFull09 {

    private static final int ID = 0;
    private static final int DEPT = 1;
    private static final int REGION = 2;
    private static final int GRADE = 3;
    private static final int AMOUNT = 4;
    private static final int QTY = 5;

    private static final int DIM_DEPT = 0;
    private static final int DIM_BONUS = 1;

    private static final int RDIM_REGION = 0;
    private static final int RDIM_RNAME = 1;

    private OracleFull09() { }

    /** Returns a list of five entries, the outputs o0 through o4, in that order. */
    public static List<List<Object[]>> compute(List<Object[]> ordersA,
                                               List<Object[]> ordersB,
                                               List<Object[]> supp,
                                               List<Object[]> dim,
                                               List<Object[]> regionDim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(ordersA, dim));
        out.add(o1(ordersA, supp));
        out.add(o2(ordersA, supp));
        out.add(o3(ordersA, ordersB, dim, regionDim));
        out.add(o4(ordersA, ordersB));
        return out;
    }

    // ---------------------------------------------------------------------
    // group by region sum(amount) -> [region,sum]
    //
    // ---------------------------------------------------------------------
    private static List<Object[]> o0(List<Object[]> a, List<Object[]> dim) {
        Map<Object, List<Object[]>> dIndex = indexByCol(dim, DIM_DEPT);

        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] ra : a) {
            Object dept = ra[DEPT];
            List<Object[]> matches = dept == null ? null : dIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                continue;  // an inner join drops a row with no match
            }
            int mult = matches.size();
            BigDecimal amt = (BigDecimal) ra[AMOUNT];
            Key k = new Key(ra[REGION]);
            BigDecimal acc = sums.get(k);
            if (amt == null) {
                if (acc == null) {
                    sums.put(k, BigDecimal.ZERO);
                }
            } else {
                BigDecimal add = amt.multiply(BigDecimal.valueOf(mult));
                sums.put(k, acc == null ? add : acc.add(add));
            }
        }

        List<Object[]> grouped = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : sums.entrySet()) {
            grouped.add(new Object[] { e.getKey().value, scale(e.getValue()) });
        }

        grouped.sort((x, y) -> compareNumNullsLast(x[0], y[0])); // region asc NULLS_LAST

        List<Object[]> res = new ArrayList<>();
        int limit = Math.min(10, grouped.size());
        for (int i = 0; i < limit; i++) {
            res.add(grouped.get(i));
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // -> select [id(idx0), dept(idx1), amount(idx4)]
    //
    // ---------------------------------------------------------------------
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> s) {
        List<Object[]> union = new ArrayList<>(a.size() + s.size());
        union.addAll(a);
        union.addAll(s);

        Map<Key2, Object[]> distinct = new LinkedHashMap<>();
        for (Object[] r : union) {
            Integer grade = (Integer) r[GRADE];
            int copies = (grade == null ? 0 : grade) + 1;
            if (copies <= 0) {
                continue;  // with no copies emitted, nothing reaches distinct
            }
            Key2 k = new Key2(r[ID], r[DEPT]);
            if (!distinct.containsKey(k)) {
                distinct.put(k, r);  // the first occurrence is the representative row
            }
        }

        List<Object[]> res = new ArrayList<>();
        for (Object[] r : distinct.values()) {
            BigDecimal amt = (BigDecimal) r[AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;  // filter amount>=0 always holds here
            }
            res.add(new Object[] { r[ID], r[DEPT], amt == null ? null : scale(amt) });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // -> reduceGroup sum(lc) by dept -> [dept, lcSum]
    // ---------------------------------------------------------------------
    private static List<Object[]> o2(List<Object[]> a, List<Object[]> s) {
        Map<Key, int[]> counts = coGroupCounts(a, s);
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, int[]> e : counts.entrySet()) {
            int lc = e.getValue()[0];
            if (lc < 0) {
                continue;  // filter lc>=0 always holds here
            }
            res.add(new Object[] { e.getKey().value, lc });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // o3: lj = select6(leftJoin(A,dim) on dept )
    // rj = select6(rightJoin(B,dim) on dept )
    // fj = select6(fullJoin(A,regionDim) on region )
    // group by dept(idx1): max(amount idx4), min(amount), count -> [dept,max,min,count]
    // ---------------------------------------------------------------------
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b,
                                     List<Object[]> dim, List<Object[]> regionDim) {
        List<Object[]> u = new ArrayList<>();  // 6 columns per row, the order-side columns

        // lj = leftJoin(A,dim) on dept, projected back to the 6 A-side columns.
        Map<Object, List<Object[]>> dIndex = indexByCol(dim, DIM_DEPT);
        for (Object[] ra : a) {
            Object dept = ra[DEPT];
            List<Object[]> matches = dept == null ? null : dIndex.get(dept);
            int mult = (matches == null || matches.isEmpty()) ? 1 : matches.size();
            for (int i = 0; i < mult; i++) {
                u.add(select6(ra));
            }
        }

        Map<Object, List<Object[]>> bIndex = indexByCol(b, DEPT);
        for (Object[] d : dim) {
            Object dept = d[DIM_DEPT];
            List<Object[]> matches = dept == null ? null : bIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(new Object[6]);  // the order side is entirely null
            } else {
                for (Object[] rb : matches) {
                    u.add(select6(rb));
                }
            }
        }

        Map<Object, List<Object[]>> rdIndex = indexByCol(regionDim, RDIM_REGION);
        for (Object[] ra : a) {
            Object region = ra[REGION];
            List<Object[]> matches = region == null ? null : rdIndex.get(region);
            int mult = (matches == null || matches.isEmpty()) ? 1 : matches.size();
            for (int i = 0; i < mult; i++) {
                u.add(select6(ra));
            }
        }
        // a region present in regionDim but absent from A yields a row that is null across the A side.
        Map<Object, List<Object[]>> aRegionIndex = indexByCol(a, REGION);
        for (Object[] rd : regionDim) {
            Object region = rd[RDIM_REGION];
            List<Object[]> matches = region == null ? null : aRegionIndex.get(region);
            if (matches == null || matches.isEmpty()) {
                u.add(new Object[6]);  // the A side is entirely null
            }
        }

        return groupMaxMinCountByDept(u);
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
    private static Object[] select6(Object[] r) {
        Object[] row = new Object[6];
        System.arraycopy(r, 0, row, 0, 6);
        return row;
    }

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

    private static final class Agg {
        BigDecimal max;
        BigDecimal min;
        int count;
    }
}

