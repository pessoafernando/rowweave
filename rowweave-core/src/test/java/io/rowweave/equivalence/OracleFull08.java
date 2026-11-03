package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleFull08 {

    private static final int ID = 0;
    private static final int DEPT = 1;
    private static final int REGION = 2;
    private static final int GRADE = 3;
    private static final int AMOUNT = 4;
    private static final int QTY = 5;

    private static final int DIM_DEPT = 0;
    private static final int DIM_BONUS = 1;

    private OracleFull08() { }

    /** Returns a list of five entries, the outputs o0 through o4, in that order. */
    public static List<List<Object[]>> compute(List<Object[]> ordersA,
                                               List<Object[]> ordersB,
                                               List<Object[]> supp,
                                               List<Object[]> dim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(ordersA));
        out.add(o1(ordersA));
        out.add(o2(ordersA, supp));
        out.add(o3(ordersA, ordersB, dim));
        out.add(o4(ordersA));
        return out;
    }

    // ---------------------------------------------------------------------
    // group by dept sum(amount):sum = Σ_{a∈A} amount*(grade+1) by dept
    // ---------------------------------------------------------------------
    private static List<Object[]> o0(List<Object[]> a) {
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] ra : a) {
            BigDecimal amt = (BigDecimal) ra[AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;  // filter amount>=0 always holds here
            }
            Integer grade = (Integer) ra[GRADE];
            int copies = (grade == null ? 0 : grade) + 1;
            Key k = new Key(ra[DEPT]);
            BigDecimal acc = sums.get(k);
            if (amt == null) {
                if (acc == null) {
                    sums.put(k, BigDecimal.ZERO);
                }
            } else if (copies > 0) {
                BigDecimal add = amt.multiply(BigDecimal.valueOf(copies));
                sums.put(k, acc == null ? add : acc.add(add));
            } else if (acc == null) {
                sums.put(k, BigDecimal.ZERO);
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
            return compareNumNullsLast(x[0], y[0]); // dept asc NULLS_LAST
        });

        List<Object[]> res = new ArrayList<>();
        int limit = Math.min(12, grouped.size());
        for (int i = 0; i < limit; i++) {
            res.add(grouped.get(i));
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // ---------------------------------------------------------------------
    private static List<Object[]> o1(List<Object[]> a) {
        Map<Key2, Object[]> distinct = new LinkedHashMap<>();
        for (Object[] r : a) {
            Key2 k = new Key2(r[DEPT], r[REGION]);
            if (!distinct.containsKey(k)) {
                distinct.put(k, r);
            }
        }

        // group by region: max and min over amount (idx4, nulls skipped) and a count of every representative row.
        Map<Key, Agg> groups = new LinkedHashMap<>();
        for (Object[] row : distinct.values()) {
            Key k = new Key(row[REGION]);
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
    // select [id(idx0), dept(idx1), amount(idx4)]
    // ---------------------------------------------------------------------
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        List<Object[]> u = new ArrayList<>();  // 6 columns per row, the order-side columns

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

        // ij = inner join(A,B) on id, projected back to the 6 A-side columns, since the A side of a joined row is ra.
        Map<Object, List<Object[]>> bIdIndex = indexByCol(b, ID);
        for (Object[] ra : a) {
            Object id = ra[ID];
            if (id == null) {
                continue;
            }
            List<Object[]> matches = bIdIndex.get(id);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            for (int i = 0; i < matches.size(); i++) {
                u.add(select6(ra));
            }
        }

        // select [id, dept, amount] = idx0, idx1 and idx4.
        List<Object[]> res = new ArrayList<>();
        for (Object[] row : u) {
            BigDecimal amt = (BigDecimal) row[AMOUNT];
            res.add(new Object[] { row[ID], row[DEPT], amt == null ? null : scale(amt) });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // fullJoin on (dept,dept) -> [dept, sum0, dept, total]
    // ---------------------------------------------------------------------
    private static List<Object[]> o4(List<Object[]> a) {
        Map<Key, BigDecimal> cg = sumByDept(a);
        Map<Key, BigDecimal> cr = sumByDept(a);

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

