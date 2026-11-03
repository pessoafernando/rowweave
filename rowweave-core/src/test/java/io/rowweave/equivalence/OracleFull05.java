package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleFull05 {

    private static final int ID = 0;
    private static final int DEPT = 1;
    private static final int REGION = 2;
    private static final int GRADE = 3;
    private static final int AMOUNT = 4;
    private static final int QTY = 5;

    private static final int DIM_DEPT = 0;
    private static final int DIM_BONUS = 1;

    private OracleFull05() { }

    /** Returns a list of five entries, the outputs o0 through o4, in that order. */
    public static List<List<Object[]>> compute(List<Object[]> ordersA,
                                               List<Object[]> ordersB,
                                               List<Object[]> supp,
                                               List<Object[]> dim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(ordersA));
        out.add(o1(ordersA, ordersB));
        out.add(o2(ordersA, supp));
        out.add(o3(ordersA, ordersB, dim));
        out.add(o4(ordersA, ordersB));
        return out;
    }

    // ---------------------------------------------------------------------
    // o0: cr1 = group A by dept sum(amount) -> [dept, total]
    // ---------------------------------------------------------------------
    private static List<Object[]> o0(List<Object[]> a) {
        Map<Key, BigDecimal> sums = sumByDept(a);

        List<Object[]> grouped = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : sums.entrySet()) {
            grouped.add(new Object[] { e.getKey().value, scale(e.getValue()) });
        }

        // top 9: total descending, dept ascending with NULLS_LAST.
        grouped.sort((x, y) -> {
            int c = ((BigDecimal) y[1]).compareTo((BigDecimal) x[1]); // total desc
            if (c != 0) {
                return c;
            }
            return compareNumNullsLast(x[0], y[0]); // dept asc NULLS_LAST
        });

        List<Object[]> res = new ArrayList<>();
        int limit = Math.min(9, grouped.size());
        for (int i = 0; i < limit; i++) {
            res.add(grouped.get(i));
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // o1: cg = group A by dept sum(amount) -> [dept, sum0]
    // cr = group B by dept sum(amount) -> [dept, total]
    // fullJoin on (dept,dept) -> [dept, sum0, dept, total]
    // ---------------------------------------------------------------------
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> b) {
        Map<Key, BigDecimal> cg = sumByDept(a);
        Map<Key, BigDecimal> cr = sumByDept(b);

        Map<Key, Object[]> merged = new LinkedHashMap<>();
        for (Map.Entry<Key, BigDecimal> e : cg.entrySet()) {
            Integer dept = (Integer) e.getKey().value;
            merged.put(e.getKey(), new Object[] { dept, scale(e.getValue()), null, null });
        }
        for (Map.Entry<Key, BigDecimal> e : cr.entrySet()) {
            Integer dept = (Integer) e.getKey().value;
            Object[] row = merged.get(e.getKey());
            if (row == null) {
                merged.put(e.getKey(), new Object[] { null, null, dept, scale(e.getValue()) });
            } else {
                row[2] = dept;
                row[3] = scale(e.getValue());
            }
        }
        return new ArrayList<>(merged.values());
    }

    // ---------------------------------------------------------------------
    // -> reduceGroup sum(lc) by dept -> [dept, lcSum]
    // ---------------------------------------------------------------------
    private static List<Object[]> o2(List<Object[]> a, List<Object[]> s) {
        Map<Key, int[]> counts = new LinkedHashMap<>(); // dept -> {lc, rc}
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
    // o3: u = union(leftJoin(A,dim) on dept, rightJoin(B,dim) on dept )
    // distinct(id, dept)(u):id=idx0,dept=idx1
    // group by dept(idx1): max(amount idx4), min(amount), count -> [dept,max,min,count]
    // ---------------------------------------------------------------------
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        Map<Object, List<Object[]>> dIndex = new LinkedHashMap<>();
        for (Object[] d : dim) {
            Object dept = d[DIM_DEPT];
            if (dept == null) {
                continue;
            }
            dIndex.computeIfAbsent(dept, kk -> new ArrayList<>()).add(d);
        }

        List<Object[]> u = new ArrayList<>();  // 8 columns = [order6, dim2]

        // leftJoin(A, dim)
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

        // rightJoin(B, dim): every dim row is kept against its matching B rows, and an unmatched dim row nulls the B side.
        Map<Object, List<Object[]>> bIndex = new LinkedHashMap<>();
        for (Object[] rb : b) {
            Object dept = rb[DEPT];
            if (dept == null) {
                continue;
            }
            bIndex.computeIfAbsent(dept, kk -> new ArrayList<>()).add(rb);
        }
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

        // distinct(id, dept) on idx0 and idx1: the representative row is kept.
        Map<Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] uRow : u) {
            Key2 k = new Key2(uRow[ID], uRow[DEPT]);
            if (!seen.containsKey(k)) {
                seen.put(k, uRow);
            }
        }

        // group by dept (idx1): max and min over amount (idx4, nulls skipped), plus a count of every row.
        Map<Key, Agg> groups = new LinkedHashMap<>();
        for (Object[] row : seen.values()) {
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

    // ---------------------------------------------------------------------
    // group by dept sum(amount) -> [dept, sum0]
    // ---------------------------------------------------------------------
    private static List<Object[]> o4(List<Object[]> a, List<Object[]> b) {
        Map<Object, List<Object[]>> bIdIndex = new LinkedHashMap<>();
        for (Object[] rb : b) {
            Object id = rb[ID];
            if (id == null) {
                continue;
            }
            bIdIndex.computeIfAbsent(id, kk -> new ArrayList<>()).add(rb);
        }

        // group by dept sum(amount):
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] ra : a) {
            Object id = ra[ID];
            if (id == null) {
                continue;  // a null join key does not match
            }
            List<Object[]> bMatches = bIdIndex.get(id);
            if (bMatches == null || bMatches.isEmpty()) {
                continue;  // an inner join with no match
            }
            BigDecimal amt = (BigDecimal) ra[AMOUNT];  // the amount on the A side
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;  // filter amount>=0 always holds here
            }
            Integer gradeA = (Integer) ra[GRADE];
            int copies = (gradeA == null ? 0 : gradeA) + 1;
            int weight = copies * bMatches.size();
            if (weight <= 0) {
                continue;
            }
            Key k = new Key(ra[DEPT]);
            BigDecimal acc = sums.get(k);
            if (amt == null) {
                if (acc == null) {
                    sums.put(k, BigDecimal.ZERO);
                }
            } else {
                BigDecimal add = amt.multiply(BigDecimal.valueOf(weight));
                sums.put(k, acc == null ? add : acc.add(add));
            }
        }

        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : sums.entrySet()) {
            res.add(new Object[] { e.getKey().value, scale(e.getValue()) });
        }
        return res;
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
            return 1;  // null last
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

