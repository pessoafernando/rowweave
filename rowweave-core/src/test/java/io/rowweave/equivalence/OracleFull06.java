package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleFull06 {

    private static final int ID = 0;
    private static final int DEPT = 1;
    private static final int REGION = 2;
    private static final int GRADE = 3;
    private static final int AMOUNT = 4;
    private static final int QTY = 5;

    private static final int DIM_DEPT = 0;
    private static final int DIM_BONUS = 1;

    private static final int RD_REGION = 0;
    private static final int RD_RNAME = 1;

    private OracleFull06() { }

    /** Returns a list of five entries, the outputs o0 through o4, in that order. */
    public static List<List<Object[]>> compute(List<Object[]> ordersA,
                                               List<Object[]> ordersB,
                                               List<Object[]> supp,
                                               List<Object[]> dim,
                                               List<Object[]> regionDim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(ordersA));
        out.add(o1(ordersA, regionDim));
        out.add(o2(ordersA, supp));
        out.add(o3(ordersA, ordersB, dim));
        out.add(o4(ordersA, dim));
        return out;
    }

    // ---------------------------------------------------------------------
    // o0: A orderBy id asc, top 100 -> project [id, dept, amount]
    // ---------------------------------------------------------------------
    private static List<Object[]> o0(List<Object[]> a) {
        List<Object[]> sorted = new ArrayList<>(a);
        sorted.sort((x, y) -> compareNumNullsLast(x[ID], y[ID])); // id asc NULLS_LAST

        List<Object[]> res = new ArrayList<>();
        int limit = Math.min(100, sorted.size());
        for (int i = 0; i < limit; i++) {
            Object[] r = sorted.get(i);
            BigDecimal amt = (BigDecimal) r[AMOUNT];
            res.add(new Object[] { r[ID], r[DEPT], amt == null ? null : scale(amt) });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // group by dept sum(amount idx4) -> [dept, sum]
    // ---------------------------------------------------------------------
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> regionDim) {
        // regionDim is indexed by region, since only a non-null region can match
        Map<Object, List<Object[]>> rdIndex = new LinkedHashMap<>();
        for (Object[] rd : regionDim) {
            Object region = rd[RD_REGION];
            if (region == null) {
                continue;
            }
            rdIndex.computeIfAbsent(region, kk -> new ArrayList<>()).add(rd);
        }

        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] ra : a) {
            Object region = ra[REGION];
            if (region == null) {
                continue;  // a null join key does not match
            }
            List<Object[]> matches = rdIndex.get(region);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            BigDecimal amt = (BigDecimal) ra[AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;  // filter amount>=0 always holds here
            }
            int mult = matches.size();  // cartesian: one row per matching regionDim row for each A row
            Key k = new Key(ra[DEPT]);
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

        // top 10: sum descending, dept ascending with NULLS_LAST.
        grouped.sort((x, y) -> {
            int c = ((BigDecimal) y[1]).compareTo((BigDecimal) x[1]); // sum desc
            if (c != 0) {
                return c;
            }
            return compareNumNullsLast(x[0], y[0]); // dept asc NULLS_LAST
        });

        List<Object[]> res = new ArrayList<>();
        int limit = Math.min(10, grouped.size());
        for (int i = 0; i < limit; i++) {
            res.add(grouped.get(i));
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
            if (lc < 1) {
                continue;
            }
            res.add(new Object[] { e.getKey().value, lc });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // distinct(id, dept)(u)
    // group by dept(idx1): max(amount idx4), min(amount), count -> [dept,max,min,count]
    // ---------------------------------------------------------------------
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        Map<Object, List<Object[]>> dIndex = indexByCol(dim, DIM_DEPT);

        List<Object[]> u = new ArrayList<>();  // 8 columns = [order6, dim2]

        // leftJoin(A, dim): every A row is kept, and an unmatched row is appended with [null,null].
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

        Map<Object, List<Object[]>> bIndex = indexByCol(b, DEPT);
        for (Object[] rb : b) {
            Object dept = rb[DEPT];
            List<Object[]> matches = dept == null ? null : dIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(join8(rb, null));
            } else {
                for (Object[] d : matches) {
                    u.add(join8(rb, d));
                }
            }
        }
        for (Object[] d : dim) {
            Object dept = d[DIM_DEPT];
            List<Object[]> matches = dept == null ? null : bIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(join8(null, d));  // a dim row with no B match leaves the B side entirely null
            }
        }

        // distinct(id, dept) on idx0 and idx1: the representative row is kept.
        Map<Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] row : u) {
            Key2 k = new Key2(row[ID], row[DEPT]);
            if (!seen.containsKey(k)) {
                seen.put(k, row);
            }
        }

        return groupMaxMinCountByDept(seen.values());
    }

    // ---------------------------------------------------------------------
    // fullJoin(cg, cr) on (dept,dept) -> [dept, sum0, dept, total]
    // ---------------------------------------------------------------------
    private static List<Object[]> o4(List<Object[]> a, List<Object[]> dim) {
        // cg: each A row's amount is weighted (grade+1) times and summed per dept.
        Map<Key, BigDecimal> cg = new LinkedHashMap<>();
        for (Object[] ra : a) {
            Integer grade = (Integer) ra[GRADE];
            int copies = (grade == null ? 0 : grade) + 1;
            Key k = new Key(ra[DEPT]);
            BigDecimal amt = (BigDecimal) ra[AMOUNT];
            BigDecimal acc = cg.get(k);
            if (amt == null) {
                if (acc == null) {
                    cg.put(k, BigDecimal.ZERO);
                }
            } else if (copies > 0) {
                BigDecimal add = amt.multiply(BigDecimal.valueOf(copies));
                cg.put(k, acc == null ? add : acc.add(add));
            } else if (acc == null) {
                cg.put(k, BigDecimal.ZERO);
            }
        }

        Map<Object, List<Object[]>> aByDept = indexByCol(a, DEPT);
        Map<Key, BigDecimal> cr = new LinkedHashMap<>();
        for (Object[] d : dim) {
            Object dept = d[DIM_DEPT];
            List<Object[]> matches = dept == null ? null : aByDept.get(dept);
            if (matches == null || matches.isEmpty()) {
                // With the A side nulled, projecting back gives dept=null and amount=null, so the null group is formed with value 0.
                Key k = new Key(null);
                if (!cr.containsKey(k)) {
                    cr.put(k, BigDecimal.ZERO);
                }
                continue;
            }
            for (Object[] ra : matches) {
                Key k = new Key(ra[DEPT]);
                BigDecimal amt = (BigDecimal) ra[AMOUNT];
                BigDecimal acc = cr.get(k);
                if (amt == null) {
                    if (acc == null) {
                        cr.put(k, BigDecimal.ZERO);
                    }
                } else {
                    cr.put(k, acc == null ? amt : acc.add(amt));
                }
            }
        }

        // fullJoin(cg, cr) on (dept, dept).
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
                continue;  // a null join key is not indexed
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

