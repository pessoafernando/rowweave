package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTyped01 {

    private static final int W_ID = 0;
    private static final int W_DEPT = 1;
    private static final int W_AMOUNT = 4;
    private static final int W_ACTIVE = 6;
    private static final int W_LABEL = 9;

    private static final int O_ID = 0;
    private static final int O_DEPT = 1;
    private static final int O_GRADE = 3;
    private static final int O_AMOUNT = 4;

    private static final int DIM_DEPT = 0;
    private static final int DIM_BONUS = 1;

    private OracleTyped01() { }

    public static List<List<Object[]>> compute(List<Object[]> wideA,
                                               List<Object[]> wideB,
                                               List<Object[]> supp,
                                               List<Object[]> dim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(wideA));
        out.add(o1(wideA, wideB));
        out.add(o2(wideA, supp));
        out.add(o3(wideA, wideB, dim));
        out.add(o4(wideA));
        return out;
    }

    // o0: filter(amount>=0)(wideA) -> [id,dept,active,label,amount] -> groupBy(active).sum(amount)
    private static List<Object[]> o0(List<Object[]> a) {
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] r : a) {
            BigDecimal amt = (BigDecimal) r[W_AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }
            Key k = new Key(r[W_ACTIVE]);
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
        // top5 sorted on sum descending, with ascending active as the tie-break: Boolean orders false before true, NULLS_LAST
        grouped.sort((x, y) -> {
            int c = ((BigDecimal) y[1]).compareTo((BigDecimal) x[1]);
            if (c != 0) {
                return c;
            }
            return compareBoolNullsLast(x[0], y[0]);
        });
        return firstN(grouped, 5);
    }

    // o1: wideA.ORDER6 join wideB.ORDER6 on id -> flatMap(gradeA+1) -> distinct(id,dept)
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> b) {
        Map<Object, List<Object[]>> bIdIndex = indexByCol(b, O_ID);
        Map<Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] ra : a) {
            Object idA = ra[O_ID];
            if (idA == null) {
                continue;
            }
            List<Object[]> matches = bIdIndex.get(idA);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            Integer gradeA = (Integer) ra[O_GRADE];
            int copies = (gradeA == null ? 0 : gradeA) + 1;
            if (copies <= 0) {
                continue;
            }
            Key2 k = new Key2(ra[O_ID], ra[O_DEPT]);
            if (!seen.containsKey(k)) {
                seen.put(k, ra);
            }
        }
        List<Object[]> res = new ArrayList<>();
        for (Object[] ra : seen.values()) {
            BigDecimal amt = (BigDecimal) ra[O_AMOUNT];
            res.add(new Object[] { ra[O_ID], ra[O_DEPT], amt == null ? null : scale(amt) });
        }
        return res;
    }

    // o2: coGroup(wideA.ORDER6, supp) on dept -> (dept,lc,rc) -> filter lc>=0 -> reduceGroup Σlc by dept
    private static List<Object[]> o2(List<Object[]> a, List<Object[]> s) {
        Map<Key, int[]> counts = coGroupCounts(a, s);
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, int[]> e : counts.entrySet()) {
            int lc = e.getValue()[0];
            if (lc < 0) {
                continue;
            }
            res.add(new Object[] { e.getKey().value, lc });
        }
        return res;
    }

    // o3: union(leftJoin(wideA.ORDER6,dim), rightJoin(wideB.ORDER6,dim)) on dept
    // -> groupBy(dept idx1).max/min(amount idx4).count -> [dept,max,min,count]
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        List<Object[]> u = new ArrayList<>();
        leftJoinDim(a, dim, u);
        rightJoinDim(b, dim, u);
        return groupMaxMinCountByDept(u);
    }

    // o4: cg = sum(wideA.ORDER6) by dept; cr = sum(wideA.ORDER6) by dept; fullJoin on dept
    private static List<Object[]> o4(List<Object[]> a) {
        Map<Key, BigDecimal> cg = sumByDept(a);
        return fullJoinSums(cg, cg);
    }

    // ---- shared helpers ----
    static void leftJoinDim(List<Object[]> left6, List<Object[]> dim, List<Object[]> u) {
        Map<Object, List<Object[]>> dIndex = indexByCol(dim, DIM_DEPT);
        for (Object[] r : left6) {
            Object dept = r[O_DEPT];
            List<Object[]> matches = dept == null ? null : dIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(join8(r, null));
            } else {
                for (Object[] d : matches) {
                    u.add(join8(r, d));
                }
            }
        }
    }

    static void rightJoinDim(List<Object[]> right6, List<Object[]> dim, List<Object[]> u) {
        Map<Object, List<Object[]>> bIndex = indexByCol(right6, O_DEPT);
        for (Object[] d : dim) {
            Object dept = d[DIM_DEPT];
            List<Object[]> matches = dept == null ? null : bIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(join8(null, d));
            } else {
                for (Object[] r : matches) {
                    u.add(join8(r, d));
                }
            }
        }
    }

    static void fullJoinDim(List<Object[]> left6, List<Object[]> dim, List<Object[]> u) {
        Map<Object, List<Object[]>> dIndex = indexByCol(dim, DIM_DEPT);
        for (Object[] r : left6) {
            Object dept = r[O_DEPT];
            List<Object[]> matches = dept == null ? null : dIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(join8(r, null));
            } else {
                for (Object[] d : matches) {
                    u.add(join8(r, d));
                }
            }
        }
        Map<Object, List<Object[]>> lIndex = indexByCol(left6, O_DEPT);
        for (Object[] d : dim) {
            Object dept = d[DIM_DEPT];
            List<Object[]> matches = dept == null ? null : lIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                u.add(join8(null, d));
            }
        }
    }

    static Object[] join8(Object[] side6, Object[] d) {
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

    static List<Object[]> groupMaxMinCountByDept(Iterable<Object[]> rows) {
        Map<Key, Agg> groups = new LinkedHashMap<>();
        for (Object[] row : rows) {
            Key k = new Key(row[O_DEPT]);
            Agg agg = groups.get(k);
            if (agg == null) {
                agg = new Agg();
                groups.put(k, agg);
            }
            agg.count++;
            BigDecimal amt = (BigDecimal) row[O_AMOUNT];
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
            res.add(new Object[] { e.getKey().value,
                    agg.max == null ? null : scale(agg.max),
                    agg.min == null ? null : scale(agg.min),
                    (long) agg.count });
        }
        return res;
    }

    static Map<Key, BigDecimal> sumByDept(List<Object[]> rows) {
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] r : rows) {
            BigDecimal amt = (BigDecimal) r[O_AMOUNT];
            Key k = new Key(r[O_DEPT]);
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

    static List<Object[]> fullJoinSums(Map<Key, BigDecimal> cg, Map<Key, BigDecimal> cr) {
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

    static Map<Key, int[]> coGroupCounts(List<Object[]> a, List<Object[]> s) {
        Map<Key, int[]> counts = new LinkedHashMap<>();
        for (Object[] r : a) {
            Key k = new Key(r[O_DEPT]);
            int[] c = counts.get(k);
            if (c == null) {
                c = new int[2];
                counts.put(k, c);
            }
            c[0]++;
        }
        for (Object[] r : s) {
            Key k = new Key(r[O_DEPT]);
            int[] c = counts.get(k);
            if (c == null) {
                c = new int[2];
                counts.put(k, c);
            }
            c[1]++;
        }
        return counts;
    }

    static Map<Object, List<Object[]>> indexByCol(List<Object[]> rows, int col) {
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

    static List<Object[]> firstN(List<Object[]> rows, int n) {
        List<Object[]> res = new ArrayList<>();
        int limit = Math.min(n, rows.size());
        for (int i = 0; i < limit; i++) {
            res.add(rows.get(i));
        }
        return res;
    }

    static BigDecimal scale(BigDecimal v) {
        return v.setScale(10, RoundingMode.HALF_UP);
    }

    /** Boolean ascending with false before true, NULLS_LAST. */
    static int compareBoolNullsLast(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;
        }
        if (b == null) {
            return -1;
        }
        return Boolean.compare((Boolean) a, (Boolean) b);
    }

    /** A single-value grouping key; null forms its own group, and Date, Timestamp, Boolean and String compare by value with equals. */
    static final class Key {
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
    static final class Key2 {
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

    /** An n-value grouping key for a multi-column distinct; null forms its own group, and Date, Timestamp, Boolean and String compare by value with equals. */
    static final class KeyN {
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

    static final class Agg {
        BigDecimal max;
        BigDecimal min;
        int count;
    }
}

