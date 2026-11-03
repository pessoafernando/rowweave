package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleFull01 {

    private static final int ID = 0;
    private static final int DEPT = 1;
    private static final int REGION = 2;
    private static final int GRADE = 3;
    private static final int AMOUNT = 4;
    private static final int QTY = 5;

    private static final int DIM_DEPT = 0;
    private static final int DIM_BONUS = 1;

    private OracleFull01() { }

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
        out.add(o4(ordersA));
        return out;
    }

    // ---------------------------------------------------------------------
    // o0: filter amount>=10 -> group by region sum(amount) -> top5 (sum desc, region asc)
    // ---------------------------------------------------------------------
    private static List<Object[]> o0(List<Object[]> a) {
        BigDecimal ten = BigDecimal.TEN;
        // high-precision accumulation: region -> sum
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] r : a) {
            BigDecimal amt = (BigDecimal) r[AMOUNT];
            if (amt == null || amt.compareTo(ten) < 0) {
                continue;
            }
            Key k = new Key(r[REGION]);
            BigDecimal acc = sums.get(k);
            sums.put(k, acc == null ? amt : acc.add(amt));
        }

        List<Object[]> grouped = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : sums.entrySet()) {
            Integer region = (Integer) e.getKey().value;
            BigDecimal sum = scale(e.getValue());
            grouped.add(new Object[] { region, sum });
        }

        // sort keys: (sumAmount desc, region asc), with NULLS_LAST applied to the numeric region.
        grouped.sort((x, y) -> {
            int c = ((BigDecimal) y[1]).compareTo((BigDecimal) x[1]); // sum desc
            if (c != 0) {
                return c;
            }
            return compareNumNullsLast((Comparable) x[0], (Comparable) y[0]); // region asc
        });

        List<Object[]> res = new ArrayList<>();
        int limit = Math.min(5, grouped.size());
        for (int i = 0; i < limit; i++) {
            res.add(grouped.get(i));
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // o1: A inner join B on id -> flatMap (gradeA+1) copies -> distinct (id,dept) -> project [id,dept,amount] (A side)
    // ---------------------------------------------------------------------
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> b) {
        // inner join on id, a cartesian pairing in which a null id never matches; a joined row is A6 ++ B6
        List<Object[]> joined = new ArrayList<>();
        for (Object[] ra : a) {
            Object idA = ra[ID];
            if (idA == null) {
                continue;
            }
            for (Object[] rb : b) {
                Object idB = rb[ID];
                if (idB == null) {
                    continue;
                }
                if (idA.equals(idB)) {
                    Object[] row = new Object[12];
                    System.arraycopy(ra, 0, row, 0, 6);
                    System.arraycopy(rb, 0, row, 6, 6);
                    joined.add(row);
                }
            }
        }

        // flatMap: each row is copied (gradeA+1) times, where gradeA is the grade held by the A side at idx3
        List<Object[]> expanded = new ArrayList<>();
        for (Object[] row : joined) {
            Integer gradeA = (Integer) row[GRADE];
            int copies = (gradeA == null ? 0 : gradeA) + 1;
            for (int i = 0; i < copies; i++) {
                expanded.add(row);
            }
        }

        // distinct by (id,dept) on the A side at idx0 and idx1; one representative row is kept per unique key
        Map<Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] row : expanded) {
            Key2 k = new Key2(row[ID], row[DEPT]);
            if (!seen.containsKey(k)) {
                seen.put(k, row);
            }
        }

        // project [id, dept, amount] from the A side at idx0, idx1 and idx4
        List<Object[]> res = new ArrayList<>();
        for (Object[] row : seen.values()) {
            BigDecimal amt = (BigDecimal) row[AMOUNT];
            res.add(new Object[] { row[ID], row[DEPT], amt == null ? null : scale(amt) });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // o2: coGroup A,S by dept -> (dept, lc, rc) -> filter lc>=1 -> group by dept sum(lc) -> [dept, lcSum]
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
            if (lc < 1) {
                continue;
            }
            res.add(new Object[] { e.getKey().value, lc });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // o3: (A leftJoin D) union (B rightJoin D) -> group by dept(idx1) -> [dept, max(amount), min(amount), count]
    // ---------------------------------------------------------------------
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        // D index: dept -> the D rows carrying it, since only a non-null dept can match
        Map<Object, List<Object[]>> dIndex = new LinkedHashMap<>();
        for (Object[] d : dim) {
            Object dept = d[DIM_DEPT];
            if (dept == null) {
                continue;
            }
            dIndex.computeIfAbsent(dept, kk -> new ArrayList<>()).add(d);
        }

        List<Object[]> u = new ArrayList<>();  // 8 columns = [the 6 B/A columns, the 2 D columns]

        // leftJoin: every A row is kept, appended with D[dept,bonus] when it matches and with [null,null] otherwise
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
                u.add(join8(null, d));  // the B side is entirely null
            } else {
                for (Object[] rb : matches) {
                    u.add(join8(rb, d));
                }
            }
        }

        // group by dept (idx1), take max and min over amount (idx4, nulls skipped) and count every row
        Map<Key, Agg> groups = new LinkedHashMap<>();
        for (Object[] row : u) {
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
        // idx 0..5 are null when side6 is null
        if (d != null) {
            row[6] = d[DIM_DEPT];
            row[7] = d[DIM_BONUS];
        }
        return row;
    }

    // ---------------------------------------------------------------------
    // ---------------------------------------------------------------------
    private static List<Object[]> o4(List<Object[]> a) {
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] r : a) {
            BigDecimal amt = (BigDecimal) r[AMOUNT];
            Key k = new Key(r[DEPT]);
            BigDecimal acc = sums.get(k);
            if (amt == null) {
                // a null amount still has to form its dept group; it contributes 0 and leaves the high-precision sum unchanged
                if (acc == null) {
                    sums.put(k, BigDecimal.ZERO);
                }
            } else {
                sums.put(k, acc == null ? amt : acc.add(amt));
            }
        }

        // cg and cr both sum the same A by dept, so the dept sets agree and fullJoin yields one row per dept
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : sums.entrySet()) {
            Integer dept = (Integer) e.getKey().value;
            BigDecimal sum0 = scale(e.getValue());
            BigDecimal total = scale(e.getValue());
            res.add(new Object[] { dept, sum0, dept, total });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------
    private static BigDecimal scale(BigDecimal v) {
        return v.setScale(10, RoundingMode.HALF_UP);
    }

    /** Ascending numeric order, NULLS_LAST. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareNumNullsLast(Comparable a, Comparable b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;  // null last
        }
        if (b == null) {
            return -1;
        }
        return a.compareTo(b);
    }

    /** A single-value grouping key; null forms its own group, and equals and hashCode handle null. */
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

