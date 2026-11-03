package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleFull02 {

    private static final int ID = 0;
    private static final int DEPT = 1;
    private static final int REGION = 2;
    private static final int GRADE = 3;
    private static final int AMOUNT = 4;
    private static final int QTY = 5;

    private static final int DIM_DEPT = 0;
    private static final int DIM_BONUS = 1;

    private OracleFull02() { }

    /** Returns a list of five entries, the outputs o0 through o4, in that order. */
    public static List<List<Object[]>> compute(List<Object[]> ordersA,
                                               List<Object[]> ordersB,
                                               List<Object[]> supp,
                                               List<Object[]> dim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(ordersA, supp));
        out.add(o1(ordersA, ordersB));
        out.add(o2(ordersA, supp));
        out.add(o3(ordersA, ordersB));
        out.add(o4(ordersA, dim));
        return out;
    }

    // ---------------------------------------------------------------------
    // -> reduceGroup by dept sum(lc) -> [dept, lcSum]
    // ---------------------------------------------------------------------
    private static List<Object[]> o0(List<Object[]> a, List<Object[]> s) {
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
            int rc = e.getValue()[1];
            if (rc < 0) {
                continue;  // never holds
            }
            int lc = e.getValue()[0];
            res.add(new Object[] { e.getKey().value, lc });
        }
        return res;
    }

    // ---------------------------------------------------------------------
    // o1: cg2 = group A by dept sum(amount) -> [dept, sum0]
    // cr = group B by dept sum(amount) -> [dept, total]
    // fullJoin(cg2, cr) on (dept,dept) -> [dept, sum0, dept, total]
    // ---------------------------------------------------------------------
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> b) {
        Map<Key, BigDecimal> sumA = sumByDept(a);
        Map<Key, BigDecimal> sumB = sumByDept(b);

        // fullJoin on (dept == dept). A null dept key never matches in a join, so it has to be handled separately.
        List<Object[]> res = new ArrayList<>();
        // Walk the left side: append a match, and null the right side for anything unmatched, including a null key.
        for (Map.Entry<Key, BigDecimal> le : sumA.entrySet()) {
            Object dept = le.getKey().value;
            BigDecimal sum0 = scale(le.getValue());
            if (dept != null && sumB.containsKey(le.getKey())) {
                BigDecimal total = scale(sumB.get(le.getKey()));
                res.add(new Object[] { dept, sum0, dept, total });
            } else {
                res.add(new Object[] { dept, sum0, null, null });
            }
        }
        // Walk the right side: only the rows found on the right alone, where the left has no match, including a null key.
        for (Map.Entry<Key, BigDecimal> re : sumB.entrySet()) {
            Object dept = re.getKey().value;
            boolean matched = dept != null && sumA.containsKey(re.getKey());
            if (!matched) {
                BigDecimal total = scale(re.getValue());
                res.add(new Object[] { null, null, dept, total });
            }
        }
        return res;
    }

    /** Sum amount per dept group, accumulated at full precision, with a null amount read as 0 but still forming its group. */
    private static Map<Key, BigDecimal> sumByDept(List<Object[]> rows) {
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] r : rows) {
            Key k = new Key(r[DEPT]);
            BigDecimal amt = (BigDecimal) r[AMOUNT];
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

    // ---------------------------------------------------------------------
    // o2: u = union(A, supp) -> group by region max(amount)/min(amount)/count
    // -> [region, max, min, count]
    // ---------------------------------------------------------------------
    private static List<Object[]> o2(List<Object[]> a, List<Object[]> s) {
        Map<Key, Agg> groups = new LinkedHashMap<>();
        accumulateRegion(groups, a);
        accumulateRegion(groups, s);

        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, Agg> e : groups.entrySet()) {
            Agg agg = e.getValue();
            BigDecimal max = agg.max == null ? null : scale(agg.max);
            BigDecimal min = agg.min == null ? null : scale(agg.min);
            res.add(new Object[] { e.getKey().value, max, min, (long) agg.count });
        }
        return res;
    }

    private static void accumulateRegion(Map<Key, Agg> groups, List<Object[]> rows) {
        for (Object[] r : rows) {
            Key k = new Key(r[REGION]);
            Agg agg = groups.get(k);
            if (agg == null) {
                agg = new Agg();
                groups.put(k, agg);
            }
            agg.count++;
            BigDecimal amt = (BigDecimal) r[AMOUNT];
            if (amt != null) {
                if (agg.max == null || amt.compareTo(agg.max) > 0) {
                    agg.max = amt;
                }
                if (agg.min == null || amt.compareTo(agg.min) < 0) {
                    agg.min = amt;
                }
            }
        }
    }

    // ---------------------------------------------------------------------
    // o3: join(A,B) on id (inner, A6++B6) -> flatMap (gradeA+1) copies
    // ---------------------------------------------------------------------
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b) {
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
    // rj = rightJoin(lj, dim) on dept -> filter amount>=5 -> map_id
    // proj = { (a.id, a.region, a.amount) | a∈A, a.amount>=5 }
    // o4 = group proj by region sum(amount) -> [region, sum]; top 6(region asc, NULLS_LAST)
    // ---------------------------------------------------------------------
    private static List<Object[]> o4(List<Object[]> a, List<Object[]> dim) {
        // dim index: dept -> the dim rows, used for the rightJoin cartesian pairing
        Map<Object, List<Object[]>> dIndex = new LinkedHashMap<>();
        for (Object[] d : dim) {
            Object dept = d[DIM_DEPT];
            if (dept == null) {
                continue;
            }
            dIndex.computeIfAbsent(dept, kk -> new ArrayList<>()).add(d);
        }

        BigDecimal five = BigDecimal.valueOf(5);

        Map<Key, BigDecimal> sums = new LinkedHashMap<>(); // region -> Σamount
        for (Object[] ra : a) {
            Object dept = ra[DEPT];
            List<Object[]> dimMatches = dept == null ? null : dIndex.get(dept);
            if (dimMatches == null || dimMatches.isEmpty()) {
                // This A row's dept is absent from dim, so rightJoin produces no row containing it.
                continue;
            }
            BigDecimal amt = (BigDecimal) ra[AMOUNT];
            if (amt == null || amt.compareTo(five) < 0) {
                continue; // filter amount>=5
            }
            Key rk = new Key(ra[REGION]);
            for (int i = 0; i < dimMatches.size(); i++) {
                BigDecimal acc = sums.get(rk);
                sums.put(rk, acc == null ? amt : acc.add(amt));
            }
        }

        List<Object[]> grouped = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : sums.entrySet()) {
            grouped.add(new Object[] { e.getKey().value, scale(e.getValue()) });
        }

        // top 6: sorted on region ascending (NULLS_LAST).
        grouped.sort((x, y) -> compareNumNullsLast(x[0], y[0]));

        List<Object[]> res = new ArrayList<>();
        int limit = Math.min(6, grouped.size());
        for (int i = 0; i < limit; i++) {
            res.add(grouped.get(i));
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

