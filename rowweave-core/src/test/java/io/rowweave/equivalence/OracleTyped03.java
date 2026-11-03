package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTyped03 {

    // the mid columns
    private static final int M_ID = 0;
    private static final int M_DEPT = 1;
    private static final int M_ACTIVE = 2;
    private static final int M_GRADE = 3;
    private static final int M_TSTAMP = 4;
    private static final int M_AMOUNT = 5;

    // the wide-table columns
    private static final int W_ID = 0;
    private static final int W_DEPT = 1;
    private static final int W_ACTIVE = 6;
    private static final int W_GRADE = 3;
    private static final int W_TSTAMP = 8;
    private static final int W_AMOUNT = 4;

    // the ORDER6 columns
    private static final int O_ID = 0;
    private static final int O_DEPT = 1;
    private static final int O_GRADE = 3;
    private static final int O_AMOUNT = 4;

    private OracleTyped03() { }

    public static List<List<Object[]>> compute(List<Object[]> wideA,
                                               List<Object[]> wideB,
                                               List<Object[]> supp,
                                               List<Object[]> dim) {
        List<Object[]> mid = mid(wideA);
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(mid));
        out.add(o1(mid));
        out.add(o2(wideA, supp));
        out.add(o3(wideA, wideB, dim));
        out.add(o4(wideA));
        return out;
    }

    // mid = filter(amount>=0)(wideA) -> select[id,dept,active,grade,tstamp,amount]
    private static List<Object[]> mid(List<Object[]> a) {
        List<Object[]> res = new ArrayList<>();
        for (Object[] r : a) {
            BigDecimal amt = (BigDecimal) r[W_AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }
            res.add(new Object[] { r[W_ID], r[W_DEPT], r[W_ACTIVE], r[W_GRADE], r[W_TSTAMP], r[W_AMOUNT] });
        }
        return res;
    }

    // o0: mid, then orderBy(tstamp idx4 asc), top150 with id ascending as the tie-break, then flatMap(grade idx3 +1), then select[id,dept,amount]
    private static List<Object[]> o0(List<Object[]> mid) {
        List<Object[]> sorted = new ArrayList<>(mid);
        sorted.sort((x, y) -> {
            int c = compareTsNullsLast(x[M_TSTAMP], y[M_TSTAMP]); // tstamp asc NULLS_LAST
            if (c != 0) {
                return c;
            }
            return compareLong(x[M_ID], y[M_ID]);  // id ascending, for a deterministic tie-break
        });
        List<Object[]> top = OracleTyped01.firstN(sorted, 150);
        // flatMap copies (grade+1) times, after the top
        List<Object[]> res = new ArrayList<>();
        for (Object[] r : top) {
            Integer grade = (Integer) r[M_GRADE];
            int copies = (grade == null ? 0 : grade) + 1;
            BigDecimal amt = (BigDecimal) r[M_AMOUNT];
            Object amount = amt == null ? null : OracleTyped01.scale(amt);
            for (int i = 0; i < copies; i++) {
                res.add(new Object[] { r[M_ID], r[M_DEPT], amount });
            }
        }
        return res;
    }

    // o1: g2 = mid.groupBy(active).max(amount idx5).min(amount).count() -> [active,max,min,count]
    private static List<Object[]> o1(List<Object[]> mid) {
        Map<OracleTyped01.Key, OracleTyped01.Agg> groups = new LinkedHashMap<>();
        for (Object[] r : mid) {
            OracleTyped01.Key k = new OracleTyped01.Key(r[M_ACTIVE]);
            OracleTyped01.Agg agg = groups.get(k);
            if (agg == null) {
                agg = new OracleTyped01.Agg();
                groups.put(k, agg);
            }
            agg.count++;
            BigDecimal amt = (BigDecimal) r[M_AMOUNT];
            if (amt != null) {
                if (agg.max == null || amt.compareTo(agg.max) > 0) {
                    agg.max = amt;
                }
                if (agg.min == null || amt.compareTo(agg.min) < 0) {
                    agg.min = amt;
                }
            }
        }
        // the g2 rows
        List<Object[]> g2 = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, OracleTyped01.Agg> e : groups.entrySet()) {
            OracleTyped01.Agg agg = e.getValue();
            g2.add(new Object[] { e.getKey().value,
                    agg.max == null ? null : OracleTyped01.scale(agg.max),
                    agg.min == null ? null : OracleTyped01.scale(agg.min),
                    (long) agg.count });
        }
        // distinct(active, max): keeps the first occurrence as the representative whole row
        Map<OracleTyped01.Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] r : g2) {
            OracleTyped01.Key2 k = new OracleTyped01.Key2(r[0], r[1]);
            if (!seen.containsKey(k)) {
                seen.put(k, r);
            }
        }
        return new ArrayList<>(seen.values());
    }

    // o2: coGroup(wideA.ORDER6, supp) on dept -> filter lc>=0 -> reduceGroup Σlc
    private static List<Object[]> o2(List<Object[]> a, List<Object[]> s) {
        Map<OracleTyped01.Key, int[]> counts = OracleTyped01.coGroupCounts(OracleTyped02.order6(a), s);
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, int[]> e : counts.entrySet()) {
            int lc = e.getValue()[0];
            if (lc < 0) {
                continue;
            }
            res.add(new Object[] { e.getKey().value, e.getValue()[0] });
        }
        return res;
    }

    // o3: union(leftJoin(wideA.ORDER6,dim), rightJoin(wideB.ORDER6,dim)) -> join(wideA.ORDER6) on id
    // -> flatMap(grade+1) -> distinct(id,dept) -> select[id,dept,amount]
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        List<Object[]> aOrder = OracleTyped02.order6(a);
        List<Object[]> bOrder = OracleTyped02.order6(b);
        List<Object[]> u = new ArrayList<>();  // 8 columns [order6, dim2]
        OracleTyped01.leftJoinDim(aOrder, dim, u);
        OracleTyped01.rightJoinDim(bOrder, dim, u);
        // joins u on id to wideA.ORDER6, where u's id sits at idx0 (a rightJoin row with no match has a null id and cannot match)
        Map<Object, List<Object[]>> aIdIndex = OracleTyped01.indexByCol(aOrder, O_ID);
        // distinct(id, dept) over the joined row, taking the u side at idx0 and idx1; flatMap only copies, so the distinct result is unchanged
        Map<OracleTyped01.Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] lu : u) {
            Object id = lu[O_ID];
            if (id == null) {
                continue;
            }
            List<Object[]> matches = aIdIndex.get(id);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            Integer grade = (Integer) lu[O_GRADE];  // flatMap uses idx3 of the current joined row, the grade on the u side
            int copies = (grade == null ? 0 : grade) + 1;
            if (copies <= 0) {
                continue;
            }
            OracleTyped01.Key2 k = new OracleTyped01.Key2(lu[O_ID], lu[O_DEPT]);
            if (!seen.containsKey(k)) {
                seen.put(k, lu);
            }
        }
        List<Object[]> res = new ArrayList<>();
        for (Object[] lu : seen.values()) {
            BigDecimal amt = (BigDecimal) lu[O_AMOUNT];
            res.add(new Object[] { lu[O_ID], lu[O_DEPT], amt == null ? null : OracleTyped01.scale(amt) });
        }
        return res;
    }

    // o4: cg = sum(wideA) by dept; cr = sum(wideA) by dept; fullJoin
    private static List<Object[]> o4(List<Object[]> a) {
        Map<OracleTyped01.Key, BigDecimal> cg = OracleTyped01.sumByDept(OracleTyped02.order6(a));
        return OracleTyped01.fullJoinSums(cg, cg);
    }

    /** Timestamp ascending by (getTime, getNanos), NULLS_LAST. */
    static int compareTsNullsLast(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;
        }
        if (b == null) {
            return -1;
        }
        Timestamp ta = (Timestamp) a;
        Timestamp tb = (Timestamp) b;
        int c = Long.compare(ta.getTime(), tb.getTime());
        if (c != 0) {
            return c;
        }
        return Integer.compare(ta.getNanos(), tb.getNanos());
    }

    static int compareLong(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;
        }
        if (b == null) {
            return -1;
        }
        return Long.compare(((Number) a).longValue(), ((Number) b).longValue());
    }
}

