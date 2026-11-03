package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTyped02 {

    private static final int W_ID = 0;
    private static final int W_DEPT = 1;
    private static final int W_GRADE = 3;
    private static final int W_AMOUNT = 4;
    private static final int W_ACTIVE = 6;
    private static final int W_CREATED = 7;
    private static final int W_TSTAMP = 8;
    private static final int W_LABEL = 9;

    private static final int LD_LABEL = 0;

    private OracleTyped02() { }

    public static List<List<Object[]>> compute(List<Object[]> wideA,
                                               List<Object[]> wideB,
                                               List<Object[]> supp,
                                               List<Object[]> dim,
                                               List<Object[]> labelDim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(wideA, labelDim));
        out.add(o1(wideA, wideB));
        out.add(o2(wideA, supp));
        out.add(o3(wideA, wideB, dim));
        out.add(o4(wideA, wideB));
        return out;
    }

    // o0: join(wideA, labelDim) on label -> filter(amount>=0) -> [id,dept,created,amount]
    // -> groupBy(created).sum(amount) -> top8(sum desc, created asc by getTime)
    private static List<Object[]> o0(List<Object[]> a, List<Object[]> labelDim) {
        Map<Object, List<Object[]>> ldIndex = OracleTyped01.indexByCol(labelDim, LD_LABEL);
        // joined on label, a String that matches exactly one row per line so nothing is amplified, then sum(amount) accumulated at full precision per created Date.
        Map<OracleTyped01.Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] ra : a) {
            Object label = ra[W_LABEL];
            if (label == null) {
                continue;
            }
            List<Object[]> matches = ldIndex.get(label);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            BigDecimal amt = (BigDecimal) ra[W_AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }
            for (int i = 0; i < matches.size(); i++) {
                OracleTyped01.Key k = new OracleTyped01.Key(ra[W_CREATED]);
                BigDecimal acc = sums.get(k);
                if (amt == null) {
                    if (acc == null) {
                        sums.put(k, BigDecimal.ZERO);
                    }
                } else {
                    sums.put(k, acc == null ? amt : acc.add(amt));
                }
            }
        }
        List<Object[]> grouped = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, BigDecimal> e : sums.entrySet()) {
            grouped.add(new Object[] { e.getKey().value, OracleTyped01.scale(e.getValue()) });
        }
        grouped.sort((x, y) -> {
            int c = ((BigDecimal) y[1]).compareTo((BigDecimal) x[1]); // sum desc
            if (c != 0) {
                return c;
            }
            return compareDateNullsLast(x[0], y[0]); // created asc by getTime, NULLS_LAST
        });
        return OracleTyped01.firstN(grouped, 8);
    }

    // -> select[id(0),dept(1),amount(4)]
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> b) {
        Map<OracleTyped01.KeyN, Object[]> seen = new LinkedHashMap<>();
        // union order: A first, then B. flatMap only copies and leaves the distinct result unchanged, and distinct keeps the first occurrence as representative.
        addDistinct(a, seen);
        addDistinct(b, seen);
        List<Object[]> res = new ArrayList<>();
        for (Object[] r : seen.values()) {
            BigDecimal amt = (BigDecimal) r[W_AMOUNT];
            res.add(new Object[] { r[W_ID], r[W_DEPT], amt == null ? null : OracleTyped01.scale(amt) });
        }
        return res;
    }

    private static void addDistinct(List<Object[]> rows, Map<OracleTyped01.KeyN, Object[]> seen) {
        for (Object[] r : rows) {
            Integer grade = (Integer) r[W_GRADE];
            int copies = (grade == null ? 0 : grade) + 1;
            if (copies <= 0) {
                continue;
            }
            OracleTyped01.KeyN k = new OracleTyped01.KeyN(new Object[] {
                    r[W_ID], r[W_ACTIVE], r[W_CREATED], r[W_TSTAMP], r[W_LABEL] });
            if (!seen.containsKey(k)) {
                seen.put(k, r);
            }
        }
    }

    // o2: coGroup(wideA.ORDER6, supp) on dept -> filter rc>=0 -> reduceGroup Σlc by dept
    private static List<Object[]> o2(List<Object[]> a, List<Object[]> s) {
        Map<OracleTyped01.Key, int[]> counts = OracleTyped01.coGroupCounts(a, s);
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, int[]> e : counts.entrySet()) {
            int rc = e.getValue()[1];
            if (rc < 0) {
                continue;
            }
            res.add(new Object[] { e.getKey().value, e.getValue()[0] });
        }
        return res;
    }

    // o3: union(union(leftJoin(wideA.ORDER6,dim), rightJoin(wideB.ORDER6,dim)), fullJoin(wideA.ORDER6,dim))
    // -> groupBy(dept).max/min/count
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        List<Object[]> aOrder = order6(a);
        List<Object[]> bOrder = order6(b);
        List<Object[]> u = new ArrayList<>();
        OracleTyped01.leftJoinDim(aOrder, dim, u);
        OracleTyped01.rightJoinDim(bOrder, dim, u);
        OracleTyped01.fullJoinDim(aOrder, dim, u);
        return OracleTyped01.groupMaxMinCountByDept(u);
    }

    // o4: cg = sum(wideA) by dept; cr = sum(wideB) by dept; fullJoin
    private static List<Object[]> o4(List<Object[]> a, List<Object[]> b) {
        Map<OracleTyped01.Key, BigDecimal> cg = OracleTyped01.sumByDept(order6(a));
        Map<OracleTyped01.Key, BigDecimal> cr = OracleTyped01.sumByDept(order6(b));
        return OracleTyped01.fullJoinSums(cg, cr);
    }

    static List<Object[]> order6(List<Object[]> wide) {
        List<Object[]> res = new ArrayList<>(wide.size());
        for (Object[] r : wide) {
            res.add(new Object[] { r[0], r[1], r[2], r[3], r[4], r[5] });
        }
        return res;
    }

    /** Date ascending by getTime, NULLS_LAST. */
    static int compareDateNullsLast(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;
        }
        if (b == null) {
            return -1;
        }
        return Long.compare(((Date) a).getTime(), ((Date) b).getTime());
    }
}

