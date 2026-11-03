package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTyped04 {

    private static final int W_ID = 0;
    private static final int W_DEPT = 1;
    private static final int W_GRADE = 3;
    private static final int W_AMOUNT = 4;
    private static final int W_ACTIVE = 6;
    private static final int W_LABEL = 9;

    private static final int O_ID = 0;
    private static final int O_DEPT = 1;

    private OracleTyped04() { }

    public static List<List<Object[]>> compute(List<Object[]> wideA,
                                               List<Object[]> wideB,
                                               List<Object[]> supp,
                                               List<Object[]> dim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(wideA));
        out.add(o1(wideA, wideB));
        out.add(o2(wideA, supp));
        out.add(o3(wideA, wideB, dim));
        out.add(o4(wideA, wideB));
        return out;
    }

    // o0: combineReduceGroup(wideA.ORDER6)=sum(amount) by dept -> top9(total desc, dept asc) -> [dept,total]
    private static List<Object[]> o0(List<Object[]> a) {
        Map<OracleTyped01.Key, BigDecimal> sums = OracleTyped01.sumByDept(OracleTyped02.order6(a));
        List<Object[]> grouped = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, BigDecimal> e : sums.entrySet()) {
            grouped.add(new Object[] { e.getKey().value, OracleTyped01.scale(e.getValue()) });
        }
        grouped.sort((x, y) -> {
            int c = ((BigDecimal) y[1]).compareTo((BigDecimal) x[1]); // total desc
            if (c != 0) {
                return c;
            }
            return compareIntNullsLast(x[0], y[0]); // dept asc NULLS_LAST
        });
        return OracleTyped01.firstN(grouped, 9);
    }

    // o1: cg = sum(wideA) by dept; cr = sum(wideB) by dept; fullJoin -> [dept,sum0,dept,total]
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> b) {
        Map<OracleTyped01.Key, BigDecimal> cg = OracleTyped01.sumByDept(OracleTyped02.order6(a));
        Map<OracleTyped01.Key, BigDecimal> cr = OracleTyped01.sumByDept(OracleTyped02.order6(b));
        return OracleTyped01.fullJoinSums(cg, cr);
    }

    // o2: coGroup + filter lc>=0 + reduceGroup Σlc
    private static List<Object[]> o2(List<Object[]> a, List<Object[]> s) {
        Map<OracleTyped01.Key, int[]> counts = OracleTyped01.coGroupCounts(OracleTyped02.order6(a), s);
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, int[]> e : counts.entrySet()) {
            if (e.getValue()[0] < 0) {
                continue;
            }
            res.add(new Object[] { e.getKey().value, e.getValue()[0] });
        }
        return res;
    }

    // o3: union(leftJoin(wideA.ORDER6,dim), rightJoin(wideB.ORDER6,dim)) -> distinct(id,dept)
    // -> groupBy(dept).max/min/count
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        List<Object[]> u = new ArrayList<>();
        OracleTyped01.leftJoinDim(OracleTyped02.order6(a), dim, u);
        OracleTyped01.rightJoinDim(OracleTyped02.order6(b), dim, u);
        // distinct(id, dept): keeps the first occurrence as the representative whole row, 8 columns wide
        Map<OracleTyped01.Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] row : u) {
            OracleTyped01.Key2 k = new OracleTyped01.Key2(row[O_ID], row[O_DEPT]);
            if (!seen.containsKey(k)) {
                seen.put(k, row);
            }
        }
        return OracleTyped01.groupMaxMinCountByDept(seen.values());
    }

    // -> select[id,dept,label,active,amount] -> groupBy(label).sum(amount)
    private static List<Object[]> o4(List<Object[]> a, List<Object[]> b) {
        Map<Object, List<Object[]>> bIdIndex = OracleTyped01.indexByCol(OracleTyped02.order6(b), O_ID);
        Map<OracleTyped01.Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] ra : a) {
            Object id = ra[W_ID];
            if (id == null) {
                continue;
            }
            List<Object[]> matches = bIdIndex.get(id);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            Integer grade = (Integer) ra[W_GRADE];  // flatMap uses idx3 of the joined row, which is wideA.grade
            int copies = (grade == null ? 0 : grade) + 1;
            if (copies <= 0) {
                continue;
            }
            BigDecimal amt = (BigDecimal) ra[W_AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;  // filter amount>=0 runs after flatMap, on rows carrying the same amount
            }
            // one copy per match of b, times the copies
            int total = copies * matches.size();
            OracleTyped01.Key k = new OracleTyped01.Key(ra[W_LABEL]);
            BigDecimal acc = sums.get(k);
            if (amt == null) {
                if (acc == null) {
                    sums.put(k, BigDecimal.ZERO);
                }
            } else {
                BigDecimal contrib = amt.multiply(BigDecimal.valueOf(total));
                sums.put(k, acc == null ? contrib : acc.add(contrib));
            }
        }
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, BigDecimal> e : sums.entrySet()) {
            res.add(new Object[] { e.getKey().value, OracleTyped01.scale(e.getValue()) });
        }
        return res;
    }

    /** Integer ascending, NULLS_LAST. */
    static int compareIntNullsLast(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;
        }
        if (b == null) {
            return -1;
        }
        return Integer.compare((Integer) a, (Integer) b);
    }
}

