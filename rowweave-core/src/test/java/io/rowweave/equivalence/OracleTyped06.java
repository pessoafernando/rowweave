package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTyped06 {

    private static final int W_ID = 0;
    private static final int W_DEPT = 1;
    private static final int W_AMOUNT = 4;
    private static final int W_TSTAMP = 8;

    private static final int O_ID = 0;
    private static final int O_DEPT = 1;
    private static final int O_GRADE = 3;
    private static final int O_AMOUNT = 4;

    private OracleTyped06() { }

    public static List<List<Object[]>> compute(List<Object[]> wideA,
                                               List<Object[]> wideB,
                                               List<Object[]> supp,
                                               List<Object[]> dim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(wideA));
        out.add(o1(wideA, wideB, dim));
        out.add(o2(wideA, supp));
        out.add(o3(wideA, wideB));
        out.add(o4(wideA));
        return out;
    }

    // -> [tstamp(Timestamp), count(Long)]
    private static List<Object[]> o0(List<Object[]> a) {
        // distinct over all 10 columns: every row is already unique, so this equals a itself; the first-occurrence rule is still applied as a guard.
        Map<OracleTyped01.KeyN, Object[]> distinct = new LinkedHashMap<>();
        for (Object[] r : a) {
            OracleTyped01.KeyN k = new OracleTyped01.KeyN(new Object[] {
                    r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9] });
            if (!distinct.containsKey(k)) {
                distinct.put(k, r);
            }
        }
        // filter(amount>=0) -> groupBy(tstamp).count()
        Map<OracleTyped01.Key, long[]> counts = new LinkedHashMap<>();
        for (Object[] r : distinct.values()) {
            BigDecimal amt = (BigDecimal) r[W_AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }
            OracleTyped01.Key k = new OracleTyped01.Key(r[W_TSTAMP]);
            long[] c = counts.get(k);
            if (c == null) {
                c = new long[1];
                counts.put(k, c);
            }
            c[0]++;
        }
        List<Object[]> grouped = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, long[]> e : counts.entrySet()) {
            grouped.add(new Object[] { e.getKey().value, e.getValue()[0] });
        }
        // top30 on tstamp ascending, NULLS_LAST; tstamp is the group key and unique, so no id tie-break is needed.
        grouped.sort((x, y) -> OracleTyped03.compareTsNullsLast(x[0], y[0]));
        return OracleTyped01.firstN(grouped, 30);
    }

    // o1: union(union(fullJoin(wideA.ORDER6,dim), leftJoin(wideA.ORDER6,dim)), rightJoin(wideB.ORDER6,dim))
    // -> groupBy(dept).max/min(amount).count -> [dept,max0,min1,count2]
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        List<Object[]> aOrder = OracleTyped02.order6(a);
        List<Object[]> bOrder = OracleTyped02.order6(b);
        List<Object[]> u = new ArrayList<>();
        OracleTyped01.fullJoinDim(aOrder, dim, u);
        OracleTyped01.leftJoinDim(aOrder, dim, u);
        OracleTyped01.rightJoinDim(bOrder, dim, u);
        return OracleTyped01.groupMaxMinCountByDept(u);
    }

    // o2: coGroup(wideA.ORDER6, supp) on dept -> filter lc>=0 -> reduceGroup Σlc by dept
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

    // o3: inner join(wideA.ORDER6, wideB.ORDER6) on id -> flatMap(grade+1) -> distinct(id,dept)
    // -> select[id,dept,amount]
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b) {
        Map<Object, List<Object[]>> bIdIndex = OracleTyped01.indexByCol(OracleTyped02.order6(b), O_ID);
        Map<OracleTyped01.Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] ra : OracleTyped02.order6(a)) {
            Object id = ra[O_ID];
            if (id == null) {
                continue;
            }
            List<Object[]> matches = bIdIndex.get(id);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            Integer grade = (Integer) ra[O_GRADE];
            int copies = (grade == null ? 0 : grade) + 1;
            if (copies <= 0) {
                continue;
            }
            OracleTyped01.Key2 k = new OracleTyped01.Key2(ra[O_ID], ra[O_DEPT]);
            if (!seen.containsKey(k)) {
                seen.put(k, ra);
            }
        }
        List<Object[]> res = new ArrayList<>();
        for (Object[] ra : seen.values()) {
            BigDecimal amt = (BigDecimal) ra[O_AMOUNT];
            res.add(new Object[] { ra[O_ID], ra[O_DEPT], amt == null ? null : OracleTyped01.scale(amt) });
        }
        return res;
    }

    // o4: cg = sum(wideA) by dept; cr = sum(wideA) by dept; fullJoin -> [dept,sum0,dept,total]
    private static List<Object[]> o4(List<Object[]> a) {
        Map<OracleTyped01.Key, BigDecimal> cg = OracleTyped01.sumByDept(OracleTyped02.order6(a));
        return OracleTyped01.fullJoinSums(cg, cg);
    }
}

