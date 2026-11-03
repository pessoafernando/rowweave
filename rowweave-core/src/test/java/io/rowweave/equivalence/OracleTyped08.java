package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTyped08 {

    private static final int W_ID = 0;
    private static final int W_DEPT = 1;
    private static final int W_GRADE = 3;
    private static final int W_AMOUNT = 4;
    private static final int W_CREATED = 7;
    private static final int W_TSTAMP = 8;
    private static final int W_LABEL = 9;

    private static final int LD_LABEL = 0;

    private OracleTyped08() { }

    public static List<List<Object[]>> compute(List<Object[]> wideA,
                                               List<Object[]> wideB,
                                               List<Object[]> supp,
                                               List<Object[]> dim,
                                               List<Object[]> labelDim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(wideA, labelDim));
        out.add(o1(wideA));
        out.add(o2(wideA, supp));
        out.add(o3(wideA, wideB, dim));
        out.add(o4(wideA, wideB));
        return out;
    }

    // o0: join(wideA,labelDim) on label -> filter(amount>=0) -> [id,dept,created,amount]
    // -> top10(sum desc, created asc by getTime) -> [created(Date), sum0]
    private static List<Object[]> o0(List<Object[]> a, List<Object[]> labelDim) {
        Map<Object, List<Object[]>> ldIndex = OracleTyped01.indexByCol(labelDim, LD_LABEL);
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
            return OracleTyped02.compareDateNullsLast(x[0], y[0]); // created asc getTime NULLS_LAST
        });
        return OracleTyped01.firstN(grouped, 10);
    }

    // -> distinct(id,tstamp) -> select[id,dept,amount]
    private static List<Object[]> o1(List<Object[]> a) {
        List<Object[]> sorted = new ArrayList<>(a);
        sorted.sort((x, y) -> {
            int c = OracleTyped03.compareTsNullsLast(x[W_TSTAMP], y[W_TSTAMP]); // tstamp asc NULLS_LAST
            if (c != 0) {
                return c;
            }
            return OracleTyped03.compareLong(x[W_ID], y[W_ID]);  // id ascending, for a deterministic tie-break
        });
        List<Object[]> top = OracleTyped01.firstN(sorted, 250);
        // flatMap copies (grade+1) times; distinct(id,tstamp) keeps the first occurrence in id order as the representative row.
        Map<OracleTyped01.Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] r : top) {
            Integer grade = (Integer) r[W_GRADE];
            int copies = (grade == null ? 0 : grade) + 1;
            if (copies <= 0) {
                continue;
            }
            OracleTyped01.Key2 k = new OracleTyped01.Key2(r[W_ID], r[W_TSTAMP]);
            Object[] cur = seen.get(k);
            if (cur == null || OracleTyped03.compareLong(r[W_ID], cur[W_ID]) < 0) {
                seen.put(k, r);
            }
        }
        List<Object[]> res = new ArrayList<>();
        for (Object[] r : seen.values()) {
            BigDecimal amt = (BigDecimal) r[W_AMOUNT];
            res.add(new Object[] { r[W_ID], r[W_DEPT], amt == null ? null : OracleTyped01.scale(amt) });
        }
        return res;
    }

    // o2: coGroup + filter rc>=0 + reduceGroup Σlc
    private static List<Object[]> o2(List<Object[]> a, List<Object[]> s) {
        Map<OracleTyped01.Key, int[]> counts = OracleTyped01.coGroupCounts(OracleTyped02.order6(a), s);
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, int[]> e : counts.entrySet()) {
            if (e.getValue()[1] < 0) {
                continue;
            }
            res.add(new Object[] { e.getKey().value, e.getValue()[0] });
        }
        return res;
    }

    // o3: leftJoin(wideA.ORDER6,dim).union(rightJoin(wideB.ORDER6,dim))
    // -> groupBy(dept).max/min/count -> [dept,max0,min1,count2]
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        List<Object[]> u = new ArrayList<>();
        OracleTyped01.leftJoinDim(OracleTyped02.order6(a), dim, u);
        OracleTyped01.rightJoinDim(OracleTyped02.order6(b), dim, u);
        return OracleTyped01.groupMaxMinCountByDept(u);
    }

    // o4: cg = sum(wideA) by dept; cr = sum(wideB) by dept; fullJoin -> [dept,sum0,dept,total]
    private static List<Object[]> o4(List<Object[]> a, List<Object[]> b) {
        Map<OracleTyped01.Key, BigDecimal> cg = OracleTyped01.sumByDept(OracleTyped02.order6(a));
        Map<OracleTyped01.Key, BigDecimal> cr = OracleTyped01.sumByDept(OracleTyped02.order6(b));
        return OracleTyped01.fullJoinSums(cg, cr);
    }
}

