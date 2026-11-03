package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTyped09 {

    private static final int W_ID = 0;
    private static final int W_DEPT = 1;
    private static final int W_REGION = 2;
    private static final int W_GRADE = 3;
    private static final int W_AMOUNT = 4;
    private static final int W_ACTIVE = 6;
    private static final int W_CREATED = 7;
    private static final int W_LABEL = 9;

    private static final int O_ID = 0;
    private static final int O_DEPT = 1;
    private static final int O_REGION = 2;
    private static final int O_AMOUNT = 4;

    private static final int DIM_DEPT = 0;
    private static final int RD_REGION = 0;

    private OracleTyped09() { }

    public static List<List<Object[]>> compute(List<Object[]> wideA,
                                               List<Object[]> wideB,
                                               List<Object[]> supp,
                                               List<Object[]> dim,
                                               List<Object[]> regionDim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(wideA, dim));
        out.add(o1(wideA, wideB));
        out.add(o2(wideA, supp));
        out.add(o3(wideA, wideB, dim, regionDim));
        out.add(o4(wideA, wideB));
        return out;
    }

    // o0: join(wideA,dim) on dept -> select(idx0,1,6,4,5)=[id,dept,active,amount,qty] -> map_id
    private static List<Object[]> o0(List<Object[]> a, List<Object[]> dim) {
        Map<Object, List<Object[]>> dIndex = OracleTyped01.indexByCol(dim, DIM_DEPT);
        Map<OracleTyped01.Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] ra : a) {
            Object dept = ra[W_DEPT];
            if (dept == null) {
                continue;
            }
            List<Object[]> matches = dIndex.get(dept);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            BigDecimal amt = (BigDecimal) ra[W_AMOUNT];
            for (int i = 0; i < matches.size(); i++) {
                OracleTyped01.Key k = new OracleTyped01.Key(ra[W_ACTIVE]);
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
            int c = ((BigDecimal) x[1]).compareTo((BigDecimal) y[1]); // sum asc
            if (c != 0) {
                return c;
            }
            return OracleTyped01.compareBoolNullsLast(x[0], y[0]);  // active ascending as the tie-break
        });
        return OracleTyped01.firstN(grouped, 5);
    }

    // o1: union(wideA,wideB) -> flatMap(grade+1) -> distinct(id,created,label) -> filter(amount>=0)
    // -> select[id,dept,amount]
    private static List<Object[]> o1(List<Object[]> a, List<Object[]> b) {
        Map<OracleTyped01.KeyN, Object[]> seen = new LinkedHashMap<>();
        addDistinct(a, seen);
        addDistinct(b, seen);
        List<Object[]> res = new ArrayList<>();
        for (Object[] r : seen.values()) {
            BigDecimal amt = (BigDecimal) r[W_AMOUNT];
            // filter(amount>=0) runs after distinct, so it applies to the representative row.
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }
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
            OracleTyped01.KeyN k = new OracleTyped01.KeyN(new Object[] { r[W_ID], r[W_CREATED], r[W_LABEL] });
            if (!seen.containsKey(k)) {
                seen.put(k, r);
            }
        }
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

    // o3: lj=select6(leftJoin(wideA.ORDER6,dim)); rj=select6(rightJoin(wideB.ORDER6,dim));
    // rjoin=select6(join(wideA.ORDER6,regionDim) on region);
    // union(union(lj,rj),rjoin) -> groupBy(dept).max/min/count -> [dept,max0,min1,count2]
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b,
                                     List<Object[]> dim, List<Object[]> regionDim) {
        List<Object[]> aOrder = OracleTyped02.order6(a);
        List<Object[]> bOrder = OracleTyped02.order6(b);
        List<Object[]> u = new ArrayList<>();
        // lj and rj are 8-column joined rows, and select6 keeps the first 6 order6 columns; groupMaxMinCountByDept uses only dept (idx1) and amount (idx4).
        OracleTyped01.leftJoinDim(aOrder, dim, u);
        OracleTyped01.rightJoinDim(bOrder, dim, u);
        // rjoin: inner join wideA.ORDER6 on region with regionDim, one match per region, then select6 gives the order6 side.
        Map<Object, List<Object[]>> rdIndex = OracleTyped01.indexByCol(regionDim, RD_REGION);
        for (Object[] ra : aOrder) {
            Object region = ra[O_REGION];
            if (region == null) {
                continue;
            }
            List<Object[]> matches = rdIndex.get(region);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            for (int i = 0; i < matches.size(); i++) {
                u.add(new Object[] { ra[O_ID], ra[O_DEPT], ra[O_REGION], ra[3], ra[O_AMOUNT], ra[5] });
            }
        }
        return OracleTyped01.groupMaxMinCountByDept(u);
    }

    // o4: cg = sum(wideA) by dept; cr = sum(wideB) by dept; fullJoin -> [dept,sum0,dept,total]
    private static List<Object[]> o4(List<Object[]> a, List<Object[]> b) {
        Map<OracleTyped01.Key, BigDecimal> cg = OracleTyped01.sumByDept(OracleTyped02.order6(a));
        Map<OracleTyped01.Key, BigDecimal> cr = OracleTyped01.sumByDept(OracleTyped02.order6(b));
        return OracleTyped01.fullJoinSums(cg, cr);
    }
}

