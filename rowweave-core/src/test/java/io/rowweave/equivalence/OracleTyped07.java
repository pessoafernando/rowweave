package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTyped07 {

    private static final int W_ID = 0;
    private static final int W_DEPT = 1;
    private static final int W_GRADE = 3;
    private static final int W_AMOUNT = 4;
    private static final int W_ACTIVE = 6;
    private static final int W_CREATED = 7;
    private static final int W_LABEL = 9;

    private static final int O_ID = 0;
    private static final int O_DEPT = 1;
    private static final int O_AMOUNT = 4;

    private OracleTyped07() { }

    public static List<List<Object[]>> compute(List<Object[]> wideA,
                                               List<Object[]> wideB,
                                               List<Object[]> supp,
                                               List<Object[]> dim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(wideA));
        out.add(o1(wideA));
        out.add(o2(wideA, supp));
        out.add(o3(wideA, wideB, dim));
        out.add(o4(wideA));
        return out;
    }

    // -> [active(Boolean), sum0(BigDecimal)]
    private static List<Object[]> o0(List<Object[]> a) {
        Map<OracleTyped01.Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] r : a) {
            Integer grade = (Integer) r[W_GRADE];
            int copies = (grade == null ? 0 : grade) + 1;
            if (copies <= 0) {
                continue;
            }
            BigDecimal amt = (BigDecimal) r[W_AMOUNT];
            // filter(amount>=0) applies to each copy, which carries the same amount, so anything below 0 is dropped entirely.
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }
            OracleTyped01.Key k = new OracleTyped01.Key(r[W_ACTIVE]);
            BigDecimal acc = sums.get(k);
            if (amt == null) {
                if (acc == null) {
                    sums.put(k, BigDecimal.ZERO);
                }
            } else {
                BigDecimal contrib = amt.multiply(BigDecimal.valueOf(copies));
                sums.put(k, acc == null ? contrib : acc.add(contrib));
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
            return OracleTyped01.compareBoolNullsLast(x[0], y[0]);  // active ascending as the tie-break
        });
        return OracleTyped01.firstN(grouped, 5);
    }

    // -> addFields tag -> removeFields(tag,qty)=[id,dept,label,created,amount]
    // -> [label(String), max0, min1, count2]
    private static List<Object[]> o1(List<Object[]> a) {
        // distinct(dept,label,created): keeps the representative whole row, the first in id order.
        Map<OracleTyped01.KeyN, Object[]> seen = new LinkedHashMap<>();
        for (Object[] r : a) {
            OracleTyped01.KeyN k = new OracleTyped01.KeyN(new Object[] { r[W_DEPT], r[W_LABEL], r[W_CREATED] });
            Object[] cur = seen.get(k);
            if (cur == null || OracleTyped03.compareLong(r[W_ID], cur[W_ID]) < 0) {
                seen.put(k, r);
            }
        }
        // groupBy(label).max/min(amount).count, over the deduplicated rows
        Map<OracleTyped01.Key, OracleTyped01.Agg> groups = new LinkedHashMap<>();
        for (Object[] r : seen.values()) {
            OracleTyped01.Key k = new OracleTyped01.Key(r[W_LABEL]);
            OracleTyped01.Agg agg = groups.get(k);
            if (agg == null) {
                agg = new OracleTyped01.Agg();
                groups.put(k, agg);
            }
            agg.count++;
            BigDecimal amt = (BigDecimal) r[W_AMOUNT];
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
        for (Map.Entry<OracleTyped01.Key, OracleTyped01.Agg> e : groups.entrySet()) {
            OracleTyped01.Agg agg = e.getValue();
            res.add(new Object[] { e.getKey().value,
                    agg.max == null ? null : OracleTyped01.scale(agg.max),
                    agg.min == null ? null : OracleTyped01.scale(agg.min),
                    (long) agg.count });
        }
        return res;
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
    // ij=select6(join(wideA.ORDER6,wideB.ORDER6) on id); union(union(lj,rj),ij) -> select[id,dept,amount]
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        List<Object[]> aOrder = OracleTyped02.order6(a);
        List<Object[]> bOrder = OracleTyped02.order6(b);
        List<Object[]> res = new ArrayList<>();
        // lj: leftJoin(wideA.ORDER6,dim).select6 keeps the order6 side, so id, dept and amount come from order6
        List<Object[]> lu = new ArrayList<>();
        OracleTyped01.leftJoinDim(aOrder, dim, lu);
        for (Object[] row : lu) {
            addIdDeptAmount(row, res);
        }
        // rj: rightJoin(wideB.ORDER6,dim).select6 leaves the order6 side null on a row with no match
        List<Object[]> ru = new ArrayList<>();
        OracleTyped01.rightJoinDim(bOrder, dim, ru);
        for (Object[] row : ru) {
            addIdDeptAmount(row, res);
        }
        // ij: inner join wideA.ORDER6 with wideB.ORDER6 on id, then select6 takes the A-side order6, one row per match
        Map<Object, List<Object[]>> bIdIndex = OracleTyped01.indexByCol(bOrder, O_ID);
        for (Object[] ra : aOrder) {
            Object id = ra[O_ID];
            if (id == null) {
                continue;
            }
            List<Object[]> matches = bIdIndex.get(id);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            for (int i = 0; i < matches.size(); i++) {
                addIdDeptAmount(ra, res);
            }
        }
        return res;
    }

    private static void addIdDeptAmount(Object[] row, List<Object[]> res) {
        BigDecimal amt = (BigDecimal) row[O_AMOUNT];
        res.add(new Object[] { row[O_ID], row[O_DEPT], amt == null ? null : OracleTyped01.scale(amt) });
    }

    // o4: cg = sum(wideA) by dept; cr = sum(wideA) by dept; fullJoin -> [dept,sum0,dept,total]
    private static List<Object[]> o4(List<Object[]> a) {
        Map<OracleTyped01.Key, BigDecimal> cg = OracleTyped01.sumByDept(OracleTyped02.order6(a));
        return OracleTyped01.fullJoinSums(cg, cg);
    }
}

