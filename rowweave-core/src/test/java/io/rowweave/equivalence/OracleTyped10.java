package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTyped10 {

    private static final int W_ID = 0;
    private static final int W_DEPT = 1;
    private static final int W_GRADE = 3;
    private static final int W_AMOUNT = 4;
    private static final int W_ACTIVE = 6;
    private static final int W_CREATED = 7;
    private static final int W_TSTAMP = 8;
    private static final int W_LABEL = 9;

    private static final int O_ID = 0;
    private static final int O_DEPT = 1;

    private static final int LD_LABEL = 0;

    private OracleTyped10() { }

    public static List<List<Object[]>> compute(List<Object[]> wideA,
                                               List<Object[]> wideB,
                                               List<Object[]> supp,
                                               List<Object[]> dim,
                                               List<Object[]> labelDim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(wideA, labelDim));
        out.add(o1(wideA));
        out.add(o2(wideA, supp));
        out.add(o3(wideA));
        out.add(o4(wideA, wideB, dim));
        return out;
    }

    // o0: chain = filter(amount>=0)(wideA) -> select[id,active,created,grade,amount,label] -> map_id
    // -> groupBy(created).sum(amount) = Σ amount*(grade+1) by created -> [created(Date), sum0]
    private static List<Object[]> o0(List<Object[]> a, List<Object[]> labelDim) {
        Map<Object, List<Object[]>> ldIndex = OracleTyped01.indexByCol(labelDim, LD_LABEL);
        Map<OracleTyped01.Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] r : a) {
            BigDecimal amt = (BigDecimal) r[W_AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;  // filter(amount>=0) at the head of the chain
            }
            Object label = r[W_LABEL];
            if (label == null) {
                continue;  // joined on label, where null does not match
            }
            List<Object[]> matches = ldIndex.get(label);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            Integer grade = (Integer) r[W_GRADE];
            int copies = (grade == null ? 0 : grade) + 1;
            if (copies <= 0) {
                continue;
            }
            // one copy per labelDim match, times the copies, then summed per created Date.
            int total = copies * matches.size();
            OracleTyped01.Key k = new OracleTyped01.Key(r[W_CREATED]);
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

    // o1: combineGroup -> groupBy(dept).sum(amount)(wideA.ORDER6) -> orderBy(sum desc, dept asc) -> top15
    // -> [dept,sum0]
    private static List<Object[]> o1(List<Object[]> a) {
        Map<OracleTyped01.Key, BigDecimal> sums = OracleTyped01.sumByDept(OracleTyped02.order6(a));
        List<Object[]> grouped = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, BigDecimal> e : sums.entrySet()) {
            grouped.add(new Object[] { e.getKey().value, OracleTyped01.scale(e.getValue()) });
        }
        grouped.sort((x, y) -> {
            int c = ((BigDecimal) y[1]).compareTo((BigDecimal) x[1]); // sum desc
            if (c != 0) {
                return c;
            }
            return OracleTyped04.compareIntNullsLast(x[0], y[0]); // dept asc NULLS_LAST
        });
        return OracleTyped01.firstN(grouped, 15);
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

    // o3: distinct(dept,active,created,tstamp,label)(wideA) -> select ORDER6 -> combineReduceGroup=sum(amount) by dept
    private static List<Object[]> o3(List<Object[]> a) {
        Map<OracleTyped01.KeyN, Object[]> seen = new LinkedHashMap<>();
        for (Object[] r : a) {
            OracleTyped01.KeyN k = new OracleTyped01.KeyN(new Object[] {
                    r[W_DEPT], r[W_ACTIVE], r[W_CREATED], r[W_TSTAMP], r[W_LABEL] });
            Object[] cur = seen.get(k);
            if (cur == null || OracleTyped03.compareLong(r[W_ID], cur[W_ID]) < 0) {
                seen.put(k, r);
            }
        }
        List<Object[]> order6 = new ArrayList<>();
        for (Object[] r : seen.values()) {
            order6.add(new Object[] { r[0], r[1], r[2], r[3], r[4], r[5] });
        }
        Map<OracleTyped01.Key, BigDecimal> sums = OracleTyped01.sumByDept(order6);
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, BigDecimal> e : sums.entrySet()) {
            res.add(new Object[] { e.getKey().value, OracleTyped01.scale(e.getValue()) });
        }
        return res;
    }

    // o4: union(union(leftJoin(wideA.ORDER6,dim), rightJoin(wideB.ORDER6,dim)), fullJoin(wideA.ORDER6,dim))
    // -> distinct(id,dept) -> groupBy(dept).max/min/count -> [dept,max0,min1,count2]
    private static List<Object[]> o4(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        List<Object[]> aOrder = OracleTyped02.order6(a);
        List<Object[]> bOrder = OracleTyped02.order6(b);
        List<Object[]> u = new ArrayList<>();
        OracleTyped01.leftJoinDim(aOrder, dim, u);
        OracleTyped01.rightJoinDim(bOrder, dim, u);
        OracleTyped01.fullJoinDim(aOrder, dim, u);
        // distinct(id,dept): keeps the first occurrence as the representative whole row, 8 columns wide.
        Map<OracleTyped01.Key2, Object[]> seen = new LinkedHashMap<>();
        for (Object[] row : u) {
            OracleTyped01.Key2 k = new OracleTyped01.Key2(row[O_ID], row[O_DEPT]);
            if (!seen.containsKey(k)) {
                seen.put(k, row);
            }
        }
        return OracleTyped01.groupMaxMinCountByDept(seen.values());
    }
}

