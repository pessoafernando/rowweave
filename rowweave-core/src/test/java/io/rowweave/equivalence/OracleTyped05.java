package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTyped05 {

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

    private OracleTyped05() { }

    public static List<List<Object[]>> compute(List<Object[]> wideA,
                                               List<Object[]> wideB,
                                               List<Object[]> supp,
                                               List<Object[]> dim) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(o0(wideA, wideB));
        out.add(o1(wideA));
        out.add(o2(wideA, supp));
        out.add(o3(wideA, wideB, dim));
        out.add(o4(wideA, wideB));
        return out;
    }

    // o0: union(wideA,wideB) -> filter(amount>=0) -> [id,dept,label,active,amount]
    // -> groupBy(label).sum(amount) -> [label(String), sum0]
    private static List<Object[]> o0(List<Object[]> a, List<Object[]> b) {
        Map<OracleTyped01.Key, BigDecimal> sums = new LinkedHashMap<>();
        sumByLabel(a, sums);
        sumByLabel(b, sums);
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<OracleTyped01.Key, BigDecimal> e : sums.entrySet()) {
            res.add(new Object[] { e.getKey().value, OracleTyped01.scale(e.getValue()) });
        }
        return res;
    }

    private static void sumByLabel(List<Object[]> rows, Map<OracleTyped01.Key, BigDecimal> sums) {
        for (Object[] r : rows) {
            BigDecimal amt = (BigDecimal) r[W_AMOUNT];
            if (amt != null && amt.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }
            OracleTyped01.Key k = new OracleTyped01.Key(r[W_LABEL]);
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

    // -> distinct(active,label,dept) -> select[id,dept,amount]
    private static List<Object[]> o1(List<Object[]> a) {
        List<Object[]> sorted = new ArrayList<>(a);
        sorted.sort((x, y) -> {
            int c = compareDateNullsLastDesc(x[W_CREATED], y[W_CREATED]); // created desc
            if (c != 0) {
                return c;
            }
            return OracleTyped03.compareLong(x[W_ID], y[W_ID]);  // id ascending, for a deterministic tie-break
        });
        List<Object[]> top = OracleTyped01.firstN(sorted, 300);
        // flatMap copies (grade+1) times; distinct(active,label,dept) keeps the representative whole row, the first in id order
        Map<OracleTyped01.KeyN, Object[]> seen = new LinkedHashMap<>();
        for (Object[] r : top) {
            Integer grade = (Integer) r[W_GRADE];
            int copies = (grade == null ? 0 : grade) + 1;
            if (copies <= 0) {
                continue;
            }
            OracleTyped01.KeyN k = new OracleTyped01.KeyN(new Object[] { r[W_ACTIVE], r[W_LABEL], r[W_DEPT] });
            Object[] cur = seen.get(k);
            if (cur == null || minId(r, cur)) {
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

    /** True when r's id is strictly less than cur's id, which keeps the first occurrence in id order as the representative. */
    private static boolean minId(Object[] r, Object[] cur) {
        return OracleTyped03.compareLong(r[W_ID], cur[W_ID]) < 0;
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

    // o3: union(leftJoin(wideA.ORDER6,dim), rightJoin(wideB.ORDER6,dim)) -> inner join wideA.ORDER6 on id
    private static List<Object[]> o3(List<Object[]> a, List<Object[]> b, List<Object[]> dim) {
        List<Object[]> aOrder = OracleTyped02.order6(a);
        List<Object[]> u = new ArrayList<>();
        OracleTyped01.leftJoinDim(aOrder, dim, u);
        OracleTyped01.rightJoinDim(OracleTyped02.order6(b), dim, u);
        Map<Object, List<Object[]>> aIdIndex = OracleTyped01.indexByCol(aOrder, O_ID);
        // inner join u on id with wideA.ORDER6: the output rows are grouped by the u-side dept, taking amount from u's idx4.
        List<Object[]> joined = new ArrayList<>();
        for (Object[] lu : u) {
            Object id = lu[O_ID];
            if (id == null) {
                continue;
            }
            List<Object[]> matches = aIdIndex.get(id);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            for (int i = 0; i < matches.size(); i++) {
                joined.add(lu);  // grouping and aggregation use only u's dept (idx1) and amount (idx4), one row per match
            }
        }
        return OracleTyped01.groupMaxMinCountByDept(joined);
    }

    // o4: cg = sum(wideA) by dept; cr = sum(wideB) by dept; fullJoin
    private static List<Object[]> o4(List<Object[]> a, List<Object[]> b) {
        Map<OracleTyped01.Key, BigDecimal> cg = OracleTyped01.sumByDept(OracleTyped02.order6(a));
        Map<OracleTyped01.Key, BigDecimal> cr = OracleTyped01.sumByDept(OracleTyped02.order6(b));
        return OracleTyped01.fullJoinSums(cg, cr);
    }

    /** Date descending by getTime, with null still sorted last. */
    static int compareDateNullsLastDesc(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;
        }
        if (b == null) {
            return -1;
        }
        return Long.compare(((Date) b).getTime(), ((Date) a).getTime());
    }
}

