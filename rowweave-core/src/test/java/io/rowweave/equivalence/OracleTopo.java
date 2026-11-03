package io.rowweave.equivalence;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OracleTopo {

    private static final int ID = 0;
    private static final int DEPT = 1;
    private static final int REGION = 2;
    private static final int GRADE = 3;
    private static final int AMOUNT = 4;
    private static final int QTY = 5;

    private static final int DIM_DEPT = 0;
    private static final int DIM_BONUS = 1;

    private OracleTopo() { }

    // ================= topo01: source wired straight to output =================
    public static List<List<Object[]>> topo01(List<Object[]> ordersA) {
        return one(copyAll(ordersA));
    }

    // ================= topo02: filter(amount>=100) -> map_id =================
    public static List<List<Object[]>> topo02(List<Object[]> ordersA) {
        List<Object[]> res = new ArrayList<>();
        for (Object[] r : ordersA) {
            if (ge100(r[AMOUNT])) {
                res.add(order6(r));
            }
        }
        return one(res);
    }

    // ================= topo03: groupBy(dept).sum(amount) -> [dept,sum0] =================
    public static List<List<Object[]>> topo03(List<Object[]> ordersA) {
        return one(sumByCol(ordersA, DEPT));
    }

    // ================= topo04: A join B on id -> 12 columns =================
    public static List<List<Object[]>> topo04(List<Object[]> ordersA, List<Object[]> ordersB) {
        return one(joinOnCol12(ordersA, ID, ordersB, ID));
    }

    // ================= topo05: union(union(union(A,B),C),supp) as a multiset union =================
    public static List<List<Object[]>> topo05(List<Object[]> ordersA, List<Object[]> ordersB,
                                              List<Object[]> ordersC, List<Object[]> supp) {
        List<Object[]> res = new ArrayList<>();
        addOrder6(res, ordersA);
        addOrder6(res, ordersB);
        addOrder6(res, ordersC);
        addOrder6(res, supp);
        return one(res);
    }

    // ================= topo06: A join dim on dept -> groupBy(dept).sum(amount).count() =================
    public static List<List<Object[]>> topo06(List<Object[]> ordersA, List<Object[]> dim) {
        List<Object[]> j = joinDim8(ordersA, dim);
        // groupBy(dept idx1).sum(amount idx4).count -> [dept, sum, count]
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        Map<Key, Long> counts = new LinkedHashMap<>();
        for (Object[] row : j) {
            Key k = new Key(row[DEPT]);
            accSum(sums, k, (BigDecimal) row[AMOUNT]);
            counts.put(k, counts.getOrDefault(k, 0L) + 1L);
        }
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : sums.entrySet()) {
            res.add(new Object[] { e.getKey().value, scale(e.getValue()), counts.get(e.getKey()) });
        }
        return one(res);
    }

    public static List<List<Object[]>> topo07(List<Object[]> ordersA) {
        return one(joinOnCol12(ordersA, ID, ordersA, ID));
    }

    // ================= topo08: 3 out(fan-out)=================
    public static List<List<Object[]>> topo08(List<Object[]> ordersA) {
        List<List<Object[]>> out = new ArrayList<>(3);
        out.add(sumByCol(ordersA, DEPT));                                // o0 [dept,sum0]
        out.add(distinctRepr(ordersA, new int[] { DEPT, REGION }));  // o1's distinct(dept,region) representative whole row
        out.add(topByKey(ordersA, ID, true, 50));                        // o2 orderBy(id asc) top50
        return out;
    }

    // ================= topo09: (A join B on id).select(ORDER6) union (C union supp) =================
    public static List<List<Object[]>> topo09(List<Object[]> ordersA, List<Object[]> ordersB,
                                              List<Object[]> ordersC, List<Object[]> supp) {
        // joined.select(ORDER6) is the 6 A-side columns, one row per id at 1:1
        List<Object[]> joinedSel = joinSelectLeft6(ordersA, ID, ordersB, ID);
        List<Object[]> res = new ArrayList<>();
        res.addAll(joinedSel);
        addOrder6(res, ordersC);
        addOrder6(res, supp);
        return one(res);
    }

    // ================= topo10: coGroup(A,supp) on dept -> [dept,lc,rc] =================
    public static List<List<Object[]>> topo10(List<Object[]> ordersA, List<Object[]> supp) {
        return one(coGroup(ordersA, supp));
    }

    // ================= topo11: j1=(A join B).select6; g1=sum(dept); j2=g1 join dim; groupBy max(sum) =================
    public static List<List<Object[]>> topo11(List<Object[]> ordersA, List<Object[]> ordersB,
                                              List<Object[]> dim) {
        List<Object[]> j1 = joinSelectLeft6(ordersA, ID, ordersB, ID);       // ORDER6
        List<Object[]> g1 = sumByCol(j1, DEPT);                              // [dept, sum]
        // j2 = g1 join dim on dept -> [dept, sum, dept, bonus] (4 columns), with g1.dept at idx0
        Map<Object, List<Object[]>> dIdx = indexByCol(dim, DIM_DEPT);
        // groupBy(dept idx0).max(j2 idx1 = sum)
        Map<Key, BigDecimal> maxes = new LinkedHashMap<>();
        for (Object[] gr : g1) {
            Object dept = gr[0];
            List<Object[]> matches = dept == null ? null : dIdx.get(dept);
            if (matches == null || matches.isEmpty()) {
                continue; // inner join
            }
            BigDecimal sum = (BigDecimal) gr[1];
            for (int i = 0; i < matches.size(); i++) {
                Key k = new Key(dept);
                BigDecimal cur = maxes.get(k);
                if (cur == null || sum.compareTo(cur) > 0) {
                    maxes.put(k, sum);
                }
            }
        }
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : maxes.entrySet()) {
            res.add(new Object[] { e.getKey().value, scale(e.getValue()) });
        }
        return one(res);
    }

    // ================= topo12: u=union(A,B).filter(amount>=0); j=(u join C).select6.map_id;
    // out=coGroup(j,supp) on dept.filter(lc>=0) =================
    public static List<List<Object[]>> topo12(List<Object[]> ordersA, List<Object[]> ordersB,
                                              List<Object[]> ordersC, List<Object[]> supp) {
        List<Object[]> u = new ArrayList<>();
        addOrder6(u, ordersA);
        addOrder6(u, ordersB);
        // filter(amount>=0) always holds; j = (u join C on id).select(ORDER6) gives the 6 u-side columns, since each u row matches exactly one C.id
        List<Object[]> j = joinSelectLeft6(u, ID, ordersC, ID);
        List<Object[]> cg = coGroup(j, supp);
        List<Object[]> res = new ArrayList<>();
        for (Object[] row : cg) {
            if (((Integer) row[1]) >= 0) {  // lc >= 0 always holds
                res.add(row);
            }
        }
        return one(res);
    }

    public static List<List<Object[]>> topo13(List<Object[]> ordersA) {
        List<Object[]> d1 = joinSelectLeft6(ordersA, ID, ordersA, ID); // = A(1:1)
        return one(joinOnCol12(d1, ID, d1, ID));
    }

    // ================= topo14: 2 outputs. o0 sums by dept; o1 joins A to dim on dept for 8 columns =================
    public static List<List<Object[]>> topo14(List<Object[]> ordersA, List<Object[]> dim) {
        List<List<Object[]>> out = new ArrayList<>(2);
        out.add(sumByCol(ordersA, DEPT));      // o0 [dept,sum0]
        out.add(joinDim8(ordersA, dim));       // o1 [A6, dim2]
        return out;
    }

    // ================= topo15: cg=coGroup(A,supp); out joins cg to dim on dept for 5 columns =================
    public static List<List<Object[]>> topo15(List<Object[]> ordersA, List<Object[]> supp,
                                              List<Object[]> dim) {
        List<Object[]> cg = coGroup(ordersA, supp); // [dept, lc, rc],dept=idx0
        Map<Object, List<Object[]>> dIdx = indexByCol(dim, DIM_DEPT);
        List<Object[]> res = new ArrayList<>();
        for (Object[] c : cg) {
            Object dept = c[0];
            List<Object[]> matches = dept == null ? null : dIdx.get(dept);
            if (matches == null || matches.isEmpty()) {
                continue; // inner join
            }
            for (Object[] d : matches) {
                res.add(new Object[] { c[0], c[1], c[2], d[DIM_DEPT], d[DIM_BONUS] });
            }
        }
        return one(res);
    }

    // ================= topo16: 5 out(fan-out)=================
    public static List<List<Object[]>> topo16(List<Object[]> ordersA) {
        List<List<Object[]>> out = new ArrayList<>(5);
        out.add(sumByCol(ordersA, DEPT));                                       // o0 [dept,sum0]
        out.add(countByCol(ordersA, REGION));                                   // o1 [region,count0]
        out.add(distinctRepr(ordersA, new int[] { DEPT, REGION, GRADE }));  // o2's distinct representative whole row
        out.add(topByBigDecimal(ordersA, AMOUNT, false, 30));                   // o3 orderBy(amount desc) top30
        out.add(filterDeptGe25(ordersA));                                       // o4 filter(dept>=25)
        return out;
    }

    public static List<List<Object[]>> topo17(List<Object[]> ordersA) {
        // left: flatMap(grade+1) copies, then distinct over all 6 columns; every row is unique, so this folds back to A with the id-ascending first occurrence as representative
        List<Object[]> left = distinctRepr(ordersA, new int[] { ID, DEPT, REGION, GRADE, AMOUNT, QTY });
        return one(joinOnCol12(left, ID, ordersA, ID));
    }

    // ================= topo18: (A join B).select6 union C union supp =================
    public static List<List<Object[]>> topo18(List<Object[]> ordersA, List<Object[]> ordersB,
                                              List<Object[]> ordersC, List<Object[]> supp) {
        List<Object[]> j = joinSelectLeft6(ordersA, ID, ordersB, ID); // = A
        List<Object[]> res = new ArrayList<>();
        res.addAll(j);
        addOrder6(res, ordersC);
        addOrder6(res, supp);
        return one(res);
    }

    // ================= topo19: 2 outputs. g sums by dept; o0 joins g to dim for 4 columns; o1 maps g and filters dept>=0 =================
    public static List<List<Object[]>> topo19(List<Object[]> ordersA, List<Object[]> dim) {
        List<Object[]> g = sumByCol(ordersA, DEPT); // [dept, sum]
        Map<Object, List<Object[]>> dIdx = indexByCol(dim, DIM_DEPT);
        // o0: g join dim on dept -> [dept, sum, dept, bonus]
        List<Object[]> o0 = new ArrayList<>();
        for (Object[] gr : g) {
            Object dept = gr[0];
            List<Object[]> matches = dept == null ? null : dIdx.get(dept);
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            for (Object[] d : matches) {
                o0.add(new Object[] { gr[0], gr[1], d[DIM_DEPT], d[DIM_BONUS] });
            }
        }
        // o1: g.map_id.filter(dept>=0) always holds, so the result stays [dept, sum]
        List<Object[]> o1 = new ArrayList<>();
        for (Object[] gr : g) {
            if (((Integer) gr[0]) >= 0) {
                o1.add(new Object[] { gr[0], gr[1] });
            }
        }
        List<List<Object[]>> out = new ArrayList<>(2);
        out.add(o0);
        out.add(o1);
        return out;
    }

    // joined=diamond join dim on dept -> [dept,sum,dept,bonus];
    // out=groupBy(dept).sum(joined idx1=sum) -> [dept, 2*g.sum] =================
    public static List<List<Object[]>> topo20(List<Object[]> ordersA, List<Object[]> dim) {
        List<Object[]> g = sumByCol(ordersA, DEPT);  // [dept, sum], since filter amount>=0 always holds
        // diamond = g ++ g.filter(dept>=0), which always holds, giving 2 rows per dept
        List<Object[]> diamond = new ArrayList<>();
        diamond.addAll(g);
        diamond.addAll(g);
        // joined = diamond join dim on dept, which stays 2 rows per dept because dim matches 1:1
        Map<Object, List<Object[]>> dIdx = indexByCol(dim, DIM_DEPT);
        // out = groupBy(dept).sum over joined idx1, the sum column
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] dr : diamond) {
            Object dept = dr[0];
            List<Object[]> matches = dept == null ? null : dIdx.get(dept);
            if (matches == null || matches.isEmpty()) {
                continue; // inner join
            }
            BigDecimal sumVal = (BigDecimal) dr[1];
            for (int i = 0; i < matches.size(); i++) {
                accSum(sums, new Key(dept), sumVal);
            }
        }
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : sums.entrySet()) {
            res.add(new Object[] { e.getKey().value, scale(e.getValue()) });
        }
        return one(res);
    }

    // ================= topo21: a real self-join. mid=A joined to itself on id at 1:1 gives 12 columns =================
    public static List<List<Object[]>> topo21(List<Object[]> ordersA) {
        return one(joinOnCol12(ordersA, ID, ordersA, ID));
    }

    // ================= topo22: 2 outputs. o0 passes the source through unchanged; o1 filters dept>=25 =================
    public static List<List<Object[]>> topo22(List<Object[]> ordersA) {
        List<List<Object[]>> out = new ArrayList<>(2);
        out.add(copyAll(ordersA));  // o0 passes ORDER6 through unchanged
        out.add(filterDeptGe25(ordersA));    // o1 filter(dept>=25)
        return out;
    }

    // =====================================================================
    // shared helpers
    // =====================================================================

    private static List<List<Object[]>> one(List<Object[]> res) {
        List<List<Object[]>> out = new ArrayList<>(1);
        out.add(res);
        return out;
    }

    private static Object[] order6(Object[] r) {
        Object[] row = new Object[6];
        System.arraycopy(r, 0, row, 0, 6);
        return row;
    }

    private static List<Object[]> copyAll(List<Object[]> src) {
        List<Object[]> res = new ArrayList<>(src.size());
        for (Object[] r : src) {
            res.add(order6(r));
        }
        return res;
    }

    private static void addOrder6(List<Object[]> res, List<Object[]> src) {
        for (Object[] r : src) {
            res.add(order6(r));
        }
    }

    private static boolean ge100(Object amount) {
        if (amount == null) {
            return false;
        }
        return ((BigDecimal) amount).compareTo(new BigDecimal(100)) >= 0;
    }

    private static List<Object[]> filterDeptGe25(List<Object[]> src) {
        List<Object[]> res = new ArrayList<>();
        for (Object[] r : src) {
            Integer dept = (Integer) r[DEPT];
            if (dept != null && dept >= 25) {
                res.add(order6(r));
            }
        }
        return res;
    }

    /** groupBy(col).sum(amount) -> [key, sum]. A null key forms its group; a null amount counts as 0 but the group is kept. */
    private static List<Object[]> sumByCol(List<Object[]> rows, int col) {
        Map<Key, BigDecimal> sums = new LinkedHashMap<>();
        for (Object[] r : rows) {
            accSum(sums, new Key(r[col]), (BigDecimal) r[AMOUNT]);
        }
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, BigDecimal> e : sums.entrySet()) {
            res.add(new Object[] { e.getKey().value, scale(e.getValue()) });
        }
        return res;
    }

    /** groupBy(col).count() -> [key, count(Long)]. */
    private static List<Object[]> countByCol(List<Object[]> rows, int col) {
        Map<Key, Long> counts = new LinkedHashMap<>();
        for (Object[] r : rows) {
            Key k = new Key(r[col]);
            counts.put(k, counts.getOrDefault(k, 0L) + 1L);
        }
        List<Object[]> res = new ArrayList<>();
        for (Map.Entry<Key, Long> e : counts.entrySet()) {
            res.add(new Object[] { e.getKey().value, e.getValue() });
        }
        return res;
    }

    private static void accSum(Map<Key, BigDecimal> sums, Key k, BigDecimal amt) {
        BigDecimal acc = sums.get(k);
        if (amt == null) {
            if (acc == null) {
                sums.put(k, BigDecimal.ZERO);
            }
        } else {
            sums.put(k, acc == null ? amt : acc.add(amt));
        }
    }

    /** An inner join emitting left6 ++ right6 = 12 columns. A null join key never matches. */
    private static List<Object[]> joinOnCol12(List<Object[]> left, int lk,
                                              List<Object[]> right, int rk) {
        Map<Object, List<Object[]>> rIdx = indexByCol(right, rk);
        List<Object[]> res = new ArrayList<>();
        for (Object[] l : left) {
            Object key = l[lk];
            if (key == null) {
                continue;
            }
            List<Object[]> matches = rIdx.get(key);
            if (matches == null) {
                continue;
            }
            for (Object[] r : matches) {
                Object[] row = new Object[12];
                System.arraycopy(l, 0, row, 0, 6);
                System.arraycopy(r, 0, row, 6, 6);
                res.add(row);
            }
        }
        return res;
    }

    /** An inner join on the key that selects the left ORDER6, one row per matched pair. */
    private static List<Object[]> joinSelectLeft6(List<Object[]> left, int lk,
                                                  List<Object[]> right, int rk) {
        Map<Object, List<Object[]>> rIdx = indexByCol(right, rk);
        List<Object[]> res = new ArrayList<>();
        for (Object[] l : left) {
            Object key = l[lk];
            if (key == null) {
                continue;
            }
            List<Object[]> matches = rIdx.get(key);
            if (matches == null) {
                continue;
            }
            for (int i = 0; i < matches.size(); i++) {
                res.add(order6(l));
            }
        }
        return res;
    }

    /** A join dim on dept -> 8 columns: [A6, dim.dept, dim.bonus]. */
    private static List<Object[]> joinDim8(List<Object[]> orders, List<Object[]> dim) {
        Map<Object, List<Object[]>> dIdx = indexByCol(dim, DIM_DEPT);
        List<Object[]> res = new ArrayList<>();
        for (Object[] r : orders) {
            Object dept = r[DEPT];
            if (dept == null) {
                continue;
            }
            List<Object[]> matches = dIdx.get(dept);
            if (matches == null) {
                continue;
            }
            for (Object[] d : matches) {
                Object[] row = new Object[8];
                System.arraycopy(r, 0, row, 0, 6);
                row[6] = d[DIM_DEPT];
                row[7] = d[DIM_BONUS];
                res.add(row);
            }
        }
        return res;
    }

    private static List<Object[]> coGroup(List<Object[]> left, List<Object[]> right) {
        Map<Key, int[]> counts = new LinkedHashMap<>(); // dept -> {lc, rc}
        for (Object[] r : left) {
            Key k = new Key(r[DEPT]);
            int[] c = counts.get(k);
            if (c == null) {
                c = new int[2];
                counts.put(k, c);
            }
            c[0]++;
        }
        for (Object[] r : right) {
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
            res.add(new Object[] { e.getKey().value, e.getValue()[0], e.getValue()[1] });
        }
        return res;
    }

    /** distinct(cols): keeps the representative whole row of each unique combination, the first in id order. */
    private static List<Object[]> distinctRepr(List<Object[]> rows, int[] cols) {
        // The representative is the first occurrence in ascending id order, which is deterministic because id is unique; a null id counts as largest and sorts last.
        List<Object[]> sorted = new ArrayList<>(rows);
        sorted.sort((x, y) -> compareLongNullsLast((Long) x[ID], (Long) y[ID]));
        Map<KeyN, Object[]> seen = new LinkedHashMap<>();
        for (Object[] r : sorted) {
            Object[] key = new Object[cols.length];
            for (int i = 0; i < cols.length; i++) {
                key[i] = r[cols[i]];
            }
            KeyN k = new KeyN(key);
            if (!seen.containsKey(k)) {
                seen.put(k, order6(r));
            }
        }
        return new ArrayList<>(seen.values());
    }

    /** orderBy(col asc/desc) top n over a numeric column, with ascending id as the tie-break. Returns ORDER6. */
    private static List<Object[]> topByKey(List<Object[]> rows, int col, boolean asc, int n) {
        List<Object[]> sorted = new ArrayList<>(rows);
        sorted.sort((x, y) -> {
            int c = compareLongNullsLast((Long) x[col], (Long) y[col]);
            if (!asc) {
                c = -c;
            }
            if (c != 0) {
                return c;
            }
            return compareLongNullsLast((Long) x[ID], (Long) y[ID]);  // ascending id as the tie-break
        });
        return firstN6(sorted, n);
    }

    /** orderBy(BigDecimal col asc/desc) top n, with ascending id as the tie-break. Returns ORDER6. */
    private static List<Object[]> topByBigDecimal(List<Object[]> rows, int col, boolean asc, int n) {
        List<Object[]> sorted = new ArrayList<>(rows);
        sorted.sort((x, y) -> {
            int c = compareBigDecNullsLast((BigDecimal) x[col], (BigDecimal) y[col]);
            if (!asc) {
                c = -c;
            }
            if (c != 0) {
                return c;
            }
            return compareLongNullsLast((Long) x[ID], (Long) y[ID]);  // ascending id as the tie-break
        });
        return firstN6(sorted, n);
    }

    private static List<Object[]> firstN6(List<Object[]> sorted, int n) {
        List<Object[]> res = new ArrayList<>();
        int limit = Math.min(n, sorted.size());
        for (int i = 0; i < limit; i++) {
            res.add(order6(sorted.get(i)));
        }
        return res;
    }

    private static Map<Object, List<Object[]>> indexByCol(List<Object[]> rows, int col) {
        Map<Object, List<Object[]>> idx = new LinkedHashMap<>();
        for (Object[] r : rows) {
            Object v = r[col];
            if (v == null) {
                continue;  // a null join key is left out of the match index
            }
            idx.computeIfAbsent(v, kk -> new ArrayList<>()).add(r);
        }
        return idx;
    }

    private static BigDecimal scale(BigDecimal v) {
        return v.setScale(10, RoundingMode.HALF_UP);
    }

    /** Long ascending, NULLS_LAST. */
    private static int compareLongNullsLast(Long a, Long b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;
        }
        if (b == null) {
            return -1;
        }
        return a.compareTo(b);
    }

    /** BigDecimal ascending, NULLS_LAST. */
    private static int compareBigDecNullsLast(BigDecimal a, BigDecimal b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;
        }
        if (b == null) {
            return -1;
        }
        return a.compareTo(b);
    }

    /** A single-value grouping key; null forms its own group. */
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

    /** An n-value grouping key for a multi-column distinct; null forms its own group. */
    private static final class KeyN {
        final Object[] vals;

        KeyN(Object[] vals) {
            this.vals = vals;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof KeyN)) {
                return false;
            }
            Object[] other = ((KeyN) o).vals;
            if (vals.length != other.length) {
                return false;
            }
            for (int i = 0; i < vals.length; i++) {
                Object x = vals[i];
                Object y = other[i];
                if (x == null ? y != null : !x.equals(y)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int h = 1;
            for (Object v : vals) {
                h = 31 * h + (v == null ? 0 : v.hashCode());
            }
            return h;
        }
    }
}

