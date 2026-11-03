package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.SortField;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * **Independent plain Java operator implementations**, built only from JDK collections (List and
 * LinkedHashMap), sharing nothing with the engine's push machinery.
 *
 * <p>Purpose: the quadrant tests originally used "single machine P1" as the oracle, which is a
 * circular argument — a bug in a shared operator would be wrong in every quadrant at once. This helper
 * hand-writes each operator's semantics as an **external reference** and asserts oracle == single
 * machine P1. Since the quadrant tests already show P1 == P4 == cluster, oracle == P1 closes the loop
 * (oracle == P1 and P1 == P4 == cluster implies oracle == every quadrant).
 *
 * <p>Semantics match the engine: sum accumulates at full precision with setScale(10, HALF_UP) applied
 * once before output; count(*) counts every row while countField counts only non-null values; max and
 * min ignore null and return null for an empty group; distinct and groupBy keep first-seen order;
 * ordering uses NULLS_LAST/DEFAULT with ASC/DESC, where a null number compares as 0 and a null
 * otherwise sorts smallest; join treats null as unequal to null.
 * Result order follows the single machine P1 order (driven by source order), but the final comparison
 * uses multiset fingerprints, so order does not matter.
 */
final class OracleOps {

    private static final String SEP = "|#|"; // sep
    private static final String NULL_MARK = "@NULL@";

    private OracleOps() { }

    static List<DataRow> filter(List<DataRow> in, Predicate<DataRow> keep) {
        List<DataRow> out = new ArrayList<>();
        for (DataRow r : in) {
            if (keep.test(r)) {
                out.add(r);
            }
        }
        return out;
    }

    static List<DataRow> select(List<DataRow> in, int... idx) {
        List<DataRow> out = new ArrayList<>(in.size());
        for (DataRow r : in) {
            DataRow o = new DataRow(idx.length);
            for (int i = 0; i < idx.length; i++) {
                o.set(i, r.get(idx[i]));
            }
            out.add(o);
        }
        return out;
    }

    private static String keyOf(DataRow r, int[] keyIdx) {
        StringBuilder sb = new StringBuilder();
        for (int k : keyIdx) {
            Object v = r.get(k);
            sb.append(v == null ? NULL_MARK : v.toString()).append(SEP);
        }
        return sb.toString();
    }

    private static DataRow keyPrefixRow(DataRow r, int[] keyIdx, int aggCols) {
        DataRow o = new DataRow(keyIdx.length + aggCols);
        for (int i = 0; i < keyIdx.length; i++) {
            o.set(i, r.get(keyIdx[i]));
        }
        return o;
    }

    static List<DataRow> groupBySum(List<DataRow> in, int[] keyIdx, int measureIdx) {
        Map<String, DataRow> tpl = new LinkedHashMap<>();
        Map<String, BigDecimal> sums = new LinkedHashMap<>();
        Map<String, Boolean> any = new LinkedHashMap<>();
        for (DataRow r : in) {
            String key = keyOf(r, keyIdx);
            tpl.putIfAbsent(key, keyPrefixRow(r, keyIdx, 1));
            Object raw = r.get(measureIdx);
            BigDecimal v = raw == null ? null : toBig(raw);
            if (v != null) {
                sums.merge(key, v, BigDecimal::add);
                any.put(key, true);
            } else {
                any.putIfAbsent(key, false);
            }
        }
        List<DataRow> out = new ArrayList<>();
        for (Map.Entry<String, DataRow> e : tpl.entrySet()) {
            DataRow o = e.getValue();
            BigDecimal s = sums.get(e.getKey());
            o.set(keyIdx.length, Boolean.TRUE.equals(any.get(e.getKey())) && s != null
                    ? s.setScale(10, RoundingMode.HALF_UP) : null);
            out.add(o);
        }
        return out;
    }

    static List<DataRow> groupByCount(List<DataRow> in, int[] keyIdx) {
        Map<String, DataRow> tpl = new LinkedHashMap<>();
        Map<String, Long> cnt = new LinkedHashMap<>();
        for (DataRow r : in) {
            String key = keyOf(r, keyIdx);
            tpl.putIfAbsent(key, keyPrefixRow(r, keyIdx, 1));
            cnt.merge(key, 1L, Long::sum);
        }
        List<DataRow> out = new ArrayList<>();
        for (Map.Entry<String, DataRow> e : tpl.entrySet()) {
            DataRow o = e.getValue();
            o.set(keyIdx.length, cnt.get(e.getKey()));
            out.add(o);
        }
        return out;
    }

    static List<DataRow> groupByCountField(List<DataRow> in, int[] keyIdx, int measureIdx) {
        Map<String, DataRow> tpl = new LinkedHashMap<>();
        Map<String, Long> cnt = new LinkedHashMap<>();
        for (DataRow r : in) {
            String key = keyOf(r, keyIdx);
            tpl.putIfAbsent(key, keyPrefixRow(r, keyIdx, 1));
            cnt.putIfAbsent(key, 0L);
            if (r.get(measureIdx) != null) {
                cnt.merge(key, 1L, Long::sum);
            }
        }
        List<DataRow> out = new ArrayList<>();
        for (Map.Entry<String, DataRow> e : tpl.entrySet()) {
            DataRow o = e.getValue();
            o.set(keyIdx.length, cnt.get(e.getKey()));
            out.add(o);
        }
        return out;
    }

    static List<DataRow> groupByExtreme(List<DataRow> in, int[] keyIdx, int measureIdx, boolean max) {
        Map<String, DataRow> tpl = new LinkedHashMap<>();
        Map<String, Object> best = new LinkedHashMap<>();
        for (DataRow r : in) {
            String key = keyOf(r, keyIdx);
            tpl.putIfAbsent(key, keyPrefixRow(r, keyIdx, 1));
            Object v = r.get(measureIdx);
            if (v == null) {
                best.putIfAbsent(key, null);
                continue;
            }
            Object cur = best.get(key);
            if (cur == null) {
                best.put(key, v);
            } else {
                int c = cmpValues(cur, v);
                if ((max && c < 0) || (!max && c > 0)) {
                    best.put(key, v);
                }
            }
        }
        List<DataRow> out = new ArrayList<>();
        for (Map.Entry<String, DataRow> e : tpl.entrySet()) {
            DataRow o = e.getValue();
            o.set(keyIdx.length, best.get(e.getKey()));
            out.add(o);
        }
        return out;
    }

    static List<DataRow> distinct(List<DataRow> in, int... keyIdx) {
        Map<String, DataRow> seen = new LinkedHashMap<>();
        for (DataRow r : in) {
            seen.putIfAbsent(keyOf(r, keyIdx), r);
        }
        return new ArrayList<>(seen.values());
    }

    static List<DataRow> orderBy(List<DataRow> in, DataSchema meta, SortField... sf) {
        List<DataRow> out = new ArrayList<>(in);
        out.sort((a, b) -> compareRows(a, b, meta, sf));
        return out;
    }

    static List<DataRow> top(List<DataRow> in, DataSchema meta, int n, SortField... sf) {
        List<DataRow> sorted = orderBy(in, meta, sf);
        return new ArrayList<>(sorted.subList(0, Math.min(n, sorted.size())));
    }

    static List<DataRow> joinInner(List<DataRow> left, int[] lk, List<DataRow> right, int[] rk) {
        Map<String, List<DataRow>> idx = new LinkedHashMap<>();
        for (DataRow r : right) {
            if (hasNullKey(r, rk)) {
                continue;
            }
            idx.computeIfAbsent(joinKey(r, rk), x -> new ArrayList<>()).add(r);
        }
        List<DataRow> out = new ArrayList<>();
        for (DataRow l : left) {
            if (hasNullKey(l, lk)) {
                continue;
            }
            List<DataRow> matches = idx.get(joinKey(l, lk));
            if (matches == null) {
                continue;
            }
            for (DataRow r : matches) {
                DataRow o = new DataRow(l.size() + r.size());
                for (int i = 0; i < l.size(); i++) {
                    o.set(i, l.get(i));
                }
                for (int i = 0; i < r.size(); i++) {
                    o.set(l.size() + i, r.get(i));
                }
                out.add(o);
            }
        }
        return out;
    }

    static List<DataRow> union(List<DataRow> a, List<DataRow> b) {
        List<DataRow> out = new ArrayList<>(a.size() + b.size());
        out.addAll(a);
        out.addAll(b);
        return out;
    }

    private static boolean hasNullKey(DataRow r, int[] keyIdx) {
        for (int k : keyIdx) {
            if (r.get(k) == null) {
                return true;
            }
        }
        return false;
    }

    private static String joinKey(DataRow r, int[] keyIdx) {
        StringBuilder sb = new StringBuilder();
        for (int k : keyIdx) {
            sb.append(r.get(k).toString()).append(SEP);
        }
        return sb.toString();
    }

    private static int compareRows(DataRow a, DataRow b, DataSchema meta, SortField[] sf) {
        for (SortField f : sf) {
            int idx = f.getFieldIndex();
            Object av = a.get(idx);
            Object bv = b.get(idx);
            DataType type = meta.getField(idx).getDataType();
            boolean desc = f.getDirection() == SortField.Direction.DESC;
            int c;
            switch (f.getNullOrder()) {
                case NULLS_FIRST:
                    c = absoluteNull(av, bv, true, desc, type);
                    break;
                case NULLS_LAST:
                    c = absoluteNull(av, bv, false, desc, type);
                    break;
                default:
                    c = valueBasedNull(av, bv, desc, type);
                    break;
            }
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private static int absoluteNull(Object av, Object bv, boolean nullsFirst, boolean desc, DataType type) {
        if (av == null && bv == null) {
            return 0;
        }
        if (av == null) {
            return nullsFirst ? -1 : 1;
        }
        if (bv == null) {
            return nullsFirst ? 1 : -1;
        }
        int cmp = cmpNonNull(av, bv, type);
        return desc ? -cmp : cmp;
    }

    private static int valueBasedNull(Object av, Object bv, boolean desc, DataType type) {
        int cmp;
        if (isNumeric(type)) {
            BigDecimal an = av == null ? BigDecimal.ZERO : toBig(av);
            BigDecimal bn = bv == null ? BigDecimal.ZERO : toBig(bv);
            cmp = an.compareTo(bn);
        } else if (av == null && bv == null) {
            cmp = 0;
        } else if (av == null) {
            cmp = -1;
        } else if (bv == null) {
            cmp = 1;
        } else {
            cmp = cmpNonNull(av, bv, type);
        }
        return desc ? -cmp : cmp;
    }

    private static boolean isNumeric(DataType type) {
        return type == DataType.IntegerType || type == DataType.LongType || type == DataType.BigDecimalType;
    }

    private static BigDecimal toBig(Object v) {
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        if (v instanceof Integer || v instanceof Long) {
            return BigDecimal.valueOf(((Number) v).longValue());
        }
        return new BigDecimal(v.toString());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int cmpNonNull(Object av, Object bv, DataType type) {
        if (isNumeric(type)) {
            return toBig(av).compareTo(toBig(bv));
        }
        return ((Comparable) av).compareTo(bv);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int cmpValues(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return toBig(a).compareTo(toBig(b));
        }
        return ((Comparable) a).compareTo(b);
    }
}

