package io.rowweave.examples.scenariolab;

import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Compares scenario results, either as a multiset, which is the default because the engine
 * does not promise row order, or position by position for orderBy and topN scenarios. On
 * failure it reports a diff with evidence: the count difference plus samples of missing and
 * extra rows.
 */
public final class RowCompare {

    private RowCompare() {
    }

    /** Comparison result: when pass is false, message carries a readable diff. */
    public static Result compare(DataSchema meta, List<DataRow> expected, List<DataRow> actual, boolean ordered) {
        if (ordered) {
            return compareOrdered(expected, actual);
        }
        return compareMultiset(expected, actual);
    }

    private static Result compareOrdered(List<DataRow> expected, List<DataRow> actual) {
        int n = Math.min(expected.size(), actual.size());
        for (int i = 0; i < n; i++) {
            RowKey ek = RowKey.of(expected.get(i));
            RowKey ak = RowKey.of(actual.get(i));
            if (!ek.equals(ak)) {
                return Result.fail("row " + i + " mismatch: expected " + ek + " but got " + ak
                        + " (expected rows=" + expected.size() + ", actual rows=" + actual.size() + ")");
            }
        }
        if (expected.size() != actual.size()) {
            return Result.fail("row count mismatch: expected " + expected.size()
                    + " rows but got " + actual.size()
                    + "; first " + n + " rows matched");
        }
        return Result.pass();
    }

    private static Result compareMultiset(List<DataRow> expected, List<DataRow> actual) {
        Map<RowKey, Integer> exp = counts(expected);
        Map<RowKey, Integer> act = counts(actual);
        List<RowKey> missing = new ArrayList<>();
        List<RowKey> extra = new ArrayList<>();
        for (Map.Entry<RowKey, Integer> e : exp.entrySet()) {
            int a = act.getOrDefault(e.getKey(), 0);
            if (a < e.getValue()) {
                for (int i = 0; i < e.getValue() - a; i++) {
                    missing.add(e.getKey());
                }
            }
        }
        for (Map.Entry<RowKey, Integer> e : act.entrySet()) {
            int x = exp.getOrDefault(e.getKey(), 0);
            if (x < e.getValue()) {
                for (int i = 0; i < e.getValue() - x; i++) {
                    extra.add(e.getKey());
                }
            }
        }
        if (missing.isEmpty() && extra.isEmpty()) {
            return Result.pass();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("multiset mismatch: expected ").append(expected.size())
                .append(" rows, got ").append(actual.size())
                .append(", missing ").append(missing.size()).append(", extra ").append(extra.size());
        for (int i = 0; i < Math.min(5, missing.size()); i++) {
            sb.append("\n  - missing: ").append(missing.get(i));
        }
        for (int i = 0; i < Math.min(5, extra.size()); i++) {
            sb.append("\n  + extra:   ").append(extra.get(i));
        }
        return Result.fail(sb.toString());
    }

    private static Map<RowKey, Integer> counts(List<DataRow> rows) {
        Map<RowKey, Integer> m = new HashMap<>();
        for (DataRow r : rows) {
            m.merge(RowKey.of(r), 1, Integer::sum);
        }
        return m;
    }

    /** Render a group of rows readably, for the report. */
    public static String render(DataSchema meta, List<DataRow> rows, int max) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(max, rows.size()); i++) {
            DataRow r = rows.get(i);
            sb.append("  row").append(i).append(": [");
            for (int c = 0; c < meta.getFieldCount(); c++) {
                if (c > 0) {
                    sb.append(", ");
                }
                sb.append(r.get(c));
            }
            sb.append("]\n");
        }
        return sb.toString();
    }

    /** A simple expected-result container: oracle rows usually group in LinkedHashMap key order. */
    public static List<DataRow> rowsOf(int width, List<Object[]> vals) {
        List<DataRow> out = new ArrayList<>(vals.size());
        for (Object[] v : vals) {
            DataRow r = new DataRow(width);
            for (int i = 0; i < width; i++) {
                r.set(i, v[i]);
            }
            out.add(r);
        }
        return out;
    }

    /** Grouping that preserves first-appearance order, used by the oracle. */
    public static class Grouped {
        private final Map<List<Object>, List<DataRow>> groups = new LinkedHashMap<>();

        public void add(DataRow row, int[] keyIdx) {
            List<Object> key = new ArrayList<>(keyIdx.length);
            for (int i : keyIdx) {
                key.add(row.get(i));
            }
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        public Map<List<Object>, List<DataRow>> groups() {
            return groups;
        }
    }

    /** BigDecimal accumulates at full precision and is put through setScale(10, HALF_UP) once, before output; with no non-null input at all the result is null. */
    public static BigDecimal bdSum(List<DataRow> rows, int idx) {
        BigDecimal total = null;
        for (DataRow r : rows) {
            BigDecimal v = r.getBigDecimal(idx);
            if (v != null) {
                total = (total == null) ? BigDecimal.ZERO : total;
                total = total.add(v);
            }
        }
        return total == null ? null : total.setScale(10, java.math.RoundingMode.HALF_UP);
    }

    /** Value list for a field with no decimal precision requirement, such as String. */
    public static boolean hasNonNull(List<DataRow> rows, int idx) {
        for (DataRow r : rows) {
            if (r.get(idx) != null) {
                return true;
            }
        }
        return false;
    }

    /** The Result type. */
    public static final class Result {
        public final boolean pass;
        public final String message;

        private Result(boolean pass, String message) {
            this.pass = pass;
            this.message = message;
        }

        static Result pass() {
            return new Result(true, "ok");
        }

        static Result fail(String message) {
            return new Result(false, message);
        }
    }
}
