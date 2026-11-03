package io.rowweave.examples.scenariolab.batches;

import io.rowweave.DataSet;
import io.rowweave.Field;
import io.rowweave.DataRow;
import io.rowweave.examples.scenariolab.BatchDesign;
import io.rowweave.examples.scenariolab.D;
import io.rowweave.examples.scenariolab.O;
import io.rowweave.examples.scenariolab.SL;
import io.rowweave.examples.scenariolab.ScenarioDef;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Family A (B01 to B12): the pass-through primitives. filter by expression, named parameter or
 * lambda; select by index, by expression or with a rename; map; flatMap; and chains of them.
 * Each batch is designed on its own as 10 operator variants over 10 data profiles, which gives
 * 100 scenarios. Every oracle is an independent Java implementation that follows SQL
 * three-valued logic, so a comparison with null is unknown and does not pass, null propagates
 * through arithmetic, and division lands at scale 10 HALF_UP.
 */
public final class FamilyA {

    private FamilyA() {
    }

    public static List<BatchDesign> batches() {
        return Arrays.asList(
                b01(), b02(), b03(), b04(), b05(), b06(),
                b07(), b08(), b09(), b10(), b11(), b12());
    }

    /** Index into the 10 data profiles of the KV schema; the names are in KV_PROFILE_NAMES. */
    static final String[] KV_PROFILES = {
            "base", "nulls", "dupes", "empty", "single",
            "negative", "scale", "unicode", "whitespace", "manyGroups"};

    static List<DataRow> kvProfile(int i) {
        switch (i) {
            case 0: return D.kvBase();
            case 1: return D.kvNulls();
            case 2: return D.kvDupes();
            case 3: return D.empty();
            case 4: return D.single();
            case 5: return D.kvNegative();
            case 6: return D.kvScale();
            case 7: return D.kvUnicode();
            case 8: return D.kvWhitespace();
            default: return D.kvManyGroups();
        }
    }

    static String id(String batch, int variant, int profile) {
        return String.format("%s-V%02d-P%02d", batch, variant + 1, profile + 1);
    }

    // ---- Small helpers for the predicate oracle (SQL three-valued logic: null does not pass) ----

    /** Numeric comparison of v against col1, with op 0 as >, 1 as >=, 2 as <, 3 as <=, 4 as = and 5 as !=. */
    static boolean vCmp(DataRow r, int op, BigDecimal c) {
        return colCmp(r, 1, op, c);
    }

    static boolean colCmp(DataRow r, int col, int op, BigDecimal c) {
        BigDecimal v = r.getBigDecimal(col);
        if (v == null) {
            return false;
        }
        int x = v.compareTo(c);
        switch (op) {
            case 0: return x > 0;
            case 1: return x >= 0;
            case 2: return x < 0;
            case 3: return x <= 0;
            case 4: return x == 0;
            default: return x != 0;
        }
    }

    static boolean strEq(DataRow r, String c) {
        Object k = r.get(0);
        return c.equals(k);
    }

    static boolean strNe(DataRow r, String c) {
        Object k = r.get(0);
        return k != null && !k.equals(c);
    }

    static String kStr(DataRow r) {
        return (String) r.get(0);
    }

    static BigDecimal kNum(DataRow r) {
        Object k = r.get(0);
        try {
            return k == null ? null : new BigDecimal((String) k);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** substr is 1-based, matching ExprFunctions. */
    static String substr(String s, int start1, Integer len) {
        int begin = Math.max(0, start1 - 1);
        if (begin >= s.length()) {
            return "";
        }
        int end = len == null ? s.length()
                : (len < 0 ? begin : Math.min(s.length(), begin + len));
        return s.substring(begin, end);
    }

    static BigDecimal lift(BigDecimal v, Function<BigDecimal, BigDecimal> fn) {
        return v == null ? null : fn.apply(v);
    }

    /** The boolean variant: null becomes false, because SQL treats unknown as not passing. */
    static boolean liftB(BigDecimal v, Predicate<BigDecimal> p) {
        return v != null && p.test(v);
    }

    // ========================================================================
    // B01 filter with a numeric expression, over 10 profiles
    // ========================================================================

    private static BatchDesign b01() {
        return new BatchDesign() {
            @Override public int batchNo() { return 1; }
            @Override public String title() { return "filter numeric expressions (10 predicates × 10 data profiles)"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] exprs = {
                        "v > 5", "v >= 0.001", "v < 0", "v <= 100", "v = 10.5",
                        "v != 0", "v + 1 > 5", "v * 2 < 10", "abs(v) >= 3", "v / 2 = 5.25"};
                List<Predicate<DataRow>> oracles = Arrays.asList(
                        r -> vCmp(r, 0, BigDecimal.valueOf(5)),
                        r -> vCmp(r, 1, new BigDecimal("0.001")),
                        r -> vCmp(r, 2, BigDecimal.ZERO),
                        r -> vCmp(r, 3, BigDecimal.valueOf(100)),
                        r -> vCmp(r, 4, new BigDecimal("10.5")),
                        r -> vCmp(r, 5, BigDecimal.ZERO),
                        r -> liftB(r.getBigDecimal(1),
                                v -> v.add(BigDecimal.ONE).compareTo(BigDecimal.valueOf(5)) > 0),
                        r -> liftB(r.getBigDecimal(1),
                                v -> v.multiply(BigDecimal.valueOf(2)).compareTo(BigDecimal.TEN) < 0),
                        r -> liftB(r.getBigDecimal(1),
                                v -> v.abs().compareTo(BigDecimal.valueOf(3)) >= 0),
                        r -> liftB(r.getBigDecimal(1),
                                v -> v.divide(BigDecimal.valueOf(2), 10, RoundingMode.HALF_UP)
                                        .compareTo(new BigDecimal("5.25")) == 0));
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < exprs.length; p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Predicate<DataRow> oracle = oracles.get(p);
                        final List<DataRow> data = kvProfile(d);
                        final String expr = exprs[p];
                        out.add(SL.sink(id("B01", p, d), "filter[" + KV_PROFILES[d] + "] " + expr,
                                D.KV, data, ds -> ds.filter(expr),
                                in -> SL.filterRows(in, oracle), false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B02 filter with a string expression, over 10 profiles
    // ========================================================================

    private static BatchDesign b02() {
        return new BatchDesign() {
            @Override public int batchNo() { return 2; }
            @Override public String title() { return "filter string expressions (10 predicates × 10 data profiles)"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] exprs = {
                        "k = 'g1'", "k != 'g1'", "contains(k, '1')", "startswith(k, 'g')",
                        "endswith(k, '2')", "lower(k) = 'g1'", "upper(k) = 'G1'", "len(k) = 2",
                        "substr(k, 1, 1) = 'g'", "k like 'g%'"};
                List<Predicate<DataRow>> oracles = Arrays.asList(
                        r -> strEq(r, "g1"),
                        r -> strNe(r, "g1"),
                        r -> kStr(r) != null && kStr(r).contains("1"),
                        r -> kStr(r) != null && kStr(r).startsWith("g"),
                        r -> kStr(r) != null && kStr(r).endsWith("2"),
                        r -> kStr(r) != null && kStr(r).toLowerCase().equals("g1"),
                        r -> kStr(r) != null && kStr(r).toUpperCase().equals("G1"),
                        r -> kStr(r) != null && kStr(r).length() == 2,
                        r -> kStr(r) != null && substr(kStr(r), 1, 1).equals("g"),
                        r -> like(kStr(r), "g%"));
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < exprs.length; p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Predicate<DataRow> oracle = oracles.get(p);
                        final List<DataRow> data = kvProfile(d);
                        final String expr = exprs[p];
                        out.add(SL.sink(id("B02", p, d), "filter[" + KV_PROFILES[d] + "] " + expr,
                                D.KV, data, ds -> ds.filter(expr),
                                in -> SL.filterRows(in, oracle), false));
                    }
                }
                return out;
            }
        };
    }

    private static boolean like(String s, String pattern) {
        if (s == null) {
            return false;
        }
        StringBuilder re = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            if (c == '%') {
                re.append(".*");
            } else if (c == '_') {
                re.append('.');
            } else {
                re.append(java.util.regex.Pattern.quote(String.valueOf(c)));
            }
        }
        return s.matches(re.toString());
    }

    // ========================================================================
    // B03 filter with three-valued logic combinations, over 10 profiles
    // ========================================================================

    private static BatchDesign b03() {
        return new BatchDesign() {
            @Override public int batchNo() { return 3; }
            @Override public String title() { return "filter three-valued logic (and/or/not/xor/is null/in) × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] exprs = {
                        "v > 5 and k = 'g1'", "v > 5 or v < 0", "not (v > 0)",
                        "v is null", "v is not null", "k is null",
                        "(v > 5 or v < 0) and k is not null", "v > 5 xor v < 0",
                        "k in ('g1', 'g3')", "k not in ('g1', 'g3')"};
                List<Predicate<DataRow>> oracles = Arrays.asList(
                        r -> vCmp(r, 0, BigDecimal.valueOf(5)) && strEq(r, "g1"),
                        r -> vCmp(r, 0, BigDecimal.valueOf(5)) || vCmp(r, 2, BigDecimal.ZERO),
                        r -> liftB(r.getBigDecimal(1), v -> v.compareTo(BigDecimal.ZERO) <= 0),
                        r -> r.get(1) == null,
                        r -> r.get(1) != null,
                        r -> r.get(0) == null,
                        r -> (vCmp(r, 0, BigDecimal.valueOf(5)) || vCmp(r, 2, BigDecimal.ZERO))
                                && r.get(0) != null,
                        r -> {
                            BigDecimal v = r.getBigDecimal(1);
                            if (v == null) {
                                return false;
                            }
                            return (v.compareTo(BigDecimal.valueOf(5)) > 0)
                                    ^ (v.compareTo(BigDecimal.ZERO) < 0);
                        },
                        r -> "g1".equals(r.get(0)) || "g3".equals(r.get(0)),
                        r -> {
                            Object k = r.get(0);
                            return k != null && !"g1".equals(k) && !"g3".equals(k);
                        });
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < exprs.length; p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Predicate<DataRow> oracle = oracles.get(p);
                        final List<DataRow> data = kvProfile(d);
                        final String expr = exprs[p];
                        out.add(SL.sink(id("B03", p, d), "filter[" + KV_PROFILES[d] + "] " + expr,
                                D.KV, data, ds -> ds.filter(expr),
                                in -> SL.filterRows(in, oracle), false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B04 filter with named parameters, over 10 parameter groups
    // ========================================================================

    private static BatchDesign b04() {
        return new BatchDesign() {
            @Override public int batchNo() { return 4; }
            @Override public String title() { return "filter named parameters (identifier resolved against fields before params) × 10 parameter sets"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] exprs = {
                        "v > thr", "v >= lo", "k = who", "v + step > 5", "contains(k, frag)",
                        "len(k) = want", "v * mult < 10", "abs(v) >= lo2", "upper(k) = up",
                        "substr(k, 1, 1) = c"};
                List<Map<String, Object>> paramSets = Arrays.asList(
                        java.util.Collections.<String, Object>singletonMap("thr", new BigDecimal("5")),
                        java.util.Collections.<String, Object>singletonMap("lo", new BigDecimal("0.001")),
                        java.util.Collections.<String, Object>singletonMap("who", "g1"),
                        java.util.Collections.<String, Object>singletonMap("step", BigDecimal.ONE),
                        java.util.Collections.<String, Object>singletonMap("frag", "1"),
                        java.util.Collections.<String, Object>singletonMap("want", new BigDecimal("2")),
                        java.util.Collections.<String, Object>singletonMap("mult", new BigDecimal("2")),
                        java.util.Collections.<String, Object>singletonMap("lo2", new BigDecimal("3")),
                        java.util.Collections.<String, Object>singletonMap("up", "G1"),
                        java.util.Collections.<String, Object>singletonMap("c", "g"));
                List<Predicate<DataRow>> oracles = Arrays.asList(
                        r -> vCmp(r, 0, new BigDecimal("5")),
                        r -> vCmp(r, 1, new BigDecimal("0.001")),
                        r -> strEq(r, "g1"),
                        r -> liftB(r.getBigDecimal(1),
                                v -> v.add(BigDecimal.ONE).compareTo(new BigDecimal("5")) > 0),
                        r -> kStr(r) != null && kStr(r).contains("1"),
                        r -> kStr(r) != null && kStr(r).length() == 2,
                        r -> liftB(r.getBigDecimal(1),
                                v -> v.multiply(BigDecimal.valueOf(2)).compareTo(BigDecimal.TEN) < 0),
                        r -> liftB(r.getBigDecimal(1),
                                v -> v.abs().compareTo(new BigDecimal("3")) >= 0),
                        r -> kStr(r) != null && kStr(r).toUpperCase().equals("G1"),
                        r -> kStr(r) != null && substr(kStr(r), 1, 1).equals("g"));
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < exprs.length; p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Predicate<DataRow> oracle = oracles.get(p);
                        final List<DataRow> data = kvProfile(d);
                        final String expr = exprs[p];
                        final Map<String, Object> params = paramSets.get(p);
                        out.add(SL.sink(id("B04", p, d), "filterParams[" + KV_PROFILES[d] + "] " + expr,
                                D.KV, data, ds -> ds.filter(expr, params),
                                in -> SL.filterRows(in, oracle), false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B05 filter with a code lambda, over 10 profiles
    // ========================================================================

    private static BatchDesign b05() {
        return new BatchDesign() {
            @Override public int batchNo() { return 5; }
            @Override public String title() { return "filter code lambdas (10 predicates × 10 data profiles)"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] names = {"v>0", "kNonNull", "kLen>=2", "vEvenIntPart", "kNumNeV",
                        "alwaysTrue", "alwaysFalse", "vScale>0", "kUpperEqLower", "vIn{10.5,42}"};
                List<Predicate<DataRow>> oracles = Arrays.asList(
                        r -> liftB(r.getBigDecimal(1), v -> v.signum() > 0),
                        r -> r.get(0) != null,
                        r -> kStr(r) != null && kStr(r).length() >= 2,
                        r -> {
                            BigDecimal v = r.getBigDecimal(1);
                            if (v == null) {
                                return false;
                            }
                            return v.setScale(0, RoundingMode.DOWN)
                                    .remainder(BigDecimal.valueOf(2)).signum() == 0;
                        },
                        r -> {
                            BigDecimal k = kNum(r);
                            BigDecimal v = r.getBigDecimal(1);
                            if (k == null || v == null) {
                                return k != null || v != null;
                            }
                            return k.compareTo(v) != 0;
                        },
                        r -> true,
                        r -> false,
                        r -> liftB(r.getBigDecimal(1), v -> v.scale() > 0),
                        r -> {
                            String k = kStr(r);
                            return k != null && k.toUpperCase().equals(k.toLowerCase());
                        },
                        r -> {
                            BigDecimal v = r.getBigDecimal(1);
                            return v != null && (v.compareTo(new BigDecimal("10.5")) == 0
                                    || v.compareTo(BigDecimal.valueOf(42)) == 0);
                        });
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < names.length; p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Predicate<DataRow> oracle = oracles.get(p);
                        final List<DataRow> data = kvProfile(d);
                        final String name = names[p];
                        out.add(SL.sink(id("B05", p, d), "filterFn[" + KV_PROFILES[d] + "] " + name,
                                D.KV, data, ds -> ds.filter(oracle::test),
                                in -> SL.filterRows(in, oracle), false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B06 select by index, 10 projections over 10 profiles
    // ========================================================================

    private static BatchDesign b06() {
        return new BatchDesign() {
            @Override public int batchNo() { return 6; }
            @Override public String title() { return "select by index (10 projections × 10 data profiles)"; }
            @Override public List<ScenarioDef> scenarios() {
                int[][] projections = {
                        {0}, {1}, {0, 1}, {1, 0}, {1, 1}, {0, 1, 0}, {1, 0, 1},
                        {0, 0, 1, 1}, {1}, {0, 1}};
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < projections.length; p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final int[] proj = projections[p];
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B06", p, d), "select" + Arrays.toString(proj)
                                        + "[" + KV_PROFILES[d] + "]",
                                D.KV, data, ds -> ds.select(proj),
                                in -> SL.mapRows(in, r -> O.project(r, proj)), false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B07 select with arithmetic expressions, over 10 expression groups
    // ========================================================================

    private static BatchDesign b07() {
        return new BatchDesign() {
            @Override public int batchNo() { return 7; }
            @Override public String title() { return "select arithmetic expressions (null propagation + division scale 10) × 10 expression sets"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] exprs = {
                        "v + 1 as v1", "v * 2 as v2", "v / 2 as vd", "v - 1 as vm",
                        "abs(v) as av", "round(v, 1) as rv", "v + 0.0000000001 as tiny",
                        "0 - v as neg", "v * v as sq", "(v + 1) * 2 as comp"};
                List<Function<BigDecimal, BigDecimal>> oracles = Arrays.asList(
                        v -> lift(v, x -> x.add(BigDecimal.ONE)),
                        v -> lift(v, x -> x.multiply(BigDecimal.valueOf(2))),
                        v -> lift(v, x -> x.divide(BigDecimal.valueOf(2), 10, RoundingMode.HALF_UP)),
                        v -> lift(v, x -> x.subtract(BigDecimal.ONE)),
                        v -> lift(v, BigDecimal::abs),
                        v -> lift(v, x -> x.setScale(1, RoundingMode.HALF_UP)),
                        v -> lift(v, x -> x.add(new BigDecimal("0.0000000001"))),
                        v -> lift(v, x -> BigDecimal.ZERO.subtract(x)),
                        v -> lift(v, x -> x.multiply(x)),
                        v -> lift(v, x -> x.add(BigDecimal.ONE).multiply(BigDecimal.valueOf(2))));
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < exprs.length; p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Function<BigDecimal, BigDecimal> oracle = oracles.get(p);
                        final List<DataRow> data = kvProfile(d);
                        final String expr = exprs[p];
                        io.rowweave.DataSchema typed = new io.rowweave.DataSchema(
                                D.f("x", io.rowweave.DataType.BigDecimalType));
                        out.add(SL.sinkTyped(id("B07", p, d), "select[" + KV_PROFILES[d] + "] " + expr,
                                D.KV, data, ds -> ds.select(expr), typed,
                                in -> SL.mapRows(in, r -> one(oracle.apply(r.getBigDecimal(1)))),
                                false));
                    }
                }
                return out;
            }

            private DataRow one(BigDecimal v) {
                DataRow o = new DataRow(1);
                o.set(0, v);
                return o;
            }
        };
    }

    // ========================================================================
    // B08 select with string expressions, over 10 expression groups
    // ========================================================================

    private static BatchDesign b08() {
        return new BatchDesign() {
            @Override public int batchNo() { return 8; }
            @Override public String title() { return "select string expressions (upper/concat/substr/left/right/replace/len)"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] exprs = {
                        "upper(k) as K", "concat(k, '-x') as kx", "substr(k, 2) as s2",
                        "substr(k, 1, 1) as s1", "left(k, 1) as l1", "right(k, 1) as r1",
                        "replace(k, 'g', 'G') as rg", "len(k) as ln", "contains(k, '1') as has1",
                        "lower(upper(k)) as lu"};
                io.rowweave.DataType[] types = {
                        io.rowweave.DataType.StringType, io.rowweave.DataType.StringType,
                        io.rowweave.DataType.StringType, io.rowweave.DataType.StringType,
                        io.rowweave.DataType.StringType, io.rowweave.DataType.StringType,
                        io.rowweave.DataType.StringType, io.rowweave.DataType.BigDecimalType,
                        io.rowweave.DataType.BooleanType, io.rowweave.DataType.StringType};
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < exprs.length; p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final int pF = p;
                        final String exprOne = exprs[p];
                        final List<DataRow> data = kvProfile(d);
                        io.rowweave.DataSchema typed = new io.rowweave.DataSchema(D.f("x", types[p]));
                        out.add(SL.sinkTyped(id("B08", p, d), "select[" + KV_PROFILES[d] + "] " + exprs[p],
                                D.KV, data, ds -> ds.select(exprOne), typed,
                                in -> SL.mapRows(in, r -> stringExprOut(pF, r)), false));
                    }
                }
                return out;
            }

            private DataRow stringExprOut(int p, DataRow r) {
                String k = kStr(r);
                DataRow o = new DataRow(1);
                switch (p) {
                    case 0: o.set(0, k == null ? null : k.toUpperCase()); break;
                    case 1: o.set(0, k == null ? null : k + "-x"); break;
                    case 2: o.set(0, k == null ? null : substr(k, 2, null)); break;
                    case 3: o.set(0, k == null ? null : substr(k, 1, 1)); break;
                    case 4: o.set(0, k == null ? null : edge(k, 1, true)); break;
                    case 5: o.set(0, k == null ? null : edge(k, 1, false)); break;
                    case 6: o.set(0, k == null ? null : k.replace("g", "G")); break;
                    case 7: o.set(0, k == null ? null : BigDecimal.valueOf(k.length())); break;
                    case 8: o.set(0, k == null ? null : k.contains("1")); break;
                    default: o.set(0, k == null ? null : k.toUpperCase().toLowerCase()); break;
                }
                return o;
            }

            private String edge(String s, int n, boolean fromLeft) {
                int m = Math.max(0, Math.min(s.length(), n));
                return fromLeft ? s.substring(0, m) : s.substring(s.length() - m);
            }
        };
    }

    // ========================================================================
    // B09 select with rename, reorder and repeated columns
    // ========================================================================

    private static BatchDesign b09() {
        return new BatchDesign() {
            @Override public int batchNo() { return 9; }
            @Override public String title() { return "select rename/reorder/repeated reference (10 variants × 10 profiles)"; }
            @Override public List<ScenarioDef> scenarios() {
                String[][] exprSets = {
                        {"v as amount", "k as key"},
                        {"k as key", "v"},
                        {"v", "v as v2"},
                        {"k as a", "k as b", "v as c"},
                        {"v + 0 as v0", "k"},
                        {"k"},
                        {"v"},
                        {"k", "v", "k as k2"},
                        {"v as x", "k as y", "v + 1 as z"},
                        {"upper(k) as u", "v as vv", "k"}};
                io.rowweave.DataType[][] types9 = {
                        {io.rowweave.DataType.BigDecimalType, io.rowweave.DataType.StringType},
                        {io.rowweave.DataType.StringType, io.rowweave.DataType.BigDecimalType},
                        {io.rowweave.DataType.BigDecimalType, io.rowweave.DataType.BigDecimalType},
                        {io.rowweave.DataType.StringType, io.rowweave.DataType.StringType,
                                io.rowweave.DataType.BigDecimalType},
                        {io.rowweave.DataType.BigDecimalType, io.rowweave.DataType.StringType},
                        {io.rowweave.DataType.StringType},
                        {io.rowweave.DataType.BigDecimalType},
                        {io.rowweave.DataType.StringType, io.rowweave.DataType.BigDecimalType,
                                io.rowweave.DataType.StringType},
                        {io.rowweave.DataType.BigDecimalType, io.rowweave.DataType.StringType,
                                io.rowweave.DataType.BigDecimalType},
                        {io.rowweave.DataType.StringType, io.rowweave.DataType.BigDecimalType,
                                io.rowweave.DataType.StringType}};
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < exprSets.length; p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final int pF = p;
                        final String[] exprSet = exprSets[p];
                        final List<DataRow> data = kvProfile(d);
                        Field[] fs = new Field[types9[p].length];
                        for (int c = 0; c < fs.length; c++) {
                            fs[c] = D.f("c" + c, types9[p][c]);
                        }
                        io.rowweave.DataSchema typed = new io.rowweave.DataSchema(fs);
                        out.add(SL.sinkTyped(id("B09", p, d), "select[" + KV_PROFILES[d] + "] "
                                        + String.join(", ", exprSets[p]),
                                D.KV, data, ds -> ds.select(exprSet), typed,
                                in -> SL.mapRows(in, r -> renameOut(pF, r)), false));
                    }
                }
                return out;
            }

            private DataRow renameOut(int p, DataRow r) {
                Object k = r.get(0);
                BigDecimal v = r.getBigDecimal(1);
                switch (p) {
                    case 0: return row(v, k);
                    case 1: return row(k, v);
                    case 2: return row(v, v);
                    case 3: return row(k, k, v);
                    case 4: return row(lift(v, x -> x.add(BigDecimal.ZERO)), k);
                    case 5: return row(k);
                    case 6: return row(v);
                    case 7: return row(k, v, k);
                    case 8: return row(v, k, lift(v, x -> x.add(BigDecimal.ONE)));
                    default: return row(k == null ? null : ((String) k).toUpperCase(), v, k);
                }
            }

            private DataRow row(Object... vals) {
                DataRow o = new DataRow(vals.length);
                for (int i = 0; i < vals.length; i++) {
                    o.set(i, vals[i]);
                }
                return o;
            }
        };
    }

    // ========================================================================
    // B10 map with a code function, over 10 transforms
    // ========================================================================

    private static BatchDesign b10() {
        return new BatchDesign() {
            @Override public int batchNo() { return 10; }
            @Override public String title() { return "map code functions (10 transformations × 10 profiles)"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] names = {"identity", "upperK", "nullNegV", "scale2", "lowerK", "prefixK",
                        "v3x", "constK", "nullShortK", "nullV"};
                List<Function<DataRow, DataRow>> fns = Arrays.asList(
                        DataRow::copy,
                        r -> upd(r, 0, kStr(r) == null ? null : kStr(r).toUpperCase()),
                        r -> upd(r, 1, r.getBigDecimal(1) != null && r.getBigDecimal(1).signum() < 0
                                ? null : r.get(1)),
                        r -> upd(r, 1, lift(r.getBigDecimal(1),
                                v -> v.setScale(2, RoundingMode.HALF_UP))),
                        r -> upd(r, 0, kStr(r) == null ? null : kStr(r).toLowerCase()),
                        r -> upd(r, 0, kStr(r) == null ? null : "x:" + kStr(r)),
                        r -> upd(r, 1, lift(r.getBigDecimal(1),
                                v -> v.multiply(BigDecimal.valueOf(3)))),
                        r -> upd(r, 0, "const"),
                        r -> upd(r, 0, kStr(r) != null && kStr(r).length() < 2 ? null : r.get(0)),
                        r -> upd(r, 1, null));
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < fns.size(); p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Function<DataRow, DataRow> fn = fns.get(p);
                        final List<DataRow> data = kvProfile(d);
                        final String name = names[p];
                        out.add(SL.sink(id("B10", p, d), "map[" + KV_PROFILES[d] + "] " + name,
                                D.KV, data, ds -> ds.map(fn::apply),
                                in -> SL.mapRows(in, fn), false));
                    }
                }
                return out;
            }

            private DataRow upd(DataRow r, int idx, Object val) {
                DataRow o = r.copy();
                o.set(idx, val);
                return o;
            }

            private DataRow row(Object... vals) {
                DataRow o = new DataRow(vals.length);
                for (int i = 0; i < vals.length; i++) {
                    o.set(i, vals[i]);
                }
                return o;
            }
        };
    }

    // ========================================================================
    // B11 flatMap expansion patterns, over 10 cases
    // ========================================================================

    private static BatchDesign b11() {
        return new BatchDesign() {
            @Override public int batchNo() { return 11; }
            @Override public String title() { return "flatMap expansion patterns (10 patterns × 10 profiles)"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] names = {"twice", "onlyPositive", "explodeIntCap3", "kAndKx", "none",
                        "allNullRow", "dupWhenVZero", "splitKAndV", "vPlus1", "vAndVPlusTiny"};
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < names.length; p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final int pF = p;
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B11", p, d), "flatMap[" + KV_PROFILES[d] + "] " + names[p],
                                D.KV, data,
                                ds -> ds.flatMap((r, collector) -> emit(pF, r, collector)),
                                in -> flatOracle(pF, in), false));
                    }
                }
                return out;
            }

            private void emit(int p, DataRow r, io.rowweave.Collector c) {
                for (DataRow o : flatOracle(p, java.util.Collections.singletonList(r))) {
                    c.collect(o);
                }
            }

            private List<DataRow> flatOracle(int p, List<DataRow> in) {
                List<DataRow> out = new ArrayList<>();
                for (DataRow r : in) {
                    BigDecimal v = r.getBigDecimal(1);
                    switch (p) {
                        case 0:
                            out.add(r.copy());
                            out.add(r.copy());
                            break;
                        case 1:
                            if (v != null && v.signum() > 0) {
                                out.add(r.copy());
                            }
                            break;
                        case 2: {
                            int n = v == null ? 0
                                    : Math.min(3, v.setScale(0, RoundingMode.DOWN).abs().intValue());
                            for (int i = 0; i < n; i++) {
                                out.add(r.copy());
                            }
                            break;
                        }
                        case 3: {
                            DataRow b = r.copy();
                            b.set(0, kStr(r) == null ? null : kStr(r) + "x");
                            out.add(r.copy());
                            out.add(b);
                            break;
                        }
                        case 4:
                            break;
                        case 5:
                            out.add(new DataRow(2));
                            break;
                        case 6:
                            if (v != null && v.signum() == 0) {
                                out.add(r.copy());
                                out.add(r.copy());
                            } else {
                                out.add(r.copy());
                            }
                            break;
                        case 7: {
                            DataRow kOnly = new DataRow(2);
                            kOnly.set(0, r.get(0));
                            DataRow vOnly = new DataRow(2);
                            vOnly.set(1, v);
                            out.add(kOnly);
                            out.add(vOnly);
                            break;
                        }
                        case 8: {
                            DataRow o = r.copy();
                            o.set(1, lift(v, x -> x.add(BigDecimal.ONE)));
                            out.add(o);
                            break;
                        }
                        default: {
                            out.add(r.copy());
                            DataRow b = r.copy();
                            b.set(1, lift(v, x -> x.add(new BigDecimal("0.0000000001"))));
                            out.add(b);
                            break;
                        }
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B12 chained pass-through combinations, depth 1..5
    // ========================================================================

    private static BatchDesign b12() {
        return new BatchDesign() {
            @Override public int batchNo() { return 12; }
            @Override public String title() { return "chained pass-through (filter/map/select depth 1..5)"; }
            @Override public List<ScenarioDef> scenarios() {
                List<Chain> chains = Arrays.asList(
                        new Chain("f(v>5)",
                                ds -> ds.filter("v > 5"),
                                rows -> SL.filterRows(rows, r -> vCmp(r, 0, BigDecimal.valueOf(5)))),
                        new Chain("f(v>0).f(k not null)",
                                ds -> ds.filter("v > 0").filter("k is not null"),
                                rows -> SL.filterRows(
                                        SL.filterRows(rows, r -> vCmp(r, 0, BigDecimal.ZERO)),
                                        r -> r.get(0) != null)),
                        new Chain("f(v>0).map(upperK)",
                                ds -> ds.filter("v > 0").map(r -> upperK(r)),
                                rows -> SL.mapRows(
                                        SL.filterRows(rows, r -> vCmp(r, 0, BigDecimal.ZERO)),
                                        FamilyA::upperK)),
                        new Chain("map(v*3).f(v>10)",
                                ds -> ds.map(r -> mulV(r, 3)).filter("v > 10"),
                                rows -> SL.filterRows(SL.mapRows(rows, r -> mulV(r, 3)),
                                        r -> vCmp(r, 0, BigDecimal.valueOf(10)))),
                        new Chain("sel(v).f(v>5)",
                                ds -> ds.select(1).filter("v > 5"),
                                rows -> SL.filterRows(
                                        SL.mapRows(rows, r -> row(r.get(1))),
                                        r -> colCmp(r, 0, 0, BigDecimal.valueOf(5)))),
                        new Chain("map(upperK).sel(v)",
                                ds -> ds.map(FamilyA::upperK).select(1),
                                rows -> SL.mapRows(SL.mapRows(rows, FamilyA::upperK),
                                        r -> row(r.get(1)))),
                        new Chain("f(k not null).f(v>=1).sel(v,k)",
                                ds -> ds.filter("k is not null").filter("v >= 1").select(1, 0),
                                rows -> SL.mapRows(
                                        SL.filterRows(
                                                SL.filterRows(rows, r -> r.get(0) != null),
                                                r -> vCmp(r, 1, BigDecimal.ONE)),
                                        r -> O.project(r, O.idx(1, 0)))),
                        new Chain("sel(v,k).f(v>5)",
                                ds -> ds.select(1, 0).filter("v > 5"),
                                rows -> SL.filterRows(
                                        SL.mapRows(rows, r -> O.project(r, O.idx(1, 0))),
                                        r -> colCmp(r, 0, 0, BigDecimal.valueOf(5)))),
                        new Chain("map(+1).map(+1).f(v>10)",
                                ds -> ds.map(r -> addV(r, 1)).map(r -> addV(r, 1)).filter("v > 10"),
                                rows -> SL.filterRows(
                                        SL.mapRows(SL.mapRows(rows, r -> addV(r, 1)),
                                                r -> addV(r, 1)),
                                        r -> vCmp(r, 0, BigDecimal.valueOf(10)))),
                        new Chain("f(v>5).sel(v as x).map(x+1)",
                                ds -> ds.filter("v > 5").select("v as x")
                                        .map(r -> row(lift(r.getBigDecimal(0),
                                                x -> x.add(BigDecimal.ONE)))),
                                rows -> SL.mapRows(
                                        SL.filterRows(rows, r -> vCmp(r, 0, BigDecimal.valueOf(5))),
                                        r -> row(lift(r.getBigDecimal(1),
                                                x -> x.add(BigDecimal.ONE))))));
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < chains.size(); p++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Chain chain = chains.get(p);
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B12", p, d), "chain[" + KV_PROFILES[d] + "] " + chain.name,
                                D.KV, data, chain.dag, chain.oracle, false));
                    }
                }
                return out;
            }
        };
    }

    static DataRow upperK(DataRow r) {
        DataRow o = r.copy();
        o.set(0, kStr(r) == null ? null : kStr(r).toUpperCase());
        return o;
    }

    private static DataRow mulV(DataRow r, int m) {
        DataRow o = r.copy();
        o.set(1, lift(r.getBigDecimal(1), v -> v.multiply(BigDecimal.valueOf(m))));
        return o;
    }

    private static DataRow addV(DataRow r, int a) {
        DataRow o = r.copy();
        o.set(1, lift(r.getBigDecimal(1), v -> v.add(BigDecimal.valueOf(a))));
        return o;
    }

    private static DataRow row(Object... vals) {
        DataRow o = new DataRow(vals.length);
        for (int i = 0; i < vals.length; i++) {
            o.set(i, vals[i]);
        }
        return o;
    }

    static final class Chain {
        final String name;
        final Function<DataSet, DataSet> dag;
        final Function<List<DataRow>, List<DataRow>> oracle;

        Chain(String name, Function<DataSet, DataSet> dag,
              Function<List<DataRow>, List<DataRow>> oracle) {
            this.name = name;
            this.dag = dag;
            this.oracle = oracle;
        }
    }
}
