package io.rowweave.examples.scenariolab.batches;

import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.examples.scenariolab.BatchDesign;
import io.rowweave.examples.scenariolab.D;
import io.rowweave.examples.scenariolab.O;
import io.rowweave.examples.scenariolab.SL;
import io.rowweave.examples.scenariolab.ScenarioDef;
import io.rowweave.examples.scenariolab.ScenarioLab;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.rowweave.examples.scenariolab.batches.FamilyA.id;
import static io.rowweave.examples.scenariolab.batches.FamilyA.kvProfile;
import static io.rowweave.examples.scenariolab.batches.FamilyA.vCmp;
import static io.rowweave.examples.scenariolab.batches.FamilyA.KV_PROFILES;
import static io.rowweave.examples.scenariolab.batches.FamilyA.kStr;
import static io.rowweave.examples.scenariolab.batches.FamilyA.lift;
import static io.rowweave.examples.scenariolab.batches.FamilyA.liftB;

/**
 * Family B (B13 to B22): the main aggregation family of union, distinct, addFields,
 * partitionByHash and groupBy. Each batch is designed on its own as 10 variants over 10 data
 * profiles, or the orthogonal product of 10 by 10, which gives 100 scenarios.
 */
public final class FamilyB {

    private FamilyB() {
    }

    public static List<BatchDesign> batches() {
        return Arrays.asList(
                b13(), b14(), b15(), b16(), b17(),
                b18(), b19(), b20(), b21(), b22());
    }

    /** The 10 profiles of the ORD schema. */
    private static final String[] ORD_PROFILES = {
            "base", "empty", "single", "dupes", "nullAmt", "nullGrp", "neg", "shuffled",
            "highScale", "many"};

    static List<DataRow> ordProfile(int i) {
        switch (i) {
            case 0: return D.ordBase();
            case 1: return D.empty();
            case 2: return D.rows(new Object[]{99L, "solo", D.bd("1.23"), 1});
            case 3: {
                List<DataRow> base = D.ordBase();
                List<DataRow> out = new ArrayList<>(base);
                out.addAll(base);
                return out;
            }
            case 4: return D.rows(
                    new Object[]{1L, "x", null, 1}, new Object[]{2L, "x", D.bd("5"), null},
                    new Object[]{3L, "y", null, null}, new Object[]{4L, null, D.bd("2"), 2});
            case 5: return D.rows(
                    new Object[]{1L, null, D.bd("1"), 1}, new Object[]{2L, null, D.bd("2"), 2},
                    new Object[]{3L, "z", D.bd("3"), null});
            case 6: return D.rows(
                    new Object[]{1L, "n", D.bd("-1.5"), 1}, new Object[]{2L, "n", D.bd("-2"), 2},
                    new Object[]{3L, "p", D.bd("3"), -1}, new Object[]{4L, "p", D.bd("0"), 0});
            case 7: {
                List<DataRow> base = D.ordBase();
                List<DataRow> out = new ArrayList<>();
                for (int j = base.size() - 1; j >= 0; j--) {
                    out.add(base.get(j));
                }
                return out;
            }
            case 8: return D.rows(
                    new Object[]{1L, "s", D.bd("0.0000000001"), 1},
                    new Object[]{2L, "s", D.bd("9999999999.9999999999"), 2},
                    new Object[]{3L, "s", D.bd("5.00000000005"), 3});
            default: {
                List<DataRow> out = new ArrayList<>();
                for (int j = 0; j < 24; j++) {
                    out.add(D.row((long) (j + 1), "g" + (j % 4),
                            D.bd(String.valueOf(j * 1.5)), j % 3));
                }
                return out;
            }
        }
    }

    // ========================================================================
    // B13 two-input union, over 10 data combinations
    // ========================================================================

    private static BatchDesign b13() {
        return new BatchDesign() {
            @Override public int batchNo() { return 13; }
            @Override public String title() { return "union of two inputs (10 data combinations: empty/self-merge/duplicate/null)"; }
            @Override public List<ScenarioDef> scenarios() {
                List<int[]> pairs = Arrays.asList(
                        new int[]{0, 0}, new int[]{0, 1}, new int[]{3, 0}, new int[]{0, 3},
                        new int[]{3, 3}, new int[]{2, 2}, new int[]{0, 4}, new int[]{1, 5},
                        new int[]{-1, -1}, new int[]{0, 7});
                String[] names = {"base+base", "base+nulls", "empty+base", "base+empty",
                        "empty+empty", "dupes+dupes", "base+single", "nulls+negative",
                        "self-union", "base+unicode"};
                List<ScenarioDef> out = new ArrayList<>();
                for (int p = 0; p < pairs.size(); p++) {
                    for (int d = 0; d < 10; d++) {
                        // multiplying every combination by 10 right profiles would only repeat the same work, so the combination is spread over 10 cases and a hash of (p*10+d) picks the left and the right
                        int li = pairs.get(p)[0] >= 0 ? pairs.get(p)[0] : d;
                        int ri = pairs.get(p)[1] >= 0 ? pairs.get(p)[1] : d;
                        final int lf = li;
                        final int rf = ri;
                        final String name = names[p];
                        out.add(SL.sink2(id("B13", p, d), "union[" + name + "] L=" + KV_PROFILES[lf]
                                        + " R=" + KV_PROFILES[rf],
                                D.KV, kvProfile(lf), D.KV, kvProfile(rf),
                                (l, r) -> l.union(r),
                                (a, b) -> SL.concat(a, b), false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B14 chained union with a downstream operator, over 10 templates
    // ========================================================================

    private static BatchDesign b14() {
        return new BatchDesign() {
            @Override public int batchNo() { return 14; }
            @Override public String title() { return "chained union (3/4 inputs) plus downstream (distinct/groupBy/orderBy/top)"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] names = {"u2", "u3", "u4", "u2+distinct", "u2+groupBySum",
                        "u2+f+groupBySum", "u2+orderByK", "u2+top5", "d+u+d", "u2+f(like)"};
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < names.length; t++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final int tf = t;
                        final List<DataRow> right = kvProfile(d);
                        final List<DataRow> base = D.kvBase();
                        final boolean ordered = (t == 6 || t == 7);
                        out.add(ScenarioDef.of(id("B14", t, d),
                                "unionChain[" + KV_PROFILES[d] + "] " + names[t],
                                lab -> {
                                    DataSet a = lab.source("a", D.KV, base);
                                    DataSet b = lab.source("b", D.KV, right);
                                    DataSet c = lab.source("c", D.KV, D.kvDupes());
                                    DataSet e = lab.source("e", D.KV, D.empty());
                                    DataSet res;
                                    List<DataRow> expected;
                                    switch (tf) {
                                        case 0:
                                            res = a.union(b);
                                            expected = SL.concat(base, right);
                                            break;
                                        case 1:
                                            res = a.union(b).union(c);
                                            expected = SL.concat(base, right, D.kvDupes());
                                            break;
                                        case 2:
                                            res = a.union(b).union(c).union(e);
                                            expected = SL.concat(base, right, D.kvDupes(), D.empty());
                                            break;
                                        case 3:
                                            res = a.union(b).distinct();
                                            expected = O.distinctAll(SL.concat(base, right));
                                            break;
                                        case 4:
                                            res = a.union(b).groupBy("k").sum("v");
                                            expected = O.groupSum(SL.concat(base, right), O.idx(0), 1);
                                            break;
                                        case 5:
                                            res = a.union(b).filter("v > 5").groupBy("k").sum("v");
                                            expected = O.groupSum(SL.filterRows(SL.concat(base, right),
                                                    r -> vCmp(r, 0, BigDecimal.valueOf(5))), O.idx(0), 1);
                                            break;
                                        case 6:
                                            res = a.union(b).orderBy(O.orderBy(D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"),
                                                    O.sf(D.KV, "v", true, "DEFAULT")), 1, false);
                                            expected = O.sortRows(SL.concat(base, right), D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"),
                                                    O.sf(D.KV, "v", true, "DEFAULT"));
                                            break;
                                        case 7:
                                            res = a.union(b).orderBy(O.orderBy(D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"),
                                                    O.sf(D.KV, "v", true, "DEFAULT")), 1, false)
                                                    .top(5);
                                            expected = O.sortRows(SL.concat(base, right), D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"),
                                                    O.sf(D.KV, "v", true, "DEFAULT"))
                                                    .subList(0, Math.min(5, base.size() + right.size()));
                                            break;
                                        case 8: {
                                            DataSet da = a.distinct();
                                            DataSet db = b.distinct();
                                            res = da.union(db).distinct();
                                            expected = O.distinctAll(SL.concat(
                                                    O.distinctAll(base), O.distinctAll(right)));
                                            break;
                                        }
                                        default:
                                            res = a.union(b).filter("k like 'g%'");
                                            expected = SL.filterRows(SL.concat(base, right),
                                                    r -> kStr(r) != null && likeG(kStr(r)));
                                            break;
                                    }
                                    if (ordered) {
                                        lab.expectOrdered(res, expected);
                                    } else {
                                        lab.expect(res, expected);
                                    }
                                }));
                    }
                }
                return out;
            }

            private io.rowweave.ops.SortField sf(io.rowweave.DataSchema meta, String f, boolean asc,
                                                  io.rowweave.ops.SortField.NullOrder no) {
                return new io.rowweave.ops.SortField(meta.getFieldIndex(f),
                        asc ? io.rowweave.ops.SortField.Direction.ASC
                                : io.rowweave.ops.SortField.Direction.DESC, no);
            }

            private boolean likeG(String s) {
                return s.matches("g.*");
            }
        };
    }

    // ========================================================================
    // B15 distinct over all columns, 10 table shapes x 10 key combinations, weighted to the all-columns variant among the 10 all-columns and subset variants
    // ========================================================================

    private static BatchDesign b15() {
        return new BatchDesign() {
            @Override public int batchNo() { return 15; }
            @Override public String title() { return "distinct (10 table shapes × 10 distinct key combinations, including all columns)"; }
            @Override public List<ScenarioDef> scenarios() {
                String[][] kvVariants = {
                        {}, {"k"}, {"v"}, {"k", "v"}, {"v", "k"},
                        {"k"}, {"v"}, {"k", "v"}, {}, {"k"}};
                String[][] ordVariants = {
                        {}, {"id"}, {"grp"}, {"amt"}, {"id", "grp"}, {"grp", "amt"},
                        {"id", "grp", "amt"}, {"qty"}, {"id", "qty"}, {"grp", "qty"}};
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < 5; t++) {
                    // table shapes: 0 KV, 1 ORD, 2 CAT, 3 K1, 4 L2, with two profile and key combinations per shape, which gives 10 design groups
                    for (int half = 0; half < 2; half++) {
                        for (int d = 0; d < KV_PROFILES.length; d++) {
                            final int tF = t;
                            final int hF = half;
                            final int dF = d;
                            out.add(ScenarioDef.of(id("B15", t * 2 + half, d),
                                    "distinct[t" + t + "h" + half + "][" + KV_PROFILES[d] + "]",
                                    lab -> buildOne(lab, tF, hF, dF)));
                        }
                    }
                }
                return out;
            }

            private void buildOne(ScenarioLab lab, int t, int half, int d) {
                switch (t) {
                    case 0: {
                        String[] keys = half == 0 ? new String[]{} : new String[]{"k"};
                        List<DataRow> data = kvProfile(d);
                        DataSet res = keys.length == 0 ? lab.source(D.KV, data).distinct()
                                : lab.source(D.KV, data).distinct(keys).select("k");
                        lab.expect(res, keys.length == 0 ? O.distinctAll(data)
                                : SL.mapRows(O.distinctBy(data, O.idx(0)),
                                        r -> O.project(r, O.idx(0))));
                        break;
                    }
                    case 1: {
                        List<DataRow> data = ordProfile(d);
                        String[] keys = half == 0 ? new String[]{"grp"} : new String[]{"grp", "amt"};
                        DataSet res = lab.source(D.ORD, data).distinct(keys).select(keys);
                        int[] kidx = keys.length > 1
                                ? O.idx(D.ORD.getFieldIndex(keys[0]), D.ORD.getFieldIndex(keys[1]))
                                : O.idx(D.ORD.getFieldIndex(keys[0]));
                        lab.expect(res, SL.mapRows(O.distinctBy(data, kidx), r -> O.project(r, kidx)));
                        break;
                    }
                    case 2: {
                        List<DataRow> data = catProfile(d);
                        String[] keys = half == 0 ? new String[]{"cat"} : new String[]{"cat", "sub"};
                        DataSet res = lab.source(D.CAT, data).distinct(keys).select(keys);
                        int[] kidx = keys.length > 1
                                ? O.idx(D.CAT.getFieldIndex(keys[0]), D.CAT.getFieldIndex(keys[1]))
                                : O.idx(D.CAT.getFieldIndex(keys[0]));
                        lab.expect(res, SL.mapRows(O.distinctBy(data, kidx), r -> O.project(r, kidx)));
                        break;
                    }
                    case 3: {
                        List<DataRow> data = D.k1Set("a", "b", "a", null, "c", "b", null,
                                "a", "d", "e", "a", "f", "g", "e");
                        DataSet res = half == 0 ? lab.source(D.K1, data).distinct()
                                : lab.source(D.K1, data).distinct("k");
                        lab.expect(res, O.distinctAll(data));
                        break;
                    }
                    default: {
                        List<DataRow> data = D.l2Sort();
                        DataSet res = half == 0 ? lab.source(D.L2, data).distinct()
                                : lab.source(D.L2, data).distinct("a").select("a");
                        lab.expect(res, half == 0 ? O.distinctAll(data)
                                : SL.mapRows(O.distinctBy(data, O.idx(0)), r -> O.project(r, O.idx(0))));
                        break;
                    }
                }
            }
        };
    }

    private static List<DataRow> catProfile(int i) {
        switch (i) {
            case 0: return D.catBase();
            case 1: return D.empty();
            case 2: return D.rows(new Object[]{1L, "only", "x", D.bd("1"), true});
            case 3: {
                List<DataRow> b = D.catBase();
                List<DataRow> out = new ArrayList<>(b);
                out.addAll(b);
                return out;
            }
            case 4: return D.rows(
                    new Object[]{1L, null, null, null, null},
                    new Object[]{2L, null, "x", D.bd("1"), null},
                    new Object[]{3L, "a", null, D.bd("2"), true});
            case 5: return D.rows(
                    new Object[]{1L, "u", "x", D.bd("1"), true},
                    new Object[]{2L, "u", "y", D.bd("1"), false},
                    new Object[]{3L, "u", "x", D.bd("2"), null});
            case 6: return D.rows(
                    new Object[]{1L, "n", "x", D.bd("-5"), false},
                    new Object[]{2L, "n", "x", D.bd("0"), true});
            default: {
                List<DataRow> out = new ArrayList<>();
                for (int j = 0; j < 18; j++) {
                    out.add(D.row((long) j, "c" + (j % 3), "s" + (j % 2),
                            D.bd(String.valueOf(j)), j % 2 == 0));
                }
                return out;
            }
        }
    }

    // ========================================================================
    // B16 distinct with a null key, 10 null-dense datasets x 10 key combinations
    // ========================================================================

    private static BatchDesign b16() {
        return new BatchDesign() {
            @Override public int batchNo() { return 16; }
            @Override public String title() { return "distinct null key semantics (null forms its own group) × 10 data × 10 keys"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                String[][] kvKeySets = {
                        {"k"}, {"v"}, {"k", "v"}, {"v", "k"}, {"k"},
                        {"v"}, {"k", "v"}, {"k"}, {"v"}, {"k", "v"}};
                List<List<Object[]>> nullish = new ArrayList<>();
                nullish.add(Arrays.<Object[]>asList(
                        new Object[]{"n1", D.bd("1")}, new Object[]{null, D.bd("1")},
                        new Object[]{"n1", null}, new Object[]{null, null},
                        new Object[]{"n1", D.bd("1")}, new Object[]{null, D.bd("1")}));
                nullish.add(Arrays.<Object[]>asList(
                        new Object[]{"a", null}, new Object[]{"a", null},
                        new Object[]{null, D.bd("2")}, new Object[]{"b", D.bd("3")},
                        new Object[]{null, D.bd("2")}));
                nullish.add(Arrays.<Object[]>asList(
                        new Object[]{"", D.bd("9")}, new Object[]{null, null},
                        new Object[]{"", D.bd("9")}));
                nullish.add(Arrays.<Object[]>asList(
                        new Object[]{"Ω", D.bd("1")}, new Object[]{"Ω", null},
                        new Object[]{null, D.bd("2")}, new Object[]{"Ω", D.bd("3")}));
                for (int v = 0; v < 10; v++) {
                    final String[] keySet = kvKeySets[v];
                    for (int d = 0; d < 10; d++) {
                        final List<DataRow> data = d < nullish.size()
                                ? D.rows(nullish.get(d).toArray(new Object[0][]))
                                : kvProfile(d - nullish.size());
                        final int[] keyIdx = new int[keySet.length];
                        for (int i = 0; i < keySet.length; i++) {
                            keyIdx[i] = D.KV.getFieldIndex(keySet[i]);
                        }
                        out.add(SL.sink(id("B16", v, d), "distinctNull[" + v + "][" + d + "] keys="
                                        + String.join(",", keySet),
                                D.KV, data, ds -> ds.distinct(keySet).select(keySet),
                                in -> SL.mapRows(O.distinctBy(in, keyIdx), r -> O.project(r, keyIdx)),
                                false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B17 addFields with constant columns, over 10 field groups
    // ========================================================================

    private static BatchDesign b17() {
        return new BatchDesign() {
            @Override public int batchNo() { return 17; }
            @Override public String title() { return "addFields constant columns (5 types + null + multiple columns) × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<Field[]> fieldSets = Arrays.asList(
                        new Field[]{D.f("tag", DataType.StringType)},
                        new Field[]{D.f("n", DataType.IntegerType)},
                        new Field[]{D.f("l", DataType.LongType)},
                        new Field[]{D.f("d", DataType.BigDecimalType)},
                        new Field[]{D.f("b", DataType.BooleanType)},
                        new Field[]{D.f("s", DataType.StringType)},
                        new Field[]{D.f("a", DataType.StringType), D.f("i", DataType.IntegerType)},
                        new Field[]{D.f("x", DataType.BigDecimalType)},
                        new Field[]{D.f("m", DataType.StringType), D.f("nn", DataType.IntegerType),
                                D.f("ff", DataType.BooleanType)},
                        new Field[]{D.f("big", DataType.BigDecimalType)});
                List<Object[]> valSets = Arrays.asList(
                        new Object[]{"T"},
                        new Object[]{7},
                        new Object[]{100L},
                        new Object[]{D.bd("1.5")},
                        new Object[]{Boolean.TRUE},
                        new Object[]{null},
                        new Object[]{"A", 1},
                        new Object[]{null},
                        new Object[]{"Ω", -3, Boolean.FALSE},
                        new Object[]{D.bd("999999999999.9999999999")});
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < fieldSets.size(); v++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Field[] fields = fieldSets.get(v);
                        final Object[] vals = valSets.get(v);
                        final List<DataRow> data = kvProfile(d);
                        final int width = 2 + fields.length;
                        out.add(SL.sink(id("B17", v, d), "addFields[" + KV_PROFILES[d] + "] #" + v,
                                D.KV, data, ds -> ds.addFields(fields, vals),
                                in -> SL.mapRows(in, r -> {
                                    DataRow o = new DataRow(width);
                                    o.set(0, r.get(0));
                                    o.set(1, r.get(1));
                                    for (int i = 0; i < fields.length; i++) {
                                        o.set(2 + i, vals[i]);
                                    }
                                    return o;
                                }), false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B18 partitionByHash followed by a breaker, over 10 templates
    // ========================================================================

    private static BatchDesign b18() {
        return new BatchDesign() {
            @Override public int batchNo() { return 18; }
            @Override public String title() { return "partitionByHash (identity) × downstream breaker × 10 templates"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] names = {"ph(k)", "ph(k,v)", "ph(k)+groupSum", "ph(k)+distinct(k)",
                        "ph(k,v)+groupCount", "ph(k)+orderByK", "ph(k)+f(v>5)", "ph(k)+top3",
                        "ph(k)+map(upper)+distinct", "ph(k)+f(v>0)+groupSum"};
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < names.length; t++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final int tf = t;
                        final List<DataRow> data = kvProfile(d);
                        final boolean ordered = (t == 5 || t == 7);
                        out.add(SL.sink(id("B18", t, d), "ph[" + KV_PROFILES[d] + "] " + names[t],
                                D.KV, data, ds -> dag(tf, ds),
                                in -> oracle(tf, in), ordered));
                    }
                }
                return out;
            }

            private DataSet dag(int t, DataSet ds) {
                switch (t) {
                    case 0: return ds.partitionByHash("k");
                    case 1: return ds.partitionByHash("k", "v");
                    case 2: return ds.partitionByHash("k").groupBy("k").sum("v");
                    case 3: return ds.partitionByHash("k").distinct("k").select("k");
                    case 4: return ds.partitionByHash("k", "v").groupBy("k", "v").count();
                    case 5: return ds.partitionByHash("k").orderBy("k");
                    case 6: return ds.partitionByHash("k").filter("v > 5");
                    case 7: return ds.partitionByHash("k").top(3);
                    case 8: return ds.partitionByHash("k")
                            .map(r -> {
                                DataRow o = r.copy();
                                o.set(0, kStr(r) == null ? null : kStr(r).toUpperCase());
                                return o;
                            }).distinct();
                    default: return ds.partitionByHash("k").filter("v > 0").groupBy("k").sum("v");
                }
            }

            private List<DataRow> oracle(int t, List<DataRow> in) {
                switch (t) {
                    case 0:
                    case 1:
                        return in;
                    case 2: return O.groupSum(in, O.idx(0), 1);
                    case 3: return SL.mapRows(O.distinctBy(in, O.idx(0)), r -> O.project(r, O.idx(0)));
                    case 4: return O.groupCount(in, O.idx(0, 1));
                    case 5: return O.sortRows(in, D.KV, sf(D.KV, "k", true, "DEFAULT"));
                    case 6: return SL.filterRows(in, r -> vCmp(r, 0, BigDecimal.valueOf(5)));
                    case 7: return in.subList(0, Math.min(3, in.size()));
                    case 8: {
                        List<DataRow> mapped = SL.mapRows(in, r -> {
                            DataRow o = r.copy();
                            o.set(0, kStr(r) == null ? null : kStr(r).toUpperCase());
                            return o;
                        });
                        return O.distinctAll(mapped);
                    }
                    default: return O.groupSum(SL.filterRows(in,
                            r -> liftB(r.getBigDecimal(1), v -> v.signum() > 0)), O.idx(0), 1);
                }
            }

            private io.rowweave.ops.SortField sf(io.rowweave.DataSchema meta, String f, boolean asc,
                                                  String no) {
                return new io.rowweave.ops.SortField(meta.getFieldIndex(f),
                        asc ? io.rowweave.ops.SortField.Direction.ASC
                                : io.rowweave.ops.SortField.Direction.DESC,
                        io.rowweave.ops.SortField.NullOrder.valueOf(no));
            }
        };
    }

    // ========================================================================
    // B19 groupBy sum on a single key, over 10 KV profiles
    // ========================================================================

    private static BatchDesign b19() {
        return new BatchDesign() {
            @Override public int batchNo() { return 19; }
            @Override public String title() { return "groupBy(k).sum(v) (scale 10 HALF_UP) × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        final String variant = variantName(vf);
                        out.add(SL.sink(id("B19", v, d), "groupSum[" + KV_PROFILES[d] + "] " + variant,
                                D.KV, data, ds -> dag(vf, ds),
                                in -> oracle(vf, in), false));
                    }
                }
                return out;
            }

            private String variantName(int v) {
                switch (v) {
                    case 0: return "sum(v)";
                    case 1: return "sum(v, alias)";
                    case 2: return "groupBy(k,v).sum(v)";
                    case 3: return "groupBy(v).sum(v)";
                    case 4: return "sum(v).select(s)";
                    case 5: return "sum(v)+f(pre v>0)";
                    case 6: return "sum(v)+f(post s>10)";
                    case 7: return "groupBy(k).sum(v).orderBy(k)";
                    case 8: return "sum(v) self-union first";
                    default: return "sum(v) twice-grouped";
                }
            }

            private DataSet dag(int v, DataSet ds) {
                switch (v) {
                    case 0: return ds.groupBy("k").sum("v");
                    case 1: return ds.groupBy("k").sum("v", "total");
                    case 2: return ds.groupBy("k", "v").sum("v");
                    case 3: return ds.groupBy("v").sum("v");
                    case 4: return ds.groupBy("k").sum("v").select("k", "sum0 as s");
                    case 5: return ds.filter("v > 0").groupBy("k").sum("v");
                    case 6: return ds.groupBy("k").sum("v", "s").filter("s > 10");
                    case 7: return ds.groupBy("k").sum("v").orderBy("k");
                    case 8: return ds.union(ds).groupBy("k").sum("v");
                    default: return ds.groupBy("k").sum("v").groupBy("k").sum("sum0");
                }
            }

            private List<DataRow> oracle(int v, List<DataRow> in) {
                switch (v) {
                    case 0:
                        return O.groupSum(in, O.idx(0), 1);
                    case 8:
                        return O.groupSum(SL.concat(in, in), O.idx(0), 1);
                    case 1:
                    case 4:
                        return O.groupSum(in, O.idx(0), 1);  // column names differ but the values match, compared as a multiset
                    case 2:
                        return O.groupSum(in, O.idx(0, 1), 1);
                    case 3:
                        return O.groupSum(in, O.idx(1), 1);
                    case 5:
                        return O.groupSum(SL.filterRows(in,
                                r -> liftB(r.getBigDecimal(1), x -> x.signum() > 0)), O.idx(0), 1);
                    case 6: {
                        List<io.rowweave.DataRow> summed = O.groupSum(in, O.idx(0), 1);
                        return SL.filterRows(summed, r -> colGt10(r, 1));
                    }
                    case 7: {
                        List<io.rowweave.DataRow> summed = O.groupSum(in, O.idx(0), 1);
                        DataSchema km = new DataSchema(D.f("k", DataType.StringType),
                                D.f("sum0", DataType.BigDecimalType));
                        return O.sortRows(summed, km, sf(km, "k", true, "DEFAULT"));
                    }
                    case 9: {
                        List<io.rowweave.DataRow> once = O.groupSum(in, O.idx(0), 1);
                        return O.groupSum(once, O.idx(0), 1);
                    }
                    default:
                        return in;
                }
            }

            private boolean colGt10(io.rowweave.DataRow r, int col) {
                BigDecimal v = r.getBigDecimal(col);
                return v != null && v.compareTo(BigDecimal.TEN) > 0;
            }

            private io.rowweave.ops.SortField sf(io.rowweave.DataSchema meta, String f, boolean asc,
                                                  String no) {
                return new io.rowweave.ops.SortField(meta.getFieldIndex(f),
                        asc ? io.rowweave.ops.SortField.Direction.ASC
                                : io.rowweave.ops.SortField.Direction.DESC,
                        io.rowweave.ops.SortField.NullOrder.valueOf(no));
            }
        };
    }

    // ========================================================================
    // B20 groupBy with composite keys, 10 key combinations over 10 ORD profiles
    // ========================================================================

    private static BatchDesign b20() {
        return new BatchDesign() {
            @Override public int batchNo() { return 20; }
            @Override public String title() { return "groupBy composite key sum (10 key combinations × 10 ORD profiles)"; }
            @Override public List<ScenarioDef> scenarios() {
                String[][] keySets = {
                        {"grp"}, {"id"}, {"qty"}, {"id", "grp"}, {"grp", "id"},
                        {"id", "qty"}, {"grp", "qty"}, {"qty", "grp"},
                        {"id", "grp", "qty"}, {"qty", "id", "grp"}};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < keySets.length; v++) {
                    for (int d = 0; d < ORD_PROFILES.length; d++) {
                        final String[] keys = keySets[v];
                        final List<DataRow> data = ordProfile(d);
                        final int[] keyIdx = idxOf(D.ORD, keys);
                        out.add(SL.sink(id("B20", v, d), "groupSum(keys=" + String.join(",", keys)
                                        + ")[" + ORD_PROFILES[d] + "]",
                                D.ORD, data, ds -> ds.groupBy(keys).sum("amt"),
                                in -> O.groupSum(in, keyIdx, 2), false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B21 the count family of groupBy, 10 aggregation variants over 10 ORD profiles
    // ========================================================================

    private static BatchDesign b21() {
        return new BatchDesign() {
            @Override public int batchNo() { return 21; }
            @Override public String title() { return "groupBy count/countField (plus multiple aggregates) × 10 variants × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < ORD_PROFILES.length; d++) {
                        final List<DataRow> data = ordProfile(d);
                        out.add(SL.sink(id("B21", v, d), "groupCount[v" + vf + "]["
                                        + ORD_PROFILES[d] + "]",
                                D.ORD, data, ds -> dag(vf, ds),
                                in -> oracle(vf, in), false));
                    }
                }
                return out;
            }

            private DataSet dag(int v, DataSet ds) {
                switch (v) {
                    case 0: return ds.groupBy("grp").count();
                    case 1: return ds.groupBy("grp").count("amt");
                    case 2: return ds.groupBy("grp").count("qty");
                    case 3: return ds.groupBy("grp").count("amt", "amtCnt");
                    case 4: return ds.groupBy(O.pga(D.ORD, "grp").count().countField("amt"));
                    case 5: return ds.groupBy(O.pga(D.ORD, "grp").count().max("amt"));
                    case 6: return ds.groupBy(O.pga(D.ORD, "grp").sum("amt").count());
                    case 7: return ds.groupBy(O.pga(D.ORD, "grp").count().countField("amt").countField("qty"));
                    case 8: return ds.groupBy("grp", "id").count();
                    default: return ds.groupBy("grp").count().filter("count0 > 1");
                }
            }

            private List<DataRow> oracle(int v, List<DataRow> in) {
                switch (v) {
                    case 0: return O.groupCount(in, O.idx(1));
                    case 1: return O.groupCountField(in, O.idx(1), 2);
                    case 2: return O.groupCountField(in, O.idx(1), 3);
                    case 3: return O.groupCountField(in, O.idx(1), 2);
                    case 4: return O.groupMultiAgg(in, O.idx(1), Arrays.asList(
                            O.Agg.countAll(), O.Agg.countField(2)));
                    case 5: return O.groupMultiAgg(in, O.idx(1), Arrays.asList(
                            O.Agg.countAll(), O.Agg.max(2)));
                    case 6: return O.groupMultiAgg(in, O.idx(1), Arrays.asList(
                            O.Agg.sum(2), O.Agg.countAll()));
                    case 7: return O.groupMultiAgg(in, O.idx(1), Arrays.asList(
                            O.Agg.countAll(), O.Agg.countField(2), O.Agg.countField(3)));
                    case 8: return O.groupCount(in, O.idx(1, 0));
                    default: {
                        List<io.rowweave.DataRow> counted = O.groupCount(in, O.idx(1));
                        return SL.filterRows(counted, r -> {
                            Long c = r.getLong(1);
                            return c != null && c > 1;
                        });
                    }
                }
            }
        };
    }

    // ========================================================================
    // B22 groupBy max and min, 10 variants over 10 profiles
    // ========================================================================

    private static BatchDesign b22() {
        return new BatchDesign() {
            @Override public int batchNo() { return 22; }
            @Override public String title() { return "groupBy max/min (numeric/string/multiple aggregates) × 10 variants × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B22", v, d), "groupMinMax[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                D.KV, data, ds -> dag(vf, ds),
                                in -> oracle(vf, in), false));
                    }
                }
                return out;
            }

            private DataSet dag(int v, DataSet ds) {
                switch (v) {
                    case 0: return ds.groupBy("k").max("v");
                    case 1: return ds.groupBy("k").min("v");
                    case 2: return ds.groupBy("k").max("k");
                    case 3: return ds.groupBy("k").min("k");
                    case 4: return ds.groupBy("k").max("v", "hi");
                    case 5: return ds.groupBy("k").min("v", "lo");
                    case 6: return ds.groupBy(O.pga(D.KV, "k").max("v").min("v"));
                    case 7: return ds.groupBy("v").max("k");
                    case 8: return ds.groupBy("k").max("v").filter("max0 >= 10");
                    default: return ds.groupBy("k").max("v").orderBy("k");
                }
            }

            private List<DataRow> oracle(int v, List<DataRow> in) {
                switch (v) {
                    case 0: return O.groupMinMax(in, O.idx(0), 1, true);
                    case 1: return O.groupMinMax(in, O.idx(0), 1, false);
                    case 2: return O.groupMinMax(in, O.idx(0), 0, true);
                    case 3: return O.groupMinMax(in, O.idx(0), 0, false);
                    case 4:
                    case 5: return O.groupMinMax(in, O.idx(0), 1, v == 4);
                    case 6: return O.groupMultiAgg(in, O.idx(0), Arrays.asList(
                            O.Agg.max(1), O.Agg.min(1)));
                    case 7: return O.groupMinMax(in, O.idx(1), 0, true);
                    case 8: {
                        List<io.rowweave.DataRow> mx = O.groupMinMax(in, O.idx(0), 1, true);
                        return SL.filterRows(mx, r -> {
                            BigDecimal x = r.getBigDecimal(1);
                            return x != null && x.compareTo(BigDecimal.TEN) >= 0;
                        });
                    }
                    default: {
                        List<io.rowweave.DataRow> mx = O.groupMinMax(in, O.idx(0), 1, true);
                        DataSchema km = new DataSchema(D.f("k", DataType.StringType),
                                D.f("max0", DataType.BigDecimalType));
                        return O.sortRows(mx, km, sf(km, "k", true, "DEFAULT"));
                    }
                }
            }

            private io.rowweave.ops.SortField sf(io.rowweave.DataSchema meta, String f, boolean asc,
                                                  String no) {
                return new io.rowweave.ops.SortField(meta.getFieldIndex(f),
                        asc ? io.rowweave.ops.SortField.Direction.ASC
                                : io.rowweave.ops.SortField.Direction.DESC,
                        io.rowweave.ops.SortField.NullOrder.valueOf(no));
            }
        };
    }

    private static int[] idxOf(io.rowweave.DataSchema meta, String... names) {
        int[] idx = new int[names.length];
        for (int i = 0; i < names.length; i++) {
            idx[i] = meta.getFieldIndex(names[i]);
        }
        return idx;
    }
}
