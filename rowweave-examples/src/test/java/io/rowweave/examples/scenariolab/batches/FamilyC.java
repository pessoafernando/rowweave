package io.rowweave.examples.scenariolab.batches;

import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.examples.scenariolab.BatchDesign;
import io.rowweave.examples.scenariolab.D;
import io.rowweave.examples.scenariolab.O;
import io.rowweave.examples.scenariolab.SL;
import io.rowweave.examples.scenariolab.ScenarioDef;
import io.rowweave.ops.SortField;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static io.rowweave.examples.scenariolab.batches.FamilyA.id;
import static io.rowweave.examples.scenariolab.batches.FamilyA.kvProfile;
import static io.rowweave.examples.scenariolab.batches.FamilyA.vCmp;
import static io.rowweave.examples.scenariolab.batches.FamilyA.KV_PROFILES;
import static io.rowweave.examples.scenariolab.batches.FamilyA.lift;
import static io.rowweave.examples.scenariolab.batches.FamilyA.liftB;
import static io.rowweave.examples.scenariolab.batches.FamilyB.ordProfile;

/**
 * Family C (B23 to B32): ordering and limits. orderBy under direction, null policy and several
 * keys; top(N) as a streaming limit; the managed heap behind Top; chains that follow an
 * ordering; and ordering after an aggregation. Ordered scenarios compare position by position
 * through expectOrdered, with the oracle using a stable sort under the same semantics.
 */
public final class FamilyC {

    private FamilyC() {
    }

    public static List<BatchDesign> batches() {
        return Arrays.asList(
                b23(), b24(), b25(), b26(), b27(),
                b28(), b29(), b30(), b31(), b32());
    }

    /** The 10 profiles of the L2 schema. */
    private static List<DataRow> l2Profile(int i) {
        switch (i) {
            case 0: return D.l2Sort();
            case 1: return D.empty();
            case 2: return D.rows(new Object[]{7L, 7L});
            case 3: {
                List<DataRow> b = D.l2Sort();
                List<DataRow> out = new ArrayList<>(b);
                out.addAll(b);
                return out;
            }
            case 4: return D.rows(
                    new Object[]{-1L, 2L}, new Object[]{3L, -4L}, new Object[]{0L, 0L},
                    new Object[]{-9L, 1L});
            case 5: return D.rows(
                    new Object[]{null, null}, new Object[]{null, 1L},
                    new Object[]{2L, null}, new Object[]{null, null});
            case 6: {
                List<DataRow> out = new ArrayList<>();
                for (long j = 0; j < 15; j++) {
                    out.add(D.row(j % 5, j % 3));
                }
                return out;
            }
            case 7: return D.rows(
                    new Object[]{5L, 5L}, new Object[]{5L, 5L}, new Object[]{5L, 5L},
                    new Object[]{2L, 2L});
            case 8: return D.rows(
                    new Object[]{Long.MAX_VALUE, 1L}, new Object[]{Long.MIN_VALUE, 2L},
                    new Object[]{0L, Long.MAX_VALUE});
            default: return D.rows(
                    new Object[]{3L, 1L}, new Object[]{1L, 3L}, new Object[]{2L, 2L},
                    new Object[]{1L, 1L}, new Object[]{3L, 3L}, new Object[]{2L, 1L});
        }
    }

    private static final String[] L2_PROFILES = {
            "sort", "empty", "single", "dupes", "neg", "nulls", "many", "equal", "extreme", "grid"};

    // ========================================================================
    // B23 orderBy on a single Long key with DEFAULT and NULLS policies under ASC and DESC, over 10 profiles
    // ========================================================================

    private static BatchDesign b23() {
        return new BatchDesign() {
            @Override public int batchNo() { return 23; }
            @Override public String title() { return "orderBy single key (ASC/DESC × DEFAULT/NULLS_FIRST/LAST) × 10 L2 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                String[] names = {
                        "a asc default", "a asc via Op", "a asc nullsFirst", "a asc nullsLast",
                        "b asc default", "b asc nullsFirst", "b desc default", "b desc nullsFirst",
                        "b desc nullsLast", "a asc default again"};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < names.length; v++) {
                    for (int d = 0; d < L2_PROFILES.length; d++) {
                        final int vf = v;
                        final List<DataRow> data = l2Profile(d);
                        out.add(SL.sink(id("B23", v, d), "orderBy[" + L2_PROFILES[d] + "] " + names[v],
                                D.L2, data, ds -> dag(vf, ds),
                                in -> O.sortRows(in, D.L2, spec(vf)), true));
                    }
                }
                return out;
            }

            private DataSet dag(int v, DataSet ds) {
                switch (v) {
                    case 0: return ds.orderBy("a");
                    case 1: return ds.orderBy(O.orderBy(D.L2,
                            O.sf(D.L2, "a", true, "DEFAULT")), 1, false);
                    case 2: return ds.orderBy(O.orderBy(D.L2,
                            O.sf(D.L2, "a", true, "NULLS_FIRST")), 1, false);
                    case 3: return ds.orderBy(O.orderBy(D.L2,
                            O.sf(D.L2, "a", true, "NULLS_LAST")), 1, false);
                    case 4: return ds.orderBy("b");
                    case 5: return ds.orderBy(O.orderBy(D.L2,
                            O.sf(D.L2, "b", true, "NULLS_FIRST")), 1, false);
                    case 6: return ds.orderBy(O.orderBy(D.L2,
                            O.sf(D.L2, "b", false, "DEFAULT")), 1, false);
                    case 7: return ds.orderBy(O.orderBy(D.L2,
                            O.sf(D.L2, "b", false, "NULLS_FIRST")), 1, false);
                    case 8: return ds.orderBy(O.orderBy(D.L2,
                            O.sf(D.L2, "b", false, "NULLS_LAST")), 1, false);
                    default: return ds.orderBy("a");
                }
            }

            private SortField spec(int v) {
                switch (v) {
                    case 0:
                    case 1:
                    case 9: return O.sf(D.L2, "a", true, "DEFAULT");
                    case 2: return O.sf(D.L2, "a", true, "NULLS_FIRST");
                    case 3: return O.sf(D.L2, "a", true, "NULLS_LAST");
                    case 4: return O.sf(D.L2, "b", true, "DEFAULT");
                    case 5: return O.sf(D.L2, "b", true, "NULLS_FIRST");
                    case 6: return O.sf(D.L2, "b", false, "DEFAULT");
                    case 7: return O.sf(D.L2, "b", false, "NULLS_FIRST");
                    default: return O.sf(D.L2, "b", false, "NULLS_LAST");
                }
            }
        };
    }

    // ========================================================================
    // B24 orderBy DESC with absolute null placement over KV BigDecimal and String, over 10 profiles
    // ========================================================================

    private static BatchDesign b24() {
        return new BatchDesign() {
            @Override public int batchNo() { return 24; }
            @Override public String title() { return "orderBy DESC/NULLS absolute positioning (BigDecimal+String) × 10 KV profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                SortField[][] specs = {
                        {O.sf(D.KV, "v", false, "DEFAULT")},
                        {O.sf(D.KV, "v", false, "NULLS_FIRST")},
                        {O.sf(D.KV, "v", false, "NULLS_LAST")},
                        {O.sf(D.KV, "v", true, "NULLS_FIRST")},
                        {O.sf(D.KV, "v", true, "NULLS_LAST")},
                        {O.sf(D.KV, "k", false, "DEFAULT")},
                        {O.sf(D.KV, "k", false, "NULLS_FIRST")},
                        {O.sf(D.KV, "k", false, "NULLS_LAST")},
                        {O.sf(D.KV, "v", false, "DEFAULT"), O.sf(D.KV, "k", true, "DEFAULT")},
                        {O.sf(D.KV, "k", false, "DEFAULT"), O.sf(D.KV, "v", true, "DEFAULT")}};
                String[] names = {"v desc", "v desc NF", "v desc NL", "v asc NF", "v asc NL",
                        "k desc", "k desc NF", "k desc NL", "v desc,k asc", "k desc,v asc"};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < specs.length; v++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final SortField[] spec = specs[v];
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B24", v, d), "orderBy[" + KV_PROFILES[d] + "] " + names[v],
                                D.KV, data, ds -> ds.orderBy(O.orderBy(D.KV, spec), 1, false),
                                in -> O.sortRows(in, D.KV, spec), true));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B25 orderBy over mixed multi-key specifications, 10 combinations over 10 ORD profiles
    // ========================================================================

    private static BatchDesign b25() {
        return new BatchDesign() {
            @Override public int batchNo() { return 25; }
            @Override public String title() { return "orderBy multiple keys with mixed directions and null strategies × 10 combinations × 10 ORD profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                SortField[][] specs = {
                        {O.sf(D.ORD, "grp", true, "DEFAULT"), O.sf(D.ORD, "amt", true, "DEFAULT")},
                        {O.sf(D.ORD, "grp", true, "DEFAULT"), O.sf(D.ORD, "amt", false, "DEFAULT")},
                        {O.sf(D.ORD, "amt", false, "DEFAULT"), O.sf(D.ORD, "id", true, "DEFAULT")},
                        {O.sf(D.ORD, "qty", true, "NULLS_LAST"), O.sf(D.ORD, "amt", false, "DEFAULT")},
                        {O.sf(D.ORD, "grp", false, "DEFAULT"), O.sf(D.ORD, "id", false, "DEFAULT")},
                        {O.sf(D.ORD, "id", true, "DEFAULT")},
                        {O.sf(D.ORD, "amt", true, "DEFAULT"), O.sf(D.ORD, "qty", false, "NULLS_LAST")},
                        {O.sf(D.ORD, "grp", true, "NULLS_FIRST"), O.sf(D.ORD, "amt", true, "NULLS_LAST")},
                        {O.sf(D.ORD, "id", false, "DEFAULT"), O.sf(D.ORD, "grp", true, "DEFAULT"),
                                O.sf(D.ORD, "qty", true, "DEFAULT")},
                        {O.sf(D.ORD, "amt", true, "DEFAULT"), O.sf(D.ORD, "grp", true, "DEFAULT"),
                                O.sf(D.ORD, "id", false, "DEFAULT")}};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < specs.length; v++) {
                    for (int d = 0; d < 10; d++) {
                        final SortField[] spec = specs[v];
                        final List<DataRow> data = ordProfile(d);
                        out.add(SL.sink(id("B25", v, d), "orderBy[ord" + d + "] spec#" + v,
                                D.ORD, data, ds -> ds.orderBy(O.orderBy(D.ORD, spec), 1, false),
                                in -> O.sortRows(in, D.ORD, spec), true));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B26 orderBy on string keys, 10 specifications over 10 profiles
    // ========================================================================

    private static BatchDesign b26() {
        return new BatchDesign() {
            @Override public int batchNo() { return 26; }
            @Override public String title() { return "orderBy on strings (null sorts first; unicode/whitespace) × 10 specs × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                SortField[][] specs = {
                        {O.sf(D.KV, "k", true, "DEFAULT")},
                        {O.sf(D.KV, "k", false, "DEFAULT")},
                        {O.sf(D.KV, "k", true, "NULLS_FIRST")},
                        {O.sf(D.KV, "k", true, "NULLS_LAST")},
                        {O.sf(D.KV, "k", true, "DEFAULT"), O.sf(D.KV, "v", true, "DEFAULT")},
                        {O.sf(D.KV, "v", true, "DEFAULT"), O.sf(D.KV, "k", false, "DEFAULT")},
                        {O.sf(D.KV, "k", true, "DEFAULT"), O.sf(D.KV, "v", false, "DEFAULT")},
                        {O.sf(D.KV, "v", false, "DEFAULT"), O.sf(D.KV, "k", true, "NULLS_LAST")},
                        {O.sf(D.KV, "k", true, "NULLS_FIRST"), O.sf(D.KV, "v", true, "NULLS_LAST")},
                        {O.sf(D.KV, "v", true, "NULLS_FIRST"), O.sf(D.KV, "k", true, "NULLS_FIRST")}};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < specs.length; v++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final SortField[] spec = specs[v];
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B26", v, d), "orderByStr["
                                        + KV_PROFILES[d] + "] spec#" + v,
                                D.KV, data, ds -> ds.orderBy(O.orderBy(D.KV, spec), 1, false),
                                in -> O.sortRows(in, D.KV, spec), true));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B27 top(n) as a streaming limit, 10 values of n over 10 profiles
    // ========================================================================

    private static BatchDesign b27() {
        return new BatchDesign() {
            @Override public int batchNo() { return 27; }
            @Override public String title() { return "top(n) over source order, limit(n=0/1/2/3/5/8/12/50/100/1000) × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                int[] ns = {0, 1, 2, 3, 5, 8, 12, 50, 100, 1000};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < ns.length; v++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final int n = ns[v];
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B27", v, d), "top(" + n + ")[" + KV_PROFILES[d] + "]",
                                D.KV, data, ds -> ds.top(n),
                                in -> in.subList(0, Math.min(n, in.size())), true));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B28 Top on the managed heap with a single key, 10 specifications over 10 profiles
    // ========================================================================

    private static BatchDesign b28() {
        return new BatchDesign() {
            @Override public int batchNo() { return 28; }
            @Override public String title() { return "Top over the managed heap (single-key top-N) × 10 specs × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                io.rowweave.ops.Top[] tops = {
                        O.top(D.KV, 3, O.sf(D.KV, "v", true, "DEFAULT")),
                        O.top(D.KV, 5, O.sf(D.KV, "v", false, "DEFAULT")),
                        O.top(D.KV, 1, O.sf(D.KV, "v", true, "DEFAULT")),
                        O.top(D.KV, 2, O.sf(D.KV, "k", true, "DEFAULT")),
                        O.top(D.KV, 4, O.sf(D.KV, "v", true, "NULLS_FIRST")),
                        O.top(D.KV, 3, O.sf(D.KV, "v", true, "NULLS_LAST")),
                        O.top(D.KV, 10, O.sf(D.KV, "v", false, "NULLS_LAST")),
                        O.top(D.KV, 2, O.sf(D.KV, "k", false, "DEFAULT")),
                        O.top(D.KV, 6, O.sf(D.KV, "v", true, "DEFAULT")),
                        O.top(D.KV, 3, O.sf(D.KV, "v", false, "DEFAULT"),
                                O.sf(D.KV, "k", true, "DEFAULT"))};
                String[] names = {"top3 v asc", "top5 v desc", "top1 v asc", "top2 k asc",
                        "top4 v asc NF", "top3 v asc NL", "top10 v desc NL", "top2 k desc",
                        "top6 v asc", "top3 v desc,k asc"};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < tops.length; v++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final io.rowweave.ops.Top op = tops[v];
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B28", v, d), "Top[" + KV_PROFILES[d] + "] " + names[v],
                                D.KV, data, ds -> ds.top(op, 1),
                                in -> O.sortRows(in, D.KV, op.sortFields())
                                        .subList(0, Math.min(op.n(), in.size())), false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B29 Top with several keys, 10 specifications over 10 L2 profiles
    // ========================================================================

    private static BatchDesign b29() {
        return new BatchDesign() {
            @Override public int batchNo() { return 29; }
            @Override public String title() { return "Top with multiple keys (a/b long combinations) × 10 specs × 10 L2 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                io.rowweave.ops.Top[] tops = {
                        O.top(D.L2, 3, O.sf(D.L2, "a", true, "DEFAULT"), O.sf(D.L2, "b", true, "DEFAULT")),
                        O.top(D.L2, 2, O.sf(D.L2, "a", false, "DEFAULT"), O.sf(D.L2, "b", false, "DEFAULT")),
                        O.top(D.L2, 4, O.sf(D.L2, "b", true, "NULLS_FIRST")),
                        O.top(D.L2, 5, O.sf(D.L2, "a", true, "NULLS_LAST"), O.sf(D.L2, "b", false, "DEFAULT")),
                        O.top(D.L2, 1, O.sf(D.L2, "b", false, "DEFAULT")),
                        O.top(D.L2, 6, O.sf(D.L2, "a", false, "NULLS_FIRST")),
                        O.top(D.L2, 3, O.sf(D.L2, "b", true, "DEFAULT"), O.sf(D.L2, "a", true, "NULLS_LAST")),
                        O.top(D.L2, 2, O.sf(D.L2, "a", true, "DEFAULT")),
                        O.top(D.L2, 8, O.sf(D.L2, "b", false, "NULLS_LAST"), O.sf(D.L2, "a", true, "DEFAULT")),
                        O.top(D.L2, 4, O.sf(D.L2, "a", true, "DEFAULT"), O.sf(D.L2, "b", true, "NULLS_FIRST"))};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < tops.length; v++) {
                    for (int d = 0; d < L2_PROFILES.length; d++) {
                        final io.rowweave.ops.Top op = tops[v];
                        final List<DataRow> data = l2Profile(d);
                        out.add(SL.sink(id("B29", v, d), "TopL2[" + L2_PROFILES[d] + "] spec#" + v,
                                D.L2, data, ds -> ds.top(op, 1),
                                in -> O.sortRows(in, D.L2, op.sortFields())
                                        .subList(0, Math.min(op.n(), in.size())), false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B30 chained orderBy and top, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b30() {
        return new BatchDesign() {
            @Override public int batchNo() { return 30; }
            @Override public String title() { return "chained orderBy/top (sort→limit→sort, distinct→sort) × 10 templates"; }
            @Override public List<ScenarioDef> scenarios() {
                List<Chain> chains = Arrays.asList(
                        new Chain("orderBy(v).top(3)",
                                ds -> ds.orderBy(O.orderBy(D.KV, O.sf(D.KV, "v", true, "DEFAULT")), 1, false).top(3),
                                in -> O.sortRows(in, D.KV, O.sf(D.KV, "v", true, "DEFAULT"))
                                        .subList(0, Math.min(3, in.size())), true),
                        new Chain("top(3).orderBy(v)",
                                ds -> ds.top(3).orderBy("v"),
                                in -> O.sortRows(in.subList(0, Math.min(3, in.size())), D.KV,
                                        O.sf(D.KV, "v", true, "DEFAULT")), true),
                        new Chain("orderBy(v desc).top(2)",
                                ds -> ds.orderBy(O.orderBy(D.KV, O.sf(D.KV, "v", false, "DEFAULT")), 1, false).top(2),
                                in -> O.sortRows(in, D.KV, O.sf(D.KV, "v", false, "DEFAULT"))
                                        .subList(0, Math.min(2, in.size())), true),
                        new Chain("f(v>0).orderBy(v)",
                                ds -> ds.filter("v > 0").orderBy("v"),
                                in -> O.sortRows(SL.filterRows(in,
                                        r -> liftB(r.getBigDecimal(1), x -> x.signum() > 0)),
                                        D.KV, O.sf(D.KV, "v", true, "DEFAULT")), true),
                        new Chain("orderBy(k).f(v>5)",
                                ds -> ds.orderBy("k").filter("v > 5"),
                                in -> SL.filterRows(O.sortRows(in, D.KV, O.sf(D.KV, "k", true, "DEFAULT")),
                                        r -> vCmp(r, 0, BigDecimal.valueOf(5))), true),
                        new Chain("distinct().orderBy(k)",
                                ds -> ds.distinct().orderBy("k"),
                                in -> O.sortRows(O.distinctAll(in), D.KV, O.sf(D.KV, "k", true, "DEFAULT")),
                                true),
                        new Chain("orderBy(v).distinct()",
                                ds -> ds.orderBy(O.orderBy(D.KV, O.sf(D.KV, "v", true, "DEFAULT")), 1, false).distinct(),
                                in -> O.distinctAll(O.sortRows(in, D.KV, O.sf(D.KV, "v", true, "DEFAULT"))),
                                false),
                        new Chain("top(5).top(3)",
                                ds -> ds.top(5).top(3),
                                in -> in.subList(0, Math.min(3, Math.min(5, in.size()))), true),
                        new Chain("orderBy(v,k).orderBy(k,v) re-sort",
                                ds -> ds.orderBy(O.orderBy(D.KV, O.sf(D.KV, "v", true, "DEFAULT"),
                                        O.sf(D.KV, "k", true, "DEFAULT")), 1, false)
                                        .orderBy(O.orderBy(D.KV, O.sf(D.KV, "k", true, "DEFAULT"),
                                                O.sf(D.KV, "v", true, "DEFAULT")), 1, false),
                                in -> O.sortRows(O.sortRows(in, D.KV,
                                        O.sf(D.KV, "v", true, "DEFAULT"), O.sf(D.KV, "k", true, "DEFAULT")),
                                        D.KV, O.sf(D.KV, "k", true, "DEFAULT"),
                                        O.sf(D.KV, "v", true, "DEFAULT")), true),
                        new Chain("orderBy(k).top(4).orderBy(v desc)",
                                ds -> ds.orderBy("k").top(4)
                                        .orderBy(O.orderBy(D.KV, O.sf(D.KV, "v", false, "DEFAULT")), 1, false),
                                in -> O.sortRows(
                                        O.sortRows(in, D.KV, O.sf(D.KV, "k", true, "DEFAULT"))
                                                .subList(0, Math.min(4, in.size())),
                                        D.KV, O.sf(D.KV, "v", false, "DEFAULT")), true));
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < chains.size(); t++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Chain chain = chains.get(t);
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B30", t, d), "sortChain[" + KV_PROFILES[d] + "] " + chain.name,
                                D.KV, data, chain.dag, chain.oracle, chain.ordered));
                    }
                }
                return out;
            }
        };
    }

    static final class Chain {
        final String name;
        final Function<DataSet, DataSet> dag;
        final Function<List<DataRow>, List<DataRow>> oracle;
        final boolean ordered;

        Chain(String name, Function<DataSet, DataSet> dag,
              Function<List<DataRow>, List<DataRow>> oracle, boolean ordered) {
            this.name = name;
            this.dag = dag;
            this.oracle = oracle;
            this.ordered = ordered;
        }
    }

    // ========================================================================
    // B31 ordering after aggregation, 10 combinations over 10 profiles
    // ========================================================================

    private static BatchDesign b31() {
        final DataSchema SUM_META = new DataSchema(D.f("k", DataType.StringType),
                D.f("sum0", DataType.BigDecimalType));
        final DataSchema S_META = new DataSchema(D.f("k", DataType.StringType),
                D.f("s", DataType.BigDecimalType));
        final DataSchema COUNT_META = new DataSchema(D.f("k", DataType.StringType),
                D.f("count0", DataType.LongType));
        final DataSchema MAX_META = new DataSchema(D.f("k", DataType.StringType),
                D.f("max0", DataType.BigDecimalType));
        final DataSchema MIN_META = new DataSchema(D.f("k", DataType.StringType),
                D.f("min0", DataType.BigDecimalType));
        return new BatchDesign() {
            @Override public int batchNo() { return 31; }
            @Override public String title() { return "sort after aggregation (sum/count/max/min × key order/value order) × 10 combinations × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<Chain> combos = Arrays.asList(
                        new Chain("sum→k asc", ds -> ds.groupBy("k").sum("v").orderBy("k"),
                                in -> O.sortRows(O.groupSum(in, O.idx(0), 1), SUM_META,
                                        O.sf(SUM_META, "k", true, "DEFAULT")), true),
                        new Chain("sum→sum desc", ds -> ds.groupBy("k").sum("v")
                                        .orderBy(O.orderBy(SUM_META, O.sf(SUM_META, "sum0", false, "DEFAULT")), 1, false),
                                in -> O.sortRows(O.groupSum(in, O.idx(0), 1), SUM_META,
                                        O.sf(SUM_META, "sum0", false, "DEFAULT")), true),
                        new Chain("count→k asc", ds -> ds.groupBy("k").count().orderBy("k"),
                                in -> O.sortRows(O.groupCount(in, O.idx(0)), COUNT_META,
                                        O.sf(COUNT_META, "k", true, "DEFAULT")), true),
                        new Chain("count→count desc", ds -> ds.groupBy("k").count()
                                        .orderBy(O.orderBy(COUNT_META, O.sf(COUNT_META, "count0", false, "DEFAULT")), 1, false),
                                in -> O.sortRows(O.groupCount(in, O.idx(0)), COUNT_META,
                                        O.sf(COUNT_META, "count0", false, "DEFAULT")), true),
                        new Chain("max→k asc", ds -> ds.groupBy("k").max("v").orderBy("k"),
                                in -> O.sortRows(O.groupMinMax(in, O.idx(0), 1, true), MAX_META,
                                        O.sf(MAX_META, "k", true, "DEFAULT")), true),
                        new Chain("max→max desc", ds -> ds.groupBy("k").max("v")
                                        .orderBy(O.orderBy(MAX_META, O.sf(MAX_META, "max0", false, "DEFAULT")), 1, false),
                                in -> O.sortRows(O.groupMinMax(in, O.idx(0), 1, true), MAX_META,
                                        O.sf(MAX_META, "max0", false, "DEFAULT")), true),
                        new Chain("min→k asc", ds -> ds.groupBy("k").min("v").orderBy("k"),
                                in -> O.sortRows(O.groupMinMax(in, O.idx(0), 1, false), MIN_META,
                                        O.sf(MIN_META, "k", true, "DEFAULT")), true),
                        new Chain("min→min asc", ds -> ds.groupBy("k").min("v")
                                        .orderBy(O.orderBy(MIN_META, O.sf(MIN_META, "min0", true, "DEFAULT")), 1, false),
                                in -> O.sortRows(O.groupMinMax(in, O.idx(0), 1, false), MIN_META,
                                        O.sf(MIN_META, "min0", true, "DEFAULT")), true),
                        new Chain("sum(s)→s desc NL", ds -> ds.groupBy("k").sum("v", "s")
                                        .orderBy(O.orderBy(S_META, O.sf(S_META, "s", false, "NULLS_LAST")), 1, false),
                                in -> O.sortRows(O.groupSum(in, O.idx(0), 1), S_META,
                                        O.sf(S_META, "s", false, "NULLS_LAST")), true),
                        new Chain("sum(s)→s asc NL", ds -> ds.groupBy("k").sum("v", "s")
                                        .orderBy(O.orderBy(S_META, O.sf(S_META, "s", true, "NULLS_LAST")), 1, false),
                                in -> O.sortRows(O.groupSum(in, O.idx(0), 1), S_META,
                                        O.sf(S_META, "s", true, "NULLS_LAST")), true));
                List<ScenarioDef> out = new ArrayList<>();
                for (int c = 0; c < combos.size(); c++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Chain combo = combos.get(c);
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B31", c, d), "aggSort[" + KV_PROFILES[d] + "] " + combo.name,
                                D.KV, data, combo.dag, combo.oracle, true));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B32 reordering and stable order among equal keys, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b32() {
        return new BatchDesign() {
            @Override public int batchNo() { return 32; }
            @Override public String title() { return "re-sort / equal key order (full-column total order asserted per position; single key as multiset) × 10 templates"; }
            @Override public List<ScenarioDef> scenarios() {
                List<Chain> chains = Arrays.asList(
                        new Chain("orderBy(v,k) total order",
                                ds -> ds.orderBy(O.orderBy(D.KV,
                                        O.sf(D.KV, "v", true, "DEFAULT"),
                                        O.sf(D.KV, "k", true, "DEFAULT")), 1, false),
                                in -> O.sortRows(in, D.KV,
                                        O.sf(D.KV, "v", true, "DEFAULT"),
                                        O.sf(D.KV, "k", true, "DEFAULT")), true),
                        new Chain("orderBy(k,v) total order",
                                ds -> ds.orderBy(O.orderBy(D.KV,
                                        O.sf(D.KV, "k", true, "DEFAULT"),
                                        O.sf(D.KV, "v", true, "DEFAULT")), 1, false),
                                in -> O.sortRows(in, D.KV,
                                        O.sf(D.KV, "k", true, "DEFAULT"),
                                        O.sf(D.KV, "v", true, "DEFAULT")), true),
                        new Chain("orderBy(v desc,k desc) total order",
                                ds -> ds.orderBy(O.orderBy(D.KV,
                                        O.sf(D.KV, "v", false, "DEFAULT"),
                                        O.sf(D.KV, "k", false, "DEFAULT")), 1, false),
                                in -> O.sortRows(in, D.KV,
                                        O.sf(D.KV, "v", false, "DEFAULT"),
                                        O.sf(D.KV, "k", false, "DEFAULT")), true),
                        new Chain("orderBy(k asc) single-key multiset",
                                ds -> ds.orderBy(O.orderBy(D.KV, O.sf(D.KV, "k", true, "DEFAULT")), 1, false),
                                in -> O.sortRows(in, D.KV, O.sf(D.KV, "k", true, "DEFAULT")), false),
                        new Chain("orderBy(v asc) single-key multiset",
                                ds -> ds.orderBy(O.orderBy(D.KV, O.sf(D.KV, "v", true, "DEFAULT")), 1, false),
                                in -> O.sortRows(in, D.KV, O.sf(D.KV, "v", true, "DEFAULT")), false),
                        new Chain("orderBy(v asc NF) single-key multiset",
                                ds -> ds.orderBy(O.orderBy(D.KV, O.sf(D.KV, "v", true, "NULLS_FIRST")), 1, false),
                                in -> O.sortRows(in, D.KV, O.sf(D.KV, "v", true, "NULLS_FIRST")), false),
                        new Chain("distinct→orderBy(v,k) total order",
                                ds -> ds.distinct()
                                        .orderBy(O.orderBy(D.KV, O.sf(D.KV, "v", true, "DEFAULT"),
                                                O.sf(D.KV, "k", true, "DEFAULT")), 1, false),
                                in -> O.sortRows(O.distinctAll(in), D.KV,
                                        O.sf(D.KV, "v", true, "DEFAULT"),
                                        O.sf(D.KV, "k", true, "DEFAULT")), true),
                        new Chain("map(upper)→orderBy(k,v) total order",
                                ds -> ds.map(FamilyA::upperK)
                                        .orderBy(O.orderBy(D.KV, O.sf(D.KV, "k", true, "DEFAULT"),
                                                O.sf(D.KV, "v", true, "DEFAULT")), 1, false),
                                in -> O.sortRows(SL.mapRows(in, FamilyA::upperK), D.KV,
                                        O.sf(D.KV, "k", true, "DEFAULT"),
                                        O.sf(D.KV, "v", true, "DEFAULT")), true),
                        new Chain("orderBy(v,k) sorted twice the same way",
                                ds -> ds.orderBy(O.orderBy(D.KV, O.sf(D.KV, "v", true, "DEFAULT"),
                                        O.sf(D.KV, "k", true, "DEFAULT")), 1, false)
                                        .orderBy(O.orderBy(D.KV, O.sf(D.KV, "v", true, "DEFAULT"),
                                                O.sf(D.KV, "k", true, "DEFAULT")), 1, false),
                                in -> {
                                    List<DataRow> once = O.sortRows(in, D.KV,
                                            O.sf(D.KV, "v", true, "DEFAULT"),
                                            O.sf(D.KV, "k", true, "DEFAULT"));
                                    return O.sortRows(once, D.KV,
                                            O.sf(D.KV, "v", true, "DEFAULT"),
                                            O.sf(D.KV, "k", true, "DEFAULT"));
                                }, true),
                        new Chain("top(100) keeps every row as a multiset",
                                ds -> ds.top(100),
                                java.util.function.Function.identity(), false));
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < chains.size(); t++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final Chain chain = chains.get(t);
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B32", t, d), "resort[" + KV_PROFILES[d] + "] " + chain.name,
                                D.KV, data, chain.dag, chain.oracle, chain.ordered));
                    }
                }
                return out;
            }
        };
    }
}
