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
import io.rowweave.ops.join.JoinType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.rowweave.examples.scenariolab.batches.FamilyA.id;
import static io.rowweave.examples.scenariolab.batches.FamilyA.kvProfile;
import static io.rowweave.examples.scenariolab.batches.FamilyA.KV_PROFILES;
import static io.rowweave.examples.scenariolab.batches.FamilyA.liftB;
import static io.rowweave.examples.scenariolab.batches.FamilyD.dimProfile;

/**
 * Family G (B63 to B72): parallelism 2, 3 and 4. A keyed breaker, join, union, deep chain and
 * several sinks have to mean the same thing as at parallelism 1, compared as a multiset, and
 * for ordered scenarios position by position once canonicalize is on.
 */
public final class FamilyG {

    private FamilyG() {
    }

    public static List<BatchDesign> batches() {
        return Arrays.asList(
                b63(), b64(), b65(), b66(), b67(),
                b68(), b69(), b70(), b71(), b72());
    }

    private static final int[] PS = {2, 2, 3, 4, 2, 2, 3, 4, 2, 3};

    // ========================================================================
    // B63 groupBy sum at parallelism 2, 3 and 4, 10 variants over 10 profiles
    // ========================================================================

    private static BatchDesign b63() {
        return new BatchDesign() {
            @Override public int batchNo() { return 63; }
            @Override public String title() { return "groupBy at P=2/3/4 (result equals the P=1 multiset) × 10 variants × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        final int p = PS[v];
                        out.add(ScenarioDef.of(id("B63", v, d), "P" + p + " groupBy[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                lab -> {
                                    lab.parallelism(p);
                                    DataSet src = lab.source(D.KV, data);
                                    DataSet res = dag(vf, src);
                                    List<DataRow> expected = oracle(vf, data);
                                    if (vf == 9) {
                                        lab.expectOrdered(res, expected);
                                        lab.canonicalize(true);
                                    } else {
                                        lab.expect(res, expected);
                                    }
                                }));
                    }
                }
                return out;
            }

            private DataSet dag(int v, DataSet ds) {
                switch (v) {
                    case 0: return ds.groupBy("k").sum("v");
                    case 1: return ds.groupBy("k").count();
                    case 2: return ds.groupBy("k").max("v");
                    case 3: return ds.groupBy("k").min("v");
                    case 4: return ds.groupBy("k", "v").count();
                    case 5: return ds.groupBy(O.pga(D.KV, "k").sum("v").count());
                    case 6: return ds.filter("v > 0").groupBy("k").sum("v");
                    case 7: return ds.distinct().groupBy("k").count();
                    case 8: return ds.union(ds).groupBy("k").sum("v");
                    default: return ds.groupBy("k").sum("v").orderBy("k");
                }
            }

            private List<DataRow> oracle(int v, List<DataRow> in) {
                switch (v) {
                    case 0: return O.groupSum(in, O.idx(0), 1);
                    case 1: return O.groupCount(in, O.idx(0));
                    case 2: return O.groupMinMax(in, O.idx(0), 1, true);
                    case 3: return O.groupMinMax(in, O.idx(0), 1, false);
                    case 4: return O.groupCount(in, O.idx(0, 1));
                    case 5: return O.groupMultiAgg(in, O.idx(0), Arrays.asList(
                            O.Agg.sum(1), O.Agg.countAll()));
                    case 6: return O.groupSum(SL.filterRows(in,
                            r -> liftB(r.getBigDecimal(1), x -> x.signum() > 0)), O.idx(0), 1);
                    case 7: return O.groupCount(O.distinctAll(in), O.idx(0));
                    case 8: return O.groupSum(SL.concat(in, in), O.idx(0), 1);
                    default: {
                        List<DataRow> summed = O.groupSum(in, O.idx(0), 1);
                        DataSchema m = new DataSchema(D.f("k", DataType.StringType),
                                D.f("sum0", DataType.BigDecimalType));
                        return O.sortRows(summed, m, O.sf(m, "k", true, "DEFAULT"));
                    }
                }
            }
        };
    }

    // ========================================================================
    // B64 groupBy with several aggregations at parallelism 2, 3 and 4, 10 variants over 10 ORD profiles
    // ========================================================================

    private static BatchDesign b64() {
        return new BatchDesign() {
            @Override public int batchNo() { return 64; }
            @Override public String title() { return "groupBy with multiple aggregates at P=2/3/4 (sum+count+max/min) × 10 variants × 10 ORD profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < 10; d++) {
                        final List<DataRow> data = FamilyB.ordProfile(d);
                        final int p = PS[v];
                        out.add(ScenarioDef.of(id("B64", v, d), "P" + p + " multiAgg[v" + vf + "][ord" + d + "]",
                                lab -> {
                                    lab.parallelism(p);
                                    DataSet src = lab.source(D.ORD, data);
                                    DataSet res = dag(vf, src);
                                    lab.expect(res, oracle(vf, data));
                                }));
                    }
                }
                return out;
            }

            private DataSet dag(int v, DataSet ds) {
                switch (v) {
                    case 0: return ds.groupBy(O.pga(D.ORD, "grp").sum("amt").count());
                    case 1: return ds.groupBy(O.pga(D.ORD, "grp").count().max("amt"));
                    case 2: return ds.groupBy(O.pga(D.ORD, "grp").min("amt").max("amt"));
                    case 3: return ds.groupBy(O.pga(D.ORD, "grp").countField("amt").countField("qty"));
                    case 4: return ds.groupBy(O.pga(D.ORD, "grp", "id").sum("amt"));
                    case 5: return ds.groupBy(O.pga(D.ORD, "grp").sum("amt").count().max("qty"));
                    case 6: return ds.groupBy(O.pga(D.ORD, "qty").count());
                    case 7: return ds.groupBy(O.pga(D.ORD, "id").min("amt"));
                    case 8: return ds.filter("amt > 0").groupBy(O.pga(D.ORD, "grp").sum("amt"));
                    default: return ds.groupBy(O.pga(D.ORD, "grp").max("id").min("id"));
                }
            }

            private List<DataRow> oracle(int v, List<DataRow> in) {
                switch (v) {
                    case 0: return O.groupMultiAgg(in, O.idx(1), Arrays.asList(
                            O.Agg.sum(2), O.Agg.countAll()));
                    case 1: return O.groupMultiAgg(in, O.idx(1), Arrays.asList(
                            O.Agg.countAll(), O.Agg.max(2)));
                    case 2: return O.groupMultiAgg(in, O.idx(1), Arrays.asList(
                            O.Agg.min(2), O.Agg.max(2)));
                    case 3: return O.groupMultiAgg(in, O.idx(1), Arrays.asList(
                            O.Agg.countField(2), O.Agg.countField(3)));
                    case 4: return O.groupMultiAgg(in, O.idx(1, 0), Arrays.asList(O.Agg.sum(2)));
                    case 5: return O.groupMultiAgg(in, O.idx(1), Arrays.asList(
                            O.Agg.sum(2), O.Agg.countAll(), O.Agg.max(3)));
                    case 6: return O.groupMultiAgg(in, O.idx(3), Arrays.asList(O.Agg.countAll()));
                    case 7: return O.groupMultiAgg(in, O.idx(0), Arrays.asList(O.Agg.min(2)));
                    case 8: return O.groupMultiAgg(SL.filterRows(in,
                            r -> liftB(r.getBigDecimal(2), x -> x.signum() > 0)),
                            O.idx(1), Arrays.asList(O.Agg.sum(2)));
                    default: return O.groupMultiAgg(in, O.idx(1), Arrays.asList(
                            O.Agg.max(0), O.Agg.min(0)));
                }
            }
        };
    }

    // ========================================================================
    // B65 join at parallelism 2, 3 and 4, 10 types and keys over 10 profiles
    // ========================================================================

    private static BatchDesign b65() {
        return new BatchDesign() {
            @Override public int batchNo() { return 65; }
            @Override public String title() { return "join at P=2/3/4 (4 types + hint rotation) × 10 variants × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                JoinType[] types = {JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL,
                        JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL,
                        JoinType.INNER, JoinType.LEFT};
                for (int v = 0; v < types.length; v++) {
                    final JoinType type = types[v];
                    final int p = PS[v];
                    final int vv = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(ScenarioDef.of(id("B65", v, d), "P" + p + " join[" + type + "]["
                                        + KV_PROFILES[d] + "]",
                                lab -> {
                                    lab.parallelism(p);
                                    DataSet a = lab.source(D.KV, left);
                                    DataSet b = lab.source(D.DIM, right);
                                    DataSet res;
                                    if (vv >= 8) {
                                        res = a.join(b, io.rowweave.ops.join.SortMergeJoin.of(
                                                a.getSchema(), b.getSchema(),
                                                new String[]{"k"}, new String[]{"k"}, type),
                                                io.rowweave.ops.join.JoinHint.HASH);
                                    } else {
                                        res = a.join(b, io.rowweave.ops.join.SortMergeJoin.of(
                                                a.getSchema(), b.getSchema(),
                                                new String[]{"k"}, new String[]{"k"}, type));
                                    }
                                    lab.expect(res, O.joinRows(left, right, O.idx(0), O.idx(0),
                                            type, 2, 2));
                                }));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B66 ordered comparison after orderBy canonicalize at parallelism 2, 10 specifications over 10 profiles
    // ========================================================================

    private static BatchDesign b66() {
        return new BatchDesign() {
            @Override public int batchNo() { return 66; }
            @Override public String title() { return "orderBy at P=2 (canonicalize=true, compared per position) × 10 specs × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                io.rowweave.ops.SortField[][] specs = {
                        {O.sf(D.KV, "k", true, "DEFAULT"), O.sf(D.KV, "v", true, "DEFAULT")},
                        {O.sf(D.KV, "k", false, "DEFAULT"), O.sf(D.KV, "v", false, "DEFAULT")},
                        {O.sf(D.KV, "v", true, "DEFAULT"), O.sf(D.KV, "k", true, "DEFAULT")},
                        {O.sf(D.KV, "v", false, "DEFAULT"), O.sf(D.KV, "k", false, "DEFAULT")},
                        {O.sf(D.KV, "k", true, "NULLS_FIRST"), O.sf(D.KV, "v", true, "NULLS_LAST")},
                        {O.sf(D.KV, "k", false, "NULLS_LAST"), O.sf(D.KV, "v", true, "DEFAULT")},
                        {O.sf(D.KV, "v", true, "NULLS_FIRST")},
                        {O.sf(D.KV, "v", false, "NULLS_LAST")},
                        {O.sf(D.KV, "k", true, "DEFAULT")},
                        {O.sf(D.KV, "k", false, "DEFAULT")}};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < specs.length; v++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final io.rowweave.ops.SortField[] spec = specs[v];
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B66", v, d), "P2 orderBy canon["
                                        + KV_PROFILES[d] + "] spec#" + v,
                                lab -> {
                                    lab.parallelism(2);
                                    lab.canonicalize(true);
                                    DataSet res = lab.source(D.KV, data)
                                            .orderBy(O.orderBy(D.KV, spec), 1, false);
                                    lab.expectOrdered(res, O.sortRows(data, D.KV, spec));
                                }));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B67 a mix of union, distinct and groupBy at parallelism 2, 3 and 4, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b67() {
        return new BatchDesign() {
            @Override public int batchNo() { return 67; }
            @Override public String title() { return "union+distinct+groupBy mix at P=2/3/4 × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    final int p = PS[v];
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = kvProfile((d + 4) % 10);
                        out.add(ScenarioDef.of(id("B67", v, d), "P" + p + " mix[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                lab -> {
                                    lab.parallelism(p);
                                    DataSet a = lab.source("A", D.KV, left);
                                    DataSet b = lab.source("B", D.KV, right);
                                    DataSet res;
                                    List<DataRow> expected;
                                    switch (vf) {
                                        case 0:
                                            res = a.union(b).distinct();
                                            expected = O.distinctAll(SL.concat(left, right));
                                            break;
                                        case 1:
                                            res = a.union(b).groupBy("k").sum("v");
                                            expected = O.groupSum(SL.concat(left, right), O.idx(0), 1);
                                            break;
                                        case 2:
                                            res = a.union(b).groupBy("k").count().orderBy("k");
                                            expected = O.groupCount(SL.concat(left, right), O.idx(0));
                                            lab.expect(res, expected);
                                            return;
                                        case 3:
                                            res = a.distinct().union(b.distinct()).distinct();
                                            expected = O.distinctAll(SL.concat(
                                                    O.distinctAll(left), O.distinctAll(right)));
                                            break;
                                        case 4:
                                            res = a.groupBy("k").sum("v").union(b.groupBy("k").sum("v"))
                                                    .groupBy("k").sum("sum0");
                                            expected = O.groupSum(SL.concat(
                                                    O.groupSum(left, O.idx(0), 1),
                                                    O.groupSum(right, O.idx(0), 1)), O.idx(0), 1);
                                            break;
                                        case 5:
                                            res = a.union(b).filter("v > 0").distinct()
                                                    .groupBy("k").count();
                                            expected = O.groupCount(O.distinctAll(SL.filterRows(
                                                    SL.concat(left, right),
                                                    r -> liftB(r.getBigDecimal(1), x -> x.signum() > 0))),
                                                    O.idx(0));
                                            break;
                                        case 6:
                                            res = a.union(b)
                                                    .orderBy(O.orderBy(D.KV,
                                                            O.sf(D.KV, "k", true, "DEFAULT"),
                                                            O.sf(D.KV, "v", true, "DEFAULT")), 1, false)
                                                    .top(6);
                                            expected = O.sortRows(SL.concat(left, right), D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"),
                                                    O.sf(D.KV, "v", true, "DEFAULT"))
                                                    .subList(0, Math.min(6, left.size() + right.size()));
                                            lab.expectOrdered(res, expected);
                                            return;
                                        case 7:
                                            res = a.union(b).union(a).distinct();
                                            expected = O.distinctAll(SL.concat(left, right, left));
                                            break;
                                        case 8:
                                            res = a.groupBy("k").count().union(b.groupBy("k").count());
                                            expected = SL.concat(O.groupCount(left, O.idx(0)),
                                                    O.groupCount(right, O.idx(0)));
                                            break;
                                        default:
                                            res = a.union(b).partitionByHash("k").groupBy("k").max("v");
                                            expected = O.groupMinMax(SL.concat(left, right),
                                                    O.idx(0), 1, true);
                                            break;
                                    }
                                    lab.expect(res, expected);
                                }));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B68 several sinks at parallelism 2, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b68() {
        return new BatchDesign() {
            @Override public int batchNo() { return 68; }
            @Override public String title() { return "one source, several sinks at P=2 × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B68", v, d), "P2 sinks[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    lab.parallelism(2);
                                    DataSet src = lab.source(D.KV, data);
                                    lab.expect(src.groupBy("k").sum("v"),
                                            O.groupSum(data, O.idx(0), 1));
                                    lab.expect(src.filter("v > 0"),
                                            SL.filterRows(data, r -> liftB(r.getBigDecimal(1),
                                                    x -> x.signum() > 0)));
                                    if (vf % 2 == 0) {
                                        lab.expect(src.distinct(), O.distinctAll(data));
                                    } else {
                                        lab.expect(src, data);
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B69 diamond fan-out at parallelism 2, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b69() {
        return new BatchDesign() {
            @Override public int batchNo() { return 69; }
            @Override public String title() { return "diamond fan-out at P=2 (shared source forks then converges) × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B69", v, d), "P2 diamond[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                lab -> {
                                    lab.parallelism(2);
                                    DataSet src = lab.source(D.KV, data);
                                    DataSet a = src.filter("v > 0");
                                    DataSet b = src.filter("v <= 0 or v is null");
                                    DataSet res;
                                    List<DataRow> expA = SL.filterRows(data, r -> liftB(r.getBigDecimal(1),
                                            x -> x.signum() > 0));
                                    List<DataRow> expB = SL.filterRows(data, r -> r.get(1) == null
                                            || r.getBigDecimal(1).signum() <= 0);
                                    switch (vf) {
                                        case 0:
                                            res = a.union(b);
                                            lab.expect(res, SL.concat(expA, expB));
                                            break;
                                        case 1:
                                            res = a.union(a);
                                            lab.expect(res, SL.concat(expA, expA));
                                            break;
                                        case 2:
                                            res = src.union(src);
                                            lab.expect(res, SL.concat(data, data));
                                            break;
                                        case 3:
                                            res = src.groupBy("k").sum("v");
                                            lab.expect(res, O.groupSum(data, O.idx(0), 1));
                                            lab.expect(src, data);
                                            break;
                                        case 4:
                                            res = a.groupBy("k").sum("v");
                                            lab.expect(res, O.groupSum(expA, O.idx(0), 1));
                                            lab.expect(b, expB);
                                            break;
                                        case 5:
                                            res = src.distinct();
                                            lab.expect(res, O.distinctAll(data));
                                            lab.expect(src.union(src), SL.concat(data, data));
                                            break;
                                        case 6:
                                            res = a.union(b).groupBy("k").count();
                                            lab.expect(res, O.groupCount(SL.concat(expA, expB), O.idx(0)));
                                            break;
                                        case 7:
                                            res = src.join(src).on("k");
                                            lab.expect(res, O.joinRows(data, data, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2));
                                            break;
                                        case 8:
                                            res = a.distinct();
                                            lab.expect(res, O.distinctAll(expA));
                                            lab.expect(src.top(3),
                                                    data.subList(0, Math.min(3, data.size())));
                                            break;
                                        default:
                                            res = src.union(b).union(a);
                                            lab.expect(res, SL.concat(data, expB, expA));
                                            break;
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B70 a deep chain at parallelism 2, 10 depths over 10 profiles
    // ========================================================================

    private static BatchDesign b70() {
        return new BatchDesign() {
            @Override public int batchNo() { return 70; }
            @Override public String title() { return "deep chain at P=2 (alternating map+filter, depth 2..20) × 10 depths × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int depthIdx = 0; depthIdx < 10; depthIdx++) {
                    final int depth = 2 + depthIdx;  // 2..11, bounded by the thread cost limit
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B70", depthIdx, d), "P2 deep[depth=" + depth
                                        + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    lab.parallelism(2);
                                    DataSet cur = lab.source(D.KV, data);
                                    List<DataRow> expected = data;
                                    for (int i = 0; i < depth; i++) {
                                        if (i % 2 == 0) {
                                            DataSet fc = cur;
                                            cur = fc.map(r -> {
                                                DataRow o = r.copy();
                                                BigDecimal v = o.getBigDecimal(1);
                                                o.set(1, v == null ? null : v.add(BigDecimal.ONE));
                                                return o;
                                            });
                                            expected = SL.mapRows(expected, r -> {
                                                DataRow o = r.copy();
                                                BigDecimal v = o.getBigDecimal(1);
                                                o.set(1, v == null ? null : v.add(BigDecimal.ONE));
                                                return o;
                                            });
                                        } else {
                                            DataSet fc = cur;
                                            cur = fc.filter("v > -1000");
                                            expected = SL.filterRows(expected, r -> r.get(1) != null
                                                    && r.getBigDecimal(1).compareTo(BigDecimal.valueOf(-1000)) > 0);
                                        }
                                    }
                                    lab.expect(cur, expected);
                                }));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B71 a join to groupBy chain at parallelism 2, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b71() {
        return new BatchDesign() {
            @Override public int batchNo() { return 71; }
            @Override public String title() { return "join→groupBy chain at P=2 × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(ScenarioDef.of(id("B71", v, d), "P2 joinGroup[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                lab -> {
                                    lab.parallelism(2);
                                    DataSet a = lab.source(D.KV, left);
                                    DataSet b = lab.source(D.DIM, right);
                                    List<DataRow> joined = O.joinRows(left, right, O.idx(0), O.idx(0),
                                            JoinType.INNER, 2, 2);
                                    DataSet j = a.join(b).on("k");
                                    switch (vf) {
                                        case 0:
                                            lab.expect(j.groupBy("k").count(),
                                                    O.groupCount(joined, O.idx(0)));
                                            break;
                                        case 1:
                                            lab.expect(j.groupBy("k").sum("v"),
                                                    O.groupSum(joined, O.idx(0), 1));
                                            break;
                                        case 2:
                                            lab.expect(j.groupBy("label").count(),
                                                    O.groupCount(joined, O.idx(3)));
                                            break;
                                        case 3:
                                            lab.expect(j.groupBy("label").sum("v"),
                                                    O.groupSum(joined, O.idx(3), 1));
                                            break;
                                        case 4:
                                            lab.expect(j.groupBy("k", "label").count(),
                                                    O.groupCount(joined, O.idx(0, 3)));
                                            break;
                                        case 5:
                                            lab.expect(j.groupBy("k").max("v"),
                                                    O.groupMinMax(joined, O.idx(0), 1, true));
                                            break;
                                        case 6:
                                            lab.expect(j.filter("v > 5").groupBy("label").count(),
                                                    O.groupCount(SL.filterRows(joined,
                                                            r -> liftB(r.getBigDecimal(1),
                                                                    x -> x.compareTo(BigDecimal.valueOf(5)) > 0)),
                                                            O.idx(3)));
                                            break;
                                        case 7:
                                            lab.expect(j.groupBy("k").sum("v")
                                                            .groupBy("k").count(),
                                                    O.groupCount(O.groupSum(joined, O.idx(0), 1),
                                                            O.idx(0)));
                                            break;
                                        case 8:
                                            lab.expect(j.union(j).groupBy("k").count(),
                                                    O.groupCount(SL.concat(joined, joined), O.idx(0)));
                                            break;
                                        default:
                                            lab.expect(j.distinct().groupBy("k").count(),
                                                    O.groupCount(O.distinctAll(joined), O.idx(0)));
                                            break;
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B72 Top and orderBy canonicalize at parallelism 4, 10 specifications over 10 profiles
    // ========================================================================

    private static BatchDesign b72() {
        return new BatchDesign() {
            @Override public int batchNo() { return 72; }
            @Override public String title() { return "Top/orderBy at P=4 (canonicalize) × 10 specs × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                io.rowweave.ops.Top[] tops = {
                        O.top(D.KV, 3, O.sf(D.KV, "v", true, "DEFAULT")),
                        O.top(D.KV, 5, O.sf(D.KV, "v", false, "DEFAULT")),
                        O.top(D.KV, 2, O.sf(D.KV, "k", true, "DEFAULT")),
                        O.top(D.KV, 4, O.sf(D.KV, "v", true, "NULLS_FIRST")),
                        O.top(D.KV, 6, O.sf(D.KV, "v", false, "NULLS_LAST")),
                        O.top(D.KV, 3, O.sf(D.KV, "k", false, "DEFAULT")),
                        O.top(D.KV, 8, O.sf(D.KV, "v", true, "DEFAULT"), O.sf(D.KV, "k", true, "DEFAULT")),
                        O.top(D.KV, 1, O.sf(D.KV, "k", true, "DEFAULT")),
                        O.top(D.KV, 3, O.sf(D.KV, "k", true, "DEFAULT"), O.sf(D.KV, "v", true, "DEFAULT")),
                        O.top(D.KV, 10, O.sf(D.KV, "v", false, "DEFAULT"))};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < tops.length; v++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final io.rowweave.ops.Top op = tops[v];
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B72", v, d), "P4 Top["
                                        + KV_PROFILES[d] + "] spec#" + v,
                                lab -> {
                                    lab.parallelism(4);
                                    DataSet res = lab.source(D.KV, data).top(op, 2);
                                    lab.expect(res, O.sortRows(data, D.KV, op.sortFields())
                                            .subList(0, Math.min(op.n(), data.size())));
                                }));
                    }
                }
                return out;
            }
        };
    }
}
