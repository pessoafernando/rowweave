package io.rowweave.examples.scenariolab.batches;

import io.rowweave.DataSet;
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
 * Family F (B53 to B62): DAG topologies. A diamond fan-out, a wide fan-out into several sinks,
 * convergence from several sources, deep chains, a breaker node shared by two sinks, a fork
 * after a join, branches transformed differently and brought back together, and a large graph
 * mixing coGroup with union. That a shared upstream is computed once shows up in the results
 * being correct and in the sinks agreeing.
 */
public final class FamilyF {

    private FamilyF() {
    }

    public static List<BatchDesign> batches() {
        return Arrays.asList(
                b53(), b54(), b55(), b56(), b57(),
                b58(), b59(), b60(), b61(), b62());
    }

    private static List<DataRow> posFilter(List<DataRow> in) {
        return SL.filterRows(in, r -> liftB(r.getBigDecimal(1), x -> x.signum() > 0));
    }

    // ========================================================================
    // B53 diamond fan-out rejoining through union or join, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b53() {
        return new BatchDesign() {
            @Override public int batchNo() { return 53; }
            @Override public String title() { return "diamond DAG (shared source forks then converges) × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B53", v, d), "diamond[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet src = lab.source("S", D.KV, data);
                                    DataSet res;
                                    List<DataRow> expected;
                                    switch (vf) {
                                        case 0: {
                                            DataSet a = src.filter("v > 0");
                                            DataSet b = src.filter("v <= 0");
                                            res = a.union(b);
                                            expected = SL.concat(posFilter(data),
                                                    SL.filterRows(data, r -> r.getBigDecimal(1) != null
                                                            && r.getBigDecimal(1).signum() <= 0));
                                            break;
                                        }
                                        case 1: {
                                            DataSet a = src.map(r -> {
                                                DataRow o = r.copy();
                                                o.set(0, FamilyA.kStr(r) == null ? null
                                                        : "L:" + FamilyA.kStr(r));
                                                return o;
                                            });
                                            DataSet b = src.map(r -> {
                                                DataRow o = r.copy();
                                                o.set(0, FamilyA.kStr(r) == null ? null
                                                        : "R:" + FamilyA.kStr(r));
                                                return o;
                                            });
                                            res = a.union(b);
                                            expected = SL.concat(
                                                    SL.mapRows(data, FamilyF::prefixL),
                                                    SL.mapRows(data, FamilyF::prefixR));
                                            break;
                                        }
                                        case 2: {
                                            // a union over mismatched schemas is rejected, so this uses complementary filter branches instead
                                            DataSet a2 = src.filter("k is not null");
                                            DataSet b2 = src.filter("k is null");
                                            res = a2.union(b2);
                                            expected = SL.concat(
                                                    SL.filterRows(data, r -> r.get(0) != null),
                                                    SL.filterRows(data, r -> r.get(0) == null));
                                            break;
                                        }
                                        case 3: {
                                            DataSet a = src.groupBy("k").sum("v");
                                            DataSet b = src.groupBy("k").count();
                                            // join of two aggregated results: a is [k,sum0], b is [k,count0]
                                            res = a.join(b).on("k");
                                            expected = O.joinRows(O.groupSum(data, O.idx(0), 1),
                                                    O.groupCount(data, O.idx(0)),
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2);
                                            break;
                                        }
                                        case 4: {
                                            DataSet a = src.distinct();
                                            res = a.union(a);
                                            expected = SL.concat(O.distinctAll(data), O.distinctAll(data));
                                            break;
                                        }
                                        case 5: {
                                            DataSet a = src.top(3);
                                            res = a.union(src);
                                            expected = SL.concat(
                                                    data.subList(0, Math.min(3, data.size())), data);
                                            break;
                                        }
                                        case 6: {
                                            DataSet a = src.filter("v > 5");
                                            DataSet b = src.filter("v > 5");
                                            res = a.union(b);
                                            expected = SL.concat(
                                                    SL.filterRows(data, r -> FamilyA.vCmp(r, 0, BigDecimal.valueOf(5))),
                                                    SL.filterRows(data, r -> FamilyA.vCmp(r, 0, BigDecimal.valueOf(5))));
                                            break;
                                        }
                                        case 7: {
                                            DataSet a = src.orderBy("v");
                                            res = a.union(src);
                                            expected = SL.concat(
                                                    O.sortRows(data, D.KV, O.sf(D.KV, "v", true, "DEFAULT")),
                                                    data);
                                            break;
                                        }
                                        case 8: {
                                            DataSet a = src.map(FamilyA::upperK);
                                            res = a.union(a);
                                            expected = SL.concat(SL.mapRows(data, FamilyA::upperK),
                                                    SL.mapRows(data, FamilyA::upperK));
                                            break;
                                        }
                                        default: {
                                            DataSet a = src.flatMap((r, c) -> {
                                                c.collect(r.copy());
                                            });
                                            DataSet b = src.flatMap((r, c) -> {
                                                // emits nothing
                                            });
                                            res = a.union(b);
                                            expected = SL.mapRows(data, DataRow::copy);
                                            break;
                                        }
                                    }
                                    lab.expect(res, expected);
                                }));
                    }
                }
                return out;
            }
        };
    }

    private static DataRow prefixL(DataRow r) {
        DataRow o = r.copy();
        o.set(0, FamilyA.kStr(r) == null ? null : "L:" + FamilyA.kStr(r));
        return o;
    }

    private static DataRow prefixR(DataRow r) {
        DataRow o = r.copy();
        o.set(0, FamilyA.kStr(r) == null ? null : "R:" + FamilyA.kStr(r));
        return o;
    }

    // ========================================================================
    // B54 wide fan-out into several sinks, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b54() {
        return new BatchDesign() {
            @Override public int batchNo() { return 54; }
            @Override public String title() { return "one source, several sinks (3 different transformations) × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B54", v, d), "fanoutSinks[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet src = lab.source("S", D.KV, data);
                                    boolean ordered = false;
                                    switch (vf) {
                                        case 0:
                                            lab.expect(src.filter("v > 5"),
                                                    SL.filterRows(data, r -> FamilyA.vCmp(r, 0, BigDecimal.valueOf(5))));
                                            lab.expect(src.filter("v <= 5"),
                                                    SL.filterRows(data, r -> liftB(r.getBigDecimal(1),
                                                            x -> x.compareTo(BigDecimal.valueOf(5)) <= 0)));
                                            lab.expect(src, data);
                                            break;
                                        case 1:
                                            lab.expect(src.groupBy("k").sum("v"),
                                                    O.groupSum(data, O.idx(0), 1));
                                            lab.expect(src.groupBy("k").count(),
                                                    O.groupCount(data, O.idx(0)));
                                            lab.expect(src.distinct(), O.distinctAll(data));
                                            break;
                                        case 2:
                                            lab.expectOrdered(src.orderBy("k"),
                                                    O.sortRows(data, D.KV, O.sf(D.KV, "k", true, "DEFAULT")));
                                            ordered = true;
                                            lab.expect(src, data);
                                            lab.expect(src.distinct(), O.distinctAll(data));
                                            break;
                                        case 3:
                                            lab.expect(src.top(2),
                                                    data.subList(0, Math.min(2, data.size())));
                                            lab.expect(src.top(4),
                                                    data.subList(0, Math.min(4, data.size())));
                                            lab.expect(src.top(8),
                                                    data.subList(0, Math.min(8, data.size())));
                                            break;
                                        case 4:
                                            lab.expect(src.select(0),
                                                    SL.mapRows(data, r -> one(r.get(0))));
                                            lab.expect(src.select(1),
                                                    SL.mapRows(data, r -> one(r.get(1))));
                                            lab.expect(src, data);
                                            break;
                                        case 5:
                                            lab.expect(src.map(FamilyA::upperK),
                                                    SL.mapRows(data, FamilyA::upperK));
                                            lab.expect(src.map(FamilyA::upperK).distinct(),
                                                    O.distinctAll(SL.mapRows(data, FamilyA::upperK)));
                                            lab.expect(src, data);
                                            break;
                                        case 6:
                                            lab.expect(src.flatMap((r, c) -> {
                                                c.collect(r.copy());
                                                c.collect(r.copy());
                                            }), SL.concat(data, data));
                                            lab.expect(src, data);
                                            lab.expect(src.addFields(
                                                    new io.rowweave.Field[]{D.f("t", io.rowweave.DataType.StringType)},
                                                    new Object[]{"X"}),
                                                    SL.mapRows(data, r -> {
                                                        DataRow o = r.copy();
                                                        o.set(0, o.get(0));
                                                        DataRow o2 = new DataRow(3);
                                                        o2.set(0, o.get(0));
                                                        o2.set(1, o.get(1));
                                                        o2.set(2, "X");
                                                        return o2;
                                                    }));
                                            break;
                                        case 7:
                                            lab.expect(src.partitionByHash("k"), data);
                                            lab.expect(src.partitionByHash("k", "v"), data);
                                            lab.expect(src.partitionByHash("k").distinct("k").select("k"),
                                                    SL.mapRows(O.distinctBy(data, O.idx(0)),
                                                            r -> O.project(r, O.idx(0))));
                                            break;
                                        case 8:
                                            lab.expect(src.filter("v > 0").groupBy("k").sum("v"),
                                                    O.groupSum(posFilter(data), O.idx(0), 1));
                                            lab.expect(src.filter("v > 0"),
                                                    posFilter(data));
                                            lab.expect(src.groupBy("k").sum("v"),
                                                    O.groupSum(data, O.idx(0), 1));
                                            break;
                                        default:
                                            lab.expectOrdered(src.orderBy(O.orderBy(D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"),
                                                    O.sf(D.KV, "v", true, "DEFAULT")), 1, false),
                                                    O.sortRows(data, D.KV,
                                                            O.sf(D.KV, "k", true, "DEFAULT"),
                                                            O.sf(D.KV, "v", true, "DEFAULT")));
                                            ordered = true;
                                            lab.expect(src.union(src), SL.concat(data, data));
                                            lab.expect(src.groupBy("k").max("v"),
                                                    O.groupMinMax(data, O.idx(0), 1, true));
                                            break;
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    private static DataRow one(Object v) {
        DataRow o = new DataRow(1);
        o.set(0, v);
        return o;
    }

    // ========================================================================
    // B55 branches from several sources converging, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b55() {
        return new BatchDesign() {
            @Override public int batchNo() { return 55; }
            @Override public String title() { return "several sources transformed independently, converging into union/join × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        final List<DataRow> third = kvProfile((d + 5) % 10);
                        out.add(ScenarioDef.of(id("B55", v, d), "multiSrc[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet a = lab.source("A", D.KV, left);
                                    DataSet b = lab.source("B", D.KV, third);
                                    DataSet dim = lab.source("D", D.DIM, right);
                                    DataSet res;
                                    List<DataRow> expected;
                                    switch (vf) {
                                        case 0:
                                            res = a.union(b);
                                            expected = SL.concat(left, third);
                                            break;
                                        case 1:
                                            res = a.filter("v > 0").union(b.filter("v > 0"));
                                            expected = SL.concat(posFilter(left), posFilter(third));
                                            break;
                                        case 2:
                                            res = a.union(b).union(a);
                                            expected = SL.concat(left, third, left);
                                            break;
                                        case 3:
                                            res = a.join(dim).on("k");
                                            expected = O.joinRows(left, right, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            break;
                                        case 4:
                                            res = a.union(b).join(dim).on("k");
                                            expected = O.joinRows(SL.concat(left, third), right,
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2);
                                            break;
                                        case 5:
                                            res = a.union(b).groupBy("k").sum("v");
                                            expected = O.groupSum(SL.concat(left, third), O.idx(0), 1);
                                            break;
                                        case 6:
                                            res = a.groupBy("k").sum("v").union(b.groupBy("k").sum("v"));
                                            expected = SL.concat(O.groupSum(left, O.idx(0), 1),
                                                    O.groupSum(third, O.idx(0), 1));
                                            break;
                                        case 7:
                                            res = a.distinct().union(b.distinct());
                                            expected = SL.concat(O.distinctAll(left), O.distinctAll(third));
                                            break;
                                        case 8:
                                            res = a.union(b).distinct();
                                            expected = O.distinctAll(SL.concat(left, third));
                                            break;
                                        default:
                                            res = a.union(b).orderBy("k");
                                            expected = O.sortRows(SL.concat(left, third), D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"));
                                            lab.expectOrdered(res, expected);
                                            return;
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
    // B56 a deep chain alternating map and filter, 10 depths from 2 to 20 over 10 profiles
    // ========================================================================

    private static BatchDesign b56() {
        return new BatchDesign() {
            @Override public int batchNo() { return 56; }
            @Override public String title() { return "deep chain (map(+1) alternating with filter(v>?), depth 2..20) × 10 depths × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int depthIdx = 0; depthIdx < 10; depthIdx++) {
                    final int depth = 2 + depthIdx;  // 2..11, because the WHOLE_GRAPH thread cost ceiling is 16 and depth 14 is rejected
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B56", depthIdx, d), "deepChain[depth=" + depth
                                        + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet cur = lab.source("S", D.KV, data);
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
    // B57 one shared breaker node feeding two sinks, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b57() {
        return new BatchDesign() {
            @Override public int batchNo() { return 57; }
            @Override public String title() { return "shared breaker (groupBy/orderBy/distinct) feeding two sinks × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B57", v, d), "sharedBreaker[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet src = lab.source("S", D.KV, data);
                                    switch (vf) {
                                        case 0: {
                                            DataSet shared = src.groupBy("k").sum("v");
                                            lab.expect(shared, O.groupSum(data, O.idx(0), 1));
                                            lab.expect(shared.filter("sum0 > 0"),
                                                    SL.filterRows(O.groupSum(data, O.idx(0), 1),
                                                            r -> r.getBigDecimal(1) != null
                                                                    && r.getBigDecimal(1).signum() > 0));
                                            break;
                                        }
                                        case 1: {
                                            DataSet shared = src.groupBy("k").count();
                                            lab.expect(shared, O.groupCount(data, O.idx(0)));
                                            lab.expect(shared.orderBy("k"),
                                                    O.sortRows(O.groupCount(data, O.idx(0)),
                                                            new io.rowweave.DataSchema(
                                                                    D.f("k", io.rowweave.DataType.StringType),
                                                                    D.f("count0", io.rowweave.DataType.LongType)),
                                                            O.sf(new io.rowweave.DataSchema(
                                                                    D.f("k", io.rowweave.DataType.StringType),
                                                                    D.f("count0", io.rowweave.DataType.LongType)),
                                                                    "k", true, "DEFAULT")));
                                            break;
                                        }
                                        case 2: {
                                            DataSet shared = src.distinct();
                                            lab.expect(shared, O.distinctAll(data));
                                            lab.expect(shared.orderBy("k"),
                                                    O.sortRows(O.distinctAll(data), D.KV,
                                                            O.sf(D.KV, "k", true, "DEFAULT")));
                                            break;
                                        }
                                        case 3: {
                                            DataSet shared = src.orderBy(O.orderBy(D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"),
                                                    O.sf(D.KV, "v", true, "DEFAULT")), 1, false);
                                            lab.expectOrdered(shared, O.sortRows(data, D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"),
                                                    O.sf(D.KV, "v", true, "DEFAULT")));
                                            lab.expect(shared.top(2),
                                                    O.sortRows(data, D.KV,
                                                            O.sf(D.KV, "k", true, "DEFAULT"),
                                                            O.sf(D.KV, "v", true, "DEFAULT"))
                                                            .subList(0, Math.min(2, data.size())));
                                            break;
                                        }
                                        case 4: {
                                            DataSet shared = src.union(src);
                                            lab.expect(shared.distinct(), O.distinctAll(data));
                                            lab.expect(shared.groupBy("k").count(),
                                                    O.groupCount(SL.concat(data, data), O.idx(0)));
                                            break;
                                        }
                                        case 5: {
                                            DataSet shared = src.groupBy("k").sum("v");
                                            lab.expect(shared.join(shared).on("k"),
                                                    O.joinRows(O.groupSum(data, O.idx(0), 1),
                                                            O.groupSum(data, O.idx(0), 1),
                                                            O.idx(0), O.idx(0), JoinType.INNER, 2, 2));
                                            lab.expect(shared, O.groupSum(data, O.idx(0), 1));
                                            break;
                                        }
                                        case 6: {
                                            DataSet shared = src.filter("v > 0");
                                            lab.expect(shared.groupBy("k").sum("v"),
                                                    O.groupSum(posFilter(data), O.idx(0), 1));
                                            lab.expect(shared, posFilter(data));
                                            break;
                                        }
                                        case 7: {
                                            DataSet shared = src.top(3);
                                            lab.expect(shared, data.subList(0, Math.min(3, data.size())));
                                            lab.expect(shared.distinct(),
                                                    O.distinctAll(data.subList(0, Math.min(3, data.size()))));
                                            break;
                                        }
                                        case 8: {
                                            DataSet shared = src.groupBy("k").max("v");
                                            lab.expect(shared, O.groupMinMax(data, O.idx(0), 1, true));
                                            lab.expect(shared.orderBy("k"),
                                                    O.sortRows(O.groupMinMax(data, O.idx(0), 1, true),
                                                            new io.rowweave.DataSchema(
                                                                    D.f("k", io.rowweave.DataType.StringType),
                                                                    D.f("max0", io.rowweave.DataType.BigDecimalType)),
                                                            O.sf(new io.rowweave.DataSchema(
                                                                    D.f("k", io.rowweave.DataType.StringType),
                                                                    D.f("max0", io.rowweave.DataType.BigDecimalType)),
                                                                    "k", true, "DEFAULT")));
                                            break;
                                        }
                                        default: {
                                            DataSet shared = src.coGroup(src, new FamilyE.CountBoth()).on("k");
                                            lab.expect(shared,
                                                    selfCount(data));
                                            lab.expect(shared.count(),
                                                    D.rows(new Object[]{(long) selfCount(data).size()}));
                                            break;
                                        }
                                    }
                                }));
                    }
                }
                return out;
            }

            private List<DataRow> selfCount(List<DataRow> data) {
                // the shared coGroup emits [k, 2n, 2n]: each side contributes the number of times the key appears in the data
                java.util.Map<Object, Long> counts = new java.util.LinkedHashMap<>();
                for (DataRow r : data) {
                    counts.merge(r.get(0), 1L, Long::sum);
                }
                List<DataRow> out = new ArrayList<>();
                for (java.util.Map.Entry<Object, Long> e : counts.entrySet()) {
                    DataRow o = new DataRow(3);
                    o.set(0, e.getKey());
                    o.set(1, e.getValue());
                    o.set(2, e.getValue());
                    out.add(o);
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B58 a join forking into two sinks, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b58() {
        return new BatchDesign() {
            @Override public int batchNo() { return 58; }
            @Override public String title() { return "fork after join (two sinks consume the same join result) × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(ScenarioDef.of(id("B58", v, d), "joinFork[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet a = lab.source("A", D.KV, left);
                                    DataSet b = lab.source("B", D.DIM, right);
                                    List<DataRow> joinedExp = O.joinRows(left, right, O.idx(0), O.idx(0),
                                            JoinType.LEFT, 2, 2);
                                    DataSet j = a.leftJoin(b).on("k");
                                    switch (vf) {
                                        case 0:
                                            lab.expect(j.filter("label is not null"),
                                                    SL.filterRows(joinedExp, r -> r.get(3) != null));
                                            lab.expect(j.filter("label is null"),
                                                    SL.filterRows(joinedExp, r -> r.get(3) == null));
                                            break;
                                        case 1:
                                            lab.expect(j.groupBy("k").count(),
                                                    O.groupCount(joinedExp, O.idx(0)));
                                            lab.expect(j.groupBy("label").count(),
                                                    O.groupCount(joinedExp, O.idx(3)));
                                            break;
                                        case 2:
                                            lab.expect(j.orderBy("k"),
                                                    O.sortRows(joinedExp, j.getSchema(),
                                                            O.sf(j.getSchema(), "k", true, "DEFAULT")));
                                            lab.expect(j, joinedExp);
                                            break;
                                        case 3:
                                            lab.expect(j.distinct(), O.distinctAll(joinedExp));
                                            lab.expect(j.select(0), SL.mapRows(joinedExp,
                                                    r -> one(r.get(0))));
                                            break;
                                        case 4:
                                            lab.expect(j.union(j), SL.concat(joinedExp, joinedExp));
                                            lab.expect(j, joinedExp);
                                            break;
                                        case 5:
                                            lab.expect(j.select(0, 1, 3),
                                                    SL.mapRows(joinedExp, r -> three(r.get(0), r.get(1), r.get(3))));
                                            lab.expect(j.select(3, 1),
                                                    SL.mapRows(joinedExp, r -> two(r.get(3), r.get(1))));
                                            break;
                                        case 6:
                                            lab.expect(j.flatMap((r, c) -> c.collect(r.copy())),
                                                    joinedExp);
                                            lab.expect(j.filter("v > 0"),
                                                    SL.filterRows(joinedExp, r -> liftB(r.getBigDecimal(1),
                                                            x -> x.signum() > 0)));
                                            break;
                                        case 7:
                                            lab.expect(j.groupBy("k", "label").count(),
                                                    O.groupCount(joinedExp, O.idx(0, 3)));
                                            lab.expect(j, joinedExp);
                                            break;
                                        case 8: {
                                            io.rowweave.DataSchema jm = j.getSchema();
                                            DataSet sorted = j.orderBy(O.orderBy(jm,
                                                    O.sf(jm, jm.getField(0).getName(), true, "DEFAULT"),
                                                    O.sf(jm, jm.getField(1).getName(), true, "DEFAULT"),
                                                    O.sf(jm, jm.getField(2).getName(), true, "DEFAULT"),
                                                    O.sf(jm, jm.getField(3).getName(), true, "DEFAULT")), 1, false);
                                            lab.expectOrdered(sorted.top(5),
                                                    O.sortRows(joinedExp, jm,
                                                            O.sf(jm, jm.getField(0).getName(), true, "DEFAULT"),
                                                            O.sf(jm, jm.getField(1).getName(), true, "DEFAULT"),
                                                            O.sf(jm, jm.getField(2).getName(), true, "DEFAULT"),
                                                            O.sf(jm, jm.getField(3).getName(), true, "DEFAULT"))
                                                            .subList(0, Math.min(5, joinedExp.size())));
                                            lab.expect(j.count(), D.rows(new Object[]{(long) joinedExp.size()}));
                                            break;
                                        }
                                        default:
                                            lab.expect(j.leftJoin(b).on("k"),
                                                    O.joinRows(joinedExp, right, O.idx(0), O.idx(0),
                                                            JoinType.LEFT, 4, 2));
                                            lab.expect(j, joinedExp);
                                            break;
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    private static DataRow two(Object a, Object b) {
        DataRow o = new DataRow(2);
        o.set(0, a);
        o.set(1, b);
        return o;
    }

    private static DataRow three(Object a, Object b, Object c) {
        DataRow o = new DataRow(3);
        o.set(0, a);
        o.set(1, b);
        o.set(2, c);
        return o;
    }

    // ========================================================================
    // B59 branches transformed differently and unioned back, 10 by 10
    // ========================================================================

    private static BatchDesign b59() {
        return new BatchDesign() {
            @Override public int batchNo() { return 59; }
            @Override public String title() { return "shared source with several transformed branches unioned (3 branches) × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B59", v, d), "branches[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet src = lab.source("S", D.KV, data);
                                    DataSet res;
                                    List<DataRow> expected;
                                    switch (vf) {
                                        case 0:
                                            res = src.map(r -> scaleV(r, 1)).union(src.map(r -> scaleV(r, 2)))
                                                    .union(src.map(r -> scaleV(r, 3)));
                                            expected = SL.concat(SL.mapRows(data, r -> scaleV(r, 1)),
                                                    SL.mapRows(data, r -> scaleV(r, 2)),
                                                    SL.mapRows(data, r -> scaleV(r, 3)));
                                            break;
                                        case 1:
                                            res = src.filter("v > 0").union(src.filter("v <= 0"))
                                                    .union(src.filter("v is null"));
                                            expected = SL.concat(
                                                    SL.filterRows(data, r -> liftB(r.getBigDecimal(1),
                                                            x -> x.signum() > 0)),
                                                    SL.filterRows(data, r -> liftB(r.getBigDecimal(1),
                                                            x -> x.signum() <= 0)),
                                                    SL.filterRows(data, r -> r.get(1) == null));
                                            break;
                                        case 2:
                                            res = src.distinct().union(src).union(src.distinct());
                                            expected = SL.concat(O.distinctAll(data), data,
                                                    O.distinctAll(data));
                                            break;
                                        case 3:
                                            res = src.groupBy("k").sum("v")
                                                    .union(src.groupBy("k").max("v"))
                                                    .union(src.groupBy("k").min("v"));
                                            expected = SL.concat(
                                                    O.groupSum(data, O.idx(0), 1),
                                                    O.groupMinMax(data, O.idx(0), 1, true),
                                                    O.groupMinMax(data, O.idx(0), 1, false));
                                            break;
                                        case 4:
                                            res = src.top(1).union(src.top(2)).union(src.top(3));
                                            expected = SL.concat(
                                                    data.subList(0, Math.min(1, data.size())),
                                                    data.subList(0, Math.min(2, data.size())),
                                                    data.subList(0, Math.min(3, data.size())));
                                            break;
                                        case 5:
                                            res = src.map(FamilyA::upperK)
                                                    .union(src.map(FamilyA::upperK).distinct());
                                            expected = SL.concat(SL.mapRows(data, FamilyA::upperK),
                                                    O.distinctAll(SL.mapRows(data, FamilyA::upperK)));
                                            break;
                                        case 6:
                                            res = src.select(1).union(src.select(1));
                                            expected = SL.concat(
                                                    SL.mapRows(data, r -> one(r.get(1))),
                                                    SL.mapRows(data, r -> one(r.get(1))));
                                            break;
                                        case 7:
                                            res = src.addFields(
                                                    new io.rowweave.Field[]{D.f("t", io.rowweave.DataType.StringType)},
                                                    new Object[]{"A"})
                                                    .union(src.addFields(
                                                            new io.rowweave.Field[]{D.f("t", io.rowweave.DataType.StringType)},
                                                            new Object[]{"B"}));
                                            expected = SL.concat(
                                                    withTag(data, "A"), withTag(data, "B"));
                                            break;
                                        case 8:
                                            res = src.partitionByHash("k").union(src.partitionByHash("v"));
                                            expected = SL.concat(data, data);
                                            break;
                                        default:
                                            res = src.orderBy("k").union(src.orderBy("v"));
                                            expected = SL.concat(
                                                    O.sortRows(data, D.KV, O.sf(D.KV, "k", true, "DEFAULT")),
                                                    O.sortRows(data, D.KV, O.sf(D.KV, "v", true, "DEFAULT")));
                                            lab.expect(res, expected);
                                            return;
                                    }
                                    lab.expect(res, expected);
                                }));
                    }
                }
                return out;
            }

            private List<DataRow> withTag(List<DataRow> data, String tag) {
                return SL.mapRows(data, r -> {
                    DataRow o = new DataRow(3);
                    o.set(0, r.get(0));
                    o.set(1, r.get(1));
                    o.set(2, tag);
                    return o;
                });
            }
        };
    }

    private static DataRow scaleV(DataRow r, int m) {
        DataRow o = r.copy();
        BigDecimal v = o.getBigDecimal(1);
        o.set(1, v == null ? null : v.multiply(BigDecimal.valueOf(m)));
        return o;
    }

    // ========================================================================
    // B60 a large mixed graph with join, union, groupBy, filter and several sinks, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b60() {
        return new BatchDesign() {
            @Override public int batchNo() { return 60; }
            @Override public String title() { return "mixed large graph (join+union+groupBy+filter, multiple sinks) × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        final List<DataRow> extra = kvProfile((d + 7) % 10);
                        out.add(ScenarioDef.of(id("B60", v, d), "mega[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet a = lab.source("A", D.KV, left);
                                    DataSet b = lab.source("B", D.DIM, right);
                                    DataSet c = lab.source("C", D.KV, extra);
                                    List<DataRow> joinedExp = O.joinRows(left, right, O.idx(0), O.idx(0),
                                            JoinType.INNER, 2, 2);
                                    switch (vf) {
                                        case 0: {
                                            DataSet j = a.join(b).on("k");
                                            DataSet u = j.union(c.join(b).on("k"));
                                            lab.expect(u.groupBy("k").sum("v"),
                                                    O.groupSum(SL.concat(joinedExp,
                                                            O.joinRows(extra, right, O.idx(0), O.idx(0),
                                                                    JoinType.INNER, 2, 2)), O.idx(0), 1));
                                            lab.expect(u, SL.concat(joinedExp,
                                                    O.joinRows(extra, right, O.idx(0), O.idx(0),
                                                            JoinType.INNER, 2, 2)));
                                            break;
                                        }
                                        case 1: {
                                            DataSet u = a.union(c);
                                            lab.expect(u.join(b).on("k").groupBy("label").count(),
                                                    O.groupCount(O.joinRows(SL.concat(left, extra), right,
                                                            O.idx(0), O.idx(0), JoinType.INNER, 2, 2),
                                                            O.idx(3)));
                                            break;
                                        }
                                        case 2: {
                                            DataSet agg = a.join(b).on("k").groupBy("label").sum("v");
                                            lab.expect(agg.filter("sum0 > 0"),
                                                    SL.filterRows(O.groupSum(joinedExp, O.idx(3), 1),
                                                            r -> r.getBigDecimal(1) != null
                                                                    && r.getBigDecimal(1).signum() > 0));
                                            lab.expect(agg, O.groupSum(joinedExp, O.idx(3), 1));
                                            break;
                                        }
                                        case 3: {
                                            DataSet res = a.union(c).distinct()
                                                    .join(b).on("k")
                                                    .select("k", "v", "label");
                                            List<DataRow> dedup = O.distinctAll(SL.concat(left, extra));
                                            List<DataRow> jd = O.joinRows(dedup, right, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            lab.expect(res, SL.mapRows(jd, r -> three(r.get(0), r.get(1), r.get(3))));
                                            break;
                                        }
                                        case 4: {
                                            DataSet res = a.join(b).on("k").union(c.join(b).on("k"));
                                            lab.expect(res,
                                                    SL.concat(joinedExp,
                                                            O.joinRows(extra, right, O.idx(0), O.idx(0),
                                                                    JoinType.INNER, 2, 2)));
                                            break;
                                        }
                                        case 5: {
                                            DataSet res = a.join(b).on("k").coGroup(
                                                    c, io.rowweave.ops.join.CoGroup.of(
                                                            a.join(b).on("k").getSchema(), D.KV,
                                                            new String[]{"k"}, new String[]{"k"},
                                                            new FamilyE.CountBoth()));
                                            lab.expect(res, countBoth(joinedExp, extra));
                                            break;
                                        }
                                        case 6: {
                                            DataSet res = a.filter("v > 0").union(c.filter("v > 0"))
                                                    .groupBy("k").count().orderBy("k");
                                            List<DataRow> pos = SL.concat(posFilter(left), posFilter(extra));
                                            lab.expectOrdered(res,
                                                    O.sortRows(O.groupCount(pos, O.idx(0)),
                                                            new io.rowweave.DataSchema(
                                                                    D.f("k", io.rowweave.DataType.StringType),
                                                                    D.f("count0", io.rowweave.DataType.LongType)),
                                                            O.sf(new io.rowweave.DataSchema(
                                                                    D.f("k", io.rowweave.DataType.StringType),
                                                                    D.f("count0", io.rowweave.DataType.LongType)),
                                                                    "k", true, "DEFAULT")));
                                            break;
                                        }
                                        case 7: {
                                            DataSet res2 = a.join(b).on("k").union(a.join(b).on("k"));
                                            List<DataRow> jj = joinedExp;
                                            lab.expect(res2, SL.concat(jj, jj));
                                            break;
                                        }
                                        case 8: {
                                            DataSet res = a.join(b).on("k").top(4)
                                                    .map(r -> {
                                                        DataRow o = new DataRow(1);
                                                        o.set(0, r.get(0));
                                                        return o;
                                                    })
                                                    .groupBy("k").count();
                                            List<DataRow> top4 = joinedExp.subList(0,
                                                    Math.min(4, joinedExp.size()));
                                            lab.expect(res, O.groupCount(SL.mapRows(top4, r -> one(r.get(0))),
                                                    O.idx(0)));
                                            break;
                                        }
                                        default: {
                                            DataSet shared = a.union(c);
                                            DataSet agg = shared.groupBy("k").sum("v");
                                            lab.expect(agg.join(b).on("k").select("k", "sum0", "label"),
                                                    SL.mapRows(O.joinRows(
                                                            O.groupSum(SL.concat(left, extra), O.idx(0), 1),
                                                            right, O.idx(0), O.idx(0), JoinType.INNER, 2, 2),
                                                            r -> three(r.get(0), r.get(1), r.get(3))));
                                            lab.expect(agg, O.groupSum(SL.concat(left, extra), O.idx(0), 1));
                                            break;
                                        }
                                    }
                                }));
                    }
                }
                return out;
            }

            private List<DataRow> countBoth(List<DataRow> l, List<DataRow> r) {
                return countBothExp(l, r);
            }
        };
    }

    // ========================================================================
    // B61 coGroup mixed with union, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b61() {
        return new BatchDesign() {
            @Override public int batchNo() { return 61; }
            @Override public String title() { return "mixed graph of coGroup with union/groupBy × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(ScenarioDef.of(id("B61", v, d), "coGroupMix[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet a = lab.source("A", D.KV, left);
                                    DataSet b = lab.source("B", D.DIM, right);
                                    DataSet c = lab.source("C", D.KV, left);
                                    switch (vf) {
                                        case 0: {
                                            DataSet cg = a.coGroup(b, new FamilyE.CountBoth()).on("k");
                                            lab.expect(cg, countBothExp(left, right));
                                            break;
                                        }
                                        case 1: {
                                            DataSet cg = a.coGroup(c, new FamilyE.CountBoth()).on("k");
                                            lab.expect(cg, countBothExp(left, left));
                                            break;
                                        }
                                        case 2: {
                                            DataSet cg = a.coGroup(b, new FamilyE.CountBoth()).on("k");
                                            lab.expect(cg.union(cg), SL.concat(countBothExp(left, right),
                                                    countBothExp(left, right)));
                                            break;
                                        }
                                        case 3: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            lab.expect(cg.groupBy("k").sum("s"),
                                                    O.groupSum(O.groupSumLeft(left, right), O.idx(0), 1));
                                            break;
                                        }
                                        case 4: {
                                            DataSet cg = a.coGroup(b, new FamilyE.EmitLeftOnly()).on("k");
                                            lab.expect(cg,
                                                    SL.mapRows(left, r -> {
                                                        DataRow o = new DataRow(2);
                                                        o.set(0, r.get(0));
                                                        o.set(1, r.get(1));
                                                        return o;
                                                    }));
                                            break;
                                        }
                                        case 5: {
                                            DataSet cg = a.coGroup(b, new FamilyE.EmitRightOnly()).on("k");
                                            lab.expect(cg,
                                                    SL.mapRows(right, r -> {
                                                        DataRow o = new DataRow(2);
                                                        o.set(0, r.get(0));
                                                        o.set(1, r.get(1));
                                                        return o;
                                                    }));
                                            break;
                                        }
                                        case 6: {
                                            DataSet cg = a.coGroup(b, new FamilyE.CountBoth()).on("k");
                                            io.rowweave.DataSchema cm = new io.rowweave.DataSchema(
                                                    D.f("k", io.rowweave.DataType.StringType),
                                                    D.f("lc", io.rowweave.DataType.LongType),
                                                    D.f("rc", io.rowweave.DataType.LongType));
                                            lab.expectOrdered(cg.orderBy(O.orderBy(cm,
                                                    O.sf(cm, "k", true, "DEFAULT")), 1, false),
                                                    O.sortRows(countBothExp(left, right), cm,
                                                            O.sf(cm, "k", true, "DEFAULT")));
                                            break;
                                        }
                                        case 7: {
                                            // both sides are KV, so the third output column is declared BigDecimal rather than holding a BigDecimal in a String column
                                            DataSet cg = a.coGroup(c, new io.rowweave.CoGroupFunction() {
                                                @Override
                                                public io.rowweave.DataSchema getResultSchema() {
                                                    return D.meta(D.f("k", io.rowweave.DataType.StringType),
                                                            D.f("v", io.rowweave.DataType.BigDecimalType),
                                                            D.f("v2", io.rowweave.DataType.BigDecimalType));
                                                }

                                                @Override
                                                public void coGroup(Iterable<DataRow> i1, Iterable<DataRow> i2,
                                                                    io.rowweave.Collector out) {
                                                    for (DataRow l : i1) {
                                                        DataRow o = new DataRow(3);
                                                        o.set(0, l.get(0));
                                                        o.set(1, l.get(1));
                                                        out.collect(o);
                                                    }
                                                    for (DataRow r : i2) {
                                                        DataRow o = new DataRow(3);
                                                        o.set(0, r.get(0));
                                                        o.set(2, r.get(1));
                                                        out.collect(o);
                                                    }
                                                }
                                            }).on("k");
                                            List<DataRow> exp = new ArrayList<>();
                                            for (DataRow r : left) {
                                                DataRow o = new DataRow(3);
                                                o.set(0, r.get(0));
                                                o.set(1, r.get(1));
                                                exp.add(o);
                                            }
                                            for (DataRow r : left) {
                                                DataRow o = new DataRow(3);
                                                o.set(0, r.get(0));
                                                o.set(2, r.get(1));
                                                exp.add(o);
                                            }
                                            lab.expect(cg, exp);
                                            break;
                                        }
                                        case 8: {
                                            DataSet cg = a.union(c).coGroup(b, new FamilyE.KeysWithBoth()).on("k");
                                            lab.expect(cg, keysBoth(SL.concat(left, left), right));
                                            break;
                                        }
                                        default: {
                                            DataSet cg = a.filter("v > 0").coGroup(
                                                    b, new FamilyE.CountBoth()).on("k");
                                            lab.expect(cg, countBothExp(posFilter(left), right));
                                            break;
                                        }
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    private static List<DataRow> keysBoth(List<DataRow> l, List<DataRow> r) {
        java.util.Set<Object> ls = new java.util.LinkedHashSet<>();
        java.util.Set<Object> rs = new java.util.LinkedHashSet<>();
        for (DataRow x : l) {
            if (x.get(0) != null) {
                ls.add(x.get(0));
            }
        }
        for (DataRow x : r) {
            if (x.get(0) != null) {
                rs.add(x.get(0));
            }
        }
        List<DataRow> out = new ArrayList<>();
        for (Object k : ls) {
            if (rs.contains(k)) {
                DataRow o = new DataRow(1);
                o.set(0, k);
                out.add(o);
            }
        }
        return out;
    }

    // ========================================================================
    // B62 a nested coGroup pipeline whose result feeds another coGroup or join, 10 by 10
    // ========================================================================

    private static BatchDesign b62() {
        return new BatchDesign() {
            @Override public int batchNo() { return 62; }
            @Override public String title() { return "nested pipeline feeding coGroup output into join/coGroup × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(ScenarioDef.of(id("B62", v, d), "cgNested[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet a = lab.source("A", D.KV, left);
                                    DataSet b = lab.source("B", D.DIM, right);
                                    DataSet c = lab.source("C", D.KV, left);
                                    List<DataRow> sumLeftExp = O.groupSumLeft(left, right);
                                    switch (vf) {
                                        case 0: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            lab.expect(cg.join(c).on("k"),
                                                    O.joinRows(sumLeftExp, left, O.idx(0), O.idx(0),
                                                            JoinType.INNER, 2, 2));
                                            break;
                                        }
                                        case 1: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            lab.expect(cg.leftJoin(c).on("k"),
                                                    O.joinRows(sumLeftExp, left, O.idx(0), O.idx(0),
                                                            JoinType.LEFT, 2, 2));
                                            break;
                                        }
                                        case 2: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            DataSet cg2 = c.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            lab.expect(cg.union(cg2), SL.concat(sumLeftExp, sumLeftExp));
                                            break;
                                        }
                                        case 3: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            lab.expect(cg.filter("s > 0"),
                                                    SL.filterRows(sumLeftExp, r -> r.getBigDecimal(1) != null
                                                            && r.getBigDecimal(1).signum() > 0));
                                            break;
                                        }
                                        case 4: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            lab.expect(cg.coGroup(cg, new FamilyE.CountBoth()).on("k"),
                                                    countBothExp(sumLeftExp, sumLeftExp));
                                            break;
                                        }
                                        case 5: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            io.rowweave.DataSchema cm = cg.getSchema();
                                            lab.expectOrdered(cg.orderBy(O.orderBy(cm,
                                                    O.sf(cm, "k", true, "DEFAULT")), 1, false),
                                                    O.sortRows(sumLeftExp, cm,
                                                            O.sf(cm, "k", true, "DEFAULT")));
                                            break;
                                        }
                                        case 6: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            lab.expect(cg.distinct(), O.distinctAll(sumLeftExp));
                                            lab.expect(cg, sumLeftExp);
                                            break;
                                        }
                                        case 7: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            lab.expect(cg.groupBy("k").max("s"),
                                                    O.groupMinMax(sumLeftExp, O.idx(0), 1, true));
                                            break;
                                        }
                                        case 8: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            DataSet sorted = cg.orderBy(O.orderBy(cg.getSchema(),
                                                    O.sf(cg.getSchema(), "k", true, "DEFAULT")), 1, false);
                                            lab.expectOrdered(sorted.top(2),
                                                    O.sortRows(sumLeftExp, cg.getSchema(),
                                                            O.sf(cg.getSchema(), "k", true, "DEFAULT"))
                                                            .subList(0, Math.min(2, sumLeftExp.size())));
                                            break;
                                        }
                                        default: {
                                            DataSet cg = a.coGroup(b, new FamilyE.SumLeft()).on("k");
                                            lab.expect(cg.map(r -> {
                                                DataRow o = r.copy();
                                                o.set(1, o.getBigDecimal(1) == null ? null
                                                        : o.getBigDecimal(1).add(BigDecimal.ONE));
                                                return o;
                                            }), SL.mapRows(sumLeftExp, r -> {
                                                DataRow o = r.copy();
                                                o.set(1, r.getBigDecimal(1) == null ? null
                                                        : r.getBigDecimal(1).add(BigDecimal.ONE));
                                                return o;
                                            }));
                                            break;
                                        }
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    private static List<DataRow> countBothExp(List<DataRow> l, List<DataRow> r) {
        java.util.Map<Object, Long> cl = groupCounts(l);
        java.util.Map<Object, Long> cr = groupCounts(r);
        if (cl.containsKey(null) || cr.containsKey(null)) {
            cl.putIfAbsent(null, 0L);
            cr.putIfAbsent(null, 0L);
        }
        List<Object> keys = new ArrayList<>();
        for (Object k : cl.keySet()) {
            if (!keys.contains(k)) {
                keys.add(k);
            }
        }
        for (Object k : cr.keySet()) {
            if (!keys.contains(k)) {
                keys.add(k);
            }
        }
        List<DataRow> out = new ArrayList<>();
        for (Object k : keys) {
            DataRow o = new DataRow(3);
            o.set(0, k);
            o.set(1, cl.getOrDefault(k, 0L));
            o.set(2, cr.getOrDefault(k, 0L));
            out.add(o);
        }
        return out;
    }

    private static java.util.Map<Object, Long> groupCounts(List<DataRow> rows) {
        java.util.Map<Object, Long> m = new java.util.LinkedHashMap<>();
        for (DataRow r : rows) {
            m.merge(r.get(0), 1L, Long::sum);
        }
        return m;
    }
}
