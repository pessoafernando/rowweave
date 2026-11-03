package io.rowweave.examples.scenariolab.batches;

import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.GroupCombineFunction;
import io.rowweave.GroupCombineReduceFunction;
import io.rowweave.GroupReduceFunction;
import io.rowweave.JoinFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.examples.scenariolab.BatchDesign;
import io.rowweave.examples.scenariolab.D;
import io.rowweave.examples.scenariolab.O;
import io.rowweave.examples.scenariolab.SL;
import io.rowweave.examples.scenariolab.ScenarioDef;
import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.JoinType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.rowweave.examples.scenariolab.batches.FamilyA.id;
import static io.rowweave.examples.scenariolab.batches.FamilyA.kvProfile;
import static io.rowweave.examples.scenariolab.batches.FamilyA.KV_PROFILES;
import static io.rowweave.examples.scenariolab.batches.FamilyA.liftB;
import static io.rowweave.examples.scenariolab.batches.FamilyD.dimProfile;
import static io.rowweave.examples.scenariolab.batches.FamilyD.DIM_NAMES;

/**
 * Family E (B43 to B52): the advanced binary operators and user functions. join.withFunc, a
 * three-way join, join combined with groupBy in both directions, coGroup, reduceGroup per group
 * and over the whole table, combineGroup and combineReduceGroup.
 */
public final class FamilyE {

    private FamilyE() {
    }

    public static List<BatchDesign> batches() {
        return Arrays.asList(
                b43(), b44(), b45(), b46(), b47(),
                b48(), b49(), b50(), b51(), b52());
    }

    // ========================================================================
    // B43 join with withFunc, 10 function variants over 10 profiles
    // ========================================================================

    private static BatchDesign b43() {
        return new BatchDesign() {
            @Override public int batchNo() { return 43; }
            @Override public String title() { return "join.withFunc custom output × 10 functions × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(SL.sink2(id("B43", v, d), "withFunc[v" + vf + "][" + KV_PROFILES[d] + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> l.leftJoin(r).withFunc("k", fn(vf)),
                                (a, b) -> withFuncOracle(vf, O.joinRows(a, b, O.idx(0), O.idx(0),
                                        JoinType.LEFT, 2, 2)),
                                false));
                    }
                }
                return out;
            }

            private JoinFunction fn(int v) {
                switch (v) {
                    case 0:
                        return new JoinFunction() {
                            @Override
                            public DataSchema getResultSchema() {
                                return new DataSchema(D.f("k", DataType.StringType),
                                        D.f("label", DataType.StringType));
                            }

                            @Override
                            public DataRow join(DataRow l, DataRow r) {
                                DataRow o = new DataRow(2);
                                o.set(0, l == null ? null : l.get(0));
                                o.set(1, r == null ? null : r.get(1));
                                return o;
                            }
                        };
                    case 1:
                        return new JoinFunction() {
                            @Override
                            public DataSchema getResultSchema() {
                                return new DataSchema(D.f("k", DataType.StringType),
                                        D.f("v", DataType.BigDecimalType));
                            }

                            @Override
                            public DataRow join(DataRow l, DataRow r) {
                                DataRow o = new DataRow(2);
                                o.set(0, l == null ? null : l.get(0));
                                o.set(1, l == null ? null : l.get(1));
                                return o;
                            }
                        };
                    case 2:
                        return new JoinFunction() {
                            @Override
                            public DataSchema getResultSchema() {
                                return new DataSchema(D.f("tag", DataType.StringType));
                            }

                            @Override
                            public DataRow join(DataRow l, DataRow r) {
                                DataRow o = new DataRow(1);
                                boolean matched = r != null;
                                o.set(0, matched ? "M" : "U");
                                return o;
                            }
                        };
                    case 3:
                        return new JoinFunction() {
                            @Override
                            public DataSchema getResultSchema() {
                                return new DataSchema(D.f("k", DataType.StringType),
                                        D.f("v", DataType.BigDecimalType),
                                        D.f("label", DataType.StringType));
                            }

                            @Override
                            public DataRow join(DataRow l, DataRow r) {
                                DataRow o = new DataRow(3);
                                o.set(0, l == null ? null : l.get(0));
                                o.set(1, l == null ? null : l.get(1));
                                o.set(2, r == null ? null : r.get(1));
                                return o;
                            }
                        };
                    case 4:
                        return new JoinFunction() {
                            @Override
                            public DataSchema getResultSchema() {
                                return new DataSchema(D.f("v2", DataType.BigDecimalType));
                            }

                            @Override
                            public DataRow join(DataRow l, DataRow r) {
                                DataRow o = new DataRow(1);
                                BigDecimal v = l == null ? null : l.getBigDecimal(1);
                                o.set(0, v == null ? null : v.multiply(BigDecimal.valueOf(2)));
                                return o;
                            }
                        };
                    case 5:
                        return new JoinFunction() {
                            @Override
                            public DataSchema getResultSchema() {
                                return new DataSchema(D.f("k", DataType.StringType));
                            }

                            @Override
                            public DataRow join(DataRow l, DataRow r) {
                                DataRow o = new DataRow(1);
                                o.set(0, l == null ? null : l.get(0));
                                return o;
                            }
                        };
                    case 6:
                        return new JoinFunction() {
                            @Override
                            public DataSchema getResultSchema() {
                                return new DataSchema(D.f("nullRow", DataType.StringType));
                            }

                            @Override
                            public DataRow join(DataRow l, DataRow r) {
                                DataRow o = new DataRow(1);
                                o.set(0, null);
                                return o;
                            }
                        };
                    case 7:
                        return new JoinFunction() {
                            @Override
                            public DataSchema getResultSchema() {
                                return new DataSchema(D.f("k", DataType.StringType),
                                        D.f("hit", DataType.BooleanType));
                            }

                            @Override
                            public DataRow join(DataRow l, DataRow r) {
                                DataRow o = new DataRow(2);
                                o.set(0, l == null ? null : l.get(0));
                                o.set(1, r != null);
                                return o;
                            }
                        };
                    case 8:
                        return new JoinFunction() {
                            @Override
                            public DataSchema getResultSchema() {
                                return new DataSchema(D.f("concat", DataType.StringType));
                            }

                            @Override
                            public DataRow join(DataRow l, DataRow r) {
                                DataRow o = new DataRow(1);
                                String k = l == null ? null : (String) l.get(0);
                                Object label = r == null ? null : r.get(1);
                                o.set(0, k == null ? null : k + ":" + label);
                                return o;
                            }
                        };
                    default:
                        return new JoinFunction() {
                            @Override
                            public DataSchema getResultSchema() {
                                return new DataSchema(D.f("k", DataType.StringType),
                                        D.f("v", DataType.BigDecimalType),
                                        D.f("doubled", DataType.BigDecimalType));
                            }

                            @Override
                            public DataRow join(DataRow l, DataRow r) {
                                DataRow o = new DataRow(3);
                                o.set(0, l == null ? null : l.get(0));
                                o.set(1, l == null ? null : l.get(1));
                                BigDecimal v = l == null ? null : l.getBigDecimal(1);
                                o.set(2, v == null ? null : v.add(v));
                                return o;
                            }
                        };
                }
            }

            private List<DataRow> withFuncOracle(int v, List<DataRow> joined) {
                return SL.mapRows(joined, x -> {
                    boolean unmatched = x.get(2) == null && x.get(3) == null;
                    boolean lNull = x.get(0) == null && x.get(1) == null;
                    DataRow l = x;
                    if (lNull) {
                        // for a left half-row that is all null, withFunc passes l=null, and the function is null-safe and emits null fields
                        switch (v) {
                            case 0: { DataRow o = new DataRow(2); o.set(1, unmatched ? null : x.get(3)); return o; }
                            case 1: return new DataRow(2);
                            case 2: { DataRow o = new DataRow(1); o.set(0, unmatched ? "U" : "M"); return o; }
                            case 3: { DataRow o = new DataRow(3); o.set(2, unmatched ? null : x.get(3)); return o; }
                            case 4: return new DataRow(1);
                            case 5: return new DataRow(1);
                            case 6: { DataRow o = new DataRow(1); o.set(0, null); return o; }
                            case 7: { DataRow o = new DataRow(2); o.set(1, !unmatched); return o; }
                            case 8: return new DataRow(1);
                            default: return new DataRow(3);
                        }
                    }
                    Object rLabel = x.get(3);
                    DataRow o;
                    switch (v) {
                        case 0:
                            o = new DataRow(2);
                            o.set(0, l.get(0));
                            o.set(1, unmatched ? null : rLabel);
                            return o;
                        case 1: {
                            o = new DataRow(2);
                            o.set(0, l.get(0));
                            o.set(1, l.get(1));
                            return o;
                        }
                        case 2: {
                            o = new DataRow(1);
                            o.set(0, unmatched ? "U" : "M");
                            return o;
                        }
                        case 3: {
                            o = new DataRow(3);
                            o.set(0, l.get(0));
                            o.set(1, l.get(1));
                            o.set(2, unmatched ? null : rLabel);
                            return o;
                        }
                        case 4: {
                            o = new DataRow(1);
                            BigDecimal val = l.getBigDecimal(1);
                            o.set(0, val == null ? null : val.multiply(BigDecimal.valueOf(2)));
                            return o;
                        }
                        case 5: {
                            o = new DataRow(1);
                            o.set(0, l.get(0));
                            return o;
                        }
                        case 6: {
                            o = new DataRow(1);
                            o.set(0, null);
                            return o;
                        }
                        case 7: {
                            o = new DataRow(2);
                            o.set(0, l.get(0));
                            o.set(1, !unmatched);
                            return o;
                        }
                        case 8: {
                            o = new DataRow(1);
                            String k = (String) l.get(0);
                            o.set(0, k == null ? null : k + ":" + (unmatched ? null : rLabel));
                            return o;
                        }
                        default: {
                            o = new DataRow(3);
                            o.set(0, l.get(0));
                            o.set(1, l.get(1));
                            BigDecimal val = l.getBigDecimal(1);
                            o.set(2, val == null ? null : val.add(val));
                            return o;
                        }
                    }
                });
            }
        };
    }

    // ========================================================================
    // B44 a three-way join chain, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b44() {
        return new BatchDesign() {
            @Override public int batchNo() { return 44; }
            @Override public String title() { return "three-way join chain (KV×DIM×DIM') × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> d1 = dimProfile(d);
                        final List<DataRow> d2 = dimProfile((d + 3) % 10);
                        out.add(ScenarioDef.of(id("B44", v, d), "join3[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet a = lab.source("A", D.KV, left);
                                    DataSet b = lab.source("B", D.DIM, d1);
                                    DataSet c = lab.source("C", D.DIM, d2);
                                    DataSet res;
                                    List<DataRow> expected;
                                    switch (vf) {
                                        case 0: {
                                            DataSet ab = a.join(b).on("k");
                                            res = ab.join(c).on("k");
                                            List<DataRow> abExp = O.joinRows(left, d1, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            // [k,v,k,label] × [k,label] on first k (idx0) vs idx0
                                            expected = O.joinRows(abExp, d2, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 4, 2);
                                            break;
                                        }
                                        case 1: {
                                            DataSet ab = a.leftJoin(b).on("k");
                                            res = ab.leftJoin(c).on("k");
                                            List<DataRow> abExp = O.joinRows(left, d1, O.idx(0), O.idx(0),
                                                    JoinType.LEFT, 2, 2);
                                            expected = O.joinRows(abExp, d2, O.idx(0), O.idx(0),
                                                    JoinType.LEFT, 4, 2);
                                            break;
                                        }
                                        case 2: {
                                            DataSet ab = a.join(b).on("k").select("k", "v", "label");
                                            res = ab.join(c).on("k");
                                            List<DataRow> abExp = SL.mapRows(O.joinRows(left, d1,
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2), x -> {
                                                DataRow o = new DataRow(3);
                                                o.set(0, x.get(0));
                                                o.set(1, x.get(1));
                                                o.set(2, x.get(3));
                                                return o;
                                            });
                                            expected = O.joinRows(abExp, d2, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 3, 2);
                                            break;
                                        }
                                        case 3: {
                                            // union the two lookup inputs before joining: b.union(c) keeps a key once, so it matches more often
                                            DataSet bc = b.union(c);
                                            res = a.join(bc).on("k");
                                            expected = O.joinRows(left, SL.concat(d1, d2),
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2);
                                            break;
                                        }
                                        case 4: {
                                            DataSet ab = a.join(b).on("k");
                                            res = ab.join(c).on("k").select("k", "v");
                                            List<DataRow> abExp = O.joinRows(left, d1, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            expected = SL.mapRows(O.joinRows(abExp, d2,
                                                    O.idx(0), O.idx(0), JoinType.INNER, 4, 2), x -> {
                                                DataRow o = new DataRow(2);
                                                o.set(0, x.get(0));
                                                o.set(1, x.get(1));
                                                return o;
                                            });
                                            break;
                                        }
                                        case 5: {
                                            DataSet bc = b.join(c).on("k");
                                            res = a.join(bc).on("k");
                                            List<DataRow> bcExp = O.joinRows(d1, d2, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            expected = O.joinRows(left, bcExp, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 4);
                                            break;
                                        }
                                        case 6: {
                                            DataSet ab = a.leftJoin(b).on("k");
                                            res = ab.join(c).on("k");
                                            List<DataRow> abExp = O.joinRows(left, d1, O.idx(0), O.idx(0),
                                                    JoinType.LEFT, 2, 2);
                                            expected = O.joinRows(abExp, d2, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 4, 2);
                                            break;
                                        }
                                        case 7: {
                                            DataSet ab = a.join(b).on("k");
                                            res = ab.fullJoin(c).on("k");
                                            List<DataRow> abExp = O.joinRows(left, d1, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            expected = O.joinRows(abExp, d2, O.idx(0), O.idx(0),
                                                    JoinType.FULL, 4, 2);
                                            break;
                                        }
                                        case 8: {
                                            DataSet ab = a.join(b).on("k")
                                                    .filter("v > 5");
                                            res = ab.join(c).on("k");
                                            List<DataRow> abExp = SL.filterRows(O.joinRows(left, d1,
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2),
                                                    x -> x.getBigDecimal(1) != null
                                                            && x.getBigDecimal(1).compareTo(BigDecimal.valueOf(5)) > 0);
                                            expected = O.joinRows(abExp, d2, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 4, 2);
                                            break;
                                        }
                                        default: {
                                            DataSet ab = a.join(b).on("k");
                                            res = ab.join(c).on("k").groupBy("k").count();
                                            List<DataRow> abExp = O.joinRows(left, d1, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            expected = O.groupCount(O.joinRows(abExp, d2,
                                                    O.idx(0), O.idx(0), JoinType.INNER, 4, 2), O.idx(0));
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

    // ========================================================================
    // B45 join followed by groupBy, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b45() {
        return new BatchDesign() {
            @Override public int batchNo() { return 45; }
            @Override public String title() { return "join then groupBy roll-up, 10 templates x 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(SL.sink2(id("B45", v, d), "joinGroup[v" + vf + "][" + KV_PROFILES[d] + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> dag(vf, l, r),
                                (a, b) -> oracle(vf, O.joinRows(a, b, O.idx(0), O.idx(0),
                                        JoinType.INNER, 2, 2)),
                                false));
                    }
                }
                return out;
            }

            private DataSet dag(int v, DataSet l, DataSet r) {
                DataSet j = l.join(r).on("k");
                switch (v) {
                    case 0: return j.groupBy("k").count();
                    case 1: return j.groupBy("k").sum("v");
                    case 2: return j.groupBy("label").count();
                    case 3: return j.groupBy("label").sum("v");
                    case 4: return j.groupBy("k", "label").count();
                    case 5: return j.groupBy("k").max("v");
                    case 6: return j.groupBy("label").min("v");
                    case 7: return j.groupBy("k").sum("v").orderBy("k");
                    case 8: return j.filter("v > 5").groupBy("label").count();
                    default: return j.groupBy("k").sum("v").filter("sum0 > 10");
                }
            }

            private List<DataRow> oracle(int v, List<DataRow> j) {
                switch (v) {
                    case 0: return O.groupCount(j, O.idx(0));
                    case 1: return O.groupSum(j, O.idx(0), 1);
                    case 2: return O.groupCount(j, O.idx(3));
                    case 3: return O.groupSum(j, O.idx(3), 1);
                    case 4: return O.groupCount(j, O.idx(0, 3));
                    case 5: return O.groupMinMax(j, O.idx(0), 1, true);
                    case 6: return O.groupMinMax(j, O.idx(3), 1, false);
                    case 7: {
                        List<DataRow> summed = O.groupSum(j, O.idx(0), 1);
                        DataSchema m = new DataSchema(D.f("k", DataType.StringType),
                                D.f("sum0", DataType.BigDecimalType));
                        return O.sortRows(summed, m, O.sf(m, "k", true, "DEFAULT"));
                    }
                    case 8: return O.groupCount(SL.filterRows(j,
                            x -> liftB(x.getBigDecimal(1), y -> y.compareTo(BigDecimal.valueOf(5)) > 0)),
                            O.idx(3));
                    default: return SL.filterRows(O.groupSum(j, O.idx(0), 1),
                            x -> x.getBigDecimal(1) != null
                                    && x.getBigDecimal(1).compareTo(BigDecimal.TEN) > 0);
                }
            }
        };
    }

    // ========================================================================
    // B46 groupBy followed by join, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b46() {
        return new BatchDesign() {
            @Override public int batchNo() { return 46; }
            @Override public String title() { return "groupBy→join (the aggregate result feeds the join) × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(ScenarioDef.of(id("B46", v, d), "aggJoin[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet a = lab.source("A", D.KV, left);
                                    DataSet b = lab.source("B", D.DIM, right);
                                    DataSet res;
                                    List<DataRow> expected;
                                    List<DataRow> summed = O.groupSum(left, O.idx(0), 1);
                                    List<DataRow> counted = O.groupCount(left, O.idx(0));
                                    switch (vf) {
                                        case 0:
                                            res = a.groupBy("k").sum("v").join(b).on("k");
                                            expected = O.joinRows(summed, right, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            break;
                                        case 1:
                                            res = a.groupBy("k").sum("v").leftJoin(b).on("k");
                                            expected = O.joinRows(summed, right, O.idx(0), O.idx(0),
                                                    JoinType.LEFT, 2, 2);
                                            break;
                                        case 2:
                                            res = b.join(a.groupBy("k").count()).on("k");
                                            expected = O.joinRows(right, counted, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            break;
                                        case 3:
                                            res = a.groupBy("k").sum("v").join(
                                                    a.groupBy("k").count()).on("k");
                                            expected = O.joinRows(summed, counted, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            break;
                                        case 4:
                                            res = a.groupBy("k").sum("v").join(b).on("k")
                                                    .select("k", "sum0", "label");
                                            expected = SL.mapRows(O.joinRows(summed, right,
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2), x -> {
                                                DataRow o = new DataRow(3);
                                                o.set(0, x.get(0));
                                                o.set(1, x.get(1));
                                                o.set(2, x.get(3));
                                                return o;
                                            });
                                            break;
                                        case 5:
                                            res = a.groupBy("k").sum("v").join(b).on("k")
                                                    .groupBy("label").sum("sum0");
                                            expected = O.groupSum(O.joinRows(summed, right,
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2), O.idx(3), 1);
                                            break;
                                        case 6:
                                            res = a.filter("v > 0").groupBy("k").sum("v")
                                                    .leftJoin(b).on("k");
                                            expected = O.joinRows(
                                                    O.groupSum(SL.filterRows(left,
                                                            x -> liftB(x.getBigDecimal(1),
                                                                    y -> y.signum() > 0)),
                                                            O.idx(0), 1),
                                                    right, O.idx(0), O.idx(0), JoinType.LEFT, 2, 2);
                                            break;
                                        case 7:
                                            res = a.groupBy("k").sum("v").join(
                                                    a.groupBy("k").max("v")).on("k");
                                            expected = O.joinRows(summed,
                                                    O.groupMinMax(left, O.idx(0), 1, true),
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2);
                                            break;
                                        case 8:
                                            res = a.union(a).groupBy("k").sum("v").join(b).on("k");
                                            expected = O.joinRows(O.groupSum(SL.concat(left, left),
                                                    O.idx(0), 1), right, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            break;
                                        default:
                                            res = a.groupBy("k").sum("v").join(b).on("k")
                                                    .orderBy("k");
                                            expected = O.joinRows(summed, right, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            break;
                                    }
                                    if (vf == 9) {
                                        lab.expect(res, expected);  // compared as a multiset, so unsorted
                                    } else {
                                        lab.expect(res, expected);
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B47 coGroup basics, 10 function variants over 10 profiles
    // ========================================================================

    private static BatchDesign b47() {
        return new BatchDesign() {
            @Override public int batchNo() { return 47; }
            @Override public String title() { return "coGroup × 10 function variants × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(SL.sink2(id("B47", v, d), "coGroup[v" + vf + "][" + KV_PROFILES[d] + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> l.coGroup(r, coGroupFn(vf)).on("k"),
                                (a, b) -> coGroupOracle(vf, a, b),
                                false));
                    }
                }
                return out;
            }

            private CoGroupFunction coGroupFn(int v) {
                switch (v) {
                    case 0:
                        return new CountBoth();
                    case 1:
                        return new SumLeft();
                    case 2:
                        return new EmitPairs();
                    case 3:
                        return new EmitLeftOnly();
                    case 4:
                        return new EmitRightOnly();
                    case 5:
                        return new ConcatLabels();
                    case 6:
                        return new MaxLeft();
                    case 7:
                        return new KeysWithBoth();
                    case 8:
                        return new LeftTimesCount();
                    default:
                        return new UnionFlatten();
                }
            }

            private List<DataRow> coGroupOracle(int v, List<DataRow> a, List<DataRow> b) {
                // observed behaviour: coGroup merges a null key from both sides into one co-group, so here null equals null.
                Map<Object, List<DataRow>> gl = groupBy(a, 0);
                Map<Object, List<DataRow>> gr = groupBy(b, 0);
                List<Object> keys = new ArrayList<>();
                for (Object k : gl.keySet()) {
                    if (!keys.contains(k)) {
                        keys.add(k);
                    }
                }
                for (Object k : gr.keySet()) {
                    if (!keys.contains(k)) {
                        keys.add(k);
                    }
                }
                List<DataRow> out = new ArrayList<>();
                for (Object k : keys) {
                    List<DataRow> ls = gl.getOrDefault(k, java.util.Collections.emptyList());
                    List<DataRow> rs = gr.getOrDefault(k, java.util.Collections.emptyList());
                    out.addAll(applyFn(v, k, ls, rs));
                }
                return out;
            }

            private List<DataRow> applyFn(int v, Object rawKey, List<DataRow> ls, List<DataRow> rs) {
                Object k = rawKey == NULL_KEY ? null : rawKey;
                List<DataRow> out = new ArrayList<>();
                switch (v) {
                    case 0: {
                        DataRow o = new DataRow(3);
                        o.set(0, k);
                        o.set(1, (long) ls.size());
                        o.set(2, (long) rs.size());
                        out.add(o);
                        break;
                    }
                    case 1: {
                        BigDecimal sum = BigDecimal.ZERO;
                        boolean any = false;
                        for (DataRow l : ls) {
                            if (l.get(1) != null) {
                                sum = sum.add(l.getBigDecimal(1));
                                any = true;
                            }
                        }
                        DataRow o = new DataRow(2);
                        o.set(0, k);
                        o.set(1, any ? sum.setScale(10, RoundingMode.HALF_UP) : null);
                        out.add(o);
                        break;
                    }
                    case 2: {
                        for (DataRow l : ls) {
                            for (DataRow r : rs) {
                                DataRow o = new DataRow(4);
                                o.set(0, l.get(0));
                                o.set(1, l.get(1));
                                o.set(2, r.get(0));
                                o.set(3, r.get(1));
                                out.add(o);
                            }
                        }
                        break;
                    }
                    case 3: {
                        for (DataRow l : ls) {
                            DataRow o = new DataRow(2);
                            o.set(0, l.get(0));
                            o.set(1, l.get(1));
                            out.add(o);
                        }
                        break;
                    }
                    case 4: {
                        for (DataRow r : rs) {
                            DataRow o = new DataRow(2);
                            o.set(0, r.get(0));
                            o.set(1, r.get(1));
                            out.add(o);
                        }
                        break;
                    }
                    case 5: {
                        StringBuilder sb = new StringBuilder();
                        for (DataRow r : rs) {
                            if (r.get(1) != null) {
                                if (sb.length() > 0) {
                                    sb.append(",");
                                }
                                sb.append(r.get(1));
                            }
                        }
                        DataRow o = new DataRow(2);
                        o.set(0, k);
                        o.set(1, sb.length() == 0 ? null : sb.toString());
                        out.add(o);
                        break;
                    }
                    case 6: {
                        BigDecimal max = null;
                        for (DataRow l : ls) {
                            if (l.get(1) != null
                                    && (max == null || l.getBigDecimal(1).compareTo(max) > 0)) {
                                max = l.getBigDecimal(1);
                            }
                        }
                        DataRow o = new DataRow(2);
                        o.set(0, k);
                        o.set(1, max);
                        out.add(o);
                        break;
                    }
                    case 7: {
                        if (!ls.isEmpty() && !rs.isEmpty()) {
                            DataRow o = new DataRow(1);
                            o.set(0, k);
                            out.add(o);
                        }
                        break;
                    }
                    case 8: {
                        BigDecimal sum = BigDecimal.ZERO;
                        boolean any = false;
                        for (DataRow l : ls) {
                            if (l.get(1) != null) {
                                sum = sum.add(l.getBigDecimal(1));
                                any = true;
                            }
                        }
                        DataRow o = new DataRow(2);
                        o.set(0, k);
                        o.set(1, any ? sum.multiply(BigDecimal.valueOf(rs.size()))
                                .setScale(10, RoundingMode.HALF_UP) : null);
                        out.add(o);
                        break;
                    }
                    default: {
                        // UnionFlatten: the left half is [k,v,null], the right half is [k,null,label]
                        for (DataRow l : ls) {
                            DataRow o = new DataRow(3);
                            o.set(0, l.get(0));
                            o.set(1, l.get(1));
                            out.add(o);
                        }
                        for (DataRow r : rs) {
                            DataRow o = new DataRow(3);
                            o.set(0, r.get(0));
                            o.set(2, r.get(1));
                            out.add(o);
                        }
                        break;
                    }
                }
                return out;
            }
        };
    }

    private static Map<Object, List<DataRow>> groupBy(List<DataRow> rows, int keyIdx) {
        Map<Object, List<DataRow>> m = new LinkedHashMap<>();
        for (DataRow r : rows) {
            Object k = r.get(keyIdx);
            if (k == null) {
                k = NULL_KEY;
            }
            m.computeIfAbsent(k, x -> new ArrayList<>()).add(r);
        }
        return m;
    }

    /** Sentinel for a null grouping key: coGroup merges null keys from both sides. */
    private static final Object NULL_KEY = new Object() {
        @Override public String toString() { return "null"; }
    };

    // ---- coGroup function implementations ----

    static final class CountBoth implements CoGroupFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType),
                    D.f("lc", DataType.LongType), D.f("rc", DataType.LongType));
        }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector out) {
            long lc = 0;
            long rc = 0;
            Object k = null;
            for (DataRow r : a) {
                lc++;
                k = r.get(0);
            }
            for (DataRow r : b) {
                rc++;
                k = r.get(0);
            }
            DataRow o = new DataRow(3);
            o.set(0, k);
            o.set(1, lc);
            o.set(2, rc);
            out.collect(o);
        }
    }

    static final class SumLeft implements CoGroupFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType), D.f("s", DataType.BigDecimalType));
        }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector out) {
            BigDecimal sum = BigDecimal.ZERO;
            boolean any = false;
            Object k = null;
            for (DataRow r : a) {
                k = r.get(0);
                if (r.get(1) != null) {
                    sum = sum.add(r.getBigDecimal(1));
                    any = true;
                }
            }
            for (DataRow r : b) {
                if (k == null) {
                    k = r.get(0);
                }
            }
            DataRow o = new DataRow(2);
            o.set(0, k);
            o.set(1, any ? sum.setScale(10, RoundingMode.HALF_UP) : null);
            out.collect(o);
        }
    }

    static final class EmitPairs implements CoGroupFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("lk", DataType.StringType), D.f("lv", DataType.BigDecimalType),
                    D.f("rk", DataType.StringType), D.f("rl", DataType.StringType));
        }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector out) {
            List<DataRow> ls = new ArrayList<>();
            List<DataRow> rs = new ArrayList<>();
            a.forEach(ls::add);
            b.forEach(rs::add);
            for (DataRow l : ls) {
                for (DataRow r : rs) {
                    DataRow o = new DataRow(4);
                    o.set(0, l.get(0));
                    o.set(1, l.get(1));
                    o.set(2, r.get(0));
                    o.set(3, r.get(1));
                    out.collect(o);
                }
            }
        }
    }

    static final class EmitLeftOnly implements CoGroupFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType), D.f("v", DataType.BigDecimalType));
        }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector out) {
            for (DataRow l : a) {
                DataRow o = new DataRow(2);
                o.set(0, l.get(0));
                o.set(1, l.get(1));
                out.collect(o);
            }
        }
    }

    static final class EmitRightOnly implements CoGroupFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType), D.f("label", DataType.StringType));
        }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector out) {
            for (DataRow r : b) {
                DataRow o = new DataRow(2);
                o.set(0, r.get(0));
                o.set(1, r.get(1));
                out.collect(o);
            }
        }
    }

    static final class ConcatLabels implements CoGroupFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType), D.f("labels", DataType.StringType));
        }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector out) {
            Object k = null;
            StringBuilder sb = new StringBuilder();
            for (DataRow r : a) {
                k = r.get(0);
            }
            for (DataRow r : b) {
                if (k == null) {
                    k = r.get(0);
                }
                if (r.get(1) != null) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(r.get(1));
                }
            }
            DataRow o = new DataRow(2);
            o.set(0, k);
            o.set(1, sb.length() == 0 ? null : sb.toString());
            out.collect(o);
        }
    }

    static final class MaxLeft implements CoGroupFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType), D.f("max", DataType.BigDecimalType));
        }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector out) {
            BigDecimal max = null;
            Object k = null;
            for (DataRow r : a) {
                k = r.get(0);
                if (r.get(1) != null && (max == null || r.getBigDecimal(1).compareTo(max) > 0)) {
                    max = r.getBigDecimal(1);
                }
            }
            for (DataRow r : b) {
                if (k == null) {
                    k = r.get(0);
                }
            }
            DataRow o = new DataRow(2);
            o.set(0, k);
            o.set(1, max);
            out.collect(o);
        }
    }

    static final class KeysWithBoth implements CoGroupFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType));
        }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector out) {
            boolean hasL = false;
            boolean hasR = false;
            Object k = null;
            for (DataRow r : a) {
                hasL = true;
                k = r.get(0);
            }
            for (DataRow r : b) {
                hasR = true;
                k = r.get(0);
            }
            if (hasL && hasR) {
                DataRow o = new DataRow(1);
                o.set(0, k);
                out.collect(o);
            }
        }
    }

    static final class LeftTimesCount implements CoGroupFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType), D.f("x", DataType.BigDecimalType));
        }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector out) {
            BigDecimal sum = BigDecimal.ZERO;
            boolean any = false;
            long rc = 0;
            Object k = null;
            for (DataRow r : a) {
                k = r.get(0);
                if (r.get(1) != null) {
                    sum = sum.add(r.getBigDecimal(1));
                    any = true;
                }
            }
            for (DataRow r : b) {
                rc++;
                if (k == null) {
                    k = r.get(0);
                }
            }
            DataRow o = new DataRow(2);
            o.set(0, k);
            o.set(1, any ? sum.multiply(BigDecimal.valueOf(rc))
                    .setScale(10, RoundingMode.HALF_UP) : null);
            out.collect(o);
        }
    }

    static final class UnionFlatten implements CoGroupFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType),
                    D.f("v", DataType.BigDecimalType), D.f("label", DataType.StringType));
        }

        @Override
        public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector out) {
            for (DataRow l : a) {
                DataRow o = new DataRow(3);
                o.set(0, l.get(0));
                o.set(1, l.get(1));
                out.collect(o);
            }
            for (DataRow r : b) {
                DataRow o = new DataRow(3);
                o.set(0, r.get(0));
                o.set(2, r.get(1));
                out.collect(o);
            }
        }
    }

    // ========================================================================
    // B48 coGroup with unmatched rows and null keys, 10 by 10, focused on one-sided group semantics
    // ========================================================================

    private static BatchDesign b48() {
        return new BatchDesign() {
            @Override public int batchNo() { return 48; }
            @Override public String title() { return "coGroup with a group on one side only / null keys isolated × 10 variants × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < DIM_NAMES.length; d++) {
                        final List<DataRow> left = kvProfile((d + v) % 10);
                        final List<DataRow> right = dimProfile(d);
                        out.add(SL.sink2(id("B48", v, d), "coGroupUnmatched[v" + vf + "]["
                                        + DIM_NAMES[d] + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> l.coGroup(r, new CountBoth()).on("k"),
                                (a, b) -> coGroupCountOracle(a, b),
                                false));
                    }
                }
                return out;
            }

            private List<DataRow> coGroupCountOracle(List<DataRow> a, List<DataRow> b) {
                // observed behaviour: null keys from both sides merge into one co-group.
                Map<Object, List<DataRow>> gl = groupBy(a, 0);
                Map<Object, List<DataRow>> gr = groupBy(b, 0);
                List<Object> keys = new ArrayList<>();
                for (Object k : gl.keySet()) {
                    if (!keys.contains(k)) {
                        keys.add(k);
                    }
                }
                for (Object k : gr.keySet()) {
                    if (!keys.contains(k)) {
                        keys.add(k);
                    }
                }
                List<DataRow> out = new ArrayList<>();
                for (Object rawK : keys) {
                    Object k = rawK == NULL_KEY ? null : rawK;
                    DataRow o = new DataRow(3);
                    o.set(0, k);
                    o.set(1, (long) gl.getOrDefault(rawK, java.util.Collections.emptyList()).size());
                    o.set(2, (long) gr.getOrDefault(rawK, java.util.Collections.emptyList()).size());
                    out.add(o);
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B49 reduceGroup with a custom per-group function, 10 functions over 10 profiles
    // ========================================================================

    private static BatchDesign b49() {
        return new BatchDesign() {
            @Override public int batchNo() { return 49; }
            @Override public String title() { return "groupBy.reduceGroup custom reduction × 10 functions × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B49", v, d), "reduceGroup[v" + vf + "][" + KV_PROFILES[d] + "]",
                                D.KV, data, ds -> ds.groupBy("k").reduceGroup(reduceFn(vf)),
                                in -> reduceOracle(vf, in), false));
                    }
                }
                return out;
            }

            private GroupReduceFunction reduceFn(int v) {
                switch (v) {
                    case 0:
                        return new FirstRowFn();
                    case 1:
                        return new LastRowFn();
                    case 2:
                        return new CountAndSumFn();
                    case 3:
                        return new EmitEachTwiceFn();
                    case 4:
                        return new EmitNoneFn();
                    case 5:
                        return new EmitVPlusIndexFn();
                    case 6:
                        return new GroupKeyListFn();
                    case 7:
                        return new MinMaxRowFn();
                    case 8:
                        return new FilterBigFn();
                    default:
                        return new NullPadFn();
                }
            }

            private List<DataRow> reduceOracle(int v, List<DataRow> in) {
                Map<Object, List<DataRow>> groups = groupBy(in, 0);
                List<DataRow> out = new ArrayList<>();
                for (Map.Entry<Object, List<DataRow>> e : groups.entrySet()) {
                    Object cleanKey = e.getKey() == NULL_KEY ? null : e.getKey();
                    List<DataRow> rows = e.getValue();
                    switch (v) {
                        case 0:
                            out.add(rows.get(0).copy());
                            break;
                        case 1:
                            out.add(rows.get(rows.size() - 1).copy());
                            break;
                        case 2: {
                            BigDecimal sum = BigDecimal.ZERO;
                            boolean any = false;
                            for (DataRow r : rows) {
                                if (r.get(1) != null) {
                                    sum = sum.add(r.getBigDecimal(1));
                                    any = true;
                                }
                            }
                            DataRow o = new DataRow(3);
                            o.set(0, cleanKey);
                            o.set(1, (long) rows.size());
                            o.set(2, any ? sum.setScale(10, RoundingMode.HALF_UP) : null);
                            out.add(o);
                            break;
                        }
                        case 3:
                            for (DataRow r : rows) {
                                out.add(r.copy());
                                out.add(r.copy());
                            }
                            break;
                        case 4:
                            break;
                        case 5: {
                            BigDecimal inc = BigDecimal.ZERO;
                            for (DataRow r : rows) {
                                DataRow o = new DataRow(2);
                                o.set(0, r.get(0));
                                BigDecimal val = r.getBigDecimal(1);
                                o.set(1, val == null ? null : val.add(inc));
                                out.add(o);
                                inc = inc.add(BigDecimal.ONE);
                            }
                            break;
                        }
                        case 6: {
                            StringBuilder sb = new StringBuilder();
                            for (DataRow r : rows) {
                                if (r.get(1) != null) {
                                    if (sb.length() > 0) {
                                        sb.append("|");
                                    }
                                    sb.append(r.getBigDecimal(1).toPlainString());
                                }
                            }
                            DataRow o = new DataRow(2);
                            o.set(0, cleanKey);
                            o.set(1, sb.length() == 0 ? null : sb.toString());
                            out.add(o);
                            break;
                        }
                        case 7: {
                            // exactly MinMaxRowFn: seed with the first row, replace only by a non-null strictly better row
                            DataRow min = null;
                            DataRow max = null;
                            for (DataRow r : rows) {
                                if (min == null) {
                                    min = r;
                                    max = r;
                                }
                                BigDecimal rv = r.getBigDecimal(1);
                                if (rv != null) {
                                    if (min.getBigDecimal(1) == null
                                            || rv.compareTo(min.getBigDecimal(1)) < 0) {
                                        min = r;
                                    }
                                    if (max.getBigDecimal(1) == null
                                            || rv.compareTo(max.getBigDecimal(1)) > 0) {
                                        max = r;
                                    }
                                }
                            }
                            if (min != null) {
                                out.add(min.copy());
                                out.add(max.copy());
                            }
                            break;
                        }
                        case 8:
                            for (DataRow r : rows) {
                                if (liftB(r.getBigDecimal(1), x -> x.compareTo(BigDecimal.TEN) > 0)) {
                                    out.add(r.copy());
                                }
                            }
                            break;
                        default: {
                            DataRow o = new DataRow(2);
                            o.set(0, cleanKey);
                            o.set(1, null);
                            out.add(o);
                            break;
                        }
                    }
                }
                return out;
            }
        };
    }

    private static int cmp1(DataRow a, DataRow b) {
        BigDecimal x = a.getBigDecimal(1);
        BigDecimal y = b.getBigDecimal(1);
        if (x == null && y == null) {
            return 0;
        }
        if (x == null) {
            return -1;
        }
        if (y == null) {
            return 1;
        }
        return x.compareTo(y);
    }

    static final class FirstRowFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return D.KV;
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            for (DataRow r : rows) {
                out.collect(r.copy());
                return;
            }
        }
    }

    static final class LastRowFn implements GroupReduceFunction {
        private DataRow last;

        @Override
        public DataSchema getResultSchema() {
            return D.KV;
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            last = null;
            for (DataRow r : rows) {
                last = r.copy();
            }
            if (last != null) {
                out.collect(last);
            }
        }
    }

    static final class CountAndSumFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType), D.f("c", DataType.LongType),
                    D.f("s", DataType.BigDecimalType));
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            long c = 0;
            BigDecimal sum = BigDecimal.ZERO;
            boolean any = false;
            Object k = null;
            for (DataRow r : rows) {
                c++;
                k = r.get(0);
                if (r.get(1) != null) {
                    sum = sum.add(r.getBigDecimal(1));
                    any = true;
                }
            }
            DataRow o = new DataRow(3);
            o.set(0, k);
            o.set(1, c);
            o.set(2, any ? sum.setScale(10, RoundingMode.HALF_UP) : null);
            out.collect(o);
        }
    }

    static final class EmitEachTwiceFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return D.KV;
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            for (DataRow r : rows) {
                out.collect(r.copy());
                out.collect(r.copy());
            }
        }
    }

    static final class EmitNoneFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return D.KV;
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            // emits nothing
        }
    }

    static final class EmitVPlusIndexFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return D.KV;
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            BigDecimal inc = BigDecimal.ZERO;
            for (DataRow r : rows) {
                DataRow o = new DataRow(2);
                o.set(0, r.get(0));
                BigDecimal v = r.getBigDecimal(1);
                o.set(1, v == null ? null : v.add(inc));
                out.collect(o);
                inc = inc.add(BigDecimal.ONE);
            }
        }
    }

    static final class GroupKeyListFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType), D.f("vals", DataType.StringType));
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            Object k = null;
            StringBuilder sb = new StringBuilder();
            for (DataRow r : rows) {
                k = r.get(0);
                if (r.get(1) != null) {
                    if (sb.length() > 0) {
                        sb.append("|");
                    }
                    sb.append(r.getBigDecimal(1).toPlainString());
                }
            }
            DataRow o = new DataRow(2);
            o.set(0, k);
            o.set(1, sb.length() == 0 ? null : sb.toString());
            out.collect(o);
        }
    }

    static final class MinMaxRowFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return D.KV;
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            DataRow min = null;
            DataRow max = null;
            for (DataRow r : rows) {
                if (min == null) {
                    min = r;
                }
                if (max == null) {
                    max = r;
                }
                BigDecimal v = r.getBigDecimal(1);
                if (v != null) {
                    if (min.getBigDecimal(1) == null || v.compareTo(min.getBigDecimal(1)) < 0) {
                        min = r;
                    }
                    if (max.getBigDecimal(1) == null || v.compareTo(max.getBigDecimal(1)) > 0) {
                        max = r;
                    }
                }
            }
            if (min != null) {
                out.collect(min.copy());
            }
            if (max != null) {
                out.collect(max.copy());
            }
        }
    }

    static final class FilterBigFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return D.KV;
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            for (DataRow r : rows) {
                BigDecimal v = r.getBigDecimal(1);
                if (v != null && v.compareTo(BigDecimal.TEN) > 0) {
                    out.collect(r.copy());
                }
            }
        }
    }

    static final class NullPadFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType), D.f("v", DataType.BigDecimalType));
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            Object k = null;
            for (DataRow r : rows) {
                k = r.get(0);
            }
            DataRow o = new DataRow(2);
            o.set(0, k);
            o.set(1, null);
            out.collect(o);
        }
    }

    // ========================================================================
    // B50 reduceGroup over the whole table, 10 functions over 10 profiles
    // ========================================================================

    private static BatchDesign b50() {
        return new BatchDesign() {
            @Override public int batchNo() { return 50; }
            @Override public String title() { return "reduceGroup over the whole table × 10 functions × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B50", v, d), "wholeReduce[v" + vf + "][" + KV_PROFILES[d] + "]",
                                D.KV, data, ds -> ds.reduceGroup(wholeFn(vf)),
                                in -> wholeOracle(vf, in), false));
                    }
                }
                return out;
            }

            private GroupReduceFunction wholeFn(int v) {
                switch (v) {
                    case 0:
                        return new CountAllFn();
                    case 1:
                        return new SumAllFn();
                    case 2:
                        return new GlobalMinMaxFn();
                    case 3:
                        return new FlattenAllFn();
                    case 4:
                        return new EmitNoneFn();
                    case 5:
                        return new SingletonRowFn();
                    case 6:
                        return new NullRowFn();
                    case 7:
                        return new DistinctKFn();
                    case 8:
                        return new ReverseEmitFn();
                    default:
                        return new ConcatAllFn();
                }
            }

            private List<DataRow> wholeOracle(int v, List<DataRow> in) {
                List<DataRow> out = new ArrayList<>();
                switch (v) {
                    case 0: {
                        DataRow o = new DataRow(1);
                        o.set(0, (long) in.size());
                        out.add(o);
                        break;
                    }
                    case 1: {
                        BigDecimal sum = BigDecimal.ZERO;
                        boolean any = false;
                        for (DataRow r : in) {
                            if (r.get(1) != null) {
                                sum = sum.add(r.getBigDecimal(1));
                                any = true;
                            }
                        }
                        DataRow o = new DataRow(1);
                        o.set(0, any ? sum.setScale(10, RoundingMode.HALF_UP) : null);
                        out.add(o);
                        break;
                    }
                    case 2: {
                        if (!in.isEmpty()) {
                            DataRow min = in.get(0);
                            DataRow max = in.get(0);
                            for (DataRow r : in) {
                                if (cmp1(r, min) < 0) {
                                    min = r;
                                }
                                if (cmp1(r, max) > 0) {
                                    max = r;
                                }
                            }
                            out.add(min.copy());
                            out.add(max.copy());
                        }
                        break;
                    }
                    case 3:
                        for (DataRow r : in) {
                            out.add(r.copy());
                        }
                        break;
                    case 4:
                        break;
                    case 5: {
                        DataRow o = new DataRow(1);
                        o.set(0, "rows=" + in.size());
                        out.add(o);
                        break;
                    }
                    case 6: {
                        out.add(new DataRow(1));
                        break;
                    }
                    case 7: {
                        java.util.LinkedHashSet<Object> seen = new java.util.LinkedHashSet<>();
                        for (DataRow r : in) {
                            if (r.get(0) != null && seen.add(r.get(0))) {
                                DataRow o = new DataRow(1);
                                o.set(0, r.get(0));
                                out.add(o);
                            }
                        }
                        break;
                    }
                    case 8:
                        for (int i = in.size() - 1; i >= 0; i--) {
                            out.add(in.get(i).copy());
                        }
                        break;
                    default: {
                        StringBuilder sb = new StringBuilder();
                        for (DataRow r : in) {
                            if (r.get(0) != null) {
                                sb.append(r.get(0));
                            }
                        }
                        DataRow o = new DataRow(1);
                        o.set(0, sb.toString());
                        out.add(o);
                        break;
                    }
                }
                return out;
            }
        };
    }

    static final class CountAllFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("n", DataType.LongType));
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            long c = 0;
            for (DataRow r : rows) {
                c++;
            }
            DataRow o = new DataRow(1);
            o.set(0, c);
            out.collect(o);
        }
    }

    static final class SumAllFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("s", DataType.BigDecimalType));
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            BigDecimal sum = BigDecimal.ZERO;
            boolean any = false;
            for (DataRow r : rows) {
                if (r.get(1) != null) {
                    sum = sum.add(r.getBigDecimal(1));
                    any = true;
                }
            }
            DataRow o = new DataRow(1);
            o.set(0, any ? sum.setScale(10, RoundingMode.HALF_UP) : null);
            out.collect(o);
        }
    }

    static final class GlobalMinMaxFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return D.KV;
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            DataRow min = null;
            DataRow max = null;
            for (DataRow r : rows) {
                if (min == null) {
                    min = r;
                    max = r;
                }
                if (cmp1(r, min) < 0) {
                    min = r;
                }
                if (cmp1(r, max) > 0) {
                    max = r;
                }
            }
            if (min != null) {
                out.collect(min.copy());
                out.collect(max.copy());
            }
        }
    }

    static final class FlattenAllFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return D.KV;
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            for (DataRow r : rows) {
                out.collect(r.copy());
            }
        }
    }

    static final class SingletonRowFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("info", DataType.StringType));
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            long c = 0;
            for (DataRow r : rows) {
                c++;
            }
            DataRow o = new DataRow(1);
            o.set(0, "rows=" + c);
            out.collect(o);
        }
    }

    static final class NullRowFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("x", DataType.StringType));
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            out.collect(new DataRow(1));
        }
    }

    static final class DistinctKFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("k", DataType.StringType));
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            java.util.LinkedHashSet<Object> seen = new java.util.LinkedHashSet<>();
            for (DataRow r : rows) {
                if (r.get(0) != null && seen.add(r.get(0))) {
                    DataRow o = new DataRow(1);
                    o.set(0, r.get(0));
                    out.collect(o);
                }
            }
        }
    }

    static final class ReverseEmitFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return D.KV;
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            List<DataRow> all = new ArrayList<>();
            for (DataRow r : rows) {
                all.add(r.copy());
            }
            for (int i = all.size() - 1; i >= 0; i--) {
                out.collect(all.get(i));
            }
        }
    }

    static final class ConcatAllFn implements GroupReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("ks", DataType.StringType));
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            StringBuilder sb = new StringBuilder();
            for (DataRow r : rows) {
                if (r.get(0) != null) {
                    sb.append(r.get(0));
                }
            }
            DataRow o = new DataRow(1);
            o.set(0, sb.toString());
            out.collect(o);
        }
    }

    // ========================================================================
    // B51 combineGroup, 10 shards over 10 profiles; combine runs once per consecutive shard
    // ========================================================================

    private static BatchDesign b51() {
        return new BatchDesign() {
            @Override public int batchNo() { return 51; }
            @Override public String title() { return "combineGroup partial aggregation per shard (shards=1/2/3/4) × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                int[] shardsList = {1, 1, 2, 2, 3, 3, 4, 4, 2, 3};
                for (int v = 0; v < shardsList.length; v++) {
                    final int shards = shardsList[v];
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B51", v, d), "combineGroup[shards=" + shards
                                        + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    /**
                                     * // combineGroup cuts consecutive shards in source order and calls combine once per shard.
                                     * // The oracle reproduces those shards and collects the rows each one emits; order matters here, so the comparison is a multiset.
                                     */
                                    io.rowweave.examples.scenariolab.ScenarioLab l2 = lab;
                                    DataSet res = l2.source(D.KV, data)
                                            .combineGroup(new PairIdentityCombine(), shards);
                                    List<DataRow> expected = shardOracle(data, shards);
                                    l2.expect(res, expected);
                                }));
                    }
                }
                return out;
            }

            /** Simulates combine: for each shard it applies identity to the rows, so a shard emits the rows it holds. */
            private List<DataRow> shardOracle(List<DataRow> in, int shards) {
                List<DataRow> out = new ArrayList<>();
                int n = in.size();
                for (int s = 0; s < shards; s++) {
                    int from = (int) ((long) n * s / shards);
                    int to = (int) ((long) n * (s + 1) / shards);
                    for (int i = from; i < to; i++) {
                        out.add(in.get(i).copy());
                    }
                }
                return out;
            }
        };
    }

    /** Identity combine: output equals input, with the same structure. */
    static final class PairIdentityCombine implements GroupCombineFunction {
        @Override
        public void combine(Iterable<DataRow> rows, Collector out) {
            for (DataRow r : rows) {
                out.collect(r.copy());
            }
        }
    }

    // ========================================================================
    // B52 combineReduceGroup, 10 shards over 10 profiles
    // ========================================================================

    private static BatchDesign b52() {
        return new BatchDesign() {
            @Override public int batchNo() { return 52; }
            @Override public String title() { return "combineReduceGroup (partial aggregation + reduction) × 10 shards × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                int[] shardsList = {1, 2, 3, 4, 5, 2, 3, 4, 5, 1};
                for (int v = 0; v < shardsList.length; v++) {
                    final int shards = shardsList[v];
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B52", v, d), "combineReduce[shards=" + shards
                                        + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet res = lab.source(D.KV, data)
                                            .combineReduceGroup(new SumCountCombineReduce(), shards);
                                    BigDecimal sum = BigDecimal.ZERO;
                                    for (DataRow r : data) {
                                        if (r.get(1) != null) {
                                            sum = sum.add(r.getBigDecimal(1));
                                        }
                                    }
                                    long shardRows = 0;
                                    int n = data.size();
                                    for (int sIdx = 0; sIdx < shards; sIdx++) {
                                        int from = (int) ((long) n * sIdx / shards);
                                        int to = (int) ((long) n * (sIdx + 1) / shards);
                                        if (to > from) {
                                            shardRows++;
                                        }
                                    }
                                    DataRow o = new DataRow(2);
                                    o.set(0, shardRows);
                                    o.set(1, shardRows == 0
                                            ? null : sum.setScale(10, RoundingMode.HALF_UP));
                                    List<DataRow> expected = new ArrayList<>();
                                    expected.add(o);
                                    lab.expect(res, expected);
                                }));
                    }
                }
                return out;
            }
        };
    }

    /**
     * combine: each shard emits one row shaped like the input key-value structure (k=null, v=the shard's
     * sum) — the contract is that combine output keeps the input structure;
     * reduce: fold all combine rows and emit [row count, total sum].
     */
    static final class SumCountCombineReduce implements GroupCombineReduceFunction {
        @Override
        public DataSchema getResultSchema() {
            return new DataSchema(D.f("c", DataType.LongType), D.f("s", DataType.BigDecimalType));
        }

        @Override
        public void combine(Iterable<DataRow> rows, Collector out) {
            BigDecimal s = BigDecimal.ZERO;
            long c = 0;
            for (DataRow r : rows) {
                c++;
                if (r.get(1) != null) {
                    s = s.add(r.getBigDecimal(1));
                }
            }
            if (c > 0) {
                DataRow o = new DataRow(2);
                o.set(0, null);
                o.set(1, s);
                out.collect(o);
            }
        }

        @Override
        public void reduce(Iterable<DataRow> rows, Collector out) {
            long c = 0;
            BigDecimal s = BigDecimal.ZERO;
            for (DataRow r : rows) {
                BigDecimal rs = r.getBigDecimal(1);
                c++;
                if (rs != null) {
                    s = s.add(rs);
                }
            }
            DataRow o = new DataRow(2);
            o.set(0, c);
            o.set(1, c == 0 ? null : s.setScale(10, RoundingMode.HALF_UP));
            out.collect(o);
        }
    }
}
