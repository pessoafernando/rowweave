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
import static io.rowweave.examples.scenariolab.batches.FamilyB.ordProfile;

/**
 * Family D (B33 to B42): the whole join family. INNER, LEFT, RIGHT and FULL crossed with
 * duplicate keys, null keys and composite keys, the HASH and SORT_MERGE hints, a self join and
 * empty inputs. The oracle is an independent nested-loop implementation in which null never
 * equals null and equal keys produce the cartesian product.
 */
public final class FamilyD {

    private FamilyD() {
    }

    public static List<BatchDesign> batches() {
        return Arrays.asList(
                b33(), b34(), b35(), b36(), b37(),
                b38(), b39(), b40(), b41(), b42());
    }

    /** The 10 profiles of the DIM side, shaped (k,label). */
    static List<DataRow> dimProfile(int i) {
        switch (i) {
            case 0: return D.dimBase();
            case 1: return D.empty();
            case 2: return D.rows(new Object[]{"g1", "only"});
            case 3: return D.rows(
                    new Object[]{"g1", "a"}, new Object[]{"g1", "b"}, new Object[]{"g1", "c"});
            case 4: return D.rows(new Object[]{null, "none"}, new Object[]{"g2", "two"});
            case 5: return D.rows(new Object[]{"g3", "three"}, new Object[]{"g9", "orphan"});
            case 6: return D.rows(
                    new Object[]{"", "empty-string"}, new Object[]{"g1", "one"}, new Object[]{" ", "whitespace"});
            case 7: return D.rows(
                    new Object[]{"α", "alpha"}, new Object[]{"g1", "one"}, new Object[]{"G1", "upper"});
            case 8: {
                List<DataRow> out = new ArrayList<>();
                for (int j = 0; j < 12; j++) {
                    out.add(D.row("g" + (j % 4), "label" + j));
                }
                return out;
            }
            default: return D.rows(
                    new Object[]{"g1", null}, new Object[]{null, null},
                    new Object[]{"g2", "two"}, new Object[]{"g4", null});
        }
    }

    static final String[] DIM_NAMES = {
            "base", "empty", "single", "tripleG1", "nullKey", "noOverlap", "blank",
            "unicode", "dupes12", "nullVals"};

    // ========================================================================
    // B33 inner join on unique keys, 10 left and right combinations over 10 profiles
    // ========================================================================

    private static BatchDesign b33() {
        return new BatchDesign() {
            @Override public int batchNo() { return 33; }
            @Override public String title() { return "inner join (KV×DIM, select after join) × 10 combinations × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        final boolean useSelect = vf >= 5;
                        out.add(SL.sink2(id("B33", v, d), "innerJoin[" + KV_PROFILES[d] + "+dim"
                                        + (useSelect ? "+select" : "") + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> useSelect
                                        ? l.join(r).on("k").select("k", "v", "label")
                                        : l.join(r).on("k"),
                                (a, b) -> projectJoin(O.joinRows(a, b, O.idx(0), O.idx(0),
                                        JoinType.INNER, 2, 2), useSelect),
                                false));
                    }
                }
                return out;
            }

            private List<DataRow> projectJoin(List<DataRow> joined, boolean select) {
                if (!select) {
                    return joined;
                }
                return SL.mapRows(joined, r -> {
                    DataRow o = new DataRow(3);
                    o.set(0, r.get(0));
                    o.set(1, r.get(1));
                    o.set(2, r.get(3));
                    return o;
                });
            }
        };
    }

    // ========================================================================
    // B34 inner join on duplicate keys, the cartesian product of equal keys, 10 by 10
    // ========================================================================

    private static BatchDesign b34() {
        return new BatchDesign() {
            @Override public int batchNo() { return 34; }
            @Override public String title() { return "inner join cartesian product on equal keys (duplicate keys) × 10 left data × 10 right data"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final List<DataRow> left = kvProfile(v);
                    for (int d = 0; d < DIM_NAMES.length; d++) {
                        final List<DataRow> right = dimProfile(d);
                        out.add(SL.sink2(id("B34", v, d), "innerJoinCartesian[L=" + KV_PROFILES[v]
                                        + ",R=" + DIM_NAMES[d] + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> l.join(r).on("k"),
                                (a, b) -> O.joinRows(a, b, O.idx(0), O.idx(0), JoinType.INNER, 2, 2),
                                false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B35 left join × 10 × 10
    // ========================================================================

    private static BatchDesign b35() {
        return new BatchDesign() {
            @Override public int batchNo() { return 35; }
            @Override public String title() { return "left join (unmatched rows get a null right half, null keys kept) × 10 left × 10 right"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final List<DataRow> left = kvProfile(v);
                    for (int d = 0; d < DIM_NAMES.length; d++) {
                        final List<DataRow> right = dimProfile(d);
                        out.add(SL.sink2(id("B35", v, d), "leftJoin[L=" + KV_PROFILES[v]
                                        + ",R=" + DIM_NAMES[d] + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> l.leftJoin(r).on("k"),
                                (a, b) -> O.joinRows(a, b, O.idx(0), O.idx(0), JoinType.LEFT, 2, 2),
                                false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B36 right join × 10 × 10
    // ========================================================================

    private static BatchDesign b36() {
        return new BatchDesign() {
            @Override public int batchNo() { return 36; }
            @Override public String title() { return "right join × 10 left × 10 right"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final List<DataRow> left = kvProfile(v);
                    for (int d = 0; d < DIM_NAMES.length; d++) {
                        final List<DataRow> right = dimProfile(d);
                        out.add(SL.sink2(id("B36", v, d), "rightJoin[L=" + KV_PROFILES[v]
                                        + ",R=" + DIM_NAMES[d] + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> l.rightJoin(r).on("k"),
                                (a, b) -> O.joinRows(a, b, O.idx(0), O.idx(0), JoinType.RIGHT, 2, 2),
                                false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B37 full join × 10 × 10
    // ========================================================================

    private static BatchDesign b37() {
        return new BatchDesign() {
            @Override public int batchNo() { return 37; }
            @Override public String title() { return "full outer join × 10 left × 10 right"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final List<DataRow> left = kvProfile(v);
                    for (int d = 0; d < DIM_NAMES.length; d++) {
                        final List<DataRow> right = dimProfile(d);
                        out.add(SL.sink2(id("B37", v, d), "fullJoin[L=" + KV_PROFILES[v]
                                        + ",R=" + DIM_NAMES[d] + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> l.fullJoin(r).on("k"),
                                (a, b) -> O.joinRows(a, b, O.idx(0), O.idx(0), JoinType.FULL, 2, 2),
                                false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B38 join on composite keys (ORD by ORD2), 10 key combinations over 10 profiles
    // ========================================================================

    private static BatchDesign b38() {
        return new BatchDesign() {
            @Override public int batchNo() { return 38; }
            @Override public String title() { return "composite key join (left ORD(id,grp,amt,qty) × right GRP(grp,qty,label)) × 10 key combinations × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                String[][] keyPairs = {
                        {"grp", "grp"}, {"id", "id"}, {"grp", "grp"}, {"id", "id"},
                        {"grp", "grp"}, {"id", "id"}, {"grp", "grp"}, {"id", "id"},
                        {"grp", "grp"}, {"id", "id"}};
                // right table schema: grp/id plus label; when a composite case uses qty as a second key, the right table carries qty too
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < keyPairs.length; v++) {
                    final int vf = v;
                    for (int d = 0; d < 10; d++) {
                        final List<DataRow> left = ordProfile(d);
                        final List<DataRow> right = rightProfile(d);
                        out.add(ScenarioDef.of(id("B38", v, d), "compositeJoin[ord" + d + "] keys#"
                                        + v,
                                lab -> {
                                    DataSet l = lab.source("L", D.ORD, left);
                                    DataSet r = lab.source("R", RIGHT_META, right);
                                    DataSet res;
                                    List<DataRow> expected;
                                    switch (vf) {
                                        case 0:
                                            res = l.join(r).on("grp");
                                            expected = O.joinRows(left, right, O.idx(1), O.idx(0),
                                                    JoinType.INNER, 4, 3);
                                            break;
                                        case 1:
                                            res = l.join(r).on("id");
                                            expected = O.joinRows(left, right, O.idx(0), O.idx(1),
                                                    JoinType.INNER, 4, 3);
                                            break;
                                        case 2:
                                            res = l.leftJoin(r).on("grp");
                                            expected = O.joinRows(left, right, O.idx(1), O.idx(0),
                                                    JoinType.LEFT, 4, 3);
                                            break;
                                        case 3:
                                            res = l.leftJoin(r).on("id");
                                            expected = O.joinRows(left, right, O.idx(0), O.idx(1),
                                                    JoinType.LEFT, 4, 3);
                                            break;
                                        case 4:
                                            res = l.join(r).on(new String[]{"grp"}, new String[]{"grp"})
                                                    .select("amt", "label");
                                            expected = SL.mapRows(O.joinRows(left, right, O.idx(1),
                                                    O.idx(0), JoinType.INNER, 4, 3), x -> {
                                                DataRow o = new DataRow(2);
                                                o.set(0, x.get(2));
                                                o.set(1, x.get(6));
                                                return o;
                                            });
                                            break;
                                        case 5:
                                            res = l.join(r).on(new String[]{"id", "grp"},
                                                    new String[]{"id", "grp"});
                                            expected = O.joinRows(left, right, O.idx(0, 1), O.idx(1, 0),
                                                    JoinType.INNER, 4, 3);
                                            break;
                                        case 6:
                                            res = l.fullJoin(r).on("grp");
                                            expected = O.joinRows(left, right, O.idx(1), O.idx(0),
                                                    JoinType.FULL, 4, 3);
                                            break;
                                        case 7:
                                            res = l.rightJoin(r).on("id");
                                            expected = O.joinRows(left, right, O.idx(0), O.idx(1),
                                                    JoinType.RIGHT, 4, 3);
                                            break;
                                        case 8:
                                            res = l.join(r).on(new String[]{"grp", "id"},
                                                    new String[]{"grp", "id"});
                                            expected = O.joinRows(left, right, O.idx(1, 0), O.idx(0, 1),
                                                    JoinType.INNER, 4, 3);
                                            break;
                                        default:
                                            res = l.leftJoin(r).on(new String[]{"id", "grp"},
                                                    new String[]{"id", "grp"});
                                            expected = O.joinRows(left, right, O.idx(0, 1), O.idx(1, 0),
                                                    JoinType.LEFT, 4, 3);
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

    private static final io.rowweave.DataSchema RIGHT_META = D.meta(
            D.f("grp", io.rowweave.DataType.StringType),
            D.f("id", io.rowweave.DataType.LongType),
            D.f("label", io.rowweave.DataType.StringType));

    private static List<DataRow> rightProfile(int i) {
        switch (i) {
            case 0: return D.rows(
                    new Object[]{"dev", 2L, "dev-two"}, new Object[]{"dev", 1L, "dev-team-one"},
                    new Object[]{"ops", 3L, "ops"}, new Object[]{null, 6L, "null-group"},
                    new Object[]{"rnd", 5L, "rnd-team"}, new Object[]{"hr", 9L, "orphan"});
            case 1: return D.empty();
            case 2: return D.rows(new Object[]{"dev", 1L, "only"});
            case 3: {
                List<DataRow> out = new ArrayList<>();
                for (int j = 0; j < 6; j++) {
                    out.add(D.row("dev", (long) (j % 3), "tag" + j));
                }
                return out;
            }
            case 4: return D.rows(
                    new Object[]{null, null, "all-null"}, new Object[]{"dev", null, "dev-no-id"});
            case 5: return D.rows(
                    new Object[]{"zz", 99L, "no-overlap"}, new Object[]{"yy", 98L, "also-no-overlap"});
            case 6: return D.rows(
                    new Object[]{"", null, "blank-grp"}, new Object[]{"dev", 1L, "dev-one"});
            default: {
                List<DataRow> out = new ArrayList<>();
                for (int j = 0; j < 10; j++) {
                    out.add(D.row("g" + (j % 3), (long) j, "lb" + j));
                }
                return out;
            }
        }
    }

    // ========================================================================
    // B39 join with the HASH hint, 10 join types over 10 profiles
    // ========================================================================

    private static BatchDesign b39() {
        return new BatchDesign() {
            @Override public int batchNo() { return 39; }
            @Override public String title() { return "join HASH hint (4 types rotated) × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                JoinType[] types = {JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL,
                        JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL,
                        JoinType.INNER, JoinType.INNER};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < types.length; v++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final JoinType type = types[v];
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        final boolean sel = v >= 8;
                        out.add(SL.sink2(id("B39", v, d), "hashJoin[" + type + "]["
                                        + KV_PROFILES[d] + "]" + (sel ? "+sel" : ""),
                                D.KV, left, D.DIM, right,
                                (l, r) -> {
                                    DataSet j = l.join(r, io.rowweave.ops.join.SortMergeJoin.of(
                                            l.getSchema(), r.getSchema(),
                                            new String[]{"k"}, new String[]{"k"}, type),
                                            io.rowweave.ops.join.JoinHint.HASH);
                                    return sel ? j.select("k", "v", "label") : j;
                                },
                                (a, b) -> {
                                    List<DataRow> joined = O.joinRows(a, b, O.idx(0), O.idx(0),
                                            type, 2, 2);
                                    if (!sel) {
                                        return joined;
                                    }
                                    return SL.mapRows(joined, x -> {
                                        DataRow o = new DataRow(3);
                                        o.set(0, x.get(0));
                                        o.set(1, x.get(1));
                                        o.set(2, x.get(3));
                                        return o;
                                    });
                                },
                                false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B40 join with the SORT_MERGE hint, 10 join types over 10 profiles
    // ========================================================================

    private static BatchDesign b40() {
        return new BatchDesign() {
            @Override public int batchNo() { return 40; }
            @Override public String title() { return "join SORT_MERGE hint (4 types rotated) × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                JoinType[] types = {JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL,
                        JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL,
                        JoinType.INNER, JoinType.LEFT};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < types.length; v++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final JoinType type = types[v];
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(SL.sink2(id("B40", v, d), "smjJoin[" + type + "][" + KV_PROFILES[d] + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> l.join(r, io.rowweave.ops.join.SortMergeJoin.of(
                                        l.getSchema(), r.getSchema(),
                                        new String[]{"k"}, new String[]{"k"}, type),
                                        io.rowweave.ops.join.JoinHint.SORT_MERGE),
                                (a, b) -> O.joinRows(a, b, O.idx(0), O.idx(0), type, 2, 2),
                                false));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B41 self join, 10 datasets over 10 combinations
    // ========================================================================

    private static BatchDesign b41() {
        return new BatchDesign() {
            @Override public int batchNo() { return 41; }
            @Override public String title() { return "self join (two instances of one source; same name, different instances) × 10 data × 10 join combinations"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B41", v, d), "selfJoin[" + KV_PROFILES[d]
                                        + "] combo#" + vf,
                                lab -> {
                                    DataSet src = lab.source("S", D.KV, data);
                                    DataSet other = lab.source("O", D.KV, data);
                                    DataSet res;
                                    List<DataRow> expected;
                                    switch (vf) {
                                        case 0:
                                            res = src.join(other).on("k");
                                            expected = O.joinRows(data, data, O.idx(0), O.idx(0),
                                                    JoinType.INNER, 2, 2);
                                            break;
                                        case 1:
                                            res = src.leftJoin(other).on("k");
                                            expected = O.joinRows(data, data, O.idx(0), O.idx(0),
                                                    JoinType.LEFT, 2, 2);
                                            break;
                                        case 2:
                                            res = src.join(other.select("k", "v as v1")).on("k")
                                                    .select(0, 1, 3);
                                            expected = SL.mapRows(O.joinRows(data, data,
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2), x -> {
                                                DataRow o = new DataRow(3);
                                                o.set(0, x.get(0));
                                                o.set(1, x.get(1));
                                                o.set(2, x.get(3));
                                                return o;
                                            });
                                            break;
                                        case 3:
                                            res = src.join(other.select("k", "v as v1")).on("k")
                                                    .filter("v > v1");
                                            expected = SL.filterRows(O.joinRows(data, data,
                                                            O.idx(0), O.idx(0), JoinType.INNER, 2, 2),
                                                    x -> gt(x, 1, 3));
                                            break;
                                        case 4:
                                            res = src.fullJoin(other).on("k");
                                            expected = O.joinRows(data, data, O.idx(0), O.idx(0),
                                                    JoinType.FULL, 2, 2);
                                            break;
                                        case 5:
                                            res = src.join(other).on("v");
                                            expected = O.joinRows(data, data, O.idx(1), O.idx(1),
                                                    JoinType.INNER, 2, 2);
                                            break;
                                        case 6:
                                            res = src.join(other).on("k")
                                                    .groupBy("k").count();
                                            expected = O.groupCount(O.joinRows(data, data,
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2), O.idx(0));
                                            break;
                                        case 7:
                                            res = src.rightJoin(other).on("k");
                                            expected = O.joinRows(data, data, O.idx(0), O.idx(0),
                                                    JoinType.RIGHT, 2, 2);
                                            break;
                                        case 8:
                                            res = src.join(other).on("k").distinct();
                                            expected = O.distinctAll(O.joinRows(data, data,
                                                    O.idx(0), O.idx(0), JoinType.INNER, 2, 2));
                                            break;
                                        default:
                                            res = src.join(other).on("k").select("k")
                                                    .distinct().orderBy("k");
                                            expected = O.sortRows(
                                                    SL.mapRows(O.distinctBy(O.joinRows(data, data,
                                                            O.idx(0), O.idx(0), JoinType.INNER, 2, 2),
                                                            O.idx(0)), r -> {
                                                        DataRow o = new DataRow(1);
                                                        o.set(0, r.get(0));
                                                        return o;
                                                    }), D.KV, O.sf(D.KV, "k", true, "DEFAULT"));
                                            break;
                                    }
                                    lab.expect(res, expected);
                                }));
                    }
                }
                return out;
            }

            private boolean gt(DataRow x, int li, int ri) {
                BigDecimal a = x.getBigDecimal(li);
                BigDecimal b = x.getBigDecimal(ri);
                return a != null && b != null && a.compareTo(b) > 0;
            }
        };
    }

    // ========================================================================
    // B42 join with an empty side or a one-sided match, 10 by 10
    // ========================================================================

    private static BatchDesign b42() {
        return new BatchDesign() {
            @Override public int batchNo() { return 42; }
            @Override public String title() { return "join with an empty side / a single-row side (4 types rotated) × 10 left × 10 right"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                JoinType[] types = {JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL,
                        JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL,
                        JoinType.LEFT, JoinType.FULL};
                for (int v = 0; v < types.length; v++) {
                    for (int d = 0; d < DIM_NAMES.length; d++) {
                        final JoinType type = types[v];
                        final List<DataRow> left = v % 2 == 0 ? D.empty() : D.single();
                        final List<DataRow> right = dimProfile(d);
                        out.add(SL.sink2(id("B42", v, d), "emptySideJoin[" + type + "][L="
                                        + (v % 2 == 0 ? "empty" : "single") + ",R=" + DIM_NAMES[d] + "]",
                                D.KV, left, D.DIM, right,
                                (l, r) -> joinOf(l, r, type),
                                (a, b) -> O.joinRows(a, b, O.idx(0), O.idx(0), type, 2, 2),
                                false));
                    }
                }
                return out;
            }

            private DataSet joinOf(DataSet l, DataSet r, JoinType type) {
                switch (type) {
                    case LEFT: return l.leftJoin(r).on("k");
                    case RIGHT: return l.rightJoin(r).on("k");
                    case FULL: return l.fullJoin(r).on("k");
                    default: return l.join(r).on("k");
                }
            }
        };
    }
}
