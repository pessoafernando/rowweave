package io.rowweave.examples.scenariolab.batches;

import io.rowweave.DataSet;
import io.rowweave.DataType;
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
import java.util.Random;

import static io.rowweave.examples.scenariolab.batches.FamilyA.id;
import static io.rowweave.examples.scenariolab.batches.FamilyA.kvProfile;
import static io.rowweave.examples.scenariolab.batches.FamilyA.KV_PROFILES;
import static io.rowweave.examples.scenariolab.batches.FamilyA.liftB;
import static io.rowweave.examples.scenariolab.batches.FamilyD.dimProfile;

/**
 * Family J (B93-B100): uid operator identity, limit bounds, multi-level dimension rollup,
 * join disambiguation chains, seeded random DAGs, dimension-table enrichment,
 * shared-subgraph multi-sink consistency, and case-when/cast expression mixes.
 */
public final class FamilyJ {

    private FamilyJ() {
    }

    public static List<BatchDesign> batches() {
        return Arrays.asList(
                b93(), b94(), b95(), b96(), b97(),
                b98(), b99(), b100());
    }

    // ========================================================================
    // B93 uid() for operators and sinks, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b93() {
        return new BatchDesign() {
            @Override public int batchNo() { return 93; }
            @Override public String title() { return "uid() logical operator identity + named sink (uid does not change semantics) × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        final int dd = d;
                        out.add(ScenarioDef.of(id("B93", vf, d), "uid[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet src = lab.source(D.KV, data).uid("src-" + vf + "-" + dd);
                                    DataSet res;
                                    List<DataRow> expected;
                                    switch (vf) {
                                        case 0:
                                            res = src.filter("v > 5").uid("f1");
                                            expected = SL.filterRows(data, r -> FamilyA.vCmp(r, 0,
                                                    BigDecimal.valueOf(5)));
                                            break;
                                        case 1:
                                            res = src.groupBy("k").sum("v").uid("agg1");
                                            expected = O.groupSum(data, O.idx(0), 1);
                                            break;
                                        case 2:
                                            res = src.orderBy("k").uid("o1");
                                            expected = O.sortRows(data, D.KV, O.sf(D.KV, "k", true, "DEFAULT"));
                                            lab.expectOrdered(res, expected);
                                            lab.expectOrdered(res.uid("o1-again"), expected);
                                            return;
                                        case 3:
                                            res = src.distinct().uid("d1").filter("v >= 0").uid("f2");
                                            expected = SL.filterRows(O.distinctAll(data),
                                                    r -> r.get(1) != null
                                                            && r.getBigDecimal(1).signum() >= 0);
                                            break;
                                        case 4: {
                                            DataSet other = lab.source("O", D.KV, data).uid("src2");
                                            res = src.union(other).uid("u1");
                                            expected = SL.concat(data, data);
                                            break;
                                        }
                                        case 5:
                                            res = src.select(1).uid("s1");
                                            expected = SL.mapRows(data, r -> {
                                                DataRow o = new DataRow(1);
                                                o.set(0, r.get(1));
                                                return o;
                                            });
                                            break;
                                        case 6:
                                            res = src.top(2).uid("t1");
                                            expected = data.subList(0, Math.min(2, data.size()));
                                            lab.expectOrdered(res, expected);
                                            return;
                                        case 7: {
                                            DataSet other = lab.source("O", D.KV, data).uid("src3");
                                            res = src.join(other).on("k").uid("j1");
                                            expected = O.joinRows(data, data, O.idx(0), O.idx(0),
                                                    io.rowweave.ops.join.JoinType.INNER, 2, 2);
                                            break;
                                        }
                                        case 8:
                                            res = src.map(r -> r.copy()).uid("m1").uid("m2");
                                            expected = data;
                                            break;
                                        default:
                                            res = src.flatMap((r, c) -> c.collect(r.copy())).uid("fm1");
                                            expected = data;
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
    // B94 limit boundaries with n of 0, 1 or very large, plus ordering over all-null data, 10 by 10
    // ========================================================================

    private static BatchDesign b94() {
        return new BatchDesign() {
            @Override public int batchNo() { return 94; }
            @Override public String title() { return "limit boundaries (0/1/n=size/n>size) plus null sort boundaries × 10 × 10"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                List<List<DataRow>> datasets = Arrays.asList(
                        D.empty(), D.single(), D.kvDupes(), D.kvNulls(), D.kvManyGroups(),
                        D.rows(new Object[]{null, null}),
                        D.kvBase(), D.kvNegative(), D.kvScale(), D.kvWhitespace());
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < datasets.size(); d++) {
                        final List<DataRow> data = datasets.get(d);
                        out.add(ScenarioDef.of(id("B94", v, d), "limit[v" + vf + "][d" + d + "]",
                                lab -> {
                                    DataSet src = lab.source(D.KV, data);
                                    switch (vf) {
                                        case 0:
                                            lab.expectOrdered(src.top(0), SL.filterRows(data, r -> false));
                                            break;
                                        case 1:
                                            lab.expectOrdered(src.top(1),
                                                    data.subList(0, Math.min(1, data.size())));
                                            break;
                                        case 2:
                                            lab.expectOrdered(src.top(data.size()),
                                                    new ArrayList<>(data));
                                            break;
                                        case 3:
                                            lab.expectOrdered(src.top(10000),
                                                    new ArrayList<>(data));
                                            break;
                                        case 4:
                                            lab.expectOrdered(src.orderBy(O.orderBy(D.KV,
                                                    O.sf(D.KV, "k", true, "NULLS_FIRST"),
                                                    O.sf(D.KV, "v", true, "NULLS_FIRST")), 1, false),
                                                    O.sortRows(data, D.KV,
                                                            O.sf(D.KV, "k", true, "NULLS_FIRST"),
                                                            O.sf(D.KV, "v", true, "NULLS_FIRST")));
                                            break;
                                        case 5:
                                            lab.expectOrdered(src.orderBy(O.orderBy(D.KV,
                                                    O.sf(D.KV, "k", true, "NULLS_LAST"),
                                                    O.sf(D.KV, "v", true, "NULLS_LAST")), 1, false),
                                                    O.sortRows(data, D.KV,
                                                            O.sf(D.KV, "k", true, "NULLS_LAST"),
                                                            O.sf(D.KV, "v", true, "NULLS_LAST")));
                                            break;
                                        case 6:
                                            lab.expectOrdered(src.orderBy(O.orderBy(D.KV,
                                                    O.sf(D.KV, "v", false, "NULLS_FIRST")), 1, false).top(1),
                                                    O.sortRows(data, D.KV,
                                                            O.sf(D.KV, "v", false, "NULLS_FIRST"))
                                                            .subList(0, Math.min(1, data.size())));
                                            break;
                                        case 7:
                                            lab.expect(src.filter("v is null"),
                                                    SL.filterRows(data, r -> r.get(1) == null));
                                            break;
                                        case 8:
                                            lab.expect(src.filter("k is null and v is null"),
                                                    SL.filterRows(data, r -> r.get(0) == null
                                                            && r.get(1) == null));
                                            break;
                                        default:
                                            lab.expect(src.distinct().top(3),
                                                    O.distinctAll(data).subList(0,
                                                            Math.min(3, O.distinctAll(data).size())));
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
    // B95 multi-level rollup (entity -> category -> grand total, several sinks) x 10 templates x 10 data
    // ========================================================================

    private static BatchDesign b95() {
        final io.rowweave.DataSchema DETAIL = D.meta(
                D.f("entity", DataType.StringType),
                D.f("category", DataType.StringType),
                D.f("amount", DataType.BigDecimalType));
        return new BatchDesign() {
            @Override public int batchNo() { return 95; }
            @Override public String title() { return "multi-level rollup (entity/category/grand total, matching per-sink sums) x 10 templates x 10 data"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                List<List<DataRow>> datasets = Arrays.asList(
                        detailBase(),
                        D.empty(),
                        java.util.Arrays.asList(d("C1", "6001", "100.005")),
                        detailDupes(),
                        detailNeg(),
                        detailNullCategory(),
                        detailScale(),
                        detailMany(),
                        detailUnicode(),
                        detailBig());
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < datasets.size(); d++) {
                        final List<DataRow> data = datasets.get(d);
                        out.add(ScenarioDef.of(id("B95", v, d), "rollup[v" + vf + "][d" + d + "]",
                                lab -> {
                                    DataSet src = lab.source(DETAIL, data);
                                    List<DataRow> byEntity = O.groupSum(data, O.idx(0), 2);
                                    List<DataRow> byCategory = O.groupSum(data, O.idx(1), 2);
                                    List<DataRow> byBoth = O.groupSum(data, O.idx(0, 1), 2);
                                    BigDecimal grand = grandTotal(data);
                                    switch (vf) {
                                        case 0:
                                            lab.expect(src.groupBy("entity").sum("amount"), byEntity);
                                            lab.expect(src.groupBy("category").sum("amount"), byCategory);
                                            break;
                                        case 1:
                                            lab.expect(src.groupBy("entity", "category").sum("amount"), byBoth);
                                            lab.expect(src.sum("amount"),
                                                    D.rows(new Object[]{grand}));
                                            break;
                                        case 2:
                                            // per-entity rollup verified against the grand-total sink
                                            lab.expect(src.groupBy("entity").sum("amount"), byEntity);
                                            lab.expect(src.filter("amount > -1000000000").sum("amount"),
                                                    D.rows(new Object[]{grand}));
                                            break;
                                        case 3:
                                            lab.expectOrdered(
                                                    src.groupBy("entity", "category").sum("amount")
                                                            .orderBy(O.orderBy(D.meta(
                                                                    D.f("entity", DataType.StringType),
                                                                    D.f("category", DataType.StringType),
                                                                    D.f("sum0", DataType.BigDecimalType)),
                                                                    O.sf(D.meta(
                                                                            D.f("entity", DataType.StringType),
                                                                            D.f("category", DataType.StringType),
                                                                            D.f("sum0", DataType.BigDecimalType)),
                                                                            "entity", true, "DEFAULT"),
                                                                    O.sf(D.meta(
                                                                            D.f("entity", DataType.StringType),
                                                                            D.f("category", DataType.StringType),
                                                                            D.f("sum0", DataType.BigDecimalType)),
                                                                            "category", true, "DEFAULT")), 1, false),
                                                    O.sortRows(byBoth, D.meta(
                                                            D.f("entity", DataType.StringType),
                                                            D.f("category", DataType.StringType),
                                                            D.f("sum0", DataType.BigDecimalType)),
                                                            O.sf(D.meta(
                                                                    D.f("entity", DataType.StringType),
                                                                    D.f("category", DataType.StringType),
                                                                    D.f("sum0", DataType.BigDecimalType)),
                                                                    "entity", true, "DEFAULT"),
                                                            O.sf(D.meta(
                                                                    D.f("entity", DataType.StringType),
                                                                    D.f("category", DataType.StringType),
                                                                    D.f("sum0", DataType.BigDecimalType)),
                                                                    "category", true, "DEFAULT")));
                                            break;
                                        case 4:
                                            lab.expect(src.filter("amount > 0").groupBy("entity")
                                                    .sum("amount"),
                                                    O.groupSum(SL.filterRows(data,
                                                            r -> r.getBigDecimal(2) != null
                                                                    && r.getBigDecimal(2).signum() > 0),
                                                            O.idx(0), 2));
                                            lab.expect(src.filter("amount < 0").groupBy("entity")
                                                    .sum("amount"),
                                                    O.groupSum(SL.filterRows(data,
                                                            r -> r.getBigDecimal(2) != null
                                                                    && r.getBigDecimal(2).signum() < 0),
                                                            O.idx(0), 2));
                                            break;
                                        case 5:
                                            lab.expect(src.groupBy("entity").count(),
                                                    O.groupCount(data, O.idx(0)));
                                            lab.expect(src.groupBy("entity").count("amount"),
                                                    O.groupCountField(data, O.idx(0), 2));
                                            break;
                                        case 6: {
                                            DataSet res = src.groupBy("entity").sum("amount", "total");
                                            lab.expect(res, byEntity);
                                            lab.expect(res.filter("total >= -1000000000"), byEntity);
                                            break;
                                        }
                                        case 7:
                                            lab.expect(src.select("entity", "category"),
                                                    SL.mapRows(data, r -> two(r.get(0), r.get(1))));
                                            lab.expect(src.select("amount"),
                                                    SL.mapRows(data, r -> one(r.get(2))));
                                            break;
                                        case 8:
                                            lab.expect(src.distinct(),
                                                    O.distinctAll(data));
                                            lab.expect(src.distinct("entity", "category")
                                                    .select("entity", "category"),
                                                    SL.mapRows(O.distinctBy(data, O.idx(0, 1)),
                                                            r -> O.project(r, O.idx(0, 1))));
                                            break;
                                        default:
                                            lab.expectOrdered(src.orderBy(O.orderBy(DETAIL,
                                                    O.sf(DETAIL, "entity", true, "DEFAULT"),
                                                    O.sf(DETAIL, "category", true, "DEFAULT"),
                                                    O.sf(DETAIL, "amount", true, "DEFAULT")), 1, false),
                                                    O.sortRows(data, DETAIL,
                                                            O.sf(DETAIL, "entity", true, "DEFAULT"),
                                                            O.sf(DETAIL, "category", true, "DEFAULT"),
                                                            O.sf(DETAIL, "amount", true, "DEFAULT")));
                                            break;
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    private static final int DETAIL_W = 3;

    private static DataRow d(String c, String a, String amt) {
        DataRow r = new DataRow(DETAIL_W);
        r.set(0, c);
        r.set(1, a);
        r.set(2, amt == null ? null : new BigDecimal(amt));
        return r;
    }

    private static List<DataRow> detailBase() {
        return Arrays.asList(
                d("C1", "6001", "100.00"), d("C1", "6001", "200.00"),
                d("C1", "6002", "50.50"), d("C2", "6001", "300.30"));
    }

    private static List<DataRow> detailDupes() {
        List<DataRow> out = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            out.add(d("C", "A", "10.01"));
            out.add(d("C", "B", "20.02"));
        }
        return out;
    }

    private static List<DataRow> detailNeg() {
        return Arrays.asList(
                d("N", "A", "-100.50"), d("N", "A", "50.25"),
                d("P", "B", "0.01"), d("P", "B", "-0.01"));
    }

    private static List<DataRow> detailNullCategory() {
        return Arrays.asList(
                d("C1", null, "10"), d("C1", "6001", "20"), d(null, "6002", "30"),
                d(null, null, "40"));
    }

    private static List<DataRow> detailScale() {
        return Arrays.asList(
                d("S", "A", "0.0000000005"), d("S", "A", "0.0000000015"),
                d("S", "B", "1.0000000005"));
    }

    private static List<DataRow> detailMany() {
        List<DataRow> out = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            out.add(d("C" + (i % 3), "A" + (i % 4), String.valueOf(i) + ".25"));
        }
        return out;
    }

    private static List<DataRow> detailUnicode() {
        return Arrays.asList(
                d("Zürich", "α", "100"), d("Oslo", "β", "50"),
                d("Zürich", "β", "25.5"));
    }

    private static List<DataRow> detailBig() {
        return Arrays.asList(
                d("B", "A", "999999999999.9999999999"),
                d("B", "A", "0.0000000001"));
    }

    private static BigDecimal grandTotal(List<DataRow> data) {
        BigDecimal t = null;
        for (DataRow r : data) {
            if (r.get(2) != null) {
                t = t == null ? BigDecimal.ZERO : t;
                t = t.add(r.getBigDecimal(2));
            }
        }
        return t == null ? null : t.setScale(10, RoundingMode.HALF_UP);
    }

    // ========================================================================
    // B96 a long chain with select and rename disambiguating after a join, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b96() {
        return new BatchDesign() {
            @Override public int batchNo() { return 96; }
            @Override public String title() { return "join→select/rename→further operators disambiguation chain × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        out.add(ScenarioDef.of(id("B96", v, d), "disamb[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet l = lab.source("L", D.KV, left);
                                    DataSet r = lab.source("R", D.DIM, right).select("k", "label as rlabel");
                                    List<DataRow> rr = SL.mapRows(right, x -> {
                                        DataRow o = new DataRow(2);
                                        o.set(0, x.get(0));
                                        o.set(1, x.get(1));
                                        return o;
                                    });
                                    List<DataRow> joined = O.joinRows(left, rr, O.idx(0), O.idx(0),
                                            io.rowweave.ops.join.JoinType.INNER, 2, 2);
                                    DataSet j = l.join(r).on("k");
                                    switch (vf) {
                                        case 0:
                                            lab.expect(j.select("k", "v", "rlabel"),
                                                    SL.mapRows(joined, x -> three(x.get(0), x.get(1), x.get(3))));
                                            break;
                                        case 1:
                                            lab.expect(j.select("rlabel"),
                                                    SL.mapRows(joined, x -> one(x.get(3))));
                                            break;
                                        case 2:
                                            lab.expectTyped(j.select("v * 1 as v2", "k as kk"),
                                                    D.meta(D.f("c0", DataType.BigDecimalType),
                                                            D.f("c1", DataType.StringType)),
                                                    SL.mapRows(joined, x -> two(
                                                            x.getBigDecimal(1) == null ? null
                                                                    : x.getBigDecimal(1).multiply(BigDecimal.ONE),
                                                            x.get(0))));
                                            break;
                                        case 3:
                                            lab.expect(j.select("k as a", "v as b", "rlabel as c")
                                                    .select("a", "c"),
                                                    SL.mapRows(joined, x -> two(x.get(0), x.get(3))));
                                            break;
                                        case 4:
                                            lab.expect(j.select("k", "v", "rlabel")
                                                    .filter("rlabel is not null"),
                                                    SL.filterRows(SL.mapRows(joined,
                                                            x -> three(x.get(0), x.get(1), x.get(3))),
                                                            y -> y.get(2) != null));
                                            break;
                                        case 5:
                                            lab.expect(j.select("k", "v", "rlabel")
                                                    .groupBy("rlabel").count(),
                                                    O.groupCount(SL.mapRows(joined,
                                                            x -> three(x.get(0), x.get(1), x.get(3))),
                                                            O.idx(2)));
                                            break;
                                        case 6:
                                            lab.expectOrdered(j.select("k", "v", "rlabel")
                                                    .orderBy("rlabel"),
                                                    O.sortRows(SL.mapRows(joined,
                                                            x -> three(x.get(0), x.get(1), x.get(3))),
                                                            j.select("k", "v", "rlabel").getSchema(),
                                                            O.sf(j.select("k", "v", "rlabel").getSchema(),
                                                                    "rlabel", true, "DEFAULT")));
                                            break;
                                        case 7:
                                            lab.expect(j.select("k", "v", "rlabel").distinct(),
                                                    O.distinctAll(SL.mapRows(joined,
                                                            x -> three(x.get(0), x.get(1), x.get(3)))));
                                            break;
                                        case 8:
                                            lab.expectTyped(j.select("concat(k, ':', rlabel) as kl"),
                                                    D.meta(D.f("c0", DataType.StringType)),
                                                    SL.mapRows(joined, x -> {
                                                        DataRow o = new DataRow(1);
                                                        String k = (String) x.get(0);
                                                        o.set(0, k == null ? null
                                                                : k + ":" + x.get(3));
                                                        return o;
                                                    }));
                                            break;
                                        default:
                                            lab.expect(j.select("k", "v", "rlabel")
                                                    .groupBy("k").sum("v"),
                                                    O.groupSum(SL.mapRows(joined,
                                                            x -> three(x.get(0), x.get(1), x.get(3))),
                                                            O.idx(0), 1));
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
    // B97 seeded random DAGs whose oracle traces the same seed, over 100 seeds
    // ========================================================================

    private static BatchDesign b97() {
        return new BatchDesign() {
            @Override public int batchNo() { return 97; }
            @Override public String title() { return "seeded random DAG (filter/map/union/groupBy/orderBy combinations) × 100 seeds"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int seed = 0; seed < 100; seed++) {
                    final int s = seed;
                    out.add(ScenarioDef.of("B97-SEED" + String.format("%03d", seed),
                            "randomDag(seed=" + seed + ")",
                            lab -> {
                                Random rng = new Random(42 + s);
                                List<DataRow> data = D.kvManyGroups();
                                // randomly perturbed data, deterministic
                                int extra = rng.nextInt(6);
                                for (int i = 0; i < extra; i++) {
                                    data.add(D.row("r" + rng.nextInt(4),
                                            rng.nextBoolean() ? D.bd(String.valueOf(rng.nextInt(20)))
                                                    : null));
                                }
                                List<DataRow> expected = data;
                                DataSet cur = lab.source(D.KV, data);
                                String c0 = "k";
                                String c1 = "v";
                                int steps = 2 + rng.nextInt(5);
                                for (int i = 0; i < steps; i++) {
                                    int op = rng.nextInt(6);
                                    DataSet fc = cur;
                                    List<DataRow> fe = expected;
                                    switch (op) {
                                        case 0:
                                            cur = fc.filter(c1 + " > 3");
                                            expected = SL.filterRows(fe, r -> r.getBigDecimal(1) != null
                                                    && r.getBigDecimal(1).compareTo(BigDecimal.valueOf(3)) > 0);
                                            break;
                                        case 1:
                                            cur = fc.map(r -> {
                                                DataRow o = r.copy();
                                                BigDecimal v = o.getBigDecimal(1);
                                                o.set(1, v == null ? null : v.add(BigDecimal.TEN));
                                                return o;
                                            });
                                            expected = SL.mapRows(fe, r -> {
                                                DataRow o = r.copy();
                                                BigDecimal v = o.getBigDecimal(1);
                                                o.set(1, v == null ? null : v.add(BigDecimal.TEN));
                                                return o;
                                            });
                                            break;
                                        case 2:
                                            cur = fc.union(fc);
                                            expected = SL.concat(fe, fe);
                                            break;
                                        case 3:
                                            cur = fc.distinct();
                                            expected = O.distinctAll(fe);
                                            break;
                                        case 4:
                                            cur = fc.groupBy(c0).sum(c1);
                                            expected = O.groupSum(fe, O.idx(0), 1);
                                            c1 = "sum0";
                                            break;
                                        default:
                                            cur = fc.select(1, 0);
                                            expected = SL.mapRows(fe, r -> {
                                                DataRow o = new DataRow(2);
                                                o.set(0, r.get(1));
                                                o.set(1, r.get(0));
                                                return o;
                                            });
                                            String tmp = c0;
                                            c0 = c1;
                                            c1 = tmp;
                                            cur = cur.orderBy(O.orderBy(cur.getSchema(),
                                                    O.sf(cur.getSchema(), c0, true, "DEFAULT"),
                                                    O.sf(cur.getSchema(), c1, true, "DEFAULT")), 1, false);
                                            expected = O.sortRows(expected, cur.getSchema(),
                                                    O.sf(cur.getSchema(), c0, true, "DEFAULT"),
                                                    O.sf(cur.getSchema(), c1, true, "DEFAULT"));
                                            lab.expectOrdered(cur, expected);
                                            return;
                                    }
                                }
                                lab.expect(cur, expected);
                            }));
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B98 lookup enrichment with leftJoin and a coalesce expression, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b98() {
        return new BatchDesign() {
            @Override public int batchNo() { return 98; }
            @Override public String title() { return "lookup enrichment (leftJoin + expression-derived column) × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                io.rowweave.DataType[][] outTypes = {
                        {DataType.StringType, DataType.BigDecimalType, DataType.StringType},
                        {DataType.StringType, DataType.BigDecimalType, DataType.StringType},
                        {DataType.StringType, DataType.BigDecimalType, DataType.StringType},
                        {DataType.StringType, DataType.BigDecimalType, DataType.StringType},
                        {DataType.StringType, DataType.BigDecimalType, DataType.BigDecimalType},
                        {DataType.StringType, DataType.BigDecimalType, DataType.BooleanType},
                        {DataType.StringType, DataType.BigDecimalType, DataType.BooleanType},
                        {DataType.StringType, DataType.BigDecimalType, DataType.StringType},
                        {DataType.StringType, DataType.BigDecimalType, DataType.StringType},
                        {DataType.StringType, DataType.BigDecimalType, DataType.StringType}};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final int vf = v;
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = dimProfile(d);
                        final io.rowweave.DataSchema typed = D.meta(
                                D.f("c0", outTypes[v][0]), D.f("c1", outTypes[v][1]),
                                D.f("c2", outTypes[v][2]));
                        out.add(ScenarioDef.of(id("B98", v, d), "enrich[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet l = lab.source("L", D.KV, left);
                                    DataSet r = lab.source("R", D.DIM, right);
                                    DataSet j = l.leftJoin(r).on("k");
                                    DataSet res = dag(vf, j);
                                    lab.expectTyped(res, typed, oracle(vf,
                                            O.joinRows(left, right, O.idx(0), O.idx(0),
                                                    io.rowweave.ops.join.JoinType.LEFT, 2, 2)));
                                }));
                    }
                }
                return out;
            }

            private DataSet dag(int v, DataSet j) {
                switch (v) {
                    case 0: return j.select("k", "v", "label");
                    case 1: return j.select("k", "v", "concat(label, '') as lbl2");
                    case 2: return j.select("k", "v", "label as dim");
                    case 3: return j.select("k", "v", "upper(label) as up");
                    case 4: return j.select("k", "v", "len(label) as ll");
                    case 5: return j.select("k", "v", "label is null as unmatched");
                    case 6: return j.select("k", "v", "contains(label, 'group') as grp");
                    case 7: return j.select("k", "v", "substr(label, 1, 1) as first");
                    case 8: return j.select("k", "v + 0 as v2", "label");
                    default: return j.select("k", "v", "label").filter("label is not null");
                }
            }

            private List<DataRow> oracle(int v, List<DataRow> joined) {
                List<DataRow> rows = SL.mapRows(joined, x -> {
                    Object k = x.get(0);
                    BigDecimal val = x.getBigDecimal(1);
                    String label = (String) x.get(3);
                    boolean unmatched = label == null;
                    switch (v) {
                        case 0: return three(k, val, label);
                        case 1: return three(k, val, label == null ? null : label + "");
                        case 2: return three(k, val, label);
                        case 3: return three(k, val, label == null ? null : label.toUpperCase());
                        case 4: return three(k, val, label == null ? null
                                : BigDecimal.valueOf(label.length()));
                        case 5: return three(k, val, unmatched);
                        case 6: return three(k, val, label == null ? null : label.contains("group"));
                        case 7: return three(k, val, label == null ? null
                                : label.substring(0, Math.min(1, label.length())));
                        case 8: return three(k, val == null ? null : val.add(BigDecimal.ZERO), label);
                        default: return three(k, val, label);
                    }
                });
                if (v == 9) {
                    return SL.filterRows(rows, x -> x.get(2) != null);
                }
                return rows;
            }
        };
    }

    // ========================================================================
    // B99 consistency across sinks fed by a shared subgraph, 10 templates over 10 profiles
    // ========================================================================

    private static BatchDesign b99() {
        return new BatchDesign() {
            @Override public int batchNo() { return 99; }
            @Override public String title() { return "shared subgraph with several sinks (the same DataSet registered twice, identical results) × 10 templates × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B99", v, d), "shared[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet src = lab.source(D.KV, data);
                                    DataSet shared;
                                    List<DataRow> expected;
                                    switch (vf) {
                                        case 0:
                                            shared = src.groupBy("k").sum("v");
                                            expected = O.groupSum(data, O.idx(0), 1);
                                            break;
                                        case 1:
                                            shared = src.distinct();
                                            expected = O.distinctAll(data);
                                            break;
                                        case 2:
                                            shared = src.filter("v > 0");
                                            expected = SL.filterRows(data, r -> liftB(r.getBigDecimal(1),
                                                    x -> x.signum() > 0));
                                            break;
                                        case 3:
                                            shared = src.map(r -> {
                                                DataRow o = r.copy();
                                                o.set(0, FamilyA.kStr(r) == null ? null
                                                        : FamilyA.kStr(r).toUpperCase());
                                                return o;
                                            });
                                            expected = SL.mapRows(data, FamilyA::upperK);
                                            break;
                                        case 4:
                                            shared = src.top(3);
                                            expected = data.subList(0, Math.min(3, data.size()));
                                            break;
                                        case 5:
                                            shared = src.select(0);
                                            expected = SL.mapRows(data, r -> one(r.get(0)));
                                            break;
                                        case 6:
                                            shared = src.union(src);
                                            expected = SL.concat(data, data);
                                            break;
                                        case 7:
                                            shared = src.orderBy(O.orderBy(D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"),
                                                    O.sf(D.KV, "v", true, "DEFAULT")), 1, false);
                                            expected = O.sortRows(data, D.KV,
                                                    O.sf(D.KV, "k", true, "DEFAULT"),
                                                    O.sf(D.KV, "v", true, "DEFAULT"));
                                            lab.expectOrdered(shared, expected);
                                            lab.expectOrdered(shared, expected);
                                            return;
                                        case 8:
                                            shared = src.groupBy("k").count();
                                            expected = O.groupCount(data, O.idx(0));
                                            break;
                                        default:
                                            shared = src.partitionByHash("k");
                                            expected = data;
                                            break;
                                    }
                                    // the same shared node feeds two sinks, so the common upstream is computed once and both results agree
                                    lab.expect(shared, expected);
                                    lab.expect(shared, expected);
                                }));
                    }
                }
                return out;
            }
        };
    }

    // ========================================================================
    // B100 a mix of case-when, cast, in and like expressions, 10 expression groups over 10 profiles
    // ========================================================================

    private static BatchDesign b100() {
        return new BatchDesign() {
            @Override public int batchNo() { return 100; }
            @Override public String title() { return "mixed case-when/cast/in/like/not in expressions × 10 sets × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                String[][] filterExprs = {
                        {"case when v > 5 then 'big' else 'small' end = 'big'"},
                        {"case when k is null then 1 else 0 end = 1"},
                        {"k like 'g_'"},
                        {"k not like 'g%'"},
                        {"v in (10.5, 42)"},
                        {"v not in (0, 1)"},
                        {"cast(v as string) = '10.5'"},
                        {"case when v is null then 'n' when v > 0 then 'p' else 'z' end = 'p'"},
                        {"not (k like '%1%')"},
                        {"v > 5 and (k like 'g%' or v < 0)"}};
                for (int vI = 0; vI < filterExprs.length; vI++) {
                    final int vf = vI;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B100", vf, d), "exprMix[" + KV_PROFILES[d] + "] "
                                        + filterExprs[vf][0],
                                D.KV, data, ds -> ds.filter(filterExprs[vf][0]),
                                in -> SL.filterRows(in, r -> caseOracle(vf, r)), false));
                    }
                }
                return out;
            }

            private boolean caseOracle(int v, DataRow r) {
                String k = FamilyA.kStr(r);
                BigDecimal val = r.getBigDecimal(1);
                switch (v) {
                    case 0: return val != null && val.compareTo(BigDecimal.valueOf(5)) > 0;
                    case 1: return r.get(0) == null;
                    case 2: return k != null && k.length() == 2 && k.startsWith("g");
                    case 3: return k != null && !likeG(k);
                    case 4: return val != null && (val.compareTo(D.bd("10.5")) == 0
                            || val.compareTo(BigDecimal.valueOf(42)) == 0);
                    case 5: return val != null && val.compareTo(BigDecimal.ZERO) != 0
                            && val.compareTo(BigDecimal.ONE) != 0;
                    case 6: return val != null && val.toPlainString().equals("10.5");
                    case 7: return val != null && val.signum() > 0;
                    case 8: return k != null && !k.contains("1");
                    case 9:
                        if (val == null) {
                            return false;
                        }
                        if (!(val.compareTo(BigDecimal.valueOf(5)) > 0)) {
                            return false;
                        }
                        return (k != null && k.startsWith("g")) || val.signum() < 0;
                    default: return false;
                }
            }

            private boolean likeG(String s) {
                return s.matches("g.*");
            }
        };
    }

    private static DataRow one(Object a) {
        DataRow o = new DataRow(1);
        o.set(0, a);
        return o;
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
}
