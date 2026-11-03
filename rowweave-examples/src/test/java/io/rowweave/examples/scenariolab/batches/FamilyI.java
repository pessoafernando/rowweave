package io.rowweave.examples.scenariolab.batches;

import io.rowweave.DataSet;
import io.rowweave.DataRow;
import io.rowweave.examples.scenariolab.BatchDesign;
import io.rowweave.examples.scenariolab.D;
import io.rowweave.examples.scenariolab.O;
import io.rowweave.examples.scenariolab.SL;
import io.rowweave.examples.scenariolab.ScenarioDef;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.rowweave.examples.scenariolab.batches.FamilyA.id;

/**
 * Family I (B83 to B92): data boundaries. An empty table, a single row, all-null rows,
 * duplicate rows, unicode, whitespace, extreme numeric values, negatives and rounding, integer
 * keys, and a wide table with several keys. The same operator templates are run over every
 * boundary dataset, 10 templates over 10 datasets per batch, which gives 100 scenarios.
 */
public final class FamilyI {

    private FamilyI() {
    }

    public static List<BatchDesign> batches() {
        return Arrays.asList(
                b83(), b84(), b85(), b86(), b87(),
                b88(), b89(), b90(), b91(), b92());
    }

    /** The 10 boundary data profiles on the KV schema. */
    private static final String[] EDGE_NAMES = {
            "empty", "single", "allNullV", "allNullBoth", "dupRows", "oneGroup",
            "unicodeGroups", "bigSmall", "negRound", "denseDupes"};

    private static List<DataRow> edgeProfile(int i) {
        switch (i) {
            case 0: return D.empty();
            case 1: return D.rows(new Object[]{"only", D.bd("1.5")});
            case 2: return D.rows(
                    new Object[]{"a", null}, new Object[]{"b", null}, new Object[]{"a", null});
            case 3: return D.rows(
                    new Object[]{null, null}, new Object[]{null, null}, new Object[]{"x", null});
            case 4: return D.rows(
                    new Object[]{"dup", D.bd("7")}, new Object[]{"dup", D.bd("7")},
                    new Object[]{"dup", D.bd("7")}, new Object[]{"dup", D.bd("7")});
            case 5: return D.rows(
                    new Object[]{"g", D.bd("1")}, new Object[]{"g", D.bd("2")},
                    new Object[]{"g", D.bd("3")}, new Object[]{"g", D.bd("4")},
                    new Object[]{"g", D.bd("5")});
            case 6: return D.kvUnicode();
            case 7: return D.rows(
                    new Object[]{"big", D.bd("99999999999999999999.9999999999")},
                    new Object[]{"small", D.bd("0.0000000001")},
                    new Object[]{"zero", D.bd("0")});
            case 8: return D.kvNegative();
            default: {
                List<DataRow> out = new ArrayList<>();
                for (int j = 0; j < 30; j++) {
                    out.add(D.row("k" + (j % 3), D.bd(String.valueOf(j % 5))));
                }
                return out;
            }
        }
    }

    /** The boundary operator templates, all over the KV schema. */
    private interface OpTpl {
        DataSet dag(DataSet ds);

        List<DataRow> oracle(List<DataRow> in);

        boolean ordered();
    }

    private static List<OpTpl> opTemplates() {
        return Arrays.asList(
                new OpTpl() {
                    @Override public DataSet dag(DataSet ds) { return ds.filter("v > 2"); }
                    @Override public List<DataRow> oracle(List<DataRow> in) {
                        return SL.filterRows(in, r -> r.getBigDecimal(1) != null
                                && r.getBigDecimal(1).compareTo(BigDecimal.valueOf(2)) > 0);
                    }
                    @Override public boolean ordered() { return false; }
                },
                new OpTpl() {
                    @Override public DataSet dag(DataSet ds) { return ds.groupBy("k").sum("v"); }
                    @Override public List<DataRow> oracle(List<DataRow> in) {
                        return O.groupSum(in, O.idx(0), 1);
                    }
                    @Override public boolean ordered() { return false; }
                },
                new OpTpl() {
                    @Override public DataSet dag(DataSet ds) { return ds.distinct(); }
                    @Override public List<DataRow> oracle(List<DataRow> in) { return O.distinctAll(in); }
                    @Override public boolean ordered() { return false; }
                },
                new OpTpl() {
                    @Override public DataSet dag(DataSet ds) { return ds.orderBy("v"); }
                    @Override public List<DataRow> oracle(List<DataRow> in) {
                        return O.sortRows(in, D.KV, O.sf(D.KV, "v", true, "DEFAULT"));
                    }
                    @Override public boolean ordered() { return false; }  // ties on a single key, so compared as a multiset
                },
                new OpTpl() {
                    @Override public DataSet dag(DataSet ds) { return ds.top(2); }
                    @Override public List<DataRow> oracle(List<DataRow> in) {
                        return in.subList(0, Math.min(2, in.size()));
                    }
                    @Override public boolean ordered() { return true; }
                },
                new OpTpl() {
                    @Override public DataSet dag(DataSet ds) {
                        return ds.map(r -> {
                            DataRow o = r.copy();
                            o.set(0, r.get(0) == null ? null : ((String) r.get(0)).toUpperCase());
                            return o;
                        });
                    }
                    @Override public List<DataRow> oracle(List<DataRow> in) {
                        return SL.mapRows(in, r -> {
                            DataRow o = r.copy();
                            o.set(0, r.get(0) == null ? null : ((String) r.get(0)).toUpperCase());
                            return o;
                        });
                    }
                    @Override public boolean ordered() { return false; }
                },
                new OpTpl() {
                    @Override public DataSet dag(DataSet ds) { return ds.count(); }
                    @Override public List<DataRow> oracle(List<DataRow> in) {
                        return D.rows(new Object[]{(long) in.size()});
                    }
                    @Override public boolean ordered() { return false; }
                },
                new OpTpl() {
                    @Override public DataSet dag(DataSet ds) {
                        return ds.groupBy("k").count().orderBy("k");
                    }
                    @Override public List<DataRow> oracle(List<DataRow> in) {
                        io.rowweave.DataSchema m = new io.rowweave.DataSchema(
                                D.f("k", io.rowweave.DataType.StringType),
                                D.f("count0", io.rowweave.DataType.LongType));
                        return O.sortRows(O.groupCount(in, O.idx(0)), m, O.sf(m, "k", true, "DEFAULT"));
                    }
                    @Override public boolean ordered() { return false; }
                },
                new OpTpl() {
                    @Override public DataSet dag(DataSet ds) { return ds.select(1, 0); }
                    @Override public List<DataRow> oracle(List<DataRow> in) {
                        return SL.mapRows(in, r -> {
                            DataRow o = new DataRow(2);
                            o.set(0, r.get(1));
                            o.set(1, r.get(0));
                            return o;
                        });
                    }
                    @Override public boolean ordered() { return false; }
                },
                new OpTpl() {
                    @Override public DataSet dag(DataSet ds) { return ds.union(ds); }
                    @Override public List<DataRow> oracle(List<DataRow> in) { return SL.concat(in, in); }
                    @Override public boolean ordered() { return false; }
                });
    }

    private static BatchDesign edgeBatch(int batchNo, String title) {
        List<OpTpl> tpls = opTemplates();
        return new BatchDesign() {
            @Override public int batchNo() { return batchNo; }
            @Override public String title() { return title; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < tpls.size(); t++) {
                    for (int d = 0; d < EDGE_NAMES.length; d++) {
                        final OpTpl tpl = tpls.get(t);
                        final List<DataRow> data = edgeProfile(d);
                        final String tplName = "tpl" + t;
                        out.add(SL.sink(String.format("B%03d", batchNo) + "-T" + t + "-D" + d,
                                tplName + "[" + EDGE_NAMES[d] + "]",
                                D.KV, data, tpl::dag, tpl::oracle, tpl.ordered()));
                    }
                }
                return out;
            }
        };
    }

    // B83 to B92 share one framework and differ in the data and the operator they emphasize, with each batch swapping in its own set of data profiles
    private static BatchDesign b83() {
        return edgeBatch(83, "boundary: empty table across 10 operator templates (every output must be empty)");
    }

    private static BatchDesign b84() {
        return edgeBatch(84, "boundary: single-row input across 10 operator templates");
    }

    private static BatchDesign b85() {
        return edgeBatch(85, "boundary: all-null value column across 10 operator templates");
    }

    private static BatchDesign b86() {
        return edgeBatch(86, "boundary: duplicate rows across 10 operator templates");
    }

    private static BatchDesign b87() {
        return new BatchDesign() {
            @Override public int batchNo() { return 87; }
            @Override public String title() { return "unicode and case-sensitive keys (grouping keys are case sensitive) × 10 templates × 10 data sets"; }
            @Override public List<ScenarioDef> scenarios() {
                List<List<DataRow>> datasets = Arrays.asList(
                        D.kvUnicode(),
                        D.rows(new Object[]{"A", D.bd("1")}, new Object[]{"a", D.bd("2")},
                                new Object[]{"A", D.bd("3")}),
                        D.rows(new Object[]{"Ω", D.bd("1")}, new Object[]{"ω", D.bd("2")},
                                new Object[]{"Ω", D.bd("3")}),
                        D.rows(new Object[]{"ß", D.bd("1")}, new Object[]{"ss", D.bd("2")}),
                        D.rows(new Object[]{"Ä", D.bd("1")}, new Object[]{"ä", D.bd("2")}),
                        D.rows(new Object[]{"😀", D.bd("1")}, new Object[]{"😎", D.bd("2")}),
                        D.rows(new Object[]{"i", D.bd("1")}, new Object[]{"İ", D.bd("2")},
                                new Object[]{"ı", D.bd("3")}),
                        D.rows(new Object[]{"Σ", D.bd("1")}, new Object[]{"Τ", D.bd("2")},
                                new Object[]{"Υ", D.bd("3")}),
                        D.rows(new Object[]{"🎉x", D.bd("1")}, new Object[]{"🎉y", D.bd("2")}),
                        D.rows(new Object[]{"nulls", null}, new Object[]{null, D.bd("1")}));
                List<OpTpl> tpls = opTemplates();
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < tpls.size(); t++) {
                    for (int d = 0; d < datasets.size(); d++) {
                        final OpTpl tpl = tpls.get(t);
                        final List<DataRow> data = datasets.get(d);
                        out.add(SL.sink(id("B87", t, d), "tpl" + t + "[uni" + d + "]",
                                D.KV, data, tpl::dag, tpl::oracle, tpl.ordered()));
                    }
                }
                return out;
            }
        };
    }

    private static BatchDesign b88() {
        return new BatchDesign() {
            @Override public int batchNo() { return 88; }
            @Override public String title() { return "whitespace string keys (empty string / space / tab) × 10 templates × 10 data sets"; }
            @Override public List<ScenarioDef> scenarios() {
                List<List<DataRow>> datasets = Arrays.asList(
                        D.kvWhitespace(),
                        D.rows(new Object[]{"", D.bd("1")}, new Object[]{"", D.bd("2")}),
                        D.rows(new Object[]{" ", D.bd("1")}, new Object[]{"  ", D.bd("2")}),
                        D.rows(new Object[]{"\t", D.bd("1")}, new Object[]{"\n", D.bd("2")}),
                        D.rows(new Object[]{"a b", D.bd("1")}, new Object[]{"a  b", D.bd("2")}),
                        D.rows(new Object[]{"​", D.bd("1")}, new Object[]{"a", D.bd("2")}),
                        D.rows(new Object[]{"", null}, new Object[]{null, D.bd("1")}),
                        D.rows(new Object[]{" ", D.bd("1")}, new Object[]{" ", D.bd("1")},
                                new Object[]{" ", D.bd("1")}),
                        D.rows(new Object[]{"a", null}, new Object[]{"b", null}),
                        D.rows(new Object[]{" leading", D.bd("1")}, new Object[]{"trailing ", D.bd("2")}));
                List<OpTpl> tpls = opTemplates();
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < tpls.size(); t++) {
                    for (int d = 0; d < datasets.size(); d++) {
                        final OpTpl tpl = tpls.get(t);
                        final List<DataRow> data = datasets.get(d);
                        out.add(SL.sink(id("B88", t, d), "tpl" + t + "[ws" + d + "]",
                                D.KV, data, tpl::dag, tpl::oracle, tpl.ordered()));
                    }
                }
                return out;
            }
        };
    }

    private static BatchDesign b89() {
        return new BatchDesign() {
            @Override public int batchNo() { return 89; }
            @Override public String title() { return "BigDecimal extremes (very large / very small scale) × 10 templates × 10 data sets"; }
            @Override public List<ScenarioDef> scenarios() {
                List<List<DataRow>> datasets = Arrays.asList(
                        D.kvScale(),
                        D.rows(new Object[]{"max", new BigDecimal("1E+30")}),
                        D.rows(new Object[]{"min", new BigDecimal("1E-30")}),
                        D.rows(new Object[]{"half", D.bd("0.5")}, new Object[]{"half", D.bd("0.25")}),
                        D.rows(new Object[]{"neg0", new BigDecimal("-0.00000000004")}),
                        D.rows(new Object[]{"many", D.bd("0.1")}, new Object[]{"many", D.bd("0.2")},
                                new Object[]{"many", D.bd("0.3")}),
                        D.rows(new Object[]{"precision", new BigDecimal("123456789012345678901234567890")}),
                        D.rows(new Object[]{"scale0", new BigDecimal("7")}),
                        D.rows(new Object[]{"a", D.bd("1")}, new Object[]{"a", D.bd("1E-10")}),
                        D.rows(new Object[]{"rounding", D.bd("2.675")}));
                List<OpTpl> tpls = opTemplates();
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < tpls.size(); t++) {
                    for (int d = 0; d < datasets.size(); d++) {
                        final OpTpl tpl = tpls.get(t);
                        final List<DataRow> data = datasets.get(d);
                        out.add(SL.sink(id("B89", t, d), "tpl" + t + "[bd" + d + "]",
                                D.KV, data, tpl::dag, tpl::oracle, tpl.ordered()));
                    }
                }
                return out;
            }
        };
    }

    private static BatchDesign b90() {
        return new BatchDesign() {
            @Override public int batchNo() { return 90; }
            @Override public String title() { return "negative numbers and HALF_UP rounding boundaries × 10 templates × 10 data sets"; }
            @Override public List<ScenarioDef> scenarios() {
                List<List<DataRow>> datasets = Arrays.asList(
                        D.rows(new Object[]{"h", D.bd("0.0000000005")}, new Object[]{"h", D.bd("0")}),
                        D.rows(new Object[]{"h", D.bd("-0.0000000005")}, new Object[]{"h", D.bd("0")}),
                        D.rows(new Object[]{"h", D.bd("1.0000000005")}),
                        D.rows(new Object[]{"h", D.bd("-1.0000000005")}),
                        D.rows(new Object[]{"h", D.bd("2.5")}, new Object[]{"h", D.bd("-2.5")}),
                        D.rows(new Object[]{"h", D.bd("3.5")}, new Object[]{"h", D.bd("-3.5")}),
                        D.rows(new Object[]{"p", D.bd("2.005")}, new Object[]{"n", D.bd("-2.005")}),
                        D.rows(new Object[]{"x", D.bd("0.00000000049")}, new Object[]{"x", D.bd("0.00000000051")}),
                        D.rows(new Object[]{"y", D.bd("-0.00000000049")}, new Object[]{"y", D.bd("-0.00000000051")}),
                        D.rows(new Object[]{"z", D.bd("99999.99999999995")}));
                List<OpTpl> tpls = opTemplates();
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < tpls.size(); t++) {
                    for (int d = 0; d < datasets.size(); d++) {
                        final OpTpl tpl = tpls.get(t);
                        final List<DataRow> data = datasets.get(d);
                        out.add(SL.sink(id("B90", t, d), "tpl" + t + "[round" + d + "]",
                                D.KV, data, tpl::dag, tpl::oracle, tpl.ordered()));
                    }
                }
                return out;
            }
        };
    }

    private static BatchDesign b91() {
        return new BatchDesign() {
            @Override public int batchNo() { return 91; }
            @Override public String title() { return "integer keys and integer columns (Long) across operators × 10 templates × 10 L2 data sets"; }
            @Override public List<ScenarioDef> scenarios() {
                List<List<DataRow>> datasets = Arrays.asList(
                        D.l2Sort(),
                        D.rows(new Object[]{1L, 1L}, new Object[]{1L, 2L}, new Object[]{2L, 1L}),
                        D.rows(new Object[]{null, 1L}, new Object[]{null, 2L}),
                        D.rows(new Object[]{0L, 0L}),
                        D.rows(new Object[]{-1L, -1L}, new Object[]{-1L, -2L}),
                        D.rows(new Object[]{Long.MAX_VALUE, 1L}, new Object[]{Long.MIN_VALUE, 1L}),
                        D.rows(new Object[]{5L, null}, new Object[]{5L, 5L}),
                        D.rows(new Object[]{7L, 7L}, new Object[]{7L, 7L}, new Object[]{7L, 7L}),
                        D.rows(new Object[]{1L, 2L}, new Object[]{2L, 1L}, new Object[]{1L, 2L},
                                new Object[]{2L, 1L}),
                        D.empty());
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < 10; t++) {
                    for (int d = 0; d < datasets.size(); d++) {
                        final int tf = t;
                        final List<DataRow> data = datasets.get(d);
                        out.add(ScenarioDef.of(id("B91", t, d), "l2[t" + tf + "][d" + d + "]",
                                lab -> {
                                    DataSet src = lab.source(D.L2, data);
                                    io.rowweave.DataSchema lm = D.L2;
                                    switch (tf) {
                                        case 0:
                                            lab.expect(src.groupBy("a").count(),
                                                    O.groupCount(data, O.idx(0)));
                                            break;
                                        case 1:
                                            lab.expect(src.groupBy("a").max("b"),
                                                    O.groupMinMax(data, O.idx(0), 1, true));
                                            break;
                                        case 2:
                                            lab.expect(src.groupBy("a").min("b"),
                                                    O.groupMinMax(data, O.idx(0), 1, false));
                                            break;
                                        case 3:
                                            lab.expectOrdered(src.orderBy(O.orderBy(lm,
                                                    O.sf(lm, "a", true, "DEFAULT"),
                                                    O.sf(lm, "b", true, "DEFAULT")), 1, false),
                                                    O.sortRows(data, lm,
                                                            O.sf(lm, "a", true, "DEFAULT"),
                                                            O.sf(lm, "b", true, "DEFAULT")));
                                            break;
                                        case 4:
                                            lab.expect(src.distinct(), O.distinctAll(data));
                                            break;
                                        case 5:
                                            lab.expect(src.filter("a > 1"),
                                                    SL.filterRows(data, r -> r.getLong(0) != null
                                                            && r.getLong(0) > 1));
                                            break;
                                        case 6:
                                            lab.expect(src.select(1, 0), SL.mapRows(data, r -> {
                                                DataRow o = new DataRow(2);
                                                o.set(0, r.get(1));
                                                o.set(1, r.get(0));
                                                return o;
                                            }));
                                            break;
                                        case 7:
                                            lab.expect(src.union(src), SL.concat(data, data));
                                            break;
                                        case 8:
                                            lab.expectOrdered(src.top(3),
                                                    data.subList(0, Math.min(3, data.size())));
                                            break;
                                        default:
                                            lab.expect(src.groupBy("a", "b").count(),
                                                    O.groupCount(data, O.idx(0, 1)));
                                            break;
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    private static BatchDesign b92() {
        return new BatchDesign() {
            @Override public int batchNo() { return 92; }
            @Override public String title() { return "wide table with several keys (CAT cat/sub two dimensions) × 10 templates × 10 data sets"; }
            @Override public List<ScenarioDef> scenarios() {
                List<List<DataRow>> datasets = Arrays.asList(
                        D.catBase(),
                        D.empty(),
                        D.rows(new Object[]{1L, "a", "x", D.bd("1"), true}),
                        D.rows(new Object[]{1L, null, null, null, null}),
                        D.rows(new Object[]{1L, "a", "x", D.bd("1"), true},
                                new Object[]{2L, "a", "x", D.bd("1"), true}),
                        D.rows(new Object[]{1L, "a", "x", D.bd("5"), true},
                                new Object[]{2L, "a", "y", D.bd("5"), false}),
                        D.rows(new Object[]{1L, "Ω", "ω", D.bd("1"), true},
                                new Object[]{2L, "Ω", "Ψ", D.bd("2"), false}),
                        D.rows(new Object[]{1L, "b", "x", D.bd("-1"), null},
                                new Object[]{2L, "b", "x", D.bd("0"), false}),
                        D.rows(new Object[]{9L, "z", null, D.bd("9"), true},
                                new Object[]{8L, null, "y", D.bd("8"), null}),
                        D.rows(new Object[]{1L, "k", "s", D.bd("1"), true},
                                new Object[]{2L, "k", "s", D.bd("2"), true},
                                new Object[]{3L, "k", "s", D.bd("3"), true},
                                new Object[]{4L, "k", "s", D.bd("4"), true}));
                List<ScenarioDef> out = new ArrayList<>();
                for (int t = 0; t < 10; t++) {
                    for (int d = 0; d < datasets.size(); d++) {
                        final int tf = t;
                        final List<DataRow> data = datasets.get(d);
                        out.add(ScenarioDef.of(id("B92", t, d), "cat[t" + tf + "][d" + d + "]",
                                lab -> {
                                    DataSet src = lab.source(D.CAT, data);
                                    io.rowweave.DataSchema m = D.CAT;
                                    switch (tf) {
                                        case 0:
                                            lab.expect(src.groupBy("cat", "sub").sum("amt"),
                                                    O.groupSum(data, O.idx(1, 2), 3));
                                            break;
                                        case 1:
                                            lab.expect(src.groupBy("cat").count(),
                                                    O.groupCount(data, O.idx(1)));
                                            break;
                                        case 2:
                                            lab.expect(src.distinct("cat", "sub").select("cat", "sub"),
                                                    SL.mapRows(O.distinctBy(data, O.idx(1, 2)),
                                                            r -> O.project(r, O.idx(1, 2))));
                                            break;
                                        case 3:
                                            lab.expect(src.groupBy("cat").max("amt"),
                                                    O.groupMinMax(data, O.idx(1), 3, true));
                                            break;
                                        case 4:
                                            lab.expect(src.groupBy("sub").min("amt"),
                                                    O.groupMinMax(data, O.idx(2), 3, false));
                                            break;
                                        case 5:
                                            lab.expect(src.filter("amt > 0").groupBy("cat").sum("amt"),
                                                    O.groupSum(SL.filterRows(data,
                                                            r -> r.getBigDecimal(3) != null
                                                                    && r.getBigDecimal(3).signum() > 0),
                                                            O.idx(1), 3));
                                            break;
                                        case 6:
                                            lab.expectOrdered(src.orderBy(O.orderBy(m,
                                                    O.sf(m, "cat", true, "DEFAULT"),
                                                    O.sf(m, "sub", true, "DEFAULT"),
                                                    O.sf(m, "amt", true, "DEFAULT")), 1, false),
                                                    O.sortRows(data, m,
                                                            O.sf(m, "cat", true, "DEFAULT"),
                                                            O.sf(m, "sub", true, "DEFAULT"),
                                                            O.sf(m, "amt", true, "DEFAULT")));
                                            break;
                                        case 7:
                                            lab.expect(src.select("cat", "amt"),
                                                    SL.mapRows(data, r -> {
                                                        DataRow o = new DataRow(2);
                                                        o.set(0, r.get(1));
                                                        o.set(1, r.get(3));
                                                        return o;
                                                    }));
                                            break;
                                        case 8:
                                            lab.expect(src.groupBy("cat", "sub", "active").count(),
                                                    O.groupCount(data, O.idx(1, 2, 4)));
                                            break;
                                        default:
                                            lab.expect(src.partitionByHash("cat", "sub")
                                                    .groupBy("cat", "sub").count(),
                                                    O.groupCount(data, O.idx(1, 2)));
                                            break;
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }
}
