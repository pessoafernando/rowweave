package io.rowweave.examples.scenariolab.batches;

import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.GroupReduceFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.examples.scenariolab.BatchDesign;
import io.rowweave.examples.scenariolab.D;
import io.rowweave.examples.scenariolab.O;
import io.rowweave.examples.scenariolab.SL;
import io.rowweave.examples.scenariolab.ScenarioDef;
import io.rowweave.examples.scenariolab.ScenarioLab;
import io.rowweave.ops.RowValidation;
import io.rowweave.ops.SchemaConversion;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.rowweave.examples.scenariolab.batches.FamilyA.id;
import static io.rowweave.examples.scenariolab.batches.FamilyA.kvProfile;
import static io.rowweave.examples.scenariolab.batches.FamilyA.KV_PROFILES;

/**
 * Family H (B73 to B82): whole-table aggregation, convertSchema, validateRows, and negative
 * scenarios for misusing the API, where expectThrows asserts that the engine fails fast with
 * the stated exception type.
 */
public final class FamilyH {

    private FamilyH() {
    }

    public static List<BatchDesign> batches() {
        return Arrays.asList(
                b73(), b74(), b75(), b76(), b77(),
                b78(), b79(), b80(), b81(), b82());
    }

    // ========================================================================
    // B73 whole-table aggregation, 10 variants over 10 KV profiles
    // ========================================================================

    private static BatchDesign b73() {
        return new BatchDesign() {
            @Override public int batchNo() { return 73; }
            @Override public String title() { return "whole-table scalar aggregates sum/count/max/min × 10 variants × 10 KV profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(SL.sink(id("B73", v, d), "wholeAgg[v" + vf + "][" + KV_PROFILES[d] + "]",
                                D.KV, data, ds -> dag(vf, ds),
                                in -> oracle(vf, in), false));
                    }
                }
                return out;
            }

            private DataSet dag(int v, DataSet ds) {
                switch (v) {
                    case 0: return ds.sum("v");
                    case 1: return ds.sum("v", "total");
                    case 2: return ds.count();
                    case 3: return ds.count("v");
                    case 4: return ds.count("k");
                    case 5: return ds.max("v");
                    case 6: return ds.min("v");
                    case 7: return ds.max("k");
                    case 8: return ds.min("k");
                    default: return ds.filter("v > 0").sum("v");
                }
            }

            private List<DataRow> oracle(int v, List<DataRow> in) {
                DataRow o = new DataRow(1);
                switch (v) {
                    case 0:
                    case 1: {
                        BigDecimal sum = null;
                        for (DataRow r : in) {
                            if (r.get(1) != null) {
                                sum = sum == null ? BigDecimal.ZERO : sum;
                                sum = sum.add(r.getBigDecimal(1));
                            }
                        }
                        o.set(0, sum == null ? null : sum.setScale(10, java.math.RoundingMode.HALF_UP));
                        break;
                    }
                    case 2:
                        o.set(0, (long) in.size());
                        break;
                    case 3: {
                        long c = 0;
                        for (DataRow r : in) {
                            if (r.get(1) != null) {
                                c++;
                            }
                        }
                        o.set(0, c);
                        break;
                    }
                    case 4: {
                        long c = 0;
                        for (DataRow r : in) {
                            if (r.get(0) != null) {
                                c++;
                            }
                        }
                        o.set(0, c);
                        break;
                    }
                    case 5: {
                        BigDecimal max = null;
                        for (DataRow r : in) {
                            if (r.get(1) != null
                                    && (max == null || r.getBigDecimal(1).compareTo(max) > 0)) {
                                max = r.getBigDecimal(1);
                            }
                        }
                        o.set(0, max);
                        break;
                    }
                    case 6: {
                        BigDecimal min = null;
                        for (DataRow r : in) {
                            if (r.get(1) != null
                                    && (min == null || r.getBigDecimal(1).compareTo(min) < 0)) {
                                min = r.getBigDecimal(1);
                            }
                        }
                        o.set(0, min);
                        break;
                    }
                    case 7: {
                        String max = null;
                        for (DataRow r : in) {
                            String k = (String) r.get(0);
                            if (k != null && (max == null || k.compareTo(max) > 0)) {
                                max = k;
                            }
                        }
                        o.set(0, max);
                        break;
                    }
                    case 8: {
                        String min = null;
                        for (DataRow r : in) {
                            String k = (String) r.get(0);
                            if (k != null && (min == null || k.compareTo(min) < 0)) {
                                min = k;
                            }
                        }
                        o.set(0, min);
                        break;
                    }
                    default: {
                        BigDecimal sum = null;
                        for (DataRow r : in) {
                            if (r.get(1) != null && r.getBigDecimal(1).signum() > 0) {
                                sum = sum == null ? BigDecimal.ZERO : sum;
                                sum = sum.add(r.getBigDecimal(1));
                            }
                        }
                        o.set(0, sum == null ? null : sum.setScale(10, java.math.RoundingMode.HALF_UP));
                        break;
                    }
                }
                List<DataRow> out = new ArrayList<>();
                out.add(o);
                return out;
            }
        };
    }

    // ========================================================================
    // B74 whole-table aggregation on empty and all-null data, 10 variants over 10 special profiles
    // ========================================================================

    private static BatchDesign b74() {
        return new BatchDesign() {
            @Override public int batchNo() { return 74; }
            @Override public String title() { return "whole-table aggregates over empty / all-null / single-row data × 10 variants × 10 special data sets"; }
            @Override public List<ScenarioDef> scenarios() {
                List<List<DataRow>> specials = Arrays.asList(
                        D.empty(),
                        D.rows(new Object[]{"a", null}, new Object[]{"b", null}),
                        D.rows(new Object[]{null, null}),
                        D.single(),
                        D.rows(new Object[]{"x", D.bd("0")}),
                        D.rows(new Object[]{null, D.bd("-5")}, new Object[]{"y", D.bd("5")}),
                        D.rows(new Object[]{"", D.bd("0.0000000001")}),
                        D.rows(new Object[]{"m", D.bd("1")}, new Object[]{"m", null},
                                new Object[]{null, null}, new Object[]{"n", D.bd("3")}),
                        D.kvDupes(),
                        D.rows(new Object[]{"z", D.bd("999999999999.9999999999")}));
                String[] names = {"empty", "allNullV", "singleNull", "single", "zeroV",
                        "negPos", "tiny", "mixed", "dupes", "huge"};
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < specials.size(); d++) {
                        final List<DataRow> data = specials.get(d);
                        out.add(SL.sink(id("B74", v, d), "aggSpecial[" + names[d] + "] v" + vf,
                                D.KV, data, ds -> dag(vf, ds),
                                in -> oracle(vf, in), false));
                    }
                }
                return out;
            }

            private DataSet dag(int v, DataSet ds) {
                switch (v) {
                    case 0: return ds.sum("v");
                    case 1: return ds.count();
                    case 2: return ds.count("v");
                    case 3: return ds.max("v");
                    case 4: return ds.min("v");
                    case 5: return ds.max("k");
                    case 6: return ds.min("k");
                    case 7: return ds.filter("v is not null").count();
                    case 8: return ds.filter("k is not null").sum("v");
                    default: return ds.distinct().count();
                }
            }

            private List<DataRow> oracle(int v, List<DataRow> in) {
                DataRow o = new DataRow(1);
                switch (v) {
                    case 0: {
                        BigDecimal sum = null;
                        for (DataRow r : in) {
                            if (r.get(1) != null) {
                                sum = sum == null ? BigDecimal.ZERO : sum;
                                sum = sum.add(r.getBigDecimal(1));
                            }
                        }
                        o.set(0, sum == null ? null : sum.setScale(10, java.math.RoundingMode.HALF_UP));
                        break;
                    }
                    case 1:
                        o.set(0, (long) in.size());
                        break;
                    case 2: {
                        long c = 0;
                        for (DataRow r : in) {
                            if (r.get(1) != null) {
                                c++;
                            }
                        }
                        o.set(0, c);
                        break;
                    }
                    case 3: {
                        BigDecimal max = null;
                        for (DataRow r : in) {
                            if (r.get(1) != null
                                    && (max == null || r.getBigDecimal(1).compareTo(max) > 0)) {
                                max = r.getBigDecimal(1);
                            }
                        }
                        o.set(0, max);
                        break;
                    }
                    case 4: {
                        BigDecimal min = null;
                        for (DataRow r : in) {
                            if (r.get(1) != null
                                    && (min == null || r.getBigDecimal(1).compareTo(min) < 0)) {
                                min = r.getBigDecimal(1);
                            }
                        }
                        o.set(0, min);
                        break;
                    }
                    case 5: {
                        String max = null;
                        for (DataRow r : in) {
                            String k = (String) r.get(0);
                            if (k != null && (max == null || k.compareTo(max) > 0)) {
                                max = k;
                            }
                        }
                        o.set(0, max);
                        break;
                    }
                    case 6: {
                        String min = null;
                        for (DataRow r : in) {
                            String k = (String) r.get(0);
                            if (k != null && (min == null || k.compareTo(min) < 0)) {
                                min = k;
                            }
                        }
                        o.set(0, min);
                        break;
                    }
                    case 7: {
                        long c = 0;
                        for (DataRow r : in) {
                            if (r.get(1) != null) {
                                c++;
                            }
                        }
                        o.set(0, c);
                        break;
                    }
                    case 8: {
                        BigDecimal sum = null;
                        for (DataRow r : in) {
                            if (r.get(0) != null && r.get(1) != null) {
                                sum = sum == null ? BigDecimal.ZERO : sum;
                                sum = sum.add(r.getBigDecimal(1));
                            }
                        }
                        o.set(0, sum == null ? null : sum.setScale(10, java.math.RoundingMode.HALF_UP));
                        break;
                    }
                    default:
                        o.set(0, (long) O.distinctAll(in).size());
                        break;
                }
                List<DataRow> out = new ArrayList<>();
                out.add(o);
                return out;
            }
        };
    }

    // ========================================================================
    // B75 convertSchema casts, 10 mappings over 10 profiles
    // ========================================================================

    private static BatchDesign b75() {
        return new BatchDesign() {
            @Override public int batchNo() { return 75; }
            @Override public String title() { return "convertSchema type mapping (IDENTITY/INT→LONG/INT→DEC/LONG→DEC) × 10 mappings × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B75", v, d), "convert[v" + vf + "][" + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet src = lab.source(D.KV, data);
                                    List<DataRow> expected;
                                    DataSet res;
                                    switch (vf) {
                                        case 0: {
                                            SchemaConversion sc = SchemaConversion.builder(D.KV)
                                                    .nullable("k", D.f("k", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .nullable("v", D.f("v", DataType.BigDecimalType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            res = src.convertSchema(sc);
                                            expected = data;
                                            break;
                                        }
                                        case 1: {
                                            SchemaConversion sc = SchemaConversion.builder(D.KV)
                                                    .nullable("k", D.f("key", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            res = src.convertSchema(sc);
                                            expected = SL.mapRows(data, r -> one(r.get(0)));
                                            break;
                                        }
                                        case 2: {
                                            SchemaConversion sc = SchemaConversion.builder(D.KV)
                                                    .nullable("v", D.f("val", DataType.BigDecimalType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            res = src.convertSchema(sc);
                                            expected = SL.mapRows(data, r -> one(r.get(1)));
                                            break;
                                        }
                                        case 3: {
                                            SchemaConversion sc = SchemaConversion.builder(D.KV)
                                                    .nullable("k", D.f("key", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .nullable("v", D.f("val", DataType.BigDecimalType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            res = src.convertSchema(sc);
                                            expected = data;
                                            break;
                                        }
                                        case 4: {
                                            SchemaConversion sc = SchemaConversion.builder(D.KV)
                                                    .nullable("k", D.f("key", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .defaulted(D.f("flag", DataType.BooleanType), Boolean.TRUE)
                                                    .build();
                                            res = src.convertSchema(sc);
                                            expected = SL.mapRows(data, r -> two(r.get(0), Boolean.TRUE));
                                            break;
                                        }
                                        case 5: {
                                            SchemaConversion sc = SchemaConversion.builder(D.KV)
                                                    .nullable("k", D.f("key", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .defaulted(D.f("n", DataType.LongType), 42L)
                                                    .build();
                                            res = src.convertSchema(sc);
                                            expected = SL.mapRows(data, r -> two(r.get(0), 42L));
                                            break;
                                        }
                                        case 6: {
                                            SchemaConversion sc = SchemaConversion.builder(D.KV)
                                                    .nullable("v", D.f("val", DataType.BigDecimalType), SchemaConversion.Cast.IDENTITY)
                                                    .defaulted(D.f("d", DataType.BigDecimalType), D.bd("-1"))
                                                    .build();
                                            res = src.convertSchema(sc);
                                            expected = SL.mapRows(data, r -> two(r.get(1), D.bd("-1")));
                                            break;
                                        }
                                        case 7: {
                                            // reorders the columns to v, k
                                            SchemaConversion sc = SchemaConversion.builder(D.KV)
                                                    .nullable("v", D.f("val", DataType.BigDecimalType), SchemaConversion.Cast.IDENTITY)
                                                    .nullable("k", D.f("key", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            res = src.convertSchema(sc);
                                            expected = SL.mapRows(data, r -> two(r.get(1), r.get(0)));
                                            break;
                                        }
                                        case 8: {
                                            // references k twice
                                            SchemaConversion sc = SchemaConversion.builder(D.KV)
                                                    .nullable("k", D.f("a", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .nullable("k", D.f("b", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            res = src.convertSchema(sc);
                                            expected = SL.mapRows(data, r -> two(r.get(0), r.get(0)));
                                            break;
                                        }
                                        default: {
                                            // three columns: k, v and a constant
                                            SchemaConversion sc = SchemaConversion.builder(D.KV)
                                                    .nullable("k", D.f("key", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .nullable("v", D.f("val", DataType.BigDecimalType), SchemaConversion.Cast.IDENTITY)
                                                    .defaulted(D.f("c", DataType.StringType), "Z")
                                                    .build();
                                            res = src.convertSchema(sc);
                                            expected = SL.mapRows(data, r -> three(r.get(0), r.get(1), "Z"));
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
    // B76 convertSchema rejections such as an input fingerprint mismatch, 10 by 10
    // ========================================================================

    private static BatchDesign b76() {
        return new BatchDesign() {
            @Override public int batchNo() { return 76; }
            @Override public String title() { return "convertSchema negative cases (wrong schema mapping) × 10 combinations × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.throwsOf(id("B76", v, d), "convertBad[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                java.lang.IllegalArgumentException.class,
                                vf == 1 || vf == 4 ? "SCHEMA_MAPPING_INVALID"
                                        : vf == 2 ? "conversion is required"
                                        : "SCHEMA_MAPPING_INPUT_MISMATCH",
                                () -> {
                                    JobSessionHolder h = new JobSessionHolder();
                                    DataSet src;
                                    SchemaConversion sc;
                                    switch (vf) {
                                        case 0:
                                            // the mapping is bound to the L2 schema but the dataset is KV, so the fingerprint does not match
                                            src = h.lab.source(D.KV, data);
                                            sc = SchemaConversion.builder(D.L2)
                                                    .required("a", D.f("x", DataType.LongType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            src.convertSchema(sc);
                                            break;
                                        case 1:
                                            src = h.lab.source(D.KV, data);
                                            sc = SchemaConversion.builder(D.KV)
                                                    .required("nosuch", D.f("x", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            src.convertSchema(sc);
                                            break;
                                        case 2:
                                            // null conversion
                                            src = h.lab.source(D.KV, data);
                                            src.convertSchema(null);
                                            break;
                                        case 3:
                                            // an L2 dataset with a KV mapping
                                            src = h.lab.source(D.L2, D.l2Sort());
                                            sc = SchemaConversion.builder(D.KV)
                                                    .required("k", D.f("x", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            src.convertSchema(sc);
                                            break;
                                        case 4:
                                            // incompatible shape: the K1 mapping (one String column) references v
                                            src = h.lab.source(D.K1, D.k1Set("a", "b"));
                                            sc = SchemaConversion.builder(D.K1)
                                                    .required("v", D.f("x", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            src.convertSchema(sc);
                                            break;
                                        default:
                                            // the same variant as case 0, with a builder for a different source schema
                                            src = h.lab.source(D.KV, data);
                                            sc = SchemaConversion.builder(D.DIM)
                                                    .required("label", D.f("x", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                                                    .build();
                                            src.convertSchema(sc);
                                            break;
                                    }
                                }));
                    }
                }
                return out;
            }
        };
    }

    /** Holder for the lab inside a scenario; a throw scenario builds one on the fly. */
    static final class JobSessionHolder {
        final io.rowweave.examples.scenariolab.ScenarioLab lab =
                new io.rowweave.examples.scenariolab.ScenarioLab(
                        System.getProperty("java.io.tmpdir") + "/scenariolab-neg-"
                                + java.util.UUID.randomUUID());
    }

    // ========================================================================
    // B77 validateRows passing valid rows, 10 rule groups over 10 profiles
    // ========================================================================

    private static BatchDesign b77() {
        return new BatchDesign() {
            @Override public int batchNo() { return 77; }
            @Override public String title() { return "validateRows (valid rows pass through unchanged) × 10 rule sets × 10 ORD profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < 10; d++) {
                        final List<DataRow> data = FamilyB.ordProfile(d);
                        out.add(ScenarioDef.of(id("B77", v, d), "validate[v" + vf + "][ord" + d + "]",
                                lab -> {
                                    // builds a subset containing only valid rows, since the oracle filters with the same rules as the engine
                                    List<DataRow> valid = filterValid(vf, data);
                                    DataSet src = lab.source(D.ORD, valid);
                                    DataSet res = src.validateRows(validation(vf));
                                    lab.expect(res, valid);
                                }));
                    }
                }
                return out;
            }

            private List<DataRow> filterValid(int v, List<DataRow> in) {
                List<DataRow> out = new ArrayList<>();
                for (DataRow r : in) {
                    if (rowValid(v, r)) {
                        out.add(r);
                    }
                }
                return out;
            }

            private boolean rowValid(int v, DataRow r) {
                switch (v) {
                    case 0: return r.get(1) != null;
                    case 1: return r.get(0) != null;
                    case 2: return r.get(3) == null
                            || (r.getInteger(3) >= 0 && r.getInteger(3) <= 100);
                    case 3: return r.get(2) == null
                            || (r.getBigDecimal(2).compareTo(BigDecimal.ZERO) >= 0
                            && r.getBigDecimal(2).compareTo(new BigDecimal("999999999")) <= 0);
                    case 4: return true;
                    case 5: return r.get(2) == null || r.getBigDecimal(2).doubleValue() <= 10000;
                    case 6: return r.get(1) != null && r.get(0) != null;
                    case 7: return r.getInteger(3) == null || r.getInteger(3) > 0;
                    case 8: return r.getBigDecimal(2) == null
                            || (r.getBigDecimal(2).compareTo(new BigDecimal("-1000")) >= 0
                            && r.getBigDecimal(2).compareTo(new BigDecimal("999999999")) <= 0);
                    default: return r.get(1) != null;
                }
            }

            private RowValidation validation(int v) {
                RowValidation.Builder b = RowValidation.builder(D.ORD);
                switch (v) {
                    case 0: b.notNull("r0", "grp"); break;
                    case 1: b.notNull("r1", "id"); break;
                    case 2: b.integerRange("r2", "qty", 0, 100); break;
                    case 3: b.decimalRange("r3", "amt", new BigDecimal("0"), new BigDecimal("999999999")); break;
                    case 4: b.notNull("r4", "id"); break;
                    case 5: b.decimalRange("r5", "amt", new BigDecimal("-999999999"), new BigDecimal("10000")); break;
                    case 6:
                        b.notNull("r6a", "grp");
                        b.notNull("r6b", "id");
                        break;
                    case 7: b.integerRange("r7", "qty", 1, Integer.MAX_VALUE); break;
                    case 8: b.decimalRange("r8", "amt", new BigDecimal("-1000"), new BigDecimal("999999999")); break;
                    default:
                        b.notNull("rd", "grp");
                        break;
                }
                return b.build();
            }
        };
    }

    // ========================================================================
    // B78 validateRows failing fast on invalid rows, 10 rule groups over 10 datasets
    // ========================================================================

    private static BatchDesign b78() {
        return new BatchDesign() {
            @Override public int batchNo() { return 78; }
            @Override public String title() { return "validateRows fail-fast on violating rows (commit throws IllegalStateException) × 10 × 10"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < 10; d++) {
                        final List<DataRow> data = injectInvalid(vf, FamilyB.ordProfile(d));
                        out.add(ScenarioDef.throwsOf(id("B78", v, d), "validateFail[v" + vf + "][ord" + d + "]",
                                java.lang.IllegalStateException.class,
                                "QUALITY_RULE_VIOLATION",
                                () -> {
                                    io.rowweave.examples.scenariolab.ScenarioLab lab =
                                            new io.rowweave.examples.scenariolab.ScenarioLab(
                                                    System.getProperty("java.io.tmpdir")
                                                            + "/scenariolab-vfail-" + java.util.UUID.randomUUID());
                                    DataSet res = lab.source(D.ORD, data).validateRows(validation(vf));
                                    lab.expect(res, data);
                                    lab.commitNow();  // expects the run to throw
                                }));
                    }
                }
                return out;
            }

            private List<DataRow> injectInvalid(int v, List<DataRow> in) {
                List<DataRow> out = new ArrayList<>(in);
                DataRow bad = new DataRow(4);
                switch (v) {
                    case 0: bad.set(0, 0L); bad.set(1, null); bad.set(2, D.bd("1")); bad.set(3, 1); break;
                    case 1: bad.set(0, null); bad.set(1, "x"); bad.set(2, D.bd("1")); bad.set(3, 1); break;
                    case 2: bad.set(0, 0L); bad.set(1, "x"); bad.set(2, D.bd("1")); bad.set(3, -5); break;
                    case 3: bad.set(0, 0L); bad.set(1, "x"); bad.set(2, D.bd("-1")); bad.set(3, 1); break;
                    case 4: bad.set(0, 0L); bad.set(1, "x"); bad.set(2, null); bad.set(3, 1); break;
                    case 5: bad.set(0, 0L); bad.set(1, "x"); bad.set(2, D.bd("99999")); bad.set(3, 1); break;
                    case 6: bad.set(0, null); bad.set(1, null); bad.set(2, D.bd("1")); bad.set(3, 1); break;
                    case 7: bad.set(0, 0L); bad.set(1, "x"); bad.set(2, D.bd("1")); bad.set(3, 0); break;
                    case 8: bad.set(0, 0L); bad.set(1, "x"); bad.set(2, D.bd("-9999")); bad.set(3, 1); break;
                    default: bad.set(0, 0L); bad.set(1, null); bad.set(2, null); bad.set(3, null); break;
                }
                out.add(bad);
                return out;
            }

            private RowValidation validation(int v) {
                RowValidation.Builder b = RowValidation.builder(D.ORD);
                switch (v) {
                    case 0: b.notNull("r0", "grp"); break;
                    case 1: b.notNull("r1", "id"); break;
                    case 2: b.integerRange("r2", "qty", 0, 100); break;
                    case 3: b.decimalRange("r3", "amt", new BigDecimal("0"), new BigDecimal("999999999")); break;
                    case 4: b.notNull("r4", "amt"); break;  // case 4 injects amt=null
                    case 5: b.decimalRange("r5", "amt", new BigDecimal("-999999999"), new BigDecimal("10000")); break;
                    case 6:
                        b.notNull("r6a", "grp");
                        b.notNull("r6b", "id");
                        break;
                    case 7: b.integerRange("r7", "qty", 1, Integer.MAX_VALUE); break;
                    case 8: b.decimalRange("r8", "amt", new BigDecimal("-1000"), new BigDecimal("999999999")); break;
                    default: b.notNull("rd", "grp"); break;
                }
                return b.build();
            }
        };
    }

    // ========================================================================
    // B79 unknown field rejected by filter, orderBy, groupBy and select, 10 by 10
    // ========================================================================

    private static BatchDesign b79() {
        return new BatchDesign() {
            @Override public int batchNo() { return 79; }
            @Override public String title() { return "fail-fast on unknown fields (filter/orderBy/groupBy/select/distinct/max) × 10 × 10"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                String[] unknownExprs = {
                        "nosuch > 1", "k = 'x' and ghost = 1", "abs(nofield) > 0",
                        "len(nosuchcol) = 1", "v > ?missing", "concat(k, nope) = 'x'",
                        "upper(zzz) = 'A'", "v + wraith > 5", "nofield is null", "v in (nope)"};
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        final int vfF = vf;
                        String[] messageFragments = {"nosuch", "ghost", "nofield", "nosuchcol",
                                "nosuch", "nosuch", "nosuch", "out of bounds", "nosuch", "nosuch"};
                        out.add(SL.fails(id("B79", v, d), "unknownField[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                D.KV, data, src -> applyBad79(vfF, unknownExprs, src), data,
                                IllegalArgumentException.class, messageFragments[vf]));
                    }
                }
                return out;
            }
        };
    }

    private static DataSet applyBad79(int vf, String[] unknownExprs, DataSet src) {
        switch (vf) {
            case 0: return src.filter(unknownExprs[0]);
            case 1: return src.filter(unknownExprs[1]);
            case 2: return src.filter(unknownExprs[2]);
            case 3: return src.filter(unknownExprs[3]);
            case 4: return src.orderBy("nosuch");
            case 5: return src.groupBy("nosuch").sum("v");
            case 6: return src.groupBy("k").sum("nosuch");
            case 7: return src.select(99);
            case 8: return src.distinct("nosuch");
            default: return src.max("nosuch");
        }
    }

    // ========================================================================
    // B80 a heterogeneous union, rejected at build or run time
    // ========================================================================

    private static BatchDesign b80() {
        return new BatchDesign() {
            @Override public int batchNo() { return 80; }
            @Override public String title() { return "union rejecting heterogeneous schemas (at build or run time) × 10 combinations × 10 profiles"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                DataSchema[] others = {D.K1, D.L2, D.DIM, D.ORD, D.CAT, D.K1, D.L2, D.DIM, D.ORD, D.CAT};
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        final DataSchema om = others[vf];
                        final List<DataRow> other = sample80(om);
                        out.add(ScenarioDef.of(id("B80", v, d), "unionMismatch[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet a = lab.source("A", D.KV, data);
                                    DataSet b = lab.source("B", om, other);
                                    try {
                                        a.union(b);
                                        throw new AssertionError("expected union schema mismatch");
                                    } catch (IllegalArgumentException expected) {
                                        ScenarioLab.assertExpectedFailure(expected,
                                                IllegalArgumentException.class,
                                                "union schema mismatch");
                                    }
                                    lab.expect(a, data);
                                }));
                    }
                }
                return out;
            }

            private List<DataRow> sample80(DataSchema meta) {
                if (meta.getFieldCount() == 1) {
                    return D.k1Set("x");
                }
                if (meta.getFieldCount() == 2) {
                    return D.rows(new Object[]{"k", D.bd("1")});
                }
                if (meta.getFieldCount() == 4) {
                    return D.rows(new Object[]{1L, "g", D.bd("1"), 1});
                }
                return D.rows(new Object[]{1L, "c", "s", D.bd("1"), true});
            }
        };
    }

    // ========================================================================
    // B81 a join key rejected at build time
    // ========================================================================

    private static BatchDesign b81() {
        return new BatchDesign() {
            @Override public int batchNo() { return 81; }
            @Override public String title() { return "fail-fast on join key count / unknown key / mismatched key types × 10 × 10"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> left = kvProfile(d);
                        final List<DataRow> right = io.rowweave.examples.scenariolab.batches.FamilyD.dimProfile(d);
                        out.add(ScenarioDef.of(id("B81", v, d), "joinBad[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet a = lab.source("A", D.KV, left);
                                    DataSet b = lab.source("B", D.DIM, right);
                                    try {
                                        switch (vf) {
                                            case 0: a.join(b).on(new String[]{"k"}, new String[]{"k", "label"}); break;
                                            case 1: a.join(b).on(new String[]{"nosuch"}, new String[]{"k"}); break;
                                            case 2: a.join(b).on(new String[]{"k"}, new String[]{"nosuch"}); break;
                                            case 3: a.join(b).on(new String[]{}, new String[]{}); break;
                                            case 4: a.join(b).on(new String[]{"k", "nosuch"}, new String[]{"k", "label"}); break;
                                            case 5: a.join(b).on(new String[]{"v"}, new String[]{"label"}); break;
                                            case 6: a.join(b).on(new String[]{"k", "v"}, new String[]{"k", "nosuch"}); break;
                                            case 7: a.rightJoin(b).on(new String[]{"nosuch"}, new String[]{"nosuch"}); break;
                                            case 8: a.fullJoin(b).on(new String[]{}, new String[]{"k"}); break;
                                            default: a.leftJoin(b).on(new String[]{"nosuch"}, new String[]{}); break;
                                        }
                                        throw new AssertionError("expected invalid join keys to be rejected");
                                    } catch (IllegalArgumentException expected) {
                                        String[] fragments = {"counts must match", "nosuch", "nosuch",
                                                "counts must match", "nosuch", "key type mismatch",
                                                "nosuch", "nosuch", "counts must match", "counts must match"};
                                        ScenarioLab.assertExpectedFailure(expected,
                                                IllegalArgumentException.class, fragments[vf]);
                                    }
                                    lab.expect(a.join(b).on("k"),
                                            O.joinRows(left, right, O.idx(0), O.idx(0),
                                                    io.rowweave.ops.join.JoinType.INNER, 2, 2));
                                }));
                    }
                }
                return out;
            }
        };
    }

    private static BatchDesign b82() {
        return new BatchDesign() {
            @Override public int batchNo() { return 82; }
            @Override public String title() { return "API misuse (empty key / illegal argument / output without a session, etc.) × 10 × 10"; }
            @Override public List<ScenarioDef> scenarios() {
                List<ScenarioDef> out = new ArrayList<>();
                for (int v = 0; v < 10; v++) {
                    final int vf = v;
                    for (int d = 0; d < KV_PROFILES.length; d++) {
                        final List<DataRow> data = kvProfile(d);
                        out.add(ScenarioDef.of(id("B82", v, d), "apiMisuse[v" + vf + "]["
                                        + KV_PROFILES[d] + "]",
                                lab -> {
                                    DataSet src = lab.source(D.KV, data);
                                    try {
                                        switch (vf) {
                                            case 0: src.groupBy(); break;
                                            case 1: src.partitionByHash(); break;
                                            case 2: src.orderBy(); break;
                                            case 3: src.top(-1); break;
                                            case 4: src.addFields(null, null); break;
                                            case 5: src.select(new int[]{}); break;
                                            case 6: src.select(new String[]{}); break;
                                            case 7: src.reduceGroup(new GroupReduceFunction() {
                                                @Override
                                                public DataSchema getResultSchema() {
                                                    return null;
                                                }

                                                @Override
                                                public void reduce(Iterable<DataRow> rows,
                                                                   io.rowweave.Collector out) {
                                                }
                                            }); break;
                                            case 8: src.top(O.top(D.KV, -5, O.sf(D.KV, "v", true, "DEFAULT")), 1); break;
                                            default: src.count("nosuch"); break;
                                        }
                                        throw new AssertionError(
                                                "expected API misuse failure but build succeeded");
                                    } catch (IllegalArgumentException expected) {
                                        String[] fragments = {"groupBy requires", "partitionByHash requires",
                                                "OrderBy requires", "n must be", "fields and values",
                                                "select requires", "select requires", "must not be null",
                                                "n must be", "unknown field"};
                                        ScenarioLab.assertExpectedFailure(expected,
                                                IllegalArgumentException.class, fragments[vf]);
                                    } catch (RuntimeException wrongType) {
                                        throw new AssertionError(
                                                "expected IllegalArgumentException but got "
                                                        + wrongType.getClass().getSimpleName(), wrongType);
                                    }
                                    lab.expect(src, data);
                                }));
                    }
                }
                return out;
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
