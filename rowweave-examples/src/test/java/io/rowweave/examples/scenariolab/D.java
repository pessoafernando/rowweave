package io.rowweave.examples.scenariolab;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared data profiles (D): each profile is a small table with fixed content, 0 to 40 rows.
 * They cover null density, duplicate rows, negatives, magnitude, scale, unicode, whitespace,
 * booleans, an empty table and a single row. The volume is deliberately small, because these
 * scenarios aim at functional coverage rather than performance.
 */
public final class D {

    /** [k:String, v:BigDecimal]: the main table for grouping and aggregation. */
    public static final DataSchema KV = meta(
            f("k", DataType.StringType), f("v", DataType.BigDecimalType));

    /** [k:String, a:Long, v:BigDecimal]: adds a second numeric column. */
    public static final DataSchema KAV = meta(
            f("k", DataType.StringType), f("a", DataType.LongType), f("v", DataType.BigDecimalType));

    /** [id:Long, grp:String, amt:BigDecimal, qty:Integer]: an order-shaped table. */
    public static final DataSchema ORD = meta(
            f("id", DataType.LongType), f("grp", DataType.StringType),
            f("amt", DataType.BigDecimalType), f("qty", DataType.IntegerType));

    /** [id:Long, cat:String, sub:String, amt:BigDecimal, active:Boolean]: a table with two dimensions. */
    public static final DataSchema CAT = meta(
            f("id", DataType.LongType), f("cat", DataType.StringType), f("sub", DataType.StringType),
            f("amt", DataType.BigDecimalType), f("active", DataType.BooleanType));

    /** [k:String, label:String]: the lookup table used on the right of a join. */
    public static final DataSchema DIM = meta(
            f("k", DataType.StringType), f("label", DataType.StringType));

    /** [k:String]: a single string column, for union and distinct. */
    public static final DataSchema K1 = meta(f("k", DataType.StringType));

    /** [a:Long, b:Long]: two integer columns. */
    public static final DataSchema L2 = meta(f("a", DataType.LongType), f("b", DataType.LongType));

    public static Field f(String name, DataType t) {
        return new Field(name, t);
    }

    public static DataSchema meta(Field... fields) {
        return new DataSchema(fields);
    }

    public static DataRow row(Object... vals) {
        DataRow r = new DataRow(vals.length);
        for (int i = 0; i < vals.length; i++) {
            r.set(i, vals[i]);
        }
        return r;
    }

    public static BigDecimal bd(String s) {
        return s == null ? null : new BigDecimal(s);
    }

    public static List<DataRow> rows(Object[]... vals) {
        List<DataRow> out = new ArrayList<>(vals.length);
        for (Object[] v : vals) {
            out.add(row(v));
        }
        return out;
    }

    // ---- Named data profiles ----

    /** 12 ordinary rows: 2 to 3 groups, no null. */
    public static List<DataRow> kvBase() {
        return rows(
                new Object[]{"g1", bd("10.5")}, new Object[]{"g1", bd("20.25")},
                new Object[]{"g1", bd("3.125")}, new Object[]{"g2", bd("100")},
                new Object[]{"g2", bd("0.001")}, new Object[]{"g2", bd("7")},
                new Object[]{"g3", bd("42")}, new Object[]{"g3", bd("-8.4")},
                new Object[]{"g3", bd("0")}, new Object[]{"g4", bd("999.999999")},
                new Object[]{"g4", bd("0.000001")}, new Object[]{"g4", null});
    }

    /** null dense: k and v both carry null. */
    public static List<DataRow> kvNulls() {
        return rows(
                new Object[]{"g1", bd("1")}, new Object[]{null, bd("2")},
                new Object[]{"g1", null}, new Object[]{null, null},
                new Object[]{"g2", bd("3")}, new Object[]{null, bd("4")},
                new Object[]{"g2", null}, new Object[]{"g1", bd("5")});
    }

    /** Many duplicate rows. */
    public static List<DataRow> kvDupes() {
        List<DataRow> out = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            out.add(row("d1", bd("10")));
            out.add(row("d2", bd("20")));
        }
        out.add(row("d1", bd("10")));
        return out;
    }

    /** Empty table. */
    public static List<DataRow> empty() {
        return new ArrayList<>();
    }

    /** Single row. */
    public static List<DataRow> single() {
        return rows(new Object[]{"only", bd("1.5")});
    }

    /** Negatives and zero. */
    public static List<DataRow> kvNegative() {
        return rows(
                new Object[]{"n", bd("-1.005")}, new Object[]{"n", bd("-2")},
                new Object[]{"n", bd("0")}, new Object[]{"n", bd("-0.0000000001")},
                new Object[]{"p", bd("2.005")}, new Object[]{"p", bd("-2.005")});
    }

    /** Extreme scale plus HALF_UP boundaries. */
    public static List<DataRow> kvScale() {
        return rows(
                new Object[]{"s", bd("0.0000000005")},  // scale 10 HALF_UP on 0.0000000005, which sits exactly on the rounding boundary
                new Object[]{"s", bd("0.0000000015")},
                new Object[]{"s", bd("1.0000000005")},
                new Object[]{"s", bd("2.5")},
                new Object[]{"s", bd("3.5")},
                new Object[]{"t", bd("0.00000000004")},
                new Object[]{"t", bd("999999999999.9999999999")});
    }

    /** unicode with mixed-case keys. */
    public static List<DataRow> kvUnicode() {
        return rows(
                new Object[]{"α", bd("1")}, new Object[]{"β", bd("2")},
                new Object[]{"A", bd("3")}, new Object[]{"a", bd("4")},
                new Object[]{"Ω", bd("5")}, new Object[]{"ω", bd("6")},
                new Object[]{"ß", bd("7")}, new Object[]{"ss", bd("8")},
                new Object[]{"Ä", bd("9")}, new Object[]{"ä", bd("10")});
    }

    /** Whitespace string keys. */
    public static List<DataRow> kvWhitespace() {
        return rows(
                new Object[]{"", bd("1")}, new Object[]{" ", bd("2")},
                new Object[]{"  ", bd("3")}, new Object[]{"a ", bd("4")},
                new Object[]{" a", bd("5")}, new Object[]{"", bd("6")},
                new Object[]{null, bd("7")}, new Object[]{"\t", bd("8")});
    }

    /** Many small groups (20 groups of 2 rows). */
    public static List<DataRow> kvManyGroups() {
        List<DataRow> out = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            out.add(row("k" + i, bd(String.valueOf(i))));
            out.add(row("k" + i, bd(String.valueOf(i * 10))));
        }
        return out;
    }

    /** Ordinary order-shaped data (with null qty). */
    public static List<DataRow> ordBase() {
        return rows(
                new Object[]{1L, "dev", bd("100.10"), 2},
                new Object[]{2L, "dev", bd("200.20"), 1},
                new Object[]{3L, "ops", bd("50.50"), null},
                new Object[]{4L, "ops", bd("70"), 3},
                new Object[]{5L, "rnd", bd("999.99"), 10},
                new Object[]{6L, null, bd("12.34"), 1},
                new Object[]{7L, "dev", null, 5},
                new Object[]{8L, "rnd", bd("0.01"), null});
    }

    /** Two-dimension table (cat/sub). */
    public static List<DataRow> catBase() {
        return rows(
                new Object[]{1L, "a", "x", bd("10"), true},
                new Object[]{2L, "a", "y", bd("20"), false},
                new Object[]{3L, "b", "x", bd("30"), true},
                new Object[]{4L, "b", "x", bd("40"), null},
                new Object[]{5L, null, "z", bd("50"), true},
                new Object[]{6L, "a", null, bd("60"), false});
    }

    /** Two integer columns (ordering and comparison). */
    public static List<DataRow> l2Sort() {
        return rows(
                new Object[]{5L, 1L}, new Object[]{null, 2L}, new Object[]{1L, null},
                new Object[]{3L, 3L}, new Object[]{null, null}, new Object[]{0L, 0L},
                new Object[]{-7L, 2L});
    }

    /** A single string column (union and distinct). */
    public static List<DataRow> k1Set(String... vals) {
        List<DataRow> out = new ArrayList<>();
        for (String v : vals) {
            out.add(row((Object) v));
        }
        return out;
    }

    /** Lookup side of a join (right-hand input, with duplicate keys and null keys). */
    public static List<DataRow> dimBase() {
        return rows(
                new Object[]{"g1", "group-one"}, new Object[]{"g2", "group-two"},
                new Object[]{"g1", "group-one-alt"}, new Object[]{null, "null-group"},
                new Object[]{"g9", "orphan-group"});
    }
}
