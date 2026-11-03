package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Appends constant columns (a pass-through, pure Push): per row it appends k constant columns at the end, so
 * the output is [source columns..., constant columns...].
 *
 * <p><b>Describable</b>: each constant value is encoded through {@link #encodeValue}/{@link #decodeValue},
 * sharing {@link io.rowweave.FileSink}'s self-describing text encoding with a bit-identical round trip
 * (BigDecimal keeps scale, Timestamp keeps nanos, null becomes \N), and goes into the PlanNode's three
 * parallel listParams (addNames/addTypes/addValues), so a cluster dispatch can rebuild it. **Only scalar
 * types that round-trip through text are supported** (String, BigDecimal, Boolean, Integer, Long, Date,
 * Timestamp); anything else fails fast, and these cover constant columns.
 *
 * <p>Pass-through, not a breaker: it emits row by row, can take part in operator chaining, and its meaning
 * does not depend on partitioning, so a single instance is enough.
 */
public final class AddFields {

    private static final String NULL_TOKEN = "\\N";

    private final DataSchema inMeta;
    private final Field[] addFields;
    private final Object[] addValues;

    private AddFields(DataSchema inMeta, Field[] addFields, Object[] addValues) {
        this.inMeta = inMeta;
        this.addFields = addFields;
        this.addValues = addValues;
    }

    public static AddFields of(DataSchema inMeta, Field[] fields, Object[] values) {
        if (fields == null || values == null || fields.length != values.length) {
            throw new IllegalArgumentException("addFields: fields and values must be non-null and equal length");
        }
        if (fields.length == 0) {
            throw new IllegalArgumentException("addFields requires at least one field");
        }
        return new AddFields(inMeta, fields.clone(), values.clone());
    }

    public DataSchema getOutputSchema() {
        int old = inMeta.getFieldCount();
        Field[] out = new Field[old + addFields.length];
        for (int i = 0; i < old; i++) {
            out[i] = inMeta.getField(i);
        }
        System.arraycopy(addFields, 0, out, old, addFields.length);
        return new DataSchema(out);
    }

    /** The only outlet (the pure Push contract): append the constant columns row by row and emit; an Error or Cancel control event passes through, and EOS closes the stream. */
    public void apply(RowStream in, RowSink out) {
        int old = inMeta.getFieldCount();
        int total = old + addFields.length;
        StreamEvent e;
        while ((e = in.next()) != null) {
            if (!e.isRecord()) {
                if (e.isError() || e.isCancel()) {
                    out.emit(e);
                    return;
                }
                continue;
            }
            DataRow r = e.row();
            DataRow o = new DataRow(total);
            for (int i = 0; i < old; i++) {
                o.set(i, r.get(i));
            }
            for (int i = 0; i < addValues.length; i++) {
                o.set(old + i, addValues[i]);
            }
            out.emitRow(o);
        }
        out.endOfStream();
    }

    // ---- describable getters (used by DataSet to build a PlanNode) ----

    public java.util.List<String> addFieldNames() {
        java.util.List<String> names = new java.util.ArrayList<>(addFields.length);
        for (Field f : addFields) {
            names.add(f.getName());
        }
        return names;
    }

    public java.util.List<String> addFieldTypes() {
        java.util.List<String> types = new java.util.ArrayList<>(addFields.length);
        for (Field f : addFields) {
            types.add(f.getDataType().name());
        }
        return types;
    }

    public java.util.List<String> addFieldValues() {
        java.util.List<String> vals = new java.util.ArrayList<>(addValues.length);
        for (int i = 0; i < addValues.length; i++) {
            vals.add(encodeValue(addFields[i].getDataType(), addValues[i]));
        }
        return vals;
    }

    /** Rebuilds AddFields from the three parallel describable listParams (on a cluster Worker). */
    public static AddFields fromDescribable(DataSchema inMeta, java.util.List<String> names,
                                            java.util.List<String> types, java.util.List<String> values) {
        Field[] fields = new Field[names.size()];
        Object[] vals = new Object[names.size()];
        for (int i = 0; i < names.size(); i++) {
            DataType type = DataType.valueOf(types.get(i));
            fields[i] = new Field(names.get(i), type);
            vals[i] = decodeValue(type, values.get(i));
        }
        return new AddFields(inMeta, fields, vals);
    }

    /** A constant value to self-describing text (the same rules as FileSink.encodeCell, so the round trip is bit-identical). */
    public static String encodeValue(DataType type, Object v) {
        if (v == null) {
            return NULL_TOKEN;
        }
        switch (type) {
            case StringType:   return escape((String) v);
            case BigDecimalType: return ((BigDecimal) v).toPlainString();
            case BooleanType:  return ((Boolean) v) ? "true" : "false";
            case IntegerType:  return Integer.toString((Integer) v);
            case LongType:     return Long.toString((Long) v);
            case TimestampType: {
                Timestamp ts = (Timestamp) v;
                return ts.getTime() + ":" + ts.getNanos();
            }
            case DateType:     return Long.toString(((Date) v).getTime());
            default:
                throw new IllegalArgumentException("addFields does not support constant type: " + type);
        }
    }

    /** Self-describing text to a constant value (the same rules as FileSink.decodeCell). */
    public static Object decodeValue(DataType type, String token) {
        if (NULL_TOKEN.equals(token)) {
            return null;
        }
        switch (type) {
            case StringType:   return unescape(token);
            case BigDecimalType: return new BigDecimal(token);
            case BooleanType:  return Boolean.parseBoolean(token);
            case IntegerType:  return Integer.parseInt(token);
            case LongType:     return Long.parseLong(token);
            case TimestampType: {
                int sep = token.indexOf(':');
                long millis = Long.parseLong(token.substring(0, sep));
                int nanos = Integer.parseInt(token.substring(sep + 1));
                Timestamp ts = new Timestamp(millis);
                ts.setNanos(nanos);
                return ts;
            }
            case DateType:     return new Date(Long.parseLong(token));
            default:
                throw new IllegalArgumentException("addFields does not support constant type: " + type);
        }
    }

    private static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '\t': sb.append("\\t"); break;
                case '\n': sb.append("\\n"); break;
                default: sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String unescape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                switch (n) {
                    case '\\': sb.append('\\'); break;
                    case 't': sb.append('\t'); break;
                    case 'n': sb.append('\n'); break;
                    default: sb.append(n);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
