package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.expr.CompiledExpr;
import io.rowweave.expr.Expr;
import io.rowweave.expr.ExprEngineFactory;
import io.rowweave.expr.ExprParser;
import io.rowweave.expr.ExprSchema;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.function.Function;

/**
 * The projection operator (pass-through, purely streaming). A 1-to-1 mapping that changes no row count. Immutable: each row builds a new {@link DataRow} and leaves the input alone.
 *
 * <p>Two constructors: {@link #byIndex} prunes or reorders columns by source column index, and {@link #of} is an expression projection over any computed column,
 * evaluated by the expr engine. Both normalize into a projection function {@code DataRow to DataRow} plus an output {@link DataSchema}.
 *
 * <p>The expression form takes {@code expr [as alias]} per column; the output column name is the alias, then a bare field name, then exprN. A bare field reference keeps the source
 * field type, while a computed column derives a concrete type from the AST and the function signature. The three-valued null behaviour is pinned by ExprRuntime.
 *
 * <p>The pure Push contract: pull one row at a time from {@link RowStream} and push to {@link RowSink}, emitting as it evaluates, and never return or materialize a List.
 */
public final class Select {

    private final DataSchema outMeta;
    private final Function<DataRow, DataRow> projection;

    private Select(DataSchema outMeta, Function<DataRow, DataRow> projection) {
        this.outMeta = outMeta;
        this.projection = projection;
    }

    /** Project by source column index: pick source fields (reordered or repeated); the output DataSchema names and types carry over from them. */
    public static Select byIndex(DataSchema inMeta, int... indices) {
        if (indices == null || indices.length == 0) {
            throw new IllegalArgumentException("select requires at least one column index");
        }
        int fieldCount = inMeta.getFieldCount();
        Field[] outFields = new Field[indices.length];
        final int[] idx = indices.clone();
        for (int i = 0; i < idx.length; i++) {
            if (idx[i] < 0 || idx[i] >= fieldCount) {
                throw new IllegalArgumentException("column index out of bounds: " + idx[i]
                        + " (field count " + fieldCount + ")");
            }
            outFields[i] = inMeta.getField(idx[i]);
        }
        Function<DataRow, DataRow> proj = in -> {
            DataRow out = new DataRow(idx.length);
            for (int i = 0; i < idx.length; i++) {
                out.set(i, in.get(idx[i]));
            }
            return out;
        };
        return new Select(new DataSchema(outFields), proj);
    }

    /**
     * Expression projection: one expression per column (optionally with {@code as alias}). The expr engine compiles them into {@link CompiledExpr}[],
     * and each row is evaluated into a new row. {@code ==} is normalized to {@code =}, since the expression syntax uses a single =.
     */
    public static Select of(DataSchema inMeta, String... expressions) {
        if (expressions == null || expressions.length == 0) {
            throw new IllegalArgumentException("select requires at least one expression");
        }
        Field[] outFields = new Field[expressions.length];
        final CompiledExpr[] compiled = new CompiledExpr[expressions.length];
        // A bare field reference (possibly aliased) reads straight from its source column index, which
        // **preserves the original type** (Integer/Long/...). It does not go through the expression engine,
        // because the engine widens Integer/Long to BigDecimal for arithmetic, which would make the output
        // value type disagree with the declared meta and produce a ClassCastException when a downstream
        // groupBy or join key is serialized. Only non-bare expressions (arithmetic or function calls) are
        // compiled and run through the engine.
        final int[] passThroughIdx = new int[expressions.length]; // >=0 is the source column index of a bare field; -1 means it goes through the compiled expression
        for (int i = 0; i < expressions.length; i++) {
            Expr parsed = ExprParser.parse(expressions[i].replace("==", "="));
            Expr inner = unwrapAlias(parsed);
            outFields[i] = new Field(deriveName(parsed, i), deriveType(parsed, inMeta));
            if (inner instanceof Expr.FieldRef) {
                int idx = inMeta.getFieldIndex(((Expr.FieldRef) inner).name);
                if (idx >= 0) {
                    passThroughIdx[i] = idx; // bare field: read the original value so the type is kept
                    continue;
                }
            }
            passThroughIdx[i] = -1;
            compiled[i] = ExprEngineFactory.getDefault().compile(inMeta, inner);
        }
        Function<DataRow, DataRow> proj = in -> {
            DataRow out = new DataRow(expressions.length);
            for (int i = 0; i < expressions.length; i++) {
                Object value = passThroughIdx[i] >= 0
                        ? in.get(passThroughIdx[i]) : compiled[i].eval(in);
                out.set(i, normalizeForDeclaredType(value, outFields[i].getDataType()));
            }
            return out;
        };
        return new Select(new DataSchema(outFields), proj);
    }

    public DataSchema getOutputSchema() {
        return outMeta;
    }

    /** Project one row (called row by row by operator chaining). */
    public DataRow project(DataRow in) {
        return projection.apply(in);
    }

    /**
     * Streaming projection: each RECORD is projected and then emitted; control events (Barrier/Error/Cancel) pass through downstream,
     * after which processing stops; once upstream is exhausted, endOfStream is emitted.
     */
    public void apply(RowStream in, RowSink out) {
        StreamEvent e;
        while ((e = in.next()) != null) {
            if (!e.isRecord()) {
                out.emit(e); // pass Barrier/Error/Cancel through
                if (e.isError() || e.isCancel()) {
                    return;
                }
                continue;
            }
            out.emitRow(project(e.row()));
        }
        out.endOfStream();
    }

    // ---- output column naming and types for the expression form ----

    private static Expr unwrapAlias(Expr e) {
        return (e instanceof Expr.Aliased) ? ((Expr.Aliased) e).expr : e;
    }

    private static String deriveName(Expr e, int index) {
        if (e instanceof Expr.Aliased) {
            return ((Expr.Aliased) e).alias;
        }
        if (e instanceof Expr.FieldRef) {
            return ((Expr.FieldRef) e).name;
        }
        return "expr" + index;
    }

    private static DataType deriveType(Expr e, DataSchema inMeta) {
        Expr inner = unwrapAlias(e);
        if (inner instanceof Expr.FieldRef) {
            int idx = inMeta.getFieldIndex(((Expr.FieldRef) inner).name);
            if (idx >= 0) {
                return inMeta.getField(idx).getDataType();
            }
        }
        ExprSchema.validateReferences(inMeta, inner);
        return ExprSchema.inferComputedType(inMeta, inner);
    }

    private static Object normalizeForDeclaredType(Object value, DataType type) {
        if (value == null) return null;
        if (type == DataType.DateType && value instanceof java.time.LocalDate) {
            return java.sql.Date.valueOf((java.time.LocalDate) value);
        }
        if (type == DataType.TimestampType && value instanceof java.time.LocalDateTime) {
            return java.sql.Timestamp.valueOf((java.time.LocalDateTime) value);
        }
        if (type == DataType.TimestampType && value instanceof java.time.LocalDate) {
            return java.sql.Timestamp.valueOf(((java.time.LocalDate) value).atStartOfDay());
        }
        return value;
    }
}
