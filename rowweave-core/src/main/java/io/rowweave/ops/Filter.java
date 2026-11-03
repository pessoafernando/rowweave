package io.rowweave.ops;

import io.rowweave.FilterFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.expr.CompiledExpr;
import io.rowweave.expr.ExprEngineFactory;
import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.Map;

/**
 * The filter operator (pass-through, purely streaming): a row is kept when the predicate returns true.
 *
 * <p>Two forms: {@link #of} takes a code predicate (FilterFunction), and {@link #fromExpression} takes a string expression evaluated by the expr engine -
 * any boolean expression (arithmetic, comparison, logic, functions, case, in, like, cast). When a predicate meets null or unknown, the row <b>does not pass</b> (SQL three-valued logic, and in a filter
 * unknown maps to false, which CompiledExpr.test pins down). {@code ==} is normalized to {@code =}, since the expression syntax uses a single =.
 *
 * <p>The pure Push contract: pull one row at a time from {@link RowStream} and push to {@link RowSink}, emitting as it evaluates, and never return or materialize a List.
 */
public final class Filter {

    private final FilterFunction predicate;

    private Filter(FilterFunction predicate) {
        this.predicate = predicate;
    }

    public static Filter of(FilterFunction predicate) {
        return new Filter(predicate);
    }

    /** A string expression predicate: evaluated by the expr engine, with unknown mapped to false (the filter's three-valued semantics). */
    public static Filter fromExpression(DataSchema dataSchema, String expression) {
        CompiledExpr compiled = ExprEngineFactory.getDefault().compile(dataSchema, expression.replace("==", "="));
        return new Filter(compiled::test);
    }

    /** The named-parameter form (fields take priority: a bare identifier resolves to a field first, and only then to params). */
    public static Filter fromExpression(DataSchema dataSchema, String expression, Map<String, Object> params) {
        CompiledExpr compiled = ExprEngineFactory.getDefault()
                .compile(dataSchema, expression.replace("==", "="), params);
        return new Filter(compiled::test);
    }

    /** Evaluate the predicate on one row (called row by row by operator chaining). */
    public boolean test(DataRow row) {
        return predicate.filter(row);
    }

    /**
     * Streaming filter: a RECORD is emitted as soon as the predicate matches, and control events (Barrier/Error/Cancel) pass through downstream,
     * after which processing stops; once upstream is exhausted, endOfStream is emitted.
     */
    public void apply(RowStream in, RowSink out) {
        StreamEvent e;
        while ((e = in.next()) != null) {
            if (!e.isRecord()) {
                out.emit(e); // pass control events such as Barrier/Error/Cancel through
                if (e.isError() || e.isCancel()) {
                    return;
                }
                continue;
            }
            if (predicate.filter(e.row())) {
                out.emitRow(e.row());
            }
        }
        out.endOfStream();
    }
}
