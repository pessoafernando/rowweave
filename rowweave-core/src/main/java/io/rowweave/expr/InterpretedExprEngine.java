package io.rowweave.expr;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;

/**
 * Interpreted expression engine: its {@link CompiledExpr} handles are backed by {@link ExprInterpreter} and reused across rows.
 *
 * <p>Compile time resolves field names to indices and builds the AST, so evaluation matches no strings. Thread-safe: a compiled handle is
 * immutable and holds no state across rows, so several threads may share it while each passes its own DataRow and parameters.
 */
public final class InterpretedExprEngine implements ExprEngine {

    @Override
    public CompiledExpr compile(DataSchema meta, String expression) {
        return new InterpretedCompiledExpr(ExprInterpreter.compile(meta, expression));
    }

    @Override
    public CompiledExpr compile(DataSchema meta, Expr expression) {
        return new InterpretedCompiledExpr(ExprInterpreter.compile(meta, expression));
    }

    @Override
    public CompiledExpr compile(DataSchema meta, String expression, java.util.Map<String, Object> namedParams) {
        return new InterpretedCompiledExpr(ExprInterpreter.compile(meta, expression, namedParams));
    }

    /** Wraps an {@link ExprInterpreter} as a reusable handle. */
    private static final class InterpretedCompiledExpr implements CompiledExpr {
        private final ExprInterpreter interp;

        InterpretedCompiledExpr(ExprInterpreter interp) {
            this.interp = interp;
        }

        @Override
        public Object eval(DataRow row) {
            return interp.eval(row);
        }

        @Override
        public Object eval(DataRow row, Object[] params) {
            return interp.eval(row, params);
        }

        @Override
        public boolean test(DataRow row) {
            return interp.test(row);
        }

        @Override
        public boolean test(DataRow row, Object[] params) {
            return interp.test(row, params);
        }
    }
}
