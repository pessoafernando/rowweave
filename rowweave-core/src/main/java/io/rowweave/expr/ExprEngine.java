package io.rowweave.expr;

import io.rowweave.DataSchema;

/**
 * Expression engine abstraction: compiles an expression string plus a {@link DataSchema} into a reusable {@link CompiledExpr}.
 *
 * <p>Seam for swapping engines: callers such as Filter and Select depend on this interface only and do not know which engine backs them. Two implementations:
 * <ul>
 * <li>{@link InterpretedExprEngine}, an interpreter.</li>
 * <li>{@link CompiledExprEngine}, which compiles at run time.</li>
 * </ul>
 *
 * <p><b>Constraints on a compiled engine:</b>
 * <ul>
 * <li>The compilation cache key is the expression string plus the {@link DataSchema} field layout, and the same key reuses the compiled artifact.</li>
 * <li>The ClassLoader of a generated class must be <b>unloadable</b> so that Metaspace is not leaked: each expression or group of expressions gets its own loader,
 * reclaimable by the GC once the handle is released.</li>
 * <li>Results are <b>byte-identical</b> to the interpreted engine on the same input, including BigDecimal scale and rounding and the three-valued null semantics.</li>
 * </ul>
 */
public interface ExprEngine {

    /** Compile an expression string into a handle reusable across rows. An invalid expression throws {@link ExprParseException}. */
    CompiledExpr compile(DataSchema meta, String expression);

    /** Compile an AST that has already been parsed, so a caller can reuse its parse result. */
    CompiledExpr compile(DataSchema meta, Expr expression);

    /**
     * Compile with named parameters, resolving a bare identifier field-first: the field is looked up first and only an
     * absent field falls back to {@code namedParams}. The default implementation degrades to plain compile when there are no
     * parameters; an engine that supports them implements this, keeping field-first resolution and byte-identical results with the interpreter.
     */
    default CompiledExpr compile(DataSchema meta, String expression, java.util.Map<String, Object> namedParams) {
        if (namedParams == null || namedParams.isEmpty()) {
            return compile(meta, expression);
        }
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support named parameters");
    }
}
