package io.rowweave.expr;

import io.rowweave.DataRow;

/**
 * A compiled, reusable expression handle: compile once, then evaluate or test the predicate across many
 * rows, reused within a thread.
 *
 * <p>Produced by {@link ExprEngine#compile}. The two engines, the interpreter and the dynamic compiler, must
 * return <b>bit-identical</b> results for the same expression and the same input.
 */
public interface CompiledExpr {

    /** Evaluate and return the value object: BigDecimal, String, Boolean, a date or null. */
    Object eval(DataRow row);

    /** Evaluate with positional ? parameters. */
    Object eval(DataRow row, Object[] params);

    /** Evaluate as a predicate: unknown, that is null, means the row does not pass a filter. */
    boolean test(DataRow row);

    /** Evaluate as a predicate with positional ? parameters. */
    boolean test(DataRow row, Object[] params);
}
