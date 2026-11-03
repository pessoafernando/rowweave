package io.rowweave.expr;

/** Raised when evaluating a well-formed expression fails. */
public final class ExprEvalException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ExprEvalException(String message) {
        super(message);
    }
}
