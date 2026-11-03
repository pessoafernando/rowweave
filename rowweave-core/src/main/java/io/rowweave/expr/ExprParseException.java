package io.rowweave.expr;

/** Raised when a lexer or parser rejects an expression string. */
public final class ExprParseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ExprParseException(String message) {
        super(message);
    }
}
