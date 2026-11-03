package io.rowweave.expr.codegen;

/**
 * Thrown when codegen does not cover a node or construct. {@code CompiledExprEngine} catches it and falls back to the interpreter.
 * Not an error: it is the normal dispatch signal between a compiled subset and an interpreting backstop.
 */
public final class UnsupportedExprException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    UnsupportedExprException(String message) {
        super(message);
    }
}
