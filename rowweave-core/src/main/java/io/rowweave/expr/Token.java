package io.rowweave.expr;

/** One lexer token: a type and its text. */
final class Token {

    enum Type {
        INTEGER, DECIMAL, STRING, IDENT, KEYWORD, OP, LPAREN, RPAREN, COMMA, PARAM, EOF
    }

    final Type type;
    final String text;

    Token(Type type, String text) {
        this.type = type;
        this.text = text;
    }

    boolean is(Type t) {
        return type == t;
    }

    boolean isOp(String s) {
        return type == Type.OP && text.equals(s);
    }

    boolean isKeyword(String kw) {
        return type == Type.KEYWORD && text.equals(kw);
    }

    @Override
    public String toString() {
        return type + "('" + text + "')";
    }
}
