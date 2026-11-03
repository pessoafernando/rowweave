package io.rowweave.expr;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Recursive-descent expression parser. Precedence from low to high:
 * alias > or > xor > and > not > comparison (= <> != < <= > >=) > add/sub > mul/div/mod > unary(-) > primary.
 *
 * <p>Covers literals, macros, fields and parameters, the four arithmetic operations with parentheses, comparison, logic (and/or/xor/not) and aliases whether written with as or bare,
 * plus Case-When, in, like, is null, cast and function calls. Each form is exercised by ExprStructuresTest through parseComparison and parsePrimary.
 */
public final class ExprParser {

    private final List<Token> tokens;
    private int pos;
    private int paramCount;

    private ExprParser(List<Token> tokens) {
        this.tokens = tokens;
    }

    public static Expr parse(String src) {
        if (src == null || src.trim().isEmpty()) {
            throw new ExprParseException("empty expression");
        }
        ExprParser p = new ExprParser(new Lexer(src).tokenize());
        Expr e = p.parseAliased();
        if (!p.peek().is(Token.Type.EOF)) {
            throw new ExprParseException("trailing tokens after expression: " + p.peek());
        }
        return e;
    }

    // alias: xorExpr (AS? alias)?
    private Expr parseAliased() {
        Expr e = parseXor();
        if (peek().isKeyword("as")) {
            advance();
            String alias = expectIdent("alias after 'as'");
            return new Expr.Aliased(e, alias);
        }
        // bare alias: an identifier directly following a complete expression
        if (peek().is(Token.Type.IDENT)) {
            String alias = advance().text;
            return new Expr.Aliased(e, alias);
        }
        return e;
    }

    private Expr parseXor() {
        Expr left = parseOr();
        while (peek().isKeyword("xor")) {
            advance();
            left = new Expr.BinaryOp("xor", left, parseOr());
        }
        return left;
    }

    private Expr parseOr() {
        Expr left = parseAnd();
        while (peek().isKeyword("or")) {
            advance();
            left = new Expr.BinaryOp("or", left, parseAnd());
        }
        return left;
    }

    private Expr parseAnd() {
        Expr left = parseNot();
        while (peek().isKeyword("and")) {
            advance();
            left = new Expr.BinaryOp("and", left, parseNot());
        }
        return left;
    }

    private Expr parseNot() {
        if (peek().isKeyword("not")) {
            advance();
            return new Expr.UnaryOp("not", parseNot());
        }
        return parseComparison();
    }

    private Expr parseComparison() {
        Expr left = parseAddSub();
        while (true) {
            Token t = peek();
            if (t.is(Token.Type.OP) && isComparisonOp(t.text)) {
                String op = advance().text;
                left = new Expr.BinaryOp(op, left, parseAddSub());
            } else if (t.isKeyword("is")) {
                left = parseIsNull(left);
            } else if (t.isKeyword("like")) {
                advance();
                left = new Expr.Like(left, parseAddSub());
            } else if (t.isKeyword("in")) {
                left = parseIn(left, false);
            } else if (t.isKeyword("not")
                    && (peekAt(1).isKeyword("in") || peekAt(1).isKeyword("like"))) {
                advance(); // 'not'
                if (peek().isKeyword("like")) {
                    advance();
                    left = new Expr.UnaryOp("not", new Expr.Like(left, parseAddSub()));
                } else {
                    left = parseIn(left, true);
                }
            } else {
                break;
            }
        }
        return left;
    }

    // is [not] null
    private Expr parseIsNull(Expr left) {
        advance(); // 'is'
        boolean negated = false;
        if (peek().isKeyword("not")) {
            advance();
            negated = true;
        }
        if (!peek().isKeyword("null")) {
            throw new ExprParseException("expected 'null' after 'is" + (negated ? " not'" : "'")
                    + " but found " + peek());
        }
        advance(); // 'null'
        return new Expr.IsNull(left, negated);
    }

    // [not] in (item, item, ...)
    private Expr parseIn(Expr left, boolean negated) {
        advance(); // 'in'
        expect(Token.Type.LPAREN, "'(' after 'in'");
        List<Expr> items = new ArrayList<>();
        if (!peek().is(Token.Type.RPAREN)) {
            items.add(parseXor());
            while (peek().is(Token.Type.COMMA)) {
                advance();
                items.add(parseXor());
            }
        }
        expect(Token.Type.RPAREN, "')' to close 'in' list");
        return new Expr.In(left, items, negated);
    }

    private boolean isComparisonOp(String s) {
        return s.equals("=") || s.equals("<>") || s.equals("!=")
                || s.equals("<") || s.equals("<=") || s.equals(">") || s.equals(">=");
    }

    private Expr parseAddSub() {
        Expr left = parseMulDiv();
        while (peek().isOp("+") || peek().isOp("-")) {
            String op = advance().text;
            left = new Expr.BinaryOp(op, left, parseMulDiv());
        }
        return left;
    }

    private Expr parseMulDiv() {
        Expr left = parseUnary();
        while (peek().isOp("*") || peek().isOp("/") || peek().isOp("%")) {
            String op = advance().text;
            left = new Expr.BinaryOp(op, left, parseUnary());
        }
        return left;
    }

    private Expr parseUnary() {
        if (peek().isOp("-")) {
            advance();
            return new Expr.UnaryOp("-", parseUnary());
        }
        if (peek().isOp("+")) {
            advance(); // unary plus is a no-op
            return parseUnary();
        }
        return parsePrimary();
    }

    private Expr parsePrimary() {
        Token t = peek();
        switch (t.type) {
            case INTEGER:
                advance();
                return new Expr.NumberLit(Long.parseLong(t.text));
            case DECIMAL:
                advance();
                return new Expr.NumberLit(new BigDecimal(t.text));
            case STRING:
                advance();
                return new Expr.StringLit(t.text);
            case IDENT:
                advance();
                if (peek().is(Token.Type.LPAREN)) {
                    return parseFuncCall(t.text);
                }
                return new Expr.FieldRef(t.text);
            case PARAM:
                advance();
                return new Expr.Param(paramCount++);
            case LPAREN: {
                advance();
                Expr e = parseXor(); // inside parens: full boolean expression (no alias)
                expect(Token.Type.RPAREN, "')'");
                return e;
            }
            case KEYWORD:
                if (t.isKeyword("null")) {
                    advance();
                    return new Expr.Macro(Expr.Macro.Kind.NULL);
                }
                if (t.isKeyword("true")) {
                    advance();
                    return new Expr.Macro(Expr.Macro.Kind.TRUE);
                }
                if (t.isKeyword("false")) {
                    advance();
                    return new Expr.Macro(Expr.Macro.Kind.FALSE);
                }
                if (t.isKeyword("cast")) {
                    return parseCast();
                }
                if (t.isKeyword("case")) {
                    return parseCase();
                }
                throw new ExprParseException("unexpected keyword in primary: " + t.text);
            default:
                throw new ExprParseException("unexpected token in primary: " + t);
        }
    }

    // name(arg, arg, ...) — IDENT already consumed, current token is LPAREN
    private Expr parseFuncCall(String name) {
        expect(Token.Type.LPAREN, "'(' after function name");
        List<Expr> args = new ArrayList<>();
        if (!peek().is(Token.Type.RPAREN)) {
            args.add(parseXor());
            while (peek().is(Token.Type.COMMA)) {
                advance();
                args.add(parseXor());
            }
        }
        expect(Token.Type.RPAREN, "')' to close function call");
        return new Expr.FuncCall(name, args);
    }

    // cast(expr as DataType)
    private Expr parseCast() {
        advance(); // 'cast'
        expect(Token.Type.LPAREN, "'(' after 'cast'");
        Expr operand = parseXor();
        if (!peek().isKeyword("as")) {
            throw new ExprParseException("expected 'as' in cast but found " + peek());
        }
        advance(); // 'as'
        String type = expectIdent("DataType in cast");
        expect(Token.Type.RPAREN, "')' to close cast");
        return new Expr.Cast(operand, type);
    }

    // case [subject] (when cond then val)+ [else val] end
    private Expr parseCase() {
        advance(); // 'case'
        Expr subject = null;
        if (!peek().isKeyword("when")) {
            subject = parseXor(); // simple-case subject
        }
        List<Expr> whens = new ArrayList<>();
        List<Expr> thens = new ArrayList<>();
        while (peek().isKeyword("when")) {
            advance();
            whens.add(parseXor());
            if (!peek().isKeyword("then")) {
                throw new ExprParseException("expected 'then' in case but found " + peek());
            }
            advance();
            thens.add(parseXor());
        }
        if (whens.isEmpty()) {
            throw new ExprParseException("case requires at least one 'when'");
        }
        Expr elseExpr = null;
        if (peek().isKeyword("else")) {
            advance();
            elseExpr = parseXor();
        }
        if (!peek().isKeyword("end")) {
            throw new ExprParseException("expected 'end' to close case but found " + peek());
        }
        advance();
        return new Expr.Case(subject, whens, thens, elseExpr);
    }

    // ---- token helpers ----

    private Token peek() {
        return tokens.get(pos);
    }

    private Token peekAt(int ahead) {
        int i = pos + ahead;
        return i < tokens.size() ? tokens.get(i) : tokens.get(tokens.size() - 1);
    }

    private Token advance() {
        return tokens.get(pos++);
    }

    private void expect(Token.Type type, String what) {
        if (!peek().is(type)) {
            throw new ExprParseException("expected " + what + " but found " + peek());
        }
        advance();
    }

    private String expectIdent(String what) {
        if (!peek().is(Token.Type.IDENT)) {
            throw new ExprParseException("expected " + what + " but found " + peek());
        }
        return advance().text;
    }
}
