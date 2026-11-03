package io.rowweave.expr;

import java.util.ArrayList;
import java.util.List;

/**
 * Hand-written lexer: turns the source string into a token list, with types declared by {@link Token.Type}.
 * The keywords and, or, xor, not, as, is, null, true, false, like, in, case, when, then, else, end and cast are case-insensitive and
 * are recognized as KEYWORD with the text lowercased; an identifier is a field name. Strings are single-quoted, with '' for one quote.
 */
final class Lexer {

    static final java.util.Set<String> KEYWORDS = new java.util.HashSet<>(java.util.Arrays.asList(
            "and", "or", "xor", "not", "as", "is", "null", "true", "false",
            "like", "in", "case", "when", "then", "else", "end", "cast"));

    private final String src;
    private int pos;

    Lexer(String src) {
        this.src = src;
    }

    List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        Token t;
        while ((t = next()) != null) {
            tokens.add(t);
        }
        tokens.add(new Token(Token.Type.EOF, ""));
        return tokens;
    }

    private Token next() {
        skipWhitespace();
        if (pos >= src.length()) {
            return null;
        }
        char c = src.charAt(pos);

        if (c == '\'') {
            return stringLiteral();
        }
        if (Character.isDigit(c) || (c == '.' && pos + 1 < src.length() && Character.isDigit(src.charAt(pos + 1)))) {
            return number();
        }
        if (Character.isLetter(c) || c == '_') {
            return identifierOrKeyword();
        }
        return operator();
    }

    private void skipWhitespace() {
        while (pos < src.length() && Character.isWhitespace(src.charAt(pos))) {
            pos++;
        }
    }

    private Token stringLiteral() {
        pos++; // opening quote
        StringBuilder sb = new StringBuilder();
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (c == '\'') {
                if (pos + 1 < src.length() && src.charAt(pos + 1) == '\'') {
                    sb.append('\''); // '' -> '
                    pos += 2;
                    continue;
                }
                pos++; // closing quote
                return new Token(Token.Type.STRING, sb.toString());
            }
            sb.append(c);
            pos++;
        }
        throw new ExprParseException("unterminated string literal");
    }

    private Token number() {
        int start = pos;
        boolean dot = false;
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (Character.isDigit(c)) {
                pos++;
            } else if (c == '.' && !dot) {
                dot = true;
                pos++;
            } else {
                break;
            }
        }
        return new Token(dot ? Token.Type.DECIMAL : Token.Type.INTEGER, src.substring(start, pos));
    }

    private Token identifierOrKeyword() {
        int start = pos;
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (Character.isLetterOrDigit(c) || c == '_') {
                pos++;
            } else {
                break;
            }
        }
        String word = src.substring(start, pos);
        if (KEYWORDS.contains(word.toLowerCase(java.util.Locale.ROOT))) {
            return new Token(Token.Type.KEYWORD, word.toLowerCase(java.util.Locale.ROOT));
        }
        return new Token(Token.Type.IDENT, word);
    }

    private Token operator() {
        // multi-char first: <= >= <> !=
        if (pos + 1 < src.length()) {
            String two = src.substring(pos, pos + 2);
            if (two.equals("<=") || two.equals(">=") || two.equals("<>") || two.equals("!=")) {
                pos += 2;
                return new Token(Token.Type.OP, two);
            }
        }
        char c = src.charAt(pos);
        switch (c) {
            case '+': case '-': case '*': case '/': case '%':
            case '<': case '>': case '=':
                pos++;
                return new Token(Token.Type.OP, String.valueOf(c));
            case '(':
                pos++;
                return new Token(Token.Type.LPAREN, "(");
            case ')':
                pos++;
                return new Token(Token.Type.RPAREN, ")");
            case ',':
                pos++;
                return new Token(Token.Type.COMMA, ",");
            case '?':
                pos++;
                return new Token(Token.Type.PARAM, "?");
            default:
                throw new ExprParseException("unexpected character: '" + c + "' at " + pos);
        }
    }
}
