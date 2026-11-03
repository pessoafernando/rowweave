package io.rowweave.expr;

import java.math.BigDecimal;
import java.util.List;

/**
 * Base class of the expression AST nodes. Immutable. The canonical {@link #toString} fully parenthesizes, which makes precedence testable.
 * Evaluation dispatches on node type inside {@link ExprInterpreter}; this class only builds the tree.
 */
public abstract class Expr {

    /** Numeric literal: an integer becomes a Long, a decimal a BigDecimal. */
    public static final class NumberLit extends Expr {
        public final Object value; // Long or BigDecimal

        NumberLit(Object value) {
            this.value = value;
        }

        @Override
        public String toString() {
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).toPlainString();
            }
            return String.valueOf(value);
        }
    }

    /** String literal. */
    public static final class StringLit extends Expr {
        public final String value;

        StringLit(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "'" + value + "'";
        }
    }

    /** Macro: NULL, TRUE or FALSE. */
    public static final class Macro extends Expr {
        public enum Kind { NULL, TRUE, FALSE }

        public final Kind kind;

        Macro(Kind kind) {
            this.kind = kind;
        }

        @Override
        public String toString() {
            return kind.name();
        }
    }

    /** Field reference written as a bare identifier. */
    public static final class FieldRef extends Expr {
        public final String name;

        FieldRef(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** Parameter placeholder ?, filled by position when a filter is evaluated with a Map. */
    public static final class Param extends Expr {
        public final int index; // 0-based positional

        Param(int index) {
            this.index = index;
        }

        @Override
        public String toString() {
            return "?";
        }
    }

    /** Binary operation: arithmetic + - * / %, comparison = <> != < <= > >=, logic and or xor, and string concatenation reusing +. */
    public static final class BinaryOp extends Expr {
        public final String op;
        public final Expr left;
        public final Expr right;

        BinaryOp(String op, Expr left, Expr right) {
            this.op = op;
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return "(" + left + " " + op + " " + right + ")";
        }
    }

    /** Unary operation: - for negation, or not. */
    public static final class UnaryOp extends Expr {
        public final String op;
        public final Expr operand;

        UnaryOp(String op, Expr operand) {
            this.op = op;
            this.operand = operand;
        }

        @Override
        public String toString() {
            return "(" + op + " " + operand + ")";
        }
    }

    /** is null / is not null: returns a definite boolean, per the SQL standard. */
    public static final class IsNull extends Expr {
        public final Expr operand;
        public final boolean negated; // true = is not null

        IsNull(Expr operand, boolean negated) {
            this.operand = operand;
            this.negated = negated;
        }

        @Override
        public String toString() {
            return "(" + operand + " IS " + (negated ? "NOT " : "") + "NULL)";
        }
    }

    /** like: the SQL wildcards % for any sequence and _ for any single character. */
    public static final class Like extends Expr {
        public final Expr value;
        public final Expr pattern;

        Like(Expr value, Expr pattern) {
            this.value = value;
            this.pattern = pattern;
        }

        @Override
        public String toString() {
            return "(" + value + " LIKE " + pattern + ")";
        }
    }

    /** in (v1, v2, ...); negated marks not in. */
    public static final class In extends Expr {
        public final Expr value;
        public final List<Expr> items;
        public final boolean negated;

        In(Expr value, List<Expr> items, boolean negated) {
            this.value = value;
            this.items = items;
            this.negated = negated;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("(").append(value)
                    .append(negated ? " NOT IN (" : " IN (");
            for (int i = 0; i < items.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(items.get(i));
            }
            return sb.append("))").toString();
        }
    }

    /** Cast(expr as DataType). */
    public static final class Cast extends Expr {
        public final Expr operand;
        public final String targetType; // canonical: Integer/Long/BigDecimal/Boolean/String/Date/Timestamp

        Cast(Expr operand, String targetType) {
            this.operand = operand;
            this.targetType = targetType;
        }

        @Override
        public String toString() {
            return "CAST(" + operand + " AS " + targetType + ")";
        }
    }

    /** Case-When: a searched form carries subject=null and a simple form carries subject!=null. */
    public static final class Case extends Expr {
        public final Expr subject; // null for searched form
        public final List<Expr> whens;
        public final List<Expr> thens;
        public final Expr elseExpr; // null if no else

        Case(Expr subject, List<Expr> whens, List<Expr> thens, Expr elseExpr) {
            this.subject = subject;
            this.whens = whens;
            this.thens = thens;
            this.elseExpr = elseExpr;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("CASE");
            if (subject != null) {
                sb.append(" ").append(subject);
            }
            for (int i = 0; i < whens.size(); i++) {
                sb.append(" WHEN ").append(whens.get(i)).append(" THEN ").append(thens.get(i));
            }
            if (elseExpr != null) {
                sb.append(" ELSE ").append(elseExpr);
            }
            return sb.append(" END").toString();
        }
    }

    /** Function call: name(arg, arg, ...). Function names are case-insensitive and uppercased in the canonical toString. */
    public static final class FuncCall extends Expr {
        public final String name;
        public final List<Expr> args;

        FuncCall(String name, List<Expr> args) {
            this.name = name;
            this.args = args;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(name.toUpperCase(java.util.Locale.ROOT)).append("(");
            for (int i = 0; i < args.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(args.get(i));
            }
            return sb.append(")").toString();
        }
    }

    /** Alias: expr AS alias. */
    public static final class Aliased extends Expr {
        public final Expr expr;
        public final String alias;

        Aliased(Expr expr, String alias) {
            this.expr = expr;
            this.alias = alias;
        }

        @Override
        public String toString() {
            return expr + " AS " + alias;
        }
    }
}
