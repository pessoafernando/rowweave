package io.rowweave.expr;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Interpreter: evaluates an {@link Expr} AST against a {@link DataRow}.
 *
 * <p>Semantics are fixed to the SQL standard:
 * <ul>
 * <li>Arithmetic runs entirely on {@link BigDecimal}; division uses scale 10 with {@link RoundingMode#HALF_UP}.</li>
 * <li>null propagation: a null operand makes arithmetic and concatenation null, and a comparison involving null is null, that is unknown.</li>
 * <li>Three-valued logic: and, or, xor and not follow the SQL truth tables, where null is unknown.</li>
 * <li>The predicate {@link #test(DataRow)} turns unknown, that is null, into false, so the row does not pass a filter.</li>
 * </ul>
 *
 * <p>Compile time resolves field names to indices; evaluation time reads only indices and matches no strings.
 */
public final class ExprInterpreter {

    /** Scale for division and for the final output: 10, rounded HALF_UP. */
    static final int SCALE = 10;

    private final Expr root;
    private final DataSchema meta;
    private final java.util.Map<String, Object> namedParams;

    private ExprInterpreter(Expr root, DataSchema meta, java.util.Map<String, Object> namedParams) {
        this.root = root;
        this.meta = meta;
        this.namedParams = namedParams;
    }

    public static ExprInterpreter compile(DataSchema meta, String expression) {
        return compile(meta, ExprParser.parse(expression));
    }

    public static ExprInterpreter compile(DataSchema meta, Expr root) {
        ExprSchema.validateReferences(meta, root);
        return new ExprInterpreter(root, meta, null);
    }

    /**
     * Compile with named parameters, resolving a bare identifier field-first: the field is looked up first and only an
     * absent field falls back to {@code namedParams}. This is what makes filter(expr, Map) work, and when a field name and a parameter name collide the field wins.
     */
    public static ExprInterpreter compile(DataSchema meta, String expression, java.util.Map<String, Object> namedParams) {
        Expr root = ExprParser.parse(expression);
        ExprSchema.validateReferences(meta, root, namedParams);
        return new ExprInterpreter(root, meta, namedParams);
    }

    private static final Object[] NO_PARAMS = new Object[0];

    /** Evaluate and return an Object: BigDecimal, String, Boolean or null. */
    public Object eval(DataRow row) {
        return evalNode(root, row, NO_PARAMS);
    }

    /** Evaluate with positional ? parameters. */
    public Object eval(DataRow row, Object... params) {
        return evalNode(root, row, params == null ? NO_PARAMS : params);
    }

    /** Evaluate as a predicate: unknown, that is null, becomes false. */
    public boolean test(DataRow row) {
        return Boolean.TRUE.equals(evalNode(root, row, NO_PARAMS));
    }

    /** Evaluate as a predicate with positional ? parameters. */
    public boolean test(DataRow row, Object[] params) {
        return Boolean.TRUE.equals(evalNode(root, row, params == null ? NO_PARAMS : params));
    }

    private Object evalNode(Expr e, DataRow row, Object[] params) {
        if (e instanceof Expr.NumberLit) {
            return toBigDecimal(((Expr.NumberLit) e).value);
        }
        if (e instanceof Expr.StringLit) {
            return ((Expr.StringLit) e).value;
        }
        if (e instanceof Expr.Macro) {
            switch (((Expr.Macro) e).kind) {
                case NULL: return null;
                case TRUE: return Boolean.TRUE;
                case FALSE: return Boolean.FALSE;
                default: throw new ExprEvalException("unknown macro");
            }
        }
        if (e instanceof Expr.FieldRef) {
            return evalField((Expr.FieldRef) e, row);
        }
        if (e instanceof Expr.Aliased) {
            return evalNode(((Expr.Aliased) e).expr, row, params);
        }
        if (e instanceof Expr.UnaryOp) {
            return evalUnary((Expr.UnaryOp) e, row, params);
        }
        if (e instanceof Expr.BinaryOp) {
            return evalBinary((Expr.BinaryOp) e, row, params);
        }
        if (e instanceof Expr.Param) {
            int idx = ((Expr.Param) e).index;
            if (idx >= params.length) {
                throw new ExprEvalException("missing positional parameter ?#" + idx);
            }
            return normalize(params[idx]);
        }
        if (e instanceof Expr.IsNull) {
            Expr.IsNull n = (Expr.IsNull) e;
            boolean isNull = evalNode(n.operand, row, params) == null;
            return n.negated ? !isNull : isNull;
        }
        if (e instanceof Expr.Like) {
            return evalLike((Expr.Like) e, row, params);
        }
        if (e instanceof Expr.In) {
            return evalIn((Expr.In) e, row, params);
        }
        if (e instanceof Expr.Cast) {
            return evalCast((Expr.Cast) e, row, params);
        }
        if (e instanceof Expr.Case) {
            return evalCase((Expr.Case) e, row, params);
        }
        if (e instanceof Expr.FuncCall) {
            return evalFuncCall((Expr.FuncCall) e, row, params);
        }
        throw new ExprEvalException("cannot evaluate node: " + e.getClass().getSimpleName());
    }

    private Object evalField(Expr.FieldRef f, DataRow row) {
        int idx = meta.getFieldIndex(f.name);
        if (idx < 0) {
            // field-priority: bare identifier falls back to a named param only if no such field exists.
            if (namedParams != null && namedParams.containsKey(f.name)) {
                return normalize(namedParams.get(f.name));
            }
            throw new ExprEvalException("unknown field: " + f.name);
        }
        Object v = row.get(idx);
        if (v instanceof Integer || v instanceof Long) {
            return BigDecimal.valueOf(((Number) v).longValue());
        }
        return v;
    }

    private Object evalUnary(Expr.UnaryOp u, DataRow row, Object[] params) {
        Object v = evalNode(u.operand, row, params);
        if ("-".equals(u.op)) {
            if (v == null) {
                return null;
            }
            return ((BigDecimal) v).negate();
        }
        if ("not".equals(u.op)) {
            if (v == null) {
                return null; // NOT unknown = unknown
            }
            return ((Boolean) v) ? Boolean.FALSE : Boolean.TRUE;
        }
        throw new ExprEvalException("unknown unary op: " + u.op);
    }

    private Object evalBinary(Expr.BinaryOp b, DataRow row, Object[] params) {
        switch (b.op) {
            case "and": case "or": case "xor":
                return evalLogical(b, row, params);
            default:
                break;
        }
        Object l = evalNode(b.left, row, params);
        Object r = evalNode(b.right, row, params);
        switch (b.op) {
            case "+":
                return evalPlus(l, r);
            case "-": case "*": case "/": case "%":
                return evalArithmetic(b.op, l, r);
            case "=": case "<>": case "!=": case "<": case "<=": case ">": case ">=":
                return evalComparison(b.op, l, r);
            default:
                throw new ExprEvalException("unknown binary op: " + b.op);
        }
    }

    /** + : concatenates when both sides are strings, otherwise adds numerically, and null propagates. Delegates to {@link ExprRuntime}, the same code the generated bytecode calls. */
    private Object evalPlus(Object l, Object r) {
        return ExprRuntime.plus(l, r);
    }

    private Object evalArithmetic(String op, Object l, Object r) {
        return ExprRuntime.arith(arithOpCode(op), l, r);
    }

    static int arithOpCode(String op) {
        switch (op) {
            case "-": return ExprRuntime.OP_SUB;
            case "*": return ExprRuntime.OP_MUL;
            case "/": return ExprRuntime.OP_DIV;
            case "%": return ExprRuntime.OP_REM;
            default: throw new ExprEvalException("unknown arithmetic op: " + op);
        }
    }

    /** Comparison: a null operand makes the result null, that is unknown. Delegates to {@link ExprRuntime}, the same code the generated bytecode calls. */
    private Object evalComparison(String op, Object l, Object r) {
        return ExprRuntime.compare(compareOpCode(op), l, r);
    }

    static int compareOpCode(String op) {
        switch (op) {
            case "=": return ExprRuntime.CMP_EQ;
            case "<>": case "!=": return ExprRuntime.CMP_NE;
            case "<": return ExprRuntime.CMP_LT;
            case "<=": return ExprRuntime.CMP_LE;
            case ">": return ExprRuntime.CMP_GT;
            case ">=": return ExprRuntime.CMP_GE;
            default: throw new ExprEvalException("unknown comparison op: " + op);
        }
    }

    /** Three-valued and, or, xor. */
    private Object evalLogical(Expr.BinaryOp b, DataRow row, Object[] params) {
        Boolean l = asBool(evalNode(b.left, row, params));
        if ("and".equals(b.op)) {
            if (Boolean.FALSE.equals(l)) {
                return Boolean.FALSE; // false AND anything = false (short-circuit)
            }
            Boolean r = asBool(evalNode(b.right, row, params));
            if (Boolean.FALSE.equals(r)) {
                return Boolean.FALSE;
            }
            if (l == null || r == null) {
                return null;
            }
            return Boolean.TRUE;
        }
        if ("or".equals(b.op)) {
            if (Boolean.TRUE.equals(l)) {
                return Boolean.TRUE; // true OR anything = true (short-circuit)
            }
            Boolean r = asBool(evalNode(b.right, row, params));
            if (Boolean.TRUE.equals(r)) {
                return Boolean.TRUE;
            }
            if (l == null || r == null) {
                return null;
            }
            return Boolean.FALSE;
        }
        // xor: null if either side unknown
        Boolean r = asBool(evalNode(b.right, row, params));
        if (l == null || r == null) {
            return null;
        }
        return l.booleanValue() ^ r.booleanValue();
    }

    /** like: a null value or pattern gives unknown, that is null. % matches any sequence, _ any single character. */
    private Object evalLike(Expr.Like expression, DataRow row, Object[] params) {
        return ExprRuntime.like(evalNode(expression.value, row, params),
                evalNode(expression.pattern, row, params));
    }

    /** in: a null value gives unknown; no match with a null in the list gives unknown; a match gives true, per the SQL standard. */
    private Object evalIn(Expr.In e, DataRow row, Object[] params) {
        Object v = evalNode(e.value, row, params);
        if (v == null) {
            return null; // null in/not in (...) → unknown
        }
        boolean sawNull = false;
        for (Expr item : e.items) {
            Object iv = evalNode(item, row, params);
            if (iv == null) {
                sawNull = true;
                continue;
            }
            if (valuesEqual(v, iv)) {
                return e.negated ? Boolean.FALSE : Boolean.TRUE;
            }
        }
        if (sawNull) {
            return null; // no match + null present → unknown
        }
        return e.negated ? Boolean.TRUE : Boolean.FALSE;
    }

    private static boolean valuesEqual(Object a, Object b) {
        if (a instanceof BigDecimal && b instanceof BigDecimal) {
            return ((BigDecimal) a).compareTo((BigDecimal) b) == 0;
        }
        return a.equals(b);
    }

    /** cast(expr as Type): null stays null, and a conversion that fails throws ExprEvalException. */
    private Object evalCast(Expr.Cast expression, DataRow row, Object[] params) {
        return ExprRuntime.cast(evalNode(expression.operand, row, params), expression.targetType);
    }

    /** case-when: the simple form compares the subject against each when value, the searched form treats each when as a boolean condition. No match and no else gives null. */
    private Object evalCase(Expr.Case e, DataRow row, Object[] params) {
        Object subject = e.subject == null ? null : evalNode(e.subject, row, params);
        for (int i = 0; i < e.whens.size(); i++) {
            Object w = evalNode(e.whens.get(i), row, params);
            boolean hit;
            if (e.subject == null) {
                hit = Boolean.TRUE.equals(w); // searched: when is a predicate
            } else {
                hit = subject != null && w != null && valuesEqual(subject, w);
            }
            if (hit) {
                return evalNode(e.thens.get(i), row, params);
            }
        }
        return e.elseExpr == null ? null : evalNode(e.elseExpr, row, params);
    }

    /** Function call: arguments are evaluated in order, then dispatched through the registered function table, where each function decides how null propagates. */
    private Object evalFuncCall(Expr.FuncCall f, DataRow row, Object[] params) {
        java.util.List<Object> args = new java.util.ArrayList<>(f.args.size());
        for (Expr arg : f.args) {
            args.add(evalNode(arg, row, params));
        }
        return ExprFunctions.call(f.name, args);
    }

    /** Normalise a value passed from outside so that an integral becomes BigDecimal, under the same rule as a field value. */
    private static Object normalize(Object v) {
        if (v instanceof Integer || v instanceof Long) {
            return BigDecimal.valueOf(((Number) v).longValue());
        }
        return v;
    }

    private static Boolean asBool(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        throw new ExprEvalException("expected boolean but got " + v.getClass().getSimpleName());
    }

    private static BigDecimal toBigDecimal(Object num) {
        if (num instanceof BigDecimal) {
            return (BigDecimal) num;
        }
        if (num instanceof Long) {
            return BigDecimal.valueOf((Long) num);
        }
        return new BigDecimal(String.valueOf(num));
    }
}
