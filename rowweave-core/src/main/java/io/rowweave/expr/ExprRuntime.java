package io.rowweave.expr;

import io.rowweave.DataRow;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;

/**
 * The expression runtime: **the single source of truth for every leaf semantic**. Both
 * {@link ExprInterpreter} and the bytecode produced by codegen call the static methods here, which is why
 * the two stay bit-identical for the same input rather than drifting apart.
 *
 * <p>Opcode convention, so the runtime avoids a string switch and codegen can push int constants:
 * <ul>
 * <li>arithmetic {@link #arith}: 0=subtract 1=multiply 2=divide (scale 10, HALF_UP) 3=remainder</li>
 * <li>comparison {@link #compare}: 0=`=` 1=`<>` 2=`<` 3=`<=` 4=`>` 5=`>=`</li>
 * </ul>
 * Null propagation, three-valued logic and scale 10 are all pinned down here.
 */
public final class ExprRuntime {

    /** Scale for division and for the final output: 10, rounded HALF_UP. */
    public static final int SCALE = 10;

    public static final int OP_SUB = 0, OP_MUL = 1, OP_DIV = 2, OP_REM = 3;
    public static final int CMP_EQ = 0, CMP_NE = 1, CMP_LT = 2, CMP_LE = 3, CMP_GT = 4, CMP_GE = 5;

    private ExprRuntime() {
    }

    /** Normalise a numeric literal to BigDecimal. */
    public static BigDecimal numberLit(Object num) {
        if (num instanceof BigDecimal) {
            return (BigDecimal) num;
        }
        if (num instanceof Long) {
            return BigDecimal.valueOf((Long) num);
        }
        return new BigDecimal(String.valueOf(num));
    }

    /** Field value: an integral is normalised to BigDecimal, the same rule the interpreter uses. */
    public static Object field(DataRow row, int index) {
        Object v = row.get(index);
        if (v instanceof Integer || v instanceof Long) {
            return BigDecimal.valueOf(((Number) v).longValue());
        }
        return v;
    }

    /** Positional parameter ?: out of range throws, and an integral is normalised. */
    public static Object param(Object[] params, int index) {
        if (params == null || index >= params.length) {
            throw new ExprEvalException("missing positional parameter ?#" + index);
        }
        return normalize(params[index]);
    }

    /** Normalise a value from outside: an integral becomes BigDecimal. */
    public static Object normalize(Object v) {
        if (v instanceof Integer || v instanceof Long) {
            return BigDecimal.valueOf(((Number) v).longValue());
        }
        return v;
    }

    /** Count of add/subtract fast-path hits, visible to tests only. */
    static long fastArithHits = 0;

    /**
     * Long fixed-point fast path for add and subtract. When both sides carry the same scale and a precision of 18 or
     * less, the unscaled values go through {@code Math.addExact} or {@code subtractExact}, where overflow throws {@link ArithmeticException}, and the result is wrapped by {@code BigDecimal.valueOf(result, scale)}.
     * A differing scale, a precision beyond that bound or an overflow returns null, which tells the caller to fall back to BigDecimal. The result is byte-identical to
     * {@code BigDecimal.add} or {@code subtract}: the scale keeps its meaning and nothing is rounded here.
     */
    private static BigDecimal fastAddSub(boolean add, BigDecimal a, BigDecimal b) {
        if (a.scale() != b.scale() || a.precision() > 18 || b.precision() > 18) {
            return null; // not eligible -> caller falls back to BigDecimal
        }
        try {
            long ua = a.unscaledValue().longValue();
            long ub = b.unscaledValue().longValue();
            long res = add ? Math.addExact(ua, ub) : Math.subtractExact(ua, ub);
            fastArithHits++;
            return BigDecimal.valueOf(res, a.scale());
        } catch (ArithmeticException overflow) {
            return null; // unscaled sum overflowed long -> caller falls back to exact BigDecimal
        }
    }

    /** + : concatenates when both sides are strings, otherwise adds numerically. null propagates. */
    public static Object plus(Object l, Object r) {
        if (l == null || r == null) {
            return null;
        }
        if (l instanceof String || r instanceof String) {
            return String.valueOf(l) + String.valueOf(r);
        }
        BigDecimal a = numericOperand("+", l);
        BigDecimal b = numericOperand("+", r);
        BigDecimal fast = fastAddSub(true, a, b); // long fast path (scale-guarded, overflow-safe)
        return fast != null ? fast : a.add(b);
    }

    /**
     * An arithmetic operand must be a BigDecimal. Anything else fails fast with an explicit
     * {@link ExprEvalException} naming the operation, the type and the value, instead of the bare {@code (BigDecimal)} cast and its unreadable ClassCastException.
     * A String is never implicitly converted to a number.
     */
    private static BigDecimal numericOperand(String op, Object v) {
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        throw new ExprEvalException("arithmetic '" + op + "' requires numeric operands, got "
                + v.getClass().getSimpleName() + " (value: " + v + ") — no implicit string→number conversion");
    }

    /** Arithmetic - * / %: null propagates; division uses scale 10 with HALF_UP. */
    public static Object arith(int op, Object l, Object r) {
        if (l == null || r == null) {
            return null;
        }
        String sym = opSymbol(op);
        BigDecimal a = numericOperand(sym, l);
        BigDecimal b = numericOperand(sym, r);
        switch (op) {
            case OP_SUB: {
                BigDecimal fast = fastAddSub(false, a, b); // long fast path
                return fast != null ? fast : a.subtract(b);
            }
            case OP_MUL: return a.multiply(b);
            case OP_DIV: return a.divide(b, SCALE, RoundingMode.HALF_UP);
            case OP_REM: return a.remainder(b);
            default: throw new ExprEvalException("unknown arithmetic op: " + op);
        }
    }

    private static String opSymbol(int op) {
        switch (op) {
            case OP_SUB: return "-";
            case OP_MUL: return "*";
            case OP_DIV: return "/";
            case OP_REM: return "%";
            default: return "?";
        }
    }

    /** Unary negation: null stays null. */
    public static Object negate(Object v) {
        return v == null ? null : ((BigDecimal) v).negate();
    }

    /** Count of fast-path hits, visible to tests only to prove the long path actually fires. Not thread-safe: a single-threaded test reads it. */
    static long fastCompareHits = 0;

    /**
     * When both sides are BigDecimal with **the same scale** and the unscaled value fits a long
     * (precision at most 18), compare the unscaled longs (~3ns) instead of calling
     * `BigDecimal.compareTo` (~20ns). **The scale must be equal** before unscaled values are comparable,
     * since otherwise 50.0 (unscaled 500) and 50.00 (unscaled 5000) would be judged unequal. A different
     * scale or a precision beyond the bound falls back to the authoritative compareTo and returns -1. The
     * outcome always matches the authoritative path, because this is a pure performance optimization with
     * no change in semantics.
     */
    private static int fastDecimalCompare(BigDecimal l, BigDecimal r) {
        if (l.scale() == r.scale() && l.precision() <= 18 && r.precision() <= 18) {
            fastCompareHits++;
            return Long.compare(l.unscaledValue().longValue(), r.unscaledValue().longValue());
        }
        return l.compareTo(r); // fall back: different scale or too many digits
    }

    /** Comparison: a null operand gives null, that is unknown; numbers use compareTo, and strings and booleans compare within their own kind. */
    public static Object compare(int op, Object l, Object r) {
        if (l == null || r == null) {
            return null;
        }
        int cmp;
        if (l instanceof BigDecimal && r instanceof BigDecimal) {
            cmp = fastDecimalCompare((BigDecimal) l, (BigDecimal) r); // long fast path (scale-guarded)
        } else if (l instanceof String && r instanceof String) {
            cmp = ((String) l).compareTo((String) r);
        } else if (l instanceof Boolean && r instanceof Boolean) {
            cmp = Boolean.compare((Boolean) l, (Boolean) r);
        } else {
            // (Plan A): clear error, no implicit string→number conversion (user rule).
            throw new ExprEvalException("incomparable operands: " + l.getClass().getSimpleName()
                    + " (value: " + l + ") vs " + r.getClass().getSimpleName() + " (value: " + r + ")"
                    + " — no implicit string→number conversion");
        }
        switch (op) {
            case CMP_EQ: return cmp == 0;
            case CMP_NE: return cmp != 0;
            case CMP_LT: return cmp < 0;
            case CMP_LE: return cmp <= 0;
            case CMP_GT: return cmp > 0;
            case CMP_GE: return cmp >= 0;
            default: throw new ExprEvalException("unknown comparison op: " + op);
        }
    }

    public static boolean isTrue(Object v) {
        return Boolean.TRUE.equals(v);
    }

    public static boolean isFalse(Object v) {
        return Boolean.FALSE.equals(v);
    }

    /** Three-valued AND: any FALSE gives FALSE, otherwise any null gives null, otherwise TRUE. */
    public static Object and(Object l, Object r) {
        Boolean lb = asBool(l);
        Boolean rb = asBool(r);
        if (Boolean.FALSE.equals(lb) || Boolean.FALSE.equals(rb)) {
            return Boolean.FALSE;
        }
        if (lb == null || rb == null) {
            return null;
        }
        return Boolean.TRUE;
    }

    /** Three-valued OR: any TRUE gives TRUE, otherwise any null gives null, otherwise FALSE. */
    public static Object or(Object l, Object r) {
        Boolean lb = asBool(l);
        Boolean rb = asBool(r);
        if (Boolean.TRUE.equals(lb) || Boolean.TRUE.equals(rb)) {
            return Boolean.TRUE;
        }
        if (lb == null || rb == null) {
            return null;
        }
        return Boolean.FALSE;
    }

    /** Three-valued XOR: any null gives null, otherwise the operands are XORed. */
    public static Object xor(Object l, Object r) {
        Boolean lb = asBool(l);
        Boolean rb = asBool(r);
        if (lb == null || rb == null) {
            return null;
        }
        return lb.booleanValue() ^ rb.booleanValue();
    }

    /** Three-valued NOT: null stays null. */
    public static Object not(Object v) {
        Boolean b = asBool(v);
        if (b == null) {
            return null;
        }
        return b ? Boolean.FALSE : Boolean.TRUE;
    }

    /** is null / is not null: returns a definite boolean. */
    public static Object isNull(Object v, boolean negated) {
        boolean isNull = v == null;
        return negated ? !isNull : isNull;
    }

    /** like: a null value or pattern gives null. % matches any sequence, _ any single character. */
    public static Object like(Object v, Object p) {
        if (v == null || p == null) {
            return null;
        }
        return matchesLike(String.valueOf(v), String.valueOf(p));
    }

    private static boolean matchesLike(String value, String pattern) {
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '%') {
                regex.append(".*");
            } else if (c == '_') {
                regex.append('.');
            } else {
                regex.append(java.util.regex.Pattern.quote(String.valueOf(c)));
            }
        }
        return java.util.regex.Pattern.compile(regex.toString(),
                java.util.regex.Pattern.DOTALL).matcher(value).matches();
    }

    /** in: a null value gives null; no match with a null in the list gives null; a match gives true, per the SQL standard. */
    public static Object in(Object v, Object[] items, boolean negated) {
        if (v == null) {
            return null;
        }
        boolean sawNull = false;
        for (Object iv : items) {
            if (iv == null) {
                sawNull = true;
                continue;
            }
            if (valuesEqual(v, iv)) {
                return negated ? Boolean.FALSE : Boolean.TRUE;
            }
        }
        if (sawNull) {
            return null;
        }
        return negated ? Boolean.TRUE : Boolean.FALSE;
    }

    public static boolean valuesEqual(Object a, Object b) {
        if (a instanceof BigDecimal && b instanceof BigDecimal) {
            return ((BigDecimal) a).compareTo((BigDecimal) b) == 0;
        }
        return a.equals(b);
    }

    /** Simple-form case hit: subject and when are both non-null and their values are equal. */
    public static boolean caseEq(Object subject, Object when) {
        return subject != null && when != null && valuesEqual(subject, when);
    }

    /** cast(v as type): null stays null, and a failed conversion throws ExprEvalException. */
    public static Object cast(Object v, String targetType) {
        if (v == null) {
            return null;
        }
        String type = targetType.toLowerCase(java.util.Locale.ROOT);
        try {
            switch (type) {
                case "integer": case "int": case "long":
                    BigDecimal integral = v instanceof BigDecimal
                            ? (BigDecimal) v : new BigDecimal(String.valueOf(v));
                    return integral.setScale(0, RoundingMode.DOWN);
                case "bigdecimal": case "decimal":
                    return v instanceof BigDecimal ? v : new BigDecimal(String.valueOf(v));
                case "boolean": case "bool":
                    if (v instanceof Boolean) {
                        return v;
                    }
                    return Boolean.parseBoolean(String.valueOf(v));
                case "string":
                    return v instanceof BigDecimal ? ((BigDecimal) v).toPlainString() : String.valueOf(v);
                case "date":
                    if (v instanceof java.time.LocalDate) return v;
                    if (v instanceof java.time.LocalDateTime) {
                        return ((java.time.LocalDateTime) v).toLocalDate();
                    }
                    if (v instanceof java.sql.Date) return ((java.sql.Date) v).toLocalDate();
                    if (v instanceof java.sql.Timestamp) {
                        return ((java.sql.Timestamp) v).toLocalDateTime().toLocalDate();
                    }
                    return java.time.LocalDate.parse(String.valueOf(v));
                case "timestamp":
                    if (v instanceof java.time.LocalDateTime) return v;
                    if (v instanceof java.time.LocalDate) {
                        return ((java.time.LocalDate) v).atStartOfDay();
                    }
                    if (v instanceof java.sql.Timestamp) {
                        return ((java.sql.Timestamp) v).toLocalDateTime();
                    }
                    if (v instanceof java.sql.Date) {
                        return ((java.sql.Date) v).toLocalDate().atStartOfDay();
                    }
                    String timestamp = String.valueOf(v);
                    return timestamp.indexOf(' ') >= 0
                            ? java.sql.Timestamp.valueOf(timestamp).toLocalDateTime()
                            : java.time.LocalDateTime.parse(timestamp);
                default:
                    throw new ExprEvalException("unknown cast target type: " + targetType);
            }
        } catch (ExprEvalException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new ExprEvalException("cast failed: '" + v + "' as " + targetType + " (" + ex.getMessage() + ")");
        }
    }

    /** Function call: dispatches the evaluated argument array through the registered function table. */
    public static Object func(String name, Object[] args) {
        return ExprFunctions.call(name, Arrays.asList(args));
    }

    public static Boolean asBool(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        throw new ExprEvalException("expected boolean but got " + v.getClass().getSimpleName());
    }
}
