package io.rowweave.expr;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The function library: a registry keyed by lower-cased function name. The evaluator resolves the arguments
 * and hands them here for dispatch.
 *
 * <p>Semantic rules, settled by tests wherever the language definition leaves them open:
 * <ul>
 * <li>for most functions a null argument makes the result null (null propagation); the exception is the
 * zero-argument Now() and Date().</li>
 * <li>Numeric values stay {@link BigDecimal} throughout, matching the evaluator, whose integer fields are
 * already normalized to BigDecimal.</li>
 * <li>Round: the second argument defaults to scale 0; an explicit scale rounds {@link RoundingMode#HALF_UP}.</li>
 * <li>SubStr: start is 1-based, per the SQL standard, and out-of-range bounds clip without error.</li>
 * <li>Dateadd(datepart, number, date): datepart in Year/Month/Day/Hour/Minute/Second.</li>
 * <li>Datedif(start, end, unit): unit in Y/M/D.</li>
 * <li>Replace(text, old, new [, instance_num]): the 3-argument form replaces every occurrence; the
 * 4-argument form replaces only the first N, where N at or below 0 replaces nothing and N at or above the
 * number of occurrences replaces all of them.</li>
 * </ul>
 * <p>An unknown function name throws {@link ExprEvalException}.
 */
final class ExprFunctions {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** One function implementation: takes evaluated arguments and returns a result, or null. */
    @FunctionalInterface
    interface Fn {
        Object apply(List<Object> args);
    }

    private static final Map<String, Fn> TABLE = new HashMap<>();

    private ExprFunctions() {
    }

    static Object call(String name, List<Object> args) {
        Fn fn = TABLE.get(name.toLowerCase(java.util.Locale.ROOT));
        if (fn == null) {
            throw new ExprEvalException("unknown function: " + name);
        }
        return fn.apply(args);
    }

    // ---- helpers ----

    private static boolean anyNull(List<Object> args) {
        for (Object a : args) {
            if (a == null) {
                return true;
            }
        }
        return false;
    }

    private static BigDecimal bd(Object v) {
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        return new BigDecimal(String.valueOf(v));
    }

    private static int intArg(Object v) {
        return bd(v).intValueExact();
    }

    private static String str(Object v) {
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).toPlainString();
        }
        return String.valueOf(v);
    }

    private static void checkArity(String name, List<Object> args, int min, int max) {
        int n = args.size();
        if (n < min || n > max) {
            throw new ExprEvalException("function " + name + " expects "
                    + (min == max ? String.valueOf(min) : min + ".." + max) + " args but got " + n);
        }
    }

    private static LocalDate asDate(Object v) {
        if (v instanceof LocalDate) {
            return (LocalDate) v;
        }
        if (v instanceof LocalDateTime) {
            return ((LocalDateTime) v).toLocalDate();
        }
        if (v instanceof java.sql.Date) {
            return ((java.sql.Date) v).toLocalDate();
        }
        if (v instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) v).toLocalDateTime().toLocalDate();
        }
        return LocalDate.parse(String.valueOf(v), DATE_FMT);
    }

    private static boolean isDateTime(Object v) {
        return v instanceof LocalDateTime || v instanceof java.sql.Timestamp;
    }

    private static LocalDateTime asDateTime(Object v) {
        return v instanceof LocalDateTime
                ? (LocalDateTime) v : ((java.sql.Timestamp) v).toLocalDateTime();
    }

    private static LocalDateTime temporalDateTime(Object v) {
        return isDateTime(v) ? asDateTime(v) : asDate(v).atStartOfDay();
    }

    // ---- registration ----

    static {
        // numeric
        reg("abs", a -> a.get(0) == null ? null : bd(a.get(0)).abs());
        reg("round", ExprFunctions::round);
        reg("int", ExprFunctions::truncate);
        reg("integer", ExprFunctions::truncate);
        reg("long", ExprFunctions::truncate);
        reg("decimal", a -> a.get(0) == null ? null : bd(a.get(0)));
        reg("bigdecimal", a -> a.get(0) == null ? null : bd(a.get(0)));
        reg("boolean", ExprFunctions::toBoolean);
        reg("bool", ExprFunctions::toBoolean);
        reg("string", a -> a.get(0) == null ? null : str(a.get(0)));

        // string
        reg("concat", ExprFunctions::concat);
        reg("contains", a -> nullOr(a, () -> str(a.get(0)).contains(str(a.get(1)))));
        reg("startswith", a -> nullOr(a, () -> str(a.get(0)).startsWith(str(a.get(1)))));
        reg("endswith", a -> nullOr(a, () -> str(a.get(0)).endsWith(str(a.get(1)))));
        reg("lower", a -> a.get(0) == null ? null : str(a.get(0)).toLowerCase(java.util.Locale.ROOT));
        reg("upper", a -> a.get(0) == null ? null : str(a.get(0)).toUpperCase(java.util.Locale.ROOT));
        reg("len", a -> a.get(0) == null ? null : BigDecimal.valueOf(str(a.get(0)).length()));
        reg("substr", ExprFunctions::substr);
        reg("left", a -> left(a, true));
        reg("right", a -> left(a, false));
        reg("replace", ExprFunctions::replace);

        // date
        reg("now", a -> LocalDateTime.now());
        reg("date", ExprFunctions::date);
        reg("year", a -> a.get(0) == null ? null : BigDecimal.valueOf(asDate(a.get(0)).getYear()));
        reg("month", a -> a.get(0) == null ? null : BigDecimal.valueOf(asDate(a.get(0)).getMonthValue()));
        reg("day", a -> a.get(0) == null ? null : BigDecimal.valueOf(asDate(a.get(0)).getDayOfMonth()));
        reg("hour", a -> a.get(0) == null ? null
                : BigDecimal.valueOf(temporalDateTime(a.get(0)).getHour()));
        reg("minute", a -> a.get(0) == null ? null
                : BigDecimal.valueOf(temporalDateTime(a.get(0)).getMinute()));
        reg("second", a -> a.get(0) == null ? null
                : BigDecimal.valueOf(temporalDateTime(a.get(0)).getSecond()));
        reg("to_date", ExprFunctions::toDate);
        reg("to_char", ExprFunctions::toChar);
        reg("dateadd", ExprFunctions::dateadd);
        reg("datedif", ExprFunctions::datedif);
    }

    private static void reg(String name, Fn fn) {
        TABLE.put(name, fn);
    }

    /** If any argument is null the result is null; otherwise the supplier computes it. */
    private static Object nullOr(List<Object> args, java.util.function.Supplier<Object> s) {
        return anyNull(args) ? null : s.get();
    }

    // ---- numeric impls ----

    private static Object round(List<Object> a) {
        checkArity("Round", a, 1, 2);
        if (anyNull(a)) {
            return null;
        }
        int scale = a.size() == 2 ? intArg(a.get(1)) : 0;
        return bd(a.get(0)).setScale(scale, RoundingMode.HALF_UP);
    }

    private static Object truncate(List<Object> a) {
        if (a.get(0) == null) {
            return null;
        }
        return bd(a.get(0)).setScale(0, RoundingMode.DOWN);
    }

    private static Object toBoolean(List<Object> a) {
        Object v = a.get(0);
        if (v == null) {
            return null;
        }
        if (v instanceof Boolean) {
            return v;
        }
        return Boolean.parseBoolean(String.valueOf(v));
    }

    // ---- string impls ----

    private static Object concat(List<Object> a) {
        if (anyNull(a)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Object o : a) {
            sb.append(str(o));
        }
        return sb.toString();
    }

    private static Object substr(List<Object> a) {
        checkArity("SubStr", a, 2, 3);
        if (anyNull(a)) {
            return null;
        }
        String s = str(a.get(0));
        int start1 = intArg(a.get(1)); // 1-based
        int begin = Math.max(0, start1 - 1);
        if (begin >= s.length()) {
            return "";
        }
        int end;
        if (a.size() == 3) {
            int len = intArg(a.get(2));
            end = (len < 0) ? begin : Math.min(s.length(), begin + len);
        } else {
            end = s.length();
        }
        return s.substring(begin, end);
    }

    private static Object left(List<Object> a, boolean fromLeft) {
        checkArity(fromLeft ? "Left" : "Right", a, 2, 2);
        if (anyNull(a)) {
            return null;
        }
        String s = str(a.get(0));
        int n = Math.max(0, Math.min(s.length(), intArg(a.get(1))));
        return fromLeft ? s.substring(0, n) : s.substring(s.length() - n);
    }

    private static Object replace(List<Object> a) {
        checkArity("Replace", a, 3, 4);
        if (anyNull(a)) {
            return null;
        }
        String text = str(a.get(0));
        String oldText = str(a.get(1));
        String newText = str(a.get(2));
        if (a.size() == 3) {
            return text.replace(oldText, newText); // default: replace all
        }
        // instance_num: replace only the first N occurrences (pptx spec); N>=count -> all; empty oldText -> no-op
        int n = intArg(a.get(3));
        if (n <= 0 || oldText.isEmpty()) {
            return text;
        }
        StringBuilder sb = new StringBuilder();
        int from = 0;
        int done = 0;
        int idx;
        while (done < n && (idx = text.indexOf(oldText, from)) >= 0) {
            sb.append(text, from, idx).append(newText);
            from = idx + oldText.length();
            done++;
        }
        sb.append(text.substring(from));
        return sb.toString();
    }

    // ---- date impls ----

    private static Object date(List<Object> a) {
        if (a.isEmpty()) {
            return LocalDate.now();
        }
        checkArity("Date", a, 3, 3);
        if (anyNull(a)) {
            return null;
        }
        return LocalDate.of(intArg(a.get(0)), intArg(a.get(1)), intArg(a.get(2)));
    }

    private static Object toDate(List<Object> a) {
        checkArity("TO_DATE", a, 1, 2);
        if (anyNull(a)) {
            return null;
        }
        DateTimeFormatter fmt = a.size() == 2
                ? DateTimeFormatter.ofPattern(str(a.get(1))) : DATE_FMT;
        return LocalDate.parse(str(a.get(0)), fmt);
    }

    private static Object toChar(List<Object> a) {
        checkArity("TO_CHAR", a, 1, 2);
        if (anyNull(a)) {
            return null;
        }
        DateTimeFormatter fmt = a.size() == 2
                ? DateTimeFormatter.ofPattern(str(a.get(1)))
                : (isDateTime(a.get(0)) ? DATETIME_FMT : DATE_FMT);
        Object v = a.get(0);
        if (isDateTime(v)) {
            return asDateTime(v).format(fmt);
        }
        return asDate(v).format(fmt);
    }

    private static Object dateadd(List<Object> a) {
        checkArity("Dateadd", a, 3, 3);
        if (anyNull(a)) {
            return null;
        }
        String part = str(a.get(0)).toLowerCase(java.util.Locale.ROOT);
        long n = bd(a.get(1)).longValueExact();
        Object d = a.get(2);
        if (isDateTime(d)) {
            return addToDateTime(asDateTime(d), part, n);
        }
        LocalDate date = asDate(d);
        switch (part) {
            case "year": return date.plusYears(n);
            case "month": return date.plusMonths(n);
            case "day": return date.plusDays(n);
            case "hour": return date.atStartOfDay().plusHours(n);
            case "minute": return date.atStartOfDay().plusMinutes(n);
            case "second": return date.atStartOfDay().plusSeconds(n);
            default: throw new ExprEvalException("unknown datepart: " + str(a.get(0)));
        }
    }

    private static Object addToDateTime(LocalDateTime dt, String part, long n) {
        switch (part) {
            case "year": return dt.plusYears(n);
            case "month": return dt.plusMonths(n);
            case "day": return dt.plusDays(n);
            case "hour": return dt.plusHours(n);
            case "minute": return dt.plusMinutes(n);
            case "second": return dt.plusSeconds(n);
            default: throw new ExprEvalException("unknown datepart: " + part);
        }
    }

    private static Object datedif(List<Object> a) {
        checkArity("Datedif", a, 3, 3);
        if (anyNull(a)) {
            return null;
        }
        LocalDate start = asDate(a.get(0));
        LocalDate end = asDate(a.get(1));
        String unit = str(a.get(2)).toUpperCase(java.util.Locale.ROOT);
        switch (unit) {
            case "Y": return BigDecimal.valueOf(ChronoUnit.YEARS.between(start, end));
            case "M": return BigDecimal.valueOf(ChronoUnit.MONTHS.between(start, end));
            case "D": return BigDecimal.valueOf(ChronoUnit.DAYS.between(start, end));
            default: throw new ExprEvalException("unknown datedif unit: " + str(a.get(2)));
        }
    }
}
