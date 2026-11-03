package io.rowweave.expr;

import io.rowweave.DataType;
import io.rowweave.DataSchema;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Compile-time field validation and result type inference for expression ASTs. */
public final class ExprSchema {

    private ExprSchema() {
    }

    public static void validateReferences(DataSchema meta, Expr expression) {
        validateReferences(meta, expression, Collections.emptySet());
    }

    public static void validateReferences(
            DataSchema meta, Expr expression, Map<String, Object> namedParams) {
        Set<String> names = namedParams == null
                ? Collections.emptySet() : namedParams.keySet();
        validateReferences(meta, expression, names);
    }

    private static void validateReferences(DataSchema meta, Expr expression, Set<String> namedParams) {
        if (expression instanceof Expr.FieldRef) {
            String name = ((Expr.FieldRef) expression).name;
            if (meta.getFieldIndex(name) < 0 && !namedParams.contains(name)) {
                throw new IllegalArgumentException("unknown field: " + name);
            }
            return;
        }
        if (expression instanceof Expr.Aliased) {
            validateReferences(meta, ((Expr.Aliased) expression).expr, namedParams);
        } else if (expression instanceof Expr.UnaryOp) {
            validateReferences(meta, ((Expr.UnaryOp) expression).operand, namedParams);
        } else if (expression instanceof Expr.BinaryOp) {
            Expr.BinaryOp binary = (Expr.BinaryOp) expression;
            validateReferences(meta, binary.left, namedParams);
            validateReferences(meta, binary.right, namedParams);
        } else if (expression instanceof Expr.IsNull) {
            validateReferences(meta, ((Expr.IsNull) expression).operand, namedParams);
        } else if (expression instanceof Expr.Like) {
            Expr.Like like = (Expr.Like) expression;
            validateReferences(meta, like.value, namedParams);
            validateReferences(meta, like.pattern, namedParams);
        } else if (expression instanceof Expr.In) {
            Expr.In in = (Expr.In) expression;
            validateReferences(meta, in.value, namedParams);
            for (Expr item : in.items) validateReferences(meta, item, namedParams);
        } else if (expression instanceof Expr.Cast) {
            validateReferences(meta, ((Expr.Cast) expression).operand, namedParams);
        } else if (expression instanceof Expr.Case) {
            Expr.Case caseExpr = (Expr.Case) expression;
            if (caseExpr.subject != null) validateReferences(meta, caseExpr.subject, namedParams);
            for (Expr when : caseExpr.whens) validateReferences(meta, when, namedParams);
            for (Expr then : caseExpr.thens) validateReferences(meta, then, namedParams);
            if (caseExpr.elseExpr != null) validateReferences(meta, caseExpr.elseExpr, namedParams);
        } else if (expression instanceof Expr.FuncCall) {
            for (Expr arg : ((Expr.FuncCall) expression).args) {
                validateReferences(meta, arg, namedParams);
            }
        }
    }

    /** Infers the concrete runtime type of a computed select expression. */
    public static DataType inferComputedType(DataSchema meta, Expr expression) {
        if (expression instanceof Expr.Aliased) {
            return inferComputedType(meta, ((Expr.Aliased) expression).expr);
        }
        if (expression instanceof Expr.FieldRef) {
            int index = meta.getFieldIndex(((Expr.FieldRef) expression).name);
            if (index < 0) throw new IllegalArgumentException(
                    "unknown field: " + ((Expr.FieldRef) expression).name);
            return computedFieldType(meta.getField(index).getDataType());
        }
        if (expression instanceof Expr.NumberLit || expression instanceof Expr.Param) {
            return DataType.BigDecimalType;
        }
        if (expression instanceof Expr.StringLit) return DataType.StringType;
        if (expression instanceof Expr.Macro) {
            Expr.Macro.Kind kind = ((Expr.Macro) expression).kind;
            return kind == Expr.Macro.Kind.NULL ? DataType.NullType : DataType.BooleanType;
        }
        if (expression instanceof Expr.IsNull
                || expression instanceof Expr.Like
                || expression instanceof Expr.In) {
            return DataType.BooleanType;
        }
        if (expression instanceof Expr.UnaryOp) {
            return "not".equals(((Expr.UnaryOp) expression).op)
                    ? DataType.BooleanType : DataType.BigDecimalType;
        }
        if (expression instanceof Expr.BinaryOp) {
            Expr.BinaryOp binary = (Expr.BinaryOp) expression;
            if (isPredicate(binary.op)) return DataType.BooleanType;
            if ("+".equals(binary.op)
                    && (inferComputedType(meta, binary.left) == DataType.StringType
                    || inferComputedType(meta, binary.right) == DataType.StringType)) {
                return DataType.StringType;
            }
            return DataType.BigDecimalType;
        }
        if (expression instanceof Expr.Cast) {
            return castType(((Expr.Cast) expression).targetType);
        }
        if (expression instanceof Expr.Case) {
            Expr.Case caseExpr = (Expr.Case) expression;
            DataType common = DataType.NullType;
            for (Expr then : caseExpr.thens) common = merge(common, inferComputedType(meta, then));
            if (caseExpr.elseExpr != null) {
                common = merge(common, inferComputedType(meta, caseExpr.elseExpr));
            }
            return common;
        }
        if (expression instanceof Expr.FuncCall) {
            return functionType(meta, (Expr.FuncCall) expression);
        }
        throw new IllegalArgumentException(
                "cannot infer expression type: " + expression.getClass().getSimpleName());
    }

    private static DataType computedFieldType(DataType type) {
        return type == DataType.IntegerType || type == DataType.LongType
                ? DataType.BigDecimalType : type;
    }

    private static boolean isPredicate(String op) {
        return "and".equals(op) || "or".equals(op) || "xor".equals(op)
                || "=".equals(op) || "<>".equals(op) || "!=".equals(op)
                || "<".equals(op) || "<=".equals(op) || ">".equals(op) || ">=".equals(op);
    }

    private static DataType castType(String target) {
        switch (target.toLowerCase(Locale.ROOT)) {
            case "integer": case "int": case "long": case "bigdecimal": case "decimal":
                return DataType.BigDecimalType;
            case "boolean": case "bool": return DataType.BooleanType;
            case "string": return DataType.StringType;
            case "date": return DataType.DateType;
            case "timestamp": return DataType.TimestampType;
            default: throw new IllegalArgumentException("unknown cast target type: " + target);
        }
    }

    private static DataType functionType(DataSchema meta, Expr.FuncCall call) {
        String name = call.name.toLowerCase(Locale.ROOT);
        switch (name) {
            case "abs": case "round": case "int": case "integer": case "long":
            case "decimal": case "bigdecimal": case "len": case "year": case "month":
            case "day": case "hour": case "minute": case "second": case "datedif":
                return DataType.BigDecimalType;
            case "boolean": case "bool": case "contains": case "startswith": case "endswith":
                return DataType.BooleanType;
            case "string": case "concat": case "lower": case "upper": case "substr":
            case "left": case "right": case "replace": case "to_char":
                return DataType.StringType;
            case "now": return DataType.TimestampType;
            case "date": case "to_date": return DataType.DateType;
            case "dateadd":
                if (call.args.size() >= 3
                        && inferComputedType(meta, call.args.get(2)) == DataType.TimestampType) {
                    return DataType.TimestampType;
                }
                if (!call.args.isEmpty() && call.args.get(0) instanceof Expr.StringLit) {
                    String part = ((Expr.StringLit) call.args.get(0)).value.toLowerCase(Locale.ROOT);
                    if ("hour".equals(part) || "minute".equals(part) || "second".equals(part)) {
                        return DataType.TimestampType;
                    }
                    return DataType.DateType;
                }
                // A row-dependent datepart can select a time unit. Dateadd then returns a
                // LocalDateTime even when its input is a Date, so use the lossless temporal
                // carrier instead of declaring DateType and failing in downstream codecs.
                return DataType.TimestampType;
            default: throw new IllegalArgumentException("unknown function: " + call.name);
        }
    }

    private static DataType merge(DataType left, DataType right) {
        if (left == DataType.NullType) return right;
        if (right == DataType.NullType || left == right) return left;
        if (isNumeric(left) && isNumeric(right)) return DataType.BigDecimalType;
        throw new IllegalArgumentException(
                "case branches have incompatible types: " + left + " and " + right);
    }

    private static boolean isNumeric(DataType type) {
        return type == DataType.IntegerType || type == DataType.LongType
                || type == DataType.BigDecimalType;
    }
}
