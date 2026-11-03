---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Expression syntax and evaluation

An expression is a string a user writes into `filter` or `select` instead of a
Java lambda. This page states the grammar, the value rules, the two engines, and
the exact failure each invalid expression produces. The rules are measured by
calling the public API, and every claim below names the source line or the test
that pins it.

For the null and decimal rules that the same engine follows outside an
expression, see [decimal and null semantics](decimal-null-semantics.md).

## Where an expression enters the API

| Entry point | Form |
|---|---|
| `DataSet.filter(String)` | `rowweave-core/src/main/java/io/rowweave/DataSet.java:291` |
| `DataSet.filter(String, Map)` named parameters | `DataSet.java:301` |
| `DataSet.select(String...)` one expression per column | `DataSet.java:325` |
| `Filter.fromExpression` | `rowweave-core/src/main/java/io/rowweave/ops/Filter.java:36` |
| `Select.of` | `rowweave-core/src/main/java/io/rowweave/ops/Select.java:68` |

Both operators hold a compiled handle, not the source text, so a job graph
carrying an expression compiles once at construction. `Expr` is a public AST
type, so `DataSet` and the operators can also accept an already parsed tree
through `ExprEngine.compile(DataSchema, Expr)`, declared at
`rowweave-core/src/main/java/io/rowweave/expr/ExprEngine.java:28`.

## Grammar

The parser is a fixed-precedence recursive-descent parser. Each level is one
method, so precedence is structural rather than table-driven:

| Level | Method | Operators |
|---|---|---|
| alias | `parseAliased` (`io/rowweave/expr/ExprParser.java:37`) | `expr AS name` |
| logical xor | `parseXor` (`io/rowweave/expr/ExprParser.java:52`) | `XOR` |
| logical or | `parseOr` (`io/rowweave/expr/ExprParser.java:61`) | `OR` |
| logical and | `parseAnd` (`io/rowweave/expr/ExprParser.java:70`) | `AND` |
| logical not | `parseNot` (`io/rowweave/expr/ExprParser.java:79`) | `NOT` |
| comparison | `parseComparison` (`io/rowweave/expr/ExprParser.java:87`) | `= <> < <= > >= LIKE` |
| null test | `parseIsNull` (`io/rowweave/expr/ExprParser.java:118`) | `IS [NOT] NULL` |
| membership | `parseIn` (`io/rowweave/expr/ExprParser.java:134`) | `IN`, `NOT IN` |
| additive | `parseAddSub` (`io/rowweave/expr/ExprParser.java:154`) | `+ -` |
| multiplicative | `parseMulDiv` (`io/rowweave/expr/ExprParser.java:163`) | `* / %` |
| unary | `parseUnary` (`io/rowweave/expr/ExprParser.java:172`) | `- +` |
| primary | `parsePrimary` (`io/rowweave/expr/ExprParser.java:184`) | literals, fields, `?`, calls, `CAST`, `CASE` |

`ExprParser.parse` (`rowweave-core/src/main/java/io/rowweave/expr/ExprParser.java:24`)
is the single entry point. A null or blank source fails at `ExprParser.java:26`,
and text that leaves unconsumed tokens fails at `ExprParser.java:31` naming the
offending token. The AST node types are nested inside
`rowweave-core/src/main/java/io/rowweave/expr/Expr.java:10`:
`NumberLit` `Expr.java:13`, `StringLit:30`, `Macro:44`, `FieldRef:60`,
`Param:74`, `BinaryOp:88`, `UnaryOp:106`, `IsNull:122`, `Like:138`, `In:154`,
`Cast:180`, `Case:196` and `FuncCall:226`.

The token set is closed: `INTEGER, DECIMAL, STRING, IDENT, KEYWORD, OP, LPAREN,
RPAREN, COMMA, PARAM, EOF` (`rowweave-core/src/main/java/io/rowweave/expr/Token.java:7`).
A string literal is single-quoted and `''` is an escaped quote, implemented in
`rowweave-core/src/main/java/io/rowweave/expr/Lexer.java:11`. `= ` and `==`
are the same comparison: `Filter.fromExpression` normalizes the source text
before compiling (`io/rowweave/ops/Filter.java:37`).

## Value rules

Numbers are `BigDecimal` throughout, including integer literals, which the
evaluator normalizes in `ExprRuntime.normalize`
(`rowweave-core/src/main/java/io/rowweave/expr/ExprRuntime.java:61`). That is
why a computed column and a stored integer column compare equal instead of
failing on type.

| Rule | Evidence |
|---|---|
| division is scale 10, `RoundingMode.HALF_UP` | `io/rowweave/expr/ExprRuntime.java:133`, constant declared at `ExprRuntime.java:24` |
| `+ - * %` return `null` when either side is `null` | `io/rowweave/expr/ExprRuntime.java:121` |
| a comparison with a `null` operand is unknown, not false | `io/rowweave/expr/ExprRuntime.java:175` |
| `AND`, `OR`, `XOR`, `NOT` are three-valued | `ExprRuntime.java:212`, `:225`, `:238`, `:248` |
| unknown maps to false when a predicate is tested | `io/rowweave/expr/CompiledExpr.java:21` |
| `LIKE` patterns are `%` and `_`, including line terminators | `io/rowweave/expr/ExprRuntime.java:263` |
| `IN` compares by value, not by identity | `io/rowweave/expr/ExprRuntime.java:287` |
| `CAST` truncates toward zero for integer targets without narrowing arbitrary-precision values | `io/rowweave/expr/ExprRuntime.java:328-330` |

`round(value [, scale])` defaults to scale `0` and rounds `HALF_UP`
(`rowweave-core/src/main/java/io/rowweave/expr/ExprFunctions.java:186`). The
integer casts take the other path: `int`, `integer` and `long` are all bound to
the private implementation at `ExprFunctions.java:189`, which cuts the value to
an integral scale instead of rounding, so it is the one place in the language
where a scale decision is not `HALF_UP`.

## Function library

The library is a registry keyed by lower-cased name
(`rowweave-core/src/main/java/io/rowweave/expr/ExprFunctions.java:33`), so
`ABS`, `Abs` and `abs` dispatch to the same entry. Lookup is in
`ExprFunctions.call` at `ExprFunctions.java:49`.

| Family | Names |
|---|---|
| numeric | `abs`, `round`, `int`, `integer`, `long`, `decimal`, `bigdecimal` |
| text | `string`, `concat`, `contains`, `startswith`, `endswith`, `lower`, `upper`, `len`, `substr`, `left`, `right`, `replace` |
| boolean | `boolean`, `bool` |
| date and time | `now`, `date`, `year`, `month`, `day`, `hour`, `minute`, `second`, `to_date`, `to_char`, `dateadd`, `datedif` |

Documented open-ended rules the language definition leaves to tests: a `null`
argument makes the result `null` except for the zero-argument `now` and `date`;
`substr` starts at position `1` and clips out-of-range bounds rather than
erroring (`ExprFunctions.java:146`); `replace` in its three-argument form
replaces every occurrence and in its four-argument form replaces the first N;
`dateadd` takes a datepart in `Year`, `Month`, `Day`, `Hour`, `Minute`,
`Second`, and an unrecognised datepart fails at `ExprFunctions.java:337`.

## Failure codes

| Condition | Exception | Message anchor |
|---|---|---|
| null or blank source | `ExprParseException` | `io/rowweave/expr/ExprParser.java:26` |
| unconsumed trailing tokens | `ExprParseException` | `io/rowweave/expr/ExprParser.java:31` |
| identifier that is neither a field nor a named parameter | `IllegalArgumentException` `unknown field:` | `io/rowweave/expr/ExprSchema.java:32` |
| unknown function name | `ExprEvalException` | `io/rowweave/expr/ExprFunctions.java:52` |
| wrong argument count | `ExprEvalException` `expects min..max args` | `io/rowweave/expr/ExprFunctions.java:89` |
| unknown datepart or `datedif` unit | `ExprEvalException` | `io/rowweave/expr/ExprFunctions.java:337`, `:365` |

`ExprParseException` and `ExprEvalException` are both unchecked and both are
final: `rowweave-core/src/main/java/io/rowweave/expr/ExprParseException.java:4`
and `rowweave-core/src/main/java/io/rowweave/expr/ExprEvalException.java:4`.
Parse and schema failures happen at compile time, that is, at graph
construction; an unknown function or a bad arity surfaces per row at
evaluation. Message text is not a stable contract; the exception type is.

## Engines and equivalence

`ExprEngineFactory.getDefault()`
(`rowweave-core/src/main/java/io/rowweave/expr/ExprEngineFactory.java:31`)
returns the dynamic compiler unless `-Drowweave.expr.codegen=false` is set, in
which case it returns the interpreter. The property name is a constant at
`ExprEngineFactory.java:19` and the two types are `INTERPRETED` and `COMPILED`
(`ExprEngineFactory.java:13`).

The compiled engine keeps an interpreter instance beside it
(`rowweave-core/src/main/java/io/rowweave/expr/CompiledExprEngine.java:18`) and
falls back to it when code generation declines a node
(`CompiledExprEngine.java:31`), which is the documented degradation path in
`CompiledExprEngine.java:13`. A node the generator does not cover raises
`UnsupportedExprException`
(`rowweave-core/src/main/java/io/rowweave/expr/codegen/UnsupportedExprException.java:7`)
from `AstBytecodeGenerator` at
`rowweave-core/src/main/java/io/rowweave/expr/codegen/AstBytecodeGenerator.java:270`,
and a macro is outside the supported generated subset at
`AstBytecodeGenerator.java:214`. Equivalence with the interpreter is what makes
the fallback invisible.

Named parameters resolve field-first: a bare identifier is looked up as a column
before it is looked up in the parameter map
(`rowweave-core/src/main/java/io/rowweave/expr/ExprEngine.java:35`). Named
parameters are read-only per row, and the compiled engine declares them as the
interpreter-only path at `CompiledExprEngine.java:41`.

## Tested by

- [`ExprInterpreterTest`](../../../rowweave-core/src/test/java/io/rowweave/expr/ExprInterpreterTest.java)
  and [`CompiledExprEngineTest`](../../../rowweave-core/src/test/java/io/rowweave/expr/CompiledExprEngineTest.java):
  the same expression corpus through both engines.
- [`ExprOperatorTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/ExprOperatorTest.java):
  `filter` and `select` driven by expression text.
- [`StreamExpressionContractTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/contract/StreamExpressionContractTest.java)
  and [`StreamExpressionEvaluatorTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/cluster/StreamExpressionEvaluatorTest.java):
  the same language on the Stream side, including its typed failure.

Related: [custom function interfaces](custom-functions.md),
[Batch behavior contracts](batch-behavior.md#operator-semantics),
[decimal and null semantics](decimal-null-semantics.md#decimal-scale-and-rounding),
[errors and refusals](errors-and-refusals.md).
