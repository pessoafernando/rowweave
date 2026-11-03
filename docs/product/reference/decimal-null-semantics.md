---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Decimal and null semantics

This page is the single authoritative statement of how RowWeave rounds
`BigDecimal` values and how it treats null. Every Batch operator, Stream
window, and expression follows the rules here. Other pages route to this page
instead of restating the rules, and a narrower adapter that deliberately
differs says so on its own page.

## Scope and authority

The rules are the contract for the `0.2.0` preview. Each rule names the
production source that implements it and the test that pins the behaviour. The
[capability matrix](capability-matrix.md) remains the authority for what the
release supports. [Batch behavior](batch-behavior.md) maps these rules onto
individual Batch operators, and [Stream behavior](stream-behavior.md) maps them
onto stateful Stream operators.

## Decimal scale and rounding

Precision is preserved while the engine reads, combines, spills and merges
partial state. Rounding happens exactly once, on the value that leaves the
engine:

* Output scale is 10 decimal places with `RoundingMode.HALF_UP`.
* The engine performs the rounding. An application that adds its own
  `setScale` call defines a different output contract and owns the difference.
* A null aggregate stays null. Rounding never turns a null into zero.

`rowweave-core/src/main/java/io/rowweave/ops/SumAggregator.java:19` declares the
`OUTPUT_SCALE` constant, and `SumAggregator.java:109` applies
`setScale(OUTPUT_SCALE, RoundingMode.HALF_UP)` to the value the aggregator
emits. The Stream runtime rounds once when a window finalizes, in
`rowweave-core/src/main/java/io/rowweave/stream/runtime/FinalEventTimeDecimalWindow.java:445-446`.
Tests: `SensitivePointEquivalenceTest#sumScale10RoundOnce`,
`DataSetApiBackfillTest#sum_scalar`, and `SpillWiringTest#groupBySpillMatchesHeap`
for the spill path.

Division and the `round` function are the two places where a scale is chosen at
evaluation time rather than at output time. Division fixes its own scale so a
repeating result stays finite: `ExprRuntime.java:24` declares `SCALE` as 10 and
`rowweave-core/src/main/java/io/rowweave/expr/ExprRuntime.java:133` evaluates
`OP_DIV` with `a.divide(b, SCALE, RoundingMode.HALF_UP)`. The compiled path uses
the same rule, and `CompiledExprEngineTest#arithmeticBitEqual` compares the two.
`ExprFunctions.java:186` applies `setScale(scale, RoundingMode.HALF_UP)` for the
`round` function, whose second argument defaults to scale 0.

## Aggregation and null

| Aggregate | Null behavior | Source |
|---|---|---|
| `SUM` | Null values are skipped. A group whose values are all null, or empty, produces null rather than zero. Non-null values accumulate at full precision. | `rowweave-core/src/main/java/io/rowweave/ops/SumAggregator.java:17` |
| `COUNT(*)` | Every row counts, including rows whose cells are null. An empty set is 0. | `rowweave-core/src/main/java/io/rowweave/ops/CountAggregator.java:8` |
| `COUNT(field)` | Counts rows whose named field is non-null. | `rowweave-core/src/main/java/io/rowweave/ops/CountAggregator.java:11` |
| `MAX` / `MIN` | Null values are ignored. A group with nothing non-null produces null. | `rowweave-core/src/main/java/io/rowweave/ops/MaxAggregator.java:11`, `rowweave-core/src/main/java/io/rowweave/ops/MinAggregator.java:11` |

Numeric aggregates compare by numeric value, so `10` and `10.0` are equal.
`ValueComparator.java:11` is the shared comparison used by `MAX`, `MIN`, and
`DISTINCT`. Tests: `SensitivePointEquivalenceTest#sumNullSkippedEmptyGroupNull`,
`DataSetApiBackfillTest#count_field`, and
`SensitivePointEquivalenceTest#wholeAggEmptyTable`.

## Null in grouping

A null grouping key is a value. It forms its own group, is never dropped, and
is never merged with another key. This holds under parallel placement, where
the null key is routed by the same partitioning rule as any other key.

Grouping keys that must be ordered for merge and block-boundary decisions use
one total order declared in
`rowweave-core/src/main/java/io/rowweave/ops/GroupKeyComparator.java:12`: a null
key sorts last, and values that compare equal are ordered further by
`BigDecimal` scale, so that an equal comparison and exact key agreement mean the
same thing. Ordering query output is a separate question, answered by the
ordering rules below.

Tests: `SensitivePointEquivalenceTest#nullParticipatesInGrouping`,
`SingleMachineKeyedShuffleTest#nullKeyGroupedUnderParallelism`, and
`SensitivePointEquivalenceTest#distinctScaleSensitive`.

## Null in joining

A null join key never matches anything, including another null key. This is the
SQL equality rule, applied on both sides of every built-in join, so a null key
cannot quietly produce a cross-product result in an enrichment pattern. An
outer join still preserves the unmatched row and fills the absent side with null
cells.

`rowweave-core/src/main/java/io/rowweave/ops/join/HashJoin.java:184-187` shows
the build side: a row whose key contains a null is entered into the match table
only when the join retains unmatched rows from that side.

Tests: `SensitivePointEquivalenceTest#joinNullNotEqual`,
`SingleMachineJoinShuffleTest#leftOuterNullNotEqualParallel`, and
`SingleMachineJoinShuffleTest#innerJoinParallelBitIdenticalToOracle`.

## Null in ordering

Each sort field carries a null order declared in
`rowweave-core/src/main/java/io/rowweave/ops/SortField.java:18` with values
`DEFAULT`, `NULLS_FIRST`, and `NULLS_LAST`.

* With `NULLS_FIRST` and `NULLS_LAST`, null placement is absolute. It ignores
  ascending versus descending, and only non-null values compare by value with
  the direction applied. This is `RowComparator.java:73`.
* With `DEFAULT`, a null takes a value-based position: a numeric null behaves as
  zero, and any other null counts as the smallest value. Both move with the sort
  direction. This is `RowComparator.java:88`.
* Equal sort keys return zero from the comparator, and the sort is stable, so
  input order is preserved for ties.

Tests: `SortBufferTest#nullsLastAbsolute`, `TopTest#nullOrderingDefaultAndAbsolute`,
`RowComparatorTest#nullsLastIgnoresDirection`, and
`SensitivePointEquivalenceTest#orderByNullsLast`.

## Null in predicates and expressions

An expression involving null follows SQL three-valued logic, stated in the
interpreter header at
`rowweave-core/src/main/java/io/rowweave/expr/ExprInterpreter.java:17-19`: a
null operand makes arithmetic and concatenation null, and a comparison
involving null is null, which means unknown. `AND`, `OR`, `XOR`, and `NOT`
follow the corresponding truth tables.

A predicate collapses unknown to false, so a row whose filter condition is
unknown does not pass. `ExprInterpreter.java:70` is that conversion. Use
`IS NULL` or `IS NOT NULL` to test absence directly.

Tests: `ExprInterpreterTest#comparisonAndNullThreeValued` and
`SensitivePointEquivalenceTest#filterNullComparisonUnknownDrops`.

## Physical types and null conversion

A field carries exactly one of the closed set of types declared in
`rowweave-core/src/main/java/io/rowweave/DataType.java:7-16`. A row is accessed
by index, and every typed accessor returns null for a null value rather than a
substitute:

| Type | Accessor | Boundary |
|---|---|---|
| `BooleanType` | `getBoolean` | `rowweave-core/src/main/java/io/rowweave/DataRow.java:97` |
| `StringType` | `getString` | `rowweave-core/src/main/java/io/rowweave/DataRow.java:65` |
| `IntegerType` | `getInteger` | 32-bit signed, `rowweave-core/src/main/java/io/rowweave/DataRow.java:70` |
| `LongType` | `getLong` | 64-bit signed, `rowweave-core/src/main/java/io/rowweave/DataRow.java:75` |
| `BigDecimalType` | `getBigDecimal` | `rowweave-core/src/main/java/io/rowweave/DataRow.java:80` |
| `DateType` | `getDate` | `rowweave-core/src/main/java/io/rowweave/DataRow.java:116` |
| `TimestampType` | `getTimestamp` | `rowweave-core/src/main/java/io/rowweave/DataRow.java:121` |

`getBigDecimal` widens `Integer` and `Long` values numerically and converts any
other `Number` from its string form, so no precision is lost by routing through
`double`. A value that cannot be converted fails with a `ClassCastException`
that names the index, which is `rowweave-core/src/main/java/io/rowweave/DataRow.java:94`.
Nothing is coerced into a default, and null is never substituted for a bad
value.

A computed projection normalizes a date or timestamp value to the declared type
at the operator boundary:
`rowweave-core/src/main/java/io/rowweave/ops/Select.java:163-172` maps a
`LocalDate` assigned to a `DateType` field and a `LocalDateTime` assigned to a
`TimestampType` field, and lands a `LocalDate` assigned to `TimestampType` at
the start of that day. No time zone conversion is applied anywhere: a date or
timestamp value means what it says.

A field's name and alias are defined in
`rowweave-core/src/main/java/io/rowweave/Field.java:9`, and an absent alias
falls back to the name.

## Changing these rules

For an end-to-end demonstration of the published scale, ordering, and file round-trip rules, see
[`AmountRollupExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/AmountRollupExample.java).

These rules are engine behaviour that applications depend on, so a change is a
contract change. [Versioned release compatibility](versioned-release-v1.md)
freezes the published statement of decimal scale, rounding, and the null rules
for grouping, sorting, and joining. Any proposal to widen or narrow a rule here
must say how that frozen contract is honored or superseded.
