---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Batch behavior reference

This page is the normative reference for Batch `DataSet` behavior. The
[capability matrix](capability-matrix.md) states what is supported; the
[API inventory](api-inventory.md) traces symbols to source. This page defines
row semantics, null and decimal policy, schema and validation behavior,
execution graph boundaries, breaker spill, and the accepted or refused shapes
that those capabilities imply.

RowWeave does not have a separate Batch language. `RowWeave.createEnvironment`
returns an `ExecutionEnvironment`; `fromSource` starts a `DataSet`; composition
returns immutable dataset nodes; and `execute` submits all attached sinks as
one graph. Code samples in the [first job](../quickstart/first-job.md) are
runnable. Contract statements on this page point to focused tests.

## Contract index

| Capability matrix row | Reference | Tested demonstration |
|---|---|---|
| Map, filter, projection, calculation | [Operators](#operator-semantics) | `DataSetApiCompatTest#filterWithNamedParams`; `ExprInterpreterTest#comparisonAndNullThreeValued`; `CompiledExprEngineTest#arithmeticBitEqual`; `ScenarioLabFindingsContractTest#selectInfersSerializableTypesForComputedExpressions` |
| Grouping and aggregation | [Aggregation](#aggregation) | `DataSetApiBackfillTest#sum_scalar`; `DataSetApiBackfillTest#count_field`; `DataSetBreakerTest#mixedPassThroughAndBreakerChain`; `SingleMachineKeyedShuffleTest#nullKeyGroupedUnderParallelism` |
| Join | [Joins and co-groups](#joins-and-co-groups) | `DataSetApiCompatJoinTest#joinWithFunc`; `SingleMachineJoinShuffleTest#innerJoinParallelBitIdenticalToOracle`; `SingleMachineJoinShuffleTest#leftOuterNullNotEqualParallel` |
| Sort, TopN, distinct | [Ordering, TopN, and distinct](#ordering-topn-and-distinct) | `DataSetBreakerTest#orderByEndToEnd`; `DataSetBreakerTest#topEndToEnd`; `DataSetBreakerTest#distinctEndToEnd` |
| Multiple inputs/outputs | [Whole-job execution](#whole-job-execution) | `FanoutMultiOutputEquivalenceTest#fiveOutputFanoutCorrectAndComputedOnce`; `DataSetApiCompatJoinTest#fromMultipleInputs`; `DiamondReJoinTest#diamondSrcTeeThenJoin` |
| Schema conversion | [Schema conversion](#schema-conversion) | `SchemaConversionModelTest#freezesOrderedRulesAndBuildsExactTargetMeta`; `SchemaConversionModelTest#acceptsOnlyTheClosedLosslessCastMatrix`; `SchemaConversionModelTest#rejectsUnboundedOrAmbiguousMappingsAndBindsDatasetInputFingerprint` |
| Validation | [Row validation](#row-validation) | `RowValidationModelTest#freezesOrderedClosedRulesAndCanonicalDescriptors`; `RowValidationModelTest#rejectsTypeMismatchesInvalidRangesAndUnboundedModels`; `RowValidationModelTest#datasetBindingRejectsSchemaDriftAndPreservesExactOutputMeta` |
| Pipeline composition | [Pipeline composition](#pipeline-composition) | `PipelineCompositionExampleTest#coversPublishedCompositionScenarios`; `PipelineCompositionExampleTest#exampleBatchCarrierPassesThePortablePreflight` |
| Batch deployment modes | [Run-mode parity](#run-mode-parity) | `RunModesExample`; `DataSetApiBackfillTest#scalarSum_clusterMatchesSingle` |
| Unbounded graph refusal | [Accepted and refused shapes](#accepted-and-refused-shapes) | `ScenarioLabFindingsContractTest#unionRejectsWidthOrTypeDifferenceAtGraphConstruction`; `ScenarioLabFindingsContractTest#binaryKeyedOperatorsRejectDifferentKeyTypesAtGraphConstruction`; `ScenarioLabFindingsContractTest#expressionCompilationRejectsUnknownFieldsEvenWithoutRows` |

## Operator semantics

`DataSet` operators build immutable nodes. A returned node is a new plan node;
the upstream graph is not modified. A method validates its schema and operands
at graph construction when the contract can be checked without reading rows.
Otherwise, data-dependent failures happen during execution and fail the job.

| Operator family | Contract |
|---|---|
| `filter` | Keep a row only when a typed predicate returns true. A lambda exception fails the job. Expression comparison with null is three-valued: unknown filters the row out. Field resolution happens before execution; an unknown field fails graph construction. |
| `map` | Apply a one-to-one `MapFunction`. The output schema remains the input schema unless a documented adapter defines a new schema. |
| `flatMap` | Apply a one-to-many `FlatMapFunction` through `Collector`. The output schema remains the input schema. |
| `mapPartition` | Apply a `MapPartitionFunction` to one partition through `PartitionDataRowCursor` and `Collector`. Partition count follows the job execution model, not an application-side partition-number contract. |
| `select` | Select ordered columns by index, or evaluate ordered expressions by name. The output schema is explicit: source projection preserves source names and types; computed expressions receive inferred names and types. |
| `addFields` | Append declared fields with canonical values. The output width is the input width plus the number of added fields. |
| `removeFields` | Remove named fields and preserve the remaining source order. |
| `union` | Concatenate two positionally compatible schemas. Names may differ; count, order, and cell types must match. |
| custom group functions | Receive an iterable group or partition and emit through `Collector`. A function returning a null result schema fails graph construction. |

Expressions use one engine contract. The default engine may compile an
expression, but compiled and interpreted evaluation must produce the same
result. Bare identifiers resolve first to schema fields and then to named
parameters; a parameter cannot shadow a field. Parsing and field resolution
fail before rows are read.

Null behavior in expressions follows three-valued logic. Arithmetic with null
produces null. Comparison with null is unknown. `AND`, `OR`, and `NOT` follow
that unknown value. Use `IS NULL` or `IS NOT NULL` to test absence directly.

## Null and decimal policy

[Decimal and null semantics](decimal-null-semantics.md) is the single canonical
statement of decimal scale, output rounding, and the null rules for grouping,
aggregation, joining, ordering, and predicates. This page does not restate them.
It only binds them to the Batch operators.

Two points are Batch-specific rather than general semantics:

* Each `SortField` in an `orderBy` or `Top` specification declares its own
  `DEFAULT`, `NULLS_FIRST`, or `NULLS_LAST` value, so the null policy of a sort
  is a per-key choice made at graph construction. The convenience
  `orderBy(String...)` form takes ascending direction with the default policy.
* `distinct` serializes null cells into the key, so an all-null key group is a
  real group rather than an absent one, and duplicates within it collapse.

A built-in decimal aggregate accumulates exactly while reading, combining, and
spilling, and rounds once at output, as
[decimal and null semantics](decimal-null-semantics.md#decimal-scale-and-rounding)
states. Do not apply another `setScale` call to a built-in sum before a sink
unless your application deliberately defines a different output contract.

## Aggregation

`sum`, `count`, `max`, and `min` without `groupBy` are whole-table aggregates
and produce one row with one value. `count()` is `COUNT(*)`; `count(field)` is
the non-null field count. Convenience aliases replace the generated output
field name without changing semantics.

`groupBy(fields)` returns a `Grouper`; `sum`, `count`, `max`, and `min` append
measures to the key fields. `ParallelGroupByAgg.of(...)` composes the same
keyed aggregate explicitly and can emit multiple measures in one aggregate.
`reduceGroup`, `combineGroup`, and `combineReduceGroup` support bounded custom
group logic; the function's result schema is the output schema.

Built-in keyed aggregation uses managed memory. A group can spill; aggregation
state merge is part of the same exact contract. Parallel execution routes all
rows for a key to one group aggregate and canonicalizes result order before
comparison where the operator requires determinism. Without an explicit
`orderBy`, consumers must treat group order as unspecified.

## Joins and co-groups

`join`, `leftJoin`, `rightJoin`, and `fullJoin` return a `JoinDataSet`. Calling
`on(key)` or `on(leftKeys, rightKeys)` creates an equality join whose default
concatenated output is every left field followed by every right field. Calling
`withFunc` passes matched rows, or the null half for an unmatched outer side,
to a `JoinFunction` whose result schema defines the output. `coGroup` passes
the two matching row groups to a `CoGroupFunction`.

Join construction requires the same key width on both sides. Corresponding key
types must match, except that RowWeave treats integer, long, and decimal keys
as one numeric key family. Different field names are allowed. Null keys never
match.

The physical plan supports `HASH`, `SORT_MERGE`, and `AUTO`. With no hint, AUTO
selects a build side or sort-merge path from bounded probes, memory, and input
completion; it can move from a resident hash to sort-merge when the hash budget
is exceeded. An explicit `JoinHint` forces the named path. Physical selection
does not change logical join type, null-key, output schema, or row contracts.

## Ordering, TopN, and distinct

`orderBy` accepts an `OrderBy` specification with one `SortField` per key.
Each key independently declares ascending or descending direction and
`DEFAULT`, `NULLS_FIRST`, or `NULLS_LAST`. A convenience `orderBy(String...)`
uses ascending order and the default null policy. Sorting is a bounded breaker:
it consumes its upstream before emitting.

`top(Top)` maintains a bounded top-N heap and falls back to a managed external
sort when the heap budget is exhausted. `Top` appends a stable sequence as the
final tie-breaker. The convenience `top(n)` selects the first `n` rows in
source order; it is not a ranked maximum unless an explicit `Top` specification
requests one.

`distinct` deduplicates by the selected key projection and emits the first
representative row. `distinct()` without fields means the whole row; named
fields form the distinct key. Null cells participate in the serialized key, so
one all-null key group is distinct from an absent group and duplicates within
that group collapse.

## Whole-job execution

`ExecutionEnvironment.execute` submits one graph containing every source,
operator, and attached sink. It does not return rows to the client. A sink
receives rows at the graph boundary; in a cluster, the Worker writes the sink.
`CollectingSink` is useful only in an addressable in-process test or local
demonstration; remote workers cannot return a client-side collection.

A shared subtree has one logical identity in the submitted graph and executes
once even when it feeds multiple downstream branches and sinks. A source that
is reused by independent binary branches remains replayable; a diamond graph
may materialize replay reads on each branch while preserving the correct join
result. This distinction is intentional: shared fan-out does not require a
consumer to re-read the source, while two independent inputs to a binary
operator preserve their own replay positions.

Multiple sources can enter one graph with `fromSource(Source...)`. The sources
must have the same schema where they feed one `DataSet`; separate source nodes
may feed different branches. A source failure is fail-fast: execution does not
substitute a partial result.

## Schema conversion

`SchemaConversion.builder` defines an ordered, bounded mapping from an exact
source schema to an exact target schema. Rules support:

* `required(source, target, cast)` — source cell must be non-null;
* `nullable(source, target, cast)` — source cell may be null;
* `defaulted(target, value)` — output-only field with a typed value or null.

The mapping accepts name lookup only when one primary field name matches
exactly; intentional duplicate primary names require an index. It accepts only
the closed lossless cast matrix: identity, integer to long, integer to
decimal, and long to decimal. Narrowing or lossy conversion, parsing strings,
and unconstrained null output are refused. The builder freezes defaults,
including mutable date and timestamp values, and rejects non-canonical values.

`convertSchema` binds the mapping to the dataset's exact schema fingerprint
before execution. A different name, alias, type, or width fails with
`SCHEMA_MAPPING_INPUT_MISMATCH`. During projection, a row width mismatch or a
required null violation fails closed with `SCHEMA_ROW_WIDTH_MISMATCH` or
`SCHEMA_NULL_VIOLATION`; the operator does not emit a partially built row.

## Row validation

`RowValidation.builder` defines an ordered, bounded rule set on an exact input
schema. The closed rule kinds are:

| Kind | Rule |
|---|---|
| `NOT_NULL` | The cell is present. |
| `INTEGER_RANGE_CLOSED` | Integer is within both inclusive bounds. |
| `LONG_RANGE_CLOSED` | Long is within both inclusive bounds. |
| `DECIMAL_RANGE_CLOSED` | Decimal is within both inclusive bounds. |
| `STRING_UTF8_BYTES_RANGE_CLOSED` | UTF-8 encoded length is within both inclusive bounds. |

Rule IDs are stable and unique. Field names resolve exactly and uniquely;
indexes are accepted for intentionally duplicate names. Model construction
rejects empty rule sets, type/rule mismatches, reversed ranges, unbounded
ranges, oversized UTF-8 bounds, duplicate or malformed rule IDs, too many
rules, and non-canonical descriptor values.

`validateRows` binds the model to an exact input schema fingerprint and keeps
the output schema identical to the input. Execution is fail-closed. A row width
mismatch, cell type mismatch, or rule violation produces the corresponding
typed quality failure rather than a silently repaired row. For routing rather
than stopping, the source-position classification API separates valid and
rejected routes through `DataRowClassification`.

## Breakers and spill

Grouping, aggregation, sorting, TopN, and distinct are pipeline breakers. A
breaker consumes its upstream working set before producing downstream rows;
pass-through operators remain streaming. Every breaker works within an explicit
managed-memory budget.

When a working set exceeds its budget, the operator spills serialized state or
ordered runs to the job workspace and then merges or replays them. Spill state
preserves the semantic contract: decimal sums retain unrounded state, ordered
runs preserve comparator policy, distinct preserves first representative
choice, and join/aggregate state remains keyed. Disk failures fail closed. The
managed budget is shared across a job; a second breaker does not multiply the
resource promise. Skew is handled by bounded progress and spill rather than by
silently dropping a hot key.

## Pipeline composition

`rowweave-pipeline` composes metadata for already compiled Batch and Stream
plan artifacts. It does not execute operators or submit jobs. The inter-stage
boundary is a versioned dataset from a materialized producer sink to a consumer
source; schemas must match, payload formats are explicit, and resource limits
are declared before allocation. See
[pipeline boundary](pipeline-boundary.md) for the composition shape authority.

## Run-mode parity

Local, InProcess, and real-cluster Batch modes execute the same plan graph,
operator implementations, join strategy, decimal policy, and row semantics.
They differ in process placement, resource ownership, sink reachability, and
diagnostics. A result readable by a client-side collection is not evidence of
cluster parity if the sink cannot be written by a Worker; use a worker-writable
sink for cross-process verification. See [run modes](../guides/run-modes.md).

## Accepted and refused shapes

Accepted Batch shapes are finite, typed, and admittable within declared
resources:

* pass-through chains and expressions with resolved fields;
* keyed breakers with explicit or derived output schemas;
* equality joins and co-groups with compatible key families;
* unions with positionally identical cell types;
* multiple sources, shared subtrees, and multiple sinks in one graph;
* explicit schema conversion and bounded validation models;
* Pipeline composition of portable artifacts at the metadata boundary.

Refusal happens at graph construction when a static contract is invalid, and
during admission or execution when a resource, replay, schema, or runtime
invariant cannot be maintained. In particular:

* union refuses width or positional type differences; field names may differ;
* keyed binary operators refuse incompatible key types;
* expressions refuse unknown fields and malformed syntax before rows are read;
* schema conversion refuses lossy casts and ambiguous mappings;
* validation refuses unbounded or non-canonical rule descriptors;
* output collection enforces explicit row and byte compatibility limits;
* an unbounded source, sort, join, aggregate, or unlimited materialization is
  outside the Batch admission model and is not approximated. Stream also
  rejects its closed-set unbounded forms as described in
  [capability matrix](capability-matrix.md).

## Verification

Run the focused behavior evidence:

```bash
mvn -q -pl rowweave-core test \
  -Dtest='DataSetApiBackfillTest,DataSetApiCompatTest,DataSetApiCompatJoinTest,DataSetBreakerTest,FanoutMultiOutputEquivalenceTest,SchemaConversionModelTest,RowValidationModelTest,SingleMachineJoinShuffleTest,SingleMachineKeyedShuffleTest,SpillWiringTest,DiamondReJoinTest,ScenarioLabFindingsContractTest'
```

Run the documentation parity gate:

```bash
python3 tools/oss_batch_docs_parity_gate.py
```
