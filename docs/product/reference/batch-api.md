---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Batch environment and DataSet method reference

This page is the method-level contract for the Batch composition surface: how a
job is created, how a graph is compiled and submitted, and what every `DataSet`
builder accepts, returns, and refuses. For what an operator *computes* — the
result-set rules behind `groupBy`, `join`, `orderBy` — see
[Batch behavior contracts](batch-behavior.md). For the value rules a row must
follow, see [decimal and null semantics](decimal-null-semantics.md).

The exhaustive source-visible declaration set and the frozen `0.2.0` vocabulary
are in [public API inventory](api-inventory.md). Every entry point here is a
public type or method reachable from a published artifact.

## Creating an environment

[`RowWeave`](../../../rowweave-core/src/main/java/io/rowweave/RowWeave.java)
is the static factory. `createEnvironment(String name)` at
`rowweave-core/src/main/java/io/rowweave/RowWeave.java:11` is the single-argument
form; `createEnvironment(String jobName, String jobTitle)` at `:15` is the form
that carries a display title. Both return an `ExecutionEnvironment` with no
injected client, so a submission goes to the process-wide shared in-process
runtime.

| Member | Declaration site | Contract |
|---|---|---|
| `ExecutionEnvironment` | `rowweave-core/src/main/java/io/rowweave/ExecutionEnvironment.java:28` | one job description: name, title, parallelism, sinks |
| `ExecutionEnvironment(String, String)` | `ExecutionEnvironment.java:59` | job name plus display title |
| `ExecutionEnvironment(String, String, JobClient)` | `ExecutionEnvironment.java:64` | injects the client a submission uses; a non-null client is never overridden |
| `MetricsListener` | `ExecutionEnvironment.java:47` | nested `@FunctionalInterface` with one method, `onSinkComplete(ExecutionReport)` at `:48` |

The job name and title are identity only: they select the compiled definition
identity and appear in the plan description. Neither one changes computed
results.

## Environment configuration

| Method | Declaration | Default | Failure and scope |
|---|---|---|---|
| `setParallelism(int)` | `ExecutionEnvironment.java:79` | `1` (`:52`) | `parallelism must be > 0, got <n>` at `:81`; compiled into the graph, so it governs both local and cluster execution |
| `getParallelism()` | `ExecutionEnvironment.java:86` | reads the value that will be compiled | |
| `setCanonicalizeOutput(boolean)` | `ExecutionEnvironment.java:97` | `false` (`:53`) | in-process runtime only; `true` reproduces the byte-for-byte `P=1` order |
| `isCanonicalizeOutput()` | `ExecutionEnvironment.java:101` | reads the flag handed to the runtime | |
| `setMetricsListener(MetricsListener)` | `ExecutionEnvironment.java:109` | `null` | `null` collects nothing |
| `getJobName()` | `ExecutionEnvironment.java:113` | the name passed to `createEnvironment` | |
| `getJobTitle()` | `ExecutionEnvironment.java:117` | falls back to the name | |
| `setDefinitionId(String)` | `ExecutionEnvironment.java:122` | derived | the stable identity carried in the artifact |

Parallelism is a job property rather than a per-operator property:
`toPlanGraph()` compiles it into `PlanGraph.parallelism()`
(`rowweave-core/src/main/java/io/rowweave/plan/PlanGraph.java:86`), and the
Master reads that same value when it sizes a keyed shuffle. Because the graph
carries it, the same plan submitted to a cluster gets the same requested
parallelism it would get locally.

A listener is advisory. At `ExecutionEnvironment.java:350` the report is handed
to the callback inside a guard that swallows a `RuntimeException`, so metrics
observation never changes a job result or turns a successful run into a failed
one.

## Opening a source

| Method | Declaration | Contract |
|---|---|---|
| `fromSource(Source)` | `ExecutionEnvironment.java:127` | one `Source`; schema validated against the first entry |
| `fromSource(Source...)` | `ExecutionEnvironment.java:138` | one logical SOURCE holding homogeneous Sources as splits |
| `DataSet.source(DataSchema, Iterable<DataRow>)` | `rowweave-core/src/main/java/io/rowweave/DataSet.java:178` | static and session-less: `sinkTo` refuses it |

The varargs form does not fold several sources into several nodes. It produces a
single logical SOURCE whose splits the runtime distributes across physical
source instances by the job parallelism, under the same rule locally and on a
cluster. Schemas must agree, which is why the validation is against the first
entry.

The static `DataSet.source(DataSchema, Iterable<DataRow>)` overload is useful
for a computation that ends in a locally-read result rather than a registered
sink. Because it has no owning environment, it can never be registered as a
sink, and `sinkTo` on it throws — see the sink boundary below.

## Sinks and the whole-graph contract

A sink registration is what makes a graph executable.

| Method | Declaration | Failure |
|---|---|---|
| `sinkTo(Sink)` | `rowweave-core/src/main/java/io/rowweave/DataSet.java:210` | `IllegalStateException` at `:213` when the node is not session-bound |
| `sinkTo(String, Sink)` | `DataSet.java:220` | same check at `:223`; the UID becomes the operator UID parameter on the SINK node |
| `uid(String)` | `DataSet.java:201` | stable operator identity for the node |
| `getSchema()` | `DataSet.java:196` | the schema the node emits |

The session is resolved by walking back along the UNARY parent or the BINARY
left input to the SOURCE, so any node derived from a session-bound source can be
registered. A graph built from two unrelated roots where only one is bound fails
that walk, not the write.

`toPlanGraph()` at `ExecutionEnvironment.java:280` compiles the whole job:

1. It refuses an empty sink list with `no sink registered; call
   DataSet.sinkTo(..) before commit` at `:282`.
2. It walks the upstream subtree of every registered sink, de-duplicating by
   object identity through an `IdentityHashMap` at `:286`, so one `DataSet`
   instance feeding several downstreams becomes one `PlanNode`.
3. It appends one SINK node per registered sink at `:290`, carrying the sink UID
   parameter when present.
4. It returns `new PlanGraph(nodes, parallelism)` at `:297`.

Because shared nodes are visited once, a fan-out graph computes the shared
subgraph once per submission rather than once per sink. `toPlanArtifact()` at
`:301` is the versioned form of the same compile and opens no source or sink.
When no explicit definition identity was set, `effectiveDefinitionId()` derives
one from the SHA-256 of the job name, a separator byte, and the title, written
as `legacy-definition-` plus the hex at `:319`.

## Submission and failure timing

`execute()` at `ExecutionEnvironment.java:332` is the single submission path.

| Step | Declaration | What can fail, and when |
|---|---|---|
| compile | `:334` | missing sink, non-unionable schema, unknown field — before any side effect |
| destination check | `:336` | duplicate exclusive destination |
| commit check | `:337` | SINK node without a Sink instance |
| artifact wrap | `:340-343` | portable wrap first; a local-only plan falls back to the local form so a cluster submission is never silently downgraded |
| submit | `:347` or `:348` | admitted client, or the process-wide in-process runtime |
| listener | `:352` | exception swallowed |

Every composition error is therefore raised before a source is opened or a sink
is written. `execute()` takes no data back: a sink is written by the worker at
the tail of execution, and the call returns only after completion or throws. The
in-process path and the cluster path share that shape —
`rowweave-core/src/main/java/io/rowweave/InProcessBatchRuntime.java:84` is the
artifact boundary the environment calls, and it runs the same graph assembly a
cluster Master schedules.

For the client-side view of the same boundary,
[`JobClient`](../../../rowweave-core/src/main/java/io/rowweave/cluster/JobClient.java)
is declared at `rowweave-core/src/main/java/io/rowweave/cluster/JobClient.java:15`:

| Member | Declaration | Contract |
|---|---|---|
| `submitToSink(PlanGraph)` | `JobClient.java:21` | the only abstract method; returns rows written |
| `submitToSinkReported(PlanGraph)` | `JobClient.java:31` | default; a client without metrics returns rows only |
| `submit(PlanGraph)` | `JobClient.java:47` | whole-graph submission; single-SINK delegates at `:50`, more than one SINK must be overridden |
| `submit(PlanArtifact)` | `JobClient.java:58` | versioned boundary; unwraps to the graph form |

A client that does not override the multi-sink form refuses rather than dropping
a sink in silence. That is the reason the whole-graph entry point exists: the
per-sink form cannot express a shared subgraph.

## Transformation builders

Every builder returns a new immutable `DataSet`; a `DataSet` is a description, so
nothing is consumed when a node is created and a node may be reused as an input
more than once. `DataSet` is declared at
`rowweave-core/src/main/java/io/rowweave/DataSet.java:36`.

| Method | Declaration | Overload rule and schema effect |
|---|---|---|
| `filter(FilterFunction)` | `DataSet.java:285` | keeps the input schema |
| `filter(String)` | `DataSet.java:291` | expression evaluated by the expression engine; unknown means not passed |
| `filter(String, Map)` | `DataSet.java:301` | empty or `null` parameters degrade to `filter(String)` at `:302` |
| `select(int...)` | `DataSet.java:314` | column pruning by index; names and types carried from the source |
| `select(String...)` | `DataSet.java:325` | projection by alias, bare field name, or generated name |
| `map(MapFunction)` | `DataSet.java:499` | must return a row matching the input schema |
| `incrementalMap(IncrementalMapFunction)` | `DataSet.java:508` | used by the bounded incremental run; byte-exact operation, key, and record position |
| `flatMap(FlatMapFunction)` | `DataSet.java:521` | zero to many rows through the collector |
| `mapPartition(fn, limit)` | `DataSet.java:530` | refuses a `null` function or `null` limit at `:533`, and a `null` result schema at `:538` |
| `convertSchema(SchemaConversion)` | `DataSet.java:335` | bounded mapping bound to this exact input schema |
| `validateRows(RowValidation)` | `DataSet.java:357` | quality gate over the current schema |
| `validateRowsWithSourcePosition(...)` | `DataSet.java:379` | SOURCE-only positioned validation |
| `addFields(Field[], Object[])` | `DataSet.java:754` | appends constant columns; output schema is input plus the new fields |
| `removeFields(String...)` | `DataSet.java:703` | drops named columns |

`convertSchema` and `validateRows` are encoded as plan parameters rather than a
user function, which is why their checks are schema-fingerprint comparisons at
build time. `SCHEMA_MAPPING_INVALID: conversion is required` at
`DataSet.java:337` and `SCHEMA_MAPPING_INPUT_MISMATCH: dataset schema differs
from mapping input` at `:341` are the two build-time refusals;
`QUALITY_VALIDATION_INVALID: validation is required` at `:359` and
`QUALITY_VALIDATION_INPUT_MISMATCH: dataset schema differs from validation
input` at `:363` are the validation pair. The positioned form adds
`QUALITY_SOURCE_POSITION_INVALID` because it needs a SOURCE: the node kind is
checked at `:386` and an explicit operator UID plus non-empty source inputs are
required at `:390`. The full rule set for these quality gates is on
[schema conversion and row validation](schema-and-validation.md).

## Sort, limit, and dedup

| Method | Declaration | Contract |
|---|---|---|
| `orderBy(OrderBy, int, boolean)` | `DataSet.java:559` | sort breaker; the extra parallelism arguments are executor-level, so they are not stored per node |
| `orderBy(String...)` | `DataSet.java:682` | all-ascending, default null order; an unknown name throws `Unknown orderBy field: <name>` at `:687` |
| `top(Top, int)` | `DataSet.java:570` | ordered limit with a sort encoding |
| `top(int)` | `DataSet.java:695` | pass-through limit: the first `n` rows in source order, with no sort |
| `distinct(Distinct)` | `DataSet.java:582` | dedup breaker; empty keys mean all columns |
| `distinct(String...)` | `DataSet.java:590` | convenience overload; empty varargs mean all columns at `:592` |
| `partitionByHash(String...)` | `DataSet.java:769` | refuses no fields at `:771` and an unknown field at `:775` |

The convenience `orderBy(String...)` is documented at `DataSet.java:681` as
all-ascending with the `DEFAULT` null order, which is the numeric-null-equals-
zero and string-null-first rule stated on
[decimal and null semantics](decimal-null-semantics.md). `top(int)` is a
different operator from `top(Top, int)`: the convenience form streams through
without ordering, so using it where a sorted prefix was intended silently
changes which rows survive. `partitionByHash` is a placement directive rather
than a computation: locally it is an identity pass-through, and a cluster forces
a keyed shuffle so equal keys reach the same downstream partition. The multiset
is unchanged either way.

## Aggregation

Batch aggregation has two shapes, and the difference is the output schema.

| Method | Declaration | Shape |
|---|---|---|
| `groupBy(ParallelGroupByAgg)` | `DataSet.java:600` | keyed aggregate breaker; one row per group |
| `groupBy(String...)` | `DataSet.java:721` | returns a `Grouper` for fluent aggregation |
| `groupBySum(String, String...)` | `DataSet.java:677` | shorthand delegating to the breaker at `:678` |
| `sum(String)` | `DataSet.java:624` | whole-table sum, one row, alias `sum` |
| `sum(String, String)` | `DataSet.java:629` | whole-table sum with an explicit alias |
| `count()` | `DataSet.java:634` | whole-table row count including null measures |
| `count(String)` | `DataSet.java:639` | whole-table non-null count, alias `count` |
| `count(String, String)` | `DataSet.java:644` | whole-table non-null count with an explicit alias |
| `max(String)` / `max(String, String)` | `DataSet.java:649` / `:654` | whole-table maximum, nulls ignored |
| `min(String)` / `min(String, String)` | `DataSet.java:659` / `:664` | whole-table minimum, nulls ignored |

The whole-table forms are scalar aggregates: they produce a single-column schema
whose name is the alias or the operator name. The no-argument `count()` counts
rows, including rows whose measured fields are null, while `count(field)` counts
non-null occurrences — the distinction a reviewer is most likely to miss.
`max` and `min` keep the field's own type, so an unknown field name is refused at
build time by `requireFieldType` at `DataSet.java:668` with `unknown field for
<operator>: <field>` at `:671`. `sum` and `count` do not need the field type to
build a schema, so they are checked when the operator binds.

The group-key encoding for the breaker is a `typeId` plus parallel list
parameters `keys`, `aggKinds` and `aggMeasures` ordered by column, with a
whole-group count recorded as an empty string. That representation is what makes
a grouped plan describable to a Worker; the computation it stands for is on
[Batch behavior contracts](batch-behavior.md).

| `Grouper` method | Declaration | Notes |
|---|---|---|
| `getFields()` | `rowweave-core/src/main/java/io/rowweave/Grouper.java:29` | the declared key fields |
| `sum` / `count` / `max` / `min` | `Grouper.java:34` / `:44` / `:59` / `:69` | each has an alias overload |
| `reduceGroup(GroupReduceFunction)` | `Grouper.java:79` | one group in hand |
| `combineGroup(GroupCombineFunction)` | `Grouper.java:91` | map-side pre-aggregation |
| `combineReduceGroup(GroupCombineReduceFunction)` | `Grouper.java:96` | combine then final reduce |

`Grouper` is a description holder, not a stateful builder: `groupBy(String...)`
at `DataSet.java:721` returns one, and every method on it returns a new
`DataSet`.

## Group functions and shard count

| Method | Declaration | Contract |
|---|---|---|
| `reduceGroup(ReduceGroup, DataSchema)` | `DataSet.java:786` | explicit output schema |
| `reduceGroup(GroupReduceFunction)` | `DataSet.java:797` | refuses a `null` `getResultSchema()` at `:800` |
| `combineGroup(GroupCombineFunction)` | `DataSet.java:810` | delegates with `shards` = 1 at `:811` |
| `combineGroup(GroupCombineFunction, int)` | `DataSet.java:815` | a `null` result schema falls back to the input schema at `:816` |
| `combineReduceGroup(GroupCombineReduceFunction)` | `DataSet.java:828` | delegates with `shards` = 1 at `:829` |
| `combineReduceGroup(GroupCombineReduceFunction, int)` | `DataSet.java:833` | refuses a `null` result schema at `:836` |

`shards` is a plan-time constant: it is fixed when the plan is built and used
identically locally and on a cluster, so a shard-count difference can never be a
source of divergence between the two. The default is 1, meaning one contiguous
range over the whole input. Bit-identical cluster output requires the same
single-instance reduce, which is why the combined form documents reduce as an
ordered final pass. The interface contracts, including `getResultSchema()`
ownership, are on [custom function interfaces](custom-functions.md).

## Join, co-group, and union

| Method | Declaration | Contract |
|---|---|---|
| `join(DataSet)` | `DataSet.java:726` | inner builder; keys chosen through `JoinDataSet` |
| `leftJoin(DataSet)` | `DataSet.java:731` | preserves the left side |
| `rightJoin(DataSet)` | `DataSet.java:736` | preserves the right side |
| `fullJoin(DataSet)` | `DataSet.java:741` | preserves both sides |
| `coGroup(DataSet, CoGroupFunction)` | `DataSet.java:746` | grouped cross product per key |
| `join(DataSet, SortMergeJoin)` | `DataSet.java:851` | explicit sort-merge strategy |
| `join(DataSet, SortMergeJoin, JoinHint)` | `DataSet.java:863` | `SORT_MERGE` is written to the plan; `HASH` becomes a hash join locally and on a cluster |
| `coGroup(DataSet, CoGroup)` | `DataSet.java:884` | explicit co-group operator |
| `union(DataSet, Union)` | `DataSet.java:893` | explicit union operator |
| `union(DataSet)` | `DataSet.java:900` | schema-checked append |

`union(DataSet)` runs `validateUnionSchema` at `DataSet.java:904`, which refuses
a field-count mismatch at `:906` and a per-index type mismatch at `:913`. Field
*names* are not part of that check — positional type equality is, so a union of
two schemas with the same types in a different name order is admitted and takes
the left side's names.

The hint form is the one place where a strategy choice is not merely advisory:
`SORT_MERGE` is recorded in the plan so every worker sees the same decision,
while `HASH` is materialized as a hash join on both the local and the cluster
path, and the node keeps the hint parameter for diagnosis. The result rules for
each join type are on [Batch behavior contracts](batch-behavior.md), and
null-never-matches-null is a value rule from
[decimal and null semantics](decimal-null-semantics.md).

| Builder method | Declaration | Contract |
|---|---|---|
| `JoinDataSet.on(String[], String[])` | `rowweave-core/src/main/java/io/rowweave/JoinDataSet.java:27` | positional key pairs; lengths must agree |
| `JoinDataSet.on(String)` | `JoinDataSet.java:34` | one key name used on both sides |
| `JoinDataSet.getJoinType()` | `JoinDataSet.java:39` | the type the originating method chose |
| `JoinDataSet.withFunc(String[], String[], JoinFunction)` | `JoinDataSet.java:48` | custom row-pair emission |
| `JoinDataSet.withFunc(String, JoinFunction)` | `JoinDataSet.java:65` | shared key name form |
| `CoGroupDataSet.on(String[], String[])` | `rowweave-core/src/main/java/io/rowweave/CoGroupDataSet.java:25` | co-group keys |
| `CoGroupDataSet.on(String)` | `CoGroupDataSet.java:31` | shared key name |

A builder that has not been given keys produces no `DataSet`: `on` and `withFunc`
are the only methods that close the builder, so an unfinished join is a
compile-time impossibility rather than a runtime refusal.

## Tested by

- [`DataSetTest`](../../../rowweave-core/src/test/java/io/rowweave/DataSetTest.java)
  and [`DataSetApiCompatTest`](../../../rowweave-core/src/test/java/io/rowweave/DataSetApiCompatTest.java):
  builder surface and published-signature compatibility.
- [`DataSetFluentBuilderTest`](../../../rowweave-core/src/test/java/io/rowweave/DataSetFluentBuilderTest.java)
  and [`DataSetBinaryTest`](../../../rowweave-core/src/test/java/io/rowweave/DataSetBinaryTest.java):
  chaining, immutability, and binary inputs.
- [`DataSetBreakerTest`](../../../rowweave-core/src/test/java/io/rowweave/DataSetBreakerTest.java)
  and [`GroupAggBufferTest`](../../../rowweave-core/src/test/java/io/rowweave/mem/GroupAggBufferTest.java):
  breaker working-set behavior behind the builders.
- [`ParallelGroupByAggTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/ParallelGroupByAggTest.java)
  and [`ReduceGroupTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/ReduceGroupTest.java):
  aggregate encoding and group-function output schema rules.
- [`UnionTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/UnionTest.java)
  and [`HashJoinHintTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/join/HashJoinHintTest.java):
  positional union schema refusal and hint-driven join materialization.
- [`ClusterJoinFanoutTwoSinkTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/ClusterJoinFanoutTwoSinkTest.java)
  and [`DiamondMaterializationTest`](../../../rowweave-core/src/test/java/io/rowweave/DiamondMaterializationTest.java):
  one shared subgraph feeding several sinks in a single submission.
- [`JobSessionPlanArtifactTest`](../../../rowweave-core/src/test/java/io/rowweave/JobSessionPlanArtifactTest.java)
  and [`ExecutionReportMetricsTest`](../../../rowweave-core/src/test/java/io/rowweave/ExecutionReportMetricsTest.java):
  artifact compilation and advisory listener reporting.
- [`SingleMachineCanonicalizeTest`](../../../rowweave-core/src/test/java/io/rowweave/SingleMachineCanonicalizeTest.java)
  and [`EndToEndEquivalenceTest`](../../../rowweave-core/src/test/java/io/rowweave/EndToEndEquivalenceTest.java):
  canonicalized output equivalence and whole-job result parity.

Related: [public API overview](api-overview.md),
[Batch behavior contracts](batch-behavior.md),
[custom function interfaces](custom-functions.md),
[schema conversion and row validation](schema-and-validation.md),
[expression evaluation](expression-evaluation.md),
[built-in Batch sources](batch-sources.md),
[Batch sinks and output boundaries](batch-sinks.md).
