---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Custom function interfaces

A custom function is the only supported way to put application logic inside an
operator. Every interface here lives in `io.rowweave`, is a plain Java
interface, and extends `java.io.Serializable`, because a function is part of the
plan description that travels to a Worker rather than something the client runs
on its own thread.

This page states the contract, the output-schema rule, the limits that are
enforced, and the failure each refusal produces. For the value rules a function
body must follow, see
[decimal and null semantics](decimal-null-semantics.md).

## Pass-through functions

These run once per row and never hold an input-sized working set.

| Interface | Signature | Emission |
|---|---|---|
| [`MapFunction`](../../../rowweave-core/src/main/java/io/rowweave/MapFunction.java) | `DataRow map(DataRow row)` | one row, or `null` to drop the row |
| [`FilterFunction`](../../../rowweave-core/src/main/java/io/rowweave/FilterFunction.java) | `boolean filter(DataRow row)` | keeps the row when the result is `true` |
| [`FlatMapFunction`](../../../rowweave-core/src/main/java/io/rowweave/FlatMapFunction.java) | `void flatMap(DataRow row, Collector collector)` | `0..N` rows through `collect` |
| [`IncrementalMapFunction`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalMapFunction.java) | extends `MapFunction` | one row, used by the bounded incremental run |

`map` is declared at `rowweave-core/src/main/java/io/rowweave/MapFunction.java:11`,
`filter` at `rowweave-core/src/main/java/io/rowweave/FilterFunction.java:11`, and
`flatMap` at `rowweave-core/src/main/java/io/rowweave/FlatMapFunction.java:11`.
All three are `@FunctionalInterface`, so a lambda is a valid implementation.

`Collector` has a single method
(`rowweave-core/src/main/java/io/rowweave/Collector.java:8`) and is the only
outlet: a function that both returns a value and collects is a contract
violation, not a feature. The entry points on `DataSet` are `filter`
(`rowweave-core/src/main/java/io/rowweave/DataSet.java:285`), `map`
(`DataSet.java:499`) and `flatMap` (`DataSet.java:521`).

A function must not retain a row it emitted. Under the pure push model the
reference handed downstream is the row itself, so holding or mutating it after
`collect` corrupts data another stage may already be reading. The executor-side
version of the same rule is stated in
[built-in Batch sources](batch-sources.md).

## Partition and group functions

| Interface | Signature | Working set |
|---|---|---|
| [`MapPartitionFunction`](../../../rowweave-core/src/main/java/io/rowweave/MapPartitionFunction.java) | `void mapPartition(PartitionDataRowCursor rows, Collector collector)` | one physical partition, streamed |
| [`GroupReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupReduceFunction.java) | `void reduce(Iterable<DataRow> iter, Collector collector)` | one group |
| [`GroupCombineFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineFunction.java) | `void combine(Iterable<DataRow> iter, Collector collector)` | one partial group |
| [`GroupCombineReduceFunction`](../../../rowweave-core/src/main/java/io/rowweave/GroupCombineReduceFunction.java) | `combine` then `reduce` | partial, then final |

`mapPartition` reads through
[`PartitionDataRowCursor`](../../../rowweave-core/src/main/java/io/rowweave/PartitionDataRowCursor.java),
whose single `next()` method (`PartitionDataRowCursor.java:10`) returns `null`
when the partition is exhausted. The cursor is valid only while the invocation
is active: retaining it, or the rows it produced, past the call is undefined.
Because the cursor is single-pass, this is the one shape that cannot be
expressed with `map`.

`DataSet.mapPartition` requires an explicit
[`MapPartitionEmissionLimit`](../../../rowweave-core/src/main/java/io/rowweave/MapPartitionEmissionLimit.java)
(`rowweave-core/src/main/java/io/rowweave/DataSet.java:530`), and a missing
function or missing limit fails at graph construction, before any row moves. The
declared row ceiling is one billion rows per partition at
`rowweave-core/src/main/java/io/rowweave/MapPartitionEmissionLimit.java:10`,
with a byte ceiling beside it at `:11`.

The group functions attach through `reduceGroup`
(`rowweave-core/src/main/java/io/rowweave/DataSet.java:797`), `combineGroup`
(`DataSet.java:810`, shard-count overload at `DataSet.java:815`) and
`combineReduceGroup` (`DataSet.java:828`, shards at `DataSet.java:833`). A group
is the unit of iteration: `iter` yields exactly the rows sharing the grouping
key, and the function may emit `0..N` rows per group. A `null` key forms its own
group rather than being dropped.

`combine` is the partial half of the pair, and its output must be rows the
`reduce` can consume. `GroupCombineFunction` declares
`default DataSchema getResultSchema()` at
`rowweave-core/src/main/java/io/rowweave/GroupCombineFunction.java:19` so the
partial header is discoverable.

## Join and co-group functions

| Interface | Signature | Semantics |
|---|---|---|
| [`JoinFunction`](../../../rowweave-core/src/main/java/io/rowweave/JoinFunction.java) | `DataRow join(DataRow row1, DataRow row2)` | one output row per matched pair |
| [`CoGroupFunction`](../../../rowweave-core/src/main/java/io/rowweave/CoGroupFunction.java) | `void coGroup(Iterable<DataRow> iter1, Iterable<DataRow> iter2, Collector collector)` | both groups for one key |

The fluent builders are `DataSet.join(DataSet)`
(`rowweave-core/src/main/java/io/rowweave/DataSet.java:726`) completed by
`JoinDataSet.on` (`rowweave-core/src/main/java/io/rowweave/JoinDataSet.java:27`)
and `JoinDataSet.withFunc` (`JoinDataSet.java:48`), and
`DataSet.coGroup(DataSet, CoGroupFunction)` (`DataSet.java:746`) completed by
`CoGroupDataSet.on` (`rowweave-core/src/main/java/io/rowweave/CoGroupDataSet.java:25`).

`coGroup` receives an empty group on the side that has no rows for the key, as
stated at `rowweave-core/src/main/java/io/rowweave/ops/join/CoGroup.java:26`, so
outer behaviour is expressible inside the function rather than through a flag.
In `join`, a `null` key never matches a `null` key. The rule is on
[decimal and null semantics](decimal-null-semantics.md).

## Output schema: `ResultSchemaAware`

Every function whose output header differs from its input header implements
[`ResultSchemaAware`](../../../rowweave-core/src/main/java/io/rowweave/ResultSchemaAware.java)
and answers `getResultSchema()` (`ResultSchemaAware.java:10`). The engine reads
the header through that method, which is why `combineGroup`, `reduceGroup`,
`mapPartition`, `join` with `withFunc`, and `coGroup` take the function instead
of a function plus a schema.

`DataSet.reduceGroup(ReduceGroup, DataSchema)` at
`rowweave-core/src/main/java/io/rowweave/DataSet.java:786` is the
describable-plan overload that carries an explicit header; the function overload
at `DataSet.java:797` derives it from the interface.

## Limits and refusals

| Condition | Where it fails | Result |
|---|---|---|
| function or emission limit absent for `mapPartition` | `DataSet.java:534` | `IllegalArgumentException` at graph construction |
| `getResultSchema()` returns `null` | `DataSet.java:539` | `IllegalArgumentException` at graph construction |
| `partitionByHash` with no field | `DataSet.java:771` | `IllegalArgumentException` |
| `partitionByHash` with an unknown field | `DataSet.java:775` | `IllegalArgumentException` naming the field |
| function instance is not serializable | plan submission | serialization failure before execution |

Hash distribution for a keyed shuffle is computed by
[`Partitioner`](../../../rowweave-core/src/main/java/io/rowweave/ops/Partitioner.java):
`of` (`io/rowweave/ops/Partitioner.java:44`), `ofFields` (`io/rowweave/ops/Partitioner.java:48`),
`numericNormalizedOfFields` (`io/rowweave/ops/Partitioner.java:59`) for keys that mix integer
widths, and `fixed` (`io/rowweave/ops/Partitioner.java:64`) to route every row to one
partition. Equal keys share a partition because `partitionOf`
(`io/rowweave/ops/Partitioner.java:84`) reads only the key columns and the partition count.

## Tested by

- [`GroupStatExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/GroupStatExample.java)
  is the end-to-end example of a custom `GroupReduceFunction` that emits a count and full-precision total
  while following the schema and serializability rules above.
- [`MapPartitionContractTest`](../../../rowweave-core/src/test/java/io/rowweave/MapPartitionContractTest.java) and
  [`MapPartitionRuntimeTest`](../../../rowweave-core/src/test/java/io/rowweave/MapPartitionRuntimeTest.java): cursor
  lifetime, emission limit, schema derivation.
- [`PublicMapPartitionJava8ConsumerTest`](../../../rowweave-core/src/test/java/io/rowweave/PublicMapPartitionJava8ConsumerTest.java):
  the interface compiles and runs against the Java 8 consumer target.
- [`CombineGroupLifecycleTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/CombineGroupLifecycleTest.java) and
  [`CombineGroupTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/CombineGroupTest.java): partial and
  final halves of the combine pair, on one machine and across Workers.
- [`CoGroupTest`](../../../rowweave-core/src/test/java/io/rowweave/ops/join/CoGroupTest.java) and
  [`SingleMachineCoGroupShuffleTest`](../../../rowweave-core/src/test/java/io/rowweave/SingleMachineCoGroupShuffleTest.java):
  empty-group side, key alignment, shuffle correctness.
- [`IncrementalMapFunctionContractTest`](../../../rowweave-core/src/test/java/io/rowweave/incremental/IncrementalMapFunctionContractTest.java):
  the bounded incremental run reuses the pass-through function contract.
- [`DataSetApiCompatTest`](../../../rowweave-core/src/test/java/io/rowweave/DataSetApiCompatTest.java) and
  [`DataSetFluentBuilderTest`](../../../rowweave-core/src/test/java/io/rowweave/DataSetFluentBuilderTest.java): the
  builder shapes that complete a function.
- [`PortableMapPartitionClusterProductTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/PortableMapPartitionClusterProductTest.java):
  a user function shipped inside a portable plan and executed on Workers.

Related: [API overview](api-overview.md), [Batch behavior contracts](batch-behavior.md),
[expression syntax and evaluation](expression-evaluation.md),
[schema conversion and row validation](schema-and-validation.md),
[Batch sinks and output boundaries](batch-sinks.md).
