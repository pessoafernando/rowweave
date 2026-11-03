---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Scenario oracle specification

This page states how Batch results are qualified. RowWeave is compared against
an independent oracle: a plain Java implementation of the same rules that shares
no code with the engine. Because the expected values are computed somewhere else,
an equivalence result is evidence rather than a tautology. The value rules the
oracle follows are the engine rules on
[decimal and null semantics](decimal-null-semantics.md).

## Why an independent oracle

The oracle helpers live in `rowweave-examples/src/test/java/io/rowweave/examples/scenariolab/O.java:21`
and recompute every expected value with `java.util` collections and
`BigDecimal` arithmetic. `O.java:35` is the grouped-sum oracle, `O.java:266` the
sort oracle, and `O.java:329` the distinct oracle. The point of the duplication
is that a wrong engine rule and a wrong oracle rule have to be written twice, on
purpose, in different code.

## How a scenario runs

A scenario is written against the public production API, not against internals.
`rowweave-examples/src/test/java/io/rowweave/examples/scenariolab/ScenarioLab.java:19-20`
records what a scenario body does: it builds a `BatchSession`, creates
sources with `ScenarioLab.java:58`, chains operators, and registers an expectation
with `ScenarioLab.java:63` for set comparison or `ScenarioLab.java:68` for
positional comparison. When the body returns, the framework calls
`session.execute()`, which submits the whole graph through the real in-process
Batch runtime.

The result is not handed back to the client. It is written by the sink, then read
back from disk with `rowweave-examples/src/test/java/io/rowweave/examples/scenariolab/ScenarioLab.java:169`,
so the qualification covers serialization and the file boundary as well as the
operators. A negative scenario registers the expected failure instead of an
expected result: `ScenarioLab.java:95` pins the exception type and message
fragment, and `rowweave-examples/src/test/java/io/rowweave/examples/scenariolab/ScenarioDef.java:46`
declares a scenario that must throw at run time. `ScenarioDef.java:20` is the
closed set of scenario kinds.

## How results are compared

`rowweave-examples/src/test/java/io/rowweave/examples/scenariolab/RowCompare.java:26`
is the single comparison entry point, and `RowCompare.java:16-17` states the two
modes:

* **Multiset**, the default, because a plan that declares no ordering makes no
  promise about row order.
* **Positional**, for `orderBy` and `top` scenarios, where position is part of the
  contract.

A failure reports a diff with evidence: the count difference plus samples of
missing and extra rows, so a red scenario names what diverged instead of only
that something diverged.

## Scenario families

The suite is organised into ten families of ten batches.
`rowweave-examples/src/test/java/io/rowweave/examples/scenariolab/ScenarioOutcome.java:38`
assembles them through the registry declared at `ScenarioOutcome.java:33`,
and each family publishes its own batches: `FamilyA.java:34`,
`FamilyB.java:38`, `FamilyC.java:39`, `FamilyD.java:34`, `FamilyE.java:46`,
`FamilyF.java:35`, `FamilyG.java:35`, `FamilyH.java:37`, `FamilyI.java:29`, and
`FamilyJ.java:36`. `rowweave-examples/src/test/java/io/rowweave/examples/scenariolab/BatchDesign.java:6`
is the batch contract, and `BatchDesign.java:9` is the batch number that makes a
failure reproducible.

| Family | Batches | Behavior qualified |
|---|---|---|
| A | B01-B12 | Pass-through operators: `filter`, `select`, `map`, `flatMap`, and chains of them, under expression, named-parameter and lambda forms. The oracle follows three-valued logic. |
| B | B13-B22 | Aggregation family: `union`, `distinct`, `addFields`, `partitionByHash`, `groupBy`. |
| C | B23-B32 | Ordering and limits: `orderBy` under direction, null policy, and multiple keys; `top(N)`; ordering after aggregation. Compared positionally. |
| D | B33-B42 | Joins: `INNER`, `LEFT`, `RIGHT`, and `FULL` crossed with duplicate keys, null keys, and composite keys; `HASH` and `SORT_MERGE` hints; self joins; empty inputs. |
| E | B43-B52 | Advanced binary operators and user functions: `join.withFunc`, three-way joins, join combined with `groupBy` in both directions, `coGroup`, `reduceGroup`, `combineGroup`, `combineReduceGroup`. |
| F | B53-B62 | Graph topologies: diamond and wide fan-out, convergence from several sources, deep chains, a breaker shared by two sinks, a fork after a join, and branches transformed separately and rejoined. A shared upstream computed once shows up as correct, agreeing sinks. |
| G | B63-B72 | Parallelism 2, 3, and 4 must mean the same thing as parallelism 1, compared as a multiset and positionally once output canonicalization is on. |
| H | B73-B82 | Whole-table aggregation, `convertSchema`, `validateRows`, and misuse of the API, where the engine must fail fast with the stated exception type. |
| I | B83-B92 | Data boundaries: an empty table, a single row, all-null rows, duplicate rows, non-Latin text, whitespace, extreme and negative numerics, rounding, integer keys, and wide multi-key tables. The same operator templates run over every boundary data set. |
| J | B93-B100 | Operator identity, `limit` bounds, multi-level dimension rollup, join disambiguation chains, seeded random graphs, dimension-table enrichment, shared-subgraph multi-sink consistency, and `case`/`cast` expression mixes. |

Each batch is designed as ten operator variants over ten data profiles, so a
batch carries 100 scenarios and the whole suite carries 10,000, as
`rowweave-examples/src/test/java/io/rowweave/examples/scenariolab/ScenarioLabBatchTest.java:15`
records. `ScenarioLabBatchTest.java:21` expands them as dynamic tests so a single
scenario id shows up as a single failure.

## Running the suite

`ScenarioLabBatchTest` runs the whole suite in one Maven pass. To work on one
batch, `rowweave-examples/src/test/java/io/rowweave/examples/scenariolab/ScenarioLabRunner.java:24`
takes an output directory and batch numbers, as `ScenarioLabRunner.java:15-17`
documents, and writes a per-batch report that `ScenarioOutcome` describes. Running
a single batch is how a red scenario gets diagnosed.

## What the suite does not claim

The suite qualifies Batch operator semantics on bounded inputs. It is not a
throughput, latency or scale measurement, and it does not certify Stream runtime
lifecycle, recovery, or cluster transport; those have their own references in
[Stream behavior](stream-behavior.md),
[recovery](../architecture/recovery.md), and
[data-plane limits](data-plane-limits.md). Supported behavior stays defined by
the [capability matrix](capability-matrix.md).
