# ScenarioLab

ScenarioLab is the examples module test-only suite of 100 batches with 100
scenarios each, for 10,000 functional scenarios. It is not a performance test.
Inputs are small by design so failures isolate behavior rather than load.

Each scenario runs the production path: build an `ExecutionEnvironment`,
compose the DAG, execute the in-process batch runtime, write a file output,
read it back, and compare the whole result with an independent Java oracle.

## Run

```bash
mvn -q test -pl rowweave-examples -am -Dtest=ScenarioLabBatchTest \
    -Dsurefire.failIfNoSpecifiedTests=false
```

A non-zero exit means at least one batch failed. To debug one batch, run
`ScenarioLabRunner` with an output directory and one or more batch numbers, for
example `target/scenariolab-report 21 47`. The report header records the Java
version and engine code source.

## Batches

The suite covers filters, projections, maps, flat maps, unions, distinct,
grouping, aggregation, sorting, TopN, all join types, coGroup, custom
functions, schema conversion, validation, parallelism, whole-table aggregate,
decimal semantics, empty and null-heavy inputs, Unicode, extreme decimals,
wide rows, UID/limit boundaries, and seeded random DAGs.

The oracle is independent of engine code and implements the decimal
semantics directly: three-valued logic, null propagation, scale-10 division,
null-aware grouping and ordering, join null inequality, high-precision
accumulation, and one final `HALF_UP` rounding step.
