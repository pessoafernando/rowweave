---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# First job

This first job filters a small in-memory collection by quantity, groups by
category, sums decimals, orders the totals, and writes a tab-separated file.
It is the fastest way to see RowWeave's explicit schema, whole-graph submission,
and decimal output scale before choosing a production source or sink.

The checked-in implementation is
[`BatchFirstJobExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/BatchFirstJobExample.java).
Its test asserts the exact three-line result shown below.

## Prerequisites

Complete [installation and modules](installation.md). In this source preview,
build the reactor before using the example. You do not need an optional
connector, registry, or monitor for this job.

## Run the tested example

Run the focused test:

```bash
mvn -q -pl rowweave-examples test -Dtest=BatchFirstJobExampleTest
```

To run `main`, package the example and build its runtime classpath:

```bash
mvn -q -pl rowweave-examples -am -DskipTests package
mvn -q -pl rowweave-examples dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
java -cp "rowweave-examples/target/classes:$(cat rowweave-examples/target/classpath.txt)" io.rowweave.examples.BatchFirstJobExample result.tsv
```

`result.tsv` must not already exist. For a repeat run, choose a new path or move
the previous result first.

## Expected output

The file contains tab-separated values in category order:

```text
alpha	13.1234567891
beta	4.0000000000
gamma	3.1234567891
```

Each decimal has exactly 10 digits after the decimal point. `beta` also shows
that the output scale is applied even when the mathematical result is an
integer.

## Job anatomy

The sample input uses this explicit schema:

```java
DataSchema inputSchema = new DataSchema(
        new Field("categoryKey", DataType.StringType),
        new Field("quantity", DataType.BigDecimalType));
```

Each row has two positional cells. `categoryKey` is a string; `quantity` is a
`BigDecimal`. The sample includes rows below the filter threshold so the output
also demonstrates that admission of every row is governed by the graph, not by
when a sink happens to run.

The job body composes five operations:

```java
DataSet totals = environment.fromSource(input)
        .filter(row -> row.getBigDecimal(1)
                .compareTo(new BigDecimal("3")) >= 0)
        .groupBy("categoryKey")
        .sum("quantity")
        .orderBy("categoryKey");

totals.sinkTo(new FileSink(totals.getSchema(), outputPath));
environment.execute();
```

| Stage | Behavior |
|---|---|
| `fromSource` | Wraps the schema and a replayable copy of the rows as one graph source. |
| `filter` | Keeps rows whose decimal quantity is greater than or equal to 3. |
| `groupBy` / `sum` | Produces one row per category. The aggregate value column is `sum0`. |
| `orderBy` | Sorts by `categoryKey` before the sink so the sample result is deterministic. |
| `sinkTo` | Attaches a file sink to the graph. The sink schema comes from `totals.getSchema()`. |
| `execute` | Submits all sources, operators, and sinks as one plan graph. |

## Decimal and null behavior

The engine rounds decimal output once, at the scale stated in
[decimal and null semantics](../reference/decimal-null-semantics.md). For
example, `10.123456789123 + 3` becomes `13.1234567891` in the file. Do not apply
another `setScale` call in a normal aggregate sink.

Null quantities and null keys follow the engine's typed semantics rather than
being silently discarded or coerced. For this first job, every supplied quantity
is non-null.

## Observe the graph boundary

`environment.execute()` does not return rows to the client. It compiles the
whole graph and submits it through the configured job client. In local mode, the
sink runs inside the same process. In a cluster, the Worker that executes the
final stage writes the sink.

`PrintSink` is useful for tiny experiments. `FileSink` is the first step toward
a production result because it writes typed, replayable text. Later, replace it
with another supported `Sink` implementation or provider.

## Common first-job problems

* **Unknown field.** The names in `groupBy`, `sum`, and `orderBy` must exist in
  the `DataSchema`. Keep names and positional indexes consistent.
* **Wrong cell type.** Use `getBigDecimal` for a `BigDecimal` cell; do not parse
  it as a string after the schema has declared its type.
* **Output path exists.** `FileSink` writes a result artifact, not an append
  log. Use an unused path for each run.
* **No output from `execute`.** This is expected: the client submits the graph;
  a sink consumes the rows.
* **More rows than expected.** Recheck the predicate and input rows. The sample
  intentionally contains quantities below 3 that must not reach the aggregate.
* **Memory pressure.** This sample is small, but production sorts, joins, and
  aggregates need resource envelopes. See
  [troubleshooting](../operations/troubleshooting.md).

## Next steps

* Compare Local, InProcess, and cluster execution in
  [run modes](../guides/run-modes.md).
* Check supported operators and stable refusals in the
  [capability matrix](../reference/capability-matrix.md).
* Read the [API overview](../reference/api-overview.md) before composing larger
  graphs.
