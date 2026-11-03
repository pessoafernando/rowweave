---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Example index

Use these examples as tested entry points for the main execution and integration
journeys. They are maintained in `rowweave-examples`; several have focused JUnit
tests, and `RunModesExample` also has a deterministic command-line path.

Build the examples and run all indexed tests from the repository root:

```bash
mvn -q -pl rowweave-examples -am test
```

The tests use temporary state and result directories. Do not treat an example as
a production topology: keep the resource envelope, sink, recovery policy, and
security profile appropriate to your deployment.

## Batch and run modes

| Example | Intent | Verified entry point |
|---|---|---|
| [`WordCountExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/WordCountExample.java) | Split text lines into words and count per word with a flatMap and groupBy graph. | `InventoryExampleQualificationTest`. |
| [`BatchFirstJobExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/BatchFirstJobExample.java) | Run a filter, group, aggregate, order, and file-sink graph with exact decimal output. | `BatchFirstJobExampleTest` and the [Batch first job](../quickstart/first-job.md). |
| [`AmountRollupExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/AmountRollupExample.java) | Group decimal amounts by dimensions, order the result, and write a file with bit-for-bit round trips. | `InventoryExampleQualificationTest`. |
| [`GroupStatExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/GroupStatExample.java) | Use a custom `GroupReduceFunction` to emit a count and full-precision total per region. | `InventoryExampleQualificationTest`. |
| [`JoinDimEnrichExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/JoinDimEnrichExample.java) | Left-join events with a dimension table; demonstrates null-key join semantics and column pruning. | `InventoryExampleQualificationTest`. |
| [`RunModesExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/RunModesExample.java) | Run one Batch graph in Local and InProcess cluster modes and compare result files. | `main`; the real-cluster path is a documented sketch, not an executed remote submission. |

Run the mode example with a fresh temporary directory so its result files do not
already exist:

```bash
mvn -q -pl rowweave-examples -am -DskipTests package
mvn -q -pl rowweave-examples dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
ROWWEAVE_TMPDIR=$(mktemp -d)
java -Djava.io.tmpdir="$ROWWEAVE_TMPDIR" \
  -cp "rowweave-examples/target/classes:$(cat rowweave-examples/target/classpath.txt)" \
  io.rowweave.examples.RunModesExample
```

Use a new empty directory for each run, or choose another `java.io.tmpdir`. The
program prints equivalent Local and InProcess aggregates and fails if the result
sets differ.

## Stream

| Example | Intent | Verified entry point |
|---|---|---|
| [`StreamLocalAppendExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamLocalAppendExample.java) | Compose a Local Stream graph with typed functions and an append sink. | `StreamLocalAppendExampleTest`. |
| [`StreamAtomicCompositionExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamAtomicCompositionExample.java) | Compile a Stream graph that uses every published node kind, including source, calc, keyed process, async lookup, split, and sink. | `InventoryExampleQualificationTest`. |
| [`StreamLocalEventTimeRecoveryExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamLocalEventTimeRecoveryExample.java) | Checkpoint a pending event-time window, cancel, restore, and emit after the final watermark. | `StreamLocalEventTimeRecoveryExampleTest` and the [Stream first job](../quickstart/stream-first-job.md). |
| [`StreamElasticExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamElasticExample.java) | Exercise dynamic unaligned rescale and compile a retract graph. | `StreamElasticExampleTest`. |
| [`StreamClusterCdcExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamClusterCdcExample.java) | Exercise independent product workers, a stop-with-savepoint boundary, and exact CDC-position restore. | `StreamClusterCdcExampleTest`. |

The Stream recovery and elasticity examples use temporary state roots. Recovery
tests await a completed operation status before cancel, restore, or rescale;
that sequence is part of the example, not an optional delay.

## Connector qualification

| Example | Intent | Verified entry point |
|---|---|---|
| [`StreamConnectorConformanceExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamConnectorConformanceExample.java) | Run the published conformance fixtures for a source, eager sink, and transactional sink. | `StreamConnectorConformanceExampleTest`. |

Run this example before enrolling a Stream provider. Conformance checks the
provider contract; it does not replace deployment qualification, security
review, or production resource testing.

## Pipeline composition

| Example | Intent | Verified entry point |
|---|---|---|
| [`PipelineCompositionExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/PipelineCompositionExample.java) | Compile Batch-only, Stream-only, mixed-runtime, shared-upstream, and revision pipelines with versioned handoff evidence. | `PipelineCompositionExampleTest`. |

Pipeline composition validates and plans a boundary; it does not execute the
underlying Batch or Stream steps.

## Monitoring

No indexed example is a substitute for the monitor contract. Monitoring is an
optional read-only surface: start it with the documented properties, then use
the dashboard, HTTP API, and Prometheus endpoint to observe a running example.
See [monitoring](../operations/monitoring.md) and the
[monitor contract](../reference/monitor-contract.md).
