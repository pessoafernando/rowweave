---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# API overview

For the exhaustive source-visible declaration set and the frozen 0.2.0 API
vocabulary, see the [public API inventory](api-inventory.md). This overview is
the task-oriented summary; the inventory is the traceability authority.

## Batch `DataSet`

`ExecutionEnvironment` is the batch entry point. Create it with
`RowWeave.createEnvironment`, open a `Source`, and compose operators through
`DataSet`. `execute()` submits the whole graph.

Common operations include `filter`, `select`, `map`, `flatMap`, `groupBy`,
`join`, `coGroup`, `union`, `orderBy`, `distinct`, `top`, `convertSchema`, and
`validateRows`. Expressions accept typed parameters; lambdas receive `DataRow`.
Use `withFunc` for custom functions and `sinkTo` to attach a `Sink`.

## StreamSession / DataStream

Create a Stream client with `createLocalStreamJobClient`,
`createRealClusterStreamJobClient`, or an in-process cluster client. A
`StreamSession` holds schema, changelog kind, checkpoint, restart, and resource
configuration. `DataStream` composes the closed Stream operator set.

Key APIs include `keyedProcessWithOutputs`, `keyedCoProcess`,
`genericWindowWithLateOutput`, `asyncWait`, `StreamJobHandle.rescale`, and
`stopWithSavepoint`. Sources can implement `StreamDynamicSourceProvider`.
Checkpoint policy supports manual and `periodicUnaligned` modes.

Stateful operators can be chained in one composed DAG. The phrase used by the
contract tests is chained stateful operators. The runtime keeps every accepted
node in the same task lifecycle.

## Cluster bootstrap

Cross-host processes use `StreamClusterMain` with explicit Master-only or
Worker-only roles. Examples include `StreamElasticExample`,
`StreamAtomicCompositionExample`, and `StreamConnectorConformance`.
`StreamConnectorConformance` is the connector qualification entry point.

## Error model

Public API failures use typed exception codes. Plan and configuration errors
fail before execution. Runtime failures are categorized and reported through
job status and monitoring. Do not parse localized message text.
