---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Batch sinks and output boundaries

A sink is where a Batch job's result leaves the engine. This page states the
sink contract, every built-in Batch sink, the two-tier capability surface a
plan reads before it schedules anything, and the commit protocol that decides
when a result becomes externally visible. For the durable provider extension
point, see [durable sink provider boundary](durable-sink-provider.md).

## The sink contract

[`Sink`](../../../rowweave-core/src/main/java/io/rowweave/Sink.java) is declared
at `rowweave-core/src/main/java/io/rowweave/Sink.java:15` as `AutoCloseable` plus
`Serializable` with two abstract methods: `getSchema()` at `:17` and
`collect(DataRow)` at `:19`. Data leaves the DAG only through a sink, and the
runtime never hands rows back to the client.

Serialization is a hard requirement rather than a convention: the sink executes
in the tail stage of a Worker, so an implementation carries a description (a
path, a domain and member name) and opens the real resource lazily after
deserialization. `close()` is a no-op by default at
`rowweave-core/src/main/java/io/rowweave/Sink.java:23`, which is safe only for a
sink that holds nothing; a buffered sink must override it to flush, because the
engine guarantees exactly one `close()` after every row has been collected.

`abort()` at `rowweave-core/src/main/java/io/rowweave/Sink.java:35` is the
discard path: it is called when the pipeline fails midway and must leave no dirty
data behind. The default delegates to `close()`. A sink that writes irreversible
effects must override it, and a cleanup failure must be thrown rather than
swallowed.

## Two capability tiers

[`BatchSink`](../../../rowweave-core/src/main/java/io/rowweave/BatchSink.java)
(`rowweave-core/src/main/java/io/rowweave/BatchSink.java:4`) is the optional
tier that adds a frozen descriptor through `batchSinkDescriptor()` at
`BatchSink.java:7`.

[`BatchSinkDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/BatchSinkDescriptor.java)
at `rowweave-core/src/main/java/io/rowweave/BatchSinkDescriptor.java:13` records
connector type, version, schema fingerprint, destination id, commit domain id,
binding identity and capability profile. Its version is
`BatchSinkDescriptor.java:17`, the identity fields are byte-capped at `:18`,
`:19`, `:20` and `:21`, and `NO_BINDING_IDENTITY` is the literal at `:22`.

`CapabilityProfile` is a closed three-value enum at `:25`:

| Value | Declares |
|---|---|
| `LEGACY_PROCESS_LOCAL` | a sink written on one process, with no commit protocol claim |
| `SIDE_EFFECT_FREE_V1` | writes nothing observable, so retry is always safe |
| `MANIFEST_ATOMIC_GROUP_V1` | publishes as one member of an atomically visible group |

## How a sink joins a job

`DataSet.sinkTo(Sink)` is declared at
`rowweave-core/src/main/java/io/rowweave/DataSet.java:210` and the
stable-UID overload `DataSet.sinkTo(String, Sink)` at `DataSet.java:220`. Both
require the DAG to originate from `ExecutionEnvironment.fromSource`; a node
built without a session fails with `IllegalStateException` at `:214`. Registering
a sink compiles the graph; it does not run anything.

## Built-in sinks

| Sink | Tier | Worker-writable | Commit protocol |
|---|---|---|---|
| [`PrintSink`](../../../rowweave-core/src/main/java/io/rowweave/PrintSink.java) | `Sink` | yes at `PrintSink.java:46` | none |
| [`FileSink`](../../../rowweave-core/src/main/java/io/rowweave/FileSink.java) | `Sink` | yes at `FileSink.java:102` | full, `FileSink.java:292` |
| [`ManifestFileSink`](../../../rowweave-core/src/main/java/io/rowweave/ManifestFileSink.java) | `BatchSink` | yes at `ManifestFileSink.java:349` | atomic group |
| [`DiscardSink`](../../../rowweave-core/src/main/java/io/rowweave/DiscardSink.java) | `BatchSink` | yes at `DiscardSink.java:37` | no-op group |
| [`CollectingSink`](../../../rowweave-core/src/main/java/io/rowweave/CollectingSink.java) | `Sink` | no | none, local only |
| [`ProviderBatchSink`](../../../rowweave-core/src/main/java/io/rowweave/ProviderBatchSink.java) | `BatchSink` | no at `ProviderBatchSink.java:88` | delegated to provider |

`PrintSink` is a debug sink: its `PrintStream` field is transient, so after
deserialization on a Worker each row lands on Worker stdout
(`rowweave-core/src/main/java/io/rowweave/PrintSink.java:33`).

`DiscardSink` buffers nothing and declares the side-effect-free profile with the
fixed identity built at `rowweave-core/src/main/java/io/rowweave/DiscardSink.java:25`:
connector type `rowweave.discard`, destination `none:discard` and commit domain
`none:side-effect-free` at `:27`, profile at `:28`. Its four commit methods are
present and inert.

`CollectingSink` holds rows in client memory and exists for local runs and
tests. Its `getRows()` at
`rowweave-core/src/main/java/io/rowweave/CollectingSink.java:121` returns a
bounded view, and the same
[`CompatibilityDataLimits`](../../../rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimits.java)
ceiling that applies to sources applies here: defaults at
`rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimits.java:9` and
`:10`, hard ceiling at `:11` and `:12`, reported as
`CompatibilityDataLimitException` from `CollectingSink.java:147`.

## Worker-writable and exclusive destinations

`isWorkerWritable()` defaults to `false` at
`rowweave-core/src/main/java/io/rowweave/Sink.java:76`. A cluster
submission checks it before the RPC and the Master checks the submitted graph
again, so a memory-only sink fails fast rather than silently producing an empty
result; there is no client-collect fallback.

`exclusiveDestinationId()` at
`rowweave-core/src/main/java/io/rowweave/Sink.java:84` names the external
destination the sink owns, and within one plan graph a non-null identity may
appear only once. `FileSink` derives its identity from the resolved target path
at `rowweave-core/src/main/java/io/rowweave/FileSink.java:107`; sinks with no
exclusive destination, such as stdout, memory and discard, return `null`.

`FileSink.withShardSuffix(int)` at `FileSink.java:91` derives the sink that
writes one shard of a distributed result, so independent shards never interfere.

## Coordinated commit

`supportsCoordinatedCommit()` defaults to `false` at
`rowweave-core/src/main/java/io/rowweave/Sink.java:42`, so an irreversible
`close()` is never mistaken for a prepare. The four protocol methods are
declared at `Sink.java:47`, `:53`, `:59` and `:65` and throw
`UnsupportedOperationException` by default. The protocol is, in order: solidify
the private attempt without publishing it, publish while keeping enough state to
undo, roll back on group failure with repeated calls safe, and finalize once the
whole group is committed.

`FileSink` implements the protocol over a staging file and an explicit state
machine declared at `rowweave-core/src/main/java/io/rowweave/FileSink.java:55`.
`prepareCommit()` at `FileSink.java:297` closes the writer and marks the attempt
prepared, and a zero-row result is still a success: an empty attempt is
published through the same atomic path as a non-empty one.
`commitPrepared()` at `FileSink.java:334` moves the previous result aside and
renames the attempt into place; `rollbackCommit()` at `FileSink.java:402`
restores it; `finalizeCommit()` at `FileSink.java:458` releases the backup. A
method called out of order fails with `IllegalStateException` naming the state,
and `abort()` at `FileSink.java:485` deletes this attempt's staging file.

`ManifestFileSink` is the atomic-group implementation: `prepareCommit()` at
`rowweave-core/src/main/java/io/rowweave/ManifestFileSink.java:359`,
`commitPrepared()` at `:373`, `rollbackCommit()` at `:428`,
`finalizeCommit()` at `:489` and `abort()` at `:513`. Its descriptor is a real
manifest domain identity, bound to the attempt it is allowed to commit through
`bindManifestAttempt()` at `ManifestFileSink.java:305`.

## Serialized row format

`FileSink` writes one tab-separated line per row and reconstructs every cell
from the schema's declared type. Null is the literal `\N` declared at
`rowweave-core/src/main/java/io/rowweave/FileSink.java:52`. An untyped computed
column is outside the file's persisted scope, because an output header always
has concrete types.

`FileSink.read(DataSchema, String)` at
`rowweave-core/src/main/java/io/rowweave/FileSink.java:524` and its explicit
limits overload at `:534` are compatibility reads for small results: they
materialize the whole file under a hard bound and throw rather than returning a
partial result. For anything larger, read the file through
`FileSource.openReader()` one row at a time.

## Provider sinks are descriptions only

When a plan is compiled for a durable provider, the sink that travels in the
graph is `ProviderBatchSink`
(`rowweave-core/src/main/java/io/rowweave/ProviderBatchSink.java:10`), not the
user's object. It carries the descriptor, the configuration codec descriptor and
canonical configuration bytes, and it refuses to act locally: `collect` at
`rowweave-core/src/main/java/io/rowweave/ProviderBatchSink.java:64` and every
later lifecycle method throws the `RUNTIME_REQUIRED` message declared at `:13`.

Its constructor is the admission gate. It requires all four fields at `:26`, a
schema fingerprint matching the descriptor at `:30`, the atomic-group profile at
`:35`, and configuration bytes within both the codec bound and the SPI ceiling
at `:41`. `ManifestFileSink.toProviderSink()` at
`rowweave-core/src/main/java/io/rowweave/ManifestFileSink.java:170` produces one,
and `fromProviderSink()` at `:176` rebuilds the manifest sink only when the
descriptor, connector version and configuration codec all match exactly, using
`PROVIDER_CONFIGURATION_CODEC` declared at `ManifestFileSink.java:29`.

## Tested by

- [`BatchSinkPlanBindingTest`](../../../rowweave-core/src/test/java/io/rowweave/plan/BatchSinkPlanBindingTest.java)
  and [`BatchSinkDescriptorCodecTest`](../../../rowweave-core/src/test/java/io/rowweave/BatchSinkDescriptorCodecTest.java):
  descriptor binding and wire round-trip within byte ceilings.
- [`MultiSinkExecutionGraphTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/MultiSinkExecutionGraphTest.java)
  and [`ClusterJoinFanoutTwoSinkTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/ClusterJoinFanoutTwoSinkTest.java):
  several sinks in one graph, shared subgraph computed once.
- [`ClusterManifestCommitTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/ClusterManifestCommitTest.java)
  and [`TypedQualityPublicationLocalExecutionTest`](../../../rowweave-core/src/test/java/io/rowweave/TypedQualityPublicationLocalExecutionTest.java):
  atomic-group publication end to end.
- [`DurableCommitBarrierFailureTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/DurableCommitBarrierFailureTest.java)
  and [`DurableSinkCommitCoordinatorTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/DurableSinkCommitCoordinatorTest.java):
  prepare, publish, roll back and finalize across failures.
- [`BatchSinkOutcomeTest`](../../../rowweave-core/src/test/java/io/rowweave/commit/BatchSinkOutcomeTest.java)
  and [`BatchSinkAbortRecordTest`](../../../rowweave-core/src/test/java/io/rowweave/run/BatchSinkAbortRecordTest.java):
  outcome reporting and abort evidence.
- [`ProviderSinkWriteSessionTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/ProviderSinkWriteSessionTest.java)
  and [`ProviderSinkConfigurationEnvelopeTest`](../../../rowweave-core/src/test/java/io/rowweave/plan/ProviderSinkConfigurationEnvelopeTest.java):
  provider sinks driven by runtime rather than locally.
- [`BatchSinkScaleProfileTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/BatchSinkScaleProfileTest.java)
  and [`SinkTargetIdTest`](../../../rowweave-core/src/test/java/io/rowweave/sched/net/SinkTargetIdTest.java):
  destination identity and placement at scale.

Related: [built-in Batch sources and source replay](batch-sources.md),
[durable sink provider boundary](durable-sink-provider.md),
[schema conversion and row validation](schema-and-validation.md),
[Batch behavior contracts](batch-behavior.md),
[format constants](format-constants.md).
