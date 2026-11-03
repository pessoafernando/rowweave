---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Source executor and provider SPI

A source describes where data comes from; an executor is the object that
actually opens a reader and emits rows on a Worker. This page states that
separation, the executor lifecycle contract, how an executor is selected, and
what a deployment has to satisfy before a provider is allowed to run. For the
source implementations themselves, see
[built-in Batch sources and source replay](batch-sources.md).

## Description separated from execution

[`Source`](../../../rowweave-core/src/main/java/io/rowweave/Source.java) is pure
metadata: it can be serialized, dispatched and retried, and it owns no open
resource. [`SourceExecutor`](../../../rowweave-core/src/main/java/io/rowweave/SourceExecutor.java)
is the executing reader, instantiated at execution time inside a stage, declared
at `io/rowweave/SourceExecutor.java:21`. The split exists because a description
is built once on the client while a reader must be created per attempt, per
Worker, per split.

The lifecycle is `open`, then repeated `hasNext` and `next`, then `close`, and
all four are abstract on the base class: `io/rowweave/SourceExecutor.java:32`,
`:35`, `:38` and `:42`. Two rules in that contract are load-bearing:

* `open()` is lazy. It positions the reader, and the first real read happens in
  `next()`, so an unused executor costs nothing.
* `next()` must produce a new `DataRow` for every logical row and never reuse or
  overwrite an object it has already emitted. The rule is stated at
  `io/rowweave/SourceExecutor.java:17`. Under pure push the reference handed
  downstream *is* the row, so reuse would silently rewrite data another stage is
  still reading. That is why there is no `next(DataRow reuse)` overload.

`close()` is idempotent and must also be called when a read throws or the
attempt is cancelled; it is the backstop for resource release, not an optional
cleanup.

## How an executor is selected

[`SourceExecutorFactory`](../../../rowweave-core/src/main/java/io/rowweave/SourceExecutorFactory.java)
at `io/rowweave/SourceExecutorFactory.java:15` resolves a source to an executor
in one fixed order, so the outcome never depends on registration timing:

| Step | Condition | Result |
|---|---|---|
| 1 | source is an assigned split batch | dedicated executor at `io/rowweave/SourceExecutorFactory.java:35` |
| 2 | source concatenates splits | dedicated executor at `:38` |
| 3 | source is a plain file reader | dedicated executor at `:41` |
| 4 | source reads one manifest member | dedicated executor at `:44` |
| 5 | source is self-executing | its own executor at `:47` |
| 6 | source type is registered | registered constructor at `:61` |
| 7 | none of the above | `IterableInputExecutor` bridge at `:64` |

A `null` source is rejected at `io/rowweave/SourceExecutorFactory.java:33`.
Step 5 additionally verifies the returned executor: a `null` result fails at
`:51`, and an executor bound to a different source instance fails at `:55`, so a
provider cannot smuggle a stale reader into a retried attempt.

`register(Class, Function)` at `io/rowweave/SourceExecutorFactory.java:24` is the
extension point for a custom source type that wants a dedicated reader rather
than the bridge.

## The compatibility bridge

Step 7 matters because every source is already `Iterable<DataRow>`, so an
unregistered type runs with no code change at all.
[`IterableInputExecutor`](../../../rowweave-core/src/main/java/io/rowweave/IterableInputExecutor.java)
at `io/rowweave/IterableInputExecutor.java:15` opens by taking the iterator
lazily at `:25` and delegates `next()` at `:37`.

The bridge deliberately does not copy rows, so the row-reference contract becomes
the source's own responsibility: an implementation that recycles one mutable row
object corrupts downstream data. The built-in in-memory sources take their row
snapshot at their own iterator boundary instead, and the bridge does not copy a
second time. When the source itself implements `AutoCloseable`, `close()` calls
back into it as a resource-release floor, at
`io/rowweave/IterableInputExecutor.java:42`.

## Self-executing sources

[`BatchExecutableSource`](../../../rowweave-core/src/main/java/io/rowweave/BatchExecutableSource.java)
at `io/rowweave/BatchExecutableSource.java:9` is the shape a connector source
uses when it must not depend on the process-global registry: `createSourceExecutor()`
at `:12` returns an executor bound to that exact instance, and direct iteration
is closed off deliberately at `:16`. A source that can only be read through a
managed executor says so in its type rather than in documentation.

The bounded incremental path layers the same idea with supervision.
[`IncrementalSourceRuntime`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalSourceRuntime.java)
at `io/rowweave/IncrementalSourceRuntime.java:19` binds a frozen split to the
Worker's provider, and its internal input refuses to enter an artifact at `:125`,
because a runtime-bound reader is not a transferable description. Its executor
acquires an attempt permit before it opens anything, from `open()` at `:141`.

## Built-in executors

| Source type | Executor | Declared |
|---|---|---|
| `AssignedSourceBatch` | `AssignedSourceInputBatchExecutor` | `io/rowweave/AssignedSourceInputBatchExecutor.java:6` |
| `ConcatSource` | `ConcatInputExecutor` | `io/rowweave/ConcatInputExecutor.java:6` |
| `FileSource` | `FileInputExecutor` | `io/rowweave/FileInputExecutor.java:4` |
| `ManifestFileSource` | `ManifestFileInputExecutor` | `io/rowweave/ManifestFileInputExecutor.java:4` |
| `JdbcSnapshotSource` | `JdbcSnapshotInputExecutor` | `io/rowweave/JdbcSnapshotInputExecutor.java:15` |
| `ObjectStorageSnapshotSource` | `ObjectStorageSnapshotInputExecutor` | `io/rowweave/ObjectStorageSnapshotInputExecutor.java:16` |
| `ObjectStorageQualitySource` | `ObjectStorageQualityInputExecutor` | `io/rowweave/ObjectStorageQualityInputExecutor.java:12` |
| any other source | `IterableInputExecutor` | `io/rowweave/IterableInputExecutor.java:15` |

The quality executor is package-private and reads with one-row lookahead, with a
sized stream buffer declared at
`io/rowweave/ObjectStorageQualityInputExecutor.java:15`
and a bounded rescan limit at
`io/rowweave/ObjectStorageQualityInputExecutor.java:16`,
so a corrupt record cannot make the reader wander.
Connector executors wrap their own failure type rather than leaking a driver
exception, at `io/rowweave/JdbcSnapshotInputExecutor.java:314` and
`io/rowweave/ObjectStorageSnapshotInputExecutor.java:231`.

## Provider contract

A connector provider is an extension point in a signed deployment, not a plugin
the client picks. The JDBC and object storage shapes are
[`JdbcSnapshotSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/JdbcSnapshotSourceProvider.java)
at `io/rowweave/JdbcSnapshotSourceProvider.java:6` and
[`ObjectStorageSourceProvider`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageSourceProvider.java)
at `io/rowweave/ObjectStorageSourceProvider.java:6`. Each declares a stable
family name, a positive version, and an `open` that resolves references on the
Worker, so a credential reference never needs to mean anything on the client.

Admission is mechanical and fails before any row moves. A provider class must be
public, must expose a public no-arg constructor, and must declare no instance
fields: `io/rowweave/JdbcSnapshotSource.java:144`, `:153` and `:160`. A provider
whose identity differs from the one frozen into the plan is refused at
`io/rowweave/JdbcSnapshotSource.java:64`. The reason is durability, not tidiness:
an instance field is invisible to the plan, so it would silently differ between
an attempt and its retry.

## Deployment discovery and limits

Provider sets are discovered from a service resource and bounded per deployment.
[`DurableBatchSinkProviderRegistry`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DurableBatchSinkProviderRegistry.java)
is declared at `io/rowweave/deployment/DurableBatchSinkProviderRegistry.java:25`,
names its service resource at `:27`, caps service size, provider count, candidate
count and name length at `:29`, `:30`, `:31` and `:32`, and offers two roles at
`:34`. [`IncrementalSourceProviderRegistry`](../../../rowweave-core/src/main/java/io/rowweave/deployment/IncrementalSourceProviderRegistry.java)
mirrors the same shape, declared at
`io/rowweave/deployment/IncrementalSourceProviderRegistry.java:25` with its
resource at `:26` and ceilings at `:28` through `:31`. Loading a provider whose
contract is not in the scanned set throws at
`io/rowweave/deployment/DurableBatchSinkProviderRegistry.java:127` and
`io/rowweave/deployment/IncrementalSourceProviderRegistry.java:137`.

[`DurableSinkProviderPlanPreflight`](../../../rowweave-core/src/main/java/io/rowweave/plan/DurableSinkProviderPlanPreflight.java)
at `io/rowweave/plan/DurableSinkProviderPlanPreflight.java:16` is where a plan
that names a provider is checked against the deployment that will run it.
`validate` at `:20` requires both inputs at `:24` and rejects an out-of-budget
attempt at `:49`; `validateClientGraph` at `:126` applies the same rule before
submission, so a plan is never accepted that the Workers cannot honour.

## Conformance expectations

A provider implementation is expected to satisfy, and be tested against:

* the exact lifecycle above, with `close()` safe to call twice,
* the row-reference rule, so every `next()` returns a fresh row,
* determinism of configuration and locator canonicalization, so two attempts of
  the same split produce byte-identical descriptions,
* the published byte and duration ceilings rather than private ones,
* replay that matches the guarantee its split descriptor declares, as stated on
  [built-in Batch sources and source replay](batch-sources.md),
* bounded resident memory and handle counts under the attempt budget on
  [durable sink provider boundary](durable-sink-provider.md).

## Tested by

- [`AssignedSourceInputBatchExecutorTest`](../../../rowweave-core/src/test/java/io/rowweave/AssignedSourceInputBatchExecutorTest.java):
  the assignment wrapper path and its refusal to be iterated directly.
- [`ObjectStorageSnapshotInputExecutorTest`](../../../rowweave-core/src/test/java/io/rowweave/ObjectStorageSnapshotInputExecutorTest.java)
  and [`ObjectStorageQualityInputExecutorTest`](../../../rowweave-core/src/test/java/io/rowweave/ObjectStorageQualityInputExecutorTest.java):
  connector executor open, read, failure and close behaviour.
- [`ObjectStorageQualityBoundsLifecycleTest`](../../../rowweave-core/src/test/java/io/rowweave/ObjectStorageQualityBoundsLifecycleTest.java)
  and [`BoundedCompatibilityReadTest`](../../../rowweave-core/src/test/java/io/rowweave/BoundedCompatibilityReadTest.java):
  published ceilings enforced at execution time.
- [`IncrementalSourceRuntimeContractTest`](../../../rowweave-core/src/test/java/io/rowweave/IncrementalSourceRuntimeContractTest.java)
  and [`IncrementalSourceProviderRegistryTest`](../../../rowweave-core/src/test/java/io/rowweave/deployment/IncrementalSourceProviderRegistryTest.java):
  runtime binding, discovery and contract lookup.
- [`WorkerIncrementalSourceActionTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/WorkerIncrementalSourceActionTest.java)
  and [`MasterDurableRunPreflightTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/MasterDurableRunPreflightTest.java):
  provider admission enforced on the Worker and in the preflight.
- [`DurableBatchSinkProviderRegistryTest`](../../../rowweave-core/src/test/java/io/rowweave/deployment/DurableBatchSinkProviderRegistryTest.java)
  and [`DurableBatchSinkProviderConformanceRedTest`](../../../rowweave-core/src/test/java/io/rowweave/sink/DurableBatchSinkProviderConformanceRedTest.java):
  durable provider discovery and conformance refusal.
- [`DurableJdbcTestProvider`](../../../rowweave-core/src/test/java/io/rowweave/cluster/DurableJdbcTestProvider.java):
  the reference provider used by the cluster suites.

Related: [built-in Batch sources and source replay](batch-sources.md),
[Batch sinks and output boundaries](batch-sinks.md),
[durable sink provider boundary](durable-sink-provider.md),
[bounded incremental Batch source](incremental-batch-source.md),
[formats, limits, and SPI](formats-limits-spi.md).
