---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Built-in Batch sources and source replay

A source is a bounded row sequence that carries its own schema. This page states
the source contract, every built-in Batch implementation, the size limits each
one enforces, and the replay contract a split declares when a Worker has to read
the same split again. For how the runtime opens a source, see
[source executor and provider SPI](source-executor-spi.md).

## The source contract

[`Source`](../../../rowweave-core/src/main/java/io/rowweave/Source.java) is
`Iterable<DataRow>` plus `Serializable` plus one schema accessor, declared at
`rowweave-core/src/main/java/io/rowweave/Source.java:18`, with `getSchema()` at
`Source.java:20`. Two consequences are load-bearing:

* Serialization is a hard contract. The plan description is built on the client
  and rebuilt on a Worker, so a source holds descriptors and references, never
  live connections or opened readers.
* Splitting is the caller's decision. The engine parallelizes over the array it
  is handed and treats each `Source` as exactly one split, so a single source
  runs at parallelism one.

`ExecutionEnvironment.fromSource(Source...)`
(`rowweave-core/src/main/java/io/rowweave/ExecutionEnvironment.java:138`) builds
one logical source from several homogeneous splits rather than unioning them
into several sources, and every split must carry an identical schema.

[`BatchSource`](../../../rowweave-core/src/main/java/io/rowweave/BatchSource.java)
(`rowweave-core/src/main/java/io/rowweave/BatchSource.java:7`) is the optional
capability that adds a frozen split descriptor through `batchSourceSplit()` at
`BatchSource.java:13`. A `BatchSource` that returns `null` for that descriptor
is asking for legacy classification; a custom implementation must not return
`null`.

## Built-in sources

| Source | Schema | Splits the descriptor | Bounded resident copy |
|---|---|---|---|
| [`CollectionSource`](../../../rowweave-core/src/main/java/io/rowweave/CollectionSource.java) | explicit | only with a stable split id | snapshots every row |
| [`IterableSource`](../../../rowweave-core/src/main/java/io/rowweave/IterableSource.java) | explicit | no | none, delegates lazily |
| [`FileSource`](../../../rowweave-core/src/main/java/io/rowweave/FileSource.java) | explicit | no | one-row lookahead |
| [`ManifestFileSource`](../../../rowweave-core/src/main/java/io/rowweave/ManifestFileSource.java) | explicit | yes when generation-pinned | none |
| [`ConcatSource`](../../../rowweave-core/src/main/java/io/rowweave/ConcatSource.java) | first part | no | none |
| [`JdbcSnapshotSource`](../../../rowweave-core/src/main/java/io/rowweave/JdbcSnapshotSource.java) | explicit | yes | none |
| [`ObjectStorageSnapshotSource`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageSnapshotSource.java) | explicit | yes | none |
| [`ObjectStorageQualitySource`](../../../rowweave-core/src/main/java/io/rowweave/ObjectStorageQualitySource.java) | carrier schema | yes | none |
| [`IncrementalSourceSplit`](../../../rowweave-core/src/main/java/io/rowweave/IncrementalSourceSplit.java) | explicit | yes | none |
| [`AssignedSourceBatch`](../../../rowweave-core/src/main/java/io/rowweave/AssignedSourceBatch.java) | first binding | per binding | none |

[`IterableSource`](../../../rowweave-core/src/main/java/io/rowweave/IterableSource.java)
is the adapter for anything already `Iterable<DataRow>`: `of` is declared at
`rowweave-core/src/main/java/io/rowweave/IterableSource.java:25` and each
`next()` takes an independent row snapshot, so the adapter never reuses a row
reference even when the underlying iterable does.

[`CollectionSource`](../../../rowweave-core/src/main/java/io/rowweave/CollectionSource.java)
copies the collection into an embedded snapshot, which is what makes it exactly
replayable. The four public constructors are `CollectionSource.java:33`,
`:38`, `:47` and `:54`; the two that take a `stableSplitId` route through
`requireStableSplitId` at `:116` and reject a null id. A source with no stable
id declares no descriptor (`:130`) and is classified as legacy. If the live
collection changes while the snapshot is being taken, the size is re-checked and
the copy fails at `:84` and `:95`.

[`FileSource`](../../../rowweave-core/src/main/java/io/rowweave/FileSource.java)
reads tab-separated lines. A null schema fails at
`rowweave-core/src/main/java/io/rowweave/FileSource.java:29` and an empty path
at `:32`. `openReader()` (`:48`) returns the closeable `Reader` declared at
`:62`, which holds one row of lookahead; a failure to open the file surfaces as
an `UncheckedIOException` naming the path at `:75`, and reading past the end
throws `NoSuchElementException` at `:105`.

[`ManifestFileSource`](../../../rowweave-core/src/main/java/io/rowweave/ManifestFileSource.java)
reads one member of a committed manifest domain. The two public constructors are
`ManifestFileSource.java:28` and `:32`, and a generation that is not positive
fails at `:46`. Opening verifies the domain identity and the pinned generation
before the first row moves, so a domain rewritten after the source was built
fails at `:72`, a missing generation at `:77`, a missing member at `:88`, and a
member whose schema fingerprint differs at `:91`.

[`ConcatSource`](../../../rowweave-core/src/main/java/io/rowweave/ConcatSource.java)
is how one physical source instance reads every split of a logical source, and
its constructor at `rowweave-core/src/main/java/io/rowweave/ConcatSource.java:24`
rejects an empty list at `:26`. Each `iterator()` call replays every part from
the beginning, so the whole construct is replayable only if every part is.

## Connector sources

The JDBC and object storage sources implement `BatchExecutableSource`, so they
are read through a managed executor and never by an enhanced-for loop. Their
published limits are constants you can read rather than conventions: the JDBC
source caps fetch size at
`rowweave-core/src/main/java/io/rowweave/JdbcSnapshotSource.java:19`, with the
default at `:18`, the query timeout at `:20` and `:21`, and cell width at `:22`,
`:23` and `:24`. The object storage source caps row width at
`rowweave-core/src/main/java/io/rowweave/ObjectStorageSnapshotSource.java:18`,
`:19` and `:20`. The quality source caps reason buckets at
`rowweave-core/src/main/java/io/rowweave/ObjectStorageQualitySource.java:24`.

Connector sources bind a provider rather than a connection. The provider must be
a public class with a public no-arg constructor and no instance fields, checked
at `rowweave-core/src/main/java/io/rowweave/JdbcSnapshotSource.java:140`, `:144`
and `:153`, and its type and version are part of the split identity. Because the
provider is inert, a source whose provider identity changes after plan binding
refuses to report its descriptor at `JdbcSnapshotSource.java:64`.

The split descriptor a connector declares names the connector type, records a
schema fingerprint, and states the replay guarantee, as built at
`rowweave-core/src/main/java/io/rowweave/JdbcSnapshotSource.java:130`.

## Replay contract

[`BatchSourceSplitDescriptor`](../../../rowweave-core/src/main/java/io/rowweave/BatchSourceSplitDescriptor.java)
is the frozen, serializable description of one bounded split, declared at
`rowweave-core/src/main/java/io/rowweave/BatchSourceSplitDescriptor.java:13`.
Its version is `BatchSourceSplitDescriptor.java:17` and the identity fields are
byte-capped: connector type at `:18`, split id at `:19`, snapshot id at `:20`.

`ReplayGuarantee` is a closed two-value enum at `:22`:

| Value | Meaning |
|---|---|
| `EXACT_ORDERED_FROM_START` | a second attempt yields the identical sequence, row for row |
| `LEGACY_SINGLE_ATTEMPT` | no ordering or re-read promise; the split is read once per attempt |

Every text field passes `boundedText` at `:82`, which requires non-empty content,
strict UTF-8 decodability, and a byte ceiling rather than a character count. A
deserialized instance is rebuilt through the public invariants at `:104`, so
serialization cannot forge an out-of-range descriptor.

`AssignedSourceBatch` is the shape the scheduler hands to a Worker after it has
assigned splits. It rejects an empty binding list at
`rowweave-core/src/main/java/io/rowweave/AssignedSourceBatch.java:25`, a repeated
split id at `:38`, and a binding whose schema fingerprint differs from the batch
schema at `:51`. Its `iterator()` at `:69` throws, because an assigned batch is
read only through its dedicated executor. `IncrementalSourceSplit`
(`rowweave-core/src/main/java/io/rowweave/IncrementalSourceSplit.java:20`)
states the same rule through its `RUNTIME_REQUIRED` message at `:23` and throws
it from `iterator()` at `:131`.

## Data limits

`CollectionSource` bounds what it will hold in memory with
[`CompatibilityDataLimits`](../../../rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimits.java):
defaults of 100000 rows and 64 MiB at
`rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimits.java:9` and
`:10`, and a hard, non-disableable ceiling of 1000000 rows and 512 MiB at `:11`
and `:12`. An explicit limit may lower those numbers but may never exceed the
hard ceiling. Overflow is reported as `CompatibilityDataLimitException`, raised
for row count at `rowweave-core/src/main/java/io/rowweave/CollectionSource.java:72`
and for resident bytes at `:110`.

## Tested by

- [`BatchSourceBuiltInAdmissionTest`](../../../rowweave-core/src/test/java/io/rowweave/plan/BatchSourceBuiltInAdmissionTest.java)
  and [`BatchSourceInputCompatibilityTest`](../../../rowweave-core/src/test/java/io/rowweave/BatchSourceInputCompatibilityTest.java):
  which sources a plan accepts and how homogeneous splits are validated.
- [`BatchSourcePlanBindingTest`](../../../rowweave-core/src/test/java/io/rowweave/plan/BatchSourcePlanBindingTest.java):
  descriptor binding from graph construction through submission.
- [`BatchSourceSplitDescriptorCodecTest`](../../../rowweave-core/src/test/java/io/rowweave/BatchSourceSplitDescriptorCodecTest.java):
  wire round-trip, byte ceilings, and rejected descriptors.
- [`AssignedSourceInputBatchExecutorTest`](../../../rowweave-core/src/test/java/io/rowweave/AssignedSourceInputBatchExecutorTest.java)
  and [`BoundedCompatibilityReadTest`](../../../rowweave-core/src/test/java/io/rowweave/BoundedCompatibilityReadTest.java):
  assigned batches and the compatibility read path under limits.
- [`ObjectStorageSnapshotSourceModelTest`](../../../rowweave-core/src/test/java/io/rowweave/ObjectStorageSnapshotSourceModelTest.java),
  [`ObjectStorageQualityBoundsLifecycleTest`](../../../rowweave-core/src/test/java/io/rowweave/ObjectStorageQualityBoundsLifecycleTest.java)
  and [`QualityEvidenceReadGuardTest`](../../../rowweave-core/src/test/java/io/rowweave/QualityEvidenceReadGuardTest.java):
  connector bounds, provider identity, and evidence read guards.
- [`BatchSourceScaleProfileTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/BatchSourceScaleProfileTest.java):
  split assignment across Workers.

Related: [source executor and provider SPI](source-executor-spi.md),
[Batch sinks and output boundaries](batch-sinks.md),
[custom function interfaces](custom-functions.md),
[Batch behavior contracts](batch-behavior.md).
