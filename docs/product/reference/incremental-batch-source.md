---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Bounded incremental Batch source

The incremental path reads a changing feed in a sequence of bounded runs: capture
where the feed stands, materialize exact splits for the interval, execute them as
an ordinary Batch job, and advance a durable frontier only when the run commits.
This page states that contract, the frontier kinds, the run modes, the published
ceilings and the refusals. The row-reading half is the same
[built-in Batch source](batch-sources.md) contract, and the writing half is the
[durable sink provider boundary](durable-sink-provider.md).

Nothing here is unbounded. A run that would never finish, a join that would hold
a feed, or a sort over an open interval is refused rather than attempted, the
same stable refusals stated in the [capability matrix](capability-matrix.md).

## Roles

| Type | Responsibility | Declared |
|---|---|---|
| `IncrementalSourceProvider` | same-build extension that captures and materializes | `io/rowweave/IncrementalSourceProvider.java:13` |
| `IncrementalSourceProviderContract` | the self-description a Worker verifies | `io/rowweave/incremental/IncrementalSourceProviderContract.java:8` |
| `IncrementalSourceConfig` | inert plan proxy for one feed | `io/rowweave/incremental/IncrementalSourceConfig.java:9` |
| `IncrementalSourceSplit` | one frozen, bounded, replayable split | `io/rowweave/IncrementalSourceSplit.java:20` |
| `IncrementalSourceRuntime` | Worker binding that turns a split into a reader | `io/rowweave/IncrementalSourceRuntime.java:19` |
| `IncrementalJobClient` | submit, cancel and recover a run | `io/rowweave/incremental/IncrementalJobClient.java:78` |
| `IncrementalAuthorityGateway` | durable frontier and joint commit state | `io/rowweave/incremental/IncrementalAuthorityGateway.java:37` |

The provider interface is deliberately small. `contract()` at
`io/rowweave/IncrementalSourceProvider.java:16` is what the Worker compares with
the signed contract it discovered; `canonicalizeConfiguration` at `:19` must
decode and re-encode the inert configuration into byte-exact canonical form;
`captureTarget` at `:22` performs one read-only bounded capture; and
`materializeInterval` at `:27` produces exact split metadata from the already
frozen receipt. `openSplit` at `:36` is the one optional member and it throws
`INCREMENTAL_SOURCE_OPEN_UNSUPPORTED` by default at
`io/rowweave/IncrementalSourceProvider.java:38`, because a provider that has not
been qualified for reading must not be able to read.

Every other member is abstract: there is no default that quietly skips capture
verification, and a provider cannot half-implement the contract.

## Configuration is inert

[`IncrementalSourceConfig`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceConfig.java)
binds a provider key, a 32-byte contract digest and opaque configuration bytes,
as its constructor at `io/rowweave/incremental/IncrementalSourceConfig.java:18`
requires. It travels into the plan and the artifact and acts as nothing until an
admitted Worker provider decodes it. That is why a plan submitted to a deployment
without the matching provider is refused before any row moves, through
`IncrementalSourceProviderRegistry.requireContract()` at
`io/rowweave/deployment/IncrementalSourceProviderRegistry.java:121`.

## Frontiers

A frontier is the durable statement of how far a feed has been consumed.
[`IncrementalFrontier`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontier.java)
at `io/rowweave/incremental/IncrementalFrontier.java:6` is a closed sum of four
kinds declared at `:8`:

| Kind | Meaning | Constructor |
|---|---|---|
| `BEGIN_V1` | nothing consumed yet | `io/rowweave/incremental/IncrementalFrontier.java:43` |
| `INT64_SEQUENCE_V1` | a monotonic integer position | `IncrementalFrontier.java:47` |
| `TIMESTAMP_KEY_V1` | an event-time instant plus an ordering key | `IncrementalFrontier.java:52` |
| `MANIFEST_GENERATION_V1` | a committed manifest generation plus digest | `IncrementalFrontier.java:61` |

Accessors are kind-checked, so reading a position of the wrong kind fails rather
than returning a misleading default: `sequence()` at
`io/rowweave/incremental/IncrementalFrontier.java:74`, `epochMicros()` at `:79`,
`orderedKey()` at `:84` and `generation()` at `:89`. The contract states which
kind a provider uses through `frontierKind()` at
`io/rowweave/incremental/IncrementalSourceProviderContract.java:139`, and a
capture that reports a different kind is a conflict.

A frontier whose identity is internally inconsistent is frozen deliberately:
[`FrontierConflictException`](../../../rowweave-core/src/main/java/io/rowweave/incremental/FrontierConflictException.java)
at `io/rowweave/incremental/FrontierConflictException.java:4` extends
`IllegalArgumentException` so the conflict surfaces as an invalid plan rather than
a transient error, and the authority records the conflict through
`MARK_CONFLICT` in the closed command set at
`io/rowweave/incremental/IncrementalAuthorityCommandKind.java:4`, whose six
members run from `INITIALIZE` at `:5` through `MARK_CONFLICT` at `:10`.

## Run modes

`IncrementalRunMode` is a closed three-value enum at
`io/rowweave/incremental/IncrementalRunMode.java:4`:

| Mode | Interval |
|---|---|
| `SNAPSHOT_V1` | the whole frozen snapshot, `IncrementalRunMode.java:5` |
| `INCREMENTAL_V1` | the interval from the committed frontier to a new capture, `:6` |
| `BACKFILL_V1` | a closed interval deliberately re-read, `:7` |

`IncrementalJobClient` exposes the three shapes as distinct calls rather than one
mode-flagged submit: `submitNormal` at
`io/rowweave/incremental/IncrementalJobClient.java:243`, `submitBackfill` at
`:264`, `cancel` at `:286` and `recover` at `:298`. Cancellation is a request, not
a cut: a run that has already committed publishes normally, and a run interrupted
before its joint commit recovers from the frontier it left behind.

## Split binding

An incremental split is assembled from four independently frozen values, and the
three public constructors are `io/rowweave/IncrementalSourceSplit.java:35`, `:43`
and `:52`. The constructor re-verifies the whole bundle before it stores
anything: required fields plus a 32-byte partition-set digest at `:64`, provider
identity agreement at `:72`, schema fingerprint agreement at `:77`, snapshot
identity agreement at `:82`, and contract digest agreement at `:88`. A split
assembled from mismatched parts therefore cannot exist, instead of failing later
mid-read.

A split is also a `BatchSource` whose descriptor always declares exact ordered
replay: `IncrementalMaterializedSplit` rejects any other guarantee at
`io/rowweave/incremental/IncrementalMaterializedSplit.java:25`, because a
retried attempt of a bounded interval must produce the identical sequence. Its
open request is bounded at `io/rowweave/incremental/IncrementalMaterializedSplit.java:11`.
Each row carries a position whose partition ordinal is capped by
`MAX_PARTITION_ORDINAL` at `io/rowweave/incremental/IncrementalRecordPosition.java:8`
and enforced at `:45`.

Direct iteration is closed off: `IncrementalSourceSplit.iterator()` at
`io/rowweave/IncrementalSourceSplit.java:131` throws the `RUNTIME_REQUIRED`
message declared at `:23`. A deserialized copy is rebuilt through the same public
invariants at `io/rowweave/IncrementalSourceSplit.java:135`, so serialization
cannot forge a split that failed binding.

## Execution binding

`IncrementalSourceRuntime.bind` at
`io/rowweave/IncrementalSourceRuntime.java:23` and its overload at `:37` attach a
frozen split to the Worker's provider, supervisor and cancellation flag. The
resulting `BatchExecutableSource` reports the same schema and descriptor as the
split it proxies, at `io/rowweave/IncrementalSourceRuntime.java:109` and `:111`,
and builds its executor at `:115`. If that binding is lost, opening fails with an
explicit `IllegalStateException` rather than an empty stream.

The bound input is barred from the artifact path: its `writeReplace` at
`io/rowweave/IncrementalSourceRuntime.java:125` throws
`NotSerializableException`, because a runtime-bound reader is not a transferable
description. Reads on the Worker go through the supervised executor declared at
`:130`, which acquires an attempt permit in `open()` at `:141` before the
provider opens anything, and releases it through `close()` at `:207`. The
row-emitting methods are `hasNext()` at `:180` and `next()` at `:192`, following
the executor lifecycle on the
[source executor and provider SPI](source-executor-spi.md) page.

Provider actions are mediated by
[`IncrementalSourceActionExecutor`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalSourceActionExecutor.java)
at `io/rowweave/incremental/IncrementalSourceActionExecutor.java:11`, which names
the two response codecs as the constants at `:13` and `:15` and requires all four
collaborators at `:32`. A capture response or a materialization response that does
not use the exact codec the contract declares is refused.

## Published ceilings

| Constant | Value site |
|---|---|
| `IncrementalSourceProviderContract.CURRENT_SPI_VERSION` | `io/rowweave/incremental/IncrementalSourceProviderContract.java:11` |
| `MAX_CONFIGURATION_BYTES` | `IncrementalSourceProviderContract.java:12` |
| `MAX_TARGET_BYTES` | `IncrementalSourceProviderContract.java:13` |
| `MAX_CAPTURE_RESPONSE_BYTES` | `IncrementalSourceProviderContract.java:14` |
| `MAX_MATERIALIZATION_RESPONSE_BYTES` | `IncrementalSourceProviderContract.java:15` |
| `IncrementalSourceProviderKey.MAX_CONNECTOR_TYPE_BYTES` | `io/rowweave/incremental/IncrementalSourceProviderKey.java:10` |
| `IncrementalSourceActionBudget.MAX_NORMAL_RESIDENT_BYTES` | `io/rowweave/incremental/IncrementalSourceActionBudget.java:10` |
| `MAX_LARGE_RESIDENT_BYTES` | `IncrementalSourceActionBudget.java:11` |
| `MAX_ACTION_TIMEOUT_MILLIS` | `IncrementalSourceActionBudget.java:12` |
| `CapturedTargetReceipt.MAX_SNAPSHOT_IDENTITY_BYTES` | `io/rowweave/incremental/CapturedTargetReceipt.java:16` |
| `IncrementalMaterializedSplit.MAX_OPEN_REQUEST_BYTES` | `io/rowweave/incremental/IncrementalMaterializedSplit.java:11` |
| `IncrementalSourceProviderRegistry.MAX_PROVIDERS_PER_DEPLOYMENT` | `io/rowweave/deployment/IncrementalSourceProviderRegistry.java:29` |

`IncrementalSourceActionBudget` at
`io/rowweave/incremental/IncrementalSourceActionBudget.java:8` separates ordinary
provider work from the one large action each run needs, and the constructor at
`:19` rejects a budget above either ceiling. The closed guarantee enums in the
contract keep the same discipline over behaviour rather than size: exactly one
ordering guarantee at
`io/rowweave/incremental/IncrementalSourceProviderContract.java:18`, exactly one
snapshot guarantee at `:32`, and the two deployment capabilities at `:46`.
Discovery is bounded too, with the service resource named at
`io/rowweave/deployment/IncrementalSourceProviderRegistry.java:26`, the size cap
at `:28` and the candidate cap at `:30`, and loading a provider that was not in
the scanned set throws at `:137`.

## Validation and joint commit

Changelog rows are validated in constant space per split by
`IncrementalChangelogRowValidator` at
`io/rowweave/incremental/IncrementalChangelogRowValidator.java:15`, so a bounded
run never needs a materialized copy of the interval to check its shape.
Interval preparation is `IncrementalIntervalPreparer` at
`io/rowweave/incremental/IncrementalIntervalPreparer.java:14`, which applies the
run-mode rules at `:163`.

Frontier advancement and publication commit are decided together, never one and
then the other, through the gateway at
`io/rowweave/incremental/IncrementalAuthorityGateway.java:37` and its closed
command set. That is what makes an incremental run idempotent: a retry either
finds its own decision already recorded or performs the same command again, and
two runs that would advance the same frontier from different positions conflict
instead of double-publishing.

## Tested by

- [`IncrementalSourceRuntimeContractTest`](../../../rowweave-core/src/test/java/io/rowweave/IncrementalSourceRuntimeContractTest.java):
  binding, refusal to iterate directly, supervised open and close.
- [`IncrementalSourceProviderRegistryTest`](../../../rowweave-core/src/test/java/io/rowweave/deployment/IncrementalSourceProviderRegistryTest.java):
  discovery, contract lookup and deployment ceilings.
- [`IncrementalSourceActionWireTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/IncrementalSourceActionWireTest.java)
  and [`IncrementalSourceActionRpcContractTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/rpc/IncrementalSourceActionRpcContractTest.java):
  capture and materialize wire form, codec identity and bound enforcement.
- [`IncrementalInitializeRpcContractTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/rpc/IncrementalInitializeRpcContractTest.java)
  and [`IncrementalSubmissionRpcContractTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/rpc/IncrementalSubmissionRpcContractTest.java):
  initialize and submit protocol.
- [`IncrementalInMemoryAuthorityClaimContractTest`](../../../rowweave-core/src/test/java/io/rowweave/incremental/IncrementalInMemoryAuthorityClaimContractTest.java):
  claim, first-attempt compare-and-set, and conflict recording.
- [`IncrementalInMemorySinkPhaseContractTest`](../../../rowweave-core/src/test/java/io/rowweave/incremental/IncrementalInMemorySinkPhaseContractTest.java):
  frontier advancement and publication decided jointly across every phase.
- [`MasterIncrementalPreparationTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/MasterIncrementalPreparationTest.java)
  and [`WorkerIncrementalSourceActionTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/WorkerIncrementalSourceActionTest.java):
  preparation on the Master and provider execution on a Worker.
- [`RealClusterIncrementalSubmissionTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/RealClusterIncrementalSubmissionTest.java)
  and [`IncrementalScaleProfileTest`](../../../rowweave-core/src/test/java/io/rowweave/cluster/IncrementalScaleProfileTest.java):
  end-to-end runs against real processes, and bounded scale behaviour.
- [`ObjectManifestIncrementalSourceProfileTest`](../../../rowweave-core/src/test/java/io/rowweave/incremental/ObjectManifestIncrementalSourceProfileTest.java):
  the manifest-generation frontier profile.

Related: [built-in Batch sources and source replay](batch-sources.md),
[source executor and provider SPI](source-executor-spi.md),
[Batch sinks and output boundaries](batch-sinks.md),
[durable sink provider boundary](durable-sink-provider.md),
[schema conversion and row validation](schema-and-validation.md),
[deployment](../operations/deployment.md).
