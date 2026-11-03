---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Stream session, DataStream, and lifecycle method reference

This page is the method-level contract for the Stream composition and control
surface: how a session is created and configured, which operators `DataStream`
exposes, what a state or timer callback may call, and which handles carry a
checkpoint, savepoint, restart, or rescale. For what an operator *computes* —
changelog rules, watermark semantics, state time-to-live behavior — see
[Stream behavior reference](stream-behavior.md).

`RowWeave` stream factories, the closed operator set, and the stable refusals are
summarised on [public API overview](api-overview.md); the exhaustive declaration
set is on [public API inventory](api-inventory.md).

## Creating a session

`RowWeave.createStreamSession(String)` at
`rowweave-core/src/main/java/io/rowweave/RowWeave.java:19` is the single-argument
form, `createStreamSession(String, String)` at `:23` adds a display title, and
the client-carrying form at `:27` binds a `StreamJobClient` so the session can
submit. `StreamSession` itself is declared at
`rowweave-core/src/main/java/io/rowweave/StreamSession.java:17`, and its two
constructors at `:47` and `:51` are package-private: the factory is the only
public creation path.

| Session setting | Default | Declaration |
|---|---|---|
| job name | required | `StreamSession.java:52` |
| job title | falls back to the job name | `StreamSession.java:56` |
| definition ID | falls back to the job name | `StreamSession.java:58` |
| parallelism | `1` | `StreamSession.java:36` |
| maximum parallelism | `128` | `StreamSession.java:37` |
| delivery guarantee | `AT_LEAST_ONCE` | `StreamSession.java:38` |
| changelog mode | `APPEND` | `StreamSession.java:39` |
| time mode | `PROCESSING_TIME` | `StreamSession.java:40` |
| checkpoint | `manualOnly(30000, 1)` | `StreamSession.java:41` |
| restart | `fixedDelay(1, 1000)` | `StreamSession.java:43` |
| resource profile | `StreamResourceProfile.defaults()` | `StreamSession.java:44` |
| deployment set ID | unset | `StreamSession.java:45` |

A maximum parallelism of `128` is a key-group space ceiling, not a suggestion: it
bounds how far the job can be rescaled later, so raising it after state exists
means an offline redistribution rather than a live change.

## Session methods

| Method | Declaration | Contract |
|---|---|---|
| `fromSource(StreamSourceSpec)` | `StreamSession.java:61` | refuses a `null` spec at `:62`; returns the SOURCE stream |
| `setDefinitionId(String)` | `StreamSession.java:80` | refuses `null` or empty at `:82` |
| `setParallelism(int)` | `StreamSession.java:86` | requested task count |
| `setMaxParallelism(int)` | `StreamSession.java:87` | key-group space ceiling for rescale |
| `setDeliveryGuarantee(StreamDeliveryGuarantee)` | `StreamSession.java:88` | refuses `null` |
| `setChangelogMode(StreamChangelogMode)` | `StreamSession.java:92` | job-level changelog default |
| `setTimeMode(StreamTimeMode)` | `StreamSession.java:96` | `PROCESSING_TIME` or event time |
| `setCheckpointConfig(StreamCheckpointConfig)` | `StreamSession.java:100` | refuses `null` |
| `setRestartPolicy(StreamRestartPolicy)` | `StreamSession.java:104` | refuses `null` |
| `setResourceProfile(StreamResourceProfile)` | `StreamSession.java:108` | refuses `null` |
| `setDeploymentSetId(String)` | `StreamSession.java:114` | must be 64 lowercase hex characters at `:116` |
| `toPlanArtifact()` | `StreamSession.java:123` | compiles through `StreamPlanCompiler` |
| `submit(StreamSubmissionId)` | `StreamSession.java:125` | compile then submit; refuses without a client |

Every setting validator on this surface raises `IllegalArgumentException` with a
message that names the rejected value. `submit` first compiles, then checks that
a client exists, and a session built without one fails at
`StreamSession.java:132` with `StreamApiException.Code.CLIENT_REQUIRED`
(`StreamSession.java:134`) — the refusal happens after the graph is proven
compilable, so a topology defect is reported before a wiring defect.

A cross-host process role is admitted only under the `MTLS` control profile;
see [security model](../operations/security.md). The client factories are
`createLocalStreamJobClient` at `RowWeave.java:180` and `:196`,
`createRealClusterStreamJobClient` at `:34` and `:43`, and
`createRegistryStreamJobClient` at `:53`.

## DataStream operators

`DataStream` is declared at
`rowweave-core/src/main/java/io/rowweave/DataStream.java:23`. Every operator
method returns a new immutable stream, except `sinkTo`, which registers an
output on the owning session and returns `void`.

| Method | Declaration | Notes |
|---|---|---|
| `uid(String)` | `DataStream.java:108` | stable operator identity required for stateful nodes |
| `map(FunctionDeploymentReference, StreamSchema)` | `DataStream.java:118` | declared output schema |
| `map(FunctionDeploymentReference, StreamSchema, StreamEmissionLimit)` | `DataStream.java:124` | with an explicit per-input emission bound |
| `filter(FunctionDeploymentReference)` | `DataStream.java:136` | predicate keeps the input schema |
| `calc(StreamCalcSpec, ...)` | `DataStream.java:144` | expression projection |
| `assignEventTime(StreamEventTimeSpec)` | `DataStream.java:157` | declares the watermark policy |
| `keyedProcess(StreamProcessSpec)` | `DataStream.java:170` | main output only |
| `keyedProcessWithOutputs(...)` | `DataStream.java:184` | returns `StreamProcessResult` with tagged views |
| `keyedCoProcess(DataStream, ...)` | `DataStream.java:195` | two inputs, one key space |
| `keyedCoProcessWithOutputs(DataStream, ...)` | `DataStream.java:214` | tagged co-process result |
| `genericWindow(StreamGenericWindowSpec)` | `DataStream.java:226` | window firing to the main output |
| `genericWindowWithLateOutput(...)` | `DataStream.java:241` | returns `StreamWindowResult` with a late view |
| `asyncWait(StreamAsyncWaitSpec)` | `DataStream.java:252` | ordered or unordered async enrichment |
| `flatMap(FunctionDeploymentReference, StreamSchema)` | `DataStream.java:265` | zero to many rows |
| `flatMap(..., StreamEmissionLimit)` | `DataStream.java:271` | bounded fan-out |
| `union(DataStream)` | `DataStream.java:296` | one schema, two inputs |
| `keyBy(String...)` | `DataStream.java:308` | declares the stable key fields |
| `keyedAggregate(FunctionDeploymentReference, ...)` | `DataStream.java:320` | aggregate over the key space |
| `temporalJoin(DataStream, ...)` | `DataStream.java:333` | versioned-table join |
| `eventTimeDecimalWindow(StreamWindowSpec, ...)` | `DataStream.java:354` | window with decimal aggregation |
| `sinkTo(String, StreamSinkSpec)` | `DataStream.java:367` | registers the graph output |
| `schema()` | `DataStream.java:371` | the `StreamSchema` of this stream |
| `getSchema()` | `DataStream.java:372` | the same schema as a `DataSchema` |

Functions are addressed through `FunctionDeploymentReference` rather than a
lambda, because a Stream function travels as a signed deployment artifact and is
resolved on the Worker. That is why `map`, `filter`, `flatMap`, and
`keyedAggregate` take a reference plus an explicit schema: the deployed code's
signature and the declared schema are both checked at compile time, and a
mismatch is `FUNCTION_ABI_MISMATCH`.

## State and time context

A keyed process callback runs against
[`StreamProcessContext`](../../../rowweave-core/src/main/java/io/rowweave/StreamProcessContext.java),
declared at `rowweave-core/src/main/java/io/rowweave/StreamProcessContext.java:4`.

| Context member | Declaration | Contract |
|---|---|---|
| `currentKey()` | `StreamProcessContext.java:7` | defensive row holding only the declared key fields |
| `hasRecordTimestamp()` | `StreamProcessContext.java:9` | whether an event timestamp exists for this input |
| `recordTimestamp()` | `StreamProcessContext.java:12` | meaningful only when `hasRecordTimestamp()` is true |
| `currentWatermark()` | `StreamProcessContext.java:14` | watermark observed by this mailbox |
| `currentProcessingTime()` | `StreamProcessContext.java:17` | Worker-controlled monotonic clock |
| `timerService()` | `StreamProcessContext.java:19` | declared timer families only |
| `valueState` / `mapState` / `listState` / `aggregatingState` | `:21` / `:23` / `:25` / `:27` | state by declared name |

A timer callback receives `StreamTimerContext`
(`rowweave-core/src/main/java/io/rowweave/StreamTimerContext.java:6`), which
extends the process context and adds `timerFamilyId()` at `:8`, the defensive
canonical `timerNamespace()` bytes at `:11`, `timeDomain()` at `:13`, and
`timerTimestamp()` at `:15`. A window callback receives `StreamWindowContext`
(`rowweave-core/src/main/java/io/rowweave/StreamWindowContext.java:4`) with
`windowStart()` inclusive at `:10` and `windowEnd()` exclusive at `:13`; for a
count window those two values are ordinals rather than timestamps.

Emission goes through `StreamCollector`
(`rowweave-core/src/main/java/io/rowweave/StreamCollector.java:6`): `collect`
to the main output at `:8` and `collect(StreamOutputTag, DataRow)` at `:10` for a
declared tag. Collecting to a tag that was not declared is not possible rather
than merely refused, because the tag type only exists as a declaration.

## State handles

All keyed state is scoped to the current key and stores canonical `byte[]`
payloads; the codec is a contract, not an implementation detail.

| Handle | Declaration | Methods |
|---|---|---|
| `StreamValueState` | `rowweave-core/src/main/java/io/rowweave/StreamValueState.java:4` | `value()` `:6`, `update()` `:8`, `clear()` `:10` |
| `StreamListState` | `rowweave-core/src/main/java/io/rowweave/StreamListState.java:4` | `add()` `:6`, `values()` `:8`, `clear()` `:10` |
| `StreamMapState` | `rowweave-core/src/main/java/io/rowweave/StreamMapState.java:4` | `get()` `:6`, `contains()` `:8`, `put()` `:10`, `remove()` `:12`, `entries()` `:14`, `clear()` `:16` |
| `StreamAggregatingState` | `rowweave-core/src/main/java/io/rowweave/StreamAggregatingState.java:4` | `value()` `:6`, `update()` `:8`, `clear()` `:10` |
| `StreamReadOnlyBroadcastState` | `rowweave-core/src/main/java/io/rowweave/StreamReadOnlyBroadcastState.java:4` | `get()` `:6`, `contains()` `:8`, `entries()` `:10` |
| `StreamBroadcastState` | `rowweave-core/src/main/java/io/rowweave/StreamBroadcastState.java:4` | adds `put()` `:6`, `remove()` `:8`, `clear()` `:10` |
| `StreamStateCursor` | `rowweave-core/src/main/java/io/rowweave/StreamStateCursor.java:6` | `advance()` `:8`, `hasKey()` `:11`, `key()` `:13`, `value()` `:15`, `close()` `:18` |
| `StreamTimerService` | `rowweave-core/src/main/java/io/rowweave/StreamTimerService.java:4` | `registerTimer()` `:6`, `deleteTimer()` `:9` |

A cursor is a one-pass view, not a snapshot: it is `AutoCloseable`, `hasKey()`
returns `false` for list entries because a list carries no map key, and
retaining a cursor past the callback is undefined. Broadcast state is writable
only inside the declared broadcast-side callback, which is why the read-only and
mutable interfaces are separate types rather than one type with a flag. Timer
registration and deletion are idempotent, so a retry of a failed callback cannot
duplicate a timer.

## Declared limits

A Stream graph is admitted only when every bounded quantity is declared and
inside a published ceiling. The ceilings are constants, and a declaration that
exceeds one is rejected at build time.

| Declaration | Ceiling constants | Factory |
|---|---|---|
| `StreamTimerSpec` | `MAX_TIMERS_PER_KEY` = 1000000 (`:7`), `MAX_TOTAL_TIMERS` = 1000000000 (`:8`), `MAX_TOTAL_BYTES` = 2^38 (`:9`) | `processingTime` `:38`, `eventTime` `:45` |
| `StreamEmissionLimit` | `MAX_OUTPUTS_PER_INPUT` = 1000000 (`:7`), `MAX_OUTPUT_BYTES_PER_INPUT` = 1 GiB (`:8`) | `of` `:26` |
| `StreamManagedStateSpec` | `MAX_ENTRIES_PER_KEY` (`:7`), `MAX_TOTAL_ENTRIES` (`:8`), `MAX_TOTAL_BYTES` = 2^40 (`:9`), `MAX_ENTRY_BYTES` = 16 MiB (`:10`) | `bounded` `:42` |
| `StreamBroadcastStateSpec` | `MAX_ENTRIES` (`:8`), `MAX_TOTAL_BYTES` = 2^40 (`:9`), `MAX_UPDATE_BYTES` = 16 MiB (`:10`), `MAX_RETAINED_VERSIONS` = 64 (`:11`) | `immutableSnapshot` `:58`, `replayableChangelog` `:70` |
| `StreamProcessSpec` | `MAX_STATES` = 64 (`:14`), `MAX_TIMER_FAMILIES` = 64 (`:15`), `MAX_OUTPUT_TAGS` = 32 (`:16`) | `keyed` `:55`, `withOutputTags` `:68` |

`StreamTimerSpec.TimeDomain` is the closed pair `PROCESSING_TIME` and
`EVENT_TIME` (`rowweave-core/src/main/java/io/rowweave/stream/api/StreamTimerSpec.java:11`),
and bound rejection happens in the constructor at
`StreamTimerSpec.java:24`. `StreamStateSpec` carries `Kind` and `TtlMode` as
closed enums at `rowweave-core/src/main/java/io/rowweave/stream/api/StreamStateSpec.java:9`
and `:12`, and the decimal window serializer contract is the frozen identifier
at `StreamStateSpec.java:7`. `StreamBroadcastStateSpec.AuthorityKind` is
`IMMUTABLE_SNAPSHOT` or `REPLAYABLE_CHANGELOG`
(`rowweave-core/src/main/java/io/rowweave/stream/api/StreamBroadcastStateSpec.java:13`),
and an authority digest must be 64 lowercase hex characters at
`StreamBroadcastStateSpec.java:86`.

`StreamOutputTag`
(`rowweave-core/src/main/java/io/rowweave/stream/api/StreamOutputTag.java:11`)
requires a complete declaration at `:30`, refuses a non-`APPEND` changelog
without stable keys at `:36`, and requires unique non-null key fields at `:71`.
`StreamProcessResult.main()` at
`rowweave-core/src/main/java/io/rowweave/StreamProcessResult.java:24` returns
the main stream and `output(String)` at `:26` returns a tagged view, refusing an
undeclared tag identifier at `:29`. `StreamWindowResult.main()` and `late()` are
at `rowweave-core/src/main/java/io/rowweave/StreamWindowResult.java:18` and
`:19`.

Row frames crossing a Stream channel are bounded: `StreamDataRowCodec` declares
`MAX_FRAME_BYTES` = 4 MiB at
`rowweave-core/src/main/java/io/rowweave/stream/api/StreamDataRowCodec.java:8`,
with the encode check at `:19` and the decode check at `:28`.
`StreamResourceProfile.defaults()` at
`rowweave-core/src/main/java/io/rowweave/stream/api/StreamResourceProfile.java:41`
is the profile the session starts with, and the constructor rejects a channel
credit smaller than one maximum frame at `:23`.

## Checkpoint, savepoint, restart, and rescale

| Type or method | Declaration | Contract |
|---|---|---|
| `StreamCheckpointConfig.manualOnly` | `rowweave-core/src/main/java/io/rowweave/stream/api/StreamCheckpointConfig.java:25` | timeout and retained count |
| `StreamCheckpointConfig.periodic` | `StreamCheckpointConfig.java:31` | requires a positive interval at `:34` |
| `StreamCheckpointConfig.manualOnlyUnaligned` | `StreamCheckpointConfig.java:41` | unaligned mode, `StreamCheckpointMode.UNALIGNED` |
| `StreamCheckpointConfig.periodicUnaligned` | `StreamCheckpointConfig.java:47` | requires a positive interval at `:50` |
| `StreamCheckpointConfig.periodic()` | `StreamCheckpointConfig.java:58` | interval greater than zero |
| `StreamRestartPolicy.fixedDelay` | `rowweave-core/src/main/java/io/rowweave/stream/api/StreamRestartPolicy.java:19` | attempts and backoff bounded at `:11` |
| `StreamJobHandle.cancel` | `rowweave-core/src/main/java/io/rowweave/StreamJobHandle.java:19` | command-identified cancellation |
| `StreamJobHandle.triggerCheckpoint` | `StreamJobHandle.java:20` | one manual checkpoint |
| `StreamJobHandle.stopWithSavepoint` | `StreamJobHandle.java:21` | drain, durable point, terminal status |
| `StreamJobHandle.rescale` | `StreamJobHandle.java:23` | default throws `live rescale control is unavailable` at `:25` |
| `StreamJobHandle.evidence` | `StreamJobHandle.java:15` | default throws `Stream operational evidence is unavailable` at `:17` |
| `StreamJobHandle.awaitRunning` / `awaitTermination` | `StreamJobHandle.java:28` / `:29` | bounded wait returning a status |

The checkpoint constructor rejects a null mode, a negative interval, a
non-positive timeout, and a retained count outside 1 to 128 at
`StreamCheckpointConfig.java:16`. `fixedDelay` accepts 0 to 1000 attempts and a
backoff up to 86400000 ms at `StreamRestartPolicy.java:11`; a policy with zero
attempts is a valid configuration meaning "fail rather than restart".

`StreamJobClient`
(`rowweave-core/src/main/java/io/rowweave/StreamJobClient.java:8`) is the only
submission and re-attachment boundary:

| Method | Declaration | Contract |
|---|---|---|
| `submit` | `StreamJobClient.java:9` | submission ID plus compiled artifact |
| `attachSubmission` / `attach` | `:11` / `:12` | reconnect by submission or job identity |
| `attachCancel` / `attachCheckpoint` / `attachSavepoint` | `:13` / `:15` / `:17` | re-attach an in-flight command |
| `attachRescale` | `StreamJobClient.java:19` | default throws `live rescale control is unavailable` at `:21` |
| `restore` | `StreamJobClient.java:24` | new job from a recovery point |
| `offlineRescale` | `StreamJobClient.java:27` | key-group redistribution at a new parallelism |
| `close` | `StreamJobClient.java:32` | default no-op |

Reattachability is the reason commands carry an explicit
`StreamCommandId` (`of` at
`rowweave-core/src/main/java/io/rowweave/stream/api/StreamCommandId.java:12`,
`random` at `:13`): a client that dies mid-command can re-attach the same command
identifier rather than issue a second one. The operation handles share
`id()`, `status()`, and `awaitCompletion()` at
`rowweave-core/src/main/java/io/rowweave/StreamOperationHandle.java:8`, `:9`,
and `:10`; a checkpoint handle adds `checkpointId()`
(`rowweave-core/src/main/java/io/rowweave/StreamCheckpointHandle.java:5`), a
savepoint handle adds `savepointId()`
(`rowweave-core/src/main/java/io/rowweave/StreamSavepointHandle.java:5`), and a
rescale handle adds `rescaleId()` and `targetParallelism()` at
`rowweave-core/src/main/java/io/rowweave/StreamRescaleHandle.java:9` and `:10`.

`StreamSavepointRequest` takes a provider key plus a configuration frame at
`rowweave-core/src/main/java/io/rowweave/StreamSavepointRequest.java:8` and
clones the frame on read at `:17`, so a caller cannot mutate what was already
submitted. `StreamRestoreRequest` validates at
`rowweave-core/src/main/java/io/rowweave/StreamRestoreRequest.java:31` with
`invalid Stream recovery request`, and `StreamOfflineRescaleRequest` delegates to
that same validation at
`rowweave-core/src/main/java/io/rowweave/StreamOfflineRescaleRequest.java:16`,
so the two recovery paths can never drift apart.

## Status and evidence

`StreamJobStatus`
(`rowweave-core/src/main/java/io/rowweave/stream/api/StreamJobStatus.java:4`)
carries a frozen wire identifier and a terminal flag:

| Status | Wire ID | Terminal |
|---|---|---|
| `SUBMITTED` | 1 | no |
| `STARTING` | 2 | no |
| `RUNNING` | 3 | no |
| `RESTARTING` | 4 | no |
| `STOPPING` | 5 | no |
| `CANCELED` | 6 | yes |
| `FAILED` | 7 | yes |
| `STOPPED_WITH_SAVEPOINT` | 8 | yes |
| `FINISHED` | 9 | yes |

A terminal status is an outcome, not a client timeout, so an await that returns a
terminal value is a completed observation rather than a deadline expiry.
`StreamOperationStatus` is the accepted, running, succeeded, or failed projection
for one command at
`rowweave-core/src/main/java/io/rowweave/stream/api/StreamOperationStatus.java:5`.
A recovery point is named by `StreamRecoveryPointId.Kind`, the closed pair
`CHECKPOINT` and `SAVEPOINT`, at
`rowweave-core/src/main/java/io/rowweave/stream/api/StreamRecoveryPointId.java:7`,
with the factories `checkpoint` at `:17` and `savepoint` at `:20`.

`StreamJobEvidence`
(`rowweave-core/src/main/java/io/rowweave/stream/api/StreamJobEvidence.java:24`)
is the durable, data-free operational view: `unavailable(...)` at `:117` is the
honest empty value, and the getters between `:124` and `:149` expose run and
attempt identity, fence token, coordinator term, assignment generation, the
latest completed checkpoint identity and duration, attempt and lifetime progress,
placement and reported task counts, and the observation time. It never carries
rows.

Sink visibility is the closed pair `CHECKPOINT_TRANSACTIONAL` and
`EAGER_AT_LEAST_ONCE` at
`rowweave-core/src/main/java/io/rowweave/stream/api/StreamSinkSpec.java:11`,
declared on the spec alongside the provider key, contract digest, and accepted
changelog modes at `:13` through `:17`.

## Stable API failures

`StreamApiException` extends `IllegalArgumentException` at
`rowweave-core/src/main/java/io/rowweave/stream/api/StreamApiException.java:4`
so an existing caller's catch still works, and carries a `Code` from the closed
enum at `:7` through `:27`. The code is required at `:34` and read through
`code()` at `:38`. Branch on `code()`, never on message text. The catalog of
every code with its route and recovery action is on
[errors and refusals](errors-and-refusals.md).

## Tested by

- [`StreamAtomicCompositionExample`](../../../rowweave-examples/src/main/java/io/rowweave/examples/StreamAtomicCompositionExample.java)
  compiles one graph from every published node kind, exercising the composition surface end to end.
- [`StreamPublicApiSignatureBaselineTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/StreamPublicApiSignatureBaselineTest.java)
  and [`StreamPublicPlanCompilerContractTest`](../../../rowweave-core/src/test/java/io/rowweave/StreamPublicPlanCompilerContractTest.java):
  published signatures and compile-time refusal set.
- [`StreamPublicJobClientContractTest`](../../../rowweave-core/src/test/java/io/rowweave/StreamPublicJobClientContractTest.java):
  submission, re-attach, and unsupported-control refusals.
- [`StreamPublicKeyedProcessCompilerContractTest`](../../../rowweave-core/src/test/java/io/rowweave/StreamPublicKeyedProcessCompilerContractTest.java)
  and [`StreamPublicTaggedOutputCompilerContractTest`](../../../rowweave-core/src/test/java/io/rowweave/StreamPublicTaggedOutputCompilerContractTest.java):
  process specification and tagged output admission.
- [`StreamKeyedProcessOperatorTaskTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/cluster/StreamKeyedProcessOperatorTaskTest.java)
  and [`ManagedKeyedStateCheckpointTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/runtime/ManagedKeyedStateCheckpointTest.java):
  context, state, and timer behavior across a checkpoint.
- [`StreamGenericWindowSdkContractTest`](../../../rowweave-core/src/test/java/io/rowweave/StreamGenericWindowSdkContractTest.java)
  and [`StreamPublicBroadcastProcessCompilerContractTest`](../../../rowweave-core/src/test/java/io/rowweave/StreamPublicBroadcastProcessCompilerContractTest.java):
  window result and broadcast authority admission.
- [`StreamCheckpointCoordinatorTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/runtime/StreamCheckpointCoordinatorTest.java),
  [`AlignedCheckpointGateTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/runtime/AlignedCheckpointGateTest.java),
  and [`UnalignedCheckpointGateTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/runtime/UnalignedCheckpointGateTest.java):
  checkpoint configuration and mode gates.
- [`StreamSavepointRestoreValidatorTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/runtime/StreamSavepointRestoreValidatorTest.java)
  and [`InProcessStreamOfflineRescaleTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/cluster/InProcessStreamOfflineRescaleTest.java):
  recovery request validation and offline redistribution.
- [`StreamLiveRescaleControlTest`](../../../rowweave-core/src/test/java/io/rowweave/stream/cluster/StreamLiveRescaleControlTest.java)
  and [`StreamElasticExampleTest`](../../../rowweave-examples/src/test/java/io/rowweave/examples/StreamElasticExampleTest.java):
  live rescale control end to end.

Related: [public API overview](api-overview.md),
[Stream behavior reference](stream-behavior.md),
[custom function interfaces](custom-functions.md),
[run modes](../guides/run-modes.md),
[deployment](../operations/deployment.md),
[errors and refusals](errors-and-refusals.md).
