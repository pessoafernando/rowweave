---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Stream behavior reference

This page is the normative contract for Stream `StreamSession`, `DataStream`,
job control, state, time, recovery, and stable refusals. The
[capability matrix](capability-matrix.md) states the support boundary; the
[API inventory](api-inventory.md) traces symbols to source. Operations use the
[Stream runbook](../../runbooks/stream-runtime.md).

Stream has one portable DAG, one task lifecycle, and one control boundary. A
graph is admitted as a whole before side effects; the runtime does not choose
an executor from operator counts or topology shape.

## Contract index

| Lifecycle contract | Reference | Tested demonstration |
|---|---|---|
| Session and job lifecycle | [Session and job lifecycle](#session-and-job-lifecycle) | `StreamLocalAppendExampleTest#publicExampleRunsWithoutInternalStreamImports`; `PortableStreamPlanContractTest#canonicalPlanRoundTripsAndProjectsExactlyIntoArtifact` |
| Closed operator set | [Closed operator set](#closed-operator-set) | `StreamAtomicPlanRegistryContractTest#atomicOpcodesAndFunctionAbisHavePermanentAdditiveWireIds`; `StreamAtomicPlanRegistryContractTest#everyAtomicCapabilityIsProjectedToTheOuterArtifact` |
| Changelog contracts | [Changelog contracts](#changelog-contracts) | `StreamChangeRecordCodecTest#allFourChangeKindsRoundTripCanonically`; `StreamUpsertRecordCodecTest#afterImageAndTombstoneRoundTripCanonically` |
| Event time and watermarks | [Event time and watermarks](#event-time-and-watermarks) | `StreamTimeServiceTest#activeMinimumIdleAndReactivationDriveCanonicalEventTimers`; `FinalEventTimeDecimalWindowTest#checkpointRestoresPendingStateTimersAndWatermark` |
| Keyed state | [Keyed state](#keyed-state) | `ManagedKeyedValueStateTest#canonicalKeyGroupsOwnershipAndMailboxThreadAreExact`; `ManagedKeyedValueStateTest#boundedResidentCacheSpillsAndReplaysWithoutLosingUpdates` |
| Broadcast state | [Broadcast state](#broadcast-state) | `StreamPublicBroadcastProcessCompilerContractTest#immutableSnapshotCompilesToBroadcastEdgeAndStateProfile`; `StreamPublicBroadcastProcessCompilerContractTest#broadcastAuthorityAndResourceBoundsFailClosed` |
| Timers | [Timers](#timers) | `StreamTimeServiceTest#processingTimersAreMonotonicBoundedAndSingleThreadOwned`; `StreamTimeServiceTest#timerQuotaAndCallbackFailureAreFailClosed` |
| Windows | [Windows](#windows) | `FinalEventTimeDecimalWindowTest#tumblingWindowEmitsFinalDecimalSumOnceAndCleansState`; `StreamGenericWindowProductAdmissionTest#eventTimeAggregateIsAStatefulProductNodeWithFunctionCapability` |
| Temporal join | [Temporal join](#temporal-join) | `BoundedEventTimeTemporalJoinTest#matchesInclusiveIntervalInCanonicalArrivalOrderAndNeverMatchesNullKeys`; `BoundedEventTimeTemporalJoinTest#watermarksCleanBothSidesAndLateRecordsFailClosed` |
| Async lookup | [Async lookup](#async-lookup) | `StreamAsyncWaitProductAdmissionTest#orderedLookupIsAnAdmittedUnaryNodeWithDeploymentCapability`; `InProcessStreamAsyncWaitDispatchTest#orderedLookupRunsAndCheckpointsThroughMasterWorkerRoles` |
| Sources and dynamic splits | [Sources and dynamic splits](#sources-and-dynamic-splits) | `InProcessStreamDynamicSplitDispatchTest#discoveredSplitsRunAndCheckpointOnPlacedSourceSubtasks`; `InProcessStreamDynamicSplitDispatchTest#eachDynamicSourceKeepsItsOwnDiscoveryInterval` |
| Checkpoints | [Checkpoints](#checkpoints) | `StreamCheckpointCoordinatorTest#exactJointClosureDecidesAndReplacementSurvivesAckLoss`; `UnalignedCheckpointGateTest#snapshotsAndForwardsAtFirstBarrierThenCompletesChannelCut` |
| Savepoints and recovery | [Savepoints and recovery](#savepoints-and-recovery) | `FileStreamSavepointStoreTest#completedCheckpointBecomesCanonicalReopenableSavepoint`; `StreamSavepointRestoreValidatorTest#exactArtifactAndCanonicalPlanAreAccepted` |
| Restart and rescale | [Restart and rescale](#restart-and-rescale) | `InProcessStreamDynamicSplitDispatchTest#liveRescaleCutsAndReplacesTheRunningGenericDagGeneration`; `OfflineKeyedStateRescalerTest#rescaleRewritesAndRestoresEveryKeyGroupExactly` |
| Deployment modes | [Deployment modes](#deployment-modes) | `StreamLocalEventTimeRecoveryExampleTest#pendingWindowRestoresBeforeItsFinalWatermark`; `StreamElasticExampleTest#publicExampleRunsDynamicUnalignedRescaleAndCompilesRetract` |
| Stable refusals | [Stable refusals](#stable-refusals) | `StreamAtomicPlanRegistryContractTest#atomicOpcodesAndFunctionAbisHavePermanentAdditiveWireIds`; `PortableStreamPlanContractTest#topologyKeyAndParallelismLegalityFailBeforeArtifactAdmission` |

## Session and job lifecycle

`StreamSession` is the composition boundary. It accepts a source, sets
parallelism, delivery guarantee, changelog mode, time mode, checkpoint
configuration, restart policy, resource profile, and deployment set, then
compiles to an immutable `PlanArtifact` or submits that artifact.

`StreamJobClient` is the sole submission and reattachment boundary. Its
methods cover submit, attach by submission or job, cancel, checkpoint,
savepoint, live rescale, restore, and offline rescale. A `StreamJobHandle` is
thread-safe and reconnectable; it exposes identity, artifact, status,
progress, evidence, command completion, and termination waits. Do not infer a
durable decision from a request return alone—wait for the typed operation to
reach `SUCCEEDED`.

Every operator node has an explicit `uid`; that identity binds the portable
plan to recovery state. A plan validates topology, keys, parallelism,
schemas, function identity, resource claims, state serializers, and required
capabilities before provider access or side effects.

## Closed operator set

The atomic registry is closed: `SOURCE`, `MAP`, `FILTER`, `KEY_BY`,
`KEYED_AGGREGATE`, `EVENT_TIME_WINDOW`, `TEMPORAL_JOIN`, `SINK`, `CALC`,
`FLAT_MAP`, `UNION`, `ASSIGN_EVENT_TIME`, `KEYED_PROCESS`,
`KEYED_CO_PROCESS`, `GENERIC_WINDOW`, and `ASYNC_WAIT`. Each opcode has a
permanent additive wire ID and a fixed input count.

The public API exposes product operations over that registry, including
`map`, `filter`, `flatMap`, `calc`, `union`, `assignEventTime`, `keyBy`,
`keyedProcess`, `keyedCoProcess`, `keyedAggregate`, `temporalJoin`,
`genericWindow`, `eventTimeDecimalWindow`, and `asyncWait`. A function is
bound by exact deployment identity and ABI, not by classpath discovery. An
unknown wire ID fails with `UNKNOWN_OPCODE`; a capability mismatch fails
before provider initialization.

## Changelog contracts

`APPEND`, `UPSERT`, and `RETRACT` are session and edge contracts. A canonical
change record has one of four kinds—`INSERT`, `UPDATE_BEFORE`,
`UPDATE_AFTER`, or `DELETE`—plus a stable key and the image required by that
kind. Encodings are checksummed, bounded, and canonical.

`UPSERT` requires a stable key and maps an after image or tombstone to the
canonical change model. `RETRACT` is valid only where every node and edge has
a stable key, a deterministic compensation rule, and a retract-aware sink.
Changing a key contract between plan generations fails before execution.
Mixed inferred input changelog modes to `UNION` are refused.

## Event time and watermarks

`PROCESSING_TIME` and `EVENT_TIME` are explicit session modes.
`ASSIGN_EVENT_TIME` binds a timestamp expression, a monotonic or bounded
out-of-orderness policy, an idle timeout, and `FAIL` or `DROP` handling for an
invalid timestamp. Source providers may advance watermarks through provider
events under an idle timeout.

Watermarks are the event-time authority. A timer cannot reactivate at a lower
watermark, and event-time state is cleaned only by the final watermark or an
explicit TTL. A finalized window or temporal-join state that receives an
invalid late sequence fails the attempt rather than emitting a hidden repair.

## Keyed state

Managed keyed state kinds are `KEYED_VALUE`, `KEYED_MAP`,
`KEYED_AGGREGATING`, `KEYED_LIST`, and `OPERATOR_LIST`. Each declaration has
a unique state name, serializer contract, positive schema version, and either
no TTL or a positive processing-time/event-time TTL. Arbitrary Java objects
are not state.

Key groups own keys deterministically. Only the task owning a key group and
its single mailbox thread may mutate that state. A bounded resident cache
spills and replays updates exactly; corruption fails closed and cannot be
masked by an overwrite. TTL expiry is deterministic and does not revive old
values.

## Broadcast state

A keyed co-process may bind a read-only broadcast state on a broadcast edge.
The authority is either an immutable snapshot or a replayable changelog. A
broadcast declaration freezes its authority identity and digest, serializer
contract, schema version, retained-version count, entry count, total bytes,
and update-byte bounds. Missing authority evidence or an over-limit profile
fails before admission; application code can read broadcast state but cannot
mutate it while processing keyed records.

## Timers

Timer families are named and fixed to `PROCESSING_TIME` or `EVENT_TIME`.
Every family declares per-key, total-count, and total-byte ceilings. The
platform ceilings are 1,000,000 timers per key, 1,000,000,000 total timers,
and 256 GiB of timer bytes; applications may declare tighter limits.

Processing timers are monotonic and owned by the operator mailbox thread.
Event timers fire as the minimum active watermark advances. Quota exhaustion
and callback failure are fail closed: no orphan window or partial state is
created, and the attempt stops with a typed failure.

## Windows

`EVENT_TIME_WINDOW` and `GENERIC_WINDOW` are bounded state operators. The
built-in final decimal window supports tumbling and sliding assignment,
accumulates at full precision, emits once at the final watermark, applies the
single output rounding defined in
[decimal and null semantics](decimal-null-semantics.md#decimal-scale-and-rounding),
and cleans state. Window start and
end fields are epoch milliseconds.

A generic window binds an assigner, trigger, accumulation mode, allowed
lateness, function, output schema and stable key, managed state, and resource
profile; aggregate or process use is selected explicitly. An optional late
output tag routes explicitly allowed late rows. Window fanout, field bindings,
decimals, and timer/state quotas are validated before state is opened. A
finalized window treats a late record as fatal unless the declaration admits
it through an explicit late route.

## Temporal join

`TEMPORAL_JOIN` is a two-input, keyed, finite event-time contract. Both keys
must be declared, compatible, and non-null; null keys never match. The lower
and upper bounds are finite and inclusive, and output stable keys are explicit.

Arrival is canonical per key. Matching retains only bounded state; final
watermarks clean both sides. Retractions compensate exact prior matches and
remove retained state. Late or non-convergent sequences, key-group ownership
errors, and entry or byte quota exhaustion fail the attempt.

## Async lookup

`ASYNC_WAIT` is a unary node with explicit ordering, replay, resource, output
schema, and stable-key declarations. Ordering is bounded rather than
unbounded concurrency. Replay evidence is required for exactly-once, and the
runner, timeout, bootstrap, and socket lane are charged to placement.

Each async node contributes its own checkpoint member and resource claim;
in-flight identities survive savepoint and live-rescale cutover without
reinvoking completed responses. A missing replay capability fails admission
rather than weakening delivery.

## Sources and dynamic splits

Source admission distinguishes exact ordered, fixed split, and dynamic split
contracts. Fixed splits have a bounded end and exact positions. Dynamic
discovery records known, pending, assigned, finished, and removed states with
generation numbers and exact cursor positions. A removed and rediscovered
identifier requires a larger generation.

Discovery intervals are per source. Duplicate discovery and ACK loss are
idempotent. Completed splits release their task for later work; removing an
active split stops its reader before task reuse. Checkpoint freezes the
split table and every position together. Worker or Master replacement
restores the discovery cursor and positions before the next discovery call.

## Checkpoints

Checkpoint modes are `ALIGNED` and `UNALIGNED`. The completed joint decision
and its exact manifest members are the authority; a request acknowledgement
is not. Checkpoint closure tolerates replacement and lost acknowledgements,
while a replacement before decision can abort but cannot commit a partial
closure.

Aligned mode checkpoints operator, source, and sink state at a common
barrier. Unaligned mode additionally captures task-local channel state at the
first barrier and completes the channel cut. Channel state has a positive
durable lease and hard byte limit. Checksum, epoch, sequence, credit, or
ownership mismatch fails closed and is never converted to aligned mode.

Exactly-once requires exact source replay, complete state members, and a
transactional or idempotent sink decision protocol. A sink may be in
`COMMIT_DECIDED`; that state is reconciled forward and is not itself a
restore point. Only `COMPLETED` is a restore authority.

## Savepoints and recovery

Stop-with-savepoint creates a named reference to a completed checkpoint.
Only that completed checkpoint can become a savepoint; publication is durable
and a first-reference-wins name. Corruption or lost acknowledgement cannot
silently publish a weaker copy.

Restore requires the original job ID, exact recovery point ID, exact artifact
and canonical plan, matching state identity, and compatible target maximum
parallelism. A forged digest, schema drift, state drift, or incompatible
parallelism fails before source reopen or sink mutation. Whole-graph recovery
fences the old attempt and generation; it never continues a partial channel
attempt or replays acknowledged output.

## Restart and rescale

`StreamRestartPolicy.fixedDelay` bounds the number of whole-graph attempts
(0 through 1,000) and delay (0 through 24 hours). Worker loss, socket loss,
timer failure, or Master replacement recovers from the latest exact completed
checkpoint under a new fenced attempt; partial-channel repair is not restart.

Live rescale persists intent, takes a rescale checkpoint, derives target key
groups and split ownership, restores target tasks, then durably cuts over.
Old and new generations never publish concurrently. Failure before cutover
returns to the source generation. Offline rescale reads a verified immutable
savepoint, rewrites key-group state in a shadow generation, validates every
count and digest, and atomically binds provenance before scheduling.

## Deployment modes

Local, InProcess, and cross-host Master plus Workers execute the same portable
DAG and task lifecycle. Local places product roles in one JVM without a
listening socket. InProcess composes roles in one JVM for deterministic
qualification. Cross-host mode separates Master-only and Worker-only
processes and uses authenticated control, record, checkpoint, and rescale
lanes.

Mode selection changes placement and transport, not operator semantics, plan
legality, checkpoint authority, changelog contracts, or recovery rules.

## Stable refusals

The following refusals are intentional and occur before provider access or
side effects unless a data-dependent boundary is reached during execution:

* unknown operator opcode (`UNKNOWN_OPCODE`);
* unbounded global sort, join, aggregate, distinct, or arbitrary group
  materialization;
* missing, duplicate, incompatible, or changed stable keys;
* disconnected nodes, invalid topology, input count, parallelism, or lease;
* incompatible input schemas or mixed inferred changelog modes;
* unbounded state, timers, windows, broadcast profiles, or async claims;
* temporal join without finite event-time bounds;
* non-retractable node or edge claiming `RETRACT`;
* arbitrary object state or an un-managed external side effect;
* missing replay or transactional evidence for a claimed exactly-once sink;
* schema, serializer, digest, checkpoint, split-generation, or authority
  mismatch during recovery or rescale.

A typed refusal is a support boundary, not an execution fallback.
