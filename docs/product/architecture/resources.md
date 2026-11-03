---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Resource and backpressure model

RowWeave treats resource ceilings as admission contracts. A plan that cannot be
represented inside the declared process, Worker, job, node, channel, and disk
budgets fails before it can create an unbounded side effect. This page explains
the resource invariants. The [configuration reference](../reference/configuration.md)
is the normative property list, [data-plane limits](../reference/data-plane-limits.md)
certifies socket behavior, and the [monitor contract](../reference/monitor-contract.md)
defines observable diagnostics.

## Resource layers and admission order

Resources are admitted from the outermost finite boundary inward:

1. The process envelope freezes heap, direct memory, metaspace, code cache,
   stack, processors, and declared process memory before a role starts.
2. A Worker freezes managed-memory capacity, concurrent stage slots, assignment
   thread ceiling, spill quotas, and connection budgets before accepting work.
3. The Master admits a whole job only when its computed placement fits the
   registered Worker ledger.
4. A Stream graph derives node-local leases from an explicit job resource
   profile and places every physical task together.
5. An operator working set reserves managed memory during execution and spills
   to its quota-backed workspace instead of growing without bound.

No layer dynamically raises another layer's limit. A failed admission or a
budget exhaustion is a typed failure; the runtime does not silently downgrade
correctness, disable authentication, or move work to an uncertified path.

## Worker capacity and Batch placement

A Worker slot admits one stage instance and is also the denominator used to
derive default operator budgets. The default Worker managed pool is half of the
JVM maximum heap, with 128 slots and a certified ceiling of 16 internal threads
per assignment. `ClusterMain` validates the aggregate thread arithmetic for
Master, Worker, registry sessions, transport lanes, and deployment probes before
it creates a role resource.

The Master slot ledger computes Worker-local demand from the physical plan. It
acquires all required slots or waits until the configured grace; when downgrade
is enabled and no full-size placement fits, it chooses the highest feasible
parallelism. It never admits part of a Batch DAG and lets the remaining stages
wait outside the ledger.

Source authorities:
[`WorkerConfig`](../../../rowweave-core/src/main/java/io/rowweave/mem/WorkerConfig.java),
[`SlotLedger`](../../../rowweave-core/src/main/java/io/rowweave/cluster/SlotLedger.java),
[`ProcessThreadCapacityModel`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ProcessThreadCapacityModel.java),
and
[`StageThreadCostModel`](../../../rowweave-core/src/main/java/io/rowweave/cluster/StageThreadCostModel.java).
Tested by `SlotLedgerTest#demandExceedsCapacityFailsFast`,
`SlotLedgerTest#concurrentAcquireNoOversell`,
`ProcessThreadCapacityModelTest#masterWorkerAndCombinedFacilitiesAreAdditive`,
`WorkerStageThreadAdmissionTest#rejectsBeforeStageTaskPortOrProgressSideEffects`,
and `ClusterConfigCapacityTest`.

## Batch managed memory and spill

`GlobalMemoryPool` owns the Worker managed-memory total. A job receives bounded
owners from `ExecutionMemory`; owners are pre-registered, activated on demand,
and borrow the shared pool through one ledger. Every reservation, peak, spill,
and close is accounted to an owner. An unknown or unregistered owner cannot use
the pool as a fallback.

Sort, aggregate, distinct, TopN, join, and materialized-fork working sets have
memory budgets. The default per-breaker upper bound is derived from the Worker
pool and slots and is clamped to 2–16 MiB. When a working set reaches its budget,
it writes bounded runs or blocks to a job workspace and merges them later. The
spill manager charges frame prefixes and payload bytes, enforces both Worker and
job quotas, checks usable space for every reservation, and marks ownership with
durable leases. A real process crash leaves a trusted orphan that the next
process reclaims; a write failure keeps the charge until the file is actually
removed. A quota or disk failure fails the affected run before output commit.

Source authorities:
[`GlobalMemoryPool`](../../../rowweave-core/src/main/java/io/rowweave/mem/GlobalMemoryPool.java),
[`ExecutionMemory`](../../../rowweave-core/src/main/java/io/rowweave/mem/ExecutionMemory.java),
[`ManagedMemory`](../../../rowweave-core/src/main/java/io/rowweave/mem/ManagedMemory.java),
and
[`SpillWorkspaceManager`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillWorkspaceManager.java).
Tested by `SpillWiringTest#groupBySpillMatchesHeap`,
`SpillWiringTest#orderBySpillOrdered`,
`SpillWiringTest#sharedBudgetMultiBreakerPeakBounded`,
`SpillWiringTest#concurrentMultiBreakerSpillDeterministic`,
`ExecutionMemoryTest#newOperatorMemoryAcquiresUpToMaxFromPool`,
`ExecutionMemoryTest#tinyPoolRejectsUnaccountedFallbackOwners`,
`SpillWorkspaceQuotaTest#jobQuotaRejectsBeforeExtendingRunByOneMoreFrame`,
`SpillWorkspaceQuotaTest#concurrentJobsLinearizeAtWorkerLimit`,
and `SpillWorkspaceProcessCrashTest#realRuntimeHaltLeavesTrustedOrphanThatNextProcessReclaims`.

## Channels, backpressure, and failure propagation

Adjacent Batch stages use bounded local channels with a capacity of 256 records.
When a channel is full, `send` parks the producer until the consumer releases a
record; pressure therefore propagates upstream instead of accumulating rows.
The channel counts parks and successful polls as bounded progress evidence. A
failure drains queued records under one lock, inserts the first error, wakes
producers and consumers, and prevents a later record from overtaking the error.

A shared physical shuffle connection multiplexes logical streams by target and
returns credit outside the queue lock when the consumer drains data. Connections,
ingress handlers, handshake attempts, resident bytes, pending frames, targets per
connection, and idle/read deadlines are bounded at startup. Physical connection
loss poisons every logical stream that shares it. Connection reuse is observable
through aggregate lifetime records, but those records are diagnostics; they are
not a throughput or latency guarantee.

Source authorities:
[`LocalChannel`](../../../rowweave-core/src/main/java/io/rowweave/sched/LocalChannel.java),
[`MultiProducerChannel`](../../../rowweave-core/src/main/java/io/rowweave/sched/MultiProducerChannel.java),
and
[`MuxConnectionPool`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/MuxConnectionPool.java).
Tested by `LocalChannelTest#parkCountIncrementsWhenFull`,
`LocalChannelTest#pollReturnsNullAtEndOfStreamAndStaysNull`,
`MuxConnectionPoolFanoutTest`, `MuxConnectionPoolLifecycleTest`, and
`ClusterPushTest#failFastNoDeadlock`.

## Control and data-plane admission

Control RPC reserves resident bytes for encoded and materialized payloads before
a handler runs, releases them when the scoped consumer returns, and returns a
stable refusal when the process-wide control budget is exhausted. Frame length,
in-flight bytes, resident bytes, and connection count are checked before handler
execution. A client reserves submit payload before writing request bytes.

The shuffle data plane derives limits from the same process envelope rather than
from a second security authority. Handshake admission bounds concurrent TLS
setup; ingress admission bounds connections, handlers, and resident bytes; and
multiplexed targets are charged to the receiving Worker. The separate
[data-plane limits](../reference/data-plane-limits.md) page records which
cross-host network conditions are certified and which remain open.

Source authorities:
[`ControlResidentBudget`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlResidentBudget.java),
[`RpcFrameBudget`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcFrameBudget.java),
and
[`ShuffleIngressResidentBudget`](../../../rowweave-core/src/main/java/io/rowweave/sched/net/ShuffleIngressResidentBudget.java).
Tested by `RpcResidentAdmissionTest#decodedFieldReservationLivesThroughHandlerAndReleasesAfterReturn`,
`RpcResidentAdmissionTest#residentExhaustionReturnsStableFailureBeforeHandlerAndLeavesNoCharge`,
`RpcResidentAdmissionTest#clientReservesExternalSubmitPayloadBeforeWritingAnyRequestBytes`,
and `DataPlaneHandshakeAdmissionTest`.

## Stream resource profile and placement

A Stream job carries an explicit `StreamResourceProfile`: mailbox events,
maximum frame bytes, channel credit bytes, managed state bytes, timer count,
window count, checkpoint bytes, threads, file descriptors, and spill disk. These
are ceilings, not allocations. Compilation derives a node-local lease from the
job profile and the node's actual state, timer, window, async, edge, and
checkpoint contracts. The lease must cover every incident edge frame and every
declared timer or window item.

Placement is all-or-nothing. `StreamTaskResourceRequest` describes every task
with resident bytes, threads, channels, frame bound, protocol versions, and
required capabilities. The Worker admits physical slots, listener and accepted
sockets, node-local channels, coordinator metadata, file descriptors, and
checkpoint payload together before task expansion. The task context consumes
the same lease authority used by placement, so a task cannot open with resources
that placement did not reserve. Checkpoint transfer has separate whole-graph
receipt and per-task member budgets.

Source authorities:
[`StreamResourceProfile`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamResourceProfile.java),
[`StreamTaskResourceRequest`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamTaskResourceRequest.java),
[`StreamNodeTaskContext`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamNodeTaskContext.java),
and
[`StreamCheckpointTransferBudget`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamCheckpointTransferBudget.java).
Tested by `StreamTopologyNeutralFoundationTest#placementExpansionUsesEveryNodeSubtaskAndNodeLocalResources`,
`StreamTopologyNeutralFoundationTest#placementRejectsAJobWhosePhysicalSocketsExceedItsFdCeiling`,
`StreamTopologyNeutralFoundationTest#placementPreflightRejectsInsufficientWorkerSlotsBeforeTaskExpansion`,
`StreamTopologyNeutralFoundationTest#jobCheckpointCeilingMustCoverEveryNodePayloadLease`,
`StreamTopologyNeutralFoundationTest#nodeTaskRuntimeConsumesTheSameResourceAuthorityAsPlacement`,
`PortableStreamPlanContractTest#nodeResidentLeaseMustCoverEveryIncidentEdgeFrame`,
and `StreamCheckpointTransferBudgetTest#wholeGraphReceiptsAndPerTaskMembersHaveIndependentWireBudgets`.

## Stream task bounds and channel evidence

A Stream task observes mailbox, managed-state, timer, and window high-water
marks from the same lease context. A timer or window declaration is checked
against the job resource profile before a worker opens the task. Channel
metrics expose current events and bytes, high-water marks, and blocked time for
physical output lanes; aggregation is saturating and explicitly unavailable when
any lane cannot report.

These observations are diagnostics. They do not replace admission, and a metric
producer failure cannot block, retry, or complete execution. Watermarks,
checkpoints, and backpressure remain authoritative only in the Stream run state
and durable evidence described by [recovery](recovery.md).

Source authorities:
[`StreamTaskResourceUsage`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamTaskResourceUsage.java),
[`StreamChannelMetrics`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamChannelMetrics.java),
and
[`StreamMonitorBridge`](../../../rowweave-core/src/main/java/io/rowweave/monitor/StreamMonitorBridge.java).
Tested by `StreamKeyedProcessOperatorTaskTest#timerDeclarationMustFitJobResourceBeforeWorkerOpen`,
`StreamMonitorBridgeTest#progressUpdatesTypedViewAndMetricSet`,
`StreamMonitorBridgeTest#registryFailuresNeverEscapeExecutionPath`, and
`InMemoryJobMonitorTest#boundedHistoryEvictsOldest`.

## Cancellation, shutdown, and cleanup

Batch cancellation gives a stage a bounded drain window and joins task threads.
An uncooperative running stage receives a bounded cancellation failure, and a
negative drain window is refused before a Worker opens. Master cancellation
reclaims Worker slots after exact-stage acknowledgement or quarantine. Worker
closure releases assignment slots, local channels, shuffle registrations,
resident leases, and spill workspace references in a bounded order.

Spill cleanup is ownership-aware rather than time-only. Live child leases are
not crossed while the owning process is alive; after a confirmed death, the next
process reclaims trusted orphans under the workspace lease and marker evidence.
Retention scans and deletes are bounded so cleanup cannot become an unbounded
stop-the-world operation.

Source authorities:
[`PipelineExecutor`](../../../rowweave-core/src/main/java/io/rowweave/sched/PipelineExecutor.java),
[`WorkerServer`](../../../rowweave-core/src/main/java/io/rowweave/cluster/WorkerServer.java),
and
[`SpillWorkspaceManager`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillWorkspaceManager.java).
Tested by `BoundedStageCancellationTest#uncooperativeRunningStageReturnsBoundedCancellationFailure`,
`BoundedStageCancellationTest#rejectsNegativeCancelDrainBeforeOpeningWorker`,
and `SpillWorkspaceProcessCrashTest#liveChildLeaseCannotBeCrossedButBecomesReclaimableAfterDeath`.

## Public boundary and tuning model

Process and Worker envelopes are startup configuration and cannot be mutated
while roles are live. Batch users choose parallelism, worker capacity properties,
and sink/output contracts; there is no public per-session Batch memory setter.
Stream users choose the finite job resource profile and required capabilities.
All other budgets are derived by compilation and admission.

Do not disable a limit, raise a quota to avoid a typed failure, or treat
monitoring as a control path. If a job exceeds a supported boundary, reduce
concurrency or state, increase the corresponding process envelope only after
capacity review, or change the plan to a supported bounded shape.
