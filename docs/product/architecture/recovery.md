---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Recovery and state authority

Recovery never guesses from timers, request acknowledgements, logs, or an
in-memory cache. Each durable concern has one bounded, authoritative state
store. This page names the authority for Batch, Stream, incremental input, and
persistent-store migration. The
[Stream runbook](../../runbooks/stream-runtime.md) and
[store migration runbook](../../runbooks/store-migration.md) are operating
procedures, not alternative authorities.

## Authority map

| Concern | Authoritative state | Recovery rule |
|---|---|---|
| Batch run and attempt | Immutable `RunSnapshot` plus ordered events in `RunStateStore` | Apply one command at a linearization point; reads return the resulting snapshot and event together. |
| Batch attempt ownership | `RunRecoveryOwnership` recovery token, revision, and owner epoch | Only the current owner may recover; stale revision or epoch cannot publish. |
| Batch retry | `WholeAttemptRetryAuthority` | Retry is a bounded whole-attempt operation with a stable run/artifact/attempt identity. |
| Batch output | Manifest generation authority and exact commit evidence | A committed manifest is reconciled forward; an uncommitted orphan is removed only under the writer lease and exact claim. |
| Stream run | `StreamClusterRunState` in `StreamClusterRunStateStore` | Attempt, coordinator, worker, assignment, channel, and latest completed checkpoint advance as one authority mutation. |
| Stream checkpoint | `StreamCheckpointAuthority` first decision, manifest, then completion | The first durable decision wins; only a completed decision is a restore point. |
| Stream savepoint | Immutable `StreamSavepoint` copy of a completed manifest | Restore validates exact artifact, canonical plan, and state/key-group compatibility. |
| Incremental/CDC frontier | `IncrementalFrontierStore` record and ownership claim | Bootstrap, query, and ownership use the durable record; conflicts fail closed. |
| Persistent store | `UpgradeAuthorityKernel` record and active namespace pointer | The complete old or new pointer/epoch is authoritative; no partial namespace is readable. |

## Batch run, attempt, and fencing

`RunStateStore.apply` is the sole mutation path for Batch run status. A command
produces a new immutable `RunSnapshot` and its event at one observable point.
The snapshot carries run, definition, artifact, bounded attempt count, revision,
status, deadline, and the current attempt. It is the source for replacement-leader
census, retry admission, status queries, and bounded event pages.

Every assignment carries a `RunAttemptFence`: run ID, attempt ID, and a positive
monotonic token. Port reports, progress, terminal reports, cancellation, and
assignment acceptance must present the exact fence. A superseded attempt cannot
complete a tracker, release a barrier, mutate monitoring, or replace the current
attempt's terminal receipt.

Replacement recovery first claims `RunRecoveryOwnership` with the observed
revision and ownership epoch. Only the current token can mutate recovery state.
`InheritedRunRecovery` classifies a durable snapshot as ready to execute, created
but not dispatched, in flight, cancellation reconciliation, or pending. It does
not redispatch work that cannot be proven safe from the snapshot and exact
artifact.

Source authorities:
[`RunStateStore`](../../../rowweave-core/src/main/java/io/rowweave/run/RunStateStore.java),
[`RunSnapshot`](../../../rowweave-core/src/main/java/io/rowweave/run/RunSnapshot.java),
[`RunAttemptFence`](../../../rowweave-core/src/main/java/io/rowweave/run/RunAttemptFence.java),
[`RunRecoveryOwnership`](../../../rowweave-core/src/main/java/io/rowweave/run/RunRecoveryOwnership.java),
[`WholeAttemptRetryAuthority`](../../../rowweave-core/src/main/java/io/rowweave/run/WholeAttemptRetryAuthority.java),
and
[`InheritedRunRecovery`](../../../rowweave-core/src/main/java/io/rowweave/cluster/InheritedRunRecovery.java).
Tested by `RunStateMatrixTest#everyDeclaredAttemptEdgeIsAccepted`,
`RunStateMatrixTest#genericRetryMutationIsRejectedWithoutConsumingRevision`,
`WholeAttemptRetryModelTest#retryPolicyIsFixedBoundedAndOverflowSafe`,
`WholeAttemptRetryModelTest#nextAttemptIdentityIsStableAndSeparatesRunArtifactAndNumber`,
`RunRecoveryOwnershipContract#competingClaimsFromTheSameObservedEpochPublishOnlyOneOwner`,
`RunRecoveryOwnershipContract#staleRevisionCannotPublishOrReplaceOwnership`,
`MasterRunAttemptFenceTest#wholeGraphAssignmentCarriesFenceAndWrongFenceCannotCompleteTracker`,
and `WorkerReportRunAttemptFenceTest#portProgressAndCancelledTerminalReportsKeepAssignmentFence`.

## Batch output, manifest, and whole-attempt retry

Retry is a whole attempt, not a partial replay of one stage. The retry authority
fixes the maximum, derives a stable next-attempt identity, and refuses generic
mutations from authorizing or prematurely creating work. Cancellation and
deadline may terminate a ready retry without erasing evidence from the prior
attempt.

Output recovery follows the manifest authority. A writer lease and durable
generation claim admit only one writer for a domain. Recovery of a confirmed
dead process still requires the same lease, exact attempt claim, and exact
generation. If the commit manifest exists, it is preserved and reconciled
forward; if it does not, bounded cleanup removes staging and orphan data. A
replacement leader converges provider, discard, cancellation, and manifest
decisions before declaring a terminal state.

Source authorities:
[`ManifestCommitAuthority`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitAuthority.java),
[`ManifestFileRecovery`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestFileRecovery.java),
and
[`MasterServer`](../../../rowweave-core/src/main/java/io/rowweave/cluster/MasterServer.java).
Tested by `WholeAttemptRetryModelTest#cancelAndDeadlineCanTerminateReadyRetryWithoutDroppingPreviousAttempt`,
`ManifestProcessCrashRecoveryTest#realProcessCrashMatrixAndRestartRecovery`,
`MasterInFlightRunRecoveryTest#replacementLeaderPersistsManifestAbortBeforeCanceledTerminal`,
`MasterInFlightRunRecoveryTest#replacementLeaderFinishesAfterManifestPublishAckLoss`,
and `MasterInFlightRunRecoveryTest#livePreparedManifestIsInstalledSealedJournaledThenPublished`.

## Stream run and checkpoint authority

`StreamClusterRunState` binds run, attempt, coordinator, worker incarnation,
assignment generation, channel epoch, placement, and latest completed
checkpoint. Replacing a coordinator advances its term and incarnation. A
whole-graph restart after Worker loss advances attempt fence, assignment
generation, and channel epoch together; it never keeps a lost Worker incarnation
or permits two writable generations.

Checkpoint publication is first-decision-wins. The coordinator acknowledges a
bounded joint closure, durably publishes the manifest for a commit, and then
durably publishes `COMMIT_DECIDED` or `ABORTED`. After that, acknowledgement
cannot change the decision. Completion is a separate immutable marker written
only after source and transactional-sink reconciliation succeeds. Therefore the
operational order is manifest, first decision, completion; only `COMPLETED` is a
restore point, while `COMMIT_DECIDED` is recovered forward.

Periodic cuts, savepoints, cancellation, and replacement operations share the
durable operation authority. A replacement master resumes the durable run from
its authority rather than requiring the old client connection. Status reads
remain bounded and do not become part of the checkpoint decision.

Source authorities:
[`FileStreamClusterRunAuthority`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/FileStreamClusterRunAuthority.java),
[`StreamClusterRunStateStore`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterRunStateStore.java),
[`StreamCheckpointAuthority`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointAuthority.java),
and
[`StreamCheckpointCoordinator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointCoordinator.java).
Tested by `FileStreamClusterRunAuthorityTest#liveRescaleAtomicallyAdvancesGenerationFromCompletedCut`,
`FileStreamClusterRunAuthorityTest#replacementRecoversCheckpointAndWorkerLossAtomicallyRestartsWholeGraph`,
`FileStreamClusterRunAuthorityTest#workerRejectsOldCoordinatorAttemptReplayAndWrongIncarnation`,
`StreamCheckpointLifecycleContractTest#replacementMasterConvergesDurableRunningCheckpointWithoutAttach`,
`StreamCheckpointLifecycleContractTest#cancelConvergesCommitDecidedCheckpointBeforeTerminalTransition`,
`FileCheckpointAuthorityTest#commitAndCompletionSurviveReopenAndAckLoss`,
`FileCheckpointAuthorityTest#firstAbortOrCommitDecisionCannotBeRewritten`,
and `StreamCheckpointCoordinatorTest#replacementBeforeDecisionCanAbortButCannotCommitPartialClosure`.

## Savepoints, restore, and Stream rescale

A savepoint is an immutable, canonically encoded copy of a completed checkpoint
manifest. It is created only from that completed decision, uses first-reference
name semantics, survives acknowledgement loss, and fails closed on corruption.
Restore validates the exact artifact ID, canonical plan digest, state schema,
and key-group/parallelism compatibility before reopening a source or mutating a
sink.

Checkpoint-driven live rescale persists intent, takes a completed cut, derives
target ownership and state, restores the target generation, and then durably
cuts over. The old generation is fenced before replacement publishes. An
uncertain response reuses the stable command identity; a post-cutover generation
does not roll back to the old one. Offline migration uses the same immutable
source evidence and writes a shadow generation before binding it atomically.

Source authorities:
[`StreamSavepoint`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSavepoint.java),
[`StreamSavepointRestoreValidator`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSavepointRestoreValidator.java),
and
[`FileOfflineRescaleAuthority`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/FileOfflineRescaleAuthority.java).
Tested by `FileStreamSavepointStoreTest#completedCheckpointBecomesCanonicalReopenableSavepoint`,
`FileStreamSavepointStoreTest#durablePublicationSurvivesAckLossAndCorruptionFailsClosed`,
`StreamSavepointRestoreValidatorTest#stateOrMaxParallelismDriftFailsClosed`,
`StreamSavepointRestoreValidatorTest#forgedPlanDigestFailsBeforeRestore`,
`StreamLiveRescaleAuthorityTest#intentAndCutoverSurviveAuthorityReplacement`,
and `StreamLiveRescaleAuthorityTest#durableCutoverCannotRollBackToOldGeneration`.

## Incremental and CDC frontier

A bounded incremental feed has one durable frontier record. Initialization is
idempotent under its command identity; the store owns bootstrap, canonical
ordering, ownership claim, and conflict reporting. Recovery claims ownership
with the expected revision and ownership epoch; a lost owner cannot continue
silently and a conflicting frontier cannot be silently overwritten.

Resident CDC follows the same identity discipline as the checkpoint authority:
the checkpoint manifest establishes the joint closure, the first decision is
durable, and completion follows external reconciliation. Snapshot-to-log handoff
uses exact split boundaries and monotonic sequence positions. A gap or duplicate
produces no output and fails the attempt rather than inventing a repair.

Source authorities:
[`IncrementalFrontierStore`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierStore.java)
and
[`IncrementalFrontierAuthorityState`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierAuthorityState.java).
Tested by `IncrementalFileAuthorityProcessCrashContractTest#processHaltAfterJointForceRecoversBothProjections`
and `StreamCheckpointLifecycleContractTest#replacementMasterConvergesDurableRunningCheckpointWithoutAttach`.

## Persistent-store migration and rollback

Store migration is controlled by one authority record, one generation, one
active namespace pointer, and an increasing writer epoch. Commands are fully
bound to store identity, source and target headers, namespaces, digests, owner,
pointer version, and rollback deadline. Replaying the same command is safe;
reusing an identity with different bytes is not.

The active leader freezes admission through the runtime coordinator, then waits
for a complete durable census of active runs and incremental intervals before
copying. Copy and verify are idempotent and preserve per-object evidence. The
active pointer changes only at one atomic cutover. Before cutover, the source
pointer is authoritative; after cutover but before writes resume, the authority
can atomically roll data back to the old pointer and advance the epoch. Once
new-semantics evidence exists or the deadline passes, forward repair is the only
path.

Source authorities:
[`UpgradeAuthorityKernel`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeAuthorityKernel.java),
[`FileUpgradeAuthority`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/FileUpgradeAuthority.java),
and
[`MasterServer`](../../../rowweave-core/src/main/java/io/rowweave/cluster/MasterServer.java).
Tested by `FileUpgradeAuthorityTest#shadowCopyVerifyAndAckLossCutoverExposeCompleteAuthority`,
`FileUpgradeAuthorityTest#corruptAuthorityFailsClosedBeforeMutation`,
`UpgradeRuntimeCoordinatorTest#freezeAndDrainShareOneLinearizationLockWithRuntimeAdmission`,
`UpgradeRuntimeCoordinatorTest#quiesceIncludesDurableWorkRediscoveredAfterLeaderFailover`,
and `MasterStoreUpgradeAdmissionTest#activeLeaderFreezesAllSubmitTypesBeforePayloadDecoding`.

## Status visibility and failure boundary

Status queries use stable run, attempt, checkpoint, savepoint, feed, and store
identities. An acknowledgement is a transport event, not a durable decision. A
query returns bounded state and event pages from the authority; monitoring stays
advisory and cannot complete, retry, or recover a run.

Recovery fails closed on a missing authority, mismatched identity or fence,
corrupt frame, incomplete manifest, unknown required capability, ambiguous sink
decision, non-monotonic CDC sequence, or two writable generations. It preserves
the latest valid recovery point and evidence. Public diagnostics are data-free:
they expose bounded counts, phase, and stable identifiers, never row images,
keys, credentials, object names, or state bytes.

Source authority:
[`RetryRecoveryMetrics`](../../../rowweave-core/src/main/java/io/rowweave/cluster/RetryRecoveryMetrics.java).
Tested by `FileStreamClusterRunAuthorityTest#decisionRecoveryIsIdempotentOutsideTheOrdinaryCommandSequence`,
`FileStreamStateMigrationAuthorityTest#corruptShadowChunkCannotMoveActivePointer`,
and `StreamStateMigrationBindingContractTest#stateIdentityAndSemanticsCannotDriftAcrossMigration`.
