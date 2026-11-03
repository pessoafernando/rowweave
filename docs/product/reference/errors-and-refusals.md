---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Error and refusal reference

This page is the unified entry point for typed failures and stable
admission refusals. It catalogs every public exception family and every
closed failure/decision enum at the supported engine boundary. Wire-level
format refusals remain normative in
[format constants](format-constants.md); Pipeline has an exhaustive
code-by-code route in [pipeline boundary](pipeline-boundary.md).

## Contract model

* Stable identity is the exception type for uncoded failures, and
  `code()`/enum identity for coded failures. For Pipeline validation, both
  `PipelineValidationException` and `VersionedDatasetHandoffException` are
  part of the same program contract; branch on `code()`, never on text.
* `message` and `cause` are diagnostics. Their wording is not a compatibility
  contract and may contain implementation or dependency detail.
* Admission failures occur before provider access or side effects. Resource,
  integrity, transport, and recovery failures fail closed; the engine does not
  silently drop data, downgrade a guarantee, or invent a partial commit.
* A runtime stage failure propagates to the job status and monitor event stream.
  Correct the input or capacity condition before resubmitting.

## Correction model

| Pattern | Meaning | First corrective action |
|---|---|---|
| `INVALID`, `MISSING`, `DUPLICATE`, `UNSUPPORTED` | Closed descriptor, topology, protocol, or capability contract violation. | Correct the public descriptor or use a supported operation. |
| `LIMIT`, `QUOTA`, `CAPACITY`, `BUDGET`, `RESIDENT` | An explicit resource ceiling was reached. | Reduce load/state or raise the declared bound only after capacity review. |
| `MISMATCH`, `CONFLICT`, `STALE`, `REPLAY` | Identity, fence, version, schema, or concurrent-writer mismatch. | Use the current identities and make retries idempotent. |
| `CORRUPT`, `MALFORMED`, `NON_CANONICAL`, `CHECKSUM` | Canonical bytes or integrity evidence failed. | Preserve evidence, stop writes, restore a known-good artifact or recovery point. |
| `IO`, `UNAVAILABLE`, `TIMEOUT`, `INTERRUPTED` | A dependency or transport failed. | Repair or wait for the dependency; retry only recoverable work. |

## Public exception families

Every public `*Exception` type in the product modules is listed below. `Coded`
means the family has a closed code table in a later section.

| Exception family | Coded | Meaning and likely cause | Corrective action | Tested evidence |
|---|---|---|---|---|
| [`ArtifactLifecycleUnavailableException`](../../../rowweave-core/src/main/java/io/rowweave/artifact/ArtifactLifecycleUnavailableException.java) | no | The artifact lifecycle service is absent, closed, or not ready. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. | `ArtifactJanitorTest` |
| [`ChannelPoisonedException`](../../../rowweave-core/src/main/java/io/rowweave/sched/ChannelPoisonedException.java) | no | A pipeline channel carries an unrecoverable failure to its consumers. | Correct the reported condition and retry only when the operation is idempotent. | `MultiProducerChannelTest`, `FailurePropagationTest` |
| [`CompatibilityDataLimitException`](../../../rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimitException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `CompatibilityDataLimitsTest`, `DataSetOutputLimitsTest` |
| [`ControlResidentBudgetExceededException`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlResidentBudgetExceededException.java) | no | A control-plane decode, handler, run, or assignment exceeded its resident budget. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. | `MasterDurableRunDispatchTest`, `WhitelistObjectInputStreamResidentTest` |
| [`DeadlockException`](../../../rowweave-core/src/main/java/io/rowweave/sched/DeadlockException.java) | no | Pipeline progress cannot continue without violating scheduling or backpressure invariants. | Correct the reported condition and retry only when the operation is idempotent. | `No direct dedicated test` |
| [`DeploymentValidationException`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentValidationException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `DeploymentArchiveScannerTest`, `TrustedSignerRegistryTest` |
| [`DurableRunSubmitException`](../../../rowweave-core/src/main/java/io/rowweave/cluster/DurableRunSubmitException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `DurableCommitBarrierFailureTest`, `ClusterJobClientDurableRunTest` |
| [`ExprEvalException`](../../../rowweave-core/src/main/java/io/rowweave/expr/ExprEvalException.java) | no | Expression evaluation failed on the supplied row or value. | Correct the reported condition and retry only when the operation is idempotent. | `No direct dedicated test` |
| [`ExprParseException`](../../../rowweave-core/src/main/java/io/rowweave/expr/ExprParseException.java) | no | Expression text or binding failed parsing and type resolution. | Correct the reported condition and retry only when the operation is idempotent. | `ExprInterpreterTest` |
| [`FrontierConflictException`](../../../rowweave-core/src/main/java/io/rowweave/incremental/FrontierConflictException.java) | no | A canonical incremental frontier identity is internally inconsistent. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. | `IncrementalFrontierContractTest` |
| [`ManifestCommitUncertainException`](../../../rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitUncertainException.java) | no | A sink commit cannot be proven either committed or aborted. | Correct the reported condition and retry only when the operation is idempotent. | `No direct dedicated test` |
| [`MemoryBudgetExceededException`](../../../rowweave-core/src/main/java/io/rowweave/mem/MemoryBudgetExceededException.java) | no | Managed memory remained insufficient after spill and admission checks. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. | `TypedQualityPublicationLocalExecutionTest`, `PushDagExecutorTest` |
| [`PipelineValidationException`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/PipelineValidationException.java) | no | A pipeline definition, artifact adapter, binding, or resource boundary is invalid. | Correct the reported condition and retry only when the operation is idempotent. | `PipelineInspectionTest`, `VersionedDatasetHandoffFaultContractTest` |
| [`PlanArtifactValidationException`](../../../rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifactValidationException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `PlanArtifactCodecTest`, `PipelineCompositionShapesTest` |
| [`PortablePlanException`](../../../rowweave-core/src/main/java/io/rowweave/portable/PortablePlanException.java) | no | A portable plan failed admission, binding, or canonical codec validation. | Correct the reported condition and retry only when the operation is idempotent. | `PortableBindingTest`, `PortablePlanCompilerTest` |
| [`ProcessCapacityException`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ProcessCapacityException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `ClusterConfigCapacityTest`, `JvmCapacityEnvelopeTest` |
| [`ProtocolNegotiationException`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ProtocolNegotiationException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `ProtocolHelloV1Test`, `NegotiatedRpcConnectionTest` |
| [`ReleaseContractException`](../../../rowweave-core/src/main/java/io/rowweave/release/ReleaseContractException.java) | no | A release qualification or manifest invariant failed. | Correct the reported condition and retry only when the operation is idempotent. | `ReleaseContractManifestTest`, `ReleaseContractAdmissionTest` |
| [`RunHistoryRetiringException`](../../../rowweave-core/src/main/java/io/rowweave/run/RunHistoryRetiringException.java) | no | A run history shard is retiring and cannot accept the requested mutation. | Correct the reported condition and retry only when the operation is idempotent. | `MasterRunExecutionEventQueryTest`, `FileRunPurgeCrashTest` |
| [`RunStateStoreUnavailableException`](../../../rowweave-core/src/main/java/io/rowweave/run/RunStateStoreUnavailableException.java) | no | The run state store is unavailable or failed its durability contract. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. | `LeaderRecoveryInventoryTest`, `TerminalRunJanitorTest` |
| [`SpillDiskException`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillDiskException.java) | no | Spill admission or lifecycle failed with a stable closed category. | Correct the reported condition and retry only when the operation is idempotent. | `ExecutionMemoryWorkspaceTest`, `SpillWorkspaceOwnershipTest` |
| [`StreamApiException`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamApiException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamPublicPlanCompilerContractTest`, `StreamPublicRetractCompilerContractTest` |
| [`StreamCdcException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCdcException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `ResidentStreamCdcEnumeratorTest` |
| [`StreamChangeException`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamChangeException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamChangeRecordCodecTest` |
| [`StreamChannelCancelledException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamChannelCancelledException.java) | no | A stream channel was cancelled by a valid lifecycle transition. | Correct the reported condition and retry only when the operation is idempotent. | `StreamSocketChannelTest`, `LocalStreamChannelTest` |
| [`StreamChannelFailedException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamChannelFailedException.java) | no | A stream channel failed and must stop its consumer. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. | `LocalStreamSourceTaskTest` |
| [`StreamCheckpointException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamCheckpointCoordinatorTest`, `StreamSavepointRestoreValidatorTest` |
| [`StreamClusterProtocolException`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterProtocolException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamClusterProtocolContractTest` |
| [`StreamConnectorConformanceException`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamConnectorConformanceException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamBoundedSourceConformanceKitTest` |
| [`StreamContractException`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamContractException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `PortableStreamPlanContractTest`, `StreamAtomicPlanRegistryContractTest` |
| [`StreamControlException`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamControlException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamPublicJobClientContractTest`, `ZkStreamJobClientTest` |
| [`StreamExpressionException`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamExpressionException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamExpressionContractTest` |
| [`StreamProcessException`](../../../rowweave-core/src/main/java/io/rowweave/StreamProcessException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamKeyedProcessOperatorTaskTest` |
| [`StreamProductControlException`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductControlException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamProductJobAuthorityContractTest`, `StreamLiveRescaleAuthorityTest` |
| [`StreamStateException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamStateException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `ManagedKeyedValueStateTest`, `ManagedKeyedStateCheckpointTest` |
| [`StreamTemporalJoinException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamTemporalJoinException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `BoundedEventTimeTemporalJoinTest` |
| [`StreamTimeException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamTimeException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamTimeServiceTest`, `FinalEventTimeDecimalWindowTest` |
| [`StreamUpsertException`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamUpsertException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `StreamUpsertRecordCodecTest` |
| [`StreamWindowException`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamWindowException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `FinalEventTimeDecimalWindowTest` |
| [`UnsupportedExprException`](../../../rowweave-core/src/main/java/io/rowweave/expr/codegen/UnsupportedExprException.java) | no | The expression uses a construct outside the compiled/interpreted supported set. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. | `No direct dedicated test` |
| [`VersionedDatasetHandoffException`](../../../rowweave-pipeline/src/main/java/io/rowweave/pipeline/VersionedDatasetHandoffException.java) | no | A versioned dataset authority or handoff transition failed. | Correct the reported condition and retry only when the operation is idempotent. | `VersionedDatasetHandoffFaultContractTest`, `InMemoryVersionedDatasetHandoffAuthorityTest` |
| [`WriterRevisionRejectedException`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/WriterRevisionRejectedException.java) | yes | A closed code table below gives each stable outcome and refusal. | Follow the code-specific corrective action in the table below. | `WriterRevisionGateTest`, `MasterStoreUpgradeAdmissionTest` |

## Closed failure and decision codes

Each table is generated from the closed enum in the linked production source.
A new value is an additive compatibility event only when its owning contract
explicitly freezes that policy; consumers must treat unknown enum values as
unsupported rather than guessing.

## Batch data, capacity, spill, and durable submission

### `CompatibilityDataLimitException`

Source: [`rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimitException.java`](../../../rowweave-core/src/main/java/io/rowweave/CompatibilityDataLimitException.java). Tested evidence: `CompatibilityDataLimitsTest`, `DataSetOutputLimitsTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `ROWS` | A compatibility materialization exceeded its maximum row count. | Correct the reported condition and retry only when the operation is idempotent. |
| `BYTES` | A compatibility materialization exceeded its maximum serialized byte size. | Correct the reported condition and retry only when the operation is idempotent. |

### `StreamProcessException`

Source: [`rowweave-core/src/main/java/io/rowweave/StreamProcessException.java`](../../../rowweave-core/src/main/java/io/rowweave/StreamProcessException.java). Tested evidence: `StreamKeyedProcessOperatorTaskTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `UNDECLARED_STATE` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `STATE_KIND_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `UNDECLARED_TIMER` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `UNDECLARED_OUTPUT_TAG` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `CALLBACK_SCOPE_VIOLATION` | The runtime reported the closed callback scope violation condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `OUTPUT_QUOTA_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `STATE_QUOTA_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `TIMER_QUOTA_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `TIMER_REENTRANCE_REJECTED` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `MISSING_RECORD_TIMESTAMP` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `INVALID_STATE_VALUE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNSUPPORTED_STATE_KIND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNSUPPORTED_TIMER_RUNTIME` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |

### `QualityGateDecision`

Source: [`rowweave-core/src/main/java/io/rowweave/QualityGateDecision.java`](../../../rowweave-core/src/main/java/io/rowweave/QualityGateDecision.java). Tested evidence: `QualityGateDecisionCodecTest`, `DurableCommitBarrierFailureTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `REASON_THRESHOLD_EXCEEDED` | The runtime reported the closed reason threshold exceeded condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `REJECTED_RATIO_EXCEEDED` | The runtime reported the closed rejected ratio exceeded condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `REJECTED_ROWS_EXCEEDED` | The runtime reported the closed rejected rows exceeded condition. | Correct the reported condition and retry only when the operation is idempotent. |

### `QualityGateDecisionResult`

Source: [`rowweave-core/src/main/java/io/rowweave/QualityGateDecisionResult.java`](../../../rowweave-core/src/main/java/io/rowweave/QualityGateDecisionResult.java). Tested evidence: `QualityGateInMemoryAuthorityTest`, `ZkQualityGateAuthorityTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `RECORDED` | The first durable decision was recorded with the supplied evidence. | Treat this as the authoritative result and retry only when the operation is intended to be idempotent. |
| `ALREADY_RECORDED` | An idempotent retry matched the existing durable decision. | Treat this as the authoritative result and retry only when the operation is intended to be idempotent. |
| `STALE_RUN` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `ATTEMPT_NOT_COMMITTING` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INCOMPLETE_MEMBERS` | The runtime reported the closed incomplete members condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `IDENTITY_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `EVIDENCE_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `DECISION_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `ABORT_ALREADY_DECIDED` | The runtime reported the closed abort already decided condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `RECOVERY_OWNED` | The runtime reported the closed recovery owned condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CORRUPT` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `UNAVAILABLE` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |

### `ProcessCapacityException`

Source: [`rowweave-core/src/main/java/io/rowweave/cluster/ProcessCapacityException.java`](../../../rowweave-core/src/main/java/io/rowweave/cluster/ProcessCapacityException.java). Tested evidence: `ClusterConfigCapacityTest`, `JvmCapacityEnvelopeTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `JVM_OPTION` | The runtime reported the closed jvm option condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CONFIGURATION` | The runtime reported the closed configuration condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `HEAP_CAPACITY` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `RSS_CAPACITY` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `STAGE_THREADS` | The runtime reported the closed stage threads condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CERTIFICATION` | The runtime reported the closed certification condition. | Correct the reported condition and retry only when the operation is idempotent. |

### `DurableRunSubmitException`

Source: [`rowweave-core/src/main/java/io/rowweave/cluster/DurableRunSubmitException.java`](../../../rowweave-core/src/main/java/io/rowweave/cluster/DurableRunSubmitException.java). Tested evidence: `DurableCommitBarrierFailureTest`, `ClusterJobClientDurableRunTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `REJECTED` | The durable submission was refused before execution. | Correct the reported condition and retry only when the operation is idempotent. |
| `TERMINAL_FAILED` | The run reached a deterministic failed terminal state. | Inspect the terminal snapshot and corrected input, then submit a new run identity. |
| `UNKNOWN` | The operation may or may not have taken effect; identity reuse is unsafe. | Keep the returned run identity, query authoritative status, and never create a new identity for this outcome. |

### `SpillFailureCode`

Source: [`rowweave-core/src/main/java/io/rowweave/mem/spill/SpillFailureCode.java`](../../../rowweave-core/src/main/java/io/rowweave/mem/spill/SpillFailureCode.java). Tested evidence: `SpillDiskFailureTest`, `SpillWorkspaceQuotaTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `SPILL_CONFIG_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SPILL_WORKSPACE_UNAVAILABLE` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `SPILL_WORKER_QUOTA_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `SPILL_JOB_QUOTA_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `SPILL_MIN_FREE_SPACE` | The spill workspace fell below its reserved free-space floor. | Correct the reported condition and retry only when the operation is idempotent. |
| `SPILL_IO_WRITE_FAILED` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `SPILL_CLEANUP_FAILED` | The operation failed before a valid result could be established. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. |

## Incremental authority and store upgrade

### `IncrementalAuthorityDecision`

Source: [`rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAuthorityDecision.java`](../../../rowweave-core/src/main/java/io/rowweave/incremental/IncrementalAuthorityDecision.java). Tested evidence: `IncrementalInMemoryAuthorityClaimContractTest`, `ZkIncrementalAuthorityGatewayTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `ACCEPTED` | The request passed admission and may proceed. | Treat this as the authoritative result and retry only when the operation is intended to be idempotent. |
| `ALREADY_APPLIED` | The runtime reported the closed already applied condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `FEED_NOT_FOUND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FEED_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `GUARD_SHARD_FULL` | The runtime reported the closed guard shard full condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STALE_REVISION` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STALE_OWNERSHIP` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `COMMAND_ID_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `ACTIVE_EXISTS` | The runtime reported the closed active exists condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `PENDING_NOT_FOUND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PENDING_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `RUN_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `CONTRACT_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |

### `UpgradeTransition`

Source: [`rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeTransition.java`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeTransition.java). Tested evidence: `UpgradeAuthorityKernelTest`, `MasterStoreUpgradeAdmissionTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `ACCEPTED` | The request passed admission and may proceed. | Treat this as the authoritative result and retry only when the operation is intended to be idempotent. |
| `REPLAYED` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `ACTIVE_WORK_REMAINS` | The runtime reported the closed active work remains condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `COMMAND_ID_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `BINDING_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `DIGEST_DRIFT` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `STALE_OWNER` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `POINTER_VERSION_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_PHASE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `ROLLBACK_FORBIDDEN` | The runtime reported the closed rollback forbidden condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DEADLINE_NOT_REACHED` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `EVIDENCE_EXHAUSTED` | The runtime reported the closed evidence exhausted condition. | Correct the reported condition and retry only when the operation is idempotent. |

## Stream API and job control

### `StreamApiException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/api/StreamApiException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamApiException.java). Tested evidence: `StreamPublicPlanCompilerContractTest`, `StreamPublicRetractCompilerContractTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_CONFIGURATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `MISSING_OPERATOR_UID` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `DUPLICATE_OPERATOR_UID` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `MISSING_SINK` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `DELIVERY_GUARANTEE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `FUNCTION_ABI_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `SCHEMA_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_STABLE_KEY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INCOMPATIBLE_KEY_CODEC` | The runtime reported the closed incompatible key codec condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `UNUSED_OUTPUT_TAG` | The runtime reported the closed unused output tag condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `MISSING_STABLE_KEY` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `TIME_MODE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `TIME_AUTHORITY_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `CHANGELOG_MODE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_WINDOW_FIELD_BINDING` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `MISSING_WATERMARK_POLICY` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `INVALID_STATE_DESCRIPTOR` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_EXPRESSION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CLIENT_REQUIRED` | The runtime reported the closed client required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `INVALID_TOPOLOGY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |

### `StreamExpressionException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/api/StreamExpression.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamExpression.java). Tested evidence: `StreamExpressionContractTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `UNKNOWN_EXPRESSION_KIND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_EXPRESSION_FUNCTION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `EXPRESSION_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `INVALID_EXPRESSION_LITERAL` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `EXPRESSION_ARITY_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `NON_CANONICAL_TEXT` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `EXPRESSION_TYPE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |

### `StreamControlException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/api/StreamControlException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/api/StreamControlException.java). Tested evidence: `StreamPublicJobClientContractTest`, `ZkStreamJobClientTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `SUBMISSION_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `COMMAND_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `COMMAND_NOT_FOUND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `JOB_NOT_FOUND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `WAIT_TIMEOUT` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `WAIT_INTERRUPTED` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `TRANSPORT_FAILURE` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `UNSUPPORTED_OPERATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_RESPONSE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |

## Stream contract and cluster protocol

### `StreamContractException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/contract/StreamContractException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamContractException.java). Tested evidence: `PortableStreamPlanContractTest`, `StreamAtomicPlanRegistryContractTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `FRAME_LENGTH_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_MAGIC` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNSUPPORTED_ENVELOPE_VERSION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CHECKSUM_MISMATCH` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `MALFORMED_UTF8` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `STREAM_SEMANTIC_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STREAM_DESCRIPTOR_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STREAM_CAPABILITY_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `MULTIPLE_DEPLOYMENT_SETS` | The runtime reported the closed multiple deployment sets condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `UNKNOWN_DELIVERY_GUARANTEE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_CHANGELOG_MODE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_TIME_MODE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNSUPPORTED_PLAN_VERSION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_PARALLELISM` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_SEMANTICS` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `MISSING_CAPABILITY` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `DUPLICATE_OPERATOR_UID` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `INVALID_TOPOLOGY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SCHEMA_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `MISSING_STABLE_KEY` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `DYNAMIC_SOURCE_CAPABILITY_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `SOURCE_TERMINATION_CAPABILITY_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_ROOT_SET` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNREACHABLE_OPERATOR` | The runtime reported the closed unreachable operator condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DELIVERY_GUARANTEE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `MISSING_NODE_EXECUTION_CONTRACT` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `INVALID_NODE_EXECUTION_CONTRACT` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_STABLE_KEY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CHANGELOG_MODE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STATE_TIME_MODE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_EDGE_SET` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_UNION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_UNION_KEY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_CALC` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `TIME_MODE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_TIME_ASSIGNMENT` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_PROCESS_RESOURCE_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `STATE_BINDING_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INCOMPATIBLE_KEY_CODEC` | The runtime reported the closed incompatible key codec condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `INVALID_WINDOW_BINDING` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `INVALID_WINDOW_RESOURCE_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `INVALID_WINDOW_FIELD_BINDING` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `INVALID_ASYNC_BINDING` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_ASYNC_RESOURCE_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `INVALID_FLAT_MAP_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `UNSUPPORTED_PARTITIONING` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `MISSING_PARTITION_KEY` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `INVALID_BROADCAST_EDGE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNUSED_OUTPUT_TAG` | The runtime reported the closed unused output tag condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `INVALID_WINDOW` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `INVALID_TEMPORAL_JOIN_KEY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_TEMPORAL_JOIN` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_TEMPORAL_JOIN_STATE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_SOURCE_TIME_POLICY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_SOURCE_TERMINATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_RESIDENT_CDC_SOURCE` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `MISSING_UPSERT_KEY` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `INVALID_UPSERT_KEY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `DUPLICATE_VALUE` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `UNKNOWN_OPERATOR_UID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `MALFORMED_CDC_ENUMERATOR_STATE` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `INVALID_EXPECTED_TASK` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_EVIDENCE_SET` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `DUPLICATE_EVIDENCE` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `UNKNOWN_ASYNC_ORDERING` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_ASYNC_REPLAY_MODE` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STALE_CHANNEL` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `CHANNEL_ALREADY_TERMINATED` | The runtime reported the closed channel already terminated condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CHANNEL_SEQUENCE_REPLAY` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `CHANNEL_SEQUENCE_GAP` | The runtime reported the closed channel sequence gap condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `WATERMARK_REGRESSION` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `WATERMARK_STATUS_GENERATION_REPLAY` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_EVIDENCE_KEY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CHANNEL_STATE_BYTE_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `CHANNEL_STATE_EVENT_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `CHANNEL_STATE_FRAME_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `INVALID_SAVEPOINT` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_OPCODE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_STATE_MIGRATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CHECKPOINT_POLICY_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_JOB_RESOURCE_CEILING` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `MISSING_SOURCE_TIME_POLICY` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `MISSING_WINDOW_FIELD_BINDING` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `UNSUPPORTED_JOB_VERSION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNSUPPORTED_CHECKPOINT_MODE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_JOB_CONFIGURATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_STATE_RESOURCE_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `UNKNOWN_ENVELOPE_KIND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_WATERMARK_STATUS` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `INVALID_ENVELOPE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_RECORD` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CHECKPOINT_SCOPE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_WATERMARK_STATUS` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `INVALID_ASYNC_COMPLETION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_ERROR` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `NON_CANONICAL_MEMBER` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `UNKNOWN_PROCESS_FUNCTION_KIND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_PROCESS_BINDING` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FUNCTION_ABI_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_BROADCAST_BINDING` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_EVIDENCE_KIND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_EVIDENCE_SIZE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_FUNCTION_KIND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_FUNCTION_BINDING` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_BROADCAST_AUTHORITY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_BROADCAST_RESOURCE_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `INVALID_EXPRESSION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `EXPRESSION_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `UNSUPPORTED_EXPRESSION_VERSION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_EXPRESSION_KIND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `EXPRESSION_ARITY_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_EXPRESSION_LITERAL` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_EXPRESSION_TYPE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `NON_CANONICAL_TEXT` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `UNKNOWN_WINDOW_ASSIGNER` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `UNKNOWN_WINDOW_TRIGGER` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `UNKNOWN_WINDOW_ACCUMULATION` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `UNKNOWN_WINDOW_FUNCTION_KIND` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `UNKNOWN_STATE_KIND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_TTL_MODE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_STATE_DESCRIPTOR` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_CALC_PROJECTION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `DECIMAL_BOUNDARY_TYPE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `EXPRESSION_TYPE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `DUPLICATE_PROJECTION` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `CALC_SCHEMA_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STALE_CHECKPOINT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `CHECKPOINT_ALREADY_DECIDED` | The runtime reported the closed checkpoint already decided condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `UNEXPECTED_TASK` | The runtime reported the closed unexpected task condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STALE_TASK_ASSIGNMENT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `EVIDENCE_SET_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `TASK_RECEIPT_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `DECISION_ALREADY_ABORTED` | The runtime reported the closed decision already aborted condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CHECKPOINT_INCOMPLETE` | The runtime reported the closed checkpoint incomplete condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DECISION_ALREADY_COMMITTED` | The runtime reported the closed decision already committed condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CHECKPOINT_NOT_COMMITTED` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `STALE_CHECKPOINT_DECISION` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `UNKNOWN_PARTITIONING` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_EDGE_INPUT` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_EDGE_CREDIT` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_TIMER_DOMAIN` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `INVALID_TIMER_RESOURCE_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `INVALID_OUTPUT_TAG` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_TASK_RECEIPT` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_CDC_ENUMERATOR_STATE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_DIGEST` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `INVALID_CHANNEL_IDENTITY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_CHECKPOINT_MANIFEST` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `DUPLICATE_TASK` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `INVALID_SCHEMA` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `DUPLICATE_STATE` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `FUNCTION_KIND_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `BINDING_KIND_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_CHECKPOINT_RUN` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CHECKPOINT_SEQUENCE_REPLAY` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `CHECKPOINT_IN_FLIGHT` | The runtime reported the closed checkpoint in flight condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `NO_ACTIVE_CHECKPOINT` | The runtime reported the closed no active checkpoint condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `UNKNOWN_CHECKPOINT_MEMBER_KIND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_CHECKPOINT_MEMBER` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_SOURCE_POSITION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_KEY_GROUP_RANGE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_STATE_HANDLE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_CHANNEL_STATE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_CHECKPOINT_STATUS` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_CHECKPOINT_DECISION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_SINK_VISIBILITY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CONFIGURATION_DIGEST_MISMATCH` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `INVALID_SINK_VISIBILITY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_SINK_CHANGELOG` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_CHECKPOINT_IDENTITY` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNKNOWN_WATERMARK_MODE` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `UNKNOWN_TIMESTAMP_POLICY` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `UNKNOWN_REPLAY_GUARANTEE` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_SPLIT_SET` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `DUPLICATE_SPLIT_ID` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `SOURCE_NOT_REPLAYABLE` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `DUPLICATE_PARAMETER` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `CHANNEL_STATE_SCOPE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `CHANNEL_STATE_SEQUENCE_GAP` | The runtime reported the closed channel state sequence gap condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CHANNEL_STATE_BARRIER_INCLUDED` | The runtime reported the closed channel state barrier included condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CHANNEL_STATE_TERMINAL_INCLUDED` | The runtime reported the closed channel state terminal included condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `INVALID_TASK_SET` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |

### `StreamChangeException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/contract/StreamChangeException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamChangeException.java). Tested evidence: `StreamChangeRecordCodecTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_RECORD` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FRAME_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `MALFORMED_FRAME` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `CHECKSUM_MISMATCH` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |

### `StreamUpsertException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/contract/StreamUpsertException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/contract/StreamUpsertException.java). Tested evidence: `StreamUpsertRecordCodecTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_RECORD` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FRAME_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `MALFORMED_FRAME` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `CHECKSUM_MISMATCH` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |

### `StreamClusterProtocolException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterProtocolException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterProtocolException.java). Tested evidence: `StreamClusterProtocolContractTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_CONTROL_FRAME` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNSUPPORTED_CONTROL_VERSION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CHECKSUM_MISMATCH` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `UNKNOWN_CONTROL_KIND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `INSUFFICIENT_CAPACITY` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `AMBIGUOUS_WORKER_INCARNATION` | The runtime reported the closed ambiguous worker incarnation condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STALE_COORDINATOR` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STALE_ATTEMPT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `COMMAND_REPLAY` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `WORKER_INCARNATION_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `RUN_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |

### `StreamProductControlException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductControlException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamProductControlException.java). Tested evidence: `StreamProductJobAuthorityContractTest`, `StreamLiveRescaleAuthorityTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `SUBMISSION_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `COMMAND_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `COMMAND_NOT_FOUND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `JOB_NOT_FOUND` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_STATE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_FRAME` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |

## Artifact, deployment, RPC admission, and writer revision

### `PlanArtifactValidationException`

Source: [`rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifactValidationException.java`](../../../rowweave-core/src/main/java/io/rowweave/artifact/PlanArtifactValidationException.java). Tested evidence: `PlanArtifactCodecTest`, `PipelineCompositionShapesTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_INCREMENTAL_PLAN` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_SEMANTICS` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PORTABLE_ADAPTER_REQUIRED` | The runtime reported the closed portable adapter required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `INCREMENTAL_RUNTIME_REQUIRED` | The runtime reported the closed incremental runtime required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `LOCAL_PAYLOAD_REFERENCE_MISSING` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `LEGACY_DESCRIPTOR_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `NON_TOPOLOGICAL_OPERATOR_INPUT` | The runtime reported the closed non topological operator input condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `NON_CANONICAL_ARTIFACT` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `NON_CANONICAL_TEXT` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `MALFORMED_UTF8` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `UNSUPPORTED_PLAN_SCHEMA` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `READER_TOO_OLD` | The runtime reported the closed reader too old condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `INVALID_DEPLOYMENT_FEATURE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNSUPPORTED_REQUIRED_FEATURE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PAYLOAD_FEATURE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STREAM_SEMANTIC_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `MISSING_STREAM_CAPABILITY` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `UNSUPPORTED_RUNTIME` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `LEGACY_NOT_PUBLIC_V1` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `STREAM_PRODUCT_JOB_REQUIRED` | The runtime reported the closed stream product job required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DUPLICATE_FEATURE` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `DUPLICATE_OPERATOR_ID` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `UNKNOWN_OPERATOR_INPUT` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `MULTIPLE_DEPLOYMENT_SETS` | The runtime reported the closed multiple deployment sets condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DEPLOYMENT_RUNTIME_REQUIRED` | The runtime reported the closed deployment runtime required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `PORTABLE_PAYLOAD_REQUIRED` | The runtime reported the closed portable payload required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `PORTABLE_SEMANTIC_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `PORTABLE_DESCRIPTOR_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INCREMENTAL_RUNTIME_LOCKED` | The runtime reported the closed incremental runtime locked condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `FRAME_LENGTH_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INVALID_MAGIC` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `UNSUPPORTED_ENVELOPE_VERSION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CHECKSUM_MISMATCH` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `DEPLOYMENT_FEATURE_REQUIRED` | The runtime reported the closed deployment feature required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DEPLOYMENT_SET_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |

### `DeploymentValidationException`

Source: [`rowweave-core/src/main/java/io/rowweave/deployment/DeploymentValidationException.java`](../../../rowweave-core/src/main/java/io/rowweave/deployment/DeploymentValidationException.java). Tested evidence: `DeploymentArchiveScannerTest`, `TrustedSignerRegistryTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `DEPLOYMENT_ENTRY_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `DEPLOYMENT_COMPRESSION_RATIO_EXCEEDED` | The runtime reported the closed deployment compression ratio exceeded condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DEPLOYMENT_DUPLICATE_ENTRY` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `DEPLOYMENT_DUPLICATE_CLASS` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `DEPLOYMENT_ENTRY_INTEGRITY_FAILED` | The operation failed before a valid result could be established. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. |
| `DEPLOYMENT_ARCHIVE_FORBIDDEN` | The runtime reported the closed deployment archive forbidden condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DEPLOYMENT_ENTRY_NAME_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `DEPLOYMENT_FORBIDDEN_NAMESPACE` | The runtime reported the closed deployment forbidden namespace condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DEPLOYMENT_ZIP_FORMAT_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `DEPLOYMENT_MANIFEST_NOT_TRUSTED` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_DEPLOYMENT_MANIFEST` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CONNECTOR_PROVIDER_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `SOURCE_BINDING_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SOURCE_BINDING_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `SINK_BINDING_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SINK_BINDING_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `SOURCE_RUNTIME_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `SINK_RUNTIME_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `PORTABLE_RUNTIME_REQUIRED` | The runtime reported the closed portable runtime required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CONNECTOR_PROVIDER_UNKNOWN` | The runtime reported the closed connector provider unknown condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CONNECTOR_CONTRACT_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `CONNECTOR_CONFIGURATION_CODEC_UNSUPPORTED` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SOURCE_DESCRIPTOR_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SINK_DESCRIPTOR_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CONNECTOR_CONTRACT_DRIFT` | The runtime reported the closed connector contract drift condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CONNECTOR_SERVICE_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CONNECTOR_CONTRACT_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CONNECTOR_PROVIDER_DUPLICATE` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `CONNECTOR_RESOURCE_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `CONNECTOR_PROVIDER_CLASS_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `STREAM_CONNECTOR_SERVICE_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `STREAM_CONNECTOR_UNKNOWN` | The runtime reported the closed stream connector unknown condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STREAM_CONNECTOR_DEPLOYMENT_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STREAM_CONNECTOR_CONFIGURATION_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `STREAM_CONNECTOR_CAPABILITY_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STREAM_CONNECTOR_CONTRACT_DRIFT` | The runtime reported the closed stream connector contract drift condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STREAM_CONNECTOR_CONFIGURATION_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `STREAM_CONNECTOR_KIND_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STREAM_CONNECTOR_RUNTIME_REQUIRED` | The runtime reported the closed stream connector runtime required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STREAM_CONNECTOR_CONTRACT_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `STREAM_CONNECTOR_DUPLICATE` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `STREAM_CONNECTOR_NOT_ADMITTED` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FUNCTION_DEPLOYMENT_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `FUNCTION_CONFIGURATION_CODEC_UNSUPPORTED` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FUNCTION_RUNTIME_REQUIRED` | The runtime reported the closed function runtime required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `FUNCTION_SERVICE_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FUNCTION_CONTRACT_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FUNCTION_PROVIDER_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `FUNCTION_PROVIDER_DUPLICATE` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `FUNCTION_PROVIDER_CLASS_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FUNCTION_CONTRACT_DRIFT` | The runtime reported the closed function contract drift condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `FUNCTION_CREATE_FAILED` | The operation failed before a valid result could be established. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. |
| `FUNCTION_RUNTIME_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `PROVIDER_GROUP_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PROVIDER_CONTEXT_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PROVIDER_LOCATOR_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PROVIDER_ATTEMPT_NOT_OPEN` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PROVIDER_OUTCOME_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `PROVIDER_GROUP_CLOSED` | The runtime reported the closed provider group closed condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DEPLOYMENT_MEMORY_BUDGET_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `DEPLOYMENT_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `DUPLICATE_DEPLOYMENT_ID` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `DEPLOYMENT_SOURCE_CHANGED` | The runtime reported the closed deployment source changed condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DEPLOYMENT_SOURCE_NOT_REGULAR` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `DEPLOYMENT_LENGTH_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `DEPLOYMENT_DIGEST_MISMATCH` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `DEPLOYMENT_SOURCE_READ_FAILED` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `DEPLOYMENT_SIGNER_UNKNOWN` | The runtime reported the closed deployment signer unknown condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DEPLOYMENT_SIGNER_KEY_TOO_WEAK` | The runtime reported the closed deployment signer key too weak condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `DEPLOYMENT_SIGNATURE_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PROVIDER_NOT_ADMITTED` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PROVIDER_CONTRACT_DRIFT` | The runtime reported the closed provider contract drift condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `PROVIDER_DUPLICATE` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `PROVIDER_UNKNOWN` | The runtime reported the closed provider unknown condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `PROVIDER_RUNTIME_REQUIRED` | The runtime reported the closed provider runtime required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `PROVIDER_SERVICE_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PROVIDER_BUDGET_INVALID` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `SIGNER_KEY_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `TRUST_ROOT_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `TRUST_ROOT_WRITABLE` | The runtime reported the closed trust root writable condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `SIGNER_KEY_ID_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SIGNER_KEY_PATH_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SIGNER_KEY_OUTSIDE_TRUST_ROOT` | The runtime reported the closed signer key outside trust root condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `SIGNER_KEY_WRITABLE` | The runtime reported the closed signer key writable condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `SIGNER_KEY_TOO_LARGE` | The runtime reported the closed signer key too large condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `SIGNER_KEY_READ_FAILED` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `SIGNER_KEY_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `STATE_MIGRATION_RUNTIME_REQUIRED` | The runtime reported the closed state migration runtime required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STATE_MIGRATION_DEPLOYMENT_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `STATE_MIGRATION_CLASS_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SOURCE_PROVIDER_NOT_ADMITTED` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SOURCE_PROVIDER_CONTRACT_DRIFT` | The runtime reported the closed source provider contract drift condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `SOURCE_PROVIDER_DUPLICATE` | A value that must be unique was reused. | Assign the required unique identifier or remove the duplicate registration. |
| `SOURCE_PROVIDER_UNKNOWN` | The runtime reported the closed source provider unknown condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `SOURCE_PROVIDER_RUNTIME_REQUIRED` | The runtime reported the closed source provider runtime required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `SOURCE_PROVIDER_SERVICE_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SOURCE_PROVIDER_BUDGET_INVALID` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |

### `ProtocolNegotiationException`

Source: [`rowweave-core/src/main/java/io/rowweave/cluster/rpc/ProtocolNegotiationException.java`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ProtocolNegotiationException.java). Tested evidence: `ProtocolHelloV1Test`, `NegotiatedRpcConnectionTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `IN_PROCESS_NEGOTIATION_REQUIRED` | The runtime reported the closed in process negotiation required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `RESIDENT_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `MESSAGE_UNAVAILABLE` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `WRITER_REVISION_UNAVAILABLE` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `FRAME_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |

### `WriterRevisionRejectedException`

Source: [`rowweave-core/src/main/java/io/rowweave/upgrade/WriterRevisionRejectedException.java`](../../../rowweave-core/src/main/java/io/rowweave/upgrade/WriterRevisionRejectedException.java). Tested evidence: `WriterRevisionGateTest`, `MasterStoreUpgradeAdmissionTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `ACTIVE_WRITER_REVISION_UNAVAILABLE` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `WRITER_REVISION_NOT_PROMOTION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `WRITER_REVISION_UNAVAILABLE` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `WRITER_SET_REQUIRED` | The runtime reported the closed writer set required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `WRITER_HELLO_REQUIRED` | The runtime reported the closed writer hello required condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `WRITER_PROTOCOL_EPOCH_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `WRITER_MANIFEST_DIGEST_MISMATCH` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `WRITER_REVISION_INTERSECTION_EMPTY` | The runtime reported the closed writer revision intersection empty condition. | Correct the reported condition and retry only when the operation is idempotent. |

## Stream runtime, checkpoint, recovery, and conformance

### `StreamCdcException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCdcException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCdcException.java). Tested evidence: `ResidentStreamCdcEnumeratorTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_CONFIGURATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_PHASE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SPLIT_ORDER` | The runtime reported the closed split order condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `INVALID_SNAPSHOT_RECORD` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SNAPSHOT_INCOMPLETE` | The runtime reported the closed snapshot incomplete condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CHECKPOINT_BOUNDARY` | The runtime reported the closed checkpoint boundary condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `LOG_SEQUENCE` | The runtime reported the closed log sequence condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `FRAME_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `OUTPUT_FAILURE` | The operation failed before a valid result could be established. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. |
| `WRONG_THREAD` | The runtime reported the closed wrong thread condition. | Correct the reported condition and retry only when the operation is idempotent. |

### `StreamCheckpointException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointException.java). Tested evidence: `StreamCheckpointCoordinatorTest`, `StreamSavepointRestoreValidatorTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_CONFIGURATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `THREAD_OWNERSHIP` | The runtime reported the closed thread ownership condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CHECKPOINT_IN_FLIGHT` | The runtime reported the closed checkpoint in flight condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CHECKPOINT_SEQUENCE_REPLAY` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `UNEXPECTED_BARRIER` | The runtime reported the closed unexpected barrier condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CHECKPOINT_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `INPUT_BLOCKED` | The runtime reported the closed input blocked condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CHANNEL_STATE_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `CALLBACK_FAILURE` | The operation failed before a valid result could be established. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. |
| `CHECKPOINT_ALREADY_DECIDED` | The runtime reported the closed checkpoint already decided condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `FIRST_DECISION_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `CORRUPT_AUTHORITY` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `CORRUPT_MEMBER_CATALOG` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `IO_FAILURE` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `COMMIT_PROOF_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `DECISION_ALREADY_COMMITTED` | The runtime reported the closed decision already committed condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `SINK_FAILURE` | The operation failed before a valid result could be established. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. |
| `SOURCE_POSITION_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `PAYLOAD_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `CORRUPT_PAYLOAD` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `CHECKPOINT_NOT_COMPLETED` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SAVEPOINT_CONFLICT` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `CORRUPT_SAVEPOINT` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `CORRUPT_CHUNK` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `CORRUPT_CHUNK_MANIFEST` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `SAVEPOINT_INCOMPATIBLE` | The runtime reported the closed savepoint incompatible condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STATE_MIGRATION_INCOMPATIBLE` | The runtime reported the closed state migration incompatible condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STATE_MIGRATION_INCOMPLETE` | The runtime reported the closed state migration incomplete condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STATE_MIGRATION_FAILED` | The operation failed before a valid result could be established. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. |
| `STATE_MIGRATION_NON_DETERMINISTIC` | The runtime reported the closed state migration non deterministic condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STATE_MIGRATION_OUTPUT_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `RESCALE_INCOMPATIBLE` | The runtime reported the closed rescale incompatible condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `RESCALE_EVIDENCE_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |

### `StreamStateException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamStateException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamStateException.java). Tested evidence: `ManagedKeyedValueStateTest`, `ManagedKeyedStateCheckpointTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_CONFIGURATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FOREIGN_KEY_GROUP` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `THREAD_OWNERSHIP` | The runtime reported the closed thread ownership condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `FRAME_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `SPILL_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `WATERMARK_REGRESSION` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `CORRUPT_SPILL` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `CORRUPT_SNAPSHOT` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `IO_FAILURE` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `CLOSED` | The runtime reported the closed closed condition. | Correct the reported condition and retry only when the operation is idempotent. |

### `StreamTemporalJoinException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamTemporalJoinException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamTemporalJoinException.java). Tested evidence: `BoundedEventTimeTemporalJoinTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_CONFIGURATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `FRAME_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `UNOWNED_KEY_GROUP` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `LATE_RECORD` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `WATERMARK_REGRESSION` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `STATE_ENTRY_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `STATE_BYTE_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `MISSING_RETRACTION` | A required declaration or value is absent. | Set the required field or declaration before resubmission. |
| `IO_FAILURE` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `USER_FUNCTION_FAILURE` | The operation failed before a valid result could be established. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. |
| `WRONG_THREAD` | The runtime reported the closed wrong thread condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `CLOSED` | The runtime reported the closed closed condition. | Correct the reported condition and retry only when the operation is idempotent. |

### `StreamTimeException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamTimeException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamTimeException.java). Tested evidence: `StreamTimeServiceTest`, `FinalEventTimeDecimalWindowTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_CONFIGURATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INVALID_INPUT` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `INPUT_WATERMARK_REGRESSION` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `OPERATOR_WATERMARK_REGRESSION` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `STATUS_GENERATION` | The runtime reported the closed status generation condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `STATUS_TRANSITION` | The runtime reported the closed status transition condition. | Correct the reported condition and retry only when the operation is idempotent. |
| `ACTIVATION_WATERMARK` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `IDLE_TIMEOUT` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `TIMER_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `INVALID_TIMER` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `CALLBACK_FAILURE` | The operation failed before a valid result could be established. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. |
| `THREAD_OWNERSHIP` | The runtime reported the closed thread ownership condition. | Correct the reported condition and retry only when the operation is idempotent. |

### `StreamWindowException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamWindowException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamWindowException.java). Tested evidence: `FinalEventTimeDecimalWindowTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_CONFIGURATION` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `WINDOW_LIMIT` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `WINDOW_ARITHMETIC` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `LATE_RECORD` | The event-time or timer contract was violated. | Correct timestamps/watermarks, bounded lateness, timer scope, or window binding before retrying. |
| `CORRUPT_STATE` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `CORRUPT_TIMER` | Bytes failed canonical integrity or identity validation. | Stop writes, preserve evidence, restore a known-good artifact/checkpoint/input, and repair storage or transport. |
| `TERMINAL_FAILURE` | The operation failed before a valid result could be established. | Inspect the preserved cause, correct the input or dependency, and retry only when the outcome is recoverable. |

### `StreamConnectorConformanceException`

Source: [`rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamConnectorConformanceException.java`](../../../rowweave-core/src/main/java/io/rowweave/stream/spi/conformance/StreamConnectorConformanceException.java). Tested evidence: `StreamBoundedSourceConformanceKitTest`.

| Code | Meaning and likely cause | Corrective action |
|---|---|---|
| `INVALID_FIXTURE` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SOURCE_REPLAY_MISMATCH` | A stale identity, concurrent writer, fence, or incompatible producer/consumer state was detected. | Use the current artifact, attempt, epoch, fence, or schema identity; make retries idempotent and resolve the coordinator conflict. |
| `SOURCE_POSITION_NOT_MONOTONIC` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SOURCE_TERMINATION_PROTOCOL_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SOURCE_TIME_PROTOCOL_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |
| `SOURCE_CONTROL_TIMEOUT` | A storage, transport, or operating-system dependency failed. | Repair or wait for the dependency, verify ownership and permissions, then retry only recoverable operations. |
| `FRAME_LIMIT_EXCEEDED` | A declared bounded resource was exhausted. | Reduce state, fan-out, parallelism, or frame pressure; raise the explicit bound only after capacity review. |
| `SINK_TRANSACTION_PROTOCOL_INVALID` | The input, descriptor, protocol, or requested operation is outside the closed supported contract. | Use a supported descriptor, opcode, capability, or protocol version; do not bypass admission. |

## Refusal route map

| Boundary | Authoritative detail |
|---|---|
| Batch graph shape, schema conversion, validation, and spill | [Batch behavior](batch-behavior.md) |
| Stream lifecycle, state/time, recovery, rescale, and stable refusals | [Stream behavior](stream-behavior.md) |
| Pipeline definition, hand-off authority, codec, and durable store refusals | [Pipeline boundary](pipeline-boundary.md) |
| Portable artifacts, magic numbers, versions, and frame limits | [Format constants](format-constants.md) |
| Operator incident flow and monitor diagnostics | [Troubleshooting](../operations/troubleshooting.md) |
